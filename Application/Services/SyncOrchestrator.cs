using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Application.Mapping;
using SqlToDataverseSync.Application.Transformations;
using SqlToDataverseSync.Configuration;
using SqlToDataverseSync.Domain.Interfaces;
using SqlToDataverseSync.Domain.Models;

namespace SqlToDataverseSync.Application.Services;

/// <summary>
/// Orchestrates the complete sync pipeline:
///
///   Phase 1 — Daily data check (SQL COUNT vs tracking file)
///   Phase 2 — Read SQL + common transformation → in-memory records
///   Phase 3 — In parallel: [truncate + load staging] + [pre-warm module lookups]
///   Phase 4 — In parallel per batch: [staging insert] + [5 module processors]
///   Phase 5 — Save tracking state
///
/// Memory model: all records loaded into memory (~284 MB for 142K rows).
/// This is within the 2 GB Flex Consumption limit.
/// </summary>
public class SyncOrchestrator
{
    private readonly ISqlDataReader _sqlReader;
    private readonly IDataverseRepository _repository;
    private readonly IEnumerable<IModuleProcessor> _processors;
    private readonly ITrackingService _tracking;
    private readonly IDeadLetterService _deadLetter;
    private readonly SyncSettings _settings;
    private readonly ILogger<SyncOrchestrator> _logger;

    public SyncOrchestrator(
        ISqlDataReader sqlReader,
        IDataverseRepository repository,
        IEnumerable<IModuleProcessor> processors,
        ITrackingService tracking,
        IDeadLetterService deadLetter,
        SyncSettings settings,
        ILogger<SyncOrchestrator> logger)
    {
        _sqlReader = sqlReader;
        _repository = repository;
        _processors = processors;
        _tracking = tracking;
        _deadLetter = deadLetter;
        _settings = settings;
        _logger = logger;
    }

    public async Task<(bool SqlOk, bool DataverseOk)> TestConnectionsAsync(CancellationToken ct = default)
    {
        var sqlOk = await _sqlReader.TestConnectionAsync(ct);
        var dvOk = await _repository.ConnectAsync(ct);
        _logger.LogInformation("Connections — SQL: {Sql}, Dataverse: {Dv}", sqlOk, dvOk);
        return (sqlOk, dvOk);
    }

    public async Task<SyncResult> ExecuteSyncAsync(CancellationToken ct = default)
    {
        var result = new SyncResult();
        var sw = Stopwatch.StartNew();

        // ═══════════════════════════════════════════════
        // PHASE 1: Daily data check
        // ═══════════════════════════════════════════════
        var maxMthKey = await _sqlReader.GetMaxMthKeyAsync(ct);
        if (maxMthKey == 0)
        {
            _logger.LogInformation("No data found in SQL. Exiting.");
            result.Duration = sw.Elapsed;
            return result;
        }

        var sqlRowCount = await _sqlReader.GetRowCountForMthKeyAsync(maxMthKey, ct);
        var trackingState = await _tracking.LoadAsync(ct);

        if (trackingState.Month == maxMthKey.ToString()
            && trackingState.StagingLoadedCount == sqlRowCount
            && trackingState.SqlRowCount == sqlRowCount)
        {
            _logger.LogInformation(
                "No new data. Month={Month}, SQL={Sql}, Tracked={Tracked}. Exiting.",
                maxMthKey, sqlRowCount, trackingState.StagingLoadedCount);
            result.Duration = sw.Elapsed;
            return result;
        }

        _logger.LogInformation("New/changed data detected. Month={Month}, SQL={Sql}, LastTracked={Last}",
            maxMthKey, sqlRowCount, trackingState.StagingLoadedCount);

        result.MthKey = maxMthKey;

        // Connect to Dataverse
        if (!await _repository.ConnectAsync(ct))
        {
            result.Errors.Add("Dataverse connection failed");
            result.Duration = sw.Elapsed;
            return result;
        }

        // ═══════════════════════════════════════════════
        // PHASE 2: Read SQL + transform → in-memory
        // ═══════════════════════════════════════════════
        _logger.LogInformation("Phase 2: Reading SQL and applying common transformation...");
        var allRecords = new List<Domain.Entities.ConsumerCreditRecord>();

        await foreach (var sqlBatch in _sqlReader.StreamBatchesAsync(maxMthKey, _settings.SqlBatchSize, ct))
        {
            var transformed = CommonTransformation.TransformBatch(sqlBatch);
            allRecords.AddRange(transformed);
            result.TotalRowsRead += sqlBatch.Count;
        }

        _logger.LogInformation("Phase 2 complete: {Count} records in memory", allRecords.Count);

        // ═══════════════════════════════════════════════
        // PHASE 3: In parallel — truncate staging + pre-warm module lookups
        // ═══════════════════════════════════════════════
        _logger.LogInformation("Phase 3: Truncate staging + pre-warm module lookups (parallel)...");
        var orderedProcessors = _processors.OrderBy(p => p.Order).ToList();

        var truncateTask = _repository.DeleteByColumnValueAsync(
            StagingEntityMapper.EntityLogicalName, "crbee_monthkey", maxMthKey, ct);

        var preWarmTasks = orderedProcessors.Select(async p =>
        {
            try { await p.PreWarmAsync(allRecords, ct); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Module}] Pre-warm FAILED", p.ModuleName);
                result.Errors.Add($"[{p.ModuleName}] Pre-warm: {ex.Message}");
            }
        });

        await Task.WhenAll(new[] { truncateTask }.Concat(preWarmTasks));
        _logger.LogInformation("Phase 3 complete. Staging truncated. Lookups ready.");

        // ═══════════════════════════════════════════════
        // PHASE 4: Process batches — staging insert + module updates (parallel)
        // ═══════════════════════════════════════════════
        _logger.LogInformation("Phase 4: Processing {Count} records through staging + {ModuleCount} modules...",
            allRecords.Count, orderedProcessors.Count);

        int stagingSucceeded = 0;
        int stagingFailed = 0;
        var stagingFailures = new List<FailedRecord>();

        // Process in batches for memory-efficient Dataverse operations
        var batches = allRecords
            .Select((r, i) => new { Record = r, Index = i })
            .GroupBy(x => x.Index / _settings.SqlBatchSize)
            .Select(g => g.Select(x => x.Record).ToList())
            .ToList();

        foreach (var batch in batches)
        {
            // Map batch to staging entities
            var stagingEntities = StagingEntityMapper.MapBatch(batch);

            // Run staging insert + all module processors in parallel
            var stagingTask = _repository.BatchInsertAsync(stagingEntities, ct);

            var moduleTasks = orderedProcessors.Select(async processor =>
            {
                try
                {
                    return await processor.ProcessBatchAsync(batch, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[{Module}] FAILED on batch", processor.ModuleName);
                    return new ModuleResult
                    {
                        ModuleName = processor.ModuleName,
                        Errors = { $"Batch failed: {ex.Message}" }
                    };
                }
            }).ToList();

            // Wait for both staging and all modules
            var allTasks = new List<Task> { stagingTask };
            allTasks.AddRange(moduleTasks);
            await Task.WhenAll(allTasks);

            // Collect staging results
            var stagingResult = await stagingTask;
            stagingSucceeded += stagingResult.Succeeded;
            stagingFailed += stagingResult.Failed;
            stagingFailures.AddRange(stagingResult.Failures);

            // Collect module results
            foreach (var moduleTask in moduleTasks)
            {
                var moduleResult = await moduleTask;
                if (!result.ModuleResults.ContainsKey(moduleResult.ModuleName))
                    result.ModuleResults[moduleResult.ModuleName] = new ModuleResult { ModuleName = moduleResult.ModuleName };

                var aggr = result.ModuleResults[moduleResult.ModuleName];
                aggr.RowsUpdated += moduleResult.RowsUpdated;
                aggr.RowsFailed += moduleResult.RowsFailed;
                aggr.RowsSkipped += moduleResult.RowsSkipped;
                aggr.RowsProcessed += moduleResult.RowsProcessed;
                aggr.Failures.AddRange(moduleResult.Failures);
                aggr.Errors.AddRange(moduleResult.Errors);
            }
        }

        result.StagingRowsInserted = stagingSucceeded;
        result.StagingRowsFailed = stagingFailed;

        // Dead-letter staging failures
        if (stagingFailures.Count > 0)
            await _deadLetter.WriteAsync("Staging", stagingFailures, ct);

        // Dead-letter module failures
        foreach (var (moduleName, moduleResult) in result.ModuleResults)
        {
            if (moduleResult.Failures.Count > 0)
                await _deadLetter.WriteAsync(moduleName, moduleResult.Failures, ct);
        }

        // ═══════════════════════════════════════════════
        // PHASE 5: Save tracking state
        // ═══════════════════════════════════════════════
        var newState = BuildTrackingState(maxMthKey, sqlRowCount, result, trackingState);
        await _tracking.SaveAsync(newState, ct);

        sw.Stop();
        result.Duration = sw.Elapsed;
        _logger.LogInformation("Sync complete. {Summary}", result.Summary);

        return result;
    }

    /// <summary>Runs a single module by name (for manual retry).</summary>
    public async Task<SyncResult> ExecuteModuleSyncAsync(
        string moduleName, int? mthKeyOverride = null, CancellationToken ct = default)
    {
        var result = new SyncResult();
        var sw = Stopwatch.StartNew();

        var processor = _processors.FirstOrDefault(p =>
            p.ModuleName.Equals(moduleName, StringComparison.OrdinalIgnoreCase));

        if (processor is null)
            throw new ArgumentException($"Module '{moduleName}' not found");

        if (!await _repository.ConnectAsync(ct))
            throw new InvalidOperationException("Dataverse connection failed");

        var mthKey = mthKeyOverride ?? await _sqlReader.GetMaxMthKeyAsync(ct);
        result.MthKey = mthKey;

        // Read + transform
        var allRecords = new List<Domain.Entities.ConsumerCreditRecord>();
        await foreach (var sqlBatch in _sqlReader.StreamBatchesAsync(mthKey, _settings.SqlBatchSize, ct))
        {
            allRecords.AddRange(CommonTransformation.TransformBatch(sqlBatch));
            result.TotalRowsRead += sqlBatch.Count;
        }

        // Pre-warm + process
        await processor.PreWarmAsync(allRecords, ct);

        var batches = allRecords
            .Select((r, i) => new { Record = r, Index = i })
            .GroupBy(x => x.Index / _settings.SqlBatchSize)
            .Select(g => g.Select(x => x.Record).ToList())
            .ToList();

        var aggr = new ModuleResult { ModuleName = moduleName };
        foreach (var batch in batches)
        {
            var mr = await processor.ProcessBatchAsync(batch, ct);
            aggr.RowsUpdated += mr.RowsUpdated;
            aggr.RowsFailed += mr.RowsFailed;
            aggr.RowsSkipped += mr.RowsSkipped;
            aggr.Failures.AddRange(mr.Failures);
        }

        result.ModuleResults[moduleName] = aggr;
        result.Duration = sw.Elapsed;
        return result;
    }

    private TrackingState BuildTrackingState(
        int mthKey, long sqlRowCount, SyncResult result, TrackingState previousState)
    {
        var state = new TrackingState
        {
            Month = mthKey.ToString(),
            SqlRowCount = sqlRowCount,
            StagingLoadedCount = result.StagingRowsInserted,
            LastRunDate = DateTime.UtcNow
        };

        var today = DateTime.UtcNow.ToString("yyyy-MM-dd");

        foreach (var (moduleName, moduleResult) in result.ModuleResults)
        {
            var prevModule = previousState.Modules.GetValueOrDefault(moduleName) ?? new ModuleTrackingState();
            var moduleState = new ModuleTrackingState { SuccessCount = moduleResult.RowsUpdated };

            if (moduleResult.Failures.Count > 0)
            {
                var failedKeys = moduleResult.Failures.Select(f => f.Key).ToList();
                var hash = ComputeFailureHash(failedKeys);

                if (hash == prevModule.LastFailedHash && prevModule.LastFailureDate != today)
                    moduleState.ConsecutiveFailureDays = prevModule.ConsecutiveFailureDays + 1;
                else if (hash != prevModule.LastFailedHash)
                    moduleState.ConsecutiveFailureDays = 1;
                else
                    moduleState.ConsecutiveFailureDays = prevModule.ConsecutiveFailureDays;

                moduleState.LastFailureDate = today;
                moduleState.LastFailedHash = hash;
                moduleState.LastFailedKeys = failedKeys;
            }

            state.Modules[moduleName] = moduleState;
        }

        return state;
    }

    private static string ComputeFailureHash(List<string> failedKeys)
    {
        var sorted = failedKeys.OrderBy(k => k).ToList();
        var combined = string.Join("|", sorted);
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(combined));
        return Convert.ToHexString(bytes);
    }
}
