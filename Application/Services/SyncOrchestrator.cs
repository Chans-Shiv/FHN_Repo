using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Application;
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

    public async Task<SyncResult> ExecuteSyncAsync(CancellationToken ct = default)
    {
        var result = new SyncResult();
        var sw = Stopwatch.StartNew();

        // ═══════════════════════════════════════════════
        // PHASE 1: Compute MaxMonth + daily data check
        // ───────────────────────────────────────────────
        // MaxMonth = previous calendar month in YYYYMM (e.g., May 18 → 202604).
        // The upstream pipeline always loads the prior month's snapshot into SQL,
        // so we filter by this computed value instead of reading MAX(MTH_KEY).
        // ═══════════════════════════════════════════════
        var maxMthKey = MonthKeyCalculator.PreviousMonth(DateTime.UtcNow);
        _logger.LogInformation(
            "EventName={EventName} MaxMthKey={MaxMthKey} Now={NowUtc:o}",
            "Phase1Started", maxMthKey, DateTime.UtcNow);

        var sqlRowCount = await _sqlReader.GetRowCountForMthKeyAsync(maxMthKey, ct);
        _logger.LogInformation(
            "EventName={EventName} MaxMthKey={MaxMthKey} SqlRowCount={SqlRowCount}",
            "Phase1SqlRowCount", maxMthKey, sqlRowCount);

        if (sqlRowCount == 0)
        {
            _logger.LogInformation(
                "EventName={EventName} MaxMthKey={MaxMthKey} Reason=NoRows",
                "Phase1Exit", maxMthKey);
            result.MthKey = maxMthKey;
            result.Duration = sw.Elapsed;
            return result;
        }

        var trackingState = await _tracking.LoadAsync(ct);

        // New month resets abandonments — last month's failures don't apply to new data.
        if (!string.IsNullOrEmpty(trackingState.Month) && trackingState.Month != maxMthKey.ToString())
        {
            _logger.LogInformation("Month changed {Old} → {New}. Clearing abandonment state.",
                trackingState.Month, maxMthKey);
            foreach (var m in trackingState.Modules.Values)
            {
                m.AbandonedAt = null;
                m.ConsecutiveFailureDays = 0;
            }
            if (trackingState.Staging != null)
            {
                trackingState.Staging.AbandonedAt = null;
                trackingState.Staging.ConsecutiveFailureDays = 0;
            }
        }

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
        // PHASE 2: Stream SQL rows + apply common transformation in memory
        // ───────────────────────────────────────────────
        // SqlMiDataReader yields batches of size SqlBatchSize so we never hold the
        // whole result set in the SQL client. Each batch is transformed eagerly and
        // appended to allRecords. Memory peaks at ~284 MB for 142K rows.
        // ═══════════════════════════════════════════════
        var phase2Sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} MaxMthKey={MaxMthKey} SqlBatchSize={SqlBatchSize}",
            "Phase2Started", maxMthKey, _settings.SqlBatchSize);

        var allRecords = new List<Domain.Entities.ConsumerCreditRecord>();
        int sqlBatchesRead = 0;
        await foreach (var sqlBatch in _sqlReader.StreamBatchesAsync(maxMthKey, _settings.SqlBatchSize, ct))
        {
            var transformed = CommonTransformation.TransformBatch(sqlBatch);
            allRecords.AddRange(transformed);
            result.TotalRowsRead += sqlBatch.Count;
            sqlBatchesRead++;

            _logger.LogInformation(
                "EventName={EventName} Batch={Batch} BatchRows={BatchRows} Cumulative={Cumulative} ElapsedSec={Elapsed:F1}",
                "Phase2SqlBatch", sqlBatchesRead, sqlBatch.Count, allRecords.Count,
                phase2Sw.Elapsed.TotalSeconds);
        }

        phase2Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} Records={Count} Batches={Batches} ElapsedSec={Elapsed:F1}",
            "Phase2Complete", allRecords.Count, sqlBatchesRead, phase2Sw.Elapsed.TotalSeconds);

        // ═══════════════════════════════════════════════
        // PHASE 3: In parallel — full truncate of staging + pre-warm module lookups
        // ───────────────────────────────────────────────
        // Truncate is unfiltered: every row in crbee_stg_consumercreditdata goes,
        // regardless of crbee_monthkey. The user's contract is that staging always
        // reflects only the current MaxMonth load.
        //
        // Pre-warm reads module tables (not staging), so the two run safely in parallel.
        // ═══════════════════════════════════════════════
        _logger.LogInformation(
            "EventName={EventName} Table={Table}",
            "Phase3Started", StagingEntityMapper.EntityLogicalName);

        var orderedProcessors = _processors.OrderBy(p => p.Order).ToList();

        // Skip processors whose tracking state has been abandoned via MaxConsecutiveFailureDays.
        // Manual retries via /api/run-module/{name} still run (they bypass this filter).
        var abandonedModules = trackingState.Modules
            .Where(kvp => kvp.Value.AbandonedAt != null)
            .Select(kvp => kvp.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var activeProcessors = orderedProcessors
            .Where(p => !abandonedModules.Contains(p.ModuleName))
            .ToList();

        if (abandonedModules.Count > 0)
        {
            _logger.LogWarning(
                "EventName={EventName} SkippedCount={Count} Modules={Modules}",
                "AbandonedModuleSkipped", abandonedModules.Count, string.Join(",", abandonedModules));
            foreach (var name in abandonedModules)
                result.Errors.Add($"[{name}] Skipped — abandoned at {trackingState.Modules[name].AbandonedAt:o}");
        }

        // Fire-and-await: truncate is a separate task so the pre-warm queries can
        // begin in parallel. Detailed progress logs live inside DeleteAllAsync —
        // grep App Insights for EventName=TruncateRetrievePage / TruncateDeleteBatch.
        var truncateTask = _repository.DeleteAllAsync(StagingEntityMapper.EntityLogicalName, ct);

        var preWarmTasks = activeProcessors.Select(async p =>
        {
            try { await p.PreWarmAsync(allRecords, ct); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Module}] Pre-warm FAILED", p.ModuleName);
                result.Errors.Add($"[{p.ModuleName}] Pre-warm: {ex.Message}");
            }
        });

        await Task.WhenAll(new[] { truncateTask }.Concat(preWarmTasks));
        _logger.LogInformation(
            "EventName={EventName} Table={Table} ActiveProcessors={Count}",
            "Phase3Complete", StagingEntityMapper.EntityLogicalName, activeProcessors.Count);

        // ═══════════════════════════════════════════════
        // PHASE 4: Process batches — staging insert + module updates in parallel
        // ───────────────────────────────────────────────
        // Outer loop is sequential (one SQL-batch slice at a time) so memory stays bounded.
        // Inside each iteration, BatchInsertAsync (staging) fans out concurrently with the
        // N processors' ProcessBatchAsync calls. Repository-level parallelism inside each
        // is capped by MaxParallelBatches.
        // ═══════════════════════════════════════════════
        var phase4Sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} Records={Records} ActiveProcessors={ModuleCount}",
            "Phase4Started", allRecords.Count, activeProcessors.Count);

        int stagingSucceeded = 0;
        int stagingFailed = 0;
        var stagingFailures = new List<FailedRecord>();

        // Slice the in-memory list into SqlBatchSize chunks. Each chunk drives one
        // staging-insert + processor fan-out cycle.
        var batches = allRecords
            .Select((r, i) => new { Record = r, Index = i })
            .GroupBy(x => x.Index / _settings.SqlBatchSize)
            .Select(g => g.Select(x => x.Record).ToList())
            .ToList();

        int batchNum = 0;
        foreach (var batch in batches)
        {
            batchNum++;
            var batchSw = Stopwatch.StartNew();
            _logger.LogInformation(
                "EventName={EventName} Batch={Batch}/{Total} BatchRows={Rows}",
                "Phase4BatchStarted", batchNum, batches.Count, batch.Count);
            // Map batch to staging entities
            var stagingEntities = StagingEntityMapper.MapBatch(batch);

            // Run staging insert + all module processors in parallel
            var stagingTask = _repository.BatchInsertAsync(stagingEntities, ct);

            var moduleTasks = activeProcessors.Select(async processor =>
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

            batchSw.Stop();
            _logger.LogInformation(
                "EventName={EventName} Batch={Batch}/{Total} StagingSucceeded={Ok} StagingFailed={Failed} BatchSec={BatchSec:F1} Phase4ElapsedSec={Phase4Elapsed:F1}",
                "Phase4BatchComplete", batchNum, batches.Count,
                stagingResult.Succeeded, stagingResult.Failed,
                batchSw.Elapsed.TotalSeconds, phase4Sw.Elapsed.TotalSeconds);
        }

        phase4Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} StagingSucceeded={Ok} StagingFailed={Failed} Batches={Batches} ElapsedSec={Elapsed:F1}",
            "Phase4Complete", stagingSucceeded, stagingFailed, batches.Count, phase4Sw.Elapsed.TotalSeconds);

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
        var newState = BuildTrackingState(maxMthKey, sqlRowCount, result, stagingFailures, trackingState);

        // Carry forward state for abandoned modules we skipped this run, so their AbandonedAt sticks.
        foreach (var name in abandonedModules)
        {
            if (!newState.Modules.ContainsKey(name) && trackingState.Modules.TryGetValue(name, out var prev))
                newState.Modules[name] = prev;
        }

        await _tracking.SaveAsync(newState, ct);

        sw.Stop();
        result.Duration = sw.Elapsed;
        _logger.LogInformation("Sync complete. {Summary}", result.Summary);

        return result;
    }

    private TrackingState BuildTrackingState(
        int mthKey,
        long sqlRowCount,
        SyncResult result,
        List<FailedRecord> stagingFailures,
        TrackingState previousState)
    {
        var state = new TrackingState
        {
            Month = mthKey.ToString(),
            SqlRowCount = sqlRowCount,
            StagingLoadedCount = result.StagingRowsInserted,
            LastRunDate = DateTime.UtcNow
        };

        // Staging: synthetic ModuleResult so it goes through the same abandonment pipeline.
        var stagingPrev = previousState.Staging ?? new ModuleTrackingState();
        var stagingPseudoResult = new ModuleResult
        {
            ModuleName = "Staging",
            RowsUpdated = result.StagingRowsInserted,
            RowsFailed = result.StagingRowsFailed,
            Failures = stagingFailures
        };
        state.Staging = UpdateModuleTrackingState(stagingPrev, stagingPseudoResult);

        // If staging was just abandoned, mark count as fully loaded so the daily check stops
        // re-triggering full truncate-and-reloads of a permanently broken dataset.
        if (state.Staging.AbandonedAt != null)
            state.StagingLoadedCount = sqlRowCount;

        foreach (var (moduleName, moduleResult) in result.ModuleResults)
        {
            var prevModule = previousState.Modules.GetValueOrDefault(moduleName) ?? new ModuleTrackingState();
            state.Modules[moduleName] = UpdateModuleTrackingState(prevModule, moduleResult);
        }

        return state;
    }

    /// <summary>
    /// Updates a ModuleTrackingState from the latest ModuleResult.
    /// Handles consecutive-failure streak, hash comparison, and MaxConsecutiveFailureDays abandonment.
    /// </summary>
    private ModuleTrackingState UpdateModuleTrackingState(ModuleTrackingState prev, ModuleResult moduleResult)
    {
        var today = DateTime.UtcNow.ToString("yyyy-MM-dd");
        var next = new ModuleTrackingState { SuccessCount = moduleResult.RowsUpdated };

        if (moduleResult.Failures.Count == 0)
        {
            // Success — clear failure streak and any prior abandonment.
            return next;
        }

        var failedKeys = moduleResult.Failures.Select(f => f.Key).ToList();
        var hash = ComputeFailureHash(failedKeys);

        if (hash == prev.LastFailedHash && prev.LastFailureDate != today)
            next.ConsecutiveFailureDays = prev.ConsecutiveFailureDays + 1;
        else if (hash != prev.LastFailedHash)
            next.ConsecutiveFailureDays = 1;
        else
            next.ConsecutiveFailureDays = prev.ConsecutiveFailureDays;

        next.LastFailureDate = today;
        next.LastFailedHash = hash;
        next.LastFailedKeys = failedKeys;

        // Preserve prior abandonment; raise a new one when the threshold trips.
        next.AbandonedAt = prev.AbandonedAt;
        if (next.AbandonedAt == null && next.ConsecutiveFailureDays >= _settings.MaxConsecutiveFailureDays)
        {
            next.AbandonedAt = DateTime.UtcNow;
            _logger.LogCritical(
                "EventName={EventName} Module={Module} ConsecutiveDays={Days} FailedCount={Count} Hash={Hash}",
                "ModuleAbandoned", moduleResult.ModuleName, next.ConsecutiveFailureDays,
                failedKeys.Count, hash);
        }

        return next;
    }

    private static string ComputeFailureHash(List<string> failedKeys)
    {
        var sorted = failedKeys.OrderBy(k => k).ToList();
        var combined = string.Join("|", sorted);
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(combined));
        return Convert.ToHexString(bytes);
    }
}
