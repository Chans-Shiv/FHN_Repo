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
        // PHASE 2: First SQL pass — build match-key set for pre-warm
        // ───────────────────────────────────────────────
        // Lightweight SELECT ACCT_NUM scan. We strip leading zeros via CommonTransformation
        // so the resulting set matches what each processor's GetMatchKey returns. Holding
        // only string keys (not full records) keeps memory minimal for the streaming Phase 4.
        // ═══════════════════════════════════════════════
        var phase2Sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} MaxMthKey={MaxMthKey}",
            "Phase2KeysStarted", maxMthKey);

        var matchKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await foreach (var raw in _sqlReader.StreamAccountNumbersAsync(maxMthKey, ct))
        {
            var stripped = CommonTransformation.StripLeadingZeros(raw.Trim());
            if (!string.IsNullOrEmpty(stripped))
                matchKeys.Add(stripped);
        }

        phase2Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} UniqueKeys={Keys} ElapsedSec={Elapsed:F1}",
            "Phase2KeysComplete", matchKeys.Count, phase2Sw.Elapsed.TotalSeconds);

        // ═══════════════════════════════════════════════
        // PHASE 3: Conditional truncate + parallel pre-warm
        // ───────────────────────────────────────────────
        // - IsTableEmptyAsync first: skip DeleteAllAsync when staging is already empty
        //   (first run / after a clean prior run). Saves the >30 min full-table scan.
        // - Pre-warm reads module tables (not staging) using the match-key set, with
        //   parallelism inside QueryByKeysAsync controlled by PreWarmParallelism.
        // ═══════════════════════════════════════════════
        _logger.LogInformation(
            "EventName={EventName} Table={Table}",
            "Phase3Started", StagingEntityMapper.EntityLogicalName);

        var orderedProcessors = _processors.OrderBy(p => p.Order).ToList();

        // Skip processors whose tracking state has been abandoned via MaxConsecutiveFailureDays.
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

        // Cheap top-1 check before kicking off the expensive truncate path.
        var stagingHasRows = !await _repository.IsTableEmptyAsync(
            StagingEntityMapper.EntityLogicalName, ct);

        Task<int> truncateTask = stagingHasRows
            ? _repository.DeleteAllAsync(StagingEntityMapper.EntityLogicalName, ct)
            : Task.FromResult(0);

        if (!stagingHasRows)
            _logger.LogInformation(
                "EventName={EventName} Table={Table} Reason=AlreadyEmpty",
                "TruncateSkipped", StagingEntityMapper.EntityLogicalName);

        // Pre-warm tasks fan out concurrently with the truncate (and with each other).
        // Each task captures its own success/failure so we can fail-fast below — without
        // this guard, a pre-warm auth failure cascades into 142K doomed staging inserts.
        var preWarmTasks = activeProcessors.Select(async p =>
        {
            try
            {
                await p.PreWarmAsync(matchKeys, ct);
                return (Module: p.ModuleName, Ok: true, Error: (string?)null);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Module}] Pre-warm FAILED", p.ModuleName);
                result.Errors.Add($"[{p.ModuleName}] Pre-warm: {ex.Message}");
                return (Module: p.ModuleName, Ok: false, Error: (string?)ex.Message);
            }
        }).ToList();

        await Task.WhenAll(new Task[] { truncateTask }.Concat(preWarmTasks));

        // Inspect pre-warm outcomes. Any failure means we don't have a valid lookup
        // dict for that module — proceeding would either silently skip every row or
        // hit the same auth error 142K times. Abort instead.
        var preWarmResults = await Task.WhenAll(preWarmTasks);
        var preWarmFailed = preWarmResults.Where(r => !r.Ok).Select(r => r.Module).ToList();
        if (preWarmFailed.Count > 0)
        {
            _logger.LogCritical(
                "EventName={EventName} FailedModules={Modules} Action=AbortSync",
                "PreWarmAborted", string.Join(",", preWarmFailed));

            result.Duration = sw.Elapsed;
            return result;
        }

        _logger.LogInformation(
            "EventName={EventName} Table={Table} ActiveProcessors={Count} TruncateRan={Ran}",
            "Phase3Complete", StagingEntityMapper.EntityLogicalName,
            activeProcessors.Count, stagingHasRows);

        // ═══════════════════════════════════════════════
        // PHASE 4: Second SQL pass — streaming transform + insert + process
        // ───────────────────────────────────────────────
        // For each SqlBatchSize-row batch from SQL: transform → map to staging entities →
        // BatchInsert (staging) in parallel with each active processor's ProcessBatchAsync.
        // The batch is discarded at the end of the iteration so memory peak stays at one
        // batch (~20 MB for 10K rows), not the full dataset.
        // ═══════════════════════════════════════════════
        var phase4Sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} ActiveProcessors={ModuleCount} SqlBatchSize={SqlBatchSize}",
            "Phase4Started", activeProcessors.Count, _settings.SqlBatchSize);

        int stagingSucceeded = 0;
        int stagingFailed = 0;
        var stagingFailures = new List<FailedRecord>();
        int batchNum = 0;

        await foreach (var sqlBatch in _sqlReader.StreamBatchesAsync(maxMthKey, _settings.SqlBatchSize, ct))
        {
            batchNum++;
            var batchSw = Stopwatch.StartNew();
            result.TotalRowsRead += sqlBatch.Count;

            var transformed = CommonTransformation.TransformBatch(sqlBatch);
            var stagingEntities = StagingEntityMapper.MapBatch(transformed);

            _logger.LogInformation(
                "EventName={EventName} Batch={Batch} BatchRows={Rows} TotalRead={TotalRead}",
                "Phase4BatchStarted", batchNum, transformed.Count, result.TotalRowsRead);

            // Run staging insert + all module processors in parallel for this batch.
            var stagingTask = _repository.BatchInsertAsync(stagingEntities, ct);

            var moduleTasks = activeProcessors.Select(async processor =>
            {
                try
                {
                    return await processor.ProcessBatchAsync(transformed, ct);
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

            var allTasks = new List<Task> { stagingTask };
            allTasks.AddRange(moduleTasks);
            await Task.WhenAll(allTasks);

            var stagingResult = await stagingTask;
            stagingSucceeded += stagingResult.Succeeded;
            stagingFailed += stagingResult.Failed;
            stagingFailures.AddRange(stagingResult.Failures);

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
                "EventName={EventName} Batch={Batch} StagingSucceeded={Ok} StagingFailed={Failed} BatchSec={BatchSec:F1} Phase4ElapsedSec={Phase4Elapsed:F1}",
                "Phase4BatchComplete", batchNum, stagingResult.Succeeded, stagingResult.Failed,
                batchSw.Elapsed.TotalSeconds, phase4Sw.Elapsed.TotalSeconds);
        }

        phase4Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} StagingSucceeded={Ok} StagingFailed={Failed} TotalBatches={Batches} ElapsedSec={Elapsed:F1}",
            "Phase4Complete", stagingSucceeded, stagingFailed, batchNum, phase4Sw.Elapsed.TotalSeconds);

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
