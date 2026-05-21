using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Application;
using Fhn.Cdm.DataverseSync.Application.Mapping;
using Fhn.Cdm.DataverseSync.Application.Transformations;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Application.Services;

/// <summary>
/// Orchestrates the complete sync pipeline:
///
///   Phase 1 — Compute MaxMonth + daily check (SQL COUNT vs tracking blob)
///   Phase 2 — First SQL pass: build match-key set for pre-warm
///   Phase 3 — Pre-warm module lookup dictionaries (parallel per module)
///   Phase 4 — Two independent tracks run in parallel until both complete:
///               • StagingTrack:  truncate → SQL stream → BatchInsert (audit copy)
///               • ModulesTrack:  SQL stream → ProcessBatchAsync per module
///             Each track has its own SQL reader and its own try/catch envelope,
///             so a slow truncate no longer blocks module updates and a failure
///             in one track doesn't abort the other.
///   Phase 5 — Save tracking state (after both tracks join)
///
/// Memory model: streaming — peak ~20 MB per active SQL batch.
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
        // PHASE 3: Pre-warm module lookup dictionaries (parallel)
        // ───────────────────────────────────────────────
        // The previous design ran the staging truncate alongside pre-warm here.
        // We learned that the bulk-delete async job often dominated wall-clock
        // (>30 min), forcing modules to wait when their data dependencies were
        // already satisfied. Truncate is now part of the staging track in Phase 4
        // so modules can start as soon as pre-warm completes.
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

        // Pre-warm tasks fan out per module. Each task captures its own success/failure
        // so we can fail-fast below — without this guard, a pre-warm auth failure
        // cascades into N batches of doomed module updates.
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

        await Task.WhenAll(preWarmTasks);

        // If any pre-warm fails we don't have a valid lookup dict for that module.
        // The staging track is still safe to run (it doesn't depend on pre-warm),
        // but we abort the entire sync so the failure is surfaced loudly — proceeding
        // would silently skip every record for the failed module.
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
            "EventName={EventName} ActiveProcessors={Count}",
            "Phase3Complete", activeProcessors.Count);

        // ═══════════════════════════════════════════════
        // PHASE 4: Two independent tracks in parallel
        // ───────────────────────────────────────────────
        // StagingTrack: truncate → SQL stream → BatchInsert
        // ModulesTrack: SQL stream → ProcessBatchAsync per module
        //
        // Each track has its own SQL reader (two independent connections, two
        // sequential passes over the same MTH_KEY partition) and its own try/catch.
        // They share no mutable state; failure of one does not abort the other.
        // Phase 5 waits for both via Task.WhenAll before saving tracking state.
        // ═══════════════════════════════════════════════
        var phase4Sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} ActiveProcessors={ModuleCount} SqlBatchSize={SqlBatchSize}",
            "Phase4Started", activeProcessors.Count, _settings.SqlBatchSize);

        var stagingTrackTask = ExecuteStagingTrackAsync(maxMthKey, ct);
        var modulesTrackTask = ExecuteModulesTrackAsync(maxMthKey, activeProcessors, ct);

        await Task.WhenAll(stagingTrackTask, modulesTrackTask);

        var stagingMetrics = await stagingTrackTask;
        var moduleResults = await modulesTrackTask;

        // Fold staging metrics into the SyncResult.
        result.StagingRowsInserted = stagingMetrics.Succeeded;
        result.StagingRowsFailed = stagingMetrics.Failed;
        var stagingFailures = stagingMetrics.Failures;

        // Fold module results into the SyncResult.
        foreach (var (moduleName, moduleResult) in moduleResults)
        {
            result.ModuleResults[moduleName] = moduleResult;
        }

        // TotalRowsRead comes from the modules track (always runs); both tracks read
        // the same SQL partition so the count is the same either way.
        result.TotalRowsRead = moduleResults.Values.Sum(m => m.RowsProcessed + m.RowsSkipped);

        phase4Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} StagingSucceeded={Ok} StagingFailed={Failed} ElapsedSec={Elapsed:F1}",
            "Phase4Complete", stagingMetrics.Succeeded, stagingMetrics.Failed,
            phase4Sw.Elapsed.TotalSeconds);

        // Correlation id for the error-table rows: Activity.Current.RootId carries the
        // Functions invocation id (set by the worker host), giving us a join key with
        // App Insights operation_Id. Fall back to a fresh GUID if no Activity exists
        // (e.g., when run outside the Functions host during tests).
        var invocationId = System.Diagnostics.Activity.Current?.RootId
                           ?? Guid.NewGuid().ToString();

        // Dead-letter staging failures
        if (stagingFailures.Count > 0)
            await _deadLetter.WriteAsync("Staging", maxMthKey, invocationId, stagingFailures, ct);

        // Dead-letter module failures
        foreach (var (moduleName, moduleResult) in result.ModuleResults)
        {
            if (moduleResult.Failures.Count > 0)
                await _deadLetter.WriteAsync(moduleName, maxMthKey, invocationId, moduleResult.Failures, ct);
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

    // ═══════════════════════════════════════════════════════════════════
    // Phase 4 — Staging track
    // ───────────────────────────────────────────────────────────────────
    // 1. Truncate staging if it has rows (skip the BulkDelete entirely when
    //    already empty — eg. first run, or after a clean prior cycle).
    // 2. Open a fresh SQL stream and insert each batch into the staging table.
    //
    // Owns: its own SqlMiDataReader connection, its own Dataverse batch calls,
    //       its own failure aggregation. Catches all exceptions so the modules
    //       track is never aborted by a staging problem.
    // ═══════════════════════════════════════════════════════════════════
    private async Task<(int Succeeded, int Failed, List<FailedRecord> Failures)>
        ExecuteStagingTrackAsync(int maxMthKey, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        int succeeded = 0;
        int failed = 0;
        var failures = new List<FailedRecord>();

        try
        {
            _logger.LogInformation(
                "EventName={EventName} Table={Table}",
                "StagingTrackStarted", StagingEntityMapper.EntityLogicalName);

            // ── Truncate (or skip if already empty) ──
            var stagingHasRows = !await _repository.IsTableEmptyAsync(
                StagingEntityMapper.EntityLogicalName, ct);

            if (stagingHasRows)
            {
                await _repository.DeleteAllAsync(StagingEntityMapper.EntityLogicalName, ct);
            }
            else
            {
                _logger.LogInformation(
                    "EventName={EventName} Table={Table} Reason=AlreadyEmpty",
                    "TruncateSkipped", StagingEntityMapper.EntityLogicalName);
            }

            // ── SQL stream → staging inserts ──
            int batchNum = 0;
            await foreach (var sqlBatch in _sqlReader.StreamBatchesAsync(maxMthKey, _settings.SqlBatchSize, ct))
            {
                batchNum++;
                var batchSw = Stopwatch.StartNew();

                var transformed = CommonTransformation.TransformBatch(sqlBatch);
                var stagingEntities = StagingEntityMapper.MapBatch(transformed);

                var batchResult = await _repository.BatchInsertAsync(stagingEntities, ct);
                succeeded += batchResult.Succeeded;
                failed += batchResult.Failed;

                // Enrich staging failures with AccountNumber + LoanIdentifier directly
                // from the insert entity's attributes — StagingEntityMapper.MapBatch
                // already set these on every entity, but the dead-letter writer wants
                // them as first-class fields, not buried in Entity.Attributes.
                foreach (var failure in batchResult.Failures)
                {
                    if (failure.Entity != null)
                    {
                        failure.AccountNumber = failure.Entity.Contains("crbee_accountnumber")
                            ? failure.Entity["crbee_accountnumber"]?.ToString()
                            : null;
                        failure.LoanIdentifier = failure.Entity.Contains("crbee_loanidentifier")
                            ? failure.Entity["crbee_loanidentifier"] as int?
                            : null;
                    }
                }
                failures.AddRange(batchResult.Failures);

                batchSw.Stop();
                _logger.LogInformation(
                    "EventName={EventName} Batch={Batch} BatchRows={Rows} Succeeded={Ok} Failed={Failed} BatchSec={BatchSec:F1} TrackElapsedSec={Elapsed:F1}",
                    "StagingBatchComplete", batchNum, stagingEntities.Count,
                    batchResult.Succeeded, batchResult.Failed,
                    batchSw.Elapsed.TotalSeconds, sw.Elapsed.TotalSeconds);
            }

            sw.Stop();
            _logger.LogInformation(
                "EventName={EventName} Succeeded={Ok} Failed={Failed} Batches={Batches} ElapsedSec={Elapsed:F1}",
                "StagingTrackComplete", succeeded, failed, batchNum, sw.Elapsed.TotalSeconds);
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex,
                "EventName={EventName} ElapsedSec={Elapsed:F1}",
                "StagingTrackFailed", sw.Elapsed.TotalSeconds);
        }

        return (succeeded, failed, failures);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Phase 4 — Modules track
    // ───────────────────────────────────────────────────────────────────
    // Opens its own SQL stream and, for each batch, fans out across every
    // active processor's ProcessBatchAsync. Lookup dicts are already populated
    // by Phase 3 pre-warm, so there's no setup latency here.
    //
    // Catches all exceptions so the staging track is never aborted by a
    // module failure. Per-batch per-module exceptions are also caught
    // individually so one bad module doesn't break the others.
    // ═══════════════════════════════════════════════════════════════════
    private async Task<Dictionary<string, ModuleResult>> ExecuteModulesTrackAsync(
        int maxMthKey,
        List<IModuleProcessor> activeProcessors,
        CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var results = new Dictionary<string, ModuleResult>(StringComparer.OrdinalIgnoreCase);

        try
        {
            _logger.LogInformation(
                "EventName={EventName} ActiveProcessors={Count}",
                "ModulesTrackStarted", activeProcessors.Count);

            int batchNum = 0;
            await foreach (var sqlBatch in _sqlReader.StreamBatchesAsync(maxMthKey, _settings.SqlBatchSize, ct))
            {
                batchNum++;
                var batchSw = Stopwatch.StartNew();

                var transformed = CommonTransformation.TransformBatch(sqlBatch);

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

                await Task.WhenAll(moduleTasks);

                foreach (var moduleTask in moduleTasks)
                {
                    var moduleResult = await moduleTask;
                    if (!results.TryGetValue(moduleResult.ModuleName, out var aggr))
                    {
                        aggr = new ModuleResult { ModuleName = moduleResult.ModuleName };
                        results[moduleResult.ModuleName] = aggr;
                    }

                    aggr.RowsUpdated += moduleResult.RowsUpdated;
                    aggr.RowsFailed += moduleResult.RowsFailed;
                    aggr.RowsSkipped += moduleResult.RowsSkipped;
                    aggr.RowsProcessed += moduleResult.RowsProcessed;
                    aggr.Failures.AddRange(moduleResult.Failures);
                    aggr.Errors.AddRange(moduleResult.Errors);
                }

                batchSw.Stop();
                _logger.LogInformation(
                    "EventName={EventName} Batch={Batch} BatchRows={Rows} BatchSec={BatchSec:F1} TrackElapsedSec={Elapsed:F1}",
                    "ModulesBatchComplete", batchNum, transformed.Count,
                    batchSw.Elapsed.TotalSeconds, sw.Elapsed.TotalSeconds);
            }

            sw.Stop();
            _logger.LogInformation(
                "EventName={EventName} Batches={Batches} Modules={Modules} ElapsedSec={Elapsed:F1}",
                "ModulesTrackComplete", batchNum, results.Count, sw.Elapsed.TotalSeconds);
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex,
                "EventName={EventName} ElapsedSec={Elapsed:F1}",
                "ModulesTrackFailed", sw.Elapsed.TotalSeconds);
        }

        return results;
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
