using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Application;
using Fhn.Cdm.DataverseSync.Application.Transformations;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Diagnostics;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Application.Services;

/// <summary>
/// Orchestrates the sync pipeline (processors-only — staging removed):
///
///   Phase 1 — Compute MaxMonth + daily check (SQL row count vs tracking blob)
///   Phase 2 — First SQL pass: build match-key set for pre-warm
///   Phase 3 — Pre-warm module lookup dictionaries (parallel per module)
///   Phase 4 — Stream SQL and run ProcessBatchAsync per module
///   Phase 5 — Save tracking state
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
            LogEvents.Phase1Started, maxMthKey, DateTime.UtcNow);

        var sqlRowCount = await _sqlReader.GetRowCountForMthKeyAsync(maxMthKey, ct);
        _logger.LogInformation(
            "EventName={EventName} MaxMthKey={MaxMthKey} SqlRowCount={SqlRowCount}",
            LogEvents.Phase1SqlRowCount, maxMthKey, sqlRowCount);

        if (sqlRowCount == 0)
        {
            _logger.LogInformation(
                "EventName={EventName} MaxMthKey={MaxMthKey} Reason=NoRows",
                LogEvents.Phase1Exit, maxMthKey);
            result.MthKey = maxMthKey;
            result.Duration = sw.Elapsed;
            return result;
        }

        var trackingState = await _tracking.LoadAsync(ct);
        var currentMonthStr = maxMthKey.ToString();

        // New month resets abandonments — last month's failures don't apply to new data.
        if (!string.IsNullOrEmpty(trackingState.Month) && trackingState.Month != currentMonthStr)
        {
            _logger.LogInformation("Month changed {Old} → {New}. Clearing abandonment state.",
                trackingState.Month, maxMthKey);
            foreach (var m in trackingState.Modules.Values)
            {
                m.AbandonedAt = null;
                m.ConsecutiveFailureDays = 0;
                // NOTE: LastCompletedMonth is NOT reset here — it naturally won't match
                // the new currentMonthStr, so the module will run for the new month.
            }
        }

        // ── Decide which processors still have work to do this run ──
        //
        // Filter the registered processor list by tracking state:
        //   - Drop processors that are AbandonedAt != null (gave up on them this month;
        //     month-rollover above will un-abandon them next month).
        //   - Drop processors that already completed this month with the same SQL row
        //     count — their work is genuinely done, no point re-running pre-warm and
        //     Phase 4 for them today.
        //
        // If nothing is left to do, exit early. This is the daily-cron's idempotency
        // guarantee: once a module finishes a month, it doesn't run again until next
        // month (or until SQL row count changes mid-month, which would indicate an
        // upstream re-load).
        var orderedProcessors = _processors.OrderBy(p => p.Order).ToList();

        var abandonedModules = trackingState.Modules
            .Where(kvp => kvp.Value.AbandonedAt != null)
            .Select(kvp => kvp.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var completedModules = trackingState.Modules
            .Where(kvp => kvp.Value.LastCompletedMonth == currentMonthStr
                       && kvp.Value.LastCompletedSqlRowCount == sqlRowCount)
            .Select(kvp => kvp.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var pendingProcessors = orderedProcessors
            .Where(p => !abandonedModules.Contains(p.ModuleName)
                     && !completedModules.Contains(p.ModuleName))
            .ToList();

        if (abandonedModules.Count > 0)
        {
            _logger.LogWarning(
                "EventName={EventName} SkippedCount={Count} Modules={Modules}",
                LogEvents.AbandonedModuleSkipped, abandonedModules.Count, string.Join(",", abandonedModules));
            foreach (var name in abandonedModules)
                result.Errors.Add($"[{name}] Skipped — abandoned at {trackingState.Modules[name].AbandonedAt:o}");
        }

        if (completedModules.Count > 0)
        {
            _logger.LogInformation(
                "EventName={EventName} Month={Month} SkippedCount={Count} Modules={Modules}",
                LogEvents.ModulesAlreadyCompleted, currentMonthStr, completedModules.Count,
                string.Join(",", completedModules));
        }

        if (pendingProcessors.Count == 0)
        {
            _logger.LogInformation(
                "EventName={EventName} Month={Month} SqlRowCount={SqlRowCount}",
                LogEvents.NothingToProcess, currentMonthStr, sqlRowCount);
            result.MthKey = maxMthKey;
            result.Duration = sw.Elapsed;
            return result;
        }

        _logger.LogInformation(
            "EventName={EventName} Month={Month} SqlRowCount={SqlRowCount} PendingModules={Pending}",
            LogEvents.WorkPending, currentMonthStr, sqlRowCount,
            string.Join(",", pendingProcessors.Select(p => p.ModuleName)));

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
            LogEvents.Phase2KeysStarted, maxMthKey);

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
            LogEvents.Phase2KeysComplete, matchKeys.Count, phase2Sw.Elapsed.TotalSeconds);

        // ═══════════════════════════════════════════════
        // PHASE 3: Pre-warm module lookup dictionaries (parallel)
        // ───────────────────────────────────────────────
        // Only the pendingProcessors set (computed in Phase 1) participates here.
        // Abandoned and already-completed modules were filtered out before we even
        // got this far.
        // ═══════════════════════════════════════════════
        _logger.LogInformation(
            "EventName={EventName} PendingProcessors={Count}",
            LogEvents.Phase3Started, pendingProcessors.Count);

        // Pre-warm tasks fan out per module. Each task captures its own success/failure
        // so we can fail-fast below — without this guard, a pre-warm auth failure
        // cascades into N batches of doomed module updates.
        var preWarmTasks = pendingProcessors.Select(async p =>
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

        var preWarmResults = await Task.WhenAll(preWarmTasks);
        var preWarmFailed = preWarmResults.Where(r => !r.Ok).Select(r => r.Module).ToList();
        var failedSet = preWarmFailed.ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Continue with only the modules whose pre-warm succeeded. A failed module is skipped
        // this run — NOT marked complete — so it carries forward its prior tracking state and
        // is retried next run. It MUST be excluded from Phase 4: an empty LookupDict would skip
        // every record, report zero failures, and falsely mark the module complete for the month.
        var activeProcessors = pendingProcessors.Where(p => !failedSet.Contains(p.ModuleName)).ToList();

        if (preWarmFailed.Count > 0)
            _logger.LogError(
                "EventName={EventName} FailedModules={Failed} ContinuingModules={Active}",
                LogEvents.PreWarmPartialFailure,
                string.Join(",", preWarmFailed),
                string.Join(",", activeProcessors.Select(p => p.ModuleName)));

        if (activeProcessors.Count == 0)
        {
            // Every pending module's pre-warm failed — nothing safe to process. Return without
            // saving tracking: prior state is unchanged, so all modules stay pending and the
            // next run retries them in full. (Per-module errors are already in result.Errors.)
            _logger.LogCritical(
                "EventName={EventName} FailedModules={Modules} Action=AbortSync",
                LogEvents.PreWarmAborted, string.Join(",", preWarmFailed));

            result.Duration = sw.Elapsed;
            return result;
        }

        _logger.LogInformation(
            "EventName={EventName} ActiveProcessors={Count}",
            LogEvents.Phase3Complete, activeProcessors.Count);

        // ═══════════════════════════════════════════════
        // PHASE 4: Stream SQL → run module processors per batch
        // ═══════════════════════════════════════════════
        var phase4Sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} ActiveProcessors={ModuleCount} SqlBatchSize={SqlBatchSize}",
            LogEvents.Phase4Started, activeProcessors.Count, _settings.SqlBatchSize);

        var (moduleResults, totalRowsRead) = await ExecuteModulesTrackAsync(maxMthKey, activeProcessors, ct);

        // Fold module results into the SyncResult.
        foreach (var kvp in moduleResults)
        {
            result.ModuleResults[kvp.Key] = kvp.Value;
        }

        // TotalRowsRead comes from the actual SQL stream in the modules track —
        // NOT from summing per-module counters (each module sees every row, so summing
        // would multiply by the module count).
        result.TotalRowsRead = totalRowsRead;

        phase4Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} ElapsedSec={Elapsed:F1}",
            LogEvents.Phase4Complete, phase4Sw.Elapsed.TotalSeconds);

        // Correlation id for error-table rows. Activity.Current.RootId carries the
        // Functions invocation id (set by the worker host), giving us a join key
        // with App Insights operation_Id.
        var invocationId = System.Diagnostics.Activity.Current?.RootId
                           ?? Guid.NewGuid().ToString();

        // Dead-letter module failures
        foreach (var kvp in result.ModuleResults)
        {
            if (kvp.Value.Failures.Count > 0)
                await _deadLetter.WriteAsync(kvp.Key, maxMthKey, invocationId, kvp.Value.Failures, ct);
        }

        // ═══════════════════════════════════════════════
        // PHASE 5: Save tracking state
        // ═══════════════════════════════════════════════
        var newState = BuildTrackingState(maxMthKey, sqlRowCount, result, trackingState);

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
    private async Task<(Dictionary<string, ModuleResult> Results, int TotalRowsRead)>
        ExecuteModulesTrackAsync(
            int maxMthKey,
            List<IModuleProcessor> activeProcessors,
            CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var results = new Dictionary<string, ModuleResult>(StringComparer.OrdinalIgnoreCase);
        int totalRowsRead = 0;

        try
        {
            _logger.LogInformation(
                "EventName={EventName} ActiveProcessors={Count}",
                LogEvents.ModulesTrackStarted, activeProcessors.Count);

            int batchNum = 0;
            await foreach (var sqlBatch in _sqlReader.StreamBatchesAsync(maxMthKey, _settings.SqlBatchSize, ct))
            {
                batchNum++;
                totalRowsRead += sqlBatch.Count;
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
                    LogEvents.ModulesBatchComplete, batchNum, transformed.Count,
                    batchSw.Elapsed.TotalSeconds, sw.Elapsed.TotalSeconds);
            }

            sw.Stop();
            _logger.LogInformation(
                "EventName={EventName} Batches={Batches} Modules={Modules} TotalRowsRead={Rows} ElapsedSec={Elapsed:F1}",
                LogEvents.ModulesTrackComplete, batchNum, results.Count, totalRowsRead, sw.Elapsed.TotalSeconds);
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex,
                "EventName={EventName} RowsReadSoFar={Rows} ElapsedSec={Elapsed:F1}",
                LogEvents.ModulesTrackFailed, totalRowsRead, sw.Elapsed.TotalSeconds);
        }

        return (results, totalRowsRead);
    }

    private TrackingState BuildTrackingState(
        int mthKey,
        long sqlRowCount,
        SyncResult result,
        TrackingState previousState)
    {
        var currentMonthStr = mthKey.ToString();
        var state = new TrackingState
        {
            Month = currentMonthStr,
            SqlRowCount = sqlRowCount,
            LastRunDate = DateTime.UtcNow
        };

        // Carry forward every previously-known module's state, then overwrite the ones
        // we processed this run. This preserves LastCompletedMonth for modules we
        // skipped today (already completed this month / abandoned) so they stay skipped.
        foreach (var kvp in previousState.Modules)
            state.Modules[kvp.Key] = kvp.Value;

        foreach (var kvp in result.ModuleResults)
        {
            var prevModule = previousState.Modules.GetValueOrDefault(kvp.Key) ?? new ModuleTrackingState();
            state.Modules[kvp.Key] = UpdateModuleTrackingState(prevModule, kvp.Value, currentMonthStr, sqlRowCount);
        }

        return state;
    }

    /// <summary>
    /// Updates a ModuleTrackingState from the latest ModuleResult.
    /// Handles consecutive-failure streak, hash comparison, MaxConsecutiveFailureDays
    /// abandonment, and the LastCompletedMonth marker that lets the daily-cron skip
    /// already-finished modules.
    /// </summary>
    private ModuleTrackingState UpdateModuleTrackingState(
        ModuleTrackingState prev,
        ModuleResult moduleResult,
        string currentMonth,
        long sqlRowCount)
    {
        var today = DateTime.UtcNow.ToString("yyyy-MM-dd");
        var next = new ModuleTrackingState { SuccessCount = moduleResult.RowsUpdated };

        // Did this run see every SQL row? Required (alongside zero failures) before we
        // can mark the module complete-for-the-month. If the SQL stream broke off
        // mid-way, RowsProcessed + RowsSkipped < sqlRowCount and we don't set the flag.
        bool sawAllRows = (moduleResult.RowsProcessed + moduleResult.RowsSkipped) == sqlRowCount;

        if (moduleResult.Failures.Count == 0)
        {
            // Success — clear failure streak and any prior abandonment.
            // Mark completion only when we also saw every SQL row this run.
            if (sawAllRows)
            {
                next.LastCompletedMonth = currentMonth;
                next.LastCompletedSqlRowCount = sqlRowCount;
                _logger.LogInformation(
                    "EventName={EventName} Module={Module} Month={Month} SqlRowCount={SqlRowCount} Updated={Updated}",
                    LogEvents.ModuleCompleted, moduleResult.ModuleName, currentMonth, sqlRowCount, moduleResult.RowsUpdated);
            }
            else
            {
                // Carry forward whatever was previously known — don't lose a prior completion
                // marker just because this run was a no-op (e.g., outer cancellation).
                next.LastCompletedMonth = prev.LastCompletedMonth;
                next.LastCompletedSqlRowCount = prev.LastCompletedSqlRowCount;

                _logger.LogWarning(
                    "EventName={EventName} Module={Module} RowsProcessed={Processed} RowsSkipped={Skipped} ExpectedSqlRowCount={Expected}",
                    LogEvents.ModuleIncompleteStream, moduleResult.ModuleName,
                    moduleResult.RowsProcessed, moduleResult.RowsSkipped, sqlRowCount);
            }
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

        // On a failed run, the module is NOT complete for this month — carry forward
        // the previous completion marker (which would be from an earlier month, or
        // null if we've never finished). The day-check will see it doesn't match the
        // current month and the module will retry tomorrow.
        next.LastCompletedMonth = prev.LastCompletedMonth;
        next.LastCompletedSqlRowCount = prev.LastCompletedSqlRowCount;

        // Preserve prior abandonment; raise a new one when the threshold trips.
        next.AbandonedAt = prev.AbandonedAt;
        if (next.AbandonedAt == null && next.ConsecutiveFailureDays >= _settings.MaxConsecutiveFailureDays)
        {
            next.AbandonedAt = DateTime.UtcNow;
            _logger.LogCritical(
                "EventName={EventName} Module={Module} ConsecutiveDays={Days} FailedCount={Count} Hash={Hash}",
                LogEvents.ModuleAbandoned, moduleResult.ModuleName, next.ConsecutiveFailureDays,
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
