using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Application.Transformations;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Diagnostics;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Application.Services;

/// <summary>
/// Orchestrates the sync pipeline for one CDM snapshot workbook:
///
///   Phase 1 — Connect to Dataverse
///   Phase 2 — First Excel pass: build match-key set for pre-warm
///   Phase 3 — Pre-warm module lookup dictionaries (parallel per module)
///   Phase 4 — Stream Excel batches and run ProcessBatchAsync per module
///   Phase 5 — Dead-letter any module failures
///
/// The source is now a blob-delivered .xlsx instead of SQL MI, but the row shape
/// and every processing phase are identical to the previous database-backed version.
/// The daily-cron tracking/abandonment machinery is gone — a blob trigger processes
/// each uploaded file exactly once.
///
/// Memory model: streaming — peak ~one active batch, independent of file size.
/// </summary>
public class SyncOrchestrator
{
    private readonly IExcelDataReader _excelReader;
    private readonly IDataverseRepository _repository;
    private readonly IEnumerable<IModuleProcessor> _processors;
    private readonly IDeadLetterService _deadLetter;
    private readonly SyncSettings _settings;
    private readonly ILogger<SyncOrchestrator> _logger;

    public SyncOrchestrator(
        IExcelDataReader excelReader,
        IDataverseRepository repository,
        IEnumerable<IModuleProcessor> processors,
        IDeadLetterService deadLetter,
        SyncSettings settings,
        ILogger<SyncOrchestrator> logger)
    {
        _excelReader = excelReader;
        _repository = repository;
        _processors = processors;
        _deadLetter = deadLetter;
        _settings = settings;
        _logger = logger;
    }

    public async Task<SyncResult> ExecuteSyncAsync(string xlsxPath, CancellationToken ct = default)
    {
        var result = new SyncResult();
        var sw = Stopwatch.StartNew();

        var orderedProcessors = _processors.OrderBy(p => p.Order).ToList();
        _logger.LogInformation(
            "EventName={EventName} Modules={Modules}",
            LogEvents.WorkPending, string.Join(",", orderedProcessors.Select(p => p.ModuleName)));

        // ═══════════════════════════════════════════════
        // PHASE 1: Connect to Dataverse
        // ═══════════════════════════════════════════════
        if (!await _repository.ConnectAsync(ct))
        {
            result.Errors.Add("Dataverse connection failed");
            result.Duration = sw.Elapsed;
            return result;
        }

        // ═══════════════════════════════════════════════
        // PHASE 2: First Excel pass — build match-key set for pre-warm
        // ───────────────────────────────────────────────
        // Lightweight ACCT_NUM scan. We strip leading zeros via CommonTransformation so
        // the resulting set matches what each processor's GetMatchKey returns. Holding
        // only string keys (not full records) keeps memory minimal for streaming Phase 4.
        // ═══════════════════════════════════════════════
        var phase2Sw = Stopwatch.StartNew();
        _logger.LogInformation("EventName={EventName}", LogEvents.Phase2KeysStarted);

        var matchKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var raw in _excelReader.StreamAccountNumbers(xlsxPath))
        {
            ct.ThrowIfCancellationRequested();
            var stripped = CommonTransformation.StripLeadingZeros(raw.Trim());
            if (!string.IsNullOrEmpty(stripped))
                matchKeys.Add(stripped);
        }

        phase2Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} UniqueKeys={Keys} ElapsedSec={Elapsed:F1}",
            LogEvents.Phase2KeysComplete, matchKeys.Count, phase2Sw.Elapsed.TotalSeconds);

        if (matchKeys.Count == 0)
        {
            _logger.LogWarning("EventName={EventName} Reason=NoRows", LogEvents.NothingToProcess);
            result.Duration = sw.Elapsed;
            return result;
        }

        // ═══════════════════════════════════════════════
        // PHASE 3: Pre-warm module lookup dictionaries (parallel)
        // ═══════════════════════════════════════════════
        _logger.LogInformation(
            "EventName={EventName} Processors={Count}", LogEvents.Phase3Started, orderedProcessors.Count);

        // Pre-warm tasks fan out per module. Each captures its own success/failure so we can
        // fail-fast — without this guard, a pre-warm auth failure cascades into N batches of
        // doomed module updates.
        var preWarmTasks = orderedProcessors.Select(async p =>
        {
            try
            {
                await p.PreWarmAsync(matchKeys, ct);
                return (Module: p.ModuleName, Ok: true);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Module}] Pre-warm FAILED", p.ModuleName);
                result.Errors.Add($"[{p.ModuleName}] Pre-warm: {ex.Message}");
                return (Module: p.ModuleName, Ok: false);
            }
        }).ToList();

        var preWarmResults = await Task.WhenAll(preWarmTasks);
        var failedSet = preWarmResults.Where(r => !r.Ok).Select(r => r.Module)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Continue with only the modules whose pre-warm succeeded. A failed module MUST be
        // excluded from Phase 4: an empty LookupDict would skip every record and report zero
        // failures, silently masking the problem.
        var activeProcessors = orderedProcessors.Where(p => !failedSet.Contains(p.ModuleName)).ToList();

        if (failedSet.Count > 0)
            _logger.LogError(
                "EventName={EventName} FailedModules={Failed} ContinuingModules={Active}",
                LogEvents.PreWarmPartialFailure,
                string.Join(",", failedSet),
                string.Join(",", activeProcessors.Select(p => p.ModuleName)));

        if (activeProcessors.Count == 0)
        {
            _logger.LogCritical(
                "EventName={EventName} FailedModules={Modules} Action=AbortSync",
                LogEvents.PreWarmAborted, string.Join(",", failedSet));
            result.Duration = sw.Elapsed;
            return result;
        }

        _logger.LogInformation(
            "EventName={EventName} ActiveProcessors={Count}", LogEvents.Phase3Complete, activeProcessors.Count);

        // ═══════════════════════════════════════════════
        // PHASE 4: Stream Excel → run module processors per batch
        // ═══════════════════════════════════════════════
        var phase4Sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} ActiveProcessors={ModuleCount} ExcelBatchSize={BatchSize}",
            LogEvents.Phase4Started, activeProcessors.Count, _settings.ExcelBatchSize);

        var (moduleResults, totalRowsRead, dataMthKey) =
            ExecuteModulesTrack(xlsxPath, activeProcessors, ct);

        foreach (var kvp in moduleResults)
            result.ModuleResults[kvp.Key] = kvp.Value;

        // TotalRowsRead comes from the actual Excel stream — NOT from summing per-module
        // counters (each module sees every row, so summing would multiply by module count).
        result.TotalRowsRead = totalRowsRead;
        result.MthKey = dataMthKey;

        phase4Sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} ElapsedSec={Elapsed:F1}",
            LogEvents.Phase4Complete, phase4Sw.Elapsed.TotalSeconds);

        // ═══════════════════════════════════════════════
        // PHASE 5: Dead-letter module failures
        // ═══════════════════════════════════════════════
        // Correlation id for error-table rows — joins with App Insights operation_Id.
        var invocationId = Activity.Current?.RootId ?? Guid.NewGuid().ToString();

        foreach (var kvp in result.ModuleResults)
        {
            if (kvp.Value.Failures.Count > 0)
                await _deadLetter.WriteAsync(kvp.Key, dataMthKey, invocationId, kvp.Value.Failures, ct);
        }

        sw.Stop();
        result.Duration = sw.Elapsed;
        _logger.LogInformation("Sync complete. {Summary}", result.Summary);

        return result;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Phase 4 — Modules track
    // ───────────────────────────────────────────────────────────────────
    // Opens its own Excel stream and, for each batch, fans out across every active
    // processor's ProcessBatchAsync. Lookup dicts are already populated by Phase 3
    // pre-warm, so there's no setup latency here.
    //
    // Catches all exceptions so a module failure never aborts the whole track. Per-batch
    // per-module exceptions are also caught individually so one bad module doesn't break
    // the others.
    // ═══════════════════════════════════════════════════════════════════
    private (Dictionary<string, ModuleResult> Results, int TotalRowsRead, int MthKey)
        ExecuteModulesTrack(
            string xlsxPath,
            List<IModuleProcessor> activeProcessors,
            CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var results = new Dictionary<string, ModuleResult>(StringComparer.OrdinalIgnoreCase);
        int totalRowsRead = 0;
        int mthKey = 0;

        try
        {
            _logger.LogInformation(
                "EventName={EventName} ActiveProcessors={Count}",
                LogEvents.ModulesTrackStarted, activeProcessors.Count);

            int batchNum = 0;
            foreach (var excelBatch in _excelReader.StreamBatches(xlsxPath, _settings.ExcelBatchSize))
            {
                ct.ThrowIfCancellationRequested();
                batchNum++;
                totalRowsRead += excelBatch.Count;
                var batchSw = Stopwatch.StartNew();

                var transformed = CommonTransformation.TransformBatch(excelBatch);

                // Capture the data partition (MTH_KEY) once — used to tag dead-letter rows.
                if (mthKey == 0 && transformed.Count > 0)
                    mthKey = transformed[0].MthKey;

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

                Task.WaitAll(moduleTasks.ToArray(), ct);

                foreach (var moduleTask in moduleTasks)
                {
                    var moduleResult = moduleTask.Result;
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

        return (results, totalRowsRead, mthKey);
    }
}
