using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Domain.Interfaces;
using SqlToDataverseSync.Domain.Models;

namespace SqlToDataverseSync.Infrastructure.DeadLetter;

/// <summary>
/// Emits dead-letter records as structured log events. On Azure Functions these flow
/// to Application Insights via the Worker logger pipeline.
///
/// Query in App Insights (KQL):
///   traces
///   | where customDimensions.EventName == "DeadLetterRecord"
///   | extend Module = tostring(customDimensions.Module),
///            Key    = tostring(customDimensions.Key),
///            Error  = tostring(customDimensions.Error)
/// </summary>
public class DeadLetterService : IDeadLetterService
{
    // Hard cap on per-record events per call to keep ingest volume bounded.
    // A summary event still records the full count.
    private const int MaxRecordsLogged = 500;

    private readonly ILogger<DeadLetterService> _logger;

    public DeadLetterService(ILogger<DeadLetterService> logger) => _logger = logger;

    public Task WriteAsync(string moduleName, List<FailedRecord> failures, CancellationToken ct = default)
    {
        if (failures.Count == 0) return Task.CompletedTask;

        _logger.LogError(
            "EventName={EventName} Module={Module} FailedCount={Count}",
            "DeadLetterSummary", moduleName, failures.Count);

        var logged = 0;
        foreach (var f in failures)
        {
            if (logged++ >= MaxRecordsLogged) break;

            _logger.LogError(
                "EventName={EventName} Module={Module} Key={Key} Error={Error}",
                "DeadLetterRecord", moduleName, f.Key, f.ErrorMessage);
        }

        if (failures.Count > MaxRecordsLogged)
        {
            _logger.LogWarning(
                "EventName={EventName} Module={Module} Suppressed={Suppressed}",
                "DeadLetterTruncated", moduleName, failures.Count - MaxRecordsLogged);
        }

        return Task.CompletedTask;
    }
}
