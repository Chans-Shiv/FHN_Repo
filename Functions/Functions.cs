using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Application.Services;

namespace SqlToDataverseSync.Functions;

public class ScheduledSyncFunction
{
    private readonly SyncOrchestrator _orchestrator;
    private readonly ILogger<ScheduledSyncFunction> _logger;

    public ScheduledSyncFunction(SyncOrchestrator orchestrator, ILogger<ScheduledSyncFunction> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    [Function("ScheduledSync")]
    public async Task Run(
        [TimerTrigger("%SyncScheduleCron%")] TimerInfo timerInfo,
        CancellationToken ct)
    {
        _logger.LogInformation("Scheduled sync triggered at {Time}", DateTime.UtcNow);
        var result = await _orchestrator.ExecuteSyncAsync(ct);
        _logger.LogInformation("Scheduled sync finished. {Summary}", result.Summary);
    }
}
