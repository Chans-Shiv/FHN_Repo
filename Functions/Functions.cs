using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Application.Services;

namespace Fhn.Cdm.DataverseSync.Functions;

public class CdmDataverseSyncFunction
{
    private readonly SyncOrchestrator _orchestrator;
    private readonly ILogger<CdmDataverseSyncFunction> _logger;

    public CdmDataverseSyncFunction(SyncOrchestrator orchestrator, ILogger<CdmDataverseSyncFunction> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    [Function("CdmDataverseSync")]
    public async Task Run(
        [TimerTrigger("%SyncScheduleCron%")] TimerInfo timerInfo,
        CancellationToken ct)
    {
        _logger.LogInformation("CdmDataverseSync triggered at {Time}", DateTime.UtcNow);
        var result = await _orchestrator.ExecuteSyncAsync(ct);
        _logger.LogInformation("CdmDataverseSync finished. {Summary}", result.Summary);
    }
}
