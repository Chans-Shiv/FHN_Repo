using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Application.Services;
using SqlToDataverseSync.Domain.Interfaces;

namespace SqlToDataverseSync.Functions;

// ═══════════════════════════════════════════════════
// SCHEDULED SYNC — daily timer trigger
// ═══════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════
// MANUAL SYNC — full run + test connections
// ═══════════════════════════════════════════════════

public class ManualSyncFunction
{
    private readonly SyncOrchestrator _orchestrator;
    private readonly ILogger<ManualSyncFunction> _logger;

    public ManualSyncFunction(SyncOrchestrator orchestrator, ILogger<ManualSyncFunction> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    [Function("TestConnections")]
    public async Task<HttpResponseData> TestConnections(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        CancellationToken ct)
    {
        var (sqlOk, dvOk) = await _orchestrator.TestConnectionsAsync(ct);
        var response = req.CreateResponse(sqlOk && dvOk ? HttpStatusCode.OK : HttpStatusCode.ServiceUnavailable);
        await response.WriteAsJsonAsync(new
        {
            Timestamp = DateTime.UtcNow,
            SqlManagedInstance = new { Connected = sqlOk },
            Dataverse = new { Connected = dvOk },
            AllSystemsGo = sqlOk && dvOk
        }, ct);
        return response;
    }

    [Function("RunSync")]
    public async Task<HttpResponseData> RunSync(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req,
        CancellationToken ct)
    {
        _logger.LogInformation("Manual sync triggered via HTTP");
        var result = await _orchestrator.ExecuteSyncAsync(ct);
        var response = req.CreateResponse(result.StagingRowsFailed == 0 ? HttpStatusCode.OK : HttpStatusCode.PartialContent);
        await response.WriteAsJsonAsync(result, ct);
        return response;
    }
}

// ═══════════════════════════════════════════════════
// PER-MODULE RETRY — run one module independently
// ═══════════════════════════════════════════════════

public class ModuleSyncFunction
{
    private readonly SyncOrchestrator _orchestrator;
    private readonly IEnumerable<IModuleProcessor> _processors;
    private readonly ILogger<ModuleSyncFunction> _logger;

    public ModuleSyncFunction(
        SyncOrchestrator orchestrator,
        IEnumerable<IModuleProcessor> processors,
        ILogger<ModuleSyncFunction> logger)
    {
        _orchestrator = orchestrator;
        _processors = processors;
        _logger = logger;
    }

    /// <summary>POST /api/run-module/{moduleName}?maxMonth=202606</summary>
    [Function("RunModule")]
    public async Task<HttpResponseData> RunSingleModule(
        [HttpTrigger(AuthorizationLevel.Function, "post",
            Route = "run-module/{moduleName}")] HttpRequestData req,
        string moduleName, CancellationToken ct)
    {
        var queryString = req.Url.Query;
        int? mthKey = null;
        if (!string.IsNullOrEmpty(queryString))
        {
            var pairs = queryString.TrimStart('?').Split('&');
            var maxMonthPair = pairs.FirstOrDefault(p => p.StartsWith("maxMonth=", StringComparison.OrdinalIgnoreCase));
            if (maxMonthPair is not null && int.TryParse(maxMonthPair.Split('=')[1], out var m))
                mthKey = m;
        }

        try
        {
            var result = await _orchestrator.ExecuteModuleSyncAsync(moduleName, mthKey, ct);
            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(result, ct);
            return response;
        }
        catch (ArgumentException ex)
        {
            var available = string.Join(", ", _processors.Select(p => p.ModuleName));
            var response = req.CreateResponse(HttpStatusCode.BadRequest);
            await response.WriteAsJsonAsync(new { Error = ex.Message, AvailableModules = available }, ct);
            return response;
        }
    }

    /// <summary>GET /api/modules — list registered processors</summary>
    [Function("ListModules")]
    public async Task<HttpResponseData> ListModules(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "modules")] HttpRequestData req,
        CancellationToken ct)
    {
        var modules = _processors.OrderBy(p => p.Order).Select(p => new
        {
            p.ModuleName, p.Order,
            RetryEndpoint = $"POST /api/run-module/{p.ModuleName}"
        });
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new { RegisteredModules = modules }, ct);
        return response;
    }
}
