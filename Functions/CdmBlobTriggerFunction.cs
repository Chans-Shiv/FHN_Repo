using System.Diagnostics;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Application.Services;
using Fhn.Cdm.DataverseSync.Diagnostics;

namespace Fhn.Cdm.DataverseSync.Functions;

/// <summary>
/// Blob-triggered entry point. Fires when a CDM snapshot .xlsx lands in the
/// <c>cdm-landing</c> container, streams the blob to a temp file, and runs the
/// same sync pipeline that previously ran on a daily timer against SQL MI.
/// </summary>
public class CdmBlobTriggerFunction
{
    private readonly SyncOrchestrator _orchestrator;
    private readonly ILogger<CdmBlobTriggerFunction> _logger;

    public CdmBlobTriggerFunction(SyncOrchestrator orchestrator, ILogger<CdmBlobTriggerFunction> logger)
    {
        _orchestrator = orchestrator;
        _logger = logger;
    }

    [Function("CdmDataverseSync")]
    public async Task Run(
        [BlobTrigger("cdm-landing/{name}.xlsx", Connection = "SyncStorage")] Stream blobStream,
        string name,
        FunctionContext context,
        CancellationToken ct)
    {
        var fullName = name + ".xlsx";
        var sw = Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} Blob={Blob} InvocationId={Invocation}",
            LogEvents.BlobTriggerStarted, fullName, context.InvocationId);

        // Persist the blob to a temp file: the pipeline reads it twice (key pass +
        // batch pass), and OpenXml needs a seekable source. Cleaned up in finally.
        var tempPath = Path.Combine(Path.GetTempPath(), $"cdm-{Guid.NewGuid():N}.xlsx");
        try
        {
            await using (var fs = File.Create(tempPath))
            {
                await blobStream.CopyToAsync(fs, ct);
                await fs.FlushAsync(ct);
            }

            var result = await _orchestrator.ExecuteSyncAsync(tempPath, ct);

            _logger.LogInformation(
                "EventName={EventName} Blob={Blob} Elapsed={Elapsed} {Summary}",
                LogEvents.BlobTriggerCompleted, fullName, sw.Elapsed, result.Summary);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "EventName={EventName} Blob={Blob} Elapsed={Elapsed}",
                LogEvents.BlobTriggerFailed, fullName, sw.Elapsed);
            throw;
        }
        finally
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); }
            catch { /* best-effort temp cleanup */ }
        }
    }
}
