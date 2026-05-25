using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Domain.Models;
using Fhn.Cdm.DataverseSync.Infrastructure.DeadLetter;

namespace Fhn.Cdm.DataverseSync.Functions;

/// <summary>
/// Drains the dead-letter Storage Queue into the Dataverse error table.
///
/// One message = one failed record. Retry semantics:
///   - Attempts 1..(MaxAttempts-1): rethrow on failure so the runtime requeues
///   - Final attempt (dequeueCount &gt;= MaxAttempts): catch, archive the record
///     to blob + App Insights, and return normally so the runtime deletes the
///     message. Nothing should ever land in the {queue}-poison sibling queue
///     because we explicitly handle the give-up case ourselves.
///
/// IMPORTANT: <see cref="MaxAttempts"/> must match <c>host.json</c>'s
/// <c>extensions.queues.maxDequeueCount</c> (default 5). If host.json raises that
/// number and this constant stays at 5, we'd archive prematurely on attempt 5
/// while the runtime keeps retrying.
/// </summary>
public class DeadLetterProcessorFunction
{
    // Must match host.json extensions.queues.maxDequeueCount.
    private const int MaxAttempts = 5;

    private readonly DataverseErrorTableService _writer;
    private readonly FailureBlobWriter _archive;
    private readonly ILogger<DeadLetterProcessorFunction> _logger;

    public DeadLetterProcessorFunction(
        DataverseErrorTableService writer,
        FailureBlobWriter archive,
        ILogger<DeadLetterProcessorFunction> logger)
    {
        _writer = writer;
        _archive = archive;
        _logger = logger;
    }

    [Function("DeadLetterProcessor")]
    public async Task RunAsync(
        [QueueTrigger("%DeadLetterQueueName%", Connection = "AzureWebJobsStorage")] DeadLetterMessage msg,
        int dequeueCount,
        CancellationToken ct)
    {
        _logger.LogInformation(
            "EventName={EventName} Module={Module} MthKey={MthKey} Account={Account} LoanId={LoanId} DequeueCount={DequeueCount}",
            "DeadLetterDequeued", msg.ModuleName, msg.MthKey,
            msg.AccountNumber ?? "",
            msg.LoanIdentifier?.ToString() ?? "",
            dequeueCount);

        try
        {
            await _writer.WriteOneAsync(msg, ct);
        }
        catch (Exception ex) when (dequeueCount >= MaxAttempts)
        {
            // Final attempt — persist a durable artifact and ACK the message.
            // Wrapped in its own try/catch so a blob-write failure can't escape
            // and put us in an infinite requeue loop.
            _logger.LogError(ex,
                "EventName={EventName} Module={Module} MthKey={MthKey} Account={Account} LoanId={LoanId} DequeueCount={DequeueCount} Error={Error}",
                "DeadLetterGivenUp", msg.ModuleName, msg.MthKey,
                msg.AccountNumber ?? "",
                msg.LoanIdentifier?.ToString() ?? "",
                dequeueCount,
                ex.Message);

            try
            {
                await _archive.WriteAsync(msg, ex, ct);
            }
            catch (Exception archiveEx)
            {
                // Absolute last-ditch — App Insights is our only remaining sink.
                // We deliberately do NOT rethrow; rethrowing here would consume
                // a dequeue attempt without a successful archive, and the next
                // delivery would loop straight back into this same branch.
                _logger.LogCritical(archiveEx,
                    "EventName={EventName} Module={Module} MthKey={MthKey} Account={Account} LoanId={LoanId} Key={Key} OriginalError={OriginalError}",
                    "FailureArchiveWriteFailed", msg.ModuleName, msg.MthKey,
                    msg.AccountNumber ?? "",
                    msg.LoanIdentifier?.ToString() ?? "",
                    msg.Key,
                    msg.ErrorMessage);
            }
        }
        catch (Exception ex)
        {
            // Not the final attempt — log and rethrow so the runtime requeues.
            _logger.LogWarning(ex,
                "EventName={EventName} Module={Module} MthKey={MthKey} Account={Account} LoanId={LoanId} DequeueCount={DequeueCount} Error={Error}",
                "ErrorTableWriteFailed", msg.ModuleName, msg.MthKey,
                msg.AccountNumber ?? "",
                msg.LoanIdentifier?.ToString() ?? "",
                dequeueCount,
                ex.Message);
            throw;
        }
    }
}
