using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Domain.Models;
using Fhn.Cdm.DataverseSync.Infrastructure.DeadLetter;

namespace Fhn.Cdm.DataverseSync.Functions;

/// <summary>
/// Drains the dead-letter Storage Queue into the Dataverse error table.
///
/// One message = one failed record. The QueueTrigger runtime delivers messages
/// concurrently (host.json batchSize, default 16) and handles retry/poison
/// transitions automatically:
///   - Throw any exception   → message visibility resets after the lock expires
///                             and the runtime re-delivers it
///   - 5th delivery failure  → runtime moves it to {queue}-poison
///
/// We never catch the Dataverse exception here — that's the whole point of the
/// queue. Letting it propagate is what gives us free retries.
/// </summary>
public class DeadLetterProcessorFunction
{
    private readonly DataverseErrorTableService _writer;
    private readonly ILogger<DeadLetterProcessorFunction> _logger;

    public DeadLetterProcessorFunction(
        DataverseErrorTableService writer,
        ILogger<DeadLetterProcessorFunction> logger)
    {
        _writer = writer;
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
        catch (Exception ex)
        {
            // Log then rethrow — the runtime owns the retry/poison decision.
            _logger.LogError(ex,
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

/// <summary>
/// Records permanently-failed dead-letter messages so they survive in App Insights
/// for manual triage. Triggered after the main queue exhausts its 5 delivery attempts.
/// Auto-created by the Functions runtime — naming convention is <c>{queue}-poison</c>.
/// </summary>
public class DeadLetterPoisonHandler
{
    private readonly ILogger<DeadLetterPoisonHandler> _logger;

    public DeadLetterPoisonHandler(ILogger<DeadLetterPoisonHandler> logger)
    {
        _logger = logger;
    }

    [Function("DeadLetterPoisonHandler")]
    public Task RunAsync(
        [QueueTrigger("%DeadLetterQueueName%-poison", Connection = "AzureWebJobsStorage")] DeadLetterMessage msg)
    {
        _logger.LogError(
            "EventName={EventName} Module={Module} MthKey={MthKey} Account={Account} LoanId={LoanId} Key={Key} InvocationId={InvocationId} EnqueuedAt={EnqueuedAt:o} Error={Error}",
            "DeadLetterPoison", msg.ModuleName, msg.MthKey,
            msg.AccountNumber ?? "",
            msg.LoanIdentifier?.ToString() ?? "",
            msg.Key,
            msg.InvocationId,
            msg.EnqueuedAt,
            msg.ErrorMessage);

        return Task.CompletedTask;
    }
}
