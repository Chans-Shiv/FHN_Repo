using System.Text.Json;
using Azure.Storage.Queues;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Diagnostics;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Infrastructure.DeadLetter;

/// <summary>
/// Durable buffer in front of the Dataverse error table.
///
/// Why a queue instead of writing directly:
///   The previous design wrote failed records to Dataverse synchronously and fell
///   back to App Insights logs on Dataverse failure. AI logs are queryable but not
///   easily replayable, so a transient Dataverse outage during dead-letter write
///   silently dropped recovery context. Buffering each failure on a Storage Queue
///   makes the path retryable end-to-end: the QueueTrigger function reads, attempts
///   the insert, and on any exception lets the runtime requeue. After 5 delivery
///   attempts the message moves to the poison queue (handled separately) so we
///   never lose a record and never spin forever.
///
/// One message per failed record (per design decision):
///   Keeps each retry atomic — a transient Dataverse failure on row 47 of 100 only
///   replays row 47, not all 100. Storage Queue is cheap enough that even 1000
///   messages/day is well under a cent.
/// </summary>
public class QueueDeadLetterService : IDeadLetterService
{
    private readonly QueueClient _queue;
    private readonly ILogger<QueueDeadLetterService> _logger;
    private int _queueEnsured;

    public QueueDeadLetterService(QueueClient queue, ILogger<QueueDeadLetterService> logger)
    {
        _queue = queue;
        _logger = logger;
    }

    public async Task WriteAsync(
        string moduleName,
        int mthKey,
        string invocationId,
        List<FailedRecord> failures,
        CancellationToken ct = default)
    {
        if (failures.Count == 0) return;

        // CreateIfNotExistsAsync once per process — the storage operation is idempotent
        // but we still skip it after the first call to avoid the round-trip.
        if (Interlocked.Exchange(ref _queueEnsured, 1) == 0)
            await _queue.CreateIfNotExistsAsync(cancellationToken: ct);

        var now = DateTimeOffset.UtcNow;
        int enqueued = 0, errored = 0;

        foreach (var f in failures)
        {
            var msg = new DeadLetterMessage
            {
                ModuleName = moduleName,
                MthKey = mthKey,
                InvocationId = invocationId,
                Key = f.Key,
                AccountNumber = f.AccountNumber,
                LoanIdentifier = f.LoanIdentifier,
                ErrorMessage = f.ErrorMessage,
                EnqueuedAt = now,
            };

            var json = JsonSerializer.Serialize(msg);

            try
            {
                await _queue.SendMessageAsync(json, ct);
                enqueued++;

                _logger.LogInformation(
                    "EventName={EventName} Module={Module} MthKey={MthKey} Account={Account} LoanId={LoanId} InvocationId={InvocationId}",
                    LogEvents.DeadLetterEnqueued, moduleName, mthKey,
                    msg.AccountNumber ?? "",
                    msg.LoanIdentifier?.ToString() ?? "",
                    invocationId);
            }
            catch (Exception ex)
            {
                errored++;

                // Queue enqueue failed — last-ditch AI log so the record is at least
                // recorded somewhere. The orchestrator will not retry; this is the
                // boundary at which durability gives up.
                _logger.LogError(ex,
                    "EventName={EventName} Module={Module} Account={Account} LoanId={LoanId} Key={Key} Error={Error}",
                    LogEvents.DeadLetterEnqueueFailed, moduleName,
                    msg.AccountNumber ?? "",
                    msg.LoanIdentifier?.ToString() ?? "",
                    msg.Key,
                    msg.ErrorMessage);
            }
        }

        _logger.LogInformation(
            "EventName={EventName} Module={Module} Total={Total} Enqueued={Enqueued} Errored={Errored}",
            LogEvents.DeadLetterBatchEnqueued, moduleName, failures.Count, enqueued, errored);
    }
}
