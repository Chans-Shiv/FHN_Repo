namespace Fhn.Cdm.DataverseSync.Configuration;

/// <summary>
/// Configuration model populated from environment variables (local.settings.json
/// locally, App Settings in Azure). Resource-pointing values (connection strings,
/// URLs, container/queue/table names) are marked <c>required</c> — Program.cs
/// throws on missing values so a forgotten setting fails loud at startup rather
/// than silently using a stale default that points at the wrong resource.
/// Tuning knobs (batch sizes, parallelism, failure thresholds) keep sensible
/// defaults because the documented values fit most deployments.
/// </summary>
public class SyncSettings
{
    public required string DataverseUrl { get; set; }

    /// <summary>Rows buffered from the Excel workbook before passing to processors. Default: 10,000.</summary>
    public int ExcelBatchSize { get; set; } = 10_000;

    /// <summary>Max records per Dataverse ExecuteMultipleRequest. API hard limit: 1,000.</summary>
    public int DataverseBatchSize { get; set; } = 1_000;

    /// <summary>Concurrent Dataverse batch requests per processor. 5 procs × 5 slots = 25 peak.</summary>
    public int MaxParallelBatches { get; set; } = 5;

    /// <summary>
    /// Concurrent QueryByKeysAsync chunk requests during module pre-warm.
    /// Pre-warm chunks the key set (1000 keys per IN-clause) and queries each chunk;
    /// running them sequentially was the main source of "stuck at Phase 3" stalls.
    /// </summary>
    public int PreWarmParallelism { get; set; } = 10;

    /// <summary>
    /// Blob endpoint of the DATA storage account ("SyncStorage", e.g.
    /// https://acct.blob.core.windows.net). Single source of truth for every
    /// identity-based blob client: the tracking blob and the failure blob archive.
    /// Sourced from the <c>SyncStorage__blobServiceUri</c> setting — the same named
    /// connection the dead-letter QueueTrigger binds to — so producer and consumer
    /// can never point at different accounts.
    /// </summary>
    public required string SyncStorageBlobServiceUri { get; set; }

    /// <summary>
    /// Queue endpoint of the DATA storage account ("SyncStorage", e.g.
    /// https://acct.queue.core.windows.net). Used by the dead-letter queue producer;
    /// the QueueTrigger consumer binds to the same <c>SyncStorage</c> connection.
    /// </summary>
    public required string SyncStorageQueueServiceUri { get; set; }

    /// <summary>
    /// Logical name of the Dataverse error table that receives one row per failed
    /// staging/module operation. If the insert into this table fails, the writer
    /// falls back to App Insights structured logging.
    /// </summary>
    public required string ErrorTableEntityName { get; set; }

    /// <summary>
    /// Storage Queue name buffering failed records before they're inserted into the
    /// Dataverse error table. One message = one failed record. After
    /// <c>maxDequeueCount</c> attempts (see host.json), the queue trigger writes the
    /// record to <see cref="FailureBlobContainerName"/> + App Insights and ACKs the
    /// message — so nothing should ever land in <c>{name}-poison</c>.
    /// </summary>
    public required string DeadLetterQueueName { get; set; }

    /// <summary>
    /// Number of delivery attempts before the dead-letter processor gives up and
    /// archives the record to <see cref="FailureBlobContainerName"/>. Populated at
    /// startup from <c>extensions.queues.maxDequeueCount</c> in host.json (default 5
    /// when host.json doesn't specify it — matching the Functions runtime's own
    /// default), so the constant can't drift from what the runtime actually uses.
    /// </summary>
    public int DeadLetterMaxAttempts { get; set; } = 5;

    /// <summary>
    /// Blob container holding the date-partitioned archive of records that exhausted
    /// all queue retries. Layout: <c>errors/yyyy/MM/dd/{invocationId}-{key}.json</c>.
    /// Records here are the durable "we gave up" artifact; App Insights gets the same
    /// payload logged as <c>EventName=DeadLetterGivenUp</c>.
    /// </summary>
    public required string FailureBlobContainerName { get; set; }
}
