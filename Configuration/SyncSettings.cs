namespace Fhn.Cdm.DataverseSync.Configuration;

public class SyncSettings
{
    public string SqlConnectionString { get; set; } = string.Empty;
    public string DataverseUrl { get; set; } = string.Empty;

    /// <summary>Rows buffered from SQL before passing to processors. Default: 10,000.</summary>
    public int SqlBatchSize { get; set; } = 10_000;

    /// <summary>Max records per Dataverse ExecuteMultipleRequest. API hard limit: 1,000.</summary>
    public int DataverseBatchSize { get; set; } = 1_000;

    /// <summary>Concurrent Dataverse batch requests per processor. 5 procs × 5 slots = 25 peak.</summary>
    public int MaxParallelBatches { get; set; } = 5;

    /// <summary>
    /// Seconds between status polls of the BulkDelete async system job.
    /// Lower = faster reaction when the job finishes; higher = fewer API calls.
    /// </summary>
    public int BulkDeletePollIntervalSeconds { get; set; } = 15;

    /// <summary>
    /// Maximum time to wait for the BulkDelete async job before throwing TimeoutException.
    /// 30 min covers staging tables up to several million rows in practice.
    /// </summary>
    public int BulkDeleteTimeoutMinutes { get; set; } = 30;

    /// <summary>
    /// Concurrent QueryByKeysAsync chunk requests during module pre-warm.
    /// Pre-warm chunks the key set (1000 keys per IN-clause) and queries each chunk;
    /// running them sequentially was the main source of "stuck at Phase 3" stalls.
    /// </summary>
    public int PreWarmParallelism { get; set; } = 10;

    /// <summary>Maximum consecutive days of same-hash failures before dead-lettering.</summary>
    public int MaxConsecutiveFailureDays { get; set; } = 3;

    /// <summary>HTTPS endpoint of the storage account hosting tracking.json (e.g., https://acct.blob.core.windows.net).</summary>
    public string TrackingStorageAccountUrl { get; set; } = string.Empty;

    /// <summary>Container holding the tracking blob. Created on first save if it doesn't exist.</summary>
    public string TrackingContainerName { get; set; } = "sync-state";

    /// <summary>Blob name for the serialized TrackingState.</summary>
    public string TrackingBlobName { get; set; } = "tracking.json";

    /// <summary>
    /// Logical name of the Dataverse error table that receives one row per failed
    /// staging/module operation. If the insert into this table fails, the writer
    /// falls back to App Insights structured logging.
    /// </summary>
    public string ErrorTableEntityName { get; set; } = "crbee_consumercredit_errortable";
}
