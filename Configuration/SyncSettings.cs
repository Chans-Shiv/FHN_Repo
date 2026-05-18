namespace SqlToDataverseSync.Configuration;

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
    /// Concurrent Dataverse batch requests used only by the staging truncate (DeleteAllAsync).
    /// Truncate runs alone, so we can push higher than MaxParallelBatches without exceeding
    /// the Dataverse concurrency limit (~52 / server). Polly handles transient 429s.
    /// </summary>
    public int DeleteParallelism { get; set; } = 15;

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
}
