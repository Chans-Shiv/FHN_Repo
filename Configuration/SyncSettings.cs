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

    /// <summary>Maximum consecutive days of same-hash failures before dead-lettering.</summary>
    public int MaxConsecutiveFailureDays { get; set; } = 3;
}
