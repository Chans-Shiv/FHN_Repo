using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Fhn.Cdm.DataverseSync.Domain.Entities;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Domain.Interfaces;

/// <summary>Streams rows from SQL MI in batches. Never loads full result set.</summary>
public interface ISqlDataReader
{
    Task<long> GetRowCountForMthKeyAsync(int mthKey, CancellationToken ct = default);

    /// <summary>
    /// Yields raw ACCT_NUM values one at a time for the given MTH_KEY. Used by the
    /// streaming pipeline to build the pre-warm match-key set without loading full rows.
    /// </summary>
    IAsyncEnumerable<string> StreamAccountNumbersAsync(int mthKey, CancellationToken ct = default);

    IAsyncEnumerable<List<Dictionary<string, object?>>> StreamBatchesAsync(
        int mthKey, int batchSize, CancellationToken ct = default);
}

/// <summary>CRUD operations against Dataverse with batch support and retry.</summary>
public interface IDataverseRepository
{
    Task<bool> ConnectAsync(CancellationToken ct = default);
    Task<(int Succeeded, int Failed, List<FailedRecord> Failures)> BatchUpdateAsync(
        List<Entity> entities, CancellationToken ct = default);
    Task<Dictionary<string, Entity>> QueryByKeysAsync(
        string entityName, string keyColumn, List<string> keyValues,
        string[] columnsToRetrieve, FilterExpression? additionalFilter = null,
        CancellationToken ct = default);
}

/// <summary>Processes a batch of transformed records for a specific module.</summary>
public interface IModuleProcessor
{
    string ModuleName { get; }
    int Order { get; }
    Task<ModuleResult> PreWarmAsync(IReadOnlyCollection<string> matchKeys, CancellationToken ct = default);
    Task<ModuleResult> ProcessBatchAsync(List<ConsumerCreditRecord> batch, CancellationToken ct = default);
    Task FlushAsync(CancellationToken ct = default);
}

/// <summary>Tracks sync state across daily runs (row counts, failure hashes).</summary>
public interface ITrackingService
{
    Task<TrackingState> LoadAsync(CancellationToken ct = default);
    Task SaveAsync(TrackingState state, CancellationToken ct = default);
}

/// <summary>Persists permanently failed records for manual review.</summary>
public interface IDeadLetterService
{
    /// <param name="moduleName">Module/Process that produced the failures (e.g. "Staging", "Foreclosure").</param>
    /// <param name="mthKey">YYYYMM identifier of the data partition being processed.</param>
    /// <param name="invocationId">Function invocation correlation id (Activity.Current.RootId).</param>
    /// <param name="failures">Failed records, each pre-enriched with AccountNumber + LoanIdentifier by the caller.</param>
    Task WriteAsync(
        string moduleName,
        int mthKey,
        string invocationId,
        List<FailedRecord> failures,
        CancellationToken ct = default);
}
