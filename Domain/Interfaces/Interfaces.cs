using Microsoft.Xrm.Sdk;
using SqlToDataverseSync.Domain.Entities;
using SqlToDataverseSync.Domain.Models;

namespace SqlToDataverseSync.Domain.Interfaces;

/// <summary>Streams rows from SQL MI in batches. Never loads full result set.</summary>
public interface ISqlDataReader
{
    Task<long> GetRowCountForMthKeyAsync(int mthKey, CancellationToken ct = default);
    IAsyncEnumerable<List<Dictionary<string, object?>>> StreamBatchesAsync(
        int mthKey, int batchSize, CancellationToken ct = default);
}

/// <summary>CRUD operations against Dataverse with batch support and retry.</summary>
public interface IDataverseRepository
{
    Task<bool> ConnectAsync(CancellationToken ct = default);
    Task<(int Succeeded, int Failed, List<FailedRecord> Failures)> BatchInsertAsync(
        List<Entity> entities, CancellationToken ct = default);
    Task<(int Succeeded, int Failed, List<FailedRecord> Failures)> BatchUpdateAsync(
        List<Entity> entities, CancellationToken ct = default);
    Task<int> DeleteAllAsync(string entityName, CancellationToken ct = default);
    Task<Dictionary<string, Entity>> QueryByKeysAsync(
        string entityName, string keyColumn, List<string> keyValues,
        string[] columnsToRetrieve, string? additionalFilter = null,
        CancellationToken ct = default);
}

/// <summary>Processes a batch of transformed records for a specific module.</summary>
public interface IModuleProcessor
{
    string ModuleName { get; }
    int Order { get; }
    Task<ModuleResult> PreWarmAsync(List<ConsumerCreditRecord> allRecords, CancellationToken ct = default);
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
    Task WriteAsync(string moduleName, List<FailedRecord> failures, CancellationToken ct = default);
}
