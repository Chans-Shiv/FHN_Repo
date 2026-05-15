using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Polly;
using Polly.Retry;
using SqlToDataverseSync.Configuration;
using SqlToDataverseSync.Domain.Interfaces;
using SqlToDataverseSync.Domain.Models;

namespace SqlToDataverseSync.Infrastructure.Dataverse;

/// <summary>
/// Dataverse CRUD operations with:
///   - 3-stage retry pipeline (batch → retry failures → dead-letter)
///   - SemaphoreSlim for parallel batch control
///   - Polly for transient error retry (429, 503)
///   - Server-side key-based queries for pre-warm lookups
/// </summary>
public class DataverseRepository : IDataverseRepository
{
    private readonly DataverseConnectionFactory _factory;
    private readonly SyncSettings _settings;
    private readonly ILogger<DataverseRepository> _logger;
    private readonly ResiliencePipeline _retryPipeline;

    public DataverseRepository(
        DataverseConnectionFactory factory,
        SyncSettings settings,
        ILogger<DataverseRepository> logger)
    {
        _factory = factory;
        _settings = settings;
        _logger = logger;

        _retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 5,
                BackoffType = DelayBackoffType.Exponential,
                Delay = TimeSpan.FromSeconds(5),
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex =>
                    ex.Message.Contains("429") || ex.Message.Contains("Too Many Requests") ||
                    ex.Message.Contains("503") || ex.Message.Contains("Server Busy"))
            })
            .Build();
    }

    public async Task<bool> ConnectAsync(CancellationToken ct = default)
    {
        try
        {
            var client = await _factory.GetClientAsync(ct);
            var response = (WhoAmIResponse)await client.ExecuteAsync(new WhoAmIRequest(), ct);
            _logger.LogInformation("Dataverse connected. Org={OrgId}, User={UserId}",
                response.OrganizationId, response.UserId);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Dataverse connection test FAILED");
            return false;
        }
    }

    // ═══════════════════════════════════════════════════
    // BATCH INSERT (for staging table)
    // ═══════════════════════════════════════════════════

    public async Task<(int Succeeded, int Failed, List<FailedRecord> Failures)> BatchInsertAsync(
        List<Entity> entities, CancellationToken ct = default)
    {
        return await ExecuteBatchOperationAsync(entities, isInsert: true, ct);
    }

    // ═══════════════════════════════════════════════════
    // BATCH UPDATE (for module tables)
    // ═══════════════════════════════════════════════════

    public async Task<(int Succeeded, int Failed, List<FailedRecord> Failures)> BatchUpdateAsync(
        List<Entity> entities, CancellationToken ct = default)
    {
        return await ExecuteBatchOperationAsync(entities, isInsert: false, ct);
    }

    // ═══════════════════════════════════════════════════
    // DELETE by column value (for staging truncation)
    // ═══════════════════════════════════════════════════

    public async Task<int> DeleteByColumnValueAsync(
        string entityName, string columnName, object value, CancellationToken ct = default)
    {
        var client = await _factory.GetClientAsync(ct);
        int totalDeleted = 0;

        // Retrieve all record IDs matching the filter, page by page
        var query = new QueryExpression(entityName)
        {
            ColumnSet = new ColumnSet(false), // Only need IDs
            Criteria = new FilterExpression()
        };
        query.Criteria.AddCondition(columnName, ConditionOperator.Equal, value);
        query.PageInfo = new PagingInfo { Count = 5000, PageNumber = 1 };

        var allIds = new List<Guid>();
        EntityCollection response;
        do
        {
            response = await client.RetrieveMultipleAsync(query, ct);
            allIds.AddRange(response.Entities.Select(e => e.Id));
            query.PageInfo.PageNumber++;
            query.PageInfo.PagingCookie = response.PagingCookie;
        } while (response.MoreRecords);

        if (allIds.Count == 0) return 0;

        _logger.LogInformation("Deleting {Count} records from {Table} where {Column}={Value}",
            allIds.Count, entityName, columnName, value);

        // Delete in batches using ExecuteMultipleRequest
        var chunks = allIds
            .Select((id, i) => new { id, i })
            .GroupBy(x => x.i / _settings.DataverseBatchSize)
            .Select(g => g.Select(x => x.id).ToList())
            .ToList();

        using var semaphore = new SemaphoreSlim(_settings.MaxParallelBatches);
        var tasks = chunks.Select(async chunk =>
        {
            await semaphore.WaitAsync(ct);
            try
            {
                var request = new ExecuteMultipleRequest
                {
                    Settings = new ExecuteMultipleSettings { ContinueOnError = true, ReturnResponses = false },
                    Requests = new OrganizationRequestCollection()
                };

                foreach (var id in chunk)
                    request.Requests.Add(new DeleteRequest
                    {
                        Target = new EntityReference(entityName, id)
                    });

                await _retryPipeline.ExecuteAsync(async token =>
                {
                    await client.ExecuteAsync(request, token);
                }, ct);

                Interlocked.Add(ref totalDeleted, chunk.Count);
            }
            finally { semaphore.Release(); }
        });

        await Task.WhenAll(tasks);
        _logger.LogInformation("Deleted {Count} records from {Table}", totalDeleted, entityName);
        return totalDeleted;
    }

    // ═══════════════════════════════════════════════════
    // QUERY by keys (for pre-warm lookup dictionaries)
    // ═══════════════════════════════════════════════════

    public async Task<Dictionary<string, Entity>> QueryByKeysAsync(
        string entityName, string keyColumn, List<string> keyValues,
        string[] columnsToRetrieve, string? additionalFilter = null,
        CancellationToken ct = default)
    {
        var client = await _factory.GetClientAsync(ct);
        var result = new Dictionary<string, Entity>(StringComparer.OrdinalIgnoreCase);

        if (keyValues.Count == 0) return result;

        // Batch keys in groups of 500 to avoid query limits
        const int chunkSize = 500;
        for (int i = 0; i < keyValues.Count; i += chunkSize)
        {
            var chunk = keyValues.Skip(i).Take(chunkSize).ToList();

            var query = new QueryExpression(entityName)
            {
                ColumnSet = new ColumnSet(columnsToRetrieve),
                Criteria = new FilterExpression(LogicalOperator.And)
            };

            // Single IN condition — cheaper than 500 OR-Equal conditions.
            query.Criteria.AddCondition(
                keyColumn, ConditionOperator.In, chunk.Cast<object>().ToArray());

            query.PageInfo = new PagingInfo { Count = 5000, PageNumber = 1 };

            EntityCollection response;
            do
            {
                response = await client.RetrieveMultipleAsync(query, ct);

                foreach (var entity in response.Entities)
                {
                    // Type-safe extraction: GetAttributeValue<string> throws if column isn't a string.
                    var key = entity.Contains(keyColumn)
                        ? entity[keyColumn]?.ToString()
                        : null;

                    if (!string.IsNullOrEmpty(key) && !result.ContainsKey(key))
                        result[key] = entity;
                }

                query.PageInfo.PageNumber++;
                query.PageInfo.PagingCookie = response.PagingCookie;
            } while (response.MoreRecords);
        }

        return result;
    }

    // ═══════════════════════════════════════════════════
    // 3-STAGE RETRY PIPELINE
    // ═══════════════════════════════════════════════════

    private async Task<(int Succeeded, int Failed, List<FailedRecord> Failures)>
        ExecuteBatchOperationAsync(List<Entity> entities, bool isInsert, CancellationToken ct)
    {
        if (entities.Count == 0) return (0, 0, new List<FailedRecord>());

        int totalSucceeded = 0;
        int totalFailed = 0;
        var allFailures = new List<FailedRecord>();

        var chunks = entities
            .Select((e, i) => new { Entity = e, Index = i })
            .GroupBy(x => x.Index / _settings.DataverseBatchSize)
            .Select(g => g.Select(x => x.Entity).ToList())
            .ToList();

        using var semaphore = new SemaphoreSlim(_settings.MaxParallelBatches);

        var tasks = chunks.Select(async (chunk, chunkIdx) =>
        {
            await semaphore.WaitAsync(ct);
            try
            {
                var (s, f, failures) = await ExecuteChunkWithRetryAsync(chunk, chunkIdx, isInsert, ct);
                Interlocked.Add(ref totalSucceeded, s);
                Interlocked.Add(ref totalFailed, f);
                lock (allFailures) { allFailures.AddRange(failures); }
            }
            finally { semaphore.Release(); }
        });

        await Task.WhenAll(tasks);
        return (totalSucceeded, totalFailed, allFailures);
    }

    private async Task<(int Succeeded, int Failed, List<FailedRecord> Failures)>
        ExecuteChunkWithRetryAsync(List<Entity> chunk, int chunkIdx, bool isInsert, CancellationToken ct)
    {
        var client = await _factory.GetClientAsync(ct);

        // ── STAGE 1: Send full batch ──
        var (stage1Succeeded, stage1Failed) = await SendBatchAsync(client, chunk, isInsert, ct);

        if (stage1Failed.Count == 0)
            return (stage1Succeeded, 0, new List<FailedRecord>());

        _logger.LogWarning("Stage 1: {Success}/{Total}, {Failed} failures — retrying",
            stage1Succeeded, chunk.Count, stage1Failed.Count);

        // ── STAGE 2: Retry only the failed entities ──
        await Task.Delay(TimeSpan.FromSeconds(5), ct);
        var (stage2Succeeded, stage2Failed) = await SendBatchAsync(client, stage1Failed.Select(f => f.Entity!).ToList(), isInsert, ct);

        var totalSucceeded = stage1Succeeded + stage2Succeeded;

        if (stage2Failed.Count == 0)
        {
            _logger.LogInformation("Stage 2: All {Count} retried records recovered", stage2Succeeded);
            return (totalSucceeded, 0, new List<FailedRecord>());
        }

        // ── STAGE 3: Dead-letter permanent failures ──
        _logger.LogWarning("Stage 3: {Count} permanently failed records", stage2Failed.Count);
        return (totalSucceeded, stage2Failed.Count, stage2Failed);
    }

    private async Task<(int Succeeded, List<FailedRecord> Failures)>
        SendBatchAsync(ServiceClient client, List<Entity> entities, bool isInsert, CancellationToken ct)
    {
        var failures = new List<FailedRecord>();
        var request = new ExecuteMultipleRequest
        {
            Settings = new ExecuteMultipleSettings { ContinueOnError = true, ReturnResponses = false },
            Requests = new OrganizationRequestCollection()
        };

        foreach (var entity in entities)
        {
            if (isInsert)
                request.Requests.Add(new CreateRequest { Target = entity });
            else
                request.Requests.Add(new UpdateRequest { Target = entity });
        }

        try
        {
            ExecuteMultipleResponse response = null!;
            await _retryPipeline.ExecuteAsync(async token =>
            {
                response = (ExecuteMultipleResponse)await client.ExecuteAsync(request, token);
            }, ct);

            if (response?.IsFaulted == true)
            {
                foreach (var item in response.Responses.Where(r => r.Fault != null))
                {
                    if (item.RequestIndex < entities.Count)
                    {
                        failures.Add(new FailedRecord
                        {
                            Key = ExtractBusinessKey(entities[item.RequestIndex]),
                            ErrorMessage = item.Fault.Message,
                            Entity = entities[item.RequestIndex]
                        });
                    }
                }
            }

            return (entities.Count - failures.Count, failures);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Entire batch failed ({Count} records)", entities.Count);
            return (0, entities.Select(e => new FailedRecord
            {
                Key = ExtractBusinessKey(e),
                ErrorMessage = ex.Message,
                Entity = e
            }).ToList());
        }
    }

    /// <summary>
    /// Extracts a stable business key for failure tracking + dead-letter telemetry.
    /// Updates carry a real GUID; inserts (staging) start with Guid.Empty, so we fall back to
    /// the first available business-key column on the entity.
    /// </summary>
    private static string ExtractBusinessKey(Entity entity)
    {
        if (entity.Id != Guid.Empty) return entity.Id.ToString();

        // Composite key for staging is monthkey:accountkey — uniquely identifies a row.
        var mthKey = entity.Contains("crbee_monthkey") ? entity["crbee_monthkey"]?.ToString() : null;
        var acctKey = entity.Contains("crbee_accountkey") ? entity["crbee_accountkey"]?.ToString() : null;
        if (!string.IsNullOrEmpty(mthKey) && !string.IsNullOrEmpty(acctKey))
            return $"{mthKey}:{acctKey}";

        foreach (var attr in new[] { "crbee_accountkey", "crbee_accountnumber", "crbee_accountid" })
        {
            if (entity.Contains(attr) && entity[attr] is not null)
            {
                var val = entity[attr]?.ToString();
                if (!string.IsNullOrEmpty(val)) return val;
            }
        }

        return Guid.Empty.ToString();
    }
}
