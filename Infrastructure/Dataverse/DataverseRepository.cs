using System.Collections.Concurrent;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Polly;
using Polly.Retry;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Infrastructure.Dataverse;

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
    // DELETE ALL (full staging truncate, no filter)
    // ═══════════════════════════════════════════════════
    //
    // Uses BulkDeleteRequest — an async system job. The server enumerates the
    // target rows and deletes them on background workers, so the work does NOT
    // accumulate against Dataverse's 1,200,000 ms / 5-min combined-execution-time
    // service-protection limit. Replaced an ExecuteMultiple-of-DeleteRequest path
    // that tripped that limit on ~142K rows.

    /// <summary>
    /// Returns true if the entity has zero rows. One cheap RetrieveMultiple with TopCount=1
    /// and no columns — used to skip the truncate path when there's nothing to delete.
    /// </summary>
    public async Task<bool> IsTableEmptyAsync(string entityName, CancellationToken ct = default)
    {
        var client = await _factory.GetClientAsync(ct);
        var query = new QueryExpression(entityName)
        {
            ColumnSet = new ColumnSet(false),
            TopCount = 1
        };

        var response = await client.RetrieveMultipleAsync(query, ct);
        var empty = response.Entities.Count == 0;

        _logger.LogInformation(
            "EventName={EventName} Table={Table} IsEmpty={IsEmpty}",
            "IsTableEmptyCheck", entityName, empty);

        return empty;
    }

    public async Task<int> DeleteAllAsync(string entityName, CancellationToken ct = default)
    {
        var client = await _factory.GetClientAsync(ct);
        var overallSw = System.Diagnostics.Stopwatch.StartNew();

        _logger.LogInformation(
            "EventName={EventName} Table={Table} PollIntervalSec={Interval} TimeoutMin={Timeout}",
            "TruncateStarted", entityName,
            _settings.BulkDeletePollIntervalSeconds, _settings.BulkDeleteTimeoutMinutes);

        var bulkDeleteRequest = new BulkDeleteRequest
        {
            JobName = $"Truncate-{entityName}-{DateTime.UtcNow:yyyyMMddHHmmss}",
            QuerySet = new[]
            {
                new QueryExpression(entityName) { ColumnSet = new ColumnSet(false) }
            },
            StartDateTime = DateTime.UtcNow,
            SendEmailNotification = false,
            ToRecipients = Array.Empty<Guid>(),
            CCRecipients = Array.Empty<Guid>(),
            RecurrencePattern = string.Empty
        };

        BulkDeleteResponse submitResponse = null!;
        await _retryPipeline.ExecuteAsync(async token =>
        {
            submitResponse = (BulkDeleteResponse)await client.ExecuteAsync(bulkDeleteRequest, token);
        }, ct);

        var jobId = submitResponse.JobId;
        _logger.LogInformation(
            "EventName={EventName} Table={Table} JobId={JobId}",
            "TruncateJobSubmitted", entityName, jobId);

        var deletedCount = await WaitForBulkDeleteAsync(client, entityName, jobId, ct);

        overallSw.Stop();
        _logger.LogInformation(
            "EventName={EventName} Table={Table} Deleted={Deleted} ElapsedSec={Elapsed:F1} Rate={Rate:F0}/s",
            "TruncateComplete", entityName, deletedCount,
            overallSw.Elapsed.TotalSeconds,
            deletedCount / Math.Max(overallSw.Elapsed.TotalSeconds, 0.001));

        return deletedCount;
    }

    private async Task<int> WaitForBulkDeleteAsync(
        ServiceClient client, string entityName, Guid jobId, CancellationToken ct)
    {
        var pollInterval = TimeSpan.FromSeconds(_settings.BulkDeletePollIntervalSeconds);
        var timeout = TimeSpan.FromMinutes(_settings.BulkDeleteTimeoutMinutes);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        int lastLoggedStatus = -1;

        while (true)
        {
            if (sw.Elapsed >= timeout)
                throw new TimeoutException(
                    $"BulkDelete job {jobId} for {entityName} did not complete within {timeout.TotalMinutes:F0} min");

            await Task.Delay(pollInterval, ct);

            Entity job = null!;
            await _retryPipeline.ExecuteAsync(async token =>
            {
                job = await client.RetrieveAsync(
                    "asyncoperation", jobId,
                    new ColumnSet("statecode", "statuscode", "friendlymessage", "message"), token);
            }, ct);

            var stateCode  = ((OptionSetValue)job["statecode"]).Value;
            var statusCode = ((OptionSetValue)job["statuscode"]).Value;

            if (statusCode != lastLoggedStatus)
            {
                _logger.LogInformation(
                    "EventName={EventName} Table={Table} JobId={JobId} State={State} Status={Status} ElapsedSec={Elapsed:F1}",
                    "TruncateJobPoll", entityName, jobId, stateCode, statusCode, sw.Elapsed.TotalSeconds);
                lastLoggedStatus = statusCode;
            }

            // statecode 3 = Completed (terminal). Anything else means still running.
            if (stateCode != 3) continue;

            // statuscode 30 = Succeeded. Any other terminal status is a failure.
            if (statusCode == 30)
                return await GetBulkDeleteSuccessCountAsync(client, jobId, ct);

            var message = job.Contains("message") ? job["message"]?.ToString() : null;
            var friendly = job.Contains("friendlymessage") ? job["friendlymessage"]?.ToString() : null;
            throw new InvalidOperationException(
                $"BulkDelete job {jobId} for {entityName} terminated with Status={statusCode}. " +
                $"FriendlyMessage='{friendly}' Message='{message}'");
        }
    }

    private async Task<int> GetBulkDeleteSuccessCountAsync(
        ServiceClient client, Guid jobId, CancellationToken ct)
    {
        var query = new QueryExpression("bulkdeleteoperation")
        {
            ColumnSet = new ColumnSet("successcount", "failurecount")
        };
        query.Criteria.AddCondition("asyncoperationid", ConditionOperator.Equal, jobId);

        var result = await client.RetrieveMultipleAsync(query, ct);
        if (result.Entities.Count == 0) return 0;

        var op = result.Entities[0];
        var success = op.Contains("successcount") ? (int)op["successcount"] : 0;
        var failure = op.Contains("failurecount") ? (int)op["failurecount"] : 0;

        if (failure > 0)
        {
            _logger.LogWarning(
                "EventName={EventName} JobId={JobId} Success={Success} Failure={Failure}",
                "TruncateJobPartialFailure", jobId, success, failure);
        }

        return success;
    }

    // ═══════════════════════════════════════════════════
    // QUERY by keys (for pre-warm lookup dictionaries)
    // ═══════════════════════════════════════════════════

    public async Task<Dictionary<string, Entity>> QueryByKeysAsync(
        string entityName, string keyColumn, List<string> keyValues,
        string[] columnsToRetrieve, FilterExpression? additionalFilter = null,
        CancellationToken ct = default)
    {
        var result = new ConcurrentDictionary<string, Entity>(StringComparer.OrdinalIgnoreCase);
        if (keyValues.Count == 0) return new Dictionary<string, Entity>(result, StringComparer.OrdinalIgnoreCase);

        var client = await _factory.GetClientAsync(ct);

        // Chunk size 1000 — Dataverse In operator accepts up to 1000 values per condition.
        // 142K keys ÷ 1000 = 142 chunks; with PreWarmParallelism=10 that's ~14 sequential
        // waves instead of 142 — typically <30 s wall-clock instead of ~5 min.
        const int chunkSize = 1000;
        var chunks = new List<List<string>>();
        for (int i = 0; i < keyValues.Count; i += chunkSize)
            chunks.Add(keyValues.Skip(i).Take(chunkSize).ToList());

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _logger.LogInformation(
            "EventName={EventName} Table={Table} TotalKeys={Keys} Chunks={Chunks} Parallelism={Parallelism} HasFilter={HasFilter}",
            "PreWarmQueryStarted", entityName, keyValues.Count, chunks.Count,
            _settings.PreWarmParallelism, additionalFilter != null);

        int chunksCompleted = 0;
        using var semaphore = new SemaphoreSlim(_settings.PreWarmParallelism);

        var tasks = chunks.Select(async (chunk, idx) =>
        {
            await semaphore.WaitAsync(ct);
            var chunkSw = System.Diagnostics.Stopwatch.StartNew();
            int chunkMatches = 0;
            try
            {
                var query = new QueryExpression(entityName)
                {
                    ColumnSet = new ColumnSet(columnsToRetrieve),
                    Criteria = new FilterExpression(LogicalOperator.And),
                    PageInfo = new PagingInfo { Count = 5000, PageNumber = 1 }
                };

                // AND: keyColumn IN (chunk) AND (additional server-side filter, if any).
                query.Criteria.AddCondition(
                    keyColumn, ConditionOperator.In, chunk.Cast<object>().ToArray());
                if (additionalFilter != null)
                    query.Criteria.AddFilter(additionalFilter);

                EntityCollection response;
                do
                {
                    response = await client.RetrieveMultipleAsync(query, ct);
                    foreach (var entity in response.Entities)
                    {
                        var key = entity.Contains(keyColumn) ? entity[keyColumn]?.ToString() : null;
                        if (!string.IsNullOrEmpty(key) && result.TryAdd(key, entity))
                            chunkMatches++;
                    }
                    query.PageInfo.PageNumber++;
                    query.PageInfo.PagingCookie = response.PagingCookie;
                } while (response.MoreRecords);

                var newCompleted = Interlocked.Increment(ref chunksCompleted);
                _logger.LogInformation(
                    "EventName={EventName} Table={Table} Chunk={Chunk}/{Total} ChunkKeys={ChunkKeys} ChunkMatches={Matches} ChunkMs={ChunkMs} ElapsedSec={Elapsed:F1}",
                    "PreWarmChunk", entityName, newCompleted, chunks.Count,
                    chunk.Count, chunkMatches, chunkSw.ElapsedMilliseconds, sw.Elapsed.TotalSeconds);
            }
            finally { semaphore.Release(); }
        });

        await Task.WhenAll(tasks);

        sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} Table={Table} Matched={Matched} OfKeys={Keys} ElapsedSec={Elapsed:F1}",
            "PreWarmQueryComplete", entityName, result.Count, keyValues.Count, sw.Elapsed.TotalSeconds);

        return new Dictionary<string, Entity>(result, StringComparer.OrdinalIgnoreCase);
    }

    // ═══════════════════════════════════════════════════
    // 3-STAGE RETRY PIPELINE
    // ═══════════════════════════════════════════════════

    private async Task<(int Succeeded, int Failed, List<FailedRecord> Failures)>
        ExecuteBatchOperationAsync(List<Entity> entities, bool isInsert, CancellationToken ct)
    {
        if (entities.Count == 0) return (0, 0, new List<FailedRecord>());

        var op = isInsert ? "Insert" : "Update";
        var sw = System.Diagnostics.Stopwatch.StartNew();
        int totalSucceeded = 0;
        int totalFailed = 0;
        int batchesCompleted = 0;
        var allFailures = new List<FailedRecord>();

        var chunks = entities
            .Select((e, i) => new { Entity = e, Index = i })
            .GroupBy(x => x.Index / _settings.DataverseBatchSize)
            .Select(g => g.Select(x => x.Entity).ToList())
            .ToList();

        _logger.LogInformation(
            "EventName={EventName} Op={Op} Entities={Entities} Batches={Batches} Parallelism={Parallelism}",
            "BatchOpStarted", op, entities.Count, chunks.Count, _settings.MaxParallelBatches);

        using var semaphore = new SemaphoreSlim(_settings.MaxParallelBatches);

        var tasks = chunks.Select(async (chunk, chunkIdx) =>
        {
            await semaphore.WaitAsync(ct);
            var batchSw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                var (s, f, failures) = await ExecuteChunkWithRetryAsync(chunk, chunkIdx, isInsert, ct);
                Interlocked.Add(ref totalSucceeded, s);
                Interlocked.Add(ref totalFailed, f);
                lock (allFailures) { allFailures.AddRange(failures); }

                var newCompleted = Interlocked.Increment(ref batchesCompleted);
                _logger.LogInformation(
                    "EventName={EventName} Op={Op} Batch={Batch}/{Total} Succeeded={Succeeded} Failed={Failed} BatchMs={BatchMs} ElapsedSec={Elapsed:F1}",
                    "BatchOpBatch", op, newCompleted, chunks.Count, s, f,
                    batchSw.ElapsedMilliseconds, sw.Elapsed.TotalSeconds);
            }
            finally { semaphore.Release(); }
        });

        await Task.WhenAll(tasks);

        sw.Stop();
        _logger.LogInformation(
            "EventName={EventName} Op={Op} Succeeded={Succeeded} Failed={Failed} ElapsedSec={Elapsed:F1}",
            "BatchOpComplete", op, totalSucceeded, totalFailed, sw.Elapsed.TotalSeconds);

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
