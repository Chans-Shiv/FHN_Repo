using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using SqlToDataverseSync.Configuration;
using SqlToDataverseSync.Domain.Entities;
using SqlToDataverseSync.Domain.Interfaces;
using SqlToDataverseSync.Domain.Models;

namespace SqlToDataverseSync.Application.Processors;

/// <summary>
/// Base class for all module processors.
/// 
/// Provides:
///   - Pre-warm: query module table upfront → build lookup dictionary
///   - ProcessBatch: use lookup to find matches → build updates → batch send
///   - Type conversion helpers
///   
/// Each module extends this and overrides:
///   - EntityLogicalName, MatchColumn, ModuleName, Order
///   - GetColumnsToRetrieve()     → which columns to pre-warm
///   - ShouldUpdate()             → filter condition (e.g., LoanIdentifier is null)
///   - BuildUpdateEntity()        → what columns to set on the matched record
///   - GetMatchKey()              → extract the key from a ConsumerCreditRecord
/// </summary>
public abstract class BaseModuleProcessor : IModuleProcessor
{
    protected readonly IDataverseRepository Repository;
    protected readonly SyncSettings Settings;
    protected readonly ILogger Logger;

    // Pre-warmed lookup: MatchKey → Dataverse Entity (with RecordId + columns)
    protected Dictionary<string, Entity> LookupDict = new(StringComparer.OrdinalIgnoreCase);

    public abstract string ModuleName { get; }
    public abstract int Order { get; }
    protected abstract string EntityLogicalName { get; }
    protected abstract string MatchColumn { get; }

    protected BaseModuleProcessor(
        IDataverseRepository repository,
        SyncSettings settings,
        ILogger logger)
    {
        Repository = repository;
        Settings = settings;
        Logger = logger;
    }

    // ═══════════════════════════════════════════════════
    // Methods to override per module
    // ═══════════════════════════════════════════════════

    /// <summary>Columns to fetch from the module table during pre-warm.</summary>
    protected abstract string[] GetColumnsToRetrieve();

    /// <summary>Extracts the matching key from a transformed record (e.g., AcctNum).</summary>
    protected abstract string GetMatchKey(ConsumerCreditRecord record);

    /// <summary>Checks if the existing Dataverse record should be updated.</summary>
    protected abstract bool ShouldUpdate(Entity existingEntity, ConsumerCreditRecord record);

    /// <summary>Builds the update entity with the new column values.</summary>
    protected abstract Entity BuildUpdateEntity(Entity existingEntity, ConsumerCreditRecord record);

    // ═══════════════════════════════════════════════════
    // Pre-warm: query module table once upfront
    // ═══════════════════════════════════════════════════

    public virtual async Task<ModuleResult> PreWarmAsync(
        List<ConsumerCreditRecord> allRecords,
        CancellationToken ct = default)
    {
        var result = new ModuleResult { ModuleName = ModuleName };

        // Extract all unique match keys from the in-memory data
        var matchKeys = allRecords
            .Select(GetMatchKey)
            .Where(k => !string.IsNullOrEmpty(k))
            .Distinct()
            .ToList();

        Logger.LogInformation("[{Module}] Pre-warming: querying {Count} unique keys against {Table}",
            ModuleName, matchKeys.Count, EntityLogicalName);

        // Query module table for matching records — server-side filter
        LookupDict = await Repository.QueryByKeysAsync(
            EntityLogicalName,
            MatchColumn,
            matchKeys,
            GetColumnsToRetrieve(),
            ct: ct);

        Logger.LogInformation("[{Module}] Pre-warm complete: {Matches}/{Total} keys found in module table",
            ModuleName, LookupDict.Count, matchKeys.Count);

        return result;
    }

    // ═══════════════════════════════════════════════════
    // Process batch: use lookup to match, filter, build updates
    // ═══════════════════════════════════════════════════

    public virtual async Task<ModuleResult> ProcessBatchAsync(
        List<ConsumerCreditRecord> batch,
        CancellationToken ct = default)
    {
        var result = new ModuleResult { ModuleName = ModuleName };
        var updateEntities = new List<Entity>();

        foreach (var record in batch)
        {
            var matchKey = GetMatchKey(record);

            if (string.IsNullOrEmpty(matchKey))
            {
                result.RowsSkipped++;
                continue;
            }

            if (!LookupDict.TryGetValue(matchKey, out var existingEntity))
            {
                // No match in module table — skip
                result.RowsSkipped++;
                continue;
            }

            if (!ShouldUpdate(existingEntity, record))
            {
                // Match found but update condition not met — skip
                result.RowsSkipped++;
                continue;
            }

            // Build the update entity
            var updateEntity = BuildUpdateEntity(existingEntity, record);
            updateEntities.Add(updateEntity);
            result.RowsProcessed++;
        }

        // Batch update all matched entities
        if (updateEntities.Count > 0)
        {
            var (updated, failed, failures) = await Repository.BatchUpdateAsync(updateEntities, ct);
            result.RowsUpdated += updated;
            result.RowsFailed += failed;
            result.Failures.AddRange(failures);
        }

        Logger.LogInformation(
            "[{Module}] Batch: {Updated} updated, {Failed} failed, {Skipped} skipped",
            ModuleName, result.RowsUpdated, result.RowsFailed, result.RowsSkipped);

        return result;
    }

    public virtual Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;

    // ═══════════════════════════════════════════════════
    // Type conversion helpers
    // ═══════════════════════════════════════════════════

    protected static string SafeToString(object? value)
        => value?.ToString()?.Trim() ?? string.Empty;

    protected static bool IsNullOrEmpty(Entity entity, string attributeName)
    {
        if (!entity.Contains(attributeName)) return true;
        var val = entity[attributeName];
        return val is null || (val is string s && string.IsNullOrEmpty(s)) || (val is int i && i == 0);
    }
}
