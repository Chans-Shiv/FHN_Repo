using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Entities;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Application.Processors;

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
///   - GetPreWarmFilter()         → optional server-side filter (e.g., LoanIdentifier IS NULL)
///   - ShouldUpdate()             → client-side guard (kept for safety)
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

    /// <summary>
    /// Optional server-side filter applied during pre-warm in addition to the IN(key)
    /// condition. Lets each module narrow the pre-warm result set so we don't bring
    /// back rows that ShouldUpdate would later reject. Default null means no filter.
    /// </summary>
    protected virtual FilterExpression? GetPreWarmFilter() => null;

    // ═══════════════════════════════════════════════════
    // Pre-warm: query module table once upfront
    // ───────────────────────────────────────────────────
    // Takes a pre-built set of match keys (stripped account numbers) from the orchestrator
    // rather than the full records list — keeps memory low under the streaming pipeline.
    // ═══════════════════════════════════════════════════

    public virtual async Task<ModuleResult> PreWarmAsync(
        IReadOnlyCollection<string> matchKeys,
        CancellationToken ct = default)
    {
        var result = new ModuleResult { ModuleName = ModuleName };

        var keys = matchKeys.Where(k => !string.IsNullOrEmpty(k)).ToList();

        Logger.LogInformation(
            "EventName={EventName} Module={Module} Table={Table} Keys={Count} HasFilter={HasFilter}",
            "PreWarmStarted", ModuleName, EntityLogicalName, keys.Count, GetPreWarmFilter() != null);

        LookupDict = await Repository.QueryByKeysAsync(
            EntityLogicalName,
            MatchColumn,
            keys,
            GetColumnsToRetrieve(),
            GetPreWarmFilter(),
            ct);

        Logger.LogInformation(
            "EventName={EventName} Module={Module} Table={Table} Matched={Matched} OfKeys={Keys}",
            "PreWarmComplete", ModuleName, EntityLogicalName, LookupDict.Count, keys.Count);

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
                // With a server-side GetPreWarmFilter, this branch should rarely fire.
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
