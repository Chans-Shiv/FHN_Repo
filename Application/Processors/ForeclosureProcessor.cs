using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using SqlToDataverseSync.Configuration;
using SqlToDataverseSync.Domain.Entities;
using SqlToDataverseSync.Domain.Interfaces;

namespace SqlToDataverseSync.Application.Processors;

/// <summary>
/// Foreclosure module processor.
/// 
/// Table:     ff_foreclosuremaintable1
/// Match:     ConsumerCreditRecord.AcctNum → ff_accountnumber
/// Condition: ff_accountnumber is not null AND ff_loanidentifier is null
/// Action:    UPDATE ff_loanidentifier = record.LoanIdentifier
/// 
/// To create a new module (e.g., Bankruptcy), copy this class and change:
///   1. EntityLogicalName → your table's logical name
///   2. MatchColumn → the column to match against
///   3. GetColumnsToRetrieve() → columns needed for ShouldUpdate check
///   4. ShouldUpdate() → your module's condition
///   5. BuildUpdateEntity() → your module's update columns
///   6. ModuleName, Order
/// </summary>
public class ForeclosureProcessor : BaseModuleProcessor
{
    // ── Configuration ─────────────────────────────────────────
    public override string ModuleName => "Foreclosure";
    public override int Order => 1;

    protected override string EntityLogicalName => "ff_foreclosuremaintable1";
    protected override string MatchColumn => "ff_accountnumber";

    // TODO: Verify this logical name matches your Foreclosure table's LoanIdentifier column
    private const string LoanIdentifierColumn = "ff_loanidentifier";

    public ForeclosureProcessor(
        IDataverseRepository repository,
        SyncSettings settings,
        ILogger<ForeclosureProcessor> logger)
        : base(repository, settings, logger) { }

    // ── Which columns to pre-warm from Foreclosure table ──────
    protected override string[] GetColumnsToRetrieve()
    {
        return new[]
        {
            "ff_foreclosuremaintable1id",   // Record ID (primary key GUID)
            "ff_accountnumber",              // Match column
            LoanIdentifierColumn,            // Checked for null condition
        };
    }

    // ── Extract match key from the in-memory record ───────────
    protected override string GetMatchKey(ConsumerCreditRecord record)
    {
        // AcctNum has already been stripped of leading zeros by CommonTransformation
        return record.AcctNum;
    }

    // ── Server-side pre-warm filter ───────────────────────────
    // Limits the pre-warm query to rows where ff_loanidentifier is null OR zero
    // (the two states we treat as "missing"). Pairs with ShouldUpdate below as a
    // belt-and-braces guard against eventual-consistency drift between the
    // pre-warm read and the batch write.
    protected override FilterExpression? GetPreWarmFilter()
    {
        var filter = new FilterExpression(LogicalOperator.Or);
        filter.AddCondition(LoanIdentifierColumn, ConditionOperator.Null);
        filter.AddCondition(LoanIdentifierColumn, ConditionOperator.Equal, 0);
        return filter;
    }

    // ── Should we update this Foreclosure record? ─────────────
    protected override bool ShouldUpdate(Entity existingEntity, ConsumerCreditRecord record)
    {
        // Condition: Account is not null (guaranteed by being in lookup dict)
        //            AND LoanIdentifier is null/empty/zero in Foreclosure.
        // The server-side GetPreWarmFilter already enforces this, so this check
        // is a safety net only and will rarely fail.
        return IsNullOrEmpty(existingEntity, LoanIdentifierColumn);
    }

    // ── Build the update entity ───────────────────────────────
    protected override Entity BuildUpdateEntity(Entity existingEntity, ConsumerCreditRecord record)
    {
        // Create an update entity with the existing record's ID
        var update = new Entity(EntityLogicalName, existingEntity.Id);

        // Set LoanIdentifier from the staging/in-memory record
        // Type conversion: staging LoanIdentifier is int, Foreclosure expects Whole number
        update[LoanIdentifierColumn] = record.LoanIdentifier;

        return update;
    }
}
