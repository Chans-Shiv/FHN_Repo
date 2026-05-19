using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Entities;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;

namespace Fhn.Cdm.DataverseSync.Application.Processors;

/// <summary>
/// Bankruptcy module processor.
///
/// Table:     crbee_dmt_bankaccountcollateralrecordses
/// Match:     ConsumerCreditRecord.AcctNum → crbee_dmt_accountnumber
/// Condition: crbee_dmt_loanidentifier is null OR zero
/// Action:    UPDATE crbee_dmt_loanidentifier = record.LoanIdentifier
/// </summary>
public class BankruptcyProcessor : BaseModuleProcessor
{
    public override string ModuleName => "Bankruptcy";
    public override int Order => 2;

    protected override string EntityLogicalName => "crbee_dmt_bankaccountcollateralrecordses";
    protected override string MatchColumn => "crbee_dmt_accountnumber";

    private const string LoanIdentifierColumn = "crbee_dmt_loanidentifier";

    public BankruptcyProcessor(
        IDataverseRepository repository,
        SyncSettings settings,
        ILogger<BankruptcyProcessor> logger)
        : base(repository, settings, logger) { }

    protected override string[] GetColumnsToRetrieve()
    {
        // Entity.Id is auto-populated from the primary key during RetrieveMultiple,
        // so we only need the match column + the column we'll be conditionally updating.
        return new[]
        {
            MatchColumn,
            LoanIdentifierColumn,
        };
    }

    protected override string GetMatchKey(ConsumerCreditRecord record)
    {
        // AcctNum has already been stripped of leading zeros by CommonTransformation.
        return record.AcctNum;
    }

    protected override FilterExpression? GetPreWarmFilter()
    {
        // Pre-warm only fetches rows where LoanIdentifier is null/zero — the rest are no-ops.
        var filter = new FilterExpression(LogicalOperator.Or);
        filter.AddCondition(LoanIdentifierColumn, ConditionOperator.Null);
        filter.AddCondition(LoanIdentifierColumn, ConditionOperator.Equal, 0);
        return filter;
    }

    protected override bool ShouldUpdate(Entity existingEntity, ConsumerCreditRecord record)
    {
        // Safety net — GetPreWarmFilter already enforces this server-side.
        return IsNullOrEmpty(existingEntity, LoanIdentifierColumn);
    }

    protected override Entity BuildUpdateEntity(Entity existingEntity, ConsumerCreditRecord record)
    {
        var update = new Entity(EntityLogicalName, existingEntity.Id);
        update[LoanIdentifierColumn] = record.LoanIdentifier;
        return update;
    }
}
