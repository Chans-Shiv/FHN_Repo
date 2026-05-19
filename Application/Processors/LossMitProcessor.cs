using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Entities;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;

namespace Fhn.Cdm.DataverseSync.Application.Processors;

/// <summary>
/// Loss Mitigation module processor.
///
/// Table:     crbee_dmt_loanmodificationrecord1s
/// Match:     ConsumerCreditRecord.AcctNum → crbee_dmt_accountnumber
/// Condition: crbee_dmt_loanidentifier is null OR zero
/// Action:    UPDATE crbee_dmt_loanidentifier = record.LoanIdentifier
/// </summary>
public class LossMitProcessor : BaseModuleProcessor
{
    public override string ModuleName => "LossMit";
    public override int Order => 3;

    protected override string EntityLogicalName => "crbee_dmt_loanmodificationrecord1s";
    protected override string MatchColumn => "crbee_dmt_accountnumber";

    private const string LoanIdentifierColumn = "crbee_dmt_loanidentifier";

    public LossMitProcessor(
        IDataverseRepository repository,
        SyncSettings settings,
        ILogger<LossMitProcessor> logger)
        : base(repository, settings, logger) { }

    protected override string[] GetColumnsToRetrieve()
    {
        return new[]
        {
            MatchColumn,
            LoanIdentifierColumn,
        };
    }

    protected override string GetMatchKey(ConsumerCreditRecord record)
    {
        return record.AcctNum;
    }

    protected override FilterExpression? GetPreWarmFilter()
    {
        var filter = new FilterExpression(LogicalOperator.Or);
        filter.AddCondition(LoanIdentifierColumn, ConditionOperator.Null);
        filter.AddCondition(LoanIdentifierColumn, ConditionOperator.Equal, 0);
        return filter;
    }

    protected override bool ShouldUpdate(Entity existingEntity, ConsumerCreditRecord record)
    {
        return IsNullOrEmpty(existingEntity, LoanIdentifierColumn);
    }

    protected override Entity BuildUpdateEntity(Entity existingEntity, ConsumerCreditRecord record)
    {
        var update = new Entity(EntityLogicalName, existingEntity.Id);
        update[LoanIdentifierColumn] = record.LoanIdentifier;
        return update;
    }
}
