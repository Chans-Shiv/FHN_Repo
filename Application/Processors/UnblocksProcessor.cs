using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Entities;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;

namespace Fhn.Cdm.DataverseSync.Application.Processors;

/// <summary>
/// Unblocks module processor.
///
/// Table:     dmt_tblmain
/// Match:     ConsumerCreditRecord.AcctNum → dmt_accountnumber
/// Condition: dmt_loanidentifier is null OR zero
/// Action:    UPDATE dmt_loanidentifier = record.LoanIdentifier
/// </summary>
public class UnblocksProcessor : BaseModuleProcessor
{
    public override string ModuleName => "Unblocks";
    public override int Order => 5;

    protected override string EntityLogicalName => "dmt_tblmain";
    protected override string MatchColumn => "dmt_accountnumber";

    private const string LoanIdentifierColumn = "dmt_loanidentifier";

    public UnblocksProcessor(
        IDataverseRepository repository,
        SyncSettings settings,
        ILogger<UnblocksProcessor> logger)
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
