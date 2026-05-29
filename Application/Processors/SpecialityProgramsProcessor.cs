using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Entities;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;

namespace Fhn.Cdm.DataverseSync.Application.Processors;

/// <summary>
/// Speciality Programs module processor.
///
/// Table:     crfdf_maintable
/// Match:     ConsumerCreditRecord.AcctNum → crfdf_accountnumber
/// Condition: crfdf_loanidentifier is null OR zero
/// Action:    UPDATE crfdf_loanidentifier = record.LoanIdentifier
/// </summary>
public class SpecialityProgramsProcessor : BaseModuleProcessor
{
    public override string ModuleName => "SpecialityPrograms";
    public override int Order => 4;

    protected override string EntityLogicalName => "crfdf_maintable";
    protected override string MatchColumn => "crfdf_accountnumber";

    private const string LoanIdentifierColumn = "crfdf_loanidentifier";

    public SpecialityProgramsProcessor(
        IDataverseRepository repository,
        SyncSettings settings,
        ILogger<SpecialityProgramsProcessor> logger)
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
