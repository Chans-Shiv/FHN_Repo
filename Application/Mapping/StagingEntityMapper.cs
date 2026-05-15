using Microsoft.Xrm.Sdk;
using SqlToDataverseSync.Domain.Entities;

namespace SqlToDataverseSync.Application.Mapping;

/// <summary>
/// Maps ConsumerCreditRecord → Dataverse Entity for staging table.
/// Table: crbee_stg_consumercreditdata
/// 
/// Each method is pure — no I/O, fully testable.
/// </summary>
public static class StagingEntityMapper
{
    public const string EntityLogicalName = "crbee_stg_consumercreditdata";

    public static Entity MapToEntity(ConsumerCreditRecord record)
    {
        var entity = new Entity(EntityLogicalName);

        entity["crbee_accountid"] = record.AcctId;
        entity["crbee_accountkey"] = record.AcctKey;
        entity["crbee_accountnumber"] = record.AcctNum;
        entity["crbee_decimalaccountnumber"] = record.AcctNumDecimal;
        entity["crbee_paddedaccountnum"] = record.AcctNumPadded;
        entity["crbee_monthkey"] = record.MthKey;
        entity["crbee_accountbalance"] = new Money(record.Balance);
        // OptionSet attributes only written when source had a real value.
        // CommonTransformation.GetInt returns 0 for null/missing — 0 is unlikely to be a valid option.
        if (record.ChargeOffIndicator != 0)
            entity["crbee_chargeoffindicator"] = new OptionSetValue(record.ChargeOffIndicator);
        entity["crbee_costcenter"] = record.CostCenter;
        entity["crbee_customername"] = record.CustomerName;
        entity["crbee_collectionaddress"] = record.CollAddr;
        entity["crbee_collectioncity"] = record.CollCity;
        entity["crbee_collectionstate"] = record.CollState;
        entity["crbee_collectionzipcode"] = record.CollZip;
        entity["crbee_customerstreetaddress"] = record.CustStreetAddress;
        entity["crbee_customercity"] = record.CustCity;
        entity["crbee_customerstate"] = record.CustState;
        entity["crbee_customerzipcode"] = record.CustZip;
        entity["crbee_loanidentifier"] = record.LoanIdentifier;
        entity["crbee_sourcesystem"] = record.SourceSystem;
        if (record.Product != 0)
            entity["crbee_producttype"] = new OptionSetValue(record.Product);

        return entity;
    }

    public static List<Entity> MapBatch(List<ConsumerCreditRecord> records)
    {
        return records.Select(MapToEntity).ToList();
    }
}
