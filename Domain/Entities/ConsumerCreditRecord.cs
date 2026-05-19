namespace Fhn.Cdm.DataverseSync.Domain.Entities;

/// <summary>
/// Strongly-typed representation of a row from dbo_ConsumerCreditDataAcq
/// after common transformation has been applied.
/// 
/// This is the domain object that flows through the entire pipeline:
///   SQL row (Dictionary) → CommonTransformation → ConsumerCreditRecord → Staging / Processors
/// </summary>
public class ConsumerCreditRecord
{
    public string AcctId { get; set; } = string.Empty;
    public int AcctKey { get; set; }
    public string AcctNum { get; set; } = string.Empty;
    public string AcctNumDecimal { get; set; } = string.Empty;
    public string AcctNumPadded { get; set; } = string.Empty;
    public int MthKey { get; set; }
    public decimal Balance { get; set; }
    public int ChargeOffIndicator { get; set; }
    public int CostCenter { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public string CollAddr { get; set; } = string.Empty;
    public string CollCity { get; set; } = string.Empty;
    public string CollState { get; set; } = string.Empty;
    public string CollZip { get; set; } = string.Empty;
    public string CustStreetAddress { get; set; } = string.Empty;
    public string CustCity { get; set; } = string.Empty;
    public string CustState { get; set; } = string.Empty;
    public string CustZip { get; set; } = string.Empty;
    public int LoanIdentifier { get; set; }
    public string SourceSystem { get; set; } = string.Empty;
    public int Product { get; set; }
}
