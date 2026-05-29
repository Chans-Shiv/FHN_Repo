using Fhn.Cdm.DataverseSync.Domain.Entities;

namespace Fhn.Cdm.DataverseSync.Application.Transformations;

/// <summary>
/// Replaces the Access queries 001–005 with in-memory transformations.
/// Each method is a pure function — no side effects, no I/O, fully testable.
/// 
/// Original Access pipeline:
///   001 → INSERT from source, replace backslash in CustomerName
///   002 → SET ACCT_NUM = ACCT_NUM_Decimal
///   003 → IF Len(ACCT_NUM)=16, SET ACCT_ID = LoanIdentifier
///   004 → IF CollState="CollStateNull", SET CollState = ""
///   005 → IF Len(ACCT_NUM)<>16, SET ACCT_KEY = LoanIdentifier
/// </summary>
public static class CommonTransformation
{
    /// <summary>
    /// Transforms a raw SQL row dictionary into a strongly-typed ConsumerCreditRecord
    /// with all common business rules applied.
    /// </summary>
    public static ConsumerCreditRecord Transform(Dictionary<string, object?> sqlRow)
    {
        var record = MapFromSqlRow(sqlRow);
        ApplyTransformations(record);
        return record;
    }

    /// <summary>Transforms a batch of SQL rows. Returns the transformed list.</summary>
    public static List<ConsumerCreditRecord> TransformBatch(List<Dictionary<string, object?>> sqlBatch)
    {
        return sqlBatch.Select(Transform).ToList();
    }

    // ═══════════════════════════════════════════════════
    // Step 1: Map raw dictionary to strongly-typed entity
    // ═══════════════════════════════════════════════════

    private static ConsumerCreditRecord MapFromSqlRow(Dictionary<string, object?> row)
    {
        return new ConsumerCreditRecord
        {
            AcctId = GetString(row, "ACCT_ID"),
            AcctKey = GetInt(row, "ACCT_KEY"),
            AcctNum = GetString(row, "ACCT_NUM"),
            AcctNumDecimal = GetString(row, "ACCT_NUM"),  // Initially same as ACCT_NUM
            AcctNumPadded = GetString(row, "ACCT_NUM"),   // Will hold the padded version
            MthKey = GetInt(row, "MTH_KEY"),
            Balance = GetDecimal(row, "Balance"),
            ChargeOffIndicator = GetInt(row, "ChargeOffIndicator"),
            CostCenter = GetInt(row, "CostCenter"),
            CustomerName = GetString(row, "CustomerName"),
            CollAddr = GetString(row, "CollAddr"),
            CollCity = GetString(row, "CollCity"),
            CollState = GetString(row, "CollState"),
            CollZip = GetString(row, "CollZip"),
            CustStreetAddress = GetString(row, "CustStreetAddress"),
            CustCity = GetString(row, "CustCity"),
            CustState = GetString(row, "CustState"),
            CustZip = GetString(row, "CustZip"),
            LoanIdentifier = GetInt(row, "LoanIdentifier"),
            SourceSystem = GetString(row, "SourceSystem"),
            Product = GetInt(row, "Product"),
        };
    }

    // ═══════════════════════════════════════════════════
    // Steps 2–6: Apply all business rules in sequence
    // ═══════════════════════════════════════════════════

    private static void ApplyTransformations(ConsumerCreditRecord record)
    {
        // Keep the original padded version before stripping zeros
        record.AcctNumPadded = record.AcctNum;

        // Strip leading zeros: "0000234567823" → "234567823"
        record.AcctNum = StripLeadingZeros(record.AcctNum);

        // Store the decimal/non-padded version
        record.AcctNumDecimal = record.AcctNum;

        // Query 001: Replace backslashes in CustomerName with spaces
        record.CustomerName = record.CustomerName.Replace("\\", " ");

        // Query 002: ACCT_NUM = ACCT_NUM_Decimal (already done by stripping zeros)
        // No-op since AcctNum is already the stripped version

        // Query 003: If Len(ACCT_NUM) == 16, push LoanIdentifier to ACCT_ID
        if (record.AcctNum.Length == 16)
        {
            record.AcctId = record.LoanIdentifier.ToString();
        }

        // Query 004: If CollState == "CollStateNull", clear it
        if (string.Equals(record.CollState, "CollStateNull", StringComparison.OrdinalIgnoreCase))
        {
            record.CollState = string.Empty;
        }

        // Query 005: If Len(ACCT_NUM) != 16, push LoanIdentifier to ACCT_KEY
        if (record.AcctNum.Length != 16)
        {
            record.AcctKey = record.LoanIdentifier;
        }
    }

    // ═══════════════════════════════════════════════════
    // Value extraction helpers (null-safe)
    // ═══════════════════════════════════════════════════

    /// <summary>Strip leading zeros: "0000234567823" → "234567823". Preserves single "0".</summary>
    public static string StripLeadingZeros(string value)
    {
        if (string.IsNullOrEmpty(value)) return string.Empty;

        var trimmed = value.TrimStart('0');
        return string.IsNullOrEmpty(trimmed) ? "0" : trimmed;
    }

    private static string GetString(Dictionary<string, object?> row, string key)
    {
        if (row.TryGetValue(key, out var val) && val is not null)
            return val.ToString()?.Trim() ?? string.Empty;
        return string.Empty;
    }

    private static int GetInt(Dictionary<string, object?> row, string key)
    {
        if (row.TryGetValue(key, out var val) && val is not null)
        {
            if (val is int i) return i;
            if (val is long l) return (int)l;
            if (val is decimal d) return (int)d;
            if (int.TryParse(val.ToString(), out var parsed)) return parsed;
        }
        return 0;
    }

    private static decimal GetDecimal(Dictionary<string, object?> row, string key)
    {
        if (row.TryGetValue(key, out var val) && val is not null)
        {
            if (val is decimal d) return d;
            if (val is double dbl) return (decimal)dbl;
            if (val is float f) return (decimal)f;
            if (decimal.TryParse(val.ToString(), out var parsed)) return parsed;
        }
        return 0m;
    }
}
