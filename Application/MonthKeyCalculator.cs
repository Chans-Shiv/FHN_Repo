namespace SqlToDataverseSync.Application;

/// <summary>
/// Computes MTH_KEY values used as the SQL filter and the staging-row month tag.
///
/// Format: YYYYMM as int. Examples:
///   May 2026  → 202605
///   Dec 2025  → 202512
///   Jan 2026  → 202601
///
/// "Current" data in SQL is always the previous calendar month — the upstream
/// pipeline loads the prior month's snapshot. So this sync filters by
/// PreviousMonth(today) rather than reading MAX(MTH_KEY) from SQL.
/// </summary>
public static class MonthKeyCalculator
{
    /// <summary>
    /// Returns the MTH_KEY for the month immediately preceding <paramref name="today"/>.
    /// Handles January rollover (Jan 2026 → MTH_KEY 202512 for Dec 2025).
    /// </summary>
    /// <param name="today">Reference date. Pass <c>DateTime.UtcNow</c> in production.</param>
    public static int PreviousMonth(DateTime today)
    {
        // Anchor to first-of-month UTC so AddMonths works deterministically
        // regardless of the day-of-month or kind of <paramref name="today"/>.
        var firstOfThisMonth = new DateTime(today.Year, today.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        var firstOfPreviousMonth = firstOfThisMonth.AddMonths(-1);
        return firstOfPreviousMonth.Year * 100 + firstOfPreviousMonth.Month;
    }
}
