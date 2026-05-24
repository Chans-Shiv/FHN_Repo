using Microsoft.Xrm.Sdk;

namespace Fhn.Cdm.DataverseSync.Domain.Models;

public class SyncResult
{
    public int MthKey { get; set; }
    public int TotalRowsRead { get; set; }
    public Dictionary<string, ModuleResult> ModuleResults { get; set; } = new();
    public TimeSpan Duration { get; set; }
    public List<string> Errors { get; set; } = new();

    public string Summary =>
        $"MthKey={MthKey}, Read={TotalRowsRead}, " +
        $"Modules=[{string.Join(", ", ModuleResults.Select(m => $"{m.Key}:{m.Value.RowsUpdated}u/{m.Value.RowsFailed}f/{m.Value.RowsSkipped}s"))}], " +
        $"Duration={Duration:hh\\:mm\\:ss}";
}

public class ModuleResult
{
    public string ModuleName { get; set; } = string.Empty;
    public int RowsProcessed { get; set; }
    public int RowsUpdated { get; set; }
    public int RowsSkipped { get; set; }
    public int RowsFailed { get; set; }
    public List<FailedRecord> Failures { get; set; } = new();
    public List<string> Errors { get; set; } = new();
}

public class FailedRecord
{
    public string Key { get; set; } = string.Empty;
    public string ErrorMessage { get; set; } = string.Empty;
    public Entity? Entity { get; set; }
    public Dictionary<string, object?>? SourceData { get; set; }

    /// <summary>
    /// Account number from the source SQL row. Populated by the caller (orchestrator
    /// for staging failures, BaseModuleProcessor for module-update failures) so the
    /// dead-letter writer can record it without re-deriving from the Entity.
    /// </summary>
    public string? AccountNumber { get; set; }

    /// <summary>
    /// LoanIdentifier value the sync was attempting to write. Same population pattern
    /// as <see cref="AccountNumber"/>.
    /// </summary>
    public int? LoanIdentifier { get; set; }
}

/// <summary>
/// Persisted state tracking sync progress across daily runs.
/// Stored as JSON in /home/data/tracking.json.
/// </summary>
public class TrackingState
{
    public string Month { get; set; } = string.Empty;
    public long SqlRowCount { get; set; }
    public DateTime LastRunDate { get; set; }
    public Dictionary<string, ModuleTrackingState> Modules { get; set; } = new();
}

public class ModuleTrackingState
{
    public int SuccessCount { get; set; }
    public int ConsecutiveFailureDays { get; set; }
    public string? LastFailureDate { get; set; }
    public string? LastFailedHash { get; set; }
    public List<string> LastFailedKeys { get; set; } = new();

    /// <summary>Set when MaxConsecutiveFailureDays is reached. Subsequent scheduled runs skip this module.</summary>
    public DateTime? AbandonedAt { get; set; }

    /// <summary>
    /// MTH_KEY of the month this module last finished cleanly (RowsFailed=0 AND saw every SQL row).
    /// Combined with <see cref="LastCompletedSqlRowCount"/>, subsequent runs skip this module until
    /// the month rolls over or the SQL row count changes.
    /// </summary>
    public string? LastCompletedMonth { get; set; }

    /// <summary>
    /// SQL row count at the time <see cref="LastCompletedMonth"/> was recorded. If SQL grows
    /// mid-month (upstream re-load), this won't match the current count and the module re-runs.
    /// </summary>
    public long? LastCompletedSqlRowCount { get; set; }
}
