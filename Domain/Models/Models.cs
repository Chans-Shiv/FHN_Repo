using Microsoft.Xrm.Sdk;

namespace SqlToDataverseSync.Domain.Models;

public class SyncResult
{
    public int MthKey { get; set; }
    public int TotalRowsRead { get; set; }
    public int StagingRowsInserted { get; set; }
    public int StagingRowsFailed { get; set; }
    public Dictionary<string, ModuleResult> ModuleResults { get; set; } = new();
    public TimeSpan Duration { get; set; }
    public List<string> Errors { get; set; } = new();

    public string Summary =>
        $"MthKey={MthKey}, Read={TotalRowsRead}, " +
        $"Staged={StagingRowsInserted}/{StagingRowsFailed}f, " +
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
}

/// <summary>
/// Persisted state tracking sync progress across daily runs.
/// Stored as JSON in /home/data/tracking.json.
/// </summary>
public class TrackingState
{
    public string Month { get; set; } = string.Empty;
    public long SqlRowCount { get; set; }
    public long StagingLoadedCount { get; set; }
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
}
