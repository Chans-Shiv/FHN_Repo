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
/// Wire format for one failed record in the dead-letter queue.
/// Each message is processed independently by <c>DeadLetterProcessorFunction</c>.
/// </summary>
public class DeadLetterMessage
{
    public string ModuleName { get; set; } = string.Empty;
    public int MthKey { get; set; }
    public string InvocationId { get; set; } = string.Empty;
    public string Key { get; set; } = string.Empty;
    public string? AccountNumber { get; set; }
    public int? LoanIdentifier { get; set; }
    public string ErrorMessage { get; set; } = string.Empty;
    public DateTimeOffset EnqueuedAt { get; set; }
}
