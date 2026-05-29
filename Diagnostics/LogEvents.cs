namespace Fhn.Cdm.DataverseSync.Diagnostics;

/// <summary>
/// Canonical event-name vocabulary for structured logs. Every <c>EventName=</c>
/// value emitted by the app comes from here, so dashboards and KQL queries that
/// filter on these names can't drift from the producers via typos.
///
/// Naming: PascalCase, present-tense for state transitions ("ModuleCompleted"),
/// past-tense for one-shots ("FailureArchived"). Add new events here first, then
/// reference the constant — never inline a literal at the call site.
/// </summary>
public static class LogEvents
{
    // ── Sync orchestration ──────────────────────────────────────────────
    public const string Phase1Started = nameof(Phase1Started);
    public const string Phase1SqlRowCount = nameof(Phase1SqlRowCount);
    public const string Phase1Exit = nameof(Phase1Exit);
    public const string Phase2KeysStarted = nameof(Phase2KeysStarted);
    public const string Phase2KeysComplete = nameof(Phase2KeysComplete);
    public const string Phase3Started = nameof(Phase3Started);
    public const string Phase3Complete = nameof(Phase3Complete);
    public const string Phase4Started = nameof(Phase4Started);
    public const string Phase4Complete = nameof(Phase4Complete);
    public const string NothingToProcess = nameof(NothingToProcess);
    public const string WorkPending = nameof(WorkPending);
    public const string ModulesAlreadyCompleted = nameof(ModulesAlreadyCompleted);
    public const string AbandonedModuleSkipped = nameof(AbandonedModuleSkipped);
    public const string ModuleCompleted = nameof(ModuleCompleted);
    public const string ModuleAbandoned = nameof(ModuleAbandoned);
    public const string ModuleIncompleteStream = nameof(ModuleIncompleteStream);
    public const string ModulesTrackStarted = nameof(ModulesTrackStarted);
    public const string ModulesTrackComplete = nameof(ModulesTrackComplete);
    public const string ModulesTrackFailed = nameof(ModulesTrackFailed);
    public const string ModulesBatchComplete = nameof(ModulesBatchComplete);

    // ── Pre-warm / SQL key stream ───────────────────────────────────────
    public const string PreWarmStarted = nameof(PreWarmStarted);
    public const string PreWarmComplete = nameof(PreWarmComplete);
    public const string PreWarmAborted = nameof(PreWarmAborted);
    public const string PreWarmChunk = nameof(PreWarmChunk);
    public const string PreWarmQueryStarted = nameof(PreWarmQueryStarted);
    public const string PreWarmQueryComplete = nameof(PreWarmQueryComplete);
    public const string SqlKeyStreamProgress = nameof(SqlKeyStreamProgress);
    public const string SqlKeyStreamComplete = nameof(SqlKeyStreamComplete);

    // ── Module per-batch ────────────────────────────────────────────────
    public const string ModuleBatchComplete = nameof(ModuleBatchComplete);

    // ── SQL streaming ───────────────────────────────────────────────────
    public const string SqlBatchYielded = nameof(SqlBatchYielded);
    public const string SqlBatchStreamComplete = nameof(SqlBatchStreamComplete);

    // ── Dataverse batch update ──────────────────────────────────────────
    public const string BatchUpdateStarted = nameof(BatchUpdateStarted);
    public const string BatchUpdateBatch = nameof(BatchUpdateBatch);
    public const string BatchUpdateComplete = nameof(BatchUpdateComplete);
    public const string BatchRetryStage1 = nameof(BatchRetryStage1);
    public const string BatchRetryStage2Recovered = nameof(BatchRetryStage2Recovered);
    public const string BatchPermanentFailures = nameof(BatchPermanentFailures);
    public const string BatchSendFailed = nameof(BatchSendFailed);

    // ── Dataverse auth ──────────────────────────────────────────────────
    public const string DataverseTokenRefresh = nameof(DataverseTokenRefresh);
    public const string DataverseTokenRefreshed = nameof(DataverseTokenRefreshed);

    // ── Dead-letter pipeline ────────────────────────────────────────────
    public const string DeadLetterEnqueued = nameof(DeadLetterEnqueued);
    public const string DeadLetterEnqueueFailed = nameof(DeadLetterEnqueueFailed);
    public const string DeadLetterBatchEnqueued = nameof(DeadLetterBatchEnqueued);
    public const string DeadLetterDequeued = nameof(DeadLetterDequeued);
    public const string DeadLetterGivenUp = nameof(DeadLetterGivenUp);
    public const string ErrorTableWritten = nameof(ErrorTableWritten);
    public const string ErrorTableWriteFailed = nameof(ErrorTableWriteFailed);
    public const string FailureArchived = nameof(FailureArchived);
    public const string FailureArchiveWriteFailed = nameof(FailureArchiveWriteFailed);

    // ── Tracking ────────────────────────────────────────────────────────
    public const string TrackingConcurrencyConflict = nameof(TrackingConcurrencyConflict);
}
