using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;
using Fhn.Cdm.DataverseSync.Infrastructure.Dataverse;

namespace Fhn.Cdm.DataverseSync.Infrastructure.DeadLetter;

/// <summary>
/// Writes one Dataverse row per failed record to the configured error table.
/// On failure of the Dataverse insert itself (transient outage, schema mismatch,
/// missing privilege, etc.) falls back to App Insights structured logging so the
/// records are still durable and recoverable via KQL.
///
/// Primary sink:   Dataverse <c>SyncSettings.ErrorTableEntityName</c>
///                 (default <c>crbee_consumercredit_errortable</c>)
/// Fallback sink:  ILogger structured events
///                 (<c>EventName=DeadLetterSummary</c>, <c>EventName=DeadLetterRecord</c>)
///
/// Column mapping (Dataverse logical names — all lowercase per the SDK convention):
///   crbee_accountnumber   ← FailedRecord.AccountNumber
///   crbee_loanidentifier  ← FailedRecord.LoanIdentifier
///   crbee_process         ← moduleName parameter
///   crbee_mth_key         ← mthKey parameter
///   crbee_invocationid    ← invocationId parameter
///   crbee_errormessage    ← FailedRecord.ErrorMessage
///   crbee_id              ← autonumber, Dataverse assigns
///   createdon             ← system, Dataverse populates
/// </summary>
public class DataverseErrorTableService : IDeadLetterService
{
    // Dataverse ExecuteMultiple hard limit.
    private const int BatchSize = 1000;

    // Cap on per-call fallback records emitted to App Insights to keep ingest volume bounded.
    private const int MaxAppInsightsLogged = 500;

    private readonly DataverseConnectionFactory _factory;
    private readonly SyncSettings _settings;
    private readonly ILogger<DataverseErrorTableService> _logger;

    public DataverseErrorTableService(
        DataverseConnectionFactory factory,
        SyncSettings settings,
        ILogger<DataverseErrorTableService> logger)
    {
        _factory = factory;
        _settings = settings;
        _logger = logger;
    }

    public async Task WriteAsync(
        string moduleName,
        int mthKey,
        string invocationId,
        List<FailedRecord> failures,
        CancellationToken ct = default)
    {
        if (failures.Count == 0) return;

        try
        {
            await WriteToDataverseAsync(moduleName, mthKey, invocationId, failures, ct);

            _logger.LogInformation(
                "EventName={EventName} Module={Module} Count={Count} Table={Table}",
                "ErrorTableWritten", moduleName, failures.Count, _settings.ErrorTableEntityName);
        }
        catch (Exception ex)
        {
            // Dataverse insert failed — fall back to App Insights so records aren't lost.
            // We re-emit ALL records, not just the partial failures, because the original
            // exception likely means none of them landed.
            _logger.LogError(ex,
                "EventName={EventName} Module={Module} Count={Count} Table={Table} - falling back to App Insights",
                "ErrorTableWriteFailed", moduleName, failures.Count, _settings.ErrorTableEntityName);

            EmitAppInsightsFallback(moduleName, failures);
        }
    }

    private async Task WriteToDataverseAsync(
        string moduleName,
        int mthKey,
        string invocationId,
        List<FailedRecord> failures,
        CancellationToken ct)
    {
        var client = await _factory.GetClientAsync(ct);

        // Build one error-table entity per failure.
        var entities = failures
            .Select(f => BuildErrorEntity(f, moduleName, mthKey, invocationId))
            .ToList();

        // Dataverse ExecuteMultiple caps at 1000 ops; chunk if more failures arrive at once.
        for (int i = 0; i < entities.Count; i += BatchSize)
        {
            var chunk = entities.Skip(i).Take(BatchSize).ToList();

            var request = new ExecuteMultipleRequest
            {
                Settings = new ExecuteMultipleSettings
                {
                    // Surface partial failures rather than aborting at first error so
                    // we can decide whether the chunk landed cleanly.
                    ContinueOnError = true,
                    ReturnResponses = true
                },
                Requests = new OrganizationRequestCollection()
            };

            foreach (var entity in chunk)
                request.Requests.Add(new CreateRequest { Target = entity });

            var response = (ExecuteMultipleResponse)await client.ExecuteAsync(request, ct);

            if (response.IsFaulted)
            {
                // Any per-row fault inside the chunk → treat as failure of the whole call
                // so the caller's catch block kicks in the App Insights fallback. Keeps
                // semantics simple: either the error-table sink fully succeeded or it didn't.
                var faultCount = response.Responses.Count(r => r.Fault != null);
                var firstFault = response.Responses.FirstOrDefault(r => r.Fault != null)?.Fault?.Message;
                throw new InvalidOperationException(
                    $"ExecuteMultiple into {_settings.ErrorTableEntityName} had {faultCount} faults out of {chunk.Count}. First: {firstFault}");
            }
        }
    }

    private Entity BuildErrorEntity(FailedRecord f, string moduleName, int mthKey, string invocationId)
    {
        var entity = new Entity(_settings.ErrorTableEntityName);

        entity["crbee_accountnumber"] = f.AccountNumber ?? string.Empty;

        if (f.LoanIdentifier.HasValue)
            entity["crbee_loanidentifier"] = f.LoanIdentifier.Value;

        entity["crbee_process"] = moduleName;
        entity["crbee_mth_key"] = mthKey;
        entity["crbee_invocationid"] = invocationId;
        entity["crbee_errormessage"] = f.ErrorMessage;

        // crbee_id (autonumber) and createdon (system) are populated by Dataverse.
        return entity;
    }

    private void EmitAppInsightsFallback(string moduleName, List<FailedRecord> failures)
    {
        _logger.LogError(
            "EventName={EventName} Module={Module} FailedCount={Count}",
            "DeadLetterSummary", moduleName, failures.Count);

        var logged = 0;
        foreach (var f in failures)
        {
            if (logged++ >= MaxAppInsightsLogged) break;

            _logger.LogError(
                "EventName={EventName} Module={Module} AccountNumber={Account} LoanIdentifier={LoanId} Key={Key} Error={Error}",
                "DeadLetterRecord", moduleName,
                f.AccountNumber ?? "",
                f.LoanIdentifier?.ToString() ?? "",
                f.Key,
                f.ErrorMessage);
        }

        if (failures.Count > MaxAppInsightsLogged)
        {
            _logger.LogWarning(
                "EventName={EventName} Module={Module} Suppressed={Suppressed}",
                "DeadLetterTruncated", moduleName, failures.Count - MaxAppInsightsLogged);
        }
    }
}
