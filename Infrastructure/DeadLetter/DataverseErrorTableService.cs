using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Models;
using Fhn.Cdm.DataverseSync.Infrastructure.Dataverse;

namespace Fhn.Cdm.DataverseSync.Infrastructure.DeadLetter;

/// <summary>
/// Writes a single failed record (delivered via Storage Queue) into the Dataverse
/// error table. Exceptions are intentionally NOT caught — the QueueTrigger runtime
/// requeues the message on any thrown exception. After 5 delivery attempts (default)
/// the message lands in the poison queue and is logged separately.
///
/// Column mapping (Dataverse logical names — all lowercase per SDK convention):
///   crbee_accountnumber   ← DeadLetterMessage.AccountNumber
///   crbee_loanidentifier  ← DeadLetterMessage.LoanIdentifier
///   crbee_process         ← DeadLetterMessage.ModuleName
///   crbee_mth_key         ← DeadLetterMessage.MthKey
///   crbee_invocationid    ← DeadLetterMessage.InvocationId
///   crbee_errormessage    ← DeadLetterMessage.ErrorMessage
///   crbee_id              ← autonumber, Dataverse assigns
///   createdon             ← system, Dataverse populates
/// </summary>
public class DataverseErrorTableService
{
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

    public async Task WriteOneAsync(DeadLetterMessage msg, CancellationToken ct = default)
    {
        var client = await _factory.GetClientAsync(ct);

        var entity = new Entity(_settings.ErrorTableEntityName);
        entity["crbee_accountnumber"] = msg.AccountNumber ?? string.Empty;

        if (msg.LoanIdentifier.HasValue)
            entity["crbee_loanidentifier"] = msg.LoanIdentifier.Value;

        entity["crbee_process"] = msg.ModuleName;
        entity["crbee_mth_key"] = msg.MthKey;
        entity["crbee_invocationid"] = msg.InvocationId;
        entity["crbee_errormessage"] = msg.ErrorMessage;

        var request = new CreateRequest { Target = entity };
        await client.ExecuteAsync(request, ct);

        _logger.LogInformation(
            "EventName={EventName} Module={Module} MthKey={MthKey} Account={Account} LoanId={LoanId} Table={Table}",
            "ErrorTableWritten", msg.ModuleName, msg.MthKey,
            msg.AccountNumber ?? "",
            msg.LoanIdentifier?.ToString() ?? "",
            _settings.ErrorTableEntityName);
    }
}
