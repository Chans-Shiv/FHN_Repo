using System.Text.Json;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Infrastructure.DeadLetter;

/// <summary>
/// Durable archive for records that exhausted all queue retries.
///
/// One blob per record under <c>errors/yyyy/MM/dd/{invocationId}-{key}.json</c>:
///   - Date partition: makes "show me what failed yesterday" a prefix listing
///   - One blob per record: no concurrent-write conflicts, no append-blob block
///     limit (50K), no need to re-upload the file to remove a row
///   - Stable filename ({invocationId}-{key}): two runs of the same failure
///     overwrite cleanly; you see the most recent attempt's error
///
/// Container is created on first use. Identity-based auth (DefaultAzureCredential)
/// uses the same Managed Identity that handles the tracking blob and dead-letter
/// queue — one role grant covers all three.
/// </summary>
public class FailureBlobWriter
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = true };

    private readonly BlobContainerClient _container;
    private readonly ILogger<FailureBlobWriter> _logger;
    private int _containerEnsured;

    public FailureBlobWriter(BlobContainerClient container, ILogger<FailureBlobWriter> logger)
    {
        _container = container;
        _logger = logger;
    }

    public async Task WriteAsync(DeadLetterMessage msg, Exception giveUpError, CancellationToken ct)
    {
        if (Interlocked.Exchange(ref _containerEnsured, 1) == 0)
            await _container.CreateIfNotExistsAsync(cancellationToken: ct);

        var now = DateTimeOffset.UtcNow;
        var blobName = $"errors/{now:yyyy}/{now:MM}/{now:dd}/{msg.InvocationId}-{msg.Key}.json";

        // Wrap the message with the final exception details so the archived blob
        // is self-contained for operator triage.
        var payload = new
        {
            msg.ModuleName,
            msg.MthKey,
            msg.InvocationId,
            msg.Key,
            msg.AccountNumber,
            msg.LoanIdentifier,
            OriginalError = msg.ErrorMessage,
            msg.EnqueuedAt,
            GivenUpAt = now,
            FinalAttemptError = giveUpError.Message,
            ExceptionType = giveUpError.GetType().FullName,
        };

        var json = JsonSerializer.Serialize(payload, JsonOpts);
        var blob = _container.GetBlobClient(blobName);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await blob.UploadAsync(stream, overwrite: true, cancellationToken: ct);

        _logger.LogInformation(
            "EventName={EventName} BlobName={BlobName} Module={Module} MthKey={MthKey}",
            "FailureArchived", blobName, msg.ModuleName, msg.MthKey);
    }
}
