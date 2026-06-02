using System.Text;
using System.Text.Json;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Diagnostics;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Domain.Models;

namespace Fhn.Cdm.DataverseSync.Infrastructure.Tracking;

/// <summary>
/// Persists sync tracking state as a single JSON blob in Azure Storage.
/// Auth: DefaultAzureCredential (Managed Identity in Azure, az login locally).
/// Concurrency: ETag-based optimistic concurrency; on conflict the latest writer wins.
/// </summary>
public class BlobTrackingService : ITrackingService
{
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = true };

    private readonly BlobContainerClient _container;
    private readonly BlobClient _blob;
    private readonly ILogger<BlobTrackingService> _logger;

    // ETag captured at last successful Load; used as IfMatch on the next Save.
    private ETag? _lastETag;

    public BlobTrackingService(SyncSettings settings, ILogger<BlobTrackingService> logger)
    {
        _logger = logger;

        // Identity-only auth (Managed Identity in Azure, `az login` locally).
        // ManagedIdentityCredential is excluded only in local dev so we don't
        // probe an IMDS endpoint that doesn't exist on a dev box.
        var isLocalDev = string.Equals(
            Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT"),
            "Development",
            StringComparison.OrdinalIgnoreCase);

        var serviceClient = new BlobServiceClient(
            new Uri(settings.SyncStorageBlobServiceUri),
            new DefaultAzureCredential(new DefaultAzureCredentialOptions
            {
                ExcludeManagedIdentityCredential = isLocalDev
            }));

        _container = serviceClient.GetBlobContainerClient(settings.TrackingContainerName);
        _blob = _container.GetBlobClient(settings.TrackingBlobName);
    }

    public async Task<TrackingState> LoadAsync(CancellationToken ct = default)
    {
        try
        {
            var response = await _blob.DownloadContentAsync(ct);
            _lastETag = response.Value.Details.ETag;

            var json = response.Value.Content.ToString();
            var state = JsonSerializer.Deserialize<TrackingState>(json);
            return state ?? new TrackingState();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            _logger.LogInformation("Tracking blob not found — treating as first run.");
            _lastETag = null;
            return new TrackingState();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not read tracking blob — treating as first run");
            _lastETag = null;
            return new TrackingState();
        }
    }

    public async Task SaveAsync(TrackingState state, CancellationToken ct = default)
    {
        await _container.CreateIfNotExistsAsync(cancellationToken: ct);

        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(state, JsonOpts));
        var headers = new BlobHttpHeaders { ContentType = "application/json" };

        var conditions = _lastETag.HasValue
            ? new BlobRequestConditions { IfMatch = _lastETag }
            : null;

        try
        {
            using var stream = new MemoryStream(bytes);
            var response = await _blob.UploadAsync(stream,
                new BlobUploadOptions { HttpHeaders = headers, Conditions = conditions }, ct);
            _lastETag = response.Value.ETag;
        }
        catch (RequestFailedException ex) when (ex.Status == 412 || ex.Status == 409)
        {
            _logger.LogWarning(
                "EventName={EventName} Status={Status} — concurrent writer detected; forcing overwrite",
                LogEvents.TrackingConcurrencyConflict, ex.Status);

            using var stream = new MemoryStream(bytes);
            var response = await _blob.UploadAsync(stream,
                new BlobUploadOptions { HttpHeaders = headers }, ct);
            _lastETag = response.Value.ETag;
        }

        _logger.LogInformation("Tracking state saved: Month={Month}, SqlRowCount={SqlRowCount}",
            state.Month, state.SqlRowCount);
    }
}
