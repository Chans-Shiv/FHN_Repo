using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Domain.Interfaces;
using SqlToDataverseSync.Domain.Models;

namespace SqlToDataverseSync.Infrastructure.Tracking;

/// <summary>
/// Persists sync tracking state as JSON to /home/data/tracking.json.
/// On Azure Functions, /home is persistent storage across restarts.
/// </summary>
public class FileTrackingService : ITrackingService
{
    private static readonly string TrackingDir =
        Path.Combine(Environment.GetEnvironmentVariable("HOME") ?? "/home", "data");
    private static readonly string TrackingFile =
        Path.Combine(TrackingDir, "tracking.json");

    private readonly ILogger<FileTrackingService> _logger;
    private static readonly JsonSerializerOptions JsonOpts = new() { WriteIndented = true };

    public FileTrackingService(ILogger<FileTrackingService> logger) => _logger = logger;

    public async Task<TrackingState> LoadAsync(CancellationToken ct = default)
    {
        try
        {
            if (File.Exists(TrackingFile))
            {
                var json = await File.ReadAllTextAsync(TrackingFile, ct);
                return JsonSerializer.Deserialize<TrackingState>(json) ?? new TrackingState();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not read tracking file — treating as first run");
        }
        return new TrackingState();
    }

    public async Task SaveAsync(TrackingState state, CancellationToken ct = default)
    {
        try
        {
            Directory.CreateDirectory(TrackingDir);
            var json = JsonSerializer.Serialize(state, JsonOpts);
            await File.WriteAllTextAsync(TrackingFile, json, ct);
            _logger.LogInformation("Tracking state saved: Month={Month}, Staging={Staging}",
                state.Month, state.StagingLoadedCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save tracking state");
        }
    }

    /// <summary>
    /// Generates a SHA-256 hash of sorted failed record keys.
    /// Used to detect if the same records are failing across consecutive days.
    /// </summary>
    public static string ComputeFailureHash(List<string> failedKeys)
    {
        var sorted = failedKeys.OrderBy(k => k).ToList();
        var combined = string.Join("|", sorted);
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(combined));
        return Convert.ToHexString(bytes);
    }
}
