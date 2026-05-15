using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using SqlToDataverseSync.Domain.Interfaces;
using SqlToDataverseSync.Domain.Models;

namespace SqlToDataverseSync.Infrastructure.DeadLetter;

/// <summary>
/// Writes permanently failed records to JSON files for manual review.
/// Path: /home/dead-letter/{Module}_{timestamp}.json
/// </summary>
public class DeadLetterService : IDeadLetterService
{
    private static readonly string DeadLetterDir =
        Path.Combine(Environment.GetEnvironmentVariable("HOME") ?? "/home", "dead-letter");

    private readonly ILogger<DeadLetterService> _logger;

    public DeadLetterService(ILogger<DeadLetterService> logger) => _logger = logger;

    public async Task WriteAsync(string moduleName, List<FailedRecord> failures, CancellationToken ct = default)
    {
        if (failures.Count == 0) return;

        try
        {
            Directory.CreateDirectory(DeadLetterDir);
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
            var fileName = $"{moduleName}_{timestamp}.json";
            var filePath = Path.Combine(DeadLetterDir, fileName);

            var payload = new
            {
                Module = moduleName,
                Timestamp = DateTime.UtcNow,
                FailedRecordCount = failures.Count,
                Records = failures.Select(f => new
                {
                    f.Key,
                    f.ErrorMessage,
                    Attributes = f.Entity?.Attributes?
                        .ToDictionary(a => a.Key, a => a.Value?.ToString())
                }).ToList()
            };

            var json = JsonSerializer.Serialize(payload,
                new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(filePath, json, ct);

            _logger.LogWarning("[{Module}] Dead-lettered {Count} records to {Path}",
                moduleName, failures.Count, filePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[{Module}] Failed to write dead-letter file", moduleName);
        }
    }
}
