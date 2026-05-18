using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using SqlToDataverseSync.Configuration;

namespace SqlToDataverseSync.Infrastructure.Dataverse;

/// <summary>
/// Creates and manages the Dataverse ServiceClient connection.
/// Uses DefaultAzureCredential for authentication:
///   Local  → az login / Visual Studio
///   Azure  → Managed Identity (System-assigned)
/// </summary>
public class DataverseConnectionFactory : IDisposable
{
    private readonly SyncSettings _settings;
    private readonly ILogger<DataverseConnectionFactory> _logger;
    private ServiceClient? _client;
    private readonly object _lock = new();

    public DataverseConnectionFactory(SyncSettings settings, ILogger<DataverseConnectionFactory> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public Task<ServiceClient> GetClientAsync(CancellationToken ct = default)
    {
        if (_client?.IsReady == true) return Task.FromResult(_client);

        lock (_lock)
        {
            if (_client?.IsReady == true) return Task.FromResult(_client);

            var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
            {
                ExcludeManagedIdentityCredential = true
            });
            var scope = _settings.DataverseUrl.TrimEnd('/') + "/.default";

            _logger.LogInformation("Creating Dataverse ServiceClient via DefaultAzureCredential...");

            _client = new ServiceClient(
                instanceUrl: new Uri(_settings.DataverseUrl),
                tokenProviderFunction: async (instanceUri) =>
                {
                    var token = await credential.GetTokenAsync(
                        new TokenRequestContext(new[] { scope }),default);
                    return token.Token;
                },
                useUniqueInstance: true);

            if (!_client.IsReady)
            {
                var error = _client.LastError;
                _logger.LogError("Dataverse connection FAILED: {Error}", error);
                throw new InvalidOperationException($"Dataverse connection failed: {error}");
            }

            _client.EnableAffinityCookie = true;
            _client.MaxRetryCount = 3;
            _logger.LogInformation("Dataverse ServiceClient connected successfully.");
        }

        return Task.FromResult(_client!);
    }

    public void Dispose() => _client?.Dispose();
}
