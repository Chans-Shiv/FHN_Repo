using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using SqlToDataverseSync.Configuration;

namespace SqlToDataverseSync.Infrastructure.Dataverse;

/// <summary>
/// Creates and manages the Dataverse ServiceClient connection.
///
/// Auth: DefaultAzureCredential
///   - Local  → az login / Visual Studio sign-in / azd
///   - Azure  → Managed Identity (System-assigned)
///   Note: ManagedIdentityCredential is excluded so local runs don't waste time
///   probing the IMDS endpoint that doesn't exist on a dev box.
///
/// Token caching:
///   The Dataverse SDK invokes our token-provider lambda on every operation,
///   not only on connect or expiry. Without caching, each call goes back through
///   DefaultAzureCredential → VisualStudioCredential → Microsoft.Asal.TokenService.exe,
///   which is unreliable under concurrent load (we've observed it returning
///   "unexpected error" intermittently and cascading every Dataverse call into a
///   failure). Caching the AccessToken until ~5 min before expiry collapses
///   thousands of credential lookups into one per hour.
/// </summary>
public class DataverseConnectionFactory : IDisposable
{
    // Refresh ~5 min before the token's stated expiry to avoid mid-request expiration.
    private static readonly TimeSpan RefreshSkew = TimeSpan.FromMinutes(5);

    private readonly SyncSettings _settings;
    private readonly ILogger<DataverseConnectionFactory> _logger;

    private readonly object _clientLock = new();
    private readonly SemaphoreSlim _tokenLock = new(1, 1);

    private ServiceClient? _client;
    private DefaultAzureCredential? _credential;
    private string? _scope;
    private AccessToken? _cachedToken;

    public DataverseConnectionFactory(SyncSettings settings, ILogger<DataverseConnectionFactory> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public Task<ServiceClient> GetClientAsync(CancellationToken ct = default)
    {
        if (_client?.IsReady == true) return Task.FromResult(_client);

        lock (_clientLock)
        {
            if (_client?.IsReady == true) return Task.FromResult(_client);

            _credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
            {
                ExcludeManagedIdentityCredential = true
            });
            _scope = _settings.DataverseUrl.TrimEnd('/') + "/.default";

            _logger.LogInformation("Creating Dataverse ServiceClient via DefaultAzureCredential...");

            _client = new ServiceClient(
                instanceUrl: new Uri(_settings.DataverseUrl),
                tokenProviderFunction: _ => GetCachedTokenAsync(),
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

    /// <summary>
    /// Returns a valid Dataverse access token, refreshing only when the cached one is
    /// within RefreshSkew of expiry. Single-flight: concurrent callers wait on the
    /// semaphore so only one of them actually goes through DefaultAzureCredential.
    /// </summary>
    private async Task<string> GetCachedTokenAsync()
    {
        // Fast path — no lock needed when the cached token is still fresh.
        if (_cachedToken.HasValue && _cachedToken.Value.ExpiresOn - DateTimeOffset.UtcNow > RefreshSkew)
            return _cachedToken.Value.Token;

        await _tokenLock.WaitAsync();
        try
        {
            // Re-check inside the lock — another caller may have refreshed already.
            if (_cachedToken.HasValue && _cachedToken.Value.ExpiresOn - DateTimeOffset.UtcNow > RefreshSkew)
                return _cachedToken.Value.Token;

            _logger.LogInformation(
                "EventName={EventName} Reason={Reason}",
                "DataverseTokenRefresh", _cachedToken.HasValue ? "ExpiringSoon" : "Initial");

            _cachedToken = await _credential!.GetTokenAsync(
                new TokenRequestContext(new[] { _scope! }));

            _logger.LogInformation(
                "EventName={EventName} ExpiresOn={ExpiresOn:o}",
                "DataverseTokenRefreshed", _cachedToken.Value.ExpiresOn);

            return _cachedToken.Value.Token;
        }
        finally
        {
            _tokenLock.Release();
        }
    }

    public void Dispose()
    {
        _client?.Dispose();
        _tokenLock.Dispose();
    }
}
