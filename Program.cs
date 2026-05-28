using System.Text.Json;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Application.Processors;
using Fhn.Cdm.DataverseSync.Application.Services;
using Fhn.Cdm.DataverseSync.Configuration;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;
using Fhn.Cdm.DataverseSync.Infrastructure.Dataverse;
using Fhn.Cdm.DataverseSync.Infrastructure.DeadLetter;
using Fhn.Cdm.DataverseSync.Infrastructure.SqlMi;
using Fhn.Cdm.DataverseSync.Infrastructure.Tracking;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        // ── Configuration ──
        // Resource-pointing values (connection strings, URLs, container/queue/table
        // names) are required — GetRequired() throws on missing so a forgotten
        // setting fails loud at startup instead of silently using a default that
        // points at the wrong resource. Tuning knobs keep their SyncSettings
        // defaults and only get overridden when explicitly set.
        var settings = new SyncSettings
        {
            SqlConnectionString = GetRequired("SqlConnectionString"),
            DataverseUrl = GetRequired("DataverseUrl"),
            TrackingStorageAccountUrl = GetRequired("TrackingStorageAccountUrl"),
            TrackingContainerName = GetRequired("TrackingContainerName"),
            TrackingBlobName = GetRequired("TrackingBlobName"),
            ErrorTableEntityName = GetRequired("ErrorTableEntityName"),
            DeadLetterQueueName = GetRequired("DeadLetterQueueName"),
            FailureBlobContainerName = GetRequired("FailureBlobContainerName"),
            // Pulled from host.json below — see ReadHostJsonMaxDequeueCount.
            DeadLetterMaxAttempts = ReadHostJsonMaxDequeueCount(),
        };
        OverrideInt("SqlBatchSize", v => settings.SqlBatchSize = v);
        OverrideInt("DataverseBatchSize", v => settings.DataverseBatchSize = v);
        OverrideInt("MaxParallelBatches", v => settings.MaxParallelBatches = v);
        OverrideInt("PreWarmParallelism", v => settings.PreWarmParallelism = v);
        OverrideInt("MaxConsecutiveFailureDays", v => settings.MaxConsecutiveFailureDays = v);
        services.AddSingleton(settings);

        // Identity-only storage auth. In Azure the deployed function's Managed
        // Identity picks up the storage roles; locally `az login` / VS sign-in
        // supplies the credential. ManagedIdentityCredential is excluded only in
        // local dev so we don't waste time probing an IMDS endpoint that doesn't
        // exist on a dev box (and can't accidentally pick up a stray VM/proxy
        // identity that lacks the storage RBAC roles).
        var storageCredential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
        {
            ExcludeManagedIdentityCredential = IsLocalDev()
        });

        // Storage Queue client used by QueueDeadLetterService to enqueue failed records.
        // Endpoint is derived from TrackingStorageAccountUrl by swapping blob→queue, so
        // tracking blob + dead-letter queue live on the same account (one identity grant).
        //
        // MessageEncoding = Base64: the WebJobs QueueTrigger extension expects
        // base64-encoded message bodies by default. Azure.Storage.Queues v12+ ships
        // with MessageEncoding=None, so without this option the consumer logs
        // "Message decoding has failed!" and the message moves to poison after 5
        // delivery attempts. Setting Base64 here makes the producer match the trigger.
        services.AddSingleton(_ =>
        {
            var queueServiceUri = new Uri(
                settings.TrackingStorageAccountUrl
                    .Replace(".blob.core.windows.net", ".queue.core.windows.net"));
            var serviceClient = new QueueServiceClient(
                queueServiceUri,
                storageCredential,
                new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });
            return serviceClient.GetQueueClient(settings.DeadLetterQueueName);
        });

        // Blob container holding the durable archive of records that exhausted all
        // queue retries (one blob per record, date-partitioned). Same storage account
        // as tracking/queue — single identity grant covers everything.
        services.AddSingleton(_ =>
        {
            var blobServiceClient = new BlobServiceClient(
                new Uri(settings.TrackingStorageAccountUrl), storageCredential);
            return blobServiceClient.GetBlobContainerClient(settings.FailureBlobContainerName);
        });

        // ── Infrastructure (external dependencies) ──
        services.AddSingleton<DataverseConnectionFactory>();
        services.AddTransient<ISqlDataReader, SqlMiDataReader>();
        services.AddTransient<IDataverseRepository, DataverseRepository>();
        services.AddTransient<ITrackingService, BlobTrackingService>();

        // Dead-letter pipeline:
        //   Orchestrator → IDeadLetterService (QueueDeadLetterService) → Storage Queue
        //   QueueTrigger fn (DeadLetterProcessorFunction) → DataverseErrorTableService → Dataverse
        services.AddTransient<IDeadLetterService, QueueDeadLetterService>();
        services.AddTransient<DataverseErrorTableService>();
        services.AddTransient<FailureBlobWriter>();

        // ── Module Processors (add new modules here) ──
        services.AddTransient<IModuleProcessor, ForeclosureProcessor>();
        services.AddTransient<IModuleProcessor, BankruptcyProcessor>();
        services.AddTransient<IModuleProcessor, LossMitProcessor>();
        services.AddTransient<IModuleProcessor, SpecialityProgramsProcessor>();
        services.AddTransient<IModuleProcessor, UnblocksProcessor>();

        // ── Application Services ──
        services.AddTransient<SyncOrchestrator>();

        // ── Logging ──
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Information));

        // ── Application Insights ──
        // No-ops cleanly when APPLICATIONINSIGHTS_CONNECTION_STRING isn't set;
        // telemetry will start flowing the moment the env var is populated
        // (no code change needed). Until then logs continue via the Functions
        // host's console/file sinks as before.
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();

        // The Functions worker installs a default LoggerFilterRule that caps
        // the App Insights logger at LogLevel.Warning, which would silently
        // drop our EventName=... Information events. Remove that rule so the
        // structured log pipeline reaches App Insights fully.
        services.Configure<LoggerFilterOptions>(options =>
        {
            var aiRule = options.Rules.FirstOrDefault(rule =>
                rule.ProviderName == "Microsoft.Extensions.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider");
            if (aiRule is not null)
                options.Rules.Remove(aiRule);
        });
    })
    .Build();

await host.RunAsync();

// ── Helpers ──
static string GetRequired(string key) =>
    Environment.GetEnvironmentVariable(key)
    ?? throw new InvalidOperationException($"Missing required setting: '{key}'");

// Optional tuning overrides — only invoke the assignment when the env var is
// set, so the property initializer in SyncSettings remains the source of truth
// for the default value.
static void OverrideInt(string key, Action<int> assign)
{
    if (int.TryParse(Environment.GetEnvironmentVariable(key), out var v)) assign(v);
}

// Core Tools sets AZURE_FUNCTIONS_ENVIRONMENT=Development for `func start`; the
// deployed Function App leaves it unset (or set to Production), so this cleanly
// separates the two without inspecting connection-string shape.
static bool IsLocalDev() =>
    string.Equals(
        Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT"),
        "Development",
        StringComparison.OrdinalIgnoreCase);

// Reads extensions.queues.maxDequeueCount from host.json so DeadLetterMaxAttempts
// is sourced from the same file the Functions runtime uses — no chance for the
// two to drift. Falls back to 5 (the Functions runtime's own default) only when
// host.json doesn't specify the key.
static int ReadHostJsonMaxDequeueCount()
{
    const int RuntimeDefault = 5;
    var path = Path.Combine(AppContext.BaseDirectory, "host.json");
    if (!File.Exists(path)) return RuntimeDefault;

    using var doc = JsonDocument.Parse(File.ReadAllText(path));
    if (doc.RootElement.TryGetProperty("extensions", out var ext)
        && ext.TryGetProperty("queues", out var queues)
        && queues.TryGetProperty("maxDequeueCount", out var max)
        && max.TryGetInt32(out var value))
    {
        return value;
    }
    return RuntimeDefault;
}
