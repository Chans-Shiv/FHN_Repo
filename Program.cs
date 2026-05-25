using Azure.Identity;
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
        var settings = new SyncSettings
        {
            SqlConnectionString = GetRequired("SqlConnectionString"),
            DataverseUrl = GetRequired("DataverseUrl"),
            SqlBatchSize = TryParseInt("SqlBatchSize", 10_000),
            DataverseBatchSize = TryParseInt("DataverseBatchSize", 1_000),
            MaxParallelBatches = TryParseInt("MaxParallelBatches", 5),
            PreWarmParallelism = TryParseInt("PreWarmParallelism", 10),
            MaxConsecutiveFailureDays = TryParseInt("MaxConsecutiveFailureDays", 3),
            TrackingStorageAccountUrl = GetRequired("TrackingStorageAccountUrl"),
            TrackingContainerName = Environment.GetEnvironmentVariable("TrackingContainerName") ?? "sync-state",
            TrackingBlobName = Environment.GetEnvironmentVariable("TrackingBlobName") ?? "tracking.json",
            ErrorTableEntityName = Environment.GetEnvironmentVariable("ErrorTableEntityName") ?? "crbee_consumercredit_errortable",
            DeadLetterQueueName = Environment.GetEnvironmentVariable("DeadLetterQueueName") ?? "dead-letter-errors",
        };
        services.AddSingleton(settings);

        // Storage Queue client used by QueueDeadLetterService to enqueue failed records.
        // Endpoint is derived from TrackingStorageAccountUrl by swapping blob→queue, so
        // tracking blob + dead-letter queue live on the same account (one identity grant).
        // The QueueTrigger function reads from the SAME queue via the
        // AzureWebJobsStorage connection — that must point at this same account.
        services.AddSingleton(_ =>
        {
            var queueServiceUri = new Uri(
                settings.TrackingStorageAccountUrl
                    .Replace(".blob.core.windows.net", ".queue.core.windows.net"));
            var serviceClient = new QueueServiceClient(queueServiceUri, new DefaultAzureCredential());
            return serviceClient.GetQueueClient(settings.DeadLetterQueueName);
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

static int TryParseInt(string key, int defaultValue) =>
    int.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : defaultValue;
