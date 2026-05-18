using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Application.Processors;
using SqlToDataverseSync.Application.Services;
using SqlToDataverseSync.Configuration;
using SqlToDataverseSync.Domain.Interfaces;
using SqlToDataverseSync.Infrastructure.Dataverse;
using SqlToDataverseSync.Infrastructure.DeadLetter;
using SqlToDataverseSync.Infrastructure.SqlMi;
using SqlToDataverseSync.Infrastructure.Tracking;

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
            DeleteParallelism = TryParseInt("DeleteParallelism", 15),
            MaxConsecutiveFailureDays = TryParseInt("MaxConsecutiveFailureDays", 3),
            TrackingStorageAccountUrl = GetRequired("TrackingStorageAccountUrl"),
            TrackingContainerName = Environment.GetEnvironmentVariable("TrackingContainerName") ?? "sync-state",
            TrackingBlobName = Environment.GetEnvironmentVariable("TrackingBlobName") ?? "tracking.json",
        };
        services.AddSingleton(settings);

        // ── Infrastructure (external dependencies) ──
        services.AddSingleton<DataverseConnectionFactory>();
        services.AddTransient<ISqlDataReader, SqlMiDataReader>();
        services.AddTransient<IDataverseRepository, DataverseRepository>();
        services.AddTransient<ITrackingService, BlobTrackingService>();
        services.AddTransient<IDeadLetterService, DeadLetterService>();

        // ── Module Processors (add new modules here) ──
        services.AddTransient<IModuleProcessor, ForeclosureProcessor>();
        // services.AddTransient<IModuleProcessor, BankruptcyProcessor>();
        // services.AddTransient<IModuleProcessor, LossMitProcessor>();
        // services.AddTransient<IModuleProcessor, SpecialityProgramsProcessor>();
        // services.AddTransient<IModuleProcessor, UnblocksProcessor>();

        // ── Application Services ──
        services.AddTransient<SyncOrchestrator>();

        // ── Logging ──
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Information));
    })
    .Build();

await host.RunAsync();

// ── Helpers ──
static string GetRequired(string key) =>
    Environment.GetEnvironmentVariable(key)
    ?? throw new InvalidOperationException($"Missing required setting: '{key}'");

static int TryParseInt(string key, int defaultValue) =>
    int.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : defaultValue;
