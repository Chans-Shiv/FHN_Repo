using System.Data;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using SqlToDataverseSync.Configuration;
using SqlToDataverseSync.Domain.Interfaces;

namespace SqlToDataverseSync.Infrastructure.SqlMi;

public class SqlMiDataReader : ISqlDataReader
{
    private readonly SyncSettings _settings;
    private readonly ILogger<SqlMiDataReader> _logger;

    public SqlMiDataReader(SyncSettings settings, ILogger<SqlMiDataReader> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public async Task<int> GetMaxMthKeyAsync(CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_settings.SqlConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(
            "SELECT Max(MTH_KEY) FROM dbo.ConsumerCreditDataAcq", conn)
        { CommandTimeout = 30 };

        var result = await cmd.ExecuteScalarAsync(ct);
        var mthKey = result is not null && result != DBNull.Value ? Convert.ToInt32(result) : 0;
        _logger.LogInformation("Max MTH_KEY in SQL: {MthKey}", mthKey);
        return mthKey;
    }

    public async Task<long> GetRowCountForMthKeyAsync(int mthKey, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_settings.SqlConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(
            "SELECT COUNT(*) FROM dbo.ConsumerCreditDataAcq WHERE MTH_KEY = @MthKey", conn)
        { CommandTimeout = 30 };
        cmd.Parameters.AddWithValue("@MthKey", mthKey);

        var result = await cmd.ExecuteScalarAsync(ct);
        var count = result is not null ? Convert.ToInt64(result) : 0;
        _logger.LogInformation("Row count for MTH_KEY={MthKey}: {Count}", mthKey, count);
        return count;
    }

    public async IAsyncEnumerable<List<Dictionary<string, object?>>> StreamBatchesAsync(
        int mthKey, int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(_settings.SqlConnectionString);
        await connection.OpenAsync(ct);

        var query = @"
            SELECT ACCT_ID, ACCT_KEY, ACCT_NUM, MTH_KEY, Balance,
                   ChargeOffIndicator, CostCenter, CustomerName,
                   CollAddr, CollCity, CollState, CollZip,
                   CustStreetAddress, CustCity, CustState, CustZip,
                   LoanIdentifier, SourceSystem, Product
            FROM dbo.ConsumerCreditDataAcq
            WHERE MTH_KEY = @MthKey
            ORDER BY ACCT_KEY";

        await using var cmd = new SqlCommand(query, connection) { CommandTimeout = 600 };
        cmd.Parameters.AddWithValue("@MthKey", mthKey);

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);

        var columnNames = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
            columnNames[i] = reader.GetName(i);

        var batch = new List<Dictionary<string, object?>>(batchSize);
        int totalRows = 0;

        while (await reader.ReadAsync(ct))
        {
            var row = new Dictionary<string, object?>(columnNames.Length);
            for (int i = 0; i < columnNames.Length; i++)
                row[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);

            batch.Add(row);
            totalRows++;

            if (batch.Count >= batchSize)
            {
                _logger.LogInformation("SQL: Yielding batch of {Count} rows (total: {Total})",
                    batch.Count, totalRows);
                yield return batch;
                batch = new List<Dictionary<string, object?>>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            _logger.LogInformation("SQL: Yielding final batch of {Count} rows (total: {Total})",
                batch.Count, totalRows);
            yield return batch;
        }

        _logger.LogInformation("SQL: Streaming complete. Total rows: {Total}", totalRows);
    }
}
