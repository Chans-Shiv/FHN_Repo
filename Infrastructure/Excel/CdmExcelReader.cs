using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using Microsoft.Extensions.Logging;
using Fhn.Cdm.DataverseSync.Diagnostics;
using Fhn.Cdm.DataverseSync.Domain.Interfaces;

namespace Fhn.Cdm.DataverseSync.Infrastructure.Excel;

/// <summary>
/// Reads the CDM snapshot from an .xlsx workbook, replacing the old SQL MI source.
///
/// The header row's column names match the SQL projection 1:1 (ACCT_ID, ACCT_KEY,
/// ACCT_NUM, MTH_KEY, Balance, ChargeOffIndicator, CostCenter, CustomerName, CollAddr,
/// CollCity, CollState, CollZip, CustStreetAddress, CustCity, CustState, CustZip,
/// LoanIdentifier, SourceSystem, Product) so <c>CommonTransformation</c> maps them
/// unchanged — every downstream phase (pre-warm, module processing, dead-letter)
/// behaves exactly as it did against the database.
///
/// Streaming design mirrors the Originate reader: shared strings are loaded once into a
/// flat array and the sheet is walked with <see cref="OpenXmlReader"/> so peak memory
/// stays flat regardless of file size (never materializes the whole sheet DOM).
/// </summary>
public sealed class CdmExcelReader : IExcelDataReader
{
    private readonly ILogger<CdmExcelReader> _logger;

    public CdmExcelReader(ILogger<CdmExcelReader> logger) => _logger = logger;

    /// <summary>
    /// Yields raw ACCT_NUM values one at a time. Used to build the pre-warm match-key
    /// set without holding full rows — the Excel equivalent of the SQL key-stream pass.
    /// </summary>
    public IEnumerable<string> StreamAccountNumbers(string xlsxPath)
    {
        foreach (var row in StreamRawRows(xlsxPath))
        {
            if (row.TryGetValue("ACCT_NUM", out var raw) && !string.IsNullOrEmpty(raw))
                yield return raw!;
        }
    }

    /// <summary>
    /// Yields the sheet in batches of <paramref name="batchSize"/> rows, each row a
    /// dictionary keyed by column name — the same shape the SQL reader produced, so
    /// <c>CommonTransformation.TransformBatch</c> consumes it without change.
    /// </summary>
    public IEnumerable<List<Dictionary<string, object?>>> StreamBatches(string xlsxPath, int batchSize)
    {
        var batch = new List<Dictionary<string, object?>>(batchSize);
        int totalRows = 0;

        foreach (var row in StreamRawRows(xlsxPath))
        {
            // Widen string values to object? so the payload matches the old SQL row shape
            // (Dictionary<string, object?>) that CommonTransformation.GetString/GetInt/GetDecimal expect.
            var typed = new Dictionary<string, object?>(row.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var kv in row) typed[kv.Key] = kv.Value;

            batch.Add(typed);
            totalRows++;

            if (batch.Count >= batchSize)
            {
                _logger.LogInformation(
                    "EventName={EventName} BatchRows={Count} TotalRows={Total} IsFinal={IsFinal}",
                    LogEvents.ExcelBatchYielded, batch.Count, totalRows, false);
                yield return batch;
                batch = new List<Dictionary<string, object?>>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            _logger.LogInformation(
                "EventName={EventName} BatchRows={Count} TotalRows={Total} IsFinal={IsFinal}",
                LogEvents.ExcelBatchYielded, batch.Count, totalRows, true);
            yield return batch;
        }

        _logger.LogInformation(
            "EventName={EventName} TotalRows={Total}",
            LogEvents.ExcelStreamComplete, totalRows);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Core streaming — walks the first worksheet row by row, mapping each
    // data row to a { columnName → value } dictionary. Empty rows skipped.
    // ═══════════════════════════════════════════════════════════════════
    private IEnumerable<Dictionary<string, string?>> StreamRawRows(string xlsxPath)
    {
        _logger.LogInformation("EventName={EventName} Path={Path}", LogEvents.ExcelReadStarted, xlsxPath);

        using var doc = SpreadsheetDocument.Open(xlsxPath, false);
        var wbPart = doc.WorkbookPart ?? throw new InvalidDataException("Workbook part missing.");
        var sheet = wbPart.Workbook.Descendants<Sheet>().FirstOrDefault()
            ?? throw new InvalidDataException("No sheet found in workbook.");
        var wsPart = (WorksheetPart)wbPart.GetPartById(sheet.Id!);

        // Resolve shared strings into a flat array ONCE by streaming the part — touching
        // SharedStringTablePart.SharedStringTable would materialize the whole table as a DOM
        // (hundreds of MB on a large file) and make every text-cell lookup O(n).
        var sharedStrings = LoadSharedStrings(wbPart);

        using var reader = OpenXmlReader.Create(wsPart);
        var headers = new Dictionary<int, string>();
        int rowNum = 0;

        while (reader.Read())
        {
            if (reader.ElementType != typeof(Row) || !reader.IsStartElement) continue;

            rowNum++;
            var values = ReadRowValues(reader, sharedStrings);

            if (rowNum == 1)
            {
                foreach (var kv in values)
                    headers[kv.Key] = (kv.Value ?? "").Trim();
                _logger.LogInformation("Excel header row: {Count} columns [{Headers}]",
                    headers.Count, string.Join(", ", headers.Values.Where(h => !string.IsNullOrWhiteSpace(h))));
                continue;
            }

            if (values.Count == 0 || values.All(v => string.IsNullOrWhiteSpace(v.Value)))
                continue;

            var row = new Dictionary<string, string?>(headers.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var (colIdx, val) in values)
            {
                if (!headers.TryGetValue(colIdx, out var header) || string.IsNullOrWhiteSpace(header)) continue;
                row[header] = val;
            }
            yield return row;
        }
    }

    private static Dictionary<int, string?> ReadRowValues(OpenXmlReader reader, string[] sharedStrings)
    {
        var result = new Dictionary<int, string?>();
        while (reader.Read())
        {
            if (reader.ElementType == typeof(Row) && reader.IsEndElement) break;
            if (reader.ElementType != typeof(Cell) || !reader.IsStartElement) continue;

            var cell = (Cell)reader.LoadCurrentElement()!;
            if (cell.CellReference?.Value is null) continue;
            var colIdx = ColRefToIndex(cell.CellReference!.Value!);

            string? value = cell.CellValue?.InnerText;
            if (cell.DataType?.Value == CellValues.SharedString
                && int.TryParse(value, out var sIdx) && sIdx >= 0 && sIdx < sharedStrings.Length)
            {
                value = sharedStrings[sIdx];
            }
            else if (cell.DataType?.Value == CellValues.InlineString)
            {
                value = cell.InlineString?.InnerText;
            }
            else if (cell.DataType?.Value == CellValues.Boolean)
            {
                value = value == "1" ? "true" : "false";
            }

            result[colIdx] = value;
        }
        return result;
    }

    // Streams sharedStrings.xml into a flat array (index → value) without building the
    // full SharedStringTable DOM: each item is loaded, its text copied out, then discarded.
    private static string[] LoadSharedStrings(WorkbookPart wbPart)
    {
        var part = wbPart.SharedStringTablePart;
        if (part is null) return Array.Empty<string>();

        var list = new List<string>();
        using var reader = OpenXmlReader.Create(part);
        while (reader.Read())
        {
            if (reader.ElementType == typeof(SharedStringItem) && reader.IsStartElement)
                list.Add(reader.LoadCurrentElement()!.InnerText);
        }
        return list.ToArray();
    }

    private static int ColRefToIndex(string cellRef)
    {
        int idx = 0;
        foreach (var c in cellRef)
        {
            if (c < 'A' || c > 'Z') break;
            idx = idx * 26 + (c - 'A' + 1);
        }
        return idx;
    }
}
