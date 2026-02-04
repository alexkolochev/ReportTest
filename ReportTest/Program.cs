using ReportTest;
using ReportTest.Aggregation;
using ReportTest.IO;
using ReportTest.Models;
using System.Collections.Concurrent;

if (args.Length == 0)
{
    Console.WriteLine("Usage: dotnet run <csv files...> [--points N] [--output-directory dir] " +
                      "[--date-from \"dd-MM-yyyy HH:mm:ss\"] [--date-to \"dd-MM-yyyy HH:mm:ss\"] " +
                      "[--date-from-table \"dd-MM-yyyy HH:mm:ss\"] [--date-to-table \"dd-MM-yyyy HH:mm:ss\"]");
    return;
}

var (Files, Points, OutputDirectory, DateFrom, DateTo, DateFromTable, DateToTable) = Parser.ParseArgs(args);
var filePaths = Parser.GetFilePaths(Files);

var statsWithUrl = new ConcurrentDictionary<string, StreamingStats>();
var statsNoUrl = new ConcurrentDictionary<string, StreamingStats>();
var tsAggregator = new TimeSeriesAggregator();
var rcAggregator = new ResponseCodeTimeSeriesAggregator();
var rcSummary = new ResponseCodeSummary();
Console.WriteLine($"Processing {filePaths.Count} files...");
var (minTimeMs, maxTimeMs) = await Parser.ComputeTimeRangeAsync(filePaths);
var (dateFromMs, dateToMs, dateFromTableMs, dateToTableMs) =
    Parser.NormalizeDateRanges(DateFrom, DateTo, DateFromTable, DateToTable, minTimeMs, maxTimeMs);

var (withUrlCount, _) = await Parser.ProcessFilesAsync(
    filePaths,
    statsWithUrl,
    statsNoUrl,
    tsAggregator,
    rcAggregator,
    rcSummary,
    dateFromMs,
    dateToMs,
    dateFromTableMs,
    dateToTableMs);
var rcRows = rcAggregator.ExportToCSV(Points);
var tsSplit = tsAggregator.ExportToSeparatedCSV(Points);

string tsDir = OutputDirectory ?? "default";
if (!Directory.Exists(tsDir)) Directory.CreateDirectory(tsDir);

string withUrlAvgPath = $@"{tsDir}\withUrl_avgElapsed.csv";
string withUrlRpsPath = $@"{tsDir}\withUrl_rps.csv";
string noUrlAvgPath = $@"{tsDir}\noUrl_avgElapsed.csv";
string noUrlRpsPath = $@"{tsDir}\noUrl_rps.csv";
string rcPath = $@"{tsDir}\responseCodes_rps.csv";
string statsWithUrlPath = $@"{tsDir}\stats_withUrl.csv";
string statsNoUrlPath = $@"{tsDir}\stats_noUrl.csv";
string summaryPath = $@"{tsDir}\responseCodes_summary.csv";

CsvWriter.WriteRows(withUrlAvgPath, tsSplit.WithUrlAvgElapsed);
CsvWriter.WriteRows(withUrlRpsPath, tsSplit.WithUrlRps);
CsvWriter.WriteRows(noUrlAvgPath, tsSplit.NoUrlAvgElapsed);
CsvWriter.WriteRows(noUrlRpsPath, tsSplit.NoUrlRps);
CsvWriter.WriteRows(rcPath, rcRows);
File.WriteAllText(summaryPath, rcSummary.ToCsv(withUrlCount));
CsvWriter.WriteRows(statsWithUrlPath, Parser.BuildStatsCsvRows(statsWithUrl));
CsvWriter.WriteRows(statsNoUrlPath, Parser.BuildStatsCsvRows(statsNoUrl));
