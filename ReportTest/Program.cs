using ReportTest;
using ReportTest.Aggregation;
using ReportTest.IO;
using ReportTest.Models;
using System.Collections.Concurrent;

if (args.Length == 0)
{
    Console.WriteLine("Usage: dotnet run <csv files...> [--points N] [--output-timeseries path.csv]");
    return;
}

var (Files, Points, TimeSeriesOutput) = Parser.ParseArgs(args);
var filePaths = Parser.GetFilePaths(Files);

var stats = new ConcurrentDictionary<string, StreamingStats>();
var tsAggregator = new TimeSeriesAggregator();
var rcAggregator = new ResponseCodeTimeSeriesAggregator();
var rcSummary = new ResponseCodeSummary();
Console.WriteLine($"Processing {filePaths.Count} files...");
var (withUrlCount, noUrlCount) = await Parser.ProcessFilesAsync(filePaths, stats, tsAggregator, rcAggregator, rcSummary);

Console.WriteLine("\n=== Summary by Label+URL ===");
Parser.PrintResults(stats);

Console.WriteLine("\n=== Response Codes Summary ===");
rcSummary.PrintSummary(withUrlCount);
Console.WriteLine($"Requests with URL: {withUrlCount:N0}, without URL: {noUrlCount:N0}");

string tsPath = TimeSeriesOutput ?? "timeseries_label.csv";
var tsSplit = tsAggregator.ExportToSeparatedCSV(Points);

string tsBaseName = Path.GetFileNameWithoutExtension(tsPath);
string? tsDir = Path.GetDirectoryName(tsPath);
string basePath = string.IsNullOrWhiteSpace(tsDir) ? tsBaseName : Path.Combine(tsDir, tsBaseName);

string bucketSecPath = $"{basePath}.bucketsec.csv";
string withUrlAvgPath = $"{basePath}.withUrl_avgElapsed.csv";
string withUrlRpsPath = $"{basePath}.withUrl_rps.csv";
string noUrlAvgPath = $"{basePath}.noUrl_avgElapsed.csv";
string noUrlRpsPath = $"{basePath}.noUrl_rps.csv";

CsvWriter.WriteRows(bucketSecPath, tsSplit.BucketSec);
CsvWriter.WriteRows(withUrlAvgPath, tsSplit.WithUrlAvgElapsed);
CsvWriter.WriteRows(withUrlRpsPath, tsSplit.WithUrlRps);
CsvWriter.WriteRows(noUrlAvgPath, tsSplit.NoUrlAvgElapsed);
CsvWriter.WriteRows(noUrlRpsPath, tsSplit.NoUrlRps);

string rcPath = Path.ChangeExtension(tsPath, ".responseCode.csv");
var rcRows = rcAggregator.ExportToCSV(Points);
CsvWriter.WriteRows(rcPath, rcRows);

string summaryPath = "responseCodes_summary.csv";
File.WriteAllText(summaryPath, rcSummary.ToCsv(withUrlCount));

Console.WriteLine($"\n✓ Time-series BucketSec: {bucketSecPath}");
Console.WriteLine($"✓ Time-series withUrl AvgElapsed: {withUrlAvgPath}");
Console.WriteLine($"✓ Time-series withUrl RPS: {withUrlRpsPath}");
Console.WriteLine($"✓ Time-series noUrl AvgElapsed: {noUrlAvgPath}");
Console.WriteLine($"✓ Time-series noUrl RPS: {noUrlRpsPath}");
Console.WriteLine($"✓ ResponseCode RPS time-series: {rcPath}");
Console.WriteLine($"✓ Response codes summary: {summaryPath}");
