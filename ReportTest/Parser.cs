using nietras.SeparatedValues;
using ReportTest.Aggregation;
using ReportTest.Models;
using System.Collections.Concurrent;

namespace ReportTest;

public static class Parser
{
    public static (List<string> Files, int? Points, string? TimeSeriesOutput) ParseArgs(string[] args)
    {
        var files = new List<string>();
        int? points = 100;
        string? output = null;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--points" when i + 1 < args.Length:
                    points = int.Parse(args[++i]);
                    break;
                case "--output-timeseries" when i + 1 < args.Length:
                    output = args[++i];
                    break;
                default:
                    files.Add(args[i]);
                    break;
            }
        }
        return (files, points, output);
    }

    public static List<string> GetFilePaths(List<string> args)
    {
        var files = new List<string>();
        foreach (string arg in args)
        {
            if (Directory.Exists(arg))
                files.AddRange(Directory.EnumerateFiles(arg, "*.csv"));
            else if (File.Exists(arg))
                files.Add(arg);
        }
        return files;
    }

    public static async Task<(long WithUrl, long NoUrl)> ProcessFilesAsync(List<string> filePaths, ConcurrentDictionary<string, StreamingStats> stats,
        TimeSeriesAggregator tsAggregator, ResponseCodeTimeSeriesAggregator rcAggregator, ResponseCodeSummary rcSummary)
    {
        var tasks = filePaths.Select(file => ProcessSingleFileAsync(file, stats, tsAggregator, rcAggregator, rcSummary));
        var counts = await Task.WhenAll(tasks);
        long withUrl = 0;
        long noUrl = 0;
        foreach (var c in counts)
        {
            withUrl += c.WithUrl;
            noUrl += c.NoUrl;
        }
        return (withUrl, noUrl);
    }

    public static async Task<(long WithUrl, long NoUrl)> ProcessSingleFileAsync(string filePath, ConcurrentDictionary<string, StreamingStats> stats,
        TimeSeriesAggregator tsAggregator, ResponseCodeTimeSeriesAggregator rcAggregator, ResponseCodeSummary rcSummary)
    {
        long withUrl = 0;
        long noUrl = 0;
        await using var stream = File.OpenRead(filePath);
        using var reader = Sep.Reader(o => o with
        {
            HasHeader = true,
            Trim = SepTrim.All,
            InitialBufferLength = 1024 * 128
        }).From(stream);

        var header = reader.Header;
        var indices = GetIndicesSafe(header);

        foreach (var row in reader)
        {
            string? urlStr = indices.url.HasValue ? row[indices.url.Value].ToString() : null;
            bool hasUrl = indices.url.HasValue &&
                          !string.IsNullOrWhiteSpace(urlStr) &&
                          urlStr != "null";
            if (hasUrl) withUrl++;
            else noUrl++;
            UpdateStats(row, indices, stats, tsAggregator, rcAggregator, rcSummary);
        }

        return (withUrl, noUrl);
    }

    public static (int? label, int? elapsed, int? success, int? url, int? timeStamp, int? responseCode, int? idleTime, int? latency) GetIndicesSafe(SepReaderHeader header)
    {
        return (
            header.IndexOf("label"),
            header.IndexOf("elapsed"),
            header.IndexOf("success"),
            header.IndexOf("URL"),
            header.IndexOf("timeStamp"),
            header.IndexOf("responseCode"),
            FindIndex(header, "IdleTime", "idleTime"),
            FindIndex(header, "Latency", "latency")
        );
    }

    private static int? FindIndex(SepReaderHeader header, params string[] names)
    {
        foreach (var name in names)
        {
            int index = header.IndexOf(name);
            if (index >= 0) return index;
        }
        return null;
    }

    public static void UpdateStats(SepReader.Row row, (int? label, int? elapsed, int? success, int? url, int? timeStamp, int? responseCode, int? idleTime, int? latency) indices,
        ConcurrentDictionary<string, StreamingStats> stats, TimeSeriesAggregator tsAggregator, ResponseCodeTimeSeriesAggregator rcAggregator, ResponseCodeSummary rcSummary)
    {
        if (!indices.elapsed.HasValue) return;

        long elapsed = row[indices.elapsed.Value].Parse<long>();
        long idleTime = indices.idleTime.HasValue ? row[indices.idleTime.Value].Parse<long>() : 0;
        long latency = indices.latency.HasValue ? row[indices.latency.Value].Parse<long>() : 0;
        long timeStamp = indices.timeStamp.HasValue ? row[indices.timeStamp.Value].Parse<long>() : 0;
        string label = indices.label.HasValue ? row[indices.label.Value].ToString() ?? "unknown" : "unknown";
        bool success = indices.success.HasValue ? row[indices.success.Value].Parse<bool>() : true;
        string? urlStr = indices.url.HasValue ? row[indices.url.Value].ToString() : null;
        bool hasUrl = indices.url.HasValue &&
                      !string.IsNullOrWhiteSpace(urlStr) &&
                      urlStr != "null";

        string key = $"{label}_{(hasUrl ? "withUrl" : "noUrl")}";
        var group = stats.GetOrAdd(key, _ => new StreamingStats());
        lock (group) group.Update(elapsed, success, timeStamp, idleTime, latency);

        lock (tsAggregator) tsAggregator.AddDataPoint(label, hasUrl, timeStamp, elapsed, success);

        if (indices.responseCode.HasValue && hasUrl)
        {
            string rc = row[indices.responseCode.Value].Parse<string>();
            
            lock (rcAggregator) rcAggregator.AddDataPoint(rc, timeStamp);
            lock (rcSummary) rcSummary.Add(rc);
        }
    }

    public static void PrintResults(ConcurrentDictionary<string, StreamingStats> stats)
    {
        Console.WriteLine("{0,-20} {1,8} {2,6:F1}% {3,6:F0} {4,8:F0} {5,8:F0} {6,8:F0} {7,8:F0} {8,8:F0}",
            "Label/Url", "Count", "Succ%", "RPS", "Mean", "P90", "P95", "IdleAvg", "LatAvg");

        foreach (var kv in stats.OrderBy(x => x.Key))
        {
            var s = kv.Value;
            Console.WriteLine("{0,-20} {1,8:N0} {2,6:F1} {3,6:F0} {4,8:F0} {5,8:F0} {6,8:F0} {7,8:F0} {8,8:F0}",
                kv.Key, s.Count, s.SuccessRate, s.RPS, s.Mean, s.P90, s.P95, s.IdleTimeAvg, s.LatencyAvg);
        }
    }
}
