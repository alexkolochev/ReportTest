using nietras.SeparatedValues;
using ReportTest.Aggregation;
using ReportTest.Models;
using System.Collections.Concurrent;
using System.Globalization;

namespace ReportTest;

public static class Parser
{
    public static (List<string> Files, int? Points, string? OutputDirectory,
        DateTimeOffset? DateFrom, DateTimeOffset? DateTo, DateTimeOffset? DateFromTable, DateTimeOffset? DateToTable) ParseArgs(string[] args)
    {
        var files = new List<string>();
        int? points = 100;
        string? output = null;
        DateTimeOffset? dateFrom = null;
        DateTimeOffset? dateTo = null;
        DateTimeOffset? dateFromTable = null;
        DateTimeOffset? dateToTable = null;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--points" when i + 1 < args.Length:
                    points = int.Parse(args[++i]);
                    break;
                case "--output-directory" when i + 1 < args.Length:
                    output = args[++i];
                    break;
                case "--date-from" when i + 1 < args.Length:
                    dateFrom = ParseDateArg(args[++i], "--date-from");
                    break;
                case "--date-to" when i + 1 < args.Length:
                    dateTo = ParseDateArg(args[++i], "--date-to");
                    break;
                case "--date-from-table" when i + 1 < args.Length:
                    dateFromTable = ParseDateArg(args[++i], "--date-from-table");
                    break;
                case "--date-to-table" when i + 1 < args.Length:
                    dateToTable = ParseDateArg(args[++i], "--date-to-table");
                    break;
                default:
                    files.Add(args[i]);
                    break;
            }
        }
        return (files, points, output, dateFrom, dateTo, dateFromTable, dateToTable);
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

    public static async Task<(long WithUrl, long NoUrl)> ProcessFilesAsync(List<string> filePaths,
        ConcurrentDictionary<string, StreamingStats> statsWithUrl,
        ConcurrentDictionary<string, StreamingStats> statsNoUrl,
        TimeSeriesAggregator tsAggregator, ResponseCodeTimeSeriesAggregator rcAggregator, ResponseCodeSummary rcSummary,
        long dateFromMs, long dateToMs, long dateFromTableMs, long dateToTableMs)
    {
        var tasks = filePaths.Select(file => ProcessSingleFileAsync(file, statsWithUrl, statsNoUrl, tsAggregator, rcAggregator, rcSummary,
            dateFromMs, dateToMs, dateFromTableMs, dateToTableMs));
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

    public static async Task<(long WithUrl, long NoUrl)> ProcessSingleFileAsync(string filePath,
        ConcurrentDictionary<string, StreamingStats> statsWithUrl,
        ConcurrentDictionary<string, StreamingStats> statsNoUrl,
        TimeSeriesAggregator tsAggregator, ResponseCodeTimeSeriesAggregator rcAggregator, ResponseCodeSummary rcSummary,
        long dateFromMs, long dateToMs, long dateFromTableMs, long dateToTableMs)
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
            long timeStamp = indices.timeStamp.HasValue ? row[indices.timeStamp.Value].Parse<long>() : 0;
            bool inTableRange = !indices.timeStamp.HasValue || (timeStamp >= dateFromTableMs && timeStamp <= dateToTableMs);
            if (inTableRange)
            {
                if (hasUrl) withUrl++;
                else noUrl++;
            }

            UpdateStats(row, indices, statsWithUrl, statsNoUrl, tsAggregator, rcAggregator, rcSummary,
                dateFromMs, dateToMs, dateFromTableMs, dateToTableMs);
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
        ConcurrentDictionary<string, StreamingStats> statsWithUrl,
        ConcurrentDictionary<string, StreamingStats> statsNoUrl,
        TimeSeriesAggregator tsAggregator, ResponseCodeTimeSeriesAggregator rcAggregator, ResponseCodeSummary rcSummary,
        long dateFromMs, long dateToMs, long dateFromTableMs, long dateToTableMs)
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

        bool inTimeSeriesRange = !indices.timeStamp.HasValue || (timeStamp >= dateFromMs && timeStamp <= dateToMs);
        bool inTableRange = !indices.timeStamp.HasValue || (timeStamp >= dateFromTableMs && timeStamp <= dateToTableMs);

        if (inTableRange)
        {
            var target = hasUrl ? statsWithUrl : statsNoUrl;
            var group = target.GetOrAdd(label, _ => new StreamingStats());
            lock (group) group.Update(elapsed, success, timeStamp, idleTime, latency);
        }

        if (inTimeSeriesRange)
        {
            lock (tsAggregator) tsAggregator.AddDataPoint(label, hasUrl, timeStamp, elapsed, success);
        }

        if (indices.responseCode.HasValue && hasUrl)
        {
            string rc = row[indices.responseCode.Value].Parse<string>();

            if (inTimeSeriesRange)
                lock (rcAggregator) rcAggregator.AddDataPoint(rc, timeStamp);
            if (inTableRange)
                lock (rcSummary) rcSummary.Add(rc);
        }
    }

    public static async Task<(long MinTimeMs, long MaxTimeMs)> ComputeTimeRangeAsync(List<string> filePaths)
    {
        var tasks = filePaths.Select(ComputeTimeRangeSingleFileAsync);
        var ranges = await Task.WhenAll(tasks);

        long min = long.MaxValue;
        long max = long.MinValue;
        foreach (var r in ranges)
        {
            if (r.MinTimeMs == long.MaxValue && r.MaxTimeMs == long.MinValue) continue;
            min = Math.Min(min, r.MinTimeMs);
            max = Math.Max(max, r.MaxTimeMs);
        }

        if (min == long.MaxValue || max == long.MinValue)
            return (0, 0);

        return (min, max);
    }

    private static async Task<(long MinTimeMs, long MaxTimeMs)> ComputeTimeRangeSingleFileAsync(string filePath)
    {
        long min = long.MaxValue;
        long max = long.MinValue;

        await using var stream = File.OpenRead(filePath);
        using var reader = Sep.Reader(o => o with
        {
            HasHeader = true,
            Trim = SepTrim.All,
            InitialBufferLength = 1024 * 128
        }).From(stream);

        var header = reader.Header;
        var indices = GetIndicesSafe(header);
        if (!indices.timeStamp.HasValue) return (min, max);

        foreach (var row in reader)
        {
            long timeStamp = row[indices.timeStamp.Value].Parse<long>();
            min = Math.Min(min, timeStamp);
            max = Math.Max(max, timeStamp);
        }

        return (min, max);
    }

    public static (long DateFromMs, long DateToMs, long DateFromTableMs, long DateToTableMs) NormalizeDateRanges(
        DateTimeOffset? dateFrom, DateTimeOffset? dateTo, DateTimeOffset? dateFromTable, DateTimeOffset? dateToTable,
        long minTimeMs, long maxTimeMs)
    {
        long min = minTimeMs == long.MaxValue ? 0 : minTimeMs;
        long max = maxTimeMs == long.MinValue ? 0 : maxTimeMs;

        long dateFromMs = dateFrom.HasValue ? ToUnixMs(dateFrom.Value) : min;
        long dateToMs = dateTo.HasValue ? ToUnixMs(dateTo.Value) : max;

        if (dateFromMs < min) dateFromMs = min;
        if (dateToMs > max) dateToMs = max;
        if (dateFromMs > dateToMs) dateFromMs = dateToMs;

        long dateFromTableMs = dateFromTable.HasValue ? ToUnixMs(dateFromTable.Value) : dateFromMs;
        long dateToTableMs = dateToTable.HasValue ? ToUnixMs(dateToTable.Value) : dateToMs;

        if (dateFromTableMs < dateFromMs) dateFromTableMs = dateFromMs;
        if (dateToTableMs > dateToMs) dateToTableMs = dateToMs;
        if (dateFromTableMs > dateToTableMs) dateFromTableMs = dateToTableMs;

        return (dateFromMs, dateToMs, dateFromTableMs, dateToTableMs);
    }

    private static DateTimeOffset ParseDateArg(string value, string argName)
    {
        const string format = "dd-MM-yyyy HH:mm:ss";
        if (!DateTime.TryParseExact(value, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
            throw new ArgumentException($"Invalid date for {argName}. Expected format: {format}");
        var tz = TimeZoneInfo.Local;
        var offset = tz.GetUtcOffset(dt);
        return new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), offset);
    }

    private static long ToUnixMs(DateTimeOffset dt)
    {
        return dt.ToUnixTimeMilliseconds();
    }

    public static void PrintResults(ConcurrentDictionary<string, StreamingStats> statsWithUrl,
        ConcurrentDictionary<string, StreamingStats> statsNoUrl)
    {
        PrintResultsSection("With URL", statsWithUrl);
        Console.WriteLine();
        PrintResultsSection("No URL", statsNoUrl);
    }

    private static void PrintResultsSection(string title, ConcurrentDictionary<string, StreamingStats> stats)
    {
        Console.WriteLine($"--- {title} ---");
        Console.WriteLine("{0,-20} {1,8} {2,6:F1}% {3,6:F0} {4,8:F0} {5,8:F0} {6,8:F0} {7,8:F0} {8,8:F0} {9,8:F0} {10,8:F0} {11,8:F0} {12,8:F0}",
            "Label", "Count", "Succ%", "RPS", "Mean", "Median", "P90", "P95", "P99", "P99.5", "P99.95", "IdleAvg", "LatAvg");

        foreach (var kv in stats.OrderBy(x => x.Key))
        {
            var s = kv.Value;
            Console.WriteLine("{0,-20} {1,8:N0} {2,6:F1} {3,6:F0} {4,8:F0} {5,8:F0} {6,8:F0} {7,8:F0} {8,8:F0} {9,8:F0} {10,8:F0} {11,8:F0} {12,8:F0}",
                kv.Key, s.Count, s.SuccessRate, s.RPS, s.Mean, s.Median, s.P90, s.P95, s.P99, s.P995, s.P9995, s.IdleTimeAvg, s.LatencyAvg);
        }
    }

    public static List<List<string>> BuildStatsCsvRows(ConcurrentDictionary<string, StreamingStats> stats)
    {
        var rows = new List<List<string>>
        {
            new()
            {
                "Label", "Count", "SuccessRate", "ErrorRate", "SuccessCount", "ErrorCount", "MinElapsed", "MaxElapsed", "MinTimeStamp", "MaxTimeStamp", "RPS", "Mean", "Median", "P90", "P95", "P99", "P99.5", "P99.95", "IdleAvg", "LatencyAvg", "StdDev", "CV", "DurationSeconds"
            }
        };

        foreach (var kv in stats.OrderBy(x => x.Key))
        {
            var s = kv.Value;
            rows.Add(new List<string>
            {
                kv.Key,
                s.Count.ToString("N0", CultureInfo.InvariantCulture),
                s.SuccessRate.ToString("F1", CultureInfo.InvariantCulture),
                s.ErrorRate.ToString("F1", CultureInfo.InvariantCulture),
                s.SuccessCount.ToString("N0", CultureInfo.InvariantCulture),
                s.ErrorCount.ToString("N0", CultureInfo.InvariantCulture),
                s.MinElapsed.ToString("F0", CultureInfo.InvariantCulture),
                s.MaxElapsed.ToString("F0", CultureInfo.InvariantCulture),
                s.MinTimeStamp.ToString("N0", CultureInfo.InvariantCulture),
                s.MaxTimeStamp.ToString("N0", CultureInfo.InvariantCulture),
                s.RPS.ToString("F2", CultureInfo.InvariantCulture),
                s.Mean.ToString("F0", CultureInfo.InvariantCulture),
                s.Median.ToString("F0", CultureInfo.InvariantCulture),
                s.P90.ToString("F0", CultureInfo.InvariantCulture),
                s.P95.ToString("F0", CultureInfo.InvariantCulture),
                s.P99.ToString("F0", CultureInfo.InvariantCulture),
                s.P995.ToString("F0", CultureInfo.InvariantCulture),
                s.P9995.ToString("F0", CultureInfo.InvariantCulture),
                s.IdleTimeAvg.ToString("F0", CultureInfo.InvariantCulture),
                s.LatencyAvg.ToString("F0", CultureInfo.InvariantCulture),
                s.StdDev.ToString("F0", CultureInfo.InvariantCulture),
                s.CV.ToString("F0", CultureInfo.InvariantCulture),
                s.DurationSeconds.ToString("F0", CultureInfo.InvariantCulture)
            });
        }

        return rows;
    }
}
