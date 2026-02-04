using ReportTest.Models;
using System.Collections.Concurrent;

namespace ReportTest.Aggregation;

public class TimeSeriesSplit
{
    public List<List<string>> BucketSec { get; init; } = new();
    public List<List<string>> WithUrlAvgElapsed { get; init; } = new();
    public List<List<string>> WithUrlRps { get; init; } = new();
    public List<List<string>> NoUrlAvgElapsed { get; init; } = new();
    public List<List<string>> NoUrlRps { get; init; } = new();
}

public class TimeSeriesAggregator
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<long, BucketData>> _rawBuckets = new();
    private long _globalMinTime = long.MaxValue;
    private long _globalMaxTime;

    public void AddDataPoint(string label, bool hasUrl, long timeStamp, long elapsed, bool success)
    {
        _globalMinTime = Math.Min(_globalMinTime, timeStamp);
        _globalMaxTime = Math.Max(_globalMaxTime, timeStamp);

        string key = $"{label}_{(hasUrl ? "withUrl" : "noUrl")}";
        var buckets = _rawBuckets.GetOrAdd(key, _ => new ConcurrentDictionary<long, BucketData>());

        long bucketSec = timeStamp / 1000;
        var bucket = buckets.GetOrAdd(bucketSec, _ => new BucketData());
        bucket.TotalRequests++;
        bucket.ElapsedSum += elapsed;
        if (success) bucket.SuccessRequests++;
        else bucket.ErrorRequests++;
    }

    public TimeSeriesSplit ExportToSeparatedCSV(int? targetPoints = null)
    {
        if (_globalMaxTime <= _globalMinTime || _rawBuckets.IsEmpty)
        {
            return new TimeSeriesSplit
            {
                BucketSec = [["Time", "BucketSec"]],
                WithUrlAvgElapsed = [["Time"]],
                WithUrlRps = [["Time"]],
                NoUrlAvgElapsed = [["Time"]],
                NoUrlRps = [["Time"]]
            };
        }

        int points = targetPoints ?? 100;
        long bucketSizeMs = BucketHelper.CalculateBucketSizeMs(_globalMinTime, _globalMaxTime, points);

        // Создаём все бакеты (включая пустые)
        var allBuckets = BucketHelper.BuildAllBuckets(_globalMinTime, _globalMaxTime, bucketSizeMs);

        // Агрегируем в финальные бакеты
        var aggregated = new Dictionary<string, SortedDictionary<long, BucketData>>();
        foreach (var kvSeries in _rawBuckets)
        {
            var finalBuckets = new SortedDictionary<long, BucketData>();

            foreach (var kvBucket in kvSeries.Value)
            {
                long originalTime = kvBucket.Key * 1000;
                long finalBucket = ((originalTime - _globalMinTime) / bucketSizeMs) * bucketSizeMs + _globalMinTime;

                if (!finalBuckets.TryGetValue(finalBucket, out var bucket))
                {
                    bucket = new BucketData();
                    finalBuckets[finalBucket] = bucket;
                }
                bucket.TotalRequests += kvBucket.Value.TotalRequests;
                bucket.SuccessRequests += kvBucket.Value.SuccessRequests;
                bucket.ErrorRequests += kvBucket.Value.ErrorRequests;
                bucket.ElapsedSum += kvBucket.Value.ElapsedSum;
            }

            // Заполняем пропуски нулями
            foreach (long t in allBuckets)
            {
                if (!finalBuckets.ContainsKey(t))
                    finalBuckets[t] = new BucketData();
            }

            aggregated[kvSeries.Key] = finalBuckets;
        }

        var bucketSeconds = bucketSizeMs / 1000.0;
        var withUrlLabels = new SortedSet<string>(StringComparer.Ordinal);
        var noUrlLabels = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var key in aggregated.Keys)
        {
            if (TryParseKey(key, out var baseLabel, out bool isWithUrl))
            {
                if (isWithUrl) withUrlLabels.Add(baseLabel);
                else noUrlLabels.Add(baseLabel);
            }
        }

        var bucketSecRows = new List<List<string>>
        {
            new() { "Time", "BucketSec" }
        };
        var withUrlAvgHeader = new List<string>(1 + withUrlLabels.Count) { "Time" };
        foreach (var label in withUrlLabels)
            withUrlAvgHeader.Add($"{label}_AvgElapsed");
        var withUrlAvgRows = new List<List<string>> { withUrlAvgHeader };
        var withUrlRpsRows = new List<List<string>>
        {
            BuildRpsHeader(withUrlLabels)
        };
        var noUrlAvgHeader = new List<string>(1 + noUrlLabels.Count) { "Time" };
        foreach (var label in noUrlLabels)
            noUrlAvgHeader.Add($"{label}_AvgElapsed");
        var noUrlAvgRows = new List<List<string>> { noUrlAvgHeader };
        var noUrlRpsRows = new List<List<string>>
        {
            BuildRpsHeader(noUrlLabels)
        };

        var allTimes = BucketHelper.BuildAllTimes(aggregated.Values);
        foreach (var time in allTimes)
        {
            string timeStr = time.ToString();
            bucketSecRows.Add([timeStr, bucketSeconds.ToString("F1")]);
            withUrlAvgRows.Add(BuildAvgRow(timeStr, time, withUrlLabels, aggregated, "withUrl"));
            withUrlRpsRows.Add(BuildRpsRow(timeStr, time, withUrlLabels, aggregated, bucketSeconds, "withUrl"));
            noUrlAvgRows.Add(BuildAvgRow(timeStr, time, noUrlLabels, aggregated, "noUrl"));
            noUrlRpsRows.Add(BuildRpsRow(timeStr, time, noUrlLabels, aggregated, bucketSeconds, "noUrl"));
        }

        return new TimeSeriesSplit
        {
            BucketSec = bucketSecRows,
            WithUrlAvgElapsed = withUrlAvgRows,
            WithUrlRps = withUrlRpsRows,
            NoUrlAvgElapsed = noUrlAvgRows,
            NoUrlRps = noUrlRpsRows
        };
    }

    private static bool TryParseKey(string key, out string baseLabel, out bool isWithUrl)
    {
        const string withUrlSuffix = "_withUrl";
        const string noUrlSuffix = "_noUrl";

        if (key.EndsWith(withUrlSuffix, StringComparison.Ordinal))
        {
            baseLabel = key[..^withUrlSuffix.Length];
            isWithUrl = true;
            return true;
        }
        if (key.EndsWith(noUrlSuffix, StringComparison.Ordinal))
        {
            baseLabel = key[..^noUrlSuffix.Length];
            isWithUrl = false;
            return true;
        }

        baseLabel = key;
        isWithUrl = false;
        return false;
    }

    private static List<string> BuildRpsHeader(IEnumerable<string> labels)
    {
        var header = new List<string> { "Time" };
        foreach (var label in labels)
        {
            header.Add($"{label} Success");
            header.Add($"{label} Failed");
        }
        return header;
    }

    private static List<string> BuildAvgRow(
        string timeStr,
        long time,
        IEnumerable<string> labels,
        Dictionary<string, SortedDictionary<long, BucketData>> aggregated,
        string urlKey)
    {
        var row = new List<string> { timeStr };
        foreach (var label in labels)
        {
            var bucket = GetBucket(aggregated, $"{label}_{urlKey}", time);
            double avgElapsed = bucket.TotalRequests > 0 ? bucket.ElapsedSum / bucket.TotalRequests : 0;
            row.Add(avgElapsed.ToString("F0"));
        }
        return row;
    }

    private static List<string> BuildRpsRow(
        string timeStr,
        long time,
        IEnumerable<string> labels,
        Dictionary<string, SortedDictionary<long, BucketData>> aggregated,
        double bucketSeconds,
        string urlKey)
    {
        var row = new List<string> { timeStr };
        foreach (var label in labels)
        {
            var bucket = GetBucket(aggregated, $"{label}_{urlKey}", time);
            double rpsSuccess = bucketSeconds > 0 ? bucket.SuccessRequests / bucketSeconds : 0;
            double rpsError = bucketSeconds > 0 ? bucket.ErrorRequests / bucketSeconds : 0;
            row.Add(rpsSuccess.ToString("F2"));
            row.Add(rpsError.ToString("F2"));
        }
        return row;
    }

    private static BucketData GetBucket(
        IReadOnlyDictionary<string, SortedDictionary<long, BucketData>> aggregated,
        string key,
        long time)
    {
        if (aggregated.TryGetValue(key, out var buckets) && buckets.TryGetValue(time, out var bucket))
            return bucket;
        return new BucketData();
    }
}
