using System.Collections.Concurrent;
using System.Globalization;

namespace ReportTest.Aggregation;

public class ResponseCodeTimeSeriesAggregator
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<long, long>> _rawBuckets = new();
    private long _globalMinTime = long.MaxValue;
    private long _globalMaxTime;

    public void AddDataPoint(string responseCode, long timeStamp)
    {
        _globalMinTime = Math.Min(_globalMinTime, timeStamp);
        _globalMaxTime = Math.Max(_globalMaxTime, timeStamp);

        string key = $"{responseCode}";
        var buckets = _rawBuckets.GetOrAdd(key, _ => new ConcurrentDictionary<long, long>());

        long bucketSec = timeStamp / 1000;
        buckets.AddOrUpdate(bucketSec, 1, (_, old) => old + 1);
    }

    public List<List<string>> ExportToCSV(int? targetPoints = null)
    {
        if (_globalMaxTime <= _globalMinTime || _rawBuckets.IsEmpty)
        {
            return new List<List<string>>
            {
                new() { "Time", "BucketSec" }
            };
        }

        int points = targetPoints ?? 100;
        long bucketSizeMs = BucketHelper.CalculateBucketSizeMs(_globalMinTime, _globalMaxTime, points);

        var allBuckets = BucketHelper.BuildAllBuckets(_globalMinTime, _globalMaxTime, bucketSizeMs);

        var aggregated = new Dictionary<string, SortedDictionary<long, long>>();
        foreach (var kvSeries in _rawBuckets)
        {
            var finalBuckets = new SortedDictionary<long, long>();

            foreach (var kv in kvSeries.Value)
            {
                long originalTime = kv.Key * 1000;
                long finalBucket = ((originalTime - _globalMinTime) / bucketSizeMs) * bucketSizeMs + _globalMinTime;

                finalBuckets.TryGetValue(finalBucket, out var cnt);
                finalBuckets[finalBucket] = cnt + kv.Value;
            }

            foreach (long t in allBuckets)
                if (!finalBuckets.ContainsKey(t))
                    finalBuckets[t] = 0;

            aggregated[kvSeries.Key] = finalBuckets;
        }

        var bucketSeconds = bucketSizeMs / 1000.0;
        var rows = new List<List<string>>();

        var labels = new List<string>(aggregated.Keys);
        labels.Sort(StringComparer.Ordinal);
        var header = new List<string> { "Time" };
        foreach (var label in labels)
            header.Add($"{label}");

        rows.Add(header);

        var allTimes = BucketHelper.BuildAllTimes(aggregated.Values);

        foreach (var time in allTimes)
        {
            var row = new List<string>
            {
                time.ToString(CultureInfo.InvariantCulture)
            };

            foreach (var label in labels)
            {
                long count = aggregated[label].TryGetValue(time, out var c) ? c : 0;
                double rps = bucketSeconds > 0 ? count / bucketSeconds : 0;
                row.Add(rps.ToString("F2", CultureInfo.InvariantCulture));
            }

            rows.Add(row);
        }

        return rows;
    }
}
