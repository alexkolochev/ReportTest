namespace ReportTest.Aggregation;

internal static class BucketHelper
{
    public static long CalculateBucketSizeMs(long minTime, long maxTime, int points, long minBucketMs = 1000)
    {
        long totalDurationMs = maxTime - minTime;
        long bucketSizeMs = totalDurationMs / points;
        if (bucketSizeMs < minBucketMs) bucketSizeMs = minBucketMs;
        return bucketSizeMs;
    }

    public static List<long> BuildAllBuckets(long minTime, long maxTime, long bucketSizeMs)
    {
        var allBuckets = new List<long>();
        for (long t = minTime; t <= maxTime; t += bucketSizeMs)
            allBuckets.Add(t);
        return allBuckets;
    }

    public static List<long> BuildAllTimes<T>(IEnumerable<SortedDictionary<long, T>> series)
    {
        var set = new HashSet<long>();
        foreach (var buckets in series)
        {
            foreach (var key in buckets.Keys)
                set.Add(key);
        }
        var allTimes = new List<long>(set);
        allTimes.Sort();
        return allTimes;
    }
}
