using System.Collections.Concurrent;

namespace ReportTest.Aggregation;

public class ResponseCodeSummary
{
    private readonly ConcurrentDictionary<string, long> _counts = new();

    public void Add(string responseCode)
    {
        _counts.AddOrUpdate(responseCode, 1, (_, old) => old + 1);
    }

    public void PrintSummary(long totalRequests)
    {
        Console.WriteLine("{0,-8} {1,10} {2,6:F1}%", "Code", "Count", "%");

        var sorted = _counts.OrderByDescending(kv => kv.Value);
        foreach (var kv in sorted)
        {
            double percent = totalRequests > 0 ? kv.Value * 100.0 / totalRequests : 0;
            Console.WriteLine("{0,-8} {1,10:N0} {2,6:F1}%", kv.Key, kv.Value, percent);
        }
    }

    public string ToCsv(long totalRequests)
    {
        var lines = new List<string> { "ResponseCode,Count,Percent" };
        long total = totalRequests;

        foreach (var kv in _counts.OrderBy(kv => kv.Key))
        {
            double percent = total > 0 ? kv.Value * 100.0 / total : 0;
            lines.Add($"{kv.Key},{kv.Value},{percent:F2}");
        }
        return string.Join("\n", lines);
    }
}
