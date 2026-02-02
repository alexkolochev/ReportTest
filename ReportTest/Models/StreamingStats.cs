namespace ReportTest.Models;

public class StreamingStats
{
    public long Count { get; set; }
    public long SuccessCount { get; set; }
    public long ErrorCount { get; set; }
    public double Sum { get; set; }
    public double MinElapsed { get; set; } = double.MaxValue;
    public double MaxElapsed { get; set; } = double.MinValue;
    public double SumIdleTime { get; set; }
    public double SumLatency { get; set; }
    public double M2 { get; set; }
    public long MinTimeStamp { get; set; } = long.MaxValue;
    public long MaxTimeStamp { get; set; }

    private readonly List<long> _samples = [];
    private bool _samplesSorted = true;

    public void Update(long elapsed, bool success, long timeStamp, long idleTime, long latency)
    {
        Count++; SuccessCount += success ? 1 : 0; ErrorCount += success ? 0 : 1;
        Sum += elapsed; MinElapsed = Math.Min(MinElapsed, elapsed); MaxElapsed = Math.Max(MaxElapsed, elapsed);
        SumIdleTime += idleTime;
        SumLatency += latency;
        MinTimeStamp = Math.Min(MinTimeStamp, timeStamp); MaxTimeStamp = Math.Max(MaxTimeStamp, timeStamp);

        double delta = elapsed - (Sum / Count);
        M2 += delta * delta;
        if (_samples.Count < 1000000)
        {
            _samples.Add(elapsed);
            _samplesSorted = false;
        }
    }

    public double SuccessRate => Count > 0 ? SuccessCount * 100.0 / Count : 0;
    public double ErrorRate => 100 - SuccessRate;
    public double Mean => Count > 0 ? Sum / Count : 0;
    public double IdleTimeAvg => Count > 0 ? SumIdleTime / Count : 0;
    public double LatencyAvg => Count > 0 ? SumLatency / Count : 0;
    public double StdDev => Count > 1 ? Math.Sqrt(M2 / (Count - 1)) : 0;
    public double CV => Mean > 0 ? StdDev / Mean * 100 : 0;
    public double RPS => DurationSeconds > 0 ? Count / DurationSeconds : 0;
    public double DurationSeconds => (MaxTimeStamp - MinTimeStamp) / 1000.0;
    public double P90 => Percentile(90);
    public double P95 => Percentile(95);

    private double Percentile(int perc)
    {
        if (_samples.Count == 0) return 0;
        if (!_samplesSorted)
        {
            _samples.Sort();
            _samplesSorted = true;
        }
        double pos = (perc / 100.0) * (_samples.Count - 1);
        int lower = (int)pos;
        double fraction = pos - lower;
        return _samples[lower] + fraction * (_samples.Count > lower + 1 ? _samples[lower + 1] - _samples[lower] : 0);
    }
}
