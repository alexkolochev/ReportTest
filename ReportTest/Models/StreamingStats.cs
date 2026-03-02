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
        Count++;
        if (success)
        {
            SuccessCount++;
            Sum += elapsed;
            MinElapsed = Math.Min(MinElapsed, elapsed);
            MaxElapsed = Math.Max(MaxElapsed, elapsed);
            SumIdleTime += idleTime;
            SumLatency += latency;
            MinTimeStamp = Math.Min(MinTimeStamp, timeStamp);
            MaxTimeStamp = Math.Max(MaxTimeStamp, timeStamp);

            double delta = elapsed - (Sum / SuccessCount);
            M2 += delta * delta;
            if (_samples.Count < 1000000)
            {
                _samples.Add(elapsed);
                _samplesSorted = false;
            }
        }
        else
        {
            ErrorCount++;
        }
    }

    public double SuccessRate => Count > 0 ? SuccessCount * 100.0 / Count : 0;
    public double ErrorRate => 100 - SuccessRate;
    public double Mean => SuccessCount > 0 ? Sum / SuccessCount : 0;
    public double IdleTimeAvg => SuccessCount > 0 ? SumIdleTime / SuccessCount : 0;
    public double LatencyAvg => SuccessCount > 0 ? SumLatency / SuccessCount : 0;
    public double StdDev => SuccessCount > 1 ? Math.Sqrt(M2 / (SuccessCount - 1)) : 0;
    public double CV => Mean > 0 ? StdDev / Mean * 100 : 0;
    public double RPS => DurationSeconds > 0 ? SuccessCount / DurationSeconds : 0;
    public double DurationSeconds => SuccessCount > 0 ? (MaxTimeStamp - MinTimeStamp) / 1000.0 : 0;
    public double MinElapsedValue => SuccessCount > 0 ? MinElapsed : 0;
    public double MaxElapsedValue => SuccessCount > 0 ? MaxElapsed : 0;
    public long MinTimeStampValue => SuccessCount > 0 ? MinTimeStamp : 0;
    public long MaxTimeStampValue => SuccessCount > 0 ? MaxTimeStamp : 0;
    public double Median => Percentile(50);
    public double P90 => Percentile(90);
    public double P95 => Percentile(95);
    public double P99 => Percentile(99);
    public double P995 => Percentile(99.5);
    public double P9995 => Percentile(99.95);

    private double Percentile(double perc)
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
