namespace ReportTest.Models;

public class BucketData
{
    public long TotalRequests { get; set; }
    public long SuccessRequests { get; set; }
    public long ErrorRequests { get; set; }
    public double ElapsedSum { get; set; }
}
