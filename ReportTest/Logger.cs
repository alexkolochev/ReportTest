namespace ReportTest
{
    public class Logger
    {
        public static void ErrorLog(Exception exception)
        {
            try
            {
                using var writer = new StreamWriter($@"{AppDomain.CurrentDomain.BaseDirectory}\errors.log", true);
                writer.WriteLine($"{DateTime.Now:dd.MM.ss HH:mm:ss}\n{exception.Message}\n{exception.Source}\n{exception.StackTrace}\n");
            }
            catch { }
        }
    }
}
