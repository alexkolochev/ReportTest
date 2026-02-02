namespace ReportTest.IO;

internal static class CsvWriter
{
    public static void WriteRows(string path, IEnumerable<IReadOnlyList<string>> rows, char separator = ';')
    {
        using var writer = new StreamWriter(path);
        foreach (var row in rows)
        {
            for (int i = 0; i < row.Count; i++)
            {
                if (i > 0) writer.Write(separator);
                writer.Write(row[i]);
            }
            writer.WriteLine();
        }
    }
}
