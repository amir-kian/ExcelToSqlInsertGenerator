using System;
using System.IO;

namespace ExcelToSqlInsertGenerator.Services;

/// <summary>Cache the last used connection string so it can be restored on next run.</summary>
public static class ConnectionStringCache
{
    private static string CacheFilePath
    {
        get
        {
            var dir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "ExcelToSqlInsertGenerator");
            try { Directory.CreateDirectory(dir); } catch { /* ignore */ }
            return Path.Combine(dir, "connection_string.txt");
        }
    }

    /// <summary>Save connection string to cache (call when executing).</summary>
    public static void Save(string? connectionString)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                return;
            File.WriteAllText(CacheFilePath, connectionString.Trim());
        }
        catch { /* ignore */ }
    }

    /// <summary>Load cached connection string. Returns null if none or file missing.</summary>
    public static string? Load()
    {
        try
        {
            var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "ExcelToSqlInsertGenerator", "connection_string.txt");
            if (!File.Exists(path))
                return null;
            var s = File.ReadAllText(path).Trim();
            return string.IsNullOrEmpty(s) ? null : s;
        }
        catch
        {
            return null;
        }
    }
}
