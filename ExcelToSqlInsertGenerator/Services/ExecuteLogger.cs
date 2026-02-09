using System;
using System.Collections.Generic;
using System.IO;

namespace ExcelToSqlInsertGenerator.Services;

/// <summary>Simple file logger for Execute process. Writes to project folder\log\ (same folder as .csproj).</summary>
public static class ExecuteLogger
{
    private static readonly object Lock = new();
    private static string? _logDir;

    /// <summary>Project folder = directory containing ExcelToSqlInsertGenerator.csproj (search up from exe).</summary>
    private static string GetLogDirectory()
    {
        if (_logDir != null) return _logDir;
        try
        {
            string? dir = Path.GetDirectoryName(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            for (int i = 0; i < 6 && !string.IsNullOrEmpty(dir); i++)
            {
                string csproj = Path.Combine(dir, "ExcelToSqlInsertGenerator.csproj");
                if (File.Exists(csproj))
                {
                    _logDir = Path.Combine(dir, "log");
                    Directory.CreateDirectory(_logDir);
                    return _logDir;
                }
                dir = Path.GetDirectoryName(dir);
            }
        }
        catch { /* ignore */ }
        _logDir = Path.Combine(AppContext.BaseDirectory, "log");
        try { Directory.CreateDirectory(_logDir); } catch { /* ignore */ }
        return _logDir;
    }

    /// <summary>Call at app startup so the log folder exists immediately.</summary>
    public static void EnsureLogDirectoryExists()
    {
        _ = GetLogDirectory();
    }

    private static string LogFilePath => Path.Combine(GetLogDirectory(), "ExcelToSqlInsertGenerator_execute.log");

    /// <summary>Get full path to the log file (e.g. for opening in Notepad).</summary>
    public static string GetLogFilePath() => LogFilePath;

    public static void Info(string message) => Write("INFO", message);
    public static void Warn(string message) => Write("WARN", message);
    public static void Error(string message) => Write("ERROR", message);
    public static void Error(string message, Exception ex) => Write("ERROR", message + "\n  " + ex.GetType().Name + ": " + ex.Message + "\n" + ex.StackTrace);

    private static void Write(string level, string message)
    {
        try
        {
            var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level}] {message}{Environment.NewLine}";
            lock (Lock)
            {
                File.AppendAllText(LogFilePath, line);
            }
        }
        catch { /* avoid crashing on log failure */ }
    }

    /// <summary>Write a session separator so runs are easy to spot.</summary>
    public static void StartSession(string mode, int totalRows, int startRow, int chunkSize)
    {
        Info("========== Execute session start ==========");
        Info($"Mode: {mode}, Total rows: {totalRows}, Start row: {startRow}, Chunk size: {chunkSize}");
    }

    public static void EndSession(int inserted, int failed, string? stoppedWithError = null)
    {
        if (!string.IsNullOrEmpty(stoppedWithError))
            Error("Stopped with error: " + stoppedWithError);
        Info($"Session end: Inserted: {inserted}, Failed: {failed}");
        Info("========== Execute session end ==========");
    }

    /// <summary>Log when rows are skipped (Skip rows &gt; 0). From row (inclusive) to row (inclusive), then execution starts at actualStartRow.</summary>
    public static void LogSkippedRange(int fromRowInclusive, int toRowInclusive, int actualStartRow)
    {
        int count = toRowInclusive - fromRowInclusive + 1;
        Info($"Skipped rows {fromRowInclusive} to {toRowInclusive} (count={count}). Execution starting at row {actualStartRow}.");
    }

    /// <summary>Log each failed row to the log file (RowIndex, IdValue, ErrorMessage).</summary>
    public static void LogFailedRows(IReadOnlyList<(int RowIndex, string? IdValue, string ErrorMessage)> failures)
    {
        if (failures == null || failures.Count == 0) return;
        Info($"Failed rows ({failures.Count}):");
        foreach (var (rowIndex, idValue, errorMessage) in failures)
        {
            string idPart = string.IsNullOrEmpty(idValue) ? "" : $" ID={idValue} |";
            string line = $"  Row {rowIndex}:{idPart} {errorMessage}";
            Write("FAIL", line);
        }
    }
}
