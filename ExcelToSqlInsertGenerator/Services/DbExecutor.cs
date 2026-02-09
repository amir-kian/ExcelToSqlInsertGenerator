using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ExcelToSqlInsertGenerator.Configuration;
using ExcelToSqlInsertGenerator.Models;
using Microsoft.Data.SqlClient;

namespace ExcelToSqlInsertGenerator.Services;

public static class DbExecutor
{
    /// <summary>Result of executing INSERT statements against the database.</summary>
    public record ExecuteResult(int Inserted, int Failed, IReadOnlyList<FailedRow> FailedRows)
    {
        public string? StoppedWithError { get; init; }
        public int? LastProcessedRow { get; init; }
    }

    public record FailedRow(int RowIndex, string? IdValue, string ErrorMessage);

    /// <summary>Result of pre-execution validation (no DB connection). Identifies rows that would fail SQL generation.</summary>
    public record ValidationResult(bool Ok, IReadOnlyList<ValidationIssue> Issues);

    public record ValidationIssue(int RowIndex, string ErrorMessage);

    /// <summary>Extract individual INSERT statements from generated SQL.</summary>
    public static List<string> ParseInsertStatements(string sql)
    {
        var lines = sql.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
        var inserts = new List<string>();
        var current = new System.Text.StringBuilder();

        foreach (var line in lines)
        {
            current.AppendLine(line);
            if (line.TrimEnd().EndsWith(");"))
            {
                inserts.Add(current.ToString().TrimEnd());
                current.Clear();
            }
        }
        if (current.Length > 0)
            inserts.Add(current.ToString().TrimEnd());

        return inserts;
    }

    /// <summary>Try to extract the first value (typically Id) from VALUES clause for logging.</summary>
    private static string? TryExtractFirstValue(string insertSql)
    {
        var match = Regex.Match(insertSql, @"VALUES\s*\(([^)]+)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success) return null;
        var firstVal = match.Groups[1].Value.Split(',')[0].Trim();
        int maxLen = AppSettings.Execute.IdValueMaxLength;
        if (firstVal.StartsWith("N'") || firstVal.StartsWith("'"))
            return firstVal.Length > maxLen ? firstVal[..maxLen] + "..." : firstVal;
        return firstVal;
    }

    /// <summary>Count INSERT statements without allocating full line array (lightweight).</summary>
    public static int CountInsertStatements(string sql)
    {
        int count = 0;
        using var sr = new StringReader(sql);
        string? line;
        while ((line = sr.ReadLine()) != null)
        {
            if (line.TrimEnd().EndsWith(");"))
                count++;
        }
        return count;
    }

    /// <summary>Execute from source - generate and execute one INSERT per row. Never holds full SQL in memory.</summary>
    public static ExecuteResult ExecuteFromSource(
        string connectionString,
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        List<Dictionary<string, object>> rows,
        IProgress<(int current, int total)>? progress,
        CancellationToken token)
    {
        int inserted = 0;
        int failed = 0;
        var failedRows = new List<FailedRow>();
        int total = rows.Count;

        if (total == 0)
            return new ExecuteResult(0, 0, failedRows);

        int reportInterval = Math.Max(1, total / AppSettings.Execute.ReportIntervalDivisor);

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        for (int i = 0; i < rows.Count; i++)
        {
            token.ThrowIfCancellationRequested();

            string sql;
            try
            {
                sql = SqlGenerator.GenerateSingleInsert(insertTemplate, placeholders, rows[i], i);
            }
            catch (Exception ex)
            {
                failed++;
                failedRows.Add(new FailedRow(i + 2, null, ex.Message));
                if (progress != null && (i % reportInterval == 0 || i == rows.Count - 1))
                    progress.Report((i + 1, total));
                continue;
            }

            try
            {
                using var cmd = new SqlCommand(sql, conn);
                cmd.CommandTimeout = 30;
                cmd.ExecuteNonQuery();
                inserted++;
            }
            catch (Exception ex)
            {
                failed++;
                string? idValue = TryExtractFirstValue(sql);
                failedRows.Add(new FailedRow(i + 2, idValue, ex.Message));
            }

            if (progress != null && (i % reportInterval == 0 || i == rows.Count - 1))
                progress.Report((i + 1, total));
        }

        return new ExecuteResult(inserted, failed, failedRows);
    }

    /// <summary>Add ROWLOCK hint to INSERT template to reduce lock contention.</summary>
    private static string AddRowLockHint(string insertTemplate)
    {
        return Regex.Replace(insertTemplate, @"(INSERT\s+INTO\s+.+?)(\s*\()", "$1 WITH (ROWLOCK) $2", RegexOptions.IgnoreCase | RegexOptions.Singleline);
    }

    /// <summary>Validate all rows before execute: try generating SQL for each row without touching the database. Identifies problematic rows (bad data, Excel errors, etc.).</summary>
    public static ValidationResult ValidateBeforeExecute(
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        List<Dictionary<string, object>> rows,
        IProgress<(int current, int total)>? progress,
        CancellationToken token)
    {
        var issues = new List<ValidationIssue>();
        int total = rows.Count;
        int reportInterval = Math.Max(1, total / AppSettings.Validate.ReportIntervalDivisor);
        string templateWithHint = AddRowLockHint(insertTemplate);

        for (int i = 0; i < total; i++)
        {
            token.ThrowIfCancellationRequested();

            try
            {
                string sql = SqlGenerator.GenerateSingleInsert(templateWithHint, placeholders, rows[i], i);
                if (sql.Length > AppSettings.Validate.MaxSqlLength)
                    issues.Add(new ValidationIssue(i + 2, $"Generated SQL too long ({sql.Length:N0} chars) — may cause memory issues"));
            }
            catch (Exception ex)
            {
                issues.Add(new ValidationIssue(i + 2, ex.Message));
            }

            if (progress != null && ((i + 1) % reportInterval == 0 || i + 1 == total))
                progress.Report((i + 1, total));
        }

        return new ValidationResult(issues.Count == 0, issues);
    }

    private static int CommandTimeoutSeconds => AppSettings.Execute.CommandTimeoutSeconds;
    private static int GcIntervalRows => AppSettings.Execute.GcIntervalRows;
    private static int ConnectionChunkRows => AppSettings.Execute.ConnectionChunkRows;
    private static int InsertBatchSize => AppSettings.Execute.InsertBatchSize;
    private static int MaxFailedRowsToKeep => AppSettings.Execute.MaxFailedRowsToKeep;

    private static string EnsureConnectionTimeout(string connectionString)
    {
        var lower = connectionString.ToLowerInvariant();
        if (lower.Contains("connection timeout=") || lower.Contains("connect timeout="))
            return connectionString;
        var sep = connectionString.TrimEnd().EndsWith(";") ? "" : ";";
        return connectionString + sep + "Connection Timeout=" + AppSettings.Execute.ConnectionTimeoutSeconds;
    }

    private static SqlConnection OpenConnectionWithRetry(string connectionString, CancellationToken token)
    {
        Exception? lastEx = null;
        int retries = Math.Max(0, AppSettings.Execute.ConnectionRetryCount);
        int delayMs = Math.Max(0, AppSettings.Execute.ConnectionRetryDelayMs);
        for (int attempt = 0; attempt <= retries; attempt++)
        {
            token.ThrowIfCancellationRequested();
            var conn = new SqlConnection(connectionString);
            try
            {
                conn.Open();
                return conn;
            }
            catch (Exception ex)
            {
                lastEx = ex;
                conn.Dispose();
                if (attempt < retries)
                    Thread.Sleep(delayMs);
            }
        }
        throw lastEx ?? new InvalidOperationException("Connection failed");
    }

    private static string CheckpointFilePath => Path.Combine(Path.GetTempPath(), "ExcelToSqlInsertGenerator_checkpoint.txt");

    /// <summary>Read last checkpoint row from file. Returns null if no checkpoint exists.</summary>
    public static int? ReadCheckpoint()
    {
        try
        {
            if (File.Exists(CheckpointFilePath) && int.TryParse(File.ReadAllText(CheckpointFilePath), out var row))
                return row;
        }
        catch { /* ignore */ }
        return null;
    }

    private static void WriteCheckpoint(int lastRow)
    {
        try
        {
            File.WriteAllText(CheckpointFilePath, lastRow.ToString());
        }
        catch { /* ignore */ }
    }

    /// <summary>Execute from source: checkpoint file, smaller chunks, LOH compaction, broad try-catch for silent crashes.</summary>
    public static ExecuteResult ExecuteFromSourceSafe(
        string connectionString,
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        List<Dictionary<string, object>> rows,
        IProgress<(int current, int total)>? progress,
        CancellationToken token,
        int startRow = 0,
        int chunkSize = 0)
    {
        int total = rows.Count;
        ExecuteLogger.StartSession("Sequential", total, startRow, chunkSize);
        try
        {
            var result = ExecuteFromSourceSafeCore(connectionString, insertTemplate, placeholders, rows, progress, token, startRow, chunkSize);
            if (result.FailedRows.Count > 0)
                ExecuteLogger.LogFailedRows(result.FailedRows.Select(f => (f.RowIndex, f.IdValue, f.ErrorMessage)).ToList());
            ExecuteLogger.EndSession(result.Inserted, result.Failed, result.StoppedWithError);
            return result;
        }
        catch (OperationCanceledException)
        {
            ExecuteLogger.Info("Execute cancelled by user.");
            ExecuteLogger.EndSession(0, 0, "Cancelled");
            throw;
        }
        catch (OutOfMemoryException ex)
        {
            ExecuteLogger.Error("Out of memory in ExecuteFromSourceSafe", ex);
            int lastRow = startRow;
            try { lastRow = int.Parse(File.ReadAllText(CheckpointFilePath)); } catch { /* use startRow */ }
            WriteCheckpoint(lastRow);
            ExecuteLogger.EndSession(0, 0, ex.Message);
            return new ExecuteResult(0, 0, new List<FailedRow>())
            { StoppedWithError = "Out of memory. (Checkpoint saved. Set Start row to " + lastRow + " and run again, or use smaller Chunk size.)", LastProcessedRow = lastRow };
        }
        catch (Exception ex)
        {
            ExecuteLogger.Error("Unhandled exception in ExecuteFromSourceSafe", ex);
            int lastRow = startRow;
            try { lastRow = int.Parse(File.ReadAllText(CheckpointFilePath)); } catch { /* use startRow */ }
            WriteCheckpoint(lastRow);
            ExecuteLogger.EndSession(0, 0, ex.Message);
            return new ExecuteResult(0, 0, new List<FailedRow>())
            { StoppedWithError = ex.Message + " (Checkpoint saved. Set Start row to " + lastRow + " and run again.)", LastProcessedRow = lastRow };
        }
    }

    private static ExecuteResult ExecuteFromSourceSafeCore(
        string connectionString,
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        List<Dictionary<string, object>> rows,
        IProgress<(int current, int total)>? progress,
        CancellationToken token,
        int startRow,
        int chunkSize,
        bool writeCheckpoint = true)
    {
        int total = rows.Count;
        if (total == 0 || startRow >= total)
            return new ExecuteResult(0, 0, new List<FailedRow>());

        connectionString = EnsureConnectionTimeout(connectionString);
        int endRow = chunkSize > 0 ? Math.Min(startRow + chunkSize, total) : total;
        if (endRow > total) endRow = total;
        if (endRow <= startRow)
        {
            ExecuteLogger.Info($"Core: no rows to process (startRow={startRow}, total={total})");
            return new ExecuteResult(0, 0, new List<FailedRow>());
        }
        int rowCount = endRow - startRow;
        ExecuteLogger.Info($"Core: processing rows {startRow}-{endRow - 1} (count={rowCount})");
        int inserted = 0;
        int failed = 0;
        var failedRows = new List<FailedRow>();
        string templateWithHint = AddRowLockHint(insertTemplate);
        int lastGcAt = startRow;
        int checkpointInterval = Math.Max(1, AppSettings.Execute.CheckpointIntervalRows);
        const int reportThrottleMs = 250;
        long lastReportTicks = Environment.TickCount64;

        void Report()
        {
            int done = inserted + failed;
            if (progress != null)
            {
                long now = Environment.TickCount64;
                if (now - lastReportTicks >= reportThrottleMs || done == 0 || done == endRow - startRow)
                {
                    lastReportTicks = now;
                    progress.Report((startRow + done, total));
                }
            }
        }

        int lastProcessedRow = startRow;

        for (int chunkStart = startRow; chunkStart < endRow; chunkStart += ConnectionChunkRows)
        {
            int chunkEnd = Math.Min(chunkStart + ConnectionChunkRows, endRow);

            SqlConnection? conn = null;
            try
            {
                conn = OpenConnectionWithRetry(connectionString, token);
            }
            catch (Exception ex)
            {
                ExecuteLogger.Error($"Connection open failed for chunk {chunkStart}-{chunkEnd}: {ex.Message}");
                return new ExecuteResult(inserted, failed, failedRows)
                { StoppedWithError = ex.Message, LastProcessedRow = lastProcessedRow };
            }

            try
            {
                using (conn)
                {
                    var batch = new List<(int Index, string Sql)>(InsertBatchSize);

                    for (int i = chunkStart; i < chunkEnd; i++)
                {
                    token.ThrowIfCancellationRequested();

                    if (i - lastGcAt >= GcIntervalRows)
                    {
                        GC.Collect(2, GCCollectionMode.Optimized);
                        GC.WaitForPendingFinalizers();
                        lastGcAt = i;
                    }

                    string? sql = null;
                    try
                    {
                        sql = SqlGenerator.GenerateSingleInsert(templateWithHint, placeholders, rows[i], i);
                    }
                    catch (Exception ex)
                    {
                        failed++;
                        if (failedRows.Count < MaxFailedRowsToKeep)
                            failedRows.Add(new FailedRow(i + 2, null, ex.Message));
                        Report();
                        continue;
                    }

                    batch.Add((i, sql));

                    if (batch.Count >= InsertBatchSize)
                    {
                        ExecuteBatch(conn, batch, ref inserted, ref failed, failedRows);
                        batch.Clear();
                        int done = startRow + inserted + failed;
                        if (writeCheckpoint && done > 0 && done % checkpointInterval == 0)
                        {
                            WriteCheckpoint(done);
                            ExecuteLogger.Info($"Checkpoint: row {done} (inserted={inserted}, failed={failed})");
                        }
                        Report();
                        Thread.Sleep(0);
                    }
                }

                if (batch.Count > 0)
                {
                    ExecuteBatch(conn, batch, ref inserted, ref failed, failedRows);
                    Report();
                }
                lastProcessedRow = startRow + inserted + failed;
                if (writeCheckpoint) WriteCheckpoint(lastProcessedRow);
                if (chunkEnd >= endRow)
                    ExecuteLogger.Info($"Chunk complete up to row {lastProcessedRow} (inserted={inserted}, failed={failed})");
                }
            }
            catch (Exception ex)
            {
                lastProcessedRow = startRow + inserted + failed;
                if (writeCheckpoint) WriteCheckpoint(lastProcessedRow);
                ExecuteLogger.Error($"Exception in chunk {chunkStart}-{chunkEnd}: {ex.Message}", ex);
                return new ExecuteResult(inserted, failed, failedRows)
                { StoppedWithError = ex.Message, LastProcessedRow = lastProcessedRow };
            }

            void ExecuteBatch(SqlConnection connection, List<(int Index, string Sql)> batchItems, ref int ins, ref int fail, List<FailedRow> failedList)
            {
                try
                {
                    var sb = new System.Text.StringBuilder(Math.Min(batchItems.Count * 512, 10 * 1024 * 1024));
                    foreach (var (_, s) in batchItems)
                        sb.AppendLine(s);
                    string batchSql = sb.ToString();
                    int batchRetries = Math.Max(0, AppSettings.Execute.BatchRetryCount);
                    int batchDelayMs = Math.Max(0, AppSettings.Execute.ConnectionRetryDelayMs);

                    bool batchOk = false;
                    for (int attempt = 0; attempt <= batchRetries && !batchOk; attempt++)
                    {
                        SqlTransaction? tran = null;
                        try
                        {
                            tran = connection.BeginTransaction();
                            using var cmd = new SqlCommand(batchSql, connection, tran);
                            cmd.CommandTimeout = CommandTimeoutSeconds;
                            cmd.ExecuteNonQuery();
                            tran.Commit();
                            tran = null;
                            ins += batchItems.Count;
                            batchOk = true;
                        }
                        catch when (attempt < batchRetries)
                        {
                            try { tran?.Rollback(); } catch { /* ignore */ }
                            Thread.Sleep(batchDelayMs);
                        }
                        catch
                        {
                            try { tran?.Rollback(); } catch { /* ignore */ }
                        }
                    }

                    if (!batchOk)
                    {
                        ExecuteLogger.Warn($"Batch failed ({batchItems.Count} rows), falling back to row-by-row");
                        for (int r = 0; r < batchItems.Count; r++)
                        {
                            try
                            {
                                var (idx, s) = batchItems[r];
                                using var c = new SqlCommand(s, connection);
                                c.CommandTimeout = CommandTimeoutSeconds;
                                c.ExecuteNonQuery();
                                ins++;
                            }
                            catch (Exception ex)
                            {
                                fail++;
                                var (idx, s) = batchItems[r];
                                ExecuteLogger.Info($"Row {idx + 2} failed in row-by-row: {ex.Message}");
                                string? idVal = null;
                                try { idVal = TryExtractFirstValue(s); } catch { /* ignore */ }
                                if (failedList.Count < MaxFailedRowsToKeep)
                                    failedList.Add(new FailedRow(idx + 2, idVal, ex.Message));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    ExecuteLogger.Error($"ExecuteBatch threw (batch size {batchItems.Count}): {ex.Message}", ex);
                    throw;
                }
            }

            GC.Collect(2, GCCollectionMode.Optimized);
            GC.WaitForPendingFinalizers();
        }

        if (failed > MaxFailedRowsToKeep)
        {
            failedRows.Add(new FailedRow(-1, null, $"... and {failed - MaxFailedRowsToKeep} more failures (only first {MaxFailedRowsToKeep} logged)"));
        }

        ExecuteLogger.Info($"Core complete: inserted={inserted}, failed={failed}, lastRow={startRow + inserted + failed}");
        return new ExecuteResult(inserted, failed, failedRows);
    }

    private static int MaxParallelChunks => Math.Max(1, Math.Min(AppSettings.Execute.MaxParallelChunks, 16));

    /// <summary>Execute from source in parallel: divide rows into chunks of chunkSize, process chunks concurrently, aggregate success/failure report.</summary>
    public static ExecuteResult ExecuteFromSourceParallelChunks(
        string connectionString,
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        List<Dictionary<string, object>> rows,
        IProgress<(int current, int total)>? progress,
        CancellationToken token,
        int startRow = 0,
        int chunkSize = 0)
    {
        int total = rows.Count;
        if (total == 0 || startRow >= total)
            return new ExecuteResult(0, 0, new List<FailedRow>());

        ExecuteLogger.StartSession("Parallel", total, startRow, chunkSize);
        connectionString = EnsureConnectionTimeout(connectionString);
        int maxParallel = MaxParallelChunks;
        int rowsToProcess = total - startRow;
        int effectiveChunkSize = chunkSize > 0 ? Math.Min(chunkSize, rowsToProcess) : Math.Max(10000, rowsToProcess / maxParallel);

        var chunkRanges = new List<(int ChunkStart, int ChunkEnd)>();
        for (int s = startRow; s < total; s += effectiveChunkSize)
            chunkRanges.Add((s, Math.Min(s + effectiveChunkSize, total)));

        int totalInserted = 0;
        int totalFailed = 0;
        var allFailedRows = new ConcurrentBag<FailedRow>();
        var chunkCompleted = new int[chunkRanges.Count];
        const int reportThrottleMs = 250;
        long lastReportTicks = Environment.TickCount64;
        var progressLock = new object();

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Min(maxParallel, chunkRanges.Count),
            CancellationToken = token
        };

        Parallel.For(0, chunkRanges.Count, options, chunkIndex =>
        {
            var (chunkStart, chunkEnd) = chunkRanges[chunkIndex];
            int chunkRows = chunkEnd - chunkStart;
            var chunkProgress = new Progress<(int current, int total)>(p =>
            {
                int doneInChunk = p.current - chunkStart;
                Interlocked.Exchange(ref chunkCompleted[chunkIndex], doneInChunk);
                if (progress != null)
                {
                    lock (progressLock)
                    {
                        long now = Environment.TickCount64;
                        if (now - lastReportTicks >= reportThrottleMs)
                        {
                            lastReportTicks = now;
                            int sum = 0;
                            for (int i = 0; i < chunkCompleted.Length; i++)
                                sum += Volatile.Read(ref chunkCompleted[i]);
                            progress.Report((startRow + sum, total));
                        }
                    }
                }
            });
            var result = ExecuteFromSourceSafeCore(connectionString, insertTemplate, placeholders, rows,
                chunkProgress, token, chunkStart, chunkRows, writeCheckpoint: false);
            Interlocked.Add(ref totalInserted, result.Inserted);
            Interlocked.Add(ref totalFailed, result.Failed);
            foreach (var fr in result.FailedRows)
                allFailedRows.Add(fr);
        });

        if (progress != null)
            progress.Report((total, total));

        var failedRowsList = allFailedRows.OrderBy(f => f.RowIndex).ToList();
        if (totalFailed > MaxFailedRowsToKeep && failedRowsList.Count > MaxFailedRowsToKeep)
            failedRowsList.Add(new FailedRow(-1, null, $"... and {totalFailed - MaxFailedRowsToKeep} more failures (only first {MaxFailedRowsToKeep} logged)"));

        if (failedRowsList.Count > 0)
            ExecuteLogger.LogFailedRows(failedRowsList.Select(f => (f.RowIndex, f.IdValue, f.ErrorMessage)).ToList());
        ExecuteLogger.EndSession(totalInserted, totalFailed);
        return new ExecuteResult(totalInserted, totalFailed, failedRowsList);
    }

    /// <summary>Execute from source in parallel - multiple workers, each with own connection. Faster but may cause crashes under load.</summary>
    public static ExecuteResult ExecuteFromSourceParallel(
        string connectionString,
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        List<Dictionary<string, object>> rows,
        IProgress<(int current, int total)>? progress,
        CancellationToken token,
        int maxDegreeOfParallelism = 4)
    {
        int total = rows.Count;
        if (total == 0)
            return new ExecuteResult(0, 0, new List<FailedRow>());

        int inserted = 0;
        int failed = 0;
        var failedRows = new ConcurrentBag<FailedRow>();
        int completed = 0;
        int reportInterval = Math.Max(1, total / AppSettings.Execute.ReportIntervalDivisor);

        // Add ROWLOCK hint to reduce SQL Server lock contention between parallel workers
        string templateWithHint = AddRowLockHint(insertTemplate);

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, Math.Min(maxDegreeOfParallelism, 16)),
            CancellationToken = token
        };

        Parallel.ForEach(
            Partitioner.Create(0, total),
            options,
            range =>
            {
                using var conn = new SqlConnection(connectionString);
                conn.Open();

                for (int i = range.Item1; i < range.Item2; i++)
                {
                    options.CancellationToken.ThrowIfCancellationRequested();

                    string sql;
                    try
                    {
                        sql = SqlGenerator.GenerateSingleInsert(templateWithHint, placeholders, rows[i], i);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref failed);
                        failedRows.Add(new FailedRow(i + 2, null, ex.Message));
                        ReportProgress();
                        continue;
                    }

                    try
                    {
                        using var cmd = new SqlCommand(sql, conn);
                        cmd.CommandTimeout = 30;
                        cmd.ExecuteNonQuery();
                        Interlocked.Increment(ref inserted);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref failed);
                        string? idValue = TryExtractFirstValue(sql);
                        failedRows.Add(new FailedRow(i + 2, idValue, ex.Message));
                    }

                    ReportProgress();
                }

                void ReportProgress()
                {
                    int c = Interlocked.Increment(ref completed);
                    if (progress != null && (c % reportInterval == 0 || c == total))
                        progress.Report((c, total));
                }
            });

        return new ExecuteResult(inserted, failed, new List<FailedRow>(failedRows));
    }

    /// <summary>Execute INSERTs by streaming - reads line-by-line and executes one at a time to avoid OOM.</summary>
    public static ExecuteResult ExecuteInsertsStreaming(
        string connectionString,
        string sqlText,
        IProgress<(int current, int total)>? progress,
        CancellationToken token)
    {
        int inserted = 0;
        int failed = 0;
        var failedRows = new List<FailedRow>();

        int total = CountInsertStatements(sqlText);
        if (total == 0)
            return new ExecuteResult(0, 0, failedRows);

        int reportInterval = Math.Max(1, total / AppSettings.Execute.ReportIntervalDivisor);

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        using var sr = new StringReader(sqlText);
        var current = new System.Text.StringBuilder(1024);
        int rowIndex = 0;
        string? line;

        while ((line = sr.ReadLine()) != null)
        {
            token.ThrowIfCancellationRequested();
            current.AppendLine(line);

            if (!line.TrimEnd().EndsWith(");"))
                continue;

            rowIndex++;
            string sql = current.ToString().TrimEnd();
            current.Clear();

            try
            {
                using var cmd = new SqlCommand(sql, conn);
                cmd.CommandTimeout = 30;
                cmd.ExecuteNonQuery();
                inserted++;
            }
            catch (Exception ex)
            {
                failed++;
                string? idValue = TryExtractFirstValue(sql);
                failedRows.Add(new FailedRow(rowIndex, idValue, ex.Message));
            }

            if (progress != null && (rowIndex % reportInterval == 0 || rowIndex == total))
                progress.Report((rowIndex, total));
        }

        return new ExecuteResult(inserted, failed, failedRows);
    }

    public static ExecuteResult ExecuteInserts(
        string connectionString,
        IReadOnlyList<string> insertStatements,
        IProgress<(int current, int total)>? progress,
        CancellationToken token)
    {
        int inserted = 0;
        int failed = 0;
        var failedRows = new List<FailedRow>();

        using var conn = new SqlConnection(connectionString);
        conn.Open();

        int total = insertStatements.Count;
        int reportInterval = Math.Max(1, total / AppSettings.Execute.ReportIntervalDivisor);

        for (int i = 0; i < insertStatements.Count; i++)
        {
            token.ThrowIfCancellationRequested();
            string sql = insertStatements[i];

            try
            {
                using var cmd = new SqlCommand(sql, conn);
                cmd.CommandTimeout = 30;
                cmd.ExecuteNonQuery();
                inserted++;
            }
            catch (Exception ex)
            {
                failed++;
                string? idValue = TryExtractFirstValue(sql);
                failedRows.Add(new FailedRow(i + 1, idValue, ex.Message));
            }

            if (progress != null && (i % reportInterval == 0 || i == insertStatements.Count - 1))
                progress.Report((i + 1, total));
        }

        return new ExecuteResult(inserted, failed, failedRows);
    }
}
