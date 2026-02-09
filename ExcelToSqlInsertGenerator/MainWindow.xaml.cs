using System.Collections.ObjectModel;
using System.IO;
using System.Windows;
using System.Windows.Media;
using Microsoft.Win32;
using ExcelToSqlInsertGenerator.Models;
using ExcelToSqlInsertGenerator.Services;

namespace ExcelToSqlInsertGenerator;

public partial class MainWindow : Window
{
    public ObservableCollection<string> ExcelColumns { get; } = new();
    /// <summary>Excel columns plus "(Custom text)" for the mapping dropdown.</summary>
    public ObservableCollection<string> MappingOptions { get; } = new();
    public ObservableCollection<SqlValuePlaceholder> Placeholders { get; } = new();

    private string _insertTemplate = string.Empty;
    private ExcelReader? _excelReader;
    private System.Threading.CancellationTokenSource? _saveCts;
    private System.Threading.CancellationTokenSource? _executeCts;

    public MainWindow()
    {
        InitializeComponent();
        DataContext = this;
        var cached = ConnectionStringCache.Load();
        if (!string.IsNullOrEmpty(cached))
            TxtConnectionString.Text = cached;
    }

    private void BtnParse_Click(object sender, RoutedEventArgs e)
    {
        var sql = TxtInsertTemplate.Text?.Trim();
        if (string.IsNullOrEmpty(sql))
        {
            TxtParseStatus.Text = "Enter a sample INSERT statement.";
            return;
        }

        try
        {
            ParseTemplateFromText(sql);
            TxtParseStatus.Text = Placeholders.Count > 0
                ? $"Parsed {Placeholders.Count} placeholder(s)."
                : "No placeholders found. Use <ColumnName, type> e.g. <Name, nvarchar(200)>.";
            ShowMappingIfReady();
        }
        catch (Exception ex)
        {
            TxtParseStatus.Text = "Error: " + ex.Message;
        }
    }

    private async void BtnLoadExcel_Click(object sender, RoutedEventArgs e)
    {
        var dlg = new OpenFileDialog
        {
            Filter = "Excel files (*.xlsx;*.xls)|*.xlsx;*.xls|All files (*.*)|*.*",
            Title = "Select Excel file"
        };
        if (dlg.ShowDialog() != true) return;

        var filePath = dlg.FileName;
        TxtExcelPath.Text = filePath;
        SetExcelStatus("Loading Excel...", isError: false);
        ExcelProgressBar.Visibility = Visibility.Visible;
        BtnLoadExcel.IsEnabled = false;

        try
        {
            var reader = await System.Threading.Tasks.Task.Run(() =>
            {
                var r = new ExcelReader();
                r.Load(filePath);
                return r;
            }).ConfigureAwait(true);

            _excelReader = reader;
            ExcelColumns.Clear();
            MappingOptions.Clear();
            foreach (var h in _excelReader.Headers)
            {
                ExcelColumns.Add(h);
                MappingOptions.Add(h);
            }
            MappingOptions.Add(AppConstants.CustomTextOption);
            MappingOptions.Add(AppConstants.ConditionOption);

            int cols = _excelReader.Headers.Count;
            int rows = _excelReader.Rows.Count;
            SetExcelStatus($"Loaded: {cols} column(s), {rows} data row(s).", isError: false);

            // If template is already in the box but not parsed yet, parse it so Generate enables
            if (Placeholders.Count == 0 && !string.IsNullOrWhiteSpace(TxtInsertTemplate.Text))
                ParseTemplateFromText(TxtInsertTemplate.Text.Trim());

            ShowMappingIfReady();
        }
        catch (Exception ex)
        {
            string fullError = GetFullExceptionMessage(ex);
            SetExcelStatus("Error: " + ex.Message, isError: true);
            MessageBox.Show(this, "Load Excel failed:\n\n" + fullError, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            ExcelProgressBar.Visibility = Visibility.Collapsed;
            BtnLoadExcel.IsEnabled = true;
        }
    }

    private void SetExcelStatus(string text, bool isError)
    {
        TxtExcelStatus.Text = text;
        TxtExcelStatus.Foreground = isError ? Brushes.DarkRed : new SolidColorBrush(Color.FromRgb(0x33, 0x33, 0x33));
        TxtExcelStatus.Visibility = string.IsNullOrEmpty(text) ? Visibility.Collapsed : Visibility.Visible;
    }

    private void ParseTemplateFromText(string sql)
    {
        try
        {
            var parsed = SqlTemplateParser.Parse(sql);
            _insertTemplate = sql;
            Placeholders.Clear();
            foreach (var p in parsed)
                Placeholders.Add(p);
            TxtParseStatus.Text = parsed.Count > 0 ? $"Parsed {parsed.Count} placeholder(s)." : "";
        }
        catch
        {
            // Ignore parse errors when auto-parsing on Excel load
        }
    }

    private void ShowMappingIfReady()
    {
        if (Placeholders.Count > 0 && _excelReader != null && _excelReader.Headers.Count > 0)
        {
            GroupMapping.Visibility = Visibility.Visible;
            GridMappings.ItemsSource = null;
            GridMappings.ItemsSource = Placeholders;
            MappingCache.ApplyTo(Placeholders, MappingOptions);
            GridMappings.ItemsSource = null;
            GridMappings.ItemsSource = Placeholders;
            BtnGenerate.IsEnabled = true;
            BtnExecute.IsEnabled = true;
            BtnValidate.IsEnabled = true;
        }
    }

    private async void BtnGenerate_Click(object sender, RoutedEventArgs e)
    {
        if (_excelReader == null || string.IsNullOrEmpty(_insertTemplate))
        {
            SetGenerateStatus("Parse a template and load an Excel file first.", isError: true);
            return;
        }

        var rows = _excelReader.Rows;
        SetGenerateStatus("Generating: 0 / " + rows.Count + " rows...", isError: false);
        GenerateProgressBar.Visibility = Visibility.Visible;
        GenerateProgressBar.IsIndeterminate = false;
        GenerateProgressBar.Maximum = 100;
        GenerateProgressBar.Value = 0;
        BtnGenerate.IsEnabled = false;

        var template = _insertTemplate;
        var placeholders = Placeholders.ToList();
        var progress = new System.Progress<(int current, int total)>(p =>
        {
            GenerateProgressBar.Value = p.total > 0 ? (p.current * 100.0 / p.total) : 0;
            TxtGenerateStatus.Text = $"Generating: {p.current:N0} / {p.total:N0} rows...";
        });

        try
        {
            var sql = await System.Threading.Tasks.Task.Run(() =>
                SqlGenerator.Generate(template, placeholders, rows, progress)).ConfigureAwait(true);

            TxtOutput.Text = sql;
            int insertCount = rows.Count;
            SetGenerateStatus($"Generated {insertCount} INSERT statement(s).", isError: false);
            BtnCopy.IsEnabled = true;
            BtnSave.IsEnabled = true;
            BtnExecute.IsEnabled = true;
        }
        catch (Exception ex)
        {
            string fullError = GetFullExceptionMessage(ex);
            SetGenerateStatus("Error: " + ex.Message, isError: true);
            TxtOutput.Clear();
            MessageBox.Show(this, "Generate failed:\n\n" + fullError, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            GenerateProgressBar.Visibility = Visibility.Collapsed;
            BtnGenerate.IsEnabled = true;
            BtnGenerate.Focus(); // Draw attention: generation is done, button is ready again
        }
    }

    private void SetGenerateStatus(string text, bool isError)
    {
        TxtGenerateStatus.Text = text;
        TxtGenerateStatus.Foreground = isError ? Brushes.DarkRed : new SolidColorBrush(Color.FromRgb(0x33, 0x33, 0x33));
        TxtGenerateStatus.Visibility = string.IsNullOrEmpty(text) ? Visibility.Collapsed : Visibility.Visible;
    }

    private void BtnReset_Click(object sender, RoutedEventArgs e)
    {
        // Reset mapping grid: clear all column selections and custom text
        foreach (var p in Placeholders)
        {
            p.SelectedExcelColumn = null;
            p.FixedValue = null;
            p.UseFixedValue = false;
            p.ValueMap = null;
            p.ConditionColumn = null;
        }
        GridMappings.ItemsSource = null;
        GridMappings.ItemsSource = Placeholders;

        // Clear generated output and status
        TxtOutput.Clear();
        SetGenerateStatus("", isError: false);
        BtnCopy.IsEnabled = false;
        BtnSave.IsEnabled = false;
        BtnExecute.IsEnabled = false;
        BtnValidate.IsEnabled = false;
        TxtExecuteLog.Clear();
    }

    private void BtnCopy_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrEmpty(TxtOutput.Text)) return;
        try
        {
            Clipboard.SetText(TxtOutput.Text);
            MessageBox.Show(this, "SQL copied to clipboard.", "Done", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, "Copy failed: " + ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void BtnSave_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrEmpty(TxtOutput.Text)) return;

        int chunkSize = 0;
        if (!string.IsNullOrWhiteSpace(TxtChunkSize.Text) && int.TryParse(TxtChunkSize.Text.Trim(), out var parsed))
            chunkSize = Math.Max(0, parsed);

        var dlg = new SaveFileDialog
        {
            Filter = "SQL files (*.sql)|*.sql|All files (*.*)|*.*",
            DefaultExt = ".sql",
            Title = chunkSize > 0 ? "Save SQL script (chunked) - pick base file" : "Save SQL script"
        };
        if (dlg.ShowDialog() != true) return;

        BtnSave.IsEnabled = false;
        BtnCancelSave.Visibility = Visibility.Visible;
        _saveCts = new System.Threading.CancellationTokenSource();
        var token = _saveCts.Token;

        // Capture UI values on UI thread before background work (avoid cross-thread access)
        string filePath = dlg.FileName;
        string output = TxtOutput.Text;

        try
        {
            if (chunkSize <= 0)
            {
                await System.Threading.Tasks.Task.Run(() =>
                    File.WriteAllText(filePath, output), token).ConfigureAwait(true);
                MessageBox.Show(this, "Script saved.", "Done", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var result = await System.Threading.Tasks.Task.Run(() => SaveChunked(output, filePath, chunkSize, token)).ConfigureAwait(true);
            MessageBox.Show(this, $"Saved {result.written} file(s) ({result.total} INSERT statements, {chunkSize} per file).", "Done", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (OperationCanceledException)
        {
            MessageBox.Show(this, "Save cancelled.", "Cancelled", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, "Save failed:\n\n" + GetFullExceptionMessage(ex), "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            _saveCts?.Dispose();
            _saveCts = null;
            BtnSave.IsEnabled = true;
            BtnCancelSave.Visibility = Visibility.Collapsed;
        }
    }

    private (int written, int total) SaveChunked(string text, string filePath, int chunkSize, System.Threading.CancellationToken token)
    {
        var lines = text.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
        var inserts = new List<string>();
        var current = new System.Text.StringBuilder();

        foreach (var line in lines)
        {
            token.ThrowIfCancellationRequested();
            current.AppendLine(line);
            if (line.TrimEnd().EndsWith(");"))
            {
                inserts.Add(current.ToString());
                current.Clear();
            }
        }
        if (current.Length > 0)
            inserts.Add(current.ToString());

        string dir = Path.GetDirectoryName(filePath)!;
        string baseName = Path.GetFileNameWithoutExtension(filePath);
        string ext = Path.GetExtension(filePath);

        int chunkIndex = 1;
        int written = 0;
        for (int i = 0; i < inserts.Count; i += chunkSize)
        {
            token.ThrowIfCancellationRequested();
            var chunk = inserts.Skip(i).Take(chunkSize);
            var content = string.Join(Environment.NewLine, chunk);
            string fileName = Path.Combine(dir, $"{baseName}_part{chunkIndex:D3}{ext}");
            File.WriteAllText(fileName, content);
            written++;
            chunkIndex++;
        }

        return (written, inserts.Count);
    }

    private void BtnCancelSave_Click(object sender, RoutedEventArgs e)
    {
        _saveCts?.Cancel();
    }

    private async void BtnValidate_Click(object sender, RoutedEventArgs e)
    {
        if (_excelReader == null || string.IsNullOrEmpty(_insertTemplate) || Placeholders.Count == 0)
        {
            MessageBox.Show(this, "Parse template, load Excel, and map columns first.", "Not ready", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        var template = _insertTemplate;
        var placeholders = Placeholders.ToList();
        var rows = _excelReader.Rows;
        if (rows.Count == 0)
        {
            MessageBox.Show(this, "No data rows to validate.", "Not ready", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        BtnValidate.IsEnabled = false;
        ExecuteProgressBar.Visibility = Visibility.Visible;
        ExecuteProgressBar.Value = 0;
        ExecuteProgressBar.Maximum = 100;
        TxtExecuteLog.Clear();
        TxtExecuteStatus.Text = $"Validating: 0 / {rows.Count:N0}...";
        TxtExecuteStatus.Visibility = Visibility.Visible;

        var progress = new System.Progress<(int current, int total)>(p =>
        {
            ExecuteProgressBar.Value = p.total > 0 ? (p.current * 100.0 / p.total) : 0;
            TxtExecuteStatus.Text = $"Validating: {p.current:N0} / {p.total:N0}...";
        });

        try
        {
            var result = await System.Threading.Tasks.Task.Run(() =>
                DbExecutor.ValidateBeforeExecute(template, placeholders, rows, progress, System.Threading.CancellationToken.None)).ConfigureAwait(true);

            var log = new System.Text.StringBuilder();
            if (result.Ok)
            {
                log.AppendLine("Validation OK: All rows can generate valid SQL.");
            }
            else
            {
                log.AppendLine($"Validation found {result.Issues.Count} problematic row(s):");
                log.AppendLine();
                int show = Math.Min(result.Issues.Count, 100);
                for (int i = 0; i < show; i++)
                {
                    var issue = result.Issues[i];
                    log.AppendLine($"  Row {issue.RowIndex}: {issue.ErrorMessage}");
                }
                if (result.Issues.Count > show)
                    log.AppendLine($"  ... and {result.Issues.Count - show} more.");
            }
            TxtExecuteLog.Text = log.ToString();
            TxtExecuteStatus.Text = result.Ok ? "Validation OK." : $"Validation: {result.Issues.Count} issue(s) found.";
            MessageBox.Show(this, result.Ok ? "All rows validated OK." : $"Found {result.Issues.Count} problematic row(s). See log for details.", "Validate", MessageBoxButton.OK, result.Ok ? MessageBoxImage.Information : MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            TxtExecuteLog.Text = "Error: " + GetFullExceptionMessage(ex);
            TxtExecuteStatus.Text = "Error.";
            MessageBox.Show(this, "Validation failed:\n\n" + ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            BtnValidate.IsEnabled = true;
            ExecuteProgressBar.Visibility = Visibility.Collapsed;
        }
    }

    private async void BtnExecute_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrWhiteSpace(TxtConnectionString.Text))
        {
            MessageBox.Show(this, "Enter a connection string.", "Not ready", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }
        if (_excelReader == null || string.IsNullOrEmpty(_insertTemplate) || Placeholders.Count == 0)
        {
            MessageBox.Show(this, "Parse template, load Excel, and map columns first.", "Not ready", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        string connectionString = TxtConnectionString.Text;
        string template = _insertTemplate;
        var placeholders = Placeholders.ToList();
        var rows = _excelReader.Rows;
        int totalCount = rows.Count;

        if (totalCount == 0)
        {
            MessageBox.Show(this, "No data rows to execute.", "Not ready", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        int startRow = 0;
        int skipRows = 0;
        int chunkSize = 0;
        if (!string.IsNullOrWhiteSpace(TxtExecuteStartRow.Text) && int.TryParse(TxtExecuteStartRow.Text.Trim(), out var sr))
            startRow = Math.Max(0, sr);
        if (!string.IsNullOrWhiteSpace(TxtExecuteSkipRows.Text) && int.TryParse(TxtExecuteSkipRows.Text.Trim(), out var skip) && skip > 0)
            skipRows = skip;
        if (!string.IsNullOrWhiteSpace(TxtExecuteChunkSize.Text) && int.TryParse(TxtExecuteChunkSize.Text.Trim(), out var cs))
            chunkSize = Math.Max(0, cs);
        int originalStartRow = startRow;
        startRow = Math.Min(startRow + skipRows, totalCount);
        if (skipRows > 0)
            ExecuteLogger.LogSkippedRange(originalStartRow, startRow - 1, startRow);

        bool parallelMode = ChkExecuteParallel.IsChecked == true;
        if (parallelMode && chunkSize <= 0)
        {
            chunkSize = Math.Max(10000, totalCount / 4);
            TxtExecuteChunkSize.Text = chunkSize.ToString();
        }

        MappingCache.Save(Placeholders);
        ConnectionStringCache.Save(TxtConnectionString.Text);

        BtnExecute.IsEnabled = false;
        BtnCancelExecute.Visibility = Visibility.Visible;
        ExecuteProgressBar.Visibility = Visibility.Visible;
        ExecuteProgressBar.Value = 0;
        ExecuteProgressBar.Maximum = 100;
        TxtExecuteLog.Clear();
        TxtExecuteStatus.Text = $"Executing: 0 / {totalCount:N0}...";
        TxtExecuteStatus.Visibility = Visibility.Visible;

        _executeCts = new System.Threading.CancellationTokenSource();
        var token = _executeCts.Token;
        var progress = new System.Progress<(int current, int total)>(p =>
        {
            Dispatcher.BeginInvoke(() =>
            {
                try
                {
                    ExecuteProgressBar.Value = p.total > 0 ? (p.current * 100.0 / p.total) : 0;
                    TxtExecuteStatus.Text = $"Executing: {p.current:N0} / {p.total:N0}...";
                }
                catch { /* avoid UI update crashing the app */ }
            }, System.Windows.Threading.DispatcherPriority.Background);
        });

        try
        {
            var result = parallelMode
                ? await System.Threading.Tasks.Task.Run(() =>
                    DbExecutor.ExecuteFromSourceParallelChunks(connectionString, template, placeholders, rows, progress, token, startRow, chunkSize)).ConfigureAwait(true)
                : await System.Threading.Tasks.Task.Run(() =>
                    DbExecutor.ExecuteFromSourceSafe(connectionString, template, placeholders, rows, progress, token, startRow, chunkSize)).ConfigureAwait(true);

            try
            {
            var log = new System.Text.StringBuilder();
            int lastProcessed = startRow + result.Inserted + result.Failed;
            if (result.LastProcessedRow.HasValue) lastProcessed = result.LastProcessedRow.Value;
            log.AppendLine($"Inserted: {result.Inserted}");
            log.AppendLine($"Failed:   {result.Failed}");
            if (!string.IsNullOrEmpty(result.StoppedWithError))
            {
                log.AppendLine();
                log.AppendLine($"Stopped at row {lastProcessed}: {result.StoppedWithError}");
                log.AppendLine($"Set Start row to {lastProcessed} and run again to resume.");
                log.AppendLine("If it stops again at the same row, set Skip rows to 500–1000 to jump past the bad range.");
            }
            if (chunkSize > 0 && lastProcessed < totalCount && string.IsNullOrEmpty(result.StoppedWithError))
                log.AppendLine($"Processed rows {startRow}-{lastProcessed - 1}. To continue, set Start row to {lastProcessed} and run again.");
            if (result.FailedRows.Count > 0)
            {
                log.AppendLine();
                log.AppendLine("Failed rows (Row | ID value | Error):");
                foreach (var fr in result.FailedRows)
                {
                    if (fr.RowIndex < 0)
                        log.AppendLine($"  {fr.ErrorMessage}");
                    else
                    {
                        string idInfo = !string.IsNullOrEmpty(fr.IdValue) ? fr.IdValue : "(no ID)";
                        log.AppendLine($"  Row {fr.RowIndex}: ID={idInfo} | {fr.ErrorMessage}");
                    }
                }
            }
            string logPath = ExecuteLogger.GetLogFilePath();
            log.AppendLine();
            log.AppendLine($"Log file: {logPath}");
            TxtExecuteLog.Text = log.ToString();
            TxtExecuteStatus.Text = $"Done. Inserted: {result.Inserted}, Failed: {result.Failed}";
            if (!string.IsNullOrEmpty(result.StoppedWithError))
            {
                TxtExecuteStartRow.Text = lastProcessed.ToString();
                MessageBox.Show(this, $"Stopped at row {lastProcessed}:\n{result.StoppedWithError}\n\nInserted: {result.Inserted}, Failed: {result.Failed}\n\nStart row updated to {lastProcessed}. Run again to resume.\n\nIf it stops again at the same row, set Skip rows to 500 or 1000 and run again to jump past the problematic range.", "Execution stopped", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
            else if (chunkSize > 0 && lastProcessed < totalCount)
            {
                TxtExecuteStartRow.Text = lastProcessed.ToString();
                MessageBox.Show(this, $"Chunk complete. Inserted: {result.Inserted}, Failed: {result.Failed}\n\nStart row updated to {lastProcessed}. Run again to continue.", "Chunk complete", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show(this, $"Inserted: {result.Inserted}\nFailed: {result.Failed}", "Execute complete", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            }
            catch (Exception uiEx)
            {
                ExecuteLogger.Error("Error updating UI after execute", uiEx);
                TxtExecuteLog.Text = $"Inserted: {result.Inserted}, Failed: {result.Failed}. Error showing details: {uiEx.Message}";
                TxtExecuteStatus.Text = "Done (see log).";
                MessageBox.Show(this, $"Execute finished but failed to show full details.\n\nInserted: {result.Inserted}, Failed: {result.Failed}\n\nSee log: " + ExecuteLogger.GetLogFilePath(), "Execute complete", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
        }
        catch (OperationCanceledException)
        {
            TxtExecuteLog.Text = "Execution cancelled.";
            TxtExecuteStatus.Text = "Cancelled.";
            MessageBox.Show(this, "Execution cancelled.", "Cancelled", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (AggregateException agg)
        {
            var inner = agg.Flatten().InnerException ?? agg;
            if (inner is OperationCanceledException)
            {
                TxtExecuteLog.Text = "Execution cancelled.";
                TxtExecuteStatus.Text = "Cancelled.";
                MessageBox.Show(this, "Execution cancelled.", "Cancelled", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                string fullError = GetFullExceptionMessage(inner);
                ExecuteLogger.Error("Execute failed (AggregateException)", inner);
                TxtExecuteLog.Text = "Error: " + fullError + "\n\nLog file: " + ExecuteLogger.GetLogFilePath();
                TxtExecuteStatus.Text = "Error.";
                MessageBox.Show(this, "Execute failed:\n\n" + fullError + "\n\nSee log file for details.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        catch (Exception ex)
        {
            string fullError = GetFullExceptionMessage(ex);
            ExecuteLogger.Error("Execute failed", ex);
            TxtExecuteLog.Text = "Error: " + fullError + "\n\nLog file: " + ExecuteLogger.GetLogFilePath();
            TxtExecuteStatus.Text = "Error.";
            MessageBox.Show(this, "Execute failed:\n\n" + fullError + "\n\nSee log file for details.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            _executeCts?.Dispose();
            _executeCts = null;
            BtnExecute.IsEnabled = true;
            BtnCancelExecute.IsEnabled = true;
            BtnCancelExecute.Content = "Cancel";
            BtnCancelExecute.Visibility = Visibility.Collapsed;
            ExecuteProgressBar.Visibility = Visibility.Collapsed;
        }
    }

    private void BtnCancelExecute_Click(object sender, RoutedEventArgs e)
    {
        if (_executeCts == null) return;
        BtnCancelExecute.IsEnabled = false;
        BtnCancelExecute.Content = "Cancelling...";
        _executeCts.Cancel();
    }

    private void BtnResumeFromCheckpoint_Click(object sender, RoutedEventArgs e)
    {
        var row = DbExecutor.ReadCheckpoint();
        if (row.HasValue)
        {
            TxtExecuteStartRow.Text = row.Value.ToString();
            TxtExecuteSkipRows.Text = "0";
            MessageBox.Show(this, $"Start row set to {row.Value} from checkpoint.\n\nIf it crashes again at the same spot, set Skip rows to 500 or 1000 to jump past the problematic range, then click Execute.", "Resume from checkpoint", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        else
        {
            MessageBox.Show(this, "No checkpoint file found. Run Execute first to create one.", "No checkpoint", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }

    private static string GetFullExceptionMessage(Exception ex)
    {
        if (ex is AggregateException agg)
            ex = agg.InnerException ?? agg;
        var sb = new System.Text.StringBuilder();
        sb.Append(ex.Message);
        if (ex.InnerException != null)
            sb.Append("\n\nInner: ").Append(GetFullExceptionMessage(ex.InnerException));
        sb.Append("\n\n").Append(ex.StackTrace);
        return sb.ToString();
    }
}
