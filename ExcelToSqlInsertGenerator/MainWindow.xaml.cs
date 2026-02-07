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

    public MainWindow()
    {
        InitializeComponent();
        DataContext = this;
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
            string message = ex.Message;
            if (ex.InnerException != null)
                message += " " + ex.InnerException.Message;
            SetExcelStatus("Error: " + message, isError: true);
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
            BtnGenerate.IsEnabled = true;
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
        }
        catch (Exception ex)
        {
            string message = ex.Message;
            if (ex.InnerException != null)
                message += " — " + ex.InnerException.Message;
            SetGenerateStatus("Error: " + message, isError: true);
            TxtOutput.Clear();
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
        }
        GridMappings.ItemsSource = null;
        GridMappings.ItemsSource = Placeholders;

        // Clear generated output and status
        TxtOutput.Clear();
        SetGenerateStatus("", isError: false);
        BtnCopy.IsEnabled = false;
        BtnSave.IsEnabled = false;
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

        try
        {
            if (chunkSize <= 0)
            {
                await System.Threading.Tasks.Task.Run(() =>
                    File.WriteAllText(dlg.FileName, TxtOutput.Text), token).ConfigureAwait(true);
                MessageBox.Show(this, "Script saved.", "Done", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var output = TxtOutput.Text;
            var result = await System.Threading.Tasks.Task.Run(() => SaveChunked(output, dlg.FileName, chunkSize, token)).ConfigureAwait(true);
            MessageBox.Show(this, $"Saved {result.written} file(s) ({result.total} INSERT statements, {chunkSize} per file).", "Done", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (OperationCanceledException)
        {
            MessageBox.Show(this, "Save cancelled.", "Cancelled", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, "Save failed: " + ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
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
}
