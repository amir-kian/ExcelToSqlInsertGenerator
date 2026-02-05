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

        SetGenerateStatus("Generating INSERT script...", isError: false);
        GenerateProgressBar.Visibility = Visibility.Visible;
        BtnGenerate.IsEnabled = false;

        var template = _insertTemplate;
        var placeholders = Placeholders.ToList();
        var rows = _excelReader.Rows;

        try
        {
            var sql = await System.Threading.Tasks.Task.Run(() =>
                SqlGenerator.Generate(template, placeholders, rows)).ConfigureAwait(true);

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

    private void BtnSave_Click(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrEmpty(TxtOutput.Text)) return;
        var dlg = new SaveFileDialog
        {
            Filter = "SQL files (*.sql)|*.sql|All files (*.*)|*.*",
            DefaultExt = ".sql",
            Title = "Save SQL script"
        };
        if (dlg.ShowDialog() != true) return;
        try
        {
            File.WriteAllText(dlg.FileName, TxtOutput.Text);
            MessageBox.Show(this, "Script saved.", "Done", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, "Save failed: " + ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }
}
