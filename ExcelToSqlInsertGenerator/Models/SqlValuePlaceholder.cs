namespace ExcelToSqlInsertGenerator.Models;

public class SqlValuePlaceholder
{
    public string ColumnName { get; set; } = string.Empty;
    public string SqlType { get; set; } = string.Empty;

    /// <summary>Excel column header to take the value from; null if using fixed value.</summary>
    public string? SelectedExcelColumn { get; set; }

    /// <summary>If true, use FixedValue instead of Excel column.</summary>
    public bool UseFixedValue { get; set; }

    /// <summary>Literal SQL fragment when UseFixedValue is true (e.g. GETDATE(), N'constant', NULL).</summary>
    public string? FixedValue { get; set; }

    /// <summary>When Excel value equals X use SQL Y. Format: 1=N'Man';2=N'Woman' (semicolon-separated, each part is value=SQL).</summary>
    public string? ValueMap { get; set; }

    /// <summary>When Source = (Condition), the Excel column to read the value from for the ValueMap lookup.</summary>
    public string? ConditionColumn { get; set; }
}