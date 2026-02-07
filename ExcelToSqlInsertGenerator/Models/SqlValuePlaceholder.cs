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
}