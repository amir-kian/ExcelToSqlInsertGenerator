using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ExcelToSqlInsertGenerator.Configuration;
using ExcelToSqlInsertGenerator.Models;
using ExcelToSqlInsertGenerator;

namespace ExcelToSqlInsertGenerator.Services;

using System.Text;

public static class SqlGenerator
{
    public static string Generate(
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        List<Dictionary<string, object>> rows,
        IProgress<(int current, int total)>? progress = null)
    {
        var sb = new StringBuilder();
        int total = rows.Count;
        int reportInterval = Math.Max(1, total / AppSettings.SqlGenerator.ReportIntervalDivisor);

        var valuesIndex = insertTemplate
            .IndexOf("VALUES", StringComparison.OrdinalIgnoreCase);

        if (valuesIndex == -1)
            throw new Exception("INSERT template must contain VALUES keyword.");

        var insertHeader = insertTemplate.Substring(0, valuesIndex + 6);

        for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++)
        {
            var row = rows[rowIndex];
            var values = placeholders
                .Select((p, colIndex) => ResolveValue(p, row, rowIndex + 2, colIndex));

            sb.AppendLine($"{insertHeader} ({string.Join(",", values)});");

            if (progress != null && (rowIndex % reportInterval == 0 || rowIndex == rows.Count - 1))
                progress.Report((rowIndex + 1, total));
        }

        return sb.ToString();
    }

    /// <summary>Generate a single INSERT for one row (for streaming execution without building full SQL).</summary>
    public static string GenerateSingleInsert(
        string insertTemplate,
        List<SqlValuePlaceholder> placeholders,
        Dictionary<string, object> row,
        int rowIndex)
    {
        var valuesIndex = insertTemplate.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase);
        if (valuesIndex == -1)
            throw new Exception("INSERT template must contain VALUES keyword.");
        var insertHeader = insertTemplate.Substring(0, valuesIndex + 6);
        var values = placeholders.Select((p, colIndex) => ResolveValue(p, row, rowIndex + 2, colIndex));
        return $"{insertHeader} ({string.Join(",", values)});";
    }

    private static string ResolveValue(
        SqlValuePlaceholder p,
        Dictionary<string, object> row,
        int rowNumber,
        int columnIndex)
    {
        string ColumnInfo() => $"Column '{p.ColumnName}' (Excel: {p.SelectedExcelColumn ?? ""}, Row {rowNumber})";

        if (p.UseFixedValue || p.SelectedExcelColumn == AppConstants.CustomTextOption)
            return string.IsNullOrEmpty(p.FixedValue) ? "NULL" : p.FixedValue;

        if (string.IsNullOrWhiteSpace(p.SelectedExcelColumn))
            return "NULL";

        int maxStringLength = AppSettings.SqlGenerator.MaxStringLength;
        string ToSafeString(object v)
        {
            var s = v?.ToString() ?? "";
            return s.Length > maxStringLength ? s.Substring(0, maxStringLength) + "…[truncated]" : s;
        }

        // (Condition): read from current row using INSERT column name as Excel column (case-insensitive), then apply ValueMap only
        if (p.SelectedExcelColumn == AppConstants.ConditionOption)
        {
            if (string.IsNullOrWhiteSpace(p.ValueMap) || string.IsNullOrWhiteSpace(p.ColumnName))
                return "NULL";
            if (!TryGetRowValue(row, p.ColumnName, out var condValue) || condValue == null || condValue == DBNull.Value)
                return "NULL";
            var condType = condValue.GetType();
            if (condType.IsArray && condValue is byte[]) return "NULL";
            if (condType.Name.Contains("Error", StringComparison.OrdinalIgnoreCase)) return "NULL";
            string condStr = ToSafeString(condValue).Trim();
            var mapped = TryApplyValueMap(condStr, p.ValueMap);
            return mapped ?? "NULL";
        }

        if (!TryGetRowValue(row, p.SelectedExcelColumn, out var value) || value == null || value == DBNull.Value)
            return "NULL";

        var valueType = value.GetType();
        if (valueType.IsArray && value is byte[]) return "NULL";
        if (valueType.Name.Contains("Error", StringComparison.OrdinalIgnoreCase)) return "NULL";

        string cellValueStr = ToSafeString(value).Trim();
        if (!string.IsNullOrEmpty(p.ValueMap))
        {
            var mapped = TryApplyValueMap(cellValueStr, p.ValueMap);
            if (mapped != null) return mapped;
        }

        try
        {
            if (p.SqlType.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase))
                return $"N'{ToSafeString(value).Replace("'", "''")}'";

            if (p.SqlType.StartsWith("uniqueidentifier", StringComparison.OrdinalIgnoreCase))
                return $"'{ToSafeString(value)}'";

            if (p.SqlType.Equals("bit", StringComparison.OrdinalIgnoreCase))
            {
                if (value is bool b) return b ? "1" : "0";
                if (value is int i) return i != 0 ? "1" : "0";
                if (value is string str && (str.Equals("1", StringComparison.OrdinalIgnoreCase) || str.Equals("true", StringComparison.OrdinalIgnoreCase) || str.Equals("yes", StringComparison.OrdinalIgnoreCase))) return "1";
                return Convert.ToBoolean(value) ? "1" : "0";
            }

            if (p.SqlType.StartsWith("datetime", StringComparison.OrdinalIgnoreCase) ||
                p.SqlType.StartsWith("date", StringComparison.OrdinalIgnoreCase))
            {
                DateTime dt = ToDateTime(value);
                return $"'{dt:yyyy-MM-dd HH:mm:ss.fff}'";
            }

            if (p.SqlType.StartsWith("varchar", StringComparison.OrdinalIgnoreCase))
                return $"'{ToSafeString(value).Replace("'", "''")}'";

            return ToSafeString(value);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"{ex.Message} — {ColumnInfo()} (value type: {value?.GetType().Name ?? "null"}, value: {value})", ex);
        }
    }

    /// <summary>If cell value matches a key in valueMap (e.g. 1=N'Man'), return the SQL expression. Keys match case-insensitive; numeric keys also match Excel doubles (e.g. 1.0 matches "1").</summary>
    private static string? TryApplyValueMap(string cellValueStr, string valueMap)
    {
        if (string.IsNullOrEmpty(valueMap)) return null;
        foreach (var part in valueMap.Split(new[] { ';', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            int eq = part.IndexOf('=');
            if (eq <= 0) continue;
            string key = part.Substring(0, eq).Trim();
            string sqlExpr = part.Substring(eq + 1).Trim();
            if (string.IsNullOrEmpty(sqlExpr)) continue;
            if (ValueMapKeyMatches(key, cellValueStr)) return sqlExpr;
        }
        return null;
    }

    private static bool ValueMapKeyMatches(string key, string cellValueStr)
    {
        if (string.IsNullOrEmpty(cellValueStr) && string.IsNullOrEmpty(key)) return true;
        if (string.IsNullOrEmpty(cellValueStr) || string.IsNullOrEmpty(key)) return false;
        if (string.Equals(key.Trim(), cellValueStr.Trim(), StringComparison.OrdinalIgnoreCase)) return true;
        if (double.TryParse(key, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var keyNum) &&
            double.TryParse(cellValueStr, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var cellNum))
            return Math.Abs(keyNum - cellNum) < 1e-9;
        return false;
    }

    /// <summary>Gets value from row by column name with case-insensitive key match (Excel headers may differ in casing from INSERT column names).</summary>
    private static bool TryGetRowValue(Dictionary<string, object> row, string columnName, out object? value)
    {
        if (row.TryGetValue(columnName, out value!)) return true;
        var key = row.Keys.FirstOrDefault(k => string.Equals(k, columnName, StringComparison.OrdinalIgnoreCase));
        if (key != null)
        {
            value = row[key];
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>Converts a value to DateTime; handles Excel OLE date (double) and standard DateTime.</summary>
    private static DateTime ToDateTime(object value)
    {
        if (value is DateTime dt)
            return dt;
        if (value is double d)
            return DateTime.FromOADate(d);
        if (value is int i)
            return DateTime.FromOADate(i);
        if (value is long l)
            return DateTime.FromOADate(l);
        if (value is string s && DateTime.TryParse(s, out var parsed))
            return parsed;
        return Convert.ToDateTime(value);
    }
}
