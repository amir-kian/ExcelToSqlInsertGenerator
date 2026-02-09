using System;
using System.Collections.Generic;
using System.IO;
using OfficeOpenXml;

namespace ExcelToSqlInsertGenerator.Services;

public class ExcelReader
{
    public List<string> Headers { get; private set; } = new();
    public List<Dictionary<string, object>> Rows { get; private set; } = new();

    public void Load(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            throw new FileNotFoundException("Excel file not found.", path);

        using var package = new ExcelPackage(new FileInfo(path));
        var ws = package.Workbook.Worksheets[0];
        if (ws.Dimension == null)
        {
            Headers.Clear();
            Rows.Clear();
            return;
        }

        int cols = ws.Dimension.Columns;
        int dimensionRows = ws.Dimension.Rows;
        Headers.Clear();
        Rows.Clear();

        for (int col = 1; col <= cols; col++)
        {
            var header = ws.Cells[1, col].Text?.Trim() ?? "";
            if (string.IsNullOrEmpty(header))
                header = $"Column{col}";
            Headers.Add(header);
        }

        // Find actual last row with data (Dimension can extend to 1,048,575 due to Excel used range)
        int lastUsedRow = GetLastUsedRow(ws, dimensionRows, cols);

        for (int row = 2; row <= lastUsedRow; row++)
        {
            var dict = new Dictionary<string, object>();
            for (int col = 1; col <= cols; col++)
                dict[Headers[col - 1]] = ws.Cells[row, col].Value ?? DBNull.Value;
            Rows.Add(dict);
        }
    }

    /// <summary>Finds the last row that has at least one non-empty cell. Uses binary search so large Dimension (e.g. 1,048,575) is handled quickly.</summary>
    private static int GetLastUsedRow(ExcelWorksheet ws, int dimensionRows, int cols)
    {
        if (dimensionRows < 2) return 1;

        int low = 2;
        int high = dimensionRows;
        int lastFound = 1;

        while (low <= high)
        {
            int mid = low + (high - low) / 2;
            if (RowHasData(ws, mid, cols))
            {
                lastFound = mid;
                low = mid + 1;
            }
            else
                high = mid - 1;
        }

        return lastFound;
    }

    private static bool RowHasData(ExcelWorksheet ws, int row, int cols)
    {
        for (int col = 1; col <= cols; col++)
        {
            var v = ws.Cells[row, col].Value;
            if (v != null && !(v is string s && string.IsNullOrWhiteSpace(s)))
                return true;
        }
        return false;
    }
}