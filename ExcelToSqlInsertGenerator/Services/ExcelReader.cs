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
        int rows = ws.Dimension.Rows;
        Headers.Clear();
        Rows.Clear();

        for (int col = 1; col <= cols; col++)
        {
            var header = ws.Cells[1, col].Text?.Trim() ?? "";
            if (string.IsNullOrEmpty(header))
                header = $"Column{col}";
            Headers.Add(header);
        }

        for (int row = 2; row <= rows; row++)
        {
            var dict = new Dictionary<string, object>();
            for (int col = 1; col <= cols; col++)
                dict[Headers[col - 1]] = ws.Cells[row, col].Value ?? DBNull.Value;
            Rows.Add(dict);
        }
    }
}