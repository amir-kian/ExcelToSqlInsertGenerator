using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using ExcelToSqlInsertGenerator.Models;

namespace ExcelToSqlInsertGenerator.Services;

/// <summary>Cache the last execution mapping (column mappings) so it can be restored on next run.</summary>
public static class MappingCache
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    private static string CacheFilePath
    {
        get
        {
            var dir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "ExcelToSqlInsertGenerator");
            try { Directory.CreateDirectory(dir); } catch { /* ignore */ }
            return Path.Combine(dir, "last_mapping.json");
        }
    }

    /// <summary>DTO for serialization.</summary>
    private sealed class CachedMappingEntry
    {
        public string ColumnName { get; set; } = "";
        public string SqlType { get; set; } = "";
        public string? SelectedExcelColumn { get; set; }
        public bool UseFixedValue { get; set; }
        public string? FixedValue { get; set; }
        public string? ValueMap { get; set; }
        public string? ConditionColumn { get; set; }
    }

    /// <summary>Save current placeholder mappings to cache (call after or before execution).</summary>
    public static void Save(IReadOnlyList<SqlValuePlaceholder> placeholders)
    {
        if (placeholders == null || placeholders.Count == 0) return;
        try
        {
            var entries = new List<CachedMappingEntry>();
            foreach (var p in placeholders)
            {
                entries.Add(new CachedMappingEntry
                {
                    ColumnName = p.ColumnName ?? "",
                    SqlType = p.SqlType ?? "",
                    SelectedExcelColumn = p.SelectedExcelColumn,
                    UseFixedValue = p.UseFixedValue,
                    FixedValue = p.FixedValue,
                    ValueMap = p.ValueMap,
                    ConditionColumn = p.ConditionColumn
                });
            }
            var json = JsonSerializer.Serialize(entries, JsonOptions);
            File.WriteAllText(CacheFilePath, json);
        }
        catch { /* ignore */ }
    }

    /// <summary>Load cached mapping entries. Returns null if no cache or invalid file.</summary>
    private static List<CachedMappingEntry>? Load()
    {
        try
        {
            var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "ExcelToSqlInsertGenerator", "last_mapping.json");
            if (!File.Exists(path)) return null;
            var json = File.ReadAllText(path);
            var entries = JsonSerializer.Deserialize<List<CachedMappingEntry>>(json);
            return entries;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>Apply cached mapping to placeholders where ColumnName+SqlType match. Call after parsing template and when mapping grid is shown.</summary>
    public static void ApplyTo(IList<SqlValuePlaceholder> placeholders, IEnumerable<string>? validExcelColumns)
    {
        var cached = Load();
        if (cached == null || placeholders.Count == 0) return;
        var dict = new Dictionary<string, CachedMappingEntry>(StringComparer.OrdinalIgnoreCase);
        foreach (var e in cached)
            dict[e.ColumnName + "|" + e.SqlType] = e;

        foreach (var p in placeholders)
        {
            var key = (p.ColumnName ?? "") + "|" + (p.SqlType ?? "");
            if (!dict.TryGetValue(key, out var e)) continue;
            if (!string.IsNullOrEmpty(e.SelectedExcelColumn))
            {
                if (string.Equals(e.SelectedExcelColumn, ExcelToSqlInsertGenerator.AppConstants.ConditionOption, StringComparison.OrdinalIgnoreCase))
                    p.SelectedExcelColumn = ExcelToSqlInsertGenerator.AppConstants.ConditionOption;
                else if (validExcelColumns == null || validExcelColumns.Any(c => string.Equals(c, e.SelectedExcelColumn, StringComparison.OrdinalIgnoreCase)))
                    p.SelectedExcelColumn = e.SelectedExcelColumn;
                else
                    p.SelectedExcelColumn = null;
            }
            p.UseFixedValue = e.UseFixedValue;
            p.FixedValue = e.FixedValue;
            p.ValueMap = e.ValueMap;
            if (!string.IsNullOrEmpty(e.ConditionColumn) && (validExcelColumns == null || validExcelColumns.Any(c => string.Equals(c, e.ConditionColumn, StringComparison.OrdinalIgnoreCase))))
                p.ConditionColumn = e.ConditionColumn;
            else
                p.ConditionColumn = null;
        }
    }
}
