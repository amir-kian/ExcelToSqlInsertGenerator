using ExcelToSqlInsertGenerator.Configuration;

namespace ExcelToSqlInsertGenerator;

public static class AppConstants
{
    public static string CustomTextOption => AppSettings.App.CustomTextOption;
    /// <summary>Source option: read from Condition column and apply ValueMap (when value = ... use).</summary>
    public const string ConditionOption = "(Condition)";
}
