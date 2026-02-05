using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ExcelToSqlInsertGenerator.Models;

namespace ExcelToSqlInsertGenerator.Services;

public static class SqlTemplateParser
{
    public static List<SqlValuePlaceholder> Parse(string sql)
    {
        var list = new List<SqlValuePlaceholder>();


        var regex = new Regex(@"<(?<name>[^,]+),\s*(?<type>[^,]+),?>",
        RegexOptions.Multiline);


        foreach (Match match in regex.Matches(sql))
        {
            list.Add(new SqlValuePlaceholder
            {
                ColumnName = match.Groups["name"].Value.Trim(),
                SqlType = match.Groups["type"].Value.Trim()
            });
        }


        return list;
    }
}
