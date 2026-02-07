using System.Windows;
using OfficeOpenXml;

namespace ExcelToSqlInsertGenerator;

public partial class App : Application
{
    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);
        ExcelPackage.License.SetNonCommercialPersonal("ExcelToSqlInsertGenerator");

        DispatcherUnhandledException += (s, args) =>
        {
            MessageBox.Show(args.Exception.ToString(), "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            args.Handled = true;
        };
    }
}
