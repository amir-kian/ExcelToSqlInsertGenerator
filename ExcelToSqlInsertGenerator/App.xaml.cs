using System.Threading.Tasks;
using System.Windows;
using ExcelToSqlInsertGenerator.Services;
using OfficeOpenXml;

namespace ExcelToSqlInsertGenerator;

public partial class App : Application
{
    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);
        ExecuteLogger.EnsureLogDirectoryExists();
        ExcelPackage.License.SetNonCommercialPersonal("ExcelToSqlInsertGenerator");

        DispatcherUnhandledException += (s, args) =>
        {
            MessageBox.Show(args.Exception.ToString(), "Unhandled Error", MessageBoxButton.OK, MessageBoxImage.Error);
            args.Handled = true;
        };

        TaskScheduler.UnobservedTaskException += (s, args) =>
        {
            MessageBox.Show(args.Exception?.ToString() ?? "Unknown task error", "Background Task Error", MessageBoxButton.OK, MessageBoxImage.Error);
            args.SetObserved();
        };

        AppDomain.CurrentDomain.UnhandledException += (s, args) =>
        {
            var ex = args.ExceptionObject as Exception;
            MessageBox.Show(ex?.ToString() ?? args.ExceptionObject?.ToString() ?? "Fatal error", "Fatal Error", MessageBoxButton.OK, MessageBoxImage.Error);
        };
    }
}
