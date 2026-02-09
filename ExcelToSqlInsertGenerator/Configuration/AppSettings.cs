using Microsoft.Extensions.Configuration;

namespace ExcelToSqlInsertGenerator.Configuration;

public static class AppSettings
{
    private static IConfiguration? _config;
    private static readonly object Lock = new();

    private static IConfiguration Config
    {
        get
        {
            if (_config != null) return _config;
            lock (Lock)
            {
                if (_config != null) return _config;
                var basePath = AppContext.BaseDirectory;
                _config = new ConfigurationBuilder()
                    .SetBasePath(basePath)
                    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                    .Build();
                return _config;
            }
        }
    }

    public static AppSection App => GetOrCreate(ref _app, "App", () => new AppSection());
    public static SqlGeneratorSection SqlGenerator => GetOrCreate(ref _sqlGenerator, "SqlGenerator", () => new SqlGeneratorSection());
    public static ExecuteSection Execute => GetOrCreate(ref _execute, "Execute", () => new ExecuteSection());
    public static ValidateSection Validate => GetOrCreate(ref _validate, "Validate", () => new ValidateSection());

    private static AppSection? _app;
    private static SqlGeneratorSection? _sqlGenerator;
    private static ExecuteSection? _execute;
    private static ValidateSection? _validate;

    private static T GetOrCreate<T>(ref T? field, string section, Func<T> factory) where T : class
    {
        if (field != null) return field;
        lock (Lock)
        {
            if (field != null) return field;
            var instance = factory();
            Config.GetSection(section).Bind(instance);
            field = instance;
            return field;
        }
    }
}

/// <summary>General application settings.</summary>
public class AppSection
{
    /// <summary>Label for custom/fixed value option in the mapping dropdown.</summary>
    public string CustomTextOption { get; set; } = "(Custom text)";
}

/// <summary>SQL generation settings.</summary>
public class SqlGeneratorSection
{
    /// <summary>Max string length before truncation. Longer Excel values are truncated to avoid memory issues.</summary>
    public int MaxStringLength { get; set; } = 100_000;
    /// <summary>Progress reports during Generate: about every (total rows / this value) rows.</summary>
    public int ReportIntervalDivisor { get; set; } = 100;
}

/// <summary>Database execution settings.</summary>
public class ExecuteSection
{
    /// <summary>SQL command timeout in seconds for each INSERT or batch. Increase for slow servers.</summary>
    public int CommandTimeoutSeconds { get; set; } = 300;
    /// <summary>Connection timeout in seconds. Applied to connection string when not already set.</summary>
    public int ConnectionTimeoutSeconds { get; set; } = 120;
    /// <summary>Run GC every N rows to reduce memory growth during long runs.</summary>
    public int GcIntervalRows { get; set; } = 10_000;
    /// <summary>Rows per connection. A new connection is opened after each chunk. Lower = more resilient to connection drops (default 10000).</summary>
    public int ConnectionChunkRows { get; set; } = 10_000;
    /// <summary>Number of retries when opening connection fails (transient errors).</summary>
    public int ConnectionRetryCount { get; set; } = 2;
    /// <summary>Delay in milliseconds between connection retries.</summary>
    public int ConnectionRetryDelayMs { get; set; } = 1000;
    /// <summary>Number of retries when a batch execute fails (transient errors).</summary>
    public int BatchRetryCount { get; set; } = 1;
    /// <summary>INSERT statements per batch. Higher = faster, more memory per batch.</summary>
    public int InsertBatchSize { get; set; } = 50;
    /// <summary>Max failed row details to keep. Extra failures are counted but not logged.</summary>
    public int MaxFailedRowsToKeep { get; set; } = 2_000;
    /// <summary>Progress reports during Execute: about every (total rows / this value) rows.</summary>
    public int ReportIntervalDivisor { get; set; } = 200;
    /// <summary>Max length of ID value in failed row error logs.</summary>
    public int IdValueMaxLength { get; set; } = 50;
}

/// <summary>Pre-execution validation settings.</summary>
public class ValidateSection
{
    /// <summary>Progress reports during Validate: about every (total rows / this value) rows.</summary>
    public int ReportIntervalDivisor { get; set; } = 500;
    /// <summary>Max generated SQL length in chars before validation warning.</summary>
    public int MaxSqlLength { get; set; } = 2_000_000;
}
