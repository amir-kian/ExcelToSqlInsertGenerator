# appsettings.json – Configuration Reference

## App
| Variable | Type | Default | Usage |
|----------|------|---------|-------|
| CustomTextOption | string | "(Custom text)" | Label shown in the mapping dropdown when user selects custom/fixed value instead of an Excel column. |

---

## SqlGenerator
| Variable | Type | Default | Usage |
|----------|------|---------|-------|
| MaxStringLength | int | 100000 | Maximum length of a string value before truncation. Longer Excel values are truncated to avoid memory issues. |
| ReportIntervalDivisor | int | 100 | Progress reports during Generate: about every (total rows / this value) rows. Higher = fewer updates. |

---

## Execute
| Variable | Type | Default | Usage |
|----------|------|---------|-------|
| CommandTimeoutSeconds | int | 300 | SQL command timeout in seconds for each INSERT or batch. Increase for slow servers. |
| ConnectionTimeoutSeconds | int | 120 | Connection timeout in seconds when opening SQL connections. Applied to connection string when not already set. |
| GcIntervalRows | int | 5000 | Run garbage collection every N rows to reduce memory growth. Lower = less crash risk. |
| ConnectionChunkRows | int | 5000 | Number of rows per connection. Lower = more resilient to silent crashes. |
| CheckpointIntervalRows | int | 25000 | Write progress to temp file every N rows. Use "Resume from checkpoint" after silent crash. |
| InsertBatchSize | int | 50 | Number of INSERT statements sent in a single batch to SQL Server. Higher = faster, but more memory per batch. |
| MaxFailedRowsToKeep | int | 2000 | Maximum failed row details kept in memory. Extra failures are counted but not logged. |
| ReportIntervalDivisor | int | 200 | Progress reports during Execute: about every (total rows / this value) rows. |
| IdValueMaxLength | int | 50 | Maximum length of the ID value shown in failed row error logs. Longer values are truncated. |
| ConnectionRetryCount | int | 2 | Number of retries when opening connection fails (transient errors). |
| ConnectionRetryDelayMs | int | 1000 | Delay in milliseconds between connection retries. |
| BatchRetryCount | int | 1 | Number of retries when a batch execute fails before falling back to one-by-one. |

---

## Validate
| Variable | Type | Default | Usage |
|----------|------|---------|-------|
| ReportIntervalDivisor | int | 500 | Progress reports during Validate: about every (total rows / this value) rows. |
| MaxSqlLength | int | 2000000 | Maximum generated SQL length in characters before a validation warning. Rows exceeding this are flagged. |
