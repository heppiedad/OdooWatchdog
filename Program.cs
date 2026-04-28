using System.Net;
using System.Net.Http.Headers;
using System.Net.Mail;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

//Bash > dotnet publish OdooWatchdog.csproj -c Release -r win-x64 --self-contained true -o .\publish-live

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var connectionString = Required(config["Database:SqlServer"], "Database:SqlServer");

var webhookUrl = Required(config["Webhook:Url"], "Webhook:Url");
var webhookUser = Required(config["Webhook:Username"], "Webhook:Username");
var webhookPassword = Required(config["Webhook:Password"], "Webhook:Password");

var businessIntervalMinutes = config.GetValue("Watchdog:BusinessIntervalMinutes", 15);
var offHoursIntervalMinutes = config.GetValue("Watchdog:OffHoursIntervalMinutes", 60);
var timeoutSeconds = config.GetValue("Watchdog:TimeoutSeconds", 180);
var maxConsecutiveFailures = config.GetValue("Watchdog:MaxConsecutiveFailures", 2);
var pollSeconds = config.GetValue("Watchdog:PollSeconds", 10);

var consoleLogging = config.GetValue("ConsoleLogging", true);

var consecutiveFailures = 0;

LogInfo("OdooWatchdog starting.");
LogInfo($"Webhook URL: {webhookUrl}");
LogInfo($"Business interval: {businessIntervalMinutes} min");
LogInfo($"Off-hours interval: {offHoursIntervalMinutes} min");
LogInfo($"Timeout: {timeoutSeconds} sec");

while (true)
{
    try
    {
        var intervalMinutes = GetWatchdogIntervalMinutes(
            businessIntervalMinutes,
            offHoursIntervalMinutes);

        var watchdogId = Guid.NewGuid().ToString().ToUpperInvariant();

        LogInfo($"Sending watchdog. ID={watchdogId}");

        var sent = await SendWatchdogAsync(
            webhookUrl,
            webhookUser,
            webhookPassword,
            watchdogId);

        if (!sent)
        {
            consecutiveFailures++;
            LogError($"Watchdog send failed. ConsecutiveFailures={consecutiveFailures}");

            if (consecutiveFailures >= maxConsecutiveFailures)
            {
                await RaiseAlertIfDueAsync(
                    connectionString,
                    config,
                    "WATCHDOG",
                    "Odoo/BOM webhook watchdog send failure",
                    $"Watchdog kon niet verzonden worden. ConsecutiveFailures={consecutiveFailures}");
            }

            await Task.Delay(TimeSpan.FromMinutes(1));
            continue;
        }

        var result = await WaitForWatchdogDoneAsync(
            connectionString,
            watchdogId,
            TimeSpan.FromSeconds(timeoutSeconds),
            TimeSpan.FromSeconds(pollSeconds));

        if (result == WatchdogResult.Done)
        {
            consecutiveFailures = 0;
            LogInfo($"Watchdog OK. ID={watchdogId}");
        }
        else
        {
            consecutiveFailures++;

            LogError($"Watchdog failed. Result={result}. ID={watchdogId}. ConsecutiveFailures={consecutiveFailures}");

            if (consecutiveFailures >= maxConsecutiveFailures)
            {
                await RaiseAlertIfDueAsync(
                    connectionString,
                    config,
                    "WATCHDOG",
                    "Odoo/BOM webhook watchdog timeout",
                    $"Watchdog probleem. Result={result}. ID={watchdogId}. ConsecutiveFailures={consecutiveFailures}");
            }
        }

        LogInfo($"Next watchdog in {intervalMinutes} minutes.");
        await Task.Delay(TimeSpan.FromMinutes(intervalMinutes));
    }
    catch (Exception ex)
    {
        consecutiveFailures++;
        LogError("Unhandled watchdog error: " + ex);

        if (consecutiveFailures >= maxConsecutiveFailures)
        {
            await RaiseAlertIfDueAsync(
                connectionString,
                config,
                "WATCHDOG",
                "Odoo/BOM webhook watchdog exception",
                $"Exception in OdooWatchdog:{Environment.NewLine}{ex}");
        }

        await Task.Delay(TimeSpan.FromMinutes(1));
    }
}

static async Task<bool> SendWatchdogAsync(
    string webhookUrl,
    string user,
    string password,
    string watchdogId)
{
    try
    {
        using var client = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30)
        };

        var authRaw = $"{user}:{password}";
        var authBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(authRaw));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authBase64);

        var payload = new
        {
            _action = "WATCHDOG",
            _id = 0,
            _model = "bom.watchdog",
            id = 0,
            move_type = "watchdog",
            x_studio_bom_ref = 0,
            source = "OdooWatchdog",
            watchdog_id = watchdogId,
            timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss")
        };

        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await client.PostAsync(webhookUrl, content);

        if ((int)response.StatusCode >= 200 && (int)response.StatusCode < 300)
        {
            return true;
        }

        var body = await response.Content.ReadAsStringAsync();
        LogError($"Webhook returned HTTP {(int)response.StatusCode}: {body}");
        return false;
    }
    catch (Exception ex)
    {
        LogError("SendWatchdogAsync failed: " + ex.Message);
        return false;
    }
}

static async Task<WatchdogResult> WaitForWatchdogDoneAsync(
    string connectionString,
    string watchdogId,
    TimeSpan timeout,
    TimeSpan pollInterval)
{
    var start = DateTime.UtcNow;
    var wasReceived = false;

    while (DateTime.UtcNow - start < timeout)
    {
        var status = await GetWatchdogQueueStatusAsync(connectionString, watchdogId);

        if (!string.IsNullOrWhiteSpace(status))
        {
            wasReceived = true;

            if (string.Equals(status, "DONE", StringComparison.OrdinalIgnoreCase))
            {
                return WatchdogResult.Done;
            }

            if (string.Equals(status, "FAILED", StringComparison.OrdinalIgnoreCase))
            {
                return WatchdogResult.Failed;
            }
        }

        await Task.Delay(pollInterval);
    }

    return wasReceived
        ? WatchdogResult.ReceivedButNotProcessed
        : WatchdogResult.NotReceived;
}

static async Task<string?> GetWatchdogQueueStatusAsync(string connectionString, string watchdogId)
{
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();

    var sql = @"
SELECT TOP (1) Status
FROM dbo.tblOdooWebhookQueue
WHERE PayloadJson LIKE @Search
ORDER BY QueueID DESC;";

    await using var cmd = new SqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("@Search", "%" + watchdogId + "%");

    var result = await cmd.ExecuteScalarAsync();
    return result?.ToString();
}

static int GetWatchdogIntervalMinutes(int businessMinutes, int offHoursMinutes)
{
    var now = DateTime.Now;
    var isWeekday = now.DayOfWeek is >= DayOfWeek.Monday and <= DayOfWeek.Friday;
    var isBusinessHour = now.Hour >= 8 && now.Hour < 18;

    return isWeekday && isBusinessHour
        ? businessMinutes
        : offHoursMinutes;
}

static async Task RaiseAlertIfDueAsync(
    string connectionString,
    IConfiguration config,
    string alertType,
    string subject,
    string body)
{
    var repeatHours = config.GetValue("Mail:AlertRepeatHours", 4);
    var key = $"OdooAlertLastSent_{alertType}";

    var lastSentRaw = await GetAppVarAsync(connectionString, key);

    if (DateTime.TryParse(lastSentRaw, out var lastSent))
    {
        if (DateTime.Now - lastSent < TimeSpan.FromHours(repeatHours))
        {
            LogInfo($"Alert suppressed by anti-spam. Type={alertType}");
            return;
        }
    }

    await SendMailAsync(config, subject, body);
    await SetAppVarAsync(connectionString, key, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
        $"Laatste alert voor {alertType}");

    LogInfo($"Alert sent. Type={alertType}");
}

static async Task<string?> GetAppVarAsync(string connectionString, string varName)
{
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();

    var sql = "SELECT VarValue FROM dbo.tblAppVars WHERE VarName = @VarName;";

    await using var cmd = new SqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("@VarName", varName);

    var result = await cmd.ExecuteScalarAsync();
    return result?.ToString();
}

static async Task SetAppVarAsync(
    string connectionString,
    string varName,
    string value,
    string notes)
{
    await using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();

    var sql = @"
MERGE dbo.tblAppVars AS tgt
USING (SELECT @VarName AS VarName) AS src
ON tgt.VarName = src.VarName
WHEN MATCHED THEN
    UPDATE SET
        VarValue = @VarValue,
        UpdatedAt = SYSDATETIME(),
        UpdatedByUser = @UpdatedByUser,
        UpdatedByPC = @UpdatedByPC,
        Notes = @Notes
WHEN NOT MATCHED THEN
    INSERT (VarName, VarValue, UpdatedAt, UpdatedByUser, UpdatedByPC, Notes)
    VALUES (@VarName, @VarValue, SYSDATETIME(), @UpdatedByUser, @UpdatedByPC, @Notes);";

    await using var cmd = new SqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("@VarName", varName);
    cmd.Parameters.AddWithValue("@VarValue", value);
    cmd.Parameters.AddWithValue("@UpdatedByUser", Environment.UserName);
    cmd.Parameters.AddWithValue("@UpdatedByPC", Environment.MachineName);
    cmd.Parameters.AddWithValue("@Notes", notes);

    await cmd.ExecuteNonQueryAsync();
}

static async Task SendMailAsync(IConfiguration config, string subject, string body)
{
    var from = Required(config["Mail:From"], "Mail:From");
    var to = Required(config["Mail:To"], "Mail:To");
    var smtpServer = Required(config["Mail:SmtpServer"], "Mail:SmtpServer");
    var port = config.GetValue("Mail:Port", 587);
    var useSsl = config.GetValue("Mail:UseSsl", true);
    var user = config["Mail:User"];
    var password = config["Mail:Password"];

    using var message = new MailMessage(from, to)
    {
        Subject = subject,
        Body =
            body +
            Environment.NewLine + Environment.NewLine +
            $"PC: {Environment.MachineName}" + Environment.NewLine +
            $"User: {Environment.UserName}" + Environment.NewLine +
            $"Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss}"
    };

    using var client = new SmtpClient(smtpServer, port)
    {
        EnableSsl = useSsl
    };

    if (!string.IsNullOrWhiteSpace(user))
    {
        client.Credentials = new NetworkCredential(user, password);
    }

    await client.SendMailAsync(message);
}

static string Required(string? value, string key)
{
    if (string.IsNullOrWhiteSpace(value))
    {
        throw new InvalidOperationException($"Missing required configuration key: {key}");
    }

    return value;
}

static void LogInfo(string message)
{
    WriteLog("INFO", message);
}

static void LogError(string message)
{
    WriteLog("ERROR", message);
}

static void WriteLog(string level, string message)
{
    var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{level}] {message}";

    Console.WriteLine(line);

    try
    {
        var logDir = Path.Combine(AppContext.BaseDirectory, "logs");
        Directory.CreateDirectory(logDir);

        var logFile = Path.Combine(logDir, $"watchdog-{DateTime.Now:yyyy-MM-dd}.log");
        File.AppendAllText(logFile, line + Environment.NewLine);
    }
    catch
    {
        // Logging mag nooit de watchdog doen crashen.
    }
}

enum WatchdogResult
{
    Done,
    NotReceived,
    ReceivedButNotProcessed,
    Failed
}