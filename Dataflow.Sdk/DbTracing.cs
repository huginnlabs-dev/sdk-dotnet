using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// ADO.NET tracing: a delegating <see cref="DbConnection"/> whose commands
/// emit one DB_QUERY span per execution — verb + table as the name (e.g.
/// "SELECT orders"), the SQL dialect in <c>db.system</c>, and the statement
/// text (single-spaced, truncated) in <c>db.statement</c>. Parameter values
/// are never captured. Wrap at the edge:
///
/// <code>await using var conn = Dataflow.Wrap(new NpgsqlConnection(cs), "npgsql");</code>
///
/// Best-effort by construction: when tracing is disabled the wrapper is a
/// pure pass-through, and span bookkeeping failures never surface through
/// the ADO.NET contract.
/// </summary>
internal sealed class DataflowConnection : DbConnection
{
    private readonly DbConnection _inner;
    private readonly string _system;

    internal DataflowConnection(DbConnection inner, string system)
    {
        _inner = inner;
        _system = SqlText.NormalizeSystem(system);
    }

    [AllowNull]
    public override string ConnectionString
    {
        get => _inner.ConnectionString;
        set => _inner.ConnectionString = value;
    }

    public override string Database => _inner.Database;
    public override string DataSource => _inner.DataSource;
    public override string ServerVersion => _inner.ServerVersion;
    public override ConnectionState State => _inner.State;

    public override event StateChangeEventHandler? StateChange
    {
        add => _inner.StateChange += value;
        remove => _inner.StateChange -= value;
    }

    public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
    public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default) =>
        _inner.ChangeDatabaseAsync(databaseName, cancellationToken);
    public override void Close() => _inner.Close();
    public override Task CloseAsync() => _inner.CloseAsync();
    public override void Open() => _inner.Open();
    public override Task OpenAsync(CancellationToken cancellationToken = default) =>
        _inner.OpenAsync(cancellationToken);
    public override void EnlistTransaction(System.Transactions.Transaction? transaction) =>
        _inner.EnlistTransaction(transaction);
    public override DataTable GetSchema() => _inner.GetSchema();
    public override DataTable GetSchema(string collectionName) => _inner.GetSchema(collectionName);
    public override DataTable GetSchema(string collectionName, string?[] restrictionValues) =>
        _inner.GetSchema(collectionName, restrictionValues);

    // Non-virtual on DbConnection (providers shadow it); a wrapper must too.
    public new DbBatch CreateBatch() => _inner.CreateBatch();

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        _inner.BeginTransaction(isolationLevel);

    protected override DbCommand CreateDbCommand() => new TracedDbCommand(_inner.CreateCommand(), _system);

    protected override void Dispose(bool disposing)
    {
        if (disposing) _inner.Dispose();
        base.Dispose(disposing);
    }
}

/// <summary>
/// Delegating DbCommand: every execution (reader, non-query, scalar; sync
/// and async) is bracketed by one DB_QUERY span. All other members —
/// CommandText, parameters, transactions, timeouts — pass through.
/// </summary>
internal sealed class TracedDbCommand : DbCommand
{
    private readonly DbCommand _inner;
    private readonly string _system;

    internal TracedDbCommand(DbCommand inner, string system)
    {
        _inner = inner;
        _system = system;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _inner.CommandText;
        set => _inner.CommandText = value;
    }

    public override int CommandTimeout
    {
        get => _inner.CommandTimeout;
        set => _inner.CommandTimeout = value;
    }

    public override CommandType CommandType
    {
        get => _inner.CommandType;
        set => _inner.CommandType = value;
    }

    public override bool DesignTimeVisible
    {
        get => _inner.DesignTimeVisible;
        set => _inner.DesignTimeVisible = value;
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => _inner.UpdatedRowSource;
        set => _inner.UpdatedRowSource = value;
    }

    protected override DbConnection? DbConnection
    {
        // The protected core members are not reachable on _inner; the
        // explicit IDbCommand implementation is the public route to them.
        get => (DbConnection?)((IDbCommand)_inner).Connection;
        set => ((IDbCommand)_inner).Connection = value;
    }

    protected override DbParameterCollection DbParameterCollection =>
        (DbParameterCollection)((IDbCommand)_inner).Parameters;

    protected override DbTransaction? DbTransaction
    {
        get => (DbTransaction?)((IDbCommand)_inner).Transaction;
        set => ((IDbCommand)_inner).Transaction = value;
    }

    public override void Cancel() => _inner.Cancel();
    public override void Prepare() => _inner.Prepare();
    protected override DbParameter CreateDbParameter() =>
        (DbParameter)((IDbCommand)_inner).CreateParameter();

    public override int ExecuteNonQuery()
    {
        var span = StartSpan();
        try
        {
            var rows = _inner.ExecuteNonQuery();
            Finish(span, error: null);
            return rows;
        }
        catch (Exception e)
        {
            Finish(span, e);
            throw;
        }
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var span = StartSpan();
        try
        {
            var rows = await _inner.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            Finish(span, error: null);
            return rows;
        }
        catch (Exception e)
        {
            Finish(span, e);
            throw;
        }
    }

    public override object? ExecuteScalar()
    {
        var span = StartSpan();
        try
        {
            var result = _inner.ExecuteScalar();
            Finish(span, error: null);
            return result;
        }
        catch (Exception e)
        {
            Finish(span, e);
            throw;
        }
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        var span = StartSpan();
        try
        {
            var result = await _inner.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            Finish(span, error: null);
            return result;
        }
        catch (Exception e)
        {
            Finish(span, e);
            throw;
        }
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var span = StartSpan();
        try
        {
            var reader = _inner.ExecuteReader(behavior);
            Finish(span, error: null);
            return reader;
        }
        catch (Exception e)
        {
            Finish(span, e);
            throw;
        }
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var span = StartSpan();
        try
        {
            // Public entry point: the protected core is not callable on _inner.
            var reader = await _inner.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
            Finish(span, error: null);
            return reader;
        }
        catch (Exception e)
        {
            Finish(span, e);
            throw;
        }
    }

    private Span? StartSpan()
    {
        if (!Dataflow.Enabled()) return null;
        try
        {
            var span = Dataflow.StartSpan(SqlText.Summary(CommandText), "DB_QUERY");
            span.Callee(_system);
            span.Attr("db.system", _system);
            var statement = SqlText.Clip(CommandText);
            if (statement.Length > 0) span.Attr("db.statement", statement);
            return span;
        }
        catch
        {
            return null; // best-effort: tracing must never break execution
        }
    }

    private static void Finish(Span? span, Exception? error)
    {
        if (span is null) return;
        try
        {
            if (error is not null)
            {
                span.RecordError(error);
                span.Status(500);
            }
            else
            {
                span.Status(200);
            }
            span.End();
        }
        catch
        {
            // best-effort
        }
    }
}

/// <summary>
/// Statement summarizing, shared by the connection wrapper: a short human
/// name for a statement (verb plus the first table reference — "SELECT
/// orders", "INSERT users"; bare verbs and non-SQL fall back to the first
/// word) and the clipped statement text for db.statement.
/// </summary>
internal static class SqlText
{
    private const int MaxStatement = 200;

    private static readonly Regex VerbRegex = new(
        @"^\s*\(?\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|WITH|BEGIN|COMMIT|ROLLBACK|SET|CALL|EXEC|SHOW|EXPLAIN)\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex TableRegex = new(
        @"\b(?:FROM|INTO|UPDATE|TABLE|JOIN)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?[`""'\[]?([A-Za-z_][\w$.]*)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    internal static string Summary(string? commandText)
    {
        var one = OneLine(commandText);
        var verb = VerbRegex.Match(one);
        if (!verb.Success)
        {
            var i = one.IndexOfAny(Separators);
            return i > 0 ? one[..i].ToUpperInvariant() : "QUERY";
        }
        var name = verb.Groups[1].Value.ToUpperInvariant();
        var table = TableRegex.Match(one);
        if (!table.Success) return name;
        // Schema-qualified names ("dbo.items") report the bare table.
        var t = table.Groups[1].Value;
        var dot = t.LastIndexOfAny(Qualifiers);
        return dot >= 0 ? $"{name} {t[(dot + 1)..]}" : $"{name} {t}";
    }

    internal static string Clip(string? commandText)
    {
        var one = OneLine(commandText);
        return one.Length <= MaxStatement ? one : one[..MaxStatement];
    }

    /// <summary>Driver/dialect names normalize to the wire's db.system.</summary>
    internal static string NormalizeSystem(string system)
    {
        if (string.IsNullOrWhiteSpace(system)) return system ?? "";
        return system.Trim().ToLowerInvariant() switch
        {
            "npgsql" => "postgres",
            "mysql" or "mysqlconnector" or "mysql.data" => "mysql",
            "sqlite" or "microsoft.data.sqlite" or "system.data.sqlite" => "sqlite",
            "sqlserver" or "microsoft.data.sqlclient" or "system.data.sqlclient" => "sqlserver",
            _ => system,
        };
    }

    private static readonly char[] Separators = { ' ', '\t', '\n', '\r', '(' };
    private static readonly char[] Qualifiers = { '.', '$' };

    private static string OneLine(string? commandText)
    {
        if (string.IsNullOrWhiteSpace(commandText)) return "";
        return string.Join(' ', commandText.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    }
}
