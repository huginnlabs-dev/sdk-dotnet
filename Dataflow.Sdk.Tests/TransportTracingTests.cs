using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using Dev.HuginnLabs.Dataflow;
using HuginnLabs.Proto;
using Xunit;

namespace DataflowSdk.Tests;

// Transport tracing end to end over fakes: a stub HttpMessageHandler and
// stub ADO.NET classes (DbConnection/DbCommand are abstract — directly
// implementable). One test class so enable/disable flips stay sequential;
// it shares the "Dataflow Globals" collection with CrashTests, the only
// other suite mutating the global settings/replay buffer.
[Collection("Dataflow Globals")]
public class TransportTracingTests
{
    // Nothing listens on port 9; the sender retries harmlessly in the
    // background while the tests assert on the replay buffer.
    private static void EnableTracing()
    {
        Dataflow.Configure(new Dataflow.Settings
        {
            Endpoint = "http://127.0.0.1:9",
            ApiKey = "test-key",
            ServiceName = "sdk-tests",
            BufferSize = 10_000,
            Disabled = false,
        });
        lock (Pipeline.BufLock) Pipeline.Buffer.Clear();
    }

    private static void DisableTracing()
    {
        Dataflow.Configure(new Dataflow.Settings { Disabled = true });
        lock (Pipeline.BufLock) Pipeline.Buffer.Clear();
    }

    private static void ClearSpans() { lock (Pipeline.BufLock) Pipeline.Buffer.Clear(); }

    private static List<TraceEvent> Spans()
    {
        lock (Pipeline.BufLock) return new List<TraceEvent>(Pipeline.Buffer);
    }

    private static TraceEvent SingleSpan()
    {
        var spans = Spans();
        var span = Assert.Single(spans);
        return span;
    }

    // ------------------------------------------------------------------
    // HTTP client tracing

    [Fact]
    public async Task HttpHandler_Disabled_IsPurePassThrough()
    {
        DisableTracing();
        var fake = new FakeHttpHandler(HttpStatusCode.OK);
        using var invoker = new HttpMessageInvoker(new DataflowHttpHandler(fake));

        using var response = await invoker.SendAsync(
            new HttpRequestMessage(HttpMethod.Get, "http://api.example.com/orders"), TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(fake.LastRequest);
        Assert.False(fake.LastRequest!.Headers.Contains(DataflowHttpHandler.TraceHeader));
        Assert.Empty(Spans());
    }

    [Fact]
    public async Task HttpHandler_EmitsSpanAndTraceIdHeader()
    {
        EnableTracing();
        var fake = new FakeHttpHandler(HttpStatusCode.OK);
        using var invoker = new HttpMessageInvoker(new DataflowHttpHandler(fake));

        using var scope = Dataflow.Trace("orders.Handler");
        using var request = new HttpRequestMessage(HttpMethod.Get, "http://api.example.com/orders?id=7");
        using var response = await invoker.SendAsync(request, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        // The downstream service joins the ambient trace via the header.
        Assert.True(fake.LastRequest!.Headers.TryGetValues(
            DataflowHttpHandler.TraceHeader, out var values));
        Assert.Equal(scope.Span.TraceIdValue(), Assert.Single(values!));

        var span = SingleSpan();
        Assert.Equal(EventType.HttpClient, span.Type);
        Assert.Equal("GET api.example.com/orders", span.Name);
        Assert.Equal("api.example.com", span.CalleePackage);
        Assert.Equal(scope.Span.TraceIdValue(), span.TraceId);
        Assert.Equal(200, span.StatusCode);
        Assert.Equal("GET", span.Metadata["http.method"]);
        Assert.Equal("http://api.example.com/orders?id=7", span.Metadata["http.url"]);
        Assert.True(span.ParentSpanId.Length > 0, "client span must nest under the ambient span");
    }

    [Fact]
    public async Task HttpHandler_RecordsServerErrors()
    {
        EnableTracing();
        var fake = new FakeHttpHandler(HttpStatusCode.ServiceUnavailable);
        using var invoker = new HttpMessageInvoker(new DataflowHttpHandler(fake));

        using var response = await invoker.SendAsync(
            new HttpRequestMessage(HttpMethod.Post, "https://api.example.com/payments"), TestContext.Current.CancellationToken);

        var span = SingleSpan();
        Assert.Equal(503, span.StatusCode);
        Assert.Equal("POST api.example.com/payments", span.Name);
        Assert.Equal("503", span.Metadata["http.status_code"]);
        Assert.True(span.ErrorMessage.Length > 0, "5xx responses must be flagged as errors");
    }

    [Fact]
    public async Task HttpHandler_RecordsTransportFailures()
    {
        EnableTracing();
        var fake = new FakeHttpHandler(HttpStatusCode.OK, throwOnSend: true);
        using var invoker = new HttpMessageInvoker(new DataflowHttpHandler(fake));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://api.example.com/x"), TestContext.Current.CancellationToken));

        var span = SingleSpan();
        Assert.Equal(503, span.StatusCode);
        Assert.Contains("InvalidOperationException", span.ErrorMessage);
    }

    [Fact]
    public async Task HttpHandler_NeverClobbersAnExistingTraceId()
    {
        EnableTracing();
        var fake = new FakeHttpHandler(HttpStatusCode.OK);
        using var invoker = new HttpMessageInvoker(new DataflowHttpHandler(fake));

        using var request = new HttpRequestMessage(HttpMethod.Get, "http://api.example.com/orders");
        request.Headers.Add(DataflowHttpHandler.TraceHeader, "incoming-trace-id");
        using var _ = await invoker.SendAsync(request, TestContext.Current.CancellationToken);

        Assert.Equal("incoming-trace-id", fake.LastRequest!
            .Headers.GetValues(DataflowHttpHandler.TraceHeader).Single());
    }

    // ------------------------------------------------------------------
    // Database tracing

    [Fact]
    public void DbWrap_Disabled_IsPurePassThrough()
    {
        DisableTracing();
        using var inner = new FakeDbConnection();
        using var conn = Dataflow.Wrap(inner, "npgsql");

        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM items";
        Assert.Equal(ConnectionState.Open, inner.State);
        Assert.Equal(1, cmd.ExecuteScalar());
        Assert.Empty(Spans());
    }

    [Fact]
    public void DbWrap_EmitsDbQuerySpanForScalar()
    {
        EnableTracing();
        using var inner = new FakeDbConnection();
        using var conn = Dataflow.Wrap(inner, "npgsql");
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, total\n FROM   dbo.items WHERE id = @id";

        Assert.Equal(1, cmd.ExecuteScalar());

        var span = SingleSpan();
        Assert.Equal(EventType.DbQuery, span.Type);
        Assert.Equal("SELECT items", span.Name);
        Assert.Equal("postgres", span.CalleePackage); // "npgsql" normalizes
        Assert.Equal("postgres", span.Metadata["db.system"]);
        Assert.Equal("SELECT id, total FROM dbo.items WHERE id = @id", span.Metadata["db.statement"]);
        Assert.Equal(200, span.StatusCode);
        Assert.Empty(span.ErrorMessage);
    }

    [Fact]
    public void DbWrap_EmitsDbQuerySpanForNonQueryAndReader()
    {
        EnableTracing();
        using var inner = new FakeDbConnection
        {
            CommandFactory = () => new FakeDbCommand
            {
                NonQuery = () => 1,
                Reader = () => new FakeDbReader(),
            },
        };
        using var conn = Dataflow.Wrap(inner, "sqlserver");

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO dbo.orders (id) VALUES (@id)";
            Assert.Equal(1, cmd.ExecuteNonQuery());
            Assert.Equal("INSERT orders", SingleSpan().Name);
        }
        ClearSpans();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT * FROM orders";
            using var reader = cmd.ExecuteReader();
            var span = SingleSpan();
            Assert.Equal("SELECT orders", span.Name);
            Assert.Equal("sqlserver", span.CalleePackage);
        }
    }

    [Fact]
    public void DbWrap_RecordsFailedExecution()
    {
        EnableTracing();
        using var inner = new FakeDbConnection
        {
            CommandFactory = () => new FakeDbCommand
            {
                NonQuery = () => throw new InvalidOperationException("relation missing"),
            },
        };
        using var conn = Dataflow.Wrap(inner, "mysql");
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE orders SET total = 1";

        var failure = Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
        var span = SingleSpan();
        Assert.Equal(500, span.StatusCode);
        Assert.Contains("InvalidOperationException", span.ErrorMessage);
        Assert.Contains("relation missing", span.ErrorMessage);
        Assert.Equal("UPDATE orders", span.Name);
    }

    [Fact]
    public async Task DbWrap_EmitsDbQuerySpanForAsyncExecution()
    {
        EnableTracing();
        using var inner = new FakeDbConnection();
        using var conn = Dataflow.Wrap(inner, "sqlite");
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM sessions WHERE expires < now()";

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

        var span = SingleSpan();
        Assert.Equal(EventType.DbQuery, span.Type);
        Assert.Equal("DELETE sessions", span.Name);
        Assert.Equal("sqlite", span.CalleePackage);
    }

    [Fact]
    public void DbWrap_NeverCapturesParameterValues()
    {
        EnableTracing();
        using var inner = new FakeDbConnection();
        using var conn = Dataflow.Wrap(inner, "npgsql");
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM users WHERE email = @email";
        var parameter = cmd.CreateParameter();
        parameter.ParameterName = "email";
        parameter.Value = "secret@corp.example";
        cmd.Parameters.Add(parameter);

        cmd.ExecuteScalar();

        var span = SingleSpan();
        Assert.DoesNotContain("secret@corp.example", span.Metadata["db.statement"]);
        Assert.DoesNotContain("secret@corp.example", span.ErrorMessage);
        foreach (var entry in span.Metadata)
        {
            Assert.DoesNotContain("secret@corp.example", entry.Value);
        }
    }

    // ------------------------------------------------------------------
    // Fakes

    private sealed class FakeHttpHandler : HttpMessageHandler
    {
        private readonly HttpStatusCode _status;
        private readonly bool _throwOnSend;

        public FakeHttpHandler(HttpStatusCode status, bool throwOnSend = false)
        {
            _status = status;
            _throwOnSend = throwOnSend;
        }

        public HttpRequestMessage? LastRequest { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            if (_throwOnSend) throw new InvalidOperationException("connection refused");
            return Task.FromResult(new HttpResponseMessage(_status));
        }
    }

    private sealed class FakeDbConnection : DbConnection
    {
        private string? _connectionString;
        private ConnectionState _state = ConnectionState.Closed;

        public Func<DbCommand>? CommandFactory { get; init; }

        [AllowNull]
        public override string ConnectionString
        {
            get => _connectionString ?? "";
            set => _connectionString = value;
        }

        public override string Database => "testdb";
        public override string DataSource => "testhost";
        public override string ServerVersion => "test 1.0";
        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) { }
        public override void Close() => _state = ConnectionState.Closed;
        public override void Open() => _state = ConnectionState.Open;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
            throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() =>
            CommandFactory?.Invoke() ?? new FakeDbCommand();
    }

    private sealed class FakeDbCommand : DbCommand
    {
        private string? _text;
        private DbConnection? _connection;
        private DbTransaction? _transaction;

        public Func<object?>? Scalar { get; init; }
        public Func<int>? NonQuery { get; init; }
        public Func<DbDataReader>? Reader { get; init; }
        public new FakeParameterCollection Parameters { get; } = new();

        [AllowNull]
        public override string CommandText { get => _text ?? ""; set => _text = value; }
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection? DbConnection
        {
            get => _connection;
            set => _connection = value;
        }

        protected override DbParameterCollection DbParameterCollection => Parameters;

        protected override DbTransaction? DbTransaction
        {
            get => _transaction;
            set => _transaction = value;
        }

        public override void Cancel() { }
        public override void Prepare() { }

        protected override DbParameter CreateDbParameter() => new FakeDbParameter();

        public override int ExecuteNonQuery() => NonQuery?.Invoke() ?? 1;

        public override object? ExecuteScalar() => Scalar?.Invoke() ?? 1;

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
            Reader?.Invoke() ?? new FakeDbReader();
    }

    private sealed class FakeParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _items = new();

        public override int Count => _items.Count;
        public override object SyncRoot => ((ICollection)_items).SyncRoot;

        public override int Add(object? value)
        {
            _items.Add((DbParameter)value!);
            return _items.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values) Add(value);
        }

        public override void Clear() => _items.Clear();
        public override bool Contains(object? value) => value is DbParameter p && _items.Contains(p);
        public override bool Contains(string value) => IndexOf(value) >= 0;
        public override void CopyTo(Array array, int index) => ((ICollection)_items).CopyTo(array, index);
        public override IEnumerator GetEnumerator() => _items.GetEnumerator();
        public override int IndexOf(object? value) => value is DbParameter p ? _items.IndexOf(p) : -1;
        public override int IndexOf(string parameterName) =>
            _items.FindIndex(p => p.ParameterName == parameterName);

        public override void Insert(int index, object? value) => _items.Insert(index, (DbParameter)value!);
        public override void Remove(object? value) => _items.Remove((DbParameter)value!);
        public override void RemoveAt(int index) => _items.RemoveAt(index);
        public override void RemoveAt(string parameterName) => _items.RemoveAt(IndexOf(parameterName));

        protected override DbParameter GetParameter(int index) => _items[index];
        protected override DbParameter GetParameter(string parameterName) => _items[IndexOf(parameterName)];
        protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
        protected override void SetParameter(string parameterName, DbParameter value) =>
            _items[IndexOf(parameterName)] = value;
    }

    private sealed class FakeDbParameter : DbParameter
    {
        public override DbType DbType { get; set; } = DbType.Object;
        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
        public override bool IsNullable { get; set; }

        [AllowNull]
        public override string ParameterName { get; set; } = "";
        public override int Size { get; set; }

        [AllowNull]
        public override string SourceColumn { get; set; } = "";
        public override bool SourceColumnNullMapping { get; set; }
        public override object? Value { get; set; }
        public override void ResetDbType() => DbType = DbType.Object;
    }

    private sealed class FakeDbReader : DbDataReader
    {
        private bool _read;

        public override int Depth => 0;
        public override int FieldCount => 1;
        public override bool HasRows => true;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override object this[int ordinal] => 1;
        public override object this[string name] => 1;

        public override bool Read()
        {
            if (_read) return false;
            _read = true;
            return true;
        }

        public override bool NextResult() => false;
        public override bool GetBoolean(int ordinal) => true;
        public override byte GetByte(int ordinal) => 1;
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
        public override char GetChar(int ordinal) => '1';
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
        public override string GetDataTypeName(int ordinal) => "int";
        public override DateTime GetDateTime(int ordinal) => DateTime.UnixEpoch;
        public override decimal GetDecimal(int ordinal) => 1;
        public override double GetDouble(int ordinal) => 1;
        public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
        public override Type GetFieldType(int ordinal) => typeof(int);
        public override float GetFloat(int ordinal) => 1;
        public override Guid GetGuid(int ordinal) => Guid.Empty;
        public override short GetInt16(int ordinal) => 1;
        public override int GetInt32(int ordinal) => 1;
        public override long GetInt64(int ordinal) => 1;
        public override string GetName(int ordinal) => "one";
        public override int GetOrdinal(string name) => 0;
        public override string GetString(int ordinal) => "1";
        public override object GetValue(int ordinal) => 1;
        public override int GetValues(object[] values)
        {
            values[0] = 1;
            return 1;
        }

        public override bool IsDBNull(int ordinal) => false;
    }
}
