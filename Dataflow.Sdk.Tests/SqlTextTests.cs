using Dev.HuginnLabs.Dataflow;
using Xunit;

namespace DataflowSdk.Tests;

// Statement summarizing must agree with the Go SDK's stmtSummary/clipStatement
// — the dashboard renders these names side by side.
public class SqlTextTests
{
    [Theory]
    [InlineData("SELECT id, total FROM orders WHERE id = $1", "SELECT orders")]
    [InlineData("  insert into users (email) values ($1)", "INSERT users")]
    [InlineData("UPDATE public.items SET total = total - 1", "UPDATE items")]
    [InlineData("UPDATE dbo.items SET total = total - 1", "UPDATE items")]
    [InlineData("DELETE FROM sessions WHERE expires < now()", "DELETE sessions")]
    [InlineData("CREATE TABLE IF NOT EXISTS migrations (id int)", "CREATE migrations")]
    [InlineData("CREATE TABLE IF NOT EXISTS dbo.items (id int)", "CREATE items")]
    [InlineData("DROP TABLE IF EXISTS dbo.items", "DROP items")]
    [InlineData("INSERT INTO public.orders (id) VALUES ($1)", "INSERT orders")]
    [InlineData("select u.id\nfrom users u\njoin orders o on o.user_id = u.id", "SELECT users")]
    [InlineData("TRUNCATE TABLE sessions", "TRUNCATE sessions")]
    // Bare verbs and non-SQL fall back to the first word.
    [InlineData("PRAGMA journal_mode=WAL", "PRAGMA")]
    [InlineData("COMMIT", "COMMIT")]
    [InlineData("", "QUERY")]
    public void Summary_RendersVerbAndTable(string statement, string expected) =>
        Assert.Equal(expected, SqlText.Summary(statement));

    [Fact]
    public void Summary_HandlesNullStatement() => Assert.Equal("QUERY", SqlText.Summary(null));

    [Fact]
    public void Clip_IsSingleSpacedAndBounded()
    {
        var statement = "SELECT   id,\n\t total\r\n  FROM   orders";
        Assert.Equal("SELECT id, total FROM orders", SqlText.Clip(statement));

        var longStatement = "SELECT " + string.Join(", ", Enumerable.Repeat("x", 100)) + ", 1";
        Assert.Equal(200, SqlText.Clip(longStatement).Length);
        Assert.Equal(200, SqlText.Clip(longStatement).TrimEnd().Length);
    }

    [Theory]
    [InlineData("npgsql", "postgres")]
    [InlineData("Npgsql", "postgres")]
    [InlineData("mysql", "mysql")]
    [InlineData("MySqlConnector", "mysql")]
    [InlineData("sqlite", "sqlite")]
    [InlineData("Microsoft.Data.Sqlite", "sqlite")]
    [InlineData("sqlserver", "sqlserver")]
    [InlineData("Microsoft.Data.SqlClient", "sqlserver")]
    [InlineData("firebird", "firebird")]
    [InlineData("", "")]
    public void NormalizeSystem_MapsDriverNamesToSystems(string system, string expected) =>
        Assert.Equal(expected, SqlText.NormalizeSystem(system));
}
