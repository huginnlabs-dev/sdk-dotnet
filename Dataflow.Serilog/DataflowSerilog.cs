using Serilog;
using Serilog.Configuration;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// Convenience factory for the Dataflow Serilog sink. The SDK's
/// <see cref="Dataflow"/> entry type is a static class, so it cannot receive
/// extension members from another assembly — the factory lives here instead:
/// <code>
/// Log.Logger = new LoggerConfiguration()
///     .WriteTo.Sink(DataflowSerilog.SerilogSink())
///     .CreateLogger();
/// </code>
/// The idiomatic <c>WriteTo.Dataflow()</c> (see
/// <see cref="DataflowLoggerConfigurationExtensions"/>) reads better:
/// <code>
/// Log.Logger = new LoggerConfiguration().WriteTo.Dataflow().CreateLogger();
/// </code>
/// </summary>
public static class DataflowSerilog
{
    /// <summary>Returns a <see cref="DataflowSink"/> for <c>WriteTo.Sink(...)</c>.</summary>
    public static DataflowSink SerilogSink() => new();
}

/// <summary>
/// Serilog <c>WriteTo</c> registration: after
/// <c>Log.Logger = new LoggerConfiguration().WriteTo.Dataflow().CreateLogger();</c>
/// every event the level filters let through is shipped through the Dataflow
/// log pipeline (see <see cref="DataflowSink"/>).
/// </summary>
public static class DataflowLoggerConfigurationExtensions
{
    /// <summary>Writes log events to the Dataflow log pipeline.</summary>
    public static LoggerConfiguration Dataflow(this LoggerSinkConfiguration sinkConfiguration) =>
        sinkConfiguration.Sink(new DataflowSink());
}
