using NLog;
using NLog.Config;

namespace Dev.HuginnLabs.Dataflow;

/// <summary>
/// Convenience registration for the Dataflow NLog target. The SDK's
/// <see cref="Dataflow"/> entry type is a static class, so it cannot receive
/// extension members from another assembly — the setup lives here instead:
/// <code>
/// DataflowNLog.Setup(); // registers on the default LogFactory
/// </code>
/// An explicit <see cref="Setup(LogFactory)"/> overload exists for apps that
/// keep their own <see cref="LogFactory"/>. Both are idempotent enough:
/// every call adds a fresh target named "Dataflow" and an all-levels rule,
/// so calling twice doubles the shipping — call it once at startup, before
/// any logging happens.
/// </summary>
public static class DataflowNLog
{
    /// <summary>
    /// Registers <see cref="DataflowTarget"/> (name "Dataflow", all levels)
    /// on the default <see cref="LogManager.LogFactory"/> configuration and
    /// returns it.
    /// </summary>
    public static DataflowTarget Setup() => Setup(LogManager.LogFactory);

    /// <summary>
    /// Registers <see cref="DataflowTarget"/> (name "Dataflow", all levels)
    /// on <paramref name="factory"/>'s configuration — created when the
    /// factory has none loaded yet — and returns the target.
    /// </summary>
    public static DataflowTarget Setup(LogFactory factory)
    {
        var configuration = factory.Configuration ?? new LoggingConfiguration();
        var target = new DataflowTarget();
        configuration.AddTarget("Dataflow", target);
        configuration.AddRuleForAllLevels(target);
        // The assignment itself reconfigures the factory's existing loggers;
        // assigning the just-loaded config back is harmless.
        factory.Configuration = configuration;
        return target;
    }
}
