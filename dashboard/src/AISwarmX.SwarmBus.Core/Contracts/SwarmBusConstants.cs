namespace AISwarmX.SwarmBus.Core.Contracts;

/// <summary>
/// Shared constants for MOBAi Swarm Bus contracts and persistence.
/// </summary>
public static class SwarmBusConstants
{
    public const string DefaultWorkspaceId = "default";
    public const string DefaultDbFileName = "swarm-bus-v2.db";
    public const string LegacyDbFileName = "swarm-bus.db";
    public const int DefaultWakeAttemptTtlSeconds = 120;
    public const int DefaultLockTtlMinutes = 120;
    public const int MinLockTtlMinutes = 5;
    public const int MaxLockTtlMinutes = 1440;
    public const int DefaultReviewClaimTtlMinutes = 60;
    public const int MinReviewClaimTtlMinutes = 1;
    public const int MaxReviewClaimTtlMinutes = 1440;
    public const int DefaultQueueLeaseHours = 4;
    public const int CodeWipClaimLimit = 2;
    public const int DefaultWaitTimeoutMs = 30_000;
    public const int MinWaitTimeoutMs = 1;
    public const int MaxWaitTimeoutMs = 120_000;
    public const int WaitFallbackPollMs = 250;
    public const int DefaultSessionHeartbeatTtlMinutes = 15;
    public const int MinSessionHeartbeatTtlMinutes = 1;
    public const int MaxSessionHeartbeatTtlMinutes = 1440;
    public const int DefaultSessionRegistrationMaxAgeHours = 72;
    public const int DefaultLeadTurnTtlMinutes = 30;
    public const int MinLeadTurnTtlMinutes = 1;
    public const int MaxLeadTurnTtlMinutes = 240;
    public const int MaxProjectDecisions = 20;

    public static readonly IReadOnlySet<string> HeadlessRecipients = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "watchdog",
        "claude-opus-worker",
        "system"
    };

    public static readonly IReadOnlySet<string> ValidPriorities = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "low",
        "normal",
        "high",
        "urgent"
    };

    public static readonly IReadOnlySet<string> ValidReviewRoles = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "companion",
        "final",
        "p3-verification"
    };

    public static readonly IReadOnlySet<string> ValidQueueStatuses = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "queued",
        "claimed",
        "done",
        "removed"
    };

    public static readonly IReadOnlySet<string> ValidSessionStates = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "idle",
        "planning",
        "building",
        "testing",
        "reviewing"
    };

    public static readonly IReadOnlySet<string> ValidIdeaStatuses = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "queued",
        "promoted",
        "done",
        "archived"
    };
}
