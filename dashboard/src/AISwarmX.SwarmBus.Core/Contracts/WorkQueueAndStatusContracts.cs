namespace AISwarmX.SwarmBus.Core.Contracts;

/// <summary>
/// Persistent Ready work queue item scoped to a workspace.
/// </summary>
public sealed record WorkQueueItem(
    long Id,
    int IssueNumber,
    string Title,
    int Priority,
    string? RiskClass,
    string Status,
    string? ClaimedBy,
    DateTimeOffset? ClaimedAtUtc,
    DateTimeOffset? LeaseExpiresUtc,
    DateTimeOffset UpdatedAtUtc,
    string WorkspaceId = SwarmBusConstants.DefaultWorkspaceId);

/// <summary>
/// Request payload to insert or update a work queue item.
/// </summary>
public sealed record WorkQueueUpsertRequest(
    int IssueNumber,
    string? Title = null,
    int? Priority = null,
    string? RiskClass = null,
    string Status = "queued",
    string? WorkspaceId = null);

/// <summary>
/// Result returned by <c>QueueNext</c> including resume status and effective code WIP metrics.
/// </summary>
public sealed record WorkQueueNextResult(
    bool Ok,
    string WorkspaceId,
    bool Resumed,
    int CodeWip,
    int ActiveClaimedCodeCount,
    int EffectiveCodeWip,
    WorkQueueItem? Item,
    string? Reason = null);

/// <summary>
/// Request payload to mark a work queue item <c>done</c> with optional atomic compare-and-set guards.
/// </summary>
public sealed record WorkQueueDoneRequest(
    int IssueNumber,
    string? WorkspaceId = null,
    string? ExpectedStatus = null,
    string? ExpectedOwner = null,
    DateTimeOffset? ClaimedBeforeUtc = null);

/// <summary>
/// Result returned by <c>QueueUpsert</c>, <c>QueueRelease</c>, or <c>QueueDone</c>.
/// </summary>
public sealed record WorkQueueMutationResult(
    bool Ok,
    string Action,
    string WorkspaceId,
    int IssueNumber,
    bool Changed,
    WorkQueueItem? Item,
    string? Reason = null);

/// <summary>
/// Request payload for single-roundtrip <c>StartWork</c> (claim locks, check unread inbox, inspect foreign locks).
/// </summary>
public sealed record SwarmStartWorkRequest(
    string Agent,
    IReadOnlyList<string> Paths,
    int? PrNumber = null,
    int TtlMinutes = SwarmBusConstants.DefaultLockTtlMinutes,
    int MessageLimit = 20,
    string? WorkspaceId = null);

/// <summary>
/// Result returned by <c>StartWork</c> / <c>StartWorkAsync</c>.
/// </summary>
public sealed record SwarmStartWorkResult(
    bool Ok,
    string WorkspaceId,
    IReadOnlyList<string> ClaimedPaths,
    IReadOnlyList<LockConflict> Conflicts,
    IReadOnlyList<PathLockLease> Locks,
    IReadOnlyList<SwarmMessageEnvelope> UnreadMessages,
    IReadOnlyList<PathLockLease> ForeignLocks);

/// <summary>
/// Result returned by <c>QueueList</c>.
/// </summary>
public sealed record WorkQueueListResult(
    bool Ok,
    string WorkspaceId,
    int CodeWip,
    IReadOnlyDictionary<string, string> Meta,
    IReadOnlyList<WorkQueueItem> Items);

/// <summary>
/// Unread count summary for a single recipient.
/// </summary>
public sealed record RecipientUnreadCount(
    string Recipient,
    int UnreadCount);

/// <summary>
/// Aggregated Swarm Bus status snapshot.
/// </summary>
public sealed record SwarmBusStatusSnapshot(
    DateTimeOffset GeneratedAtUtc,
    string WorkspaceId,
    int TotalUnread,
    IReadOnlyList<RecipientUnreadCount> UnreadByRecipient,
    IReadOnlyList<PathLockLease> ActiveLocks,
    IReadOnlyList<SwarmMessageEnvelope> RecentMessages,
    IReadOnlyList<AgentSessionRecord>? Sessions = null,
    IReadOnlyList<ProjectStateRecord>? Projects = null,
    int QueuedIdeaCount = 0);

/// <summary>
/// Summary of a read-only import from a legacy single-repo <c>swarm-bus.db</c> into <c>swarm-bus-v2.db</c>.
/// </summary>
public sealed record LegacyImportResult(
    string SourcePath,
    int MessagesImported,
    int LocksImported,
    int ReviewClaimsImported,
    int WorkQueueItemsImported,
    int MetaEntriesImported);
