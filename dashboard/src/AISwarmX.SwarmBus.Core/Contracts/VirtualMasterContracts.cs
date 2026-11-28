namespace AISwarmX.SwarmBus.Core.Contracts;

/// <summary>
/// Live registered agent session and heartbeat state scoped to a workspace.
/// When <see cref="State"/> is non-idle and <see cref="ActivityExpiresUtc"/> has elapsed,
/// <see cref="EffectiveState"/> degrades to <c>"idle"</c>.
/// </summary>
public sealed record AgentSessionRecord(
    string WorkspaceId,
    string Agent,
    string? SessionId,
    string? Tool,
    string? Model,
    string State,
    string EffectiveState,
    string? Activity,
    int? IssueNumber,
    string? Branch,
    DateTimeOffset RegisteredAtUtc,
    DateTimeOffset UpdatedAtUtc,
    DateTimeOffset? ActivityExpiresUtc);

/// <summary>
/// Request payload to register or update an agent session heartbeat within a workspace.
/// </summary>
public sealed record AgentSessionUpsertRequest(
    string WorkspaceId,
    string Agent,
    string? SessionId = null,
    string? Tool = null,
    string? Model = null,
    string? State = null,
    string? Activity = null,
    int? IssueNumber = null,
    string? Branch = null,
    int TtlMinutes = SwarmBusConstants.DefaultSessionHeartbeatTtlMinutes);

/// <summary>
/// Result returned by <c>UpsertSession</c> or <c>UnregisterSession</c>.
/// </summary>
public sealed record AgentSessionMutationResult(
    bool Ok,
    string Action,
    string WorkspaceId,
    string Agent,
    bool Changed,
    AgentSessionRecord? Session);

/// <summary>
/// Result returned by <c>ListSessions</c>.
/// </summary>
public sealed record AgentSessionListResult(
    bool Ok,
    string WorkspaceId,
    IReadOnlyList<AgentSessionRecord> Sessions);

/// <summary>
/// Shared workspace project memory and active <c>Lead-on-Input</c> lease snapshot.
/// </summary>
public sealed record ProjectStateRecord(
    string WorkspaceId,
    string? ActiveGoal,
    string? CurrentSlice,
    string? LeadAgent,
    DateTimeOffset? LeadLeaseExpiresUtc,
    bool IsLeadActive,
    IReadOnlyList<string> Decisions,
    DateTimeOffset UpdatedAtUtc,
    string UpdatedBy);

/// <summary>
/// Request payload to claim the workspace lead baton (<c>Lead-on-Input</c>) and optionally update session heartbeat.
/// </summary>
public sealed record ProjectTurnClaimRequest(
    string WorkspaceId,
    string Agent,
    string? ActiveGoal = null,
    string? CurrentSlice = null,
    string? Decision = null,
    int TtlMinutes = SwarmBusConstants.DefaultLeadTurnTtlMinutes,
    string? SessionId = null,
    string? Tool = null,
    string? Model = null,
    string? State = null,
    string? Activity = null,
    int? IssueNumber = null,
    string? Branch = null);

/// <summary>
/// Request payload to update shared workspace project state and/or release the lead baton.
/// </summary>
public sealed record ProjectStateUpdateRequest(
    string WorkspaceId,
    string Agent,
    string? ActiveGoal = null,
    string? CurrentSlice = null,
    string? Decision = null,
    IReadOnlyList<string>? Decisions = null,
    bool ReleaseLead = false);

/// <summary>
/// Consolidated workspace sync result returned by <c>GetProjectSync</c>, <c>ClaimProjectTurn</c>, and <c>UpdateProjectState</c>.
/// </summary>
public sealed record ProjectSyncResult(
    bool Ok,
    string Action,
    string WorkspaceId,
    ProjectStateRecord? ProjectState,
    string? PreviousLeadAgent,
    IReadOnlyList<AgentSessionRecord> Sessions,
    IReadOnlyList<ProjectIdeaItem> QueuedIdeas,
    IReadOnlyList<SwarmMessageEnvelope> UnreadMessages);

/// <summary>
/// Result returned by <c>ListProjectStates</c>.
/// </summary>
public sealed record ProjectStateListResult(
    bool Ok,
    string WorkspaceId,
    IReadOnlyList<ProjectStateRecord> Projects);

/// <summary>
/// Quick-capture idea queue item scoped to a workspace.
/// </summary>
public sealed record ProjectIdeaItem(
    long Id,
    string WorkspaceId,
    string Title,
    string? Details,
    int Priority,
    string Status,
    int? IssueNumber,
    string CreatedBy,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc);

/// <summary>
/// Request payload to append a quick-capture idea to a workspace idea queue.
/// </summary>
public sealed record ProjectIdeaAddRequest(
    string WorkspaceId,
    string Title,
    string CreatedBy,
    string? Details = null,
    int Priority = 2,
    int? IssueNumber = null);

/// <summary>
/// Request payload to update or promote a quick-capture idea.
/// </summary>
public sealed record ProjectIdeaUpdateRequest(
    long Id,
    string? WorkspaceId = null,
    string? Title = null,
    string? Details = null,
    int? Priority = null,
    string? Status = null,
    int? IssueNumber = null);

/// <summary>
/// Result returned by <c>AddIdea</c> or <c>UpdateIdea</c>.
/// </summary>
public sealed record ProjectIdeaMutationResult(
    bool Ok,
    string Action,
    string WorkspaceId,
    bool Changed,
    ProjectIdeaItem? Idea,
    string? Reason = null);

/// <summary>
/// Result returned by <c>ListIdeas</c>.
/// </summary>
public sealed record ProjectIdeaListResult(
    bool Ok,
    string WorkspaceId,
    IReadOnlyList<ProjectIdeaItem> Ideas);
