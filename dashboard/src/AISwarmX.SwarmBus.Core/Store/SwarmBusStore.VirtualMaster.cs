using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using AISwarmX.SwarmBus.Core.Contracts;
using AISwarmX.SwarmBus.Core.Locking;

namespace AISwarmX.SwarmBus.Core.Store;

public sealed partial class SwarmBusStore
{
    private const string SessionStateIdle = "idle";
    private const string IdeaStatusQueued = "queued";

    private sealed record ValidatedSessionUpsert(
        string WorkspaceId,
        string Agent,
        string Action,
        string? SessionId,
        string? Tool,
        string? Model,
        string? State,
        string? Activity,
        int? IssueNumber,
        string? Branch,
        int TtlMinutes);

    /// <summary>
    /// Registers or refreshes an agent session heartbeat within a workspace.
    /// </summary>
    public AgentSessionMutationResult UpsertSession(AgentSessionUpsertRequest request, string action = "register")
    {
        var parsed = ValidateSessionUpsertRequest(request, action);
        return ExecuteInImmediateTransaction(connection => UpsertSessionInTx(connection, parsed));
    }

    /// <summary>
    /// Asynchronously registers or refreshes an agent session heartbeat within a workspace.
    /// </summary>
    public Task<AgentSessionMutationResult> UpsertSessionAsync(
        AgentSessionUpsertRequest request,
        string action = "register",
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var parsed = ValidateSessionUpsertRequest(request, action);
        return ExecuteInImmediateTransactionAsync(
            connection => UpsertSessionInTx(connection, parsed),
            cancellationToken);
    }

    /// <summary>
    /// Removes a registered agent session from a workspace.
    /// </summary>
    public AgentSessionMutationResult UnregisterSession(string agent, string? workspaceId = null)
    {
        var (normalizedAgent, normalizedWorkspace) = ValidateUnregisterSessionArgs(agent, workspaceId);
        return ExecuteInImmediateTransaction(connection =>
            UnregisterSessionInTx(connection, normalizedAgent, normalizedWorkspace));
    }

    /// <summary>
    /// Asynchronously removes a registered agent session from a workspace.
    /// </summary>
    public Task<AgentSessionMutationResult> UnregisterSessionAsync(
        string agent,
        string? workspaceId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalizedAgent, normalizedWorkspace) = ValidateUnregisterSessionArgs(agent, workspaceId);
        return ExecuteInImmediateTransactionAsync(
            connection => UnregisterSessionInTx(connection, normalizedAgent, normalizedWorkspace),
            cancellationToken);
    }

    /// <summary>
    /// Lists registered agent sessions for a workspace or <c>"*"</c> across all workspaces.
    /// </summary>
    public AgentSessionListResult ListSessions(string? workspaceId = null)
    {
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        var sessions = ExecuteReadTransaction(connection =>
            ReadAgentSessionsInTx(connection, allWorkspaces, normalizedWorkspace, _timeProvider.GetUtcNow()));
        return new AgentSessionListResult(Ok: true, WorkspaceId: normalizedWorkspace, Sessions: sessions);
    }

    /// <summary>
    /// Asynchronously lists registered agent sessions for a workspace or <c>"*"</c> across all workspaces.
    /// </summary>
    public async Task<AgentSessionListResult> ListSessionsAsync(
        string? workspaceId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        var sessions = await ExecuteReadTransactionAsync(
            connection => ReadAgentSessionsInTx(connection, allWorkspaces, normalizedWorkspace, _timeProvider.GetUtcNow()),
            cancellationToken).ConfigureAwait(false);
        return new AgentSessionListResult(Ok: true, WorkspaceId: normalizedWorkspace, Sessions: sessions);
    }

    /// <summary>
    /// Reads shared workspace project state, active sessions, queued ideas, and unread messages in one snapshot.
    /// </summary>
    public ProjectSyncResult GetProjectSync(string? workspaceId = null, string? agent = null)
    {
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        var trimmedAgent = NormalizeOptionalText(agent);
        return ExecuteReadTransaction(connection =>
            BuildProjectSyncResultInTx(connection, "get", normalizedWorkspace, trimmedAgent, previousLeadAgent: null));
    }

    /// <summary>
    /// Asynchronously reads shared workspace project state, active sessions, queued ideas, and unread messages in one snapshot.
    /// </summary>
    public Task<ProjectSyncResult> GetProjectSyncAsync(
        string? workspaceId = null,
        string? agent = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        var trimmedAgent = NormalizeOptionalText(agent);
        return ExecuteReadTransactionAsync(
            connection => BuildProjectSyncResultInTx(connection, "get", normalizedWorkspace, trimmedAgent, previousLeadAgent: null),
            cancellationToken);
    }

    /// <summary>
    /// Atomically claims the workspace <c>Lead-on-Input</c> baton, updates shared project state,
    /// optionally touches/upserts the agent's session heartbeat, and returns the full sync snapshot.
    /// </summary>
    public ProjectSyncResult ClaimProjectTurn(ProjectTurnClaimRequest request)
    {
        ValidateClaimProjectTurnRequest(request);
        return ExecuteInImmediateTransaction(connection => ClaimProjectTurnInTx(connection, request));
    }

    /// <summary>
    /// Asynchronously claims the workspace <c>Lead-on-Input</c> baton and returns the full sync snapshot.
    /// </summary>
    public Task<ProjectSyncResult> ClaimProjectTurnAsync(
        ProjectTurnClaimRequest request,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ValidateClaimProjectTurnRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => ClaimProjectTurnInTx(connection, request),
            cancellationToken);
    }

    /// <summary>
    /// Updates shared workspace project state and optionally releases the active lead baton when owned or expired.
    /// </summary>
    public ProjectSyncResult UpdateProjectState(ProjectStateUpdateRequest request)
    {
        ValidateUpdateProjectStateRequest(request);
        return ExecuteInImmediateTransaction(connection => UpdateProjectStateInTx(connection, request));
    }

    /// <summary>
    /// Asynchronously updates shared workspace project state and optionally releases the active lead baton.
    /// </summary>
    public Task<ProjectSyncResult> UpdateProjectStateAsync(
        ProjectStateUpdateRequest request,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ValidateUpdateProjectStateRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => UpdateProjectStateInTx(connection, request),
            cancellationToken);
    }

    /// <summary>
    /// Lists shared project state records for a workspace or <c>"*"</c> across all workspaces.
    /// </summary>
    public ProjectStateListResult ListProjectStates(string? workspaceId = null)
    {
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        var projects = ExecuteReadTransaction(connection =>
            ReadProjectStatesInTx(connection, allWorkspaces, normalizedWorkspace, _timeProvider.GetUtcNow()));
        return new ProjectStateListResult(Ok: true, WorkspaceId: normalizedWorkspace, Projects: projects);
    }

    /// <summary>
    /// Asynchronously lists shared project state records for a workspace or <c>"*"</c> across all workspaces.
    /// </summary>
    public async Task<ProjectStateListResult> ListProjectStatesAsync(
        string? workspaceId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        var projects = await ExecuteReadTransactionAsync(
            connection => ReadProjectStatesInTx(connection, allWorkspaces, normalizedWorkspace, _timeProvider.GetUtcNow()),
            cancellationToken).ConfigureAwait(false);
        return new ProjectStateListResult(Ok: true, WorkspaceId: normalizedWorkspace, Projects: projects);
    }

    /// <summary>
    /// Appends a new quick-capture idea to a workspace idea queue.
    /// </summary>
    public ProjectIdeaMutationResult AddIdea(ProjectIdeaAddRequest request)
    {
        var (workspaceId, title, createdBy, details, priority, issueNumber) = ValidateIdeaAddRequest(request);
        return ExecuteInImmediateTransaction(connection =>
            AddIdeaInTx(connection, workspaceId, title, createdBy, details, priority, issueNumber));
    }

    /// <summary>
    /// Asynchronously appends a new quick-capture idea to a workspace idea queue.
    /// </summary>
    public Task<ProjectIdeaMutationResult> AddIdeaAsync(
        ProjectIdeaAddRequest request,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (workspaceId, title, createdBy, details, priority, issueNumber) = ValidateIdeaAddRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => AddIdeaInTx(connection, workspaceId, title, createdBy, details, priority, issueNumber),
            cancellationToken);
    }

    /// <summary>
    /// Updates or promotes an existing quick-capture idea, enforcing workspace isolation when <see cref="ProjectIdeaUpdateRequest.WorkspaceId"/> is specified.
    /// </summary>
    public ProjectIdeaMutationResult UpdateIdea(ProjectIdeaUpdateRequest request)
    {
        var (id, expectedWorkspaceId, title, details, hasDetails, priority, status, issueNumber) = ValidateIdeaUpdateRequest(request);
        return ExecuteInImmediateTransaction(connection =>
            UpdateIdeaInTx(connection, id, expectedWorkspaceId, title, details, hasDetails, priority, status, issueNumber));
    }

    /// <summary>
    /// Asynchronously updates or promotes an existing quick-capture idea.
    /// </summary>
    public Task<ProjectIdeaMutationResult> UpdateIdeaAsync(
        ProjectIdeaUpdateRequest request,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (id, expectedWorkspaceId, title, details, hasDetails, priority, status, issueNumber) = ValidateIdeaUpdateRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => UpdateIdeaInTx(connection, id, expectedWorkspaceId, title, details, hasDetails, priority, status, issueNumber),
            cancellationToken);
    }

    /// <summary>
    /// Lists quick-capture ideas for a workspace (or <c>"*"</c> for all workspaces) with optional status filtering.
    /// </summary>
    public ProjectIdeaListResult ListIdeas(string? workspaceId = null, string? status = null)
    {
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        var statusFilter = NormalizeOptionalIdeaStatus(status);
        var ideas = ExecuteReadTransaction(connection =>
            ReadIdeasInTx(connection, allWorkspaces, normalizedWorkspace, statusFilter));
        return new ProjectIdeaListResult(Ok: true, WorkspaceId: normalizedWorkspace, Ideas: ideas);
    }

    /// <summary>
    /// Asynchronously lists quick-capture ideas for a workspace (or <c>"*"</c> for all workspaces) with optional status filtering.
    /// </summary>
    public async Task<ProjectIdeaListResult> ListIdeasAsync(
        string? workspaceId = null,
        string? status = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        var statusFilter = NormalizeOptionalIdeaStatus(status);
        var ideas = await ExecuteReadTransactionAsync(
            connection => ReadIdeasInTx(connection, allWorkspaces, normalizedWorkspace, statusFilter),
            cancellationToken).ConfigureAwait(false);
        return new ProjectIdeaListResult(Ok: true, WorkspaceId: normalizedWorkspace, Ideas: ideas);
    }

    private static void EnsureVirtualMasterSchema(SqliteConnection connection)
    {
        ExecuteNonQuery(connection, """
            CREATE TABLE IF NOT EXISTS agent_sessions (
              workspace_id TEXT NOT NULL,
              agent TEXT NOT NULL COLLATE NOCASE,
              session_id TEXT,
              tool TEXT,
              model TEXT,
              state TEXT NOT NULL DEFAULT 'idle',
              activity TEXT,
              issue_number INTEGER,
              branch TEXT,
              registered_at_utc TEXT NOT NULL,
              updated_at_utc TEXT NOT NULL,
              activity_expires_utc TEXT,
              PRIMARY KEY (workspace_id, agent)
            );
            CREATE TABLE IF NOT EXISTS project_state (
              workspace_id TEXT PRIMARY KEY,
              active_goal TEXT,
              current_slice TEXT,
              lead_agent TEXT,
              lead_lease_expires_utc TEXT,
              decisions_json TEXT NOT NULL DEFAULT '[]',
              updated_at_utc TEXT NOT NULL,
              updated_by TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS project_ideas (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id TEXT NOT NULL,
              title TEXT NOT NULL,
              details TEXT,
              priority INTEGER NOT NULL DEFAULT 2,
              status TEXT NOT NULL DEFAULT 'queued',
              issue_number INTEGER,
              created_by TEXT NOT NULL,
              created_at_utc TEXT NOT NULL,
              updated_at_utc TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_project_ideas_ws_status
              ON project_ideas (workspace_id, status, priority, id);
            """);
    }

    private static string? ResolveRegisteredCodexThreadIdInTx(
        SqliteConnection connection,
        string workspaceId,
        string recipient,
        DateTimeOffset now)
    {
        var isBroadcast = string.Equals(recipient, AllRecipient, StringComparison.OrdinalIgnoreCase);
        var minUpdatedAtUtc = now.AddHours(-SwarmBusConstants.DefaultSessionRegistrationMaxAgeHours);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT session_id, updated_at_utc
            FROM agent_sessions
            WHERE workspace_id = @workspaceId
              AND (lower(agent) = lower(@recipient) OR (@isBroadcast = 1 AND lower(agent) = 'codex'))
              AND session_id IS NOT NULL
              AND trim(session_id) <> ''
            ORDER BY updated_at_utc DESC
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamRecipient, recipient);
        cmd.Parameters.AddWithValue("@isBroadcast", isBroadcast ? 1 : 0);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        var sessionId = GetNullableString(reader, 0);
        var updatedAtUtc = GetNullableUtc(reader, 1);
        if (string.IsNullOrWhiteSpace(sessionId) || !updatedAtUtc.HasValue || updatedAtUtc.Value <= minUpdatedAtUtc)
        {
            return null;
        }

        return sessionId.Trim();
    }

    private static ValidatedSessionUpsert ValidateSessionUpsertRequest(AgentSessionUpsertRequest request, string action)
    {
        ArgumentNullException.ThrowIfNull(request);

        var agent = RequireNonEmpty(request.Agent, "bus_sessions requires non-empty \"agent\".", nameof(request));
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var normalizedAction = string.Equals(action?.Trim(), "heartbeat", StringComparison.OrdinalIgnoreCase)
            ? "heartbeat"
            : "register";
        var normalizedState = NormalizeOptionalSessionState(request.State, nameof(request));
        var ttlMinutes = Math.Clamp(
            request.TtlMinutes,
            SwarmBusConstants.MinSessionHeartbeatTtlMinutes,
            SwarmBusConstants.MaxSessionHeartbeatTtlMinutes);

        return new ValidatedSessionUpsert(
            WorkspaceId: workspaceId,
            Agent: agent,
            Action: normalizedAction,
            SessionId: NormalizeOptionalText(request.SessionId),
            Tool: NormalizeOptionalText(request.Tool),
            Model: NormalizeOptionalText(request.Model),
            State: normalizedState,
            Activity: NormalizeOptionalText(request.Activity),
            IssueNumber: request.IssueNumber,
            Branch: NormalizeOptionalText(request.Branch),
            TtlMinutes: ttlMinutes);
    }

    private static (string Agent, string WorkspaceId) ValidateUnregisterSessionArgs(string agent, string? workspaceId)
    {
        var normalizedAgent = RequireNonEmpty(agent, "bus_sessions unregister requires non-empty \"agent\".", nameof(agent));
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        return (normalizedAgent, normalizedWorkspace);
    }

    private AgentSessionMutationResult UpsertSessionInTx(SqliteConnection connection, ValidatedSessionUpsert input)
    {
        var now = _timeProvider.GetUtcNow();
        var existing = ReadSingleSessionInTx(connection, input.WorkspaceId, input.Agent, now);
        var effectiveAgent = existing?.Agent ?? input.Agent;

        var sessionId = input.SessionId ?? existing?.SessionId;
        var tool = input.Tool ?? existing?.Tool;
        var model = input.Model ?? existing?.Model;
        var issueNumber = input.IssueNumber ?? existing?.IssueNumber;
        var branch = input.Branch ?? existing?.Branch;
        var state = input.State ?? existing?.EffectiveState ?? SessionStateIdle;

        string? activity;
        DateTimeOffset? activityExpiresUtc;
        if (string.Equals(state, SessionStateIdle, StringComparison.Ordinal))
        {
            activity = input.Activity;
            activityExpiresUtc = null;
        }
        else
        {
            var inheritedActivity = existing is not null &&
                                    !string.Equals(existing.EffectiveState, SessionStateIdle, StringComparison.Ordinal)
                ? existing.Activity
                : null;
            activity = input.Activity ?? inheritedActivity;
            activityExpiresUtc = now.AddMinutes(input.TtlMinutes);
        }

        var registeredAtIso = FormatUtc(existing?.RegisteredAtUtc ?? now);
        var updatedAtIso = FormatUtc(now);
        var activityExpiresIso = activityExpiresUtc.HasValue ? FormatUtc(activityExpiresUtc.Value) : null;

        using var upsertCmd = connection.CreateCommand();
        upsertCmd.CommandText = """
            INSERT INTO agent_sessions (
                workspace_id, agent, session_id, tool, model, state, activity,
                issue_number, branch, registered_at_utc, updated_at_utc, activity_expires_utc)
            VALUES (
                @workspaceId, @agent, @sessionId, @tool, @model, @state, @activity,
                @issueNumber, @branch, @registeredAt, @updatedAt, @activityExpires)
            ON CONFLICT(workspace_id, agent) DO UPDATE SET
                session_id = excluded.session_id,
                tool = excluded.tool,
                model = excluded.model,
                state = excluded.state,
                activity = excluded.activity,
                issue_number = excluded.issue_number,
                branch = excluded.branch,
                updated_at_utc = excluded.updated_at_utc,
                activity_expires_utc = excluded.activity_expires_utc
            """;
        upsertCmd.Parameters.AddWithValue(ParamWorkspaceId, input.WorkspaceId);
        upsertCmd.Parameters.AddWithValue(ParamAgent, effectiveAgent);
        upsertCmd.Parameters.AddWithValue("@sessionId", (object?)sessionId ?? DBNull.Value);
        upsertCmd.Parameters.AddWithValue("@tool", (object?)tool ?? DBNull.Value);
        upsertCmd.Parameters.AddWithValue("@model", (object?)model ?? DBNull.Value);
        upsertCmd.Parameters.AddWithValue(ParamState, state);
        upsertCmd.Parameters.AddWithValue("@activity", (object?)activity ?? DBNull.Value);
        upsertCmd.Parameters.AddWithValue(ParamIssueNumber, (object?)issueNumber ?? DBNull.Value);
        upsertCmd.Parameters.AddWithValue("@branch", (object?)branch ?? DBNull.Value);
        upsertCmd.Parameters.AddWithValue("@registeredAt", registeredAtIso);
        upsertCmd.Parameters.AddWithValue(ParamUpdatedAt, updatedAtIso);
        upsertCmd.Parameters.AddWithValue("@activityExpires", (object?)activityExpiresIso ?? DBNull.Value);
        upsertCmd.ExecuteNonQuery();

        var saved = ReadSingleSessionInTx(connection, input.WorkspaceId, effectiveAgent, now);
        return new AgentSessionMutationResult(
            Ok: true,
            Action: input.Action,
            WorkspaceId: input.WorkspaceId,
            Agent: effectiveAgent,
            Changed: true,
            Session: saved);
    }

    private static AgentSessionMutationResult UnregisterSessionInTx(
        SqliteConnection connection,
        string agent,
        string workspaceId)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            DELETE FROM agent_sessions
            WHERE workspace_id = @workspaceId
              AND lower(agent) = lower(@agent)
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamAgent, agent);
        var removed = cmd.ExecuteNonQuery();

        return new AgentSessionMutationResult(
            Ok: true,
            Action: "unregister",
            WorkspaceId: workspaceId,
            Agent: agent,
            Changed: removed > 0,
            Session: null);
    }

    private static AgentSessionRecord? ReadSingleSessionInTx(
        SqliteConnection connection,
        string workspaceId,
        string agent,
        DateTimeOffset now)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT workspace_id, agent, session_id, tool, model, state, activity,
                   issue_number, branch, registered_at_utc, updated_at_utc, activity_expires_utc
            FROM agent_sessions
            WHERE workspace_id = @workspaceId
              AND lower(agent) = lower(@agent)
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamAgent, agent);

        using var reader = cmd.ExecuteReader();
        return reader.Read() ? MapAgentSession(reader, now) : null;
    }

    private static List<AgentSessionRecord> ReadAgentSessionsInTx(
        SqliteConnection connection,
        bool allWorkspaces,
        string workspaceId,
        DateTimeOffset now)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT workspace_id, agent, session_id, tool, model, state, activity,
                   issue_number, branch, registered_at_utc, updated_at_utc, activity_expires_utc
            FROM agent_sessions
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
            ORDER BY workspace_id ASC, lower(agent) ASC
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);

        var rows = new List<AgentSessionRecord>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(MapAgentSession(reader, now));
        }

        return rows;
    }

    private static AgentSessionRecord MapAgentSession(SqliteDataReader reader, DateTimeOffset now)
    {
        var state = GetStringOrDefault(reader, 5, SessionStateIdle).Trim().ToLowerInvariant();
        if (!SwarmBusConstants.ValidSessionStates.Contains(state))
        {
            state = SessionStateIdle;
        }

        var activityExpiresUtc = GetNullableUtc(reader, 11);
        var effectiveState = !string.Equals(state, SessionStateIdle, StringComparison.Ordinal) &&
                             (!activityExpiresUtc.HasValue || activityExpiresUtc.Value <= now)
            ? SessionStateIdle
            : state;

        return new AgentSessionRecord(
            WorkspaceId: reader.GetString(0),
            Agent: reader.GetString(1),
            SessionId: GetNullableString(reader, 2),
            Tool: GetNullableString(reader, 3),
            Model: GetNullableString(reader, 4),
            State: state,
            EffectiveState: effectiveState,
            Activity: GetNullableString(reader, 6),
            IssueNumber: GetNullableInt32(reader, 7),
            Branch: GetNullableString(reader, 8),
            RegisteredAtUtc: ParseUtcOrMin(reader.GetString(9)),
            UpdatedAtUtc: ParseUtcOrMin(reader.GetString(10)),
            ActivityExpiresUtc: activityExpiresUtc);
    }

    private static void ValidateClaimProjectTurnRequest(ProjectTurnClaimRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        _ = RequireNonEmpty(request.Agent, "bus_project_sync claim-turn requires non-empty \"agent\".", nameof(request));
        _ = NormalizeOptionalSessionState(request.State, nameof(request));
    }

    private static void ValidateUpdateProjectStateRequest(ProjectStateUpdateRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        _ = RequireNonEmpty(request.Agent, "bus_project_sync update requires non-empty \"agent\".", nameof(request));
    }

    private ProjectSyncResult ClaimProjectTurnInTx(SqliteConnection connection, ProjectTurnClaimRequest request)
    {
        var agent = request.Agent.Trim();
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var ttlMinutes = Math.Clamp(
            request.TtlMinutes,
            SwarmBusConstants.MinLeadTurnTtlMinutes,
            SwarmBusConstants.MaxLeadTurnTtlMinutes);

        var now = _timeProvider.GetUtcNow();
        var existing = ReadSingleProjectStateInTx(connection, workspaceId, now);
        var previousLeadAgent = existing is not null && existing.IsLeadActive ? existing.LeadAgent : null;

        var activeGoal = NormalizeOptionalText(request.ActiveGoal) ?? existing?.ActiveGoal;
        var currentSlice = NormalizeOptionalText(request.CurrentSlice) ?? existing?.CurrentSlice;
        var decisions = AppendDecision(existing?.Decisions, request.Decision);
        var leaseExpiresIso = FormatUtc(now.AddMinutes(ttlMinutes));

        UpsertProjectStateRowInTx(
            connection,
            workspaceId,
            activeGoal,
            currentSlice,
            leadAgent: agent,
            leadLeaseExpiresIso: leaseExpiresIso,
            decisions: decisions,
            updatedAtIso: FormatUtc(now),
            updatedBy: agent);

        var hasSessionFields = !string.IsNullOrWhiteSpace(request.SessionId) ||
                               !string.IsNullOrWhiteSpace(request.Tool) ||
                               !string.IsNullOrWhiteSpace(request.Model) ||
                               !string.IsNullOrWhiteSpace(request.State) ||
                               !string.IsNullOrWhiteSpace(request.Activity) ||
                               request.IssueNumber.HasValue ||
                               !string.IsNullOrWhiteSpace(request.Branch);

        if (hasSessionFields || ReadSingleSessionInTx(connection, workspaceId, agent, now) is not null)
        {
            var sessionUpsert = new ValidatedSessionUpsert(
                WorkspaceId: workspaceId,
                Agent: agent,
                Action: "heartbeat",
                SessionId: NormalizeOptionalText(request.SessionId),
                Tool: NormalizeOptionalText(request.Tool),
                Model: NormalizeOptionalText(request.Model),
                State: NormalizeOptionalSessionState(request.State, nameof(request)),
                Activity: NormalizeOptionalText(request.Activity),
                IssueNumber: request.IssueNumber,
                Branch: NormalizeOptionalText(request.Branch),
                TtlMinutes: SwarmBusConstants.DefaultSessionHeartbeatTtlMinutes);
            UpsertSessionInTx(connection, sessionUpsert);
        }

        return BuildProjectSyncResultInTx(connection, "claim-turn", workspaceId, agent, previousLeadAgent);
    }

    private ProjectSyncResult UpdateProjectStateInTx(SqliteConnection connection, ProjectStateUpdateRequest request)
    {
        var agent = request.Agent.Trim();
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var now = _timeProvider.GetUtcNow();
        var existing = ReadSingleProjectStateInTx(connection, workspaceId, now);

        var activeGoal = NormalizeOptionalText(request.ActiveGoal) ?? existing?.ActiveGoal;
        var currentSlice = NormalizeOptionalText(request.CurrentSlice) ?? existing?.CurrentSlice;

        IReadOnlyList<string> decisions;
        if (request.Decisions is not null)
        {
            decisions = NormalizeDecisionsList(request.Decisions);
        }
        else if (!string.IsNullOrWhiteSpace(request.Decision))
        {
            decisions = AppendDecision(existing?.Decisions, request.Decision);
        }
        else
        {
            decisions = existing?.Decisions ?? [];
        }

        string? leadAgent = existing?.LeadAgent;
        string? leadLeaseExpiresIso = existing?.LeadLeaseExpiresUtc is DateTimeOffset exp ? FormatUtc(exp) : null;
        string? previousLeadAgent = null;

        if (request.ReleaseLead)
        {
            var canRelease = existing is null ||
                             string.IsNullOrWhiteSpace(existing.LeadAgent) ||
                             !existing.IsLeadActive ||
                             string.Equals(existing.LeadAgent, agent, StringComparison.OrdinalIgnoreCase);

            if (!canRelease)
            {
                return BuildProjectSyncResultInTx(connection, "update", workspaceId, agent, previousLeadAgent: null) with
                {
                    Ok = false,
                    Reason = "lead_held_by_other"
                };
            }

            if (existing is not null && existing.IsLeadActive)
            {
                previousLeadAgent = existing.LeadAgent;
            }

            leadAgent = null;
            leadLeaseExpiresIso = null;
        }

        UpsertProjectStateRowInTx(
            connection,
            workspaceId,
            activeGoal,
            currentSlice,
            leadAgent,
            leadLeaseExpiresIso,
            decisions,
            updatedAtIso: FormatUtc(now),
            updatedBy: agent);

        return BuildProjectSyncResultInTx(connection, "update", workspaceId, agent, previousLeadAgent);
    }

    private ProjectSyncResult BuildProjectSyncResultInTx(
        SqliteConnection connection,
        string action,
        string workspaceId,
        string? agent,
        string? previousLeadAgent)
    {
        var now = _timeProvider.GetUtcNow();
        var projectState = ReadSingleProjectStateInTx(connection, workspaceId, now);
        var sessions = ReadAgentSessionsInTx(connection, allWorkspaces: false, workspaceId, now);
        var queuedIdeas = ReadIdeasInTx(connection, allWorkspaces: false, workspaceId, IdeaStatusQueued);

        var effectiveAgent = string.IsNullOrWhiteSpace(agent) ? AllRecipient : agent.Trim();
        var includeAllRecipients = effectiveAgent.Equals(AllRecipient, StringComparison.OrdinalIgnoreCase) ||
                                   effectiveAgent == AllWildcard;
        var unreadMessages = ReadInboxMessages(
            connection,
            allWorkspaces: false,
            workspaceId,
            includeAllRecipients,
            effectiveAgent,
            unreadOnly: true,
            limit: 20);

        return new ProjectSyncResult(
            Ok: true,
            Action: action,
            WorkspaceId: workspaceId,
            ProjectState: projectState,
            PreviousLeadAgent: previousLeadAgent,
            Sessions: sessions,
            QueuedIdeas: queuedIdeas,
            UnreadMessages: unreadMessages);
    }

    private static void UpsertProjectStateRowInTx(
        SqliteConnection connection,
        string workspaceId,
        string? activeGoal,
        string? currentSlice,
        string? leadAgent,
        string? leadLeaseExpiresIso,
        IReadOnlyList<string> decisions,
        string updatedAtIso,
        string updatedBy)
    {
        var decisionsJson = JsonSerializer.Serialize(decisions);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO project_state (
                workspace_id, active_goal, current_slice, lead_agent,
                lead_lease_expires_utc, decisions_json, updated_at_utc, updated_by)
            VALUES (
                @workspaceId, @activeGoal, @currentSlice, @leadAgent,
                @leadLeaseExpires, @decisionsJson, @updatedAt, @updatedBy)
            ON CONFLICT(workspace_id) DO UPDATE SET
                active_goal = excluded.active_goal,
                current_slice = excluded.current_slice,
                lead_agent = excluded.lead_agent,
                lead_lease_expires_utc = excluded.lead_lease_expires_utc,
                decisions_json = excluded.decisions_json,
                updated_at_utc = excluded.updated_at_utc,
                updated_by = excluded.updated_by
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue("@activeGoal", (object?)activeGoal ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@currentSlice", (object?)currentSlice ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@leadAgent", (object?)leadAgent ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@leadLeaseExpires", (object?)leadLeaseExpiresIso ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@decisionsJson", decisionsJson);
        cmd.Parameters.AddWithValue(ParamUpdatedAt, updatedAtIso);
        cmd.Parameters.AddWithValue("@updatedBy", updatedBy);
        cmd.ExecuteNonQuery();
    }

    private static ProjectStateRecord? ReadSingleProjectStateInTx(
        SqliteConnection connection,
        string workspaceId,
        DateTimeOffset now)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT workspace_id, active_goal, current_slice, lead_agent,
                   lead_lease_expires_utc, decisions_json, updated_at_utc, updated_by
            FROM project_state
            WHERE workspace_id = @workspaceId
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);

        using var reader = cmd.ExecuteReader();
        return reader.Read() ? MapProjectState(reader, now) : null;
    }

    private static List<ProjectStateRecord> ReadProjectStatesInTx(
        SqliteConnection connection,
        bool allWorkspaces,
        string workspaceId,
        DateTimeOffset now)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT workspace_id, active_goal, current_slice, lead_agent,
                   lead_lease_expires_utc, decisions_json, updated_at_utc, updated_by
            FROM project_state
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
            ORDER BY workspace_id ASC
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);

        var rows = new List<ProjectStateRecord>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(MapProjectState(reader, now));
        }

        return rows;
    }

    private static ProjectStateRecord MapProjectState(SqliteDataReader reader, DateTimeOffset now)
    {
        var leadAgent = GetNullableString(reader, 3);
        var leadLeaseExpiresUtc = GetNullableUtc(reader, 4);
        var isLeadActive = !string.IsNullOrWhiteSpace(leadAgent) &&
                           leadLeaseExpiresUtc.HasValue &&
                           leadLeaseExpiresUtc.Value > now;
        var decisions = DeserializeDecisions(GetNullableString(reader, 5));

        return new ProjectStateRecord(
            WorkspaceId: reader.GetString(0),
            ActiveGoal: GetNullableString(reader, 1),
            CurrentSlice: GetNullableString(reader, 2),
            LeadAgent: leadAgent,
            LeadLeaseExpiresUtc: leadLeaseExpiresUtc,
            IsLeadActive: isLeadActive,
            Decisions: decisions,
            UpdatedAtUtc: ParseUtcOrMin(reader.GetString(6)),
            UpdatedBy: reader.GetString(7));
    }

    private static (
        string WorkspaceId,
        string Title,
        string CreatedBy,
        string? Details,
        int Priority,
        int? IssueNumber) ValidateIdeaAddRequest(ProjectIdeaAddRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var title = RequireNonEmpty(request.Title, "bus_ideas add requires non-empty \"title\".", nameof(request));
        var createdBy = RequireNonEmpty(request.CreatedBy, "bus_ideas add requires non-empty \"agent\".", nameof(request));
        var details = NormalizeOptionalText(request.Details);
        var priority = Math.Clamp(request.Priority, 0, 3);
        return (workspaceId, title, createdBy, details, priority, request.IssueNumber);
    }

    private static (
        long Id,
        string? ExpectedWorkspaceId,
        string? Title,
        string? Details,
        bool HasDetails,
        int? Priority,
        string? Status,
        int? IssueNumber) ValidateIdeaUpdateRequest(ProjectIdeaUpdateRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Id <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "bus_ideas update requires positive \"id\".");
        }

        var expectedWorkspaceId = string.IsNullOrWhiteSpace(request.WorkspaceId)
            ? null
            : PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var title = NormalizeOptionalText(request.Title);
        var hasDetails = request.Details is not null;
        var details = NormalizeOptionalText(request.Details);
        int? priority = request.Priority.HasValue ? Math.Clamp(request.Priority.Value, 0, 3) : null;
        var status = NormalizeOptionalIdeaStatus(request.Status);
        return (request.Id, expectedWorkspaceId, title, details, hasDetails, priority, status, request.IssueNumber);
    }

    private ProjectIdeaMutationResult AddIdeaInTx(
        SqliteConnection connection,
        string workspaceId,
        string title,
        string createdBy,
        string? details,
        int priority,
        int? issueNumber)
    {
        var nowIso = FormatUtc(_timeProvider.GetUtcNow());

        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO project_ideas (
                workspace_id, title, details, priority, status,
                issue_number, created_by, created_at_utc, updated_at_utc)
            VALUES (
                @workspaceId, @title, @details, @priority, 'queued',
                @issueNumber, @createdBy, @createdAt, @updatedAt);
            SELECT last_insert_rowid();
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamTitle, title);
        cmd.Parameters.AddWithValue("@details", (object?)details ?? DBNull.Value);
        cmd.Parameters.AddWithValue(ParamPriority, priority);
        cmd.Parameters.AddWithValue(ParamIssueNumber, (object?)issueNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@createdBy", createdBy);
        cmd.Parameters.AddWithValue(ParamCreatedAt, nowIso);
        cmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);

        var id = Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
        var item = ReadSingleIdeaByIdInTx(connection, id);

        return new ProjectIdeaMutationResult(
            Ok: true,
            Action: "add",
            WorkspaceId: workspaceId,
            Changed: true,
            Idea: item);
    }

    private ProjectIdeaMutationResult UpdateIdeaInTx(
        SqliteConnection connection,
        long id,
        string? expectedWorkspaceId,
        string? title,
        string? details,
        bool hasDetails,
        int? priority,
        string? status,
        int? issueNumber)
    {
        var existing = ReadSingleIdeaByIdInTx(connection, id);
        if (existing is null)
        {
            return new ProjectIdeaMutationResult(
                Ok: false,
                Action: "update",
                WorkspaceId: expectedWorkspaceId ?? SwarmBusConstants.DefaultWorkspaceId,
                Changed: false,
                Idea: null,
                Reason: "not_found");
        }

        if (expectedWorkspaceId is not null &&
            !string.Equals(existing.WorkspaceId, expectedWorkspaceId, StringComparison.Ordinal))
        {
            return new ProjectIdeaMutationResult(
                Ok: false,
                Action: "update",
                WorkspaceId: expectedWorkspaceId,
                Changed: false,
                Idea: null,
                Reason: "workspace_mismatch");
        }

        var nextTitle = title ?? existing.Title;
        var nextDetails = hasDetails ? details : existing.Details;
        var nextPriority = priority ?? existing.Priority;
        var nextStatus = status ?? existing.Status;
        var nextIssueNumber = issueNumber ?? existing.IssueNumber;
        var nowIso = FormatUtc(_timeProvider.GetUtcNow());

        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE project_ideas
            SET title = @title,
                details = @details,
                priority = @priority,
                status = @status,
                issue_number = @issueNumber,
                updated_at_utc = @updatedAt
            WHERE id = @id
            """;
        cmd.Parameters.AddWithValue(ParamTitle, nextTitle);
        cmd.Parameters.AddWithValue("@details", (object?)nextDetails ?? DBNull.Value);
        cmd.Parameters.AddWithValue(ParamPriority, nextPriority);
        cmd.Parameters.AddWithValue(ParamStatus, nextStatus);
        cmd.Parameters.AddWithValue(ParamIssueNumber, (object?)nextIssueNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        cmd.Parameters.AddWithValue(ParamId, id);
        cmd.ExecuteNonQuery();

        var updated = ReadSingleIdeaByIdInTx(connection, id);
        return new ProjectIdeaMutationResult(
            Ok: true,
            Action: "update",
            WorkspaceId: existing.WorkspaceId,
            Changed: true,
            Idea: updated);
    }

    private static ProjectIdeaItem? ReadSingleIdeaByIdInTx(SqliteConnection connection, long id)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, title, details, priority, status,
                   issue_number, created_by, created_at_utc, updated_at_utc
            FROM project_ideas
            WHERE id = @id
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue(ParamId, id);

        using var reader = cmd.ExecuteReader();
        return reader.Read() ? MapProjectIdea(reader) : null;
    }

    private static List<ProjectIdeaItem> ReadIdeasInTx(
        SqliteConnection connection,
        bool allWorkspaces,
        string workspaceId,
        string? statusFilter)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, title, details, priority, status,
                   issue_number, created_by, created_at_utc, updated_at_utc
            FROM project_ideas
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
              AND (@status IS NULL OR status = @status)
            ORDER BY priority ASC, id ASC
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamStatus, (object?)statusFilter ?? DBNull.Value);

        var items = new List<ProjectIdeaItem>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(MapProjectIdea(reader));
        }

        return items;
    }

    private static int CountQueuedIdeasInTx(
        SqliteConnection connection,
        bool allWorkspaces,
        string workspaceId)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*)
            FROM project_ideas
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
              AND status = 'queued'
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        return Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private static ProjectIdeaItem MapProjectIdea(SqliteDataReader reader) =>
        new(
            Id: reader.GetInt64(0),
            WorkspaceId: reader.GetString(1),
            Title: reader.GetString(2),
            Details: GetNullableString(reader, 3),
            Priority: reader.GetInt32(4),
            Status: reader.GetString(5),
            IssueNumber: GetNullableInt32(reader, 6),
            CreatedBy: reader.GetString(7),
            CreatedAtUtc: ParseUtcOrMin(reader.GetString(8)),
            UpdatedAtUtc: ParseUtcOrMin(reader.GetString(9)));

    private static string? NormalizeOptionalSessionState(string? rawState, string paramName)
    {
        if (string.IsNullOrWhiteSpace(rawState))
        {
            return null;
        }

        var normalized = rawState.Trim().ToLowerInvariant();
        if (!SwarmBusConstants.ValidSessionStates.Contains(normalized))
        {
            throw new ArgumentException(
                $"Invalid session state '{rawState}'. Expected one of: {string.Join(", ", SwarmBusConstants.ValidSessionStates)}.",
                paramName);
        }

        return normalized;
    }

    private static string? NormalizeOptionalIdeaStatus(string? rawStatus)
    {
        if (string.IsNullOrWhiteSpace(rawStatus))
        {
            return null;
        }

        var normalized = rawStatus.Trim().ToLowerInvariant();
        if (!SwarmBusConstants.ValidIdeaStatuses.Contains(normalized))
        {
            throw new ArgumentException(
                $"Invalid idea status '{rawStatus}'. Expected one of: {string.Join(", ", SwarmBusConstants.ValidIdeaStatuses)}.",
                nameof(rawStatus));
        }

        return normalized;
    }

    private static string? NormalizeOptionalText(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static List<string> AppendDecision(IReadOnlyList<string>? existing, string? rawDecision)
    {
        var list = existing is null ? new List<string>() : new List<string>(existing);
        var trimmed = NormalizeOptionalText(rawDecision);
        if (trimmed is null)
        {
            return CapDecisions(list);
        }

        if (list.Count == 0 || !string.Equals(list[^1], trimmed, StringComparison.Ordinal))
        {
            list.Add(trimmed);
        }

        return CapDecisions(list);
    }

    private static List<string> NormalizeDecisionsList(IReadOnlyList<string> rawDecisions)
    {
        var list = new List<string>(rawDecisions.Count);
        foreach (var item in rawDecisions)
        {
            var trimmed = NormalizeOptionalText(item);
            if (trimmed is not null)
            {
                list.Add(trimmed);
            }
        }

        return CapDecisions(list);
    }

    private static List<string> CapDecisions(List<string> list)
    {
        if (list.Count <= SwarmBusConstants.MaxProjectDecisions)
        {
            return list;
        }

        return list.GetRange(list.Count - SwarmBusConstants.MaxProjectDecisions, SwarmBusConstants.MaxProjectDecisions);
    }

    private static IReadOnlyList<string> DeserializeDecisions(string? rawJson)
    {
        if (string.IsNullOrWhiteSpace(rawJson))
        {
            return [];
        }

        try
        {
            var parsed = JsonSerializer.Deserialize<List<string>>(rawJson);
            return parsed is null ? [] : NormalizeDecisionsList(parsed);
        }
        catch (JsonException)
        {
            return [];
        }
    }
}
