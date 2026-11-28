using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using AISwarmX.SwarmBus.Core.Contracts;
using AISwarmX.SwarmBus.Core.Locking;
using AISwarmX.SwarmBus.Core.Store;

namespace AISwarmX.SwarmBus.Mcp;

/// <summary>
/// Shared tool execution dispatcher used by both the stdio MCP server and <c>--cli</c> mode.
/// </summary>
internal static class SwarmBusToolDispatcher
{
    public static readonly JsonSerializerOptions CompactJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    public static readonly JsonSerializerOptions IndentedJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true
    };

    public static bool IsKnownTool(string toolName) => toolName switch
    {
        "bus_send" or
        "bus_inbox" or
        "bus_ack" or
        "bus_locks" or
        "bus_review_claim" or
        "bus_queue" or
        "bus_status" or
        "bus_wait" or
        "bus_start_work" or
        "bus_sessions" or
        "bus_project_sync" or
        "bus_ideas" => true,
        _ => false
    };

    public static async Task<(object Payload, bool IsError)> ExecuteAsync(
        SwarmBusStore store,
        string toolName,
        JsonElement args,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(store);

        switch (toolName)
        {
            case "bus_send":
                return (await ExecuteSendAsync(store, args, cancellationToken).ConfigureAwait(false), false);

            case "bus_inbox":
                return (await ExecuteInboxAsync(store, args, cancellationToken).ConfigureAwait(false), false);

            case "bus_ack":
                return (await ExecuteAckAsync(store, args, cancellationToken).ConfigureAwait(false), false);

            case "bus_locks":
                return await ExecuteLocksAsync(store, args, cancellationToken).ConfigureAwait(false);

            case "bus_review_claim":
                return (await ExecuteReviewClaimAsync(store, args, cancellationToken).ConfigureAwait(false), false);

            case "bus_queue":
                return await ExecuteQueueAsync(store, args, cancellationToken).ConfigureAwait(false);

            case "bus_status":
                return (await ExecuteStatusAsync(store, args, cancellationToken).ConfigureAwait(false), false);

            case "bus_wait":
                return (await ExecuteWaitAsync(store, args, cancellationToken).ConfigureAwait(false), false);

            case "bus_start_work":
                var startRes = await ExecuteStartWorkAsync(store, args, cancellationToken).ConfigureAwait(false);
                return (startRes, !startRes.Ok);

            case "bus_sessions":
                return await ExecuteSessionsAsync(store, args, cancellationToken).ConfigureAwait(false);

            case "bus_project_sync":
                return await ExecuteProjectSyncAsync(store, args, cancellationToken).ConfigureAwait(false);

            case "bus_ideas":
                return await ExecuteIdeasAsync(store, args, cancellationToken).ConfigureAwait(false);

            default:
                throw new ArgumentException($"Unknown tool: '{toolName}'.", nameof(toolName));
        }
    }

    private static Task<SwarmSendResult> ExecuteSendAsync(SwarmBusStore store, JsonElement args, CancellationToken ct)
    {
        var request = new SwarmSendRequest(
            From: GetString(args, "from", "sender") ?? string.Empty,
            To: GetString(args, "to", "recipient") ?? string.Empty,
            Topic: GetString(args, "topic") ?? string.Empty,
            Body: GetString(args, "body") ?? string.Empty,
            PrNumber: GetOptionalInt32(args, "pr", "pr_number", "prNumber"),
            IssueNumber: GetOptionalInt32(args, "issue", "issue_number", "issueNumber"),
            Priority: GetString(args, "priority"),
            ReplyToId: GetOptionalInt64(args, "reply_to", "reply_to_id", "replyToId"),
            State: GetString(args, "state"),
            Wake: GetBool(args, false, "wake"),
            CodexThreadId: GetString(args, "codex_thread_id", "codexThreadId"),
            WorkspaceId: GetString(args, "workspace_id", "workspaceId"));

        return store.SendMessageAsync(request, ct);
    }

    private static Task<SwarmInboxResult> ExecuteInboxAsync(SwarmBusStore store, JsonElement args, CancellationToken ct)
    {
        var agent = GetString(args, "agent") ?? "all";
        var unreadOnly = GetBool(args, true, "unread_only", "unreadOnly");
        var limit = GetOptionalInt32(args, "limit") ?? 20;
        var workspaceId = GetString(args, "workspace_id", "workspaceId");
        return store.GetInboxAsync(agent, unreadOnly, limit, workspaceId, ct);
    }

    private static Task<SwarmAckResult> ExecuteAckAsync(SwarmBusStore store, JsonElement args, CancellationToken ct)
    {
        var messageId = GetOptionalInt64(args, "message_id", "messageId", "id") ?? 0;
        var agent = GetString(args, "agent") ?? string.Empty;
        var reply = GetString(args, "reply");
        return store.AckMessageAsync(messageId, agent, reply, ct);
    }

    private static async Task<(object Payload, bool IsError)> ExecuteLocksAsync(
        SwarmBusStore store,
        JsonElement args,
        CancellationToken ct)
    {
        var action = (GetString(args, "action") ?? "list").Trim().ToLowerInvariant();
        var workspaceId = GetString(args, "workspace_id", "workspaceId");

        switch (action)
        {
            case "list":
            {
                var locks = await store.ListLocksAsync(workspaceId, ct).ConfigureAwait(false);
                var normalizedWs = string.Equals(workspaceId?.Trim(), "*", StringComparison.Ordinal)
                    ? "*"
                    : PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
                return (new { ok = true, action = "list", workspaceId = normalizedWs, locks }, false);
            }

            case "claim":
            {
                var claimRes = await store.ClaimLocksAsync(new LockClaimRequest(
                    Agent: GetString(args, "agent") ?? string.Empty,
                    Paths: GetStringList(args, "paths"),
                    PrNumber: GetOptionalInt32(args, "pr", "pr_number", "prNumber"),
                    TtlMinutes: GetOptionalInt32(args, "ttl_minutes", "ttlMinutes") ?? SwarmBusConstants.DefaultLockTtlMinutes,
                    WorkspaceId: workspaceId), ct).ConfigureAwait(false);
                return (claimRes, !claimRes.Ok);
            }

            case "release":
            {
                var releaseRes = await store.ReleaseLocksAsync(new LockReleaseRequest(
                    Agent: GetString(args, "agent"),
                    Paths: GetStringList(args, "paths"),
                    PrNumber: GetOptionalInt32(args, "pr", "pr_number", "prNumber"),
                    WorkspaceId: workspaceId), ct).ConfigureAwait(false);
                return (releaseRes, !releaseRes.Ok);
            }

            default:
                throw new ArgumentException($"Unsupported bus_locks action '{action}'. Use 'list', 'claim', or 'release'.");
        }
    }

    private static Task<ReviewClaimResult> ExecuteReviewClaimAsync(SwarmBusStore store, JsonElement args, CancellationToken ct)
    {
        var request = new ReviewClaimRequest(
            Action: GetString(args, "action") ?? "claim",
            Agent: GetString(args, "agent"),
            PrNumber: GetOptionalInt32(args, "pr_number", "prNumber", "pr"),
            HeadSha: GetString(args, "head_sha", "headSha", "head", "sha"),
            Role: GetString(args, "role"),
            TtlMinutes: GetOptionalInt32(args, "ttl_minutes", "ttlMinutes"),
            WorkspaceId: GetString(args, "workspace_id", "workspaceId"));
        return store.ReviewClaimAsync(request, ct);
    }

    private static async Task<(object Payload, bool IsError)> ExecuteQueueAsync(
        SwarmBusStore store,
        JsonElement args,
        CancellationToken ct)
    {
        var action = (GetString(args, "action") ?? "list").Trim().ToLowerInvariant();
        var workspaceId = GetString(args, "workspace_id", "workspaceId");

        switch (action)
        {
            case "list":
            {
                var res = await store.QueueListAsync(GetString(args, "status"), workspaceId, ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "next":
            {
                var res = await store.QueueNextAsync(GetString(args, "agent") ?? string.Empty, workspaceId, ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "resume":
            {
                var issueNumber = GetOptionalInt32(args, "issue_number", "issueNumber", "issue") ?? 0;
                var owner = GetString(args, "owner", "agent") ?? string.Empty;
                var res = await store.QueueResumeAsync(issueNumber, owner, workspaceId, ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "release":
            {
                var issueNumber = GetOptionalInt32(args, "issue_number", "issueNumber", "issue") ?? 0;
                var res = await store.QueueReleaseAsync(issueNumber, GetString(args, "agent"), workspaceId, ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "done":
            {
                var issueNumber = GetOptionalInt32(args, "issue_number", "issueNumber", "issue") ?? 0;
                var claimedBeforeUtc = GetOptionalUtc(args, "claimed_before_utc", "claimedBeforeUtc");
                var res = await store.QueueDoneAsync(new WorkQueueDoneRequest(
                    IssueNumber: issueNumber,
                    WorkspaceId: workspaceId,
                    ExpectedStatus: GetString(args, "expected_status", "expectedStatus"),
                    ExpectedOwner: GetString(args, "expected_owner", "expectedOwner"),
                    ClaimedBeforeUtc: claimedBeforeUtc), ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "upsert":
            {
                var issueNumber = GetOptionalInt32(args, "issue_number", "issueNumber", "issue") ?? 0;
                var res = await store.QueueUpsertAsync(new WorkQueueUpsertRequest(
                    IssueNumber: issueNumber,
                    Title: GetString(args, "title"),
                    Priority: GetOptionalInt32(args, "priority"),
                    RiskClass: GetString(args, "risk_class", "riskClass"),
                    Status: GetString(args, "status") ?? "queued",
                    WorkspaceId: workspaceId), ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "sync-meta":
            {
                var metaKey = GetString(args, "meta_key", "metaKey") ?? string.Empty;
                var metaValue = GetScalarString(args, "meta_value", "metaValue");
                if (metaValue is null)
                {
                    throw new ArgumentException("bus_queue sync-meta requires non-empty \"meta_value\".");
                }

                await store.SyncMetaAsync(metaKey, metaValue, workspaceId, ct).ConfigureAwait(false);
                return (new
                {
                    ok = true,
                    action = "sync-meta",
                    workspaceId = PathPatternMatcher.NormalizeWorkspaceId(workspaceId),
                    key = metaKey.Trim(),
                    value = metaValue
                }, false);
            }

            default:
                throw new ArgumentException(
                    $"Unsupported bus_queue action '{action}'. Use: list, next, resume, release, done, upsert, sync-meta.");
        }
    }

    private static Task<SwarmBusStatusSnapshot> ExecuteStatusAsync(SwarmBusStore store, JsonElement args, CancellationToken ct)
    {
        var limit = GetOptionalInt32(args, "limit") ?? 15;
        var workspaceId = GetString(args, "workspace_id", "workspaceId");
        return store.GetStatusAsync(limit, workspaceId, ct);
    }

    private static Task<SwarmWaitResult> ExecuteWaitAsync(SwarmBusStore store, JsonElement args, CancellationToken ct)
    {
        var request = new SwarmWaitRequest(
            Agent: GetString(args, "agent") ?? string.Empty,
            SinceId: GetOptionalInt64(args, "since_id", "sinceId") ?? 0,
            MentionsOnly: GetBool(args, false, "mentions_only", "mentionsOnly"),
            TimeoutMs: GetOptionalInt32(args, "timeout_ms", "timeoutMs") ?? SwarmBusConstants.DefaultWaitTimeoutMs,
            Limit: GetOptionalInt32(args, "limit") ?? 20,
            WorkspaceId: GetString(args, "workspace_id", "workspaceId"));
        return store.WaitForMessagesAsync(request, ct);
    }

    private static Task<SwarmStartWorkResult> ExecuteStartWorkAsync(SwarmBusStore store, JsonElement args, CancellationToken ct)
    {
        var request = new SwarmStartWorkRequest(
            Agent: GetString(args, "agent") ?? string.Empty,
            Paths: GetStringList(args, "paths"),
            PrNumber: GetOptionalInt32(args, "pr", "pr_number", "prNumber"),
            TtlMinutes: GetOptionalInt32(args, "ttl_minutes", "ttlMinutes") ?? SwarmBusConstants.DefaultLockTtlMinutes,
            MessageLimit: GetOptionalInt32(args, "message_limit", "messageLimit") ?? 20,
            WorkspaceId: GetString(args, "workspace_id", "workspaceId"));
        return store.StartWorkAsync(request, ct);
    }

    private static async Task<(object Payload, bool IsError)> ExecuteSessionsAsync(
        SwarmBusStore store,
        JsonElement args,
        CancellationToken ct)
    {
        var action = (GetString(args, "action") ?? "list").Trim().ToLowerInvariant();
        var workspaceId = GetString(args, "workspace_id", "workspaceId");

        switch (action)
        {
            case "list":
            {
                var res = await store.ListSessionsAsync(workspaceId, ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "register":
            case "heartbeat":
            {
                var res = await store.UpsertSessionAsync(
                    new AgentSessionUpsertRequest(
                        WorkspaceId: workspaceId ?? string.Empty,
                        Agent: GetString(args, "agent") ?? string.Empty,
                        SessionId: GetString(args, "session_id", "sessionId"),
                        Tool: GetString(args, "tool"),
                        Model: GetString(args, "model"),
                        State: GetString(args, "state"),
                        Activity: GetString(args, "activity"),
                        IssueNumber: GetOptionalInt32(args, "issue_number", "issueNumber", "issue"),
                        Branch: GetString(args, "branch"),
                        TtlMinutes: GetOptionalInt32(args, "ttl_minutes", "ttlMinutes")
                            ?? SwarmBusConstants.DefaultSessionHeartbeatTtlMinutes),
                    action,
                    ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "unregister":
            {
                var res = await store.UnregisterSessionAsync(
                    GetString(args, "agent") ?? string.Empty,
                    workspaceId,
                    ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            default:
                throw new ArgumentException($"Unsupported bus_sessions action '{action}'. Use: list, register, heartbeat, unregister.");
        }
    }

    private static async Task<(object Payload, bool IsError)> ExecuteProjectSyncAsync(
        SwarmBusStore store,
        JsonElement args,
        CancellationToken ct)
    {
        var action = (GetString(args, "action") ?? "get").Trim().ToLowerInvariant();
        var workspaceId = GetString(args, "workspace_id", "workspaceId");

        switch (action)
        {
            case "get":
            {
                var res = await store.GetProjectSyncAsync(
                    workspaceId,
                    GetString(args, "agent"),
                    ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "claim-turn":
            case "claim_turn":
            {
                var res = await store.ClaimProjectTurnAsync(
                    new ProjectTurnClaimRequest(
                        WorkspaceId: workspaceId ?? string.Empty,
                        Agent: GetString(args, "agent") ?? string.Empty,
                        ActiveGoal: GetString(args, "active_goal", "activeGoal"),
                        CurrentSlice: GetString(args, "current_slice", "currentSlice"),
                        Decision: GetString(args, "decision"),
                        TtlMinutes: GetOptionalInt32(args, "ttl_minutes", "ttlMinutes")
                            ?? SwarmBusConstants.DefaultLeadTurnTtlMinutes,
                        SessionId: GetString(args, "session_id", "sessionId"),
                        Tool: GetString(args, "tool"),
                        Model: GetString(args, "model"),
                        State: GetString(args, "state"),
                        Activity: GetString(args, "activity"),
                        IssueNumber: GetOptionalInt32(args, "issue_number", "issueNumber", "issue"),
                        Branch: GetString(args, "branch")),
                    ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "update":
            {
                IReadOnlyList<string>? decisions = TryGetProperty(args, out var decisionsElem, "decisions") &&
                                                   decisionsElem.ValueKind == JsonValueKind.Array
                    ? GetStringList(args, "decisions")
                    : null;

                var res = await store.UpdateProjectStateAsync(
                    new ProjectStateUpdateRequest(
                        WorkspaceId: workspaceId ?? string.Empty,
                        Agent: GetString(args, "agent") ?? string.Empty,
                        ActiveGoal: GetString(args, "active_goal", "activeGoal"),
                        CurrentSlice: GetString(args, "current_slice", "currentSlice"),
                        Decision: GetString(args, "decision"),
                        Decisions: decisions,
                        ReleaseLead: GetBool(args, false, "release_lead", "releaseLead")),
                    ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "list":
            {
                var res = await store.ListProjectStatesAsync(workspaceId, ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            default:
                throw new ArgumentException($"Unsupported bus_project_sync action '{action}'. Use: get, claim-turn, update, list.");
        }
    }

    private static async Task<(object Payload, bool IsError)> ExecuteIdeasAsync(
        SwarmBusStore store,
        JsonElement args,
        CancellationToken ct)
    {
        var action = (GetString(args, "action") ?? "list").Trim().ToLowerInvariant();
        var workspaceId = GetString(args, "workspace_id", "workspaceId");

        switch (action)
        {
            case "list":
            {
                var res = await store.ListIdeasAsync(workspaceId, GetString(args, "status"), ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "add":
            {
                var res = await store.AddIdeaAsync(
                    new ProjectIdeaAddRequest(
                        WorkspaceId: workspaceId ?? string.Empty,
                        Title: GetString(args, "title") ?? string.Empty,
                        CreatedBy: GetString(args, "agent", "created_by", "createdBy") ?? string.Empty,
                        Details: GetString(args, "details"),
                        Priority: GetOptionalInt32(args, "priority") ?? 2,
                        IssueNumber: GetOptionalInt32(args, "issue_number", "issueNumber", "issue")),
                    ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            case "update":
            {
                var id = GetOptionalInt64(args, "id") ?? 0;
                var res = await store.UpdateIdeaAsync(
                    new ProjectIdeaUpdateRequest(
                        Id: id,
                        WorkspaceId: workspaceId,
                        Title: GetString(args, "title"),
                        Details: GetString(args, "details"),
                        Priority: GetOptionalInt32(args, "priority"),
                        Status: GetString(args, "status"),
                        IssueNumber: GetOptionalInt32(args, "issue_number", "issueNumber", "issue")),
                    ct).ConfigureAwait(false);
                return (res, !res.Ok);
            }

            default:
                throw new ArgumentException($"Unsupported bus_ideas action '{action}'. Use: list, add, update.");
        }
    }

    private static bool TryGetProperty(JsonElement obj, out JsonElement value, params string[] names)
    {
        if (obj.ValueKind == JsonValueKind.Object)
        {
            foreach (var name in names)
            {
                if (obj.TryGetProperty(name, out value))
                {
                    return true;
                }
            }
        }

        value = default;
        return false;
    }

    private static string? GetString(JsonElement obj, params string[] names)
    {
        if (!TryGetProperty(obj, out var elem, names))
        {
            return null;
        }

        return elem.ValueKind switch
        {
            JsonValueKind.String => elem.GetString(),
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            _ => elem.ToString()
        };
    }

    private static string? GetScalarString(JsonElement obj, params string[] names)
    {
        if (!TryGetProperty(obj, out var elem, names) || elem.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return null;
        }

        return elem.ValueKind == JsonValueKind.String ? elem.GetString() : elem.GetRawText();
    }

    private static int? GetOptionalInt32(JsonElement obj, params string[] names)
    {
        if (!TryGetProperty(obj, out var elem, names) || elem.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return null;
        }

        if (elem.ValueKind == JsonValueKind.Number && elem.TryGetInt32(out var num))
        {
            return num;
        }

        if (elem.ValueKind == JsonValueKind.String)
        {
            var raw = elem.GetString();
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            if (int.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }
        }

        throw new ArgumentException($"Parameter '{names[0]}' must be an integer.");
    }

    private static long? GetOptionalInt64(JsonElement obj, params string[] names)
    {
        if (!TryGetProperty(obj, out var elem, names) || elem.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return null;
        }

        if (elem.ValueKind == JsonValueKind.Number && elem.TryGetInt64(out var num))
        {
            return num;
        }

        if (elem.ValueKind == JsonValueKind.String)
        {
            var raw = elem.GetString();
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            if (long.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }
        }

        throw new ArgumentException($"Parameter '{names[0]}' must be a 64-bit integer.");
    }

    private static bool GetBool(JsonElement obj, bool defaultValue, params string[] names)
    {
        if (!TryGetProperty(obj, out var elem, names) || elem.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return defaultValue;
        }

        return elem.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(elem.GetString()?.Trim(), out var parsed) => parsed,
            _ => defaultValue
        };
    }

    private static DateTimeOffset? GetOptionalUtc(JsonElement obj, params string[] names)
    {
        var raw = GetString(obj, names);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        if (DateTimeOffset.TryParse(
                raw.Trim(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var parsed))
        {
            return parsed;
        }

        throw new ArgumentException($"Parameter '{names[0]}' must be a valid ISO-8601 UTC timestamp.");
    }

    private static IReadOnlyList<string> GetStringList(JsonElement obj, params string[] names)
    {
        if (!TryGetProperty(obj, out var elem, names) || elem.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return [];
        }

        if (elem.ValueKind == JsonValueKind.Array)
        {
            var list = new List<string>();
            foreach (var item in elem.EnumerateArray())
            {
                var str = item.ValueKind == JsonValueKind.String ? item.GetString() : item.ToString();
                if (!string.IsNullOrWhiteSpace(str))
                {
                    list.Add(str.Trim());
                }
            }

            return list;
        }

        if (elem.ValueKind == JsonValueKind.String)
        {
            var raw = elem.GetString() ?? string.Empty;
            return raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        return [];
    }
}
