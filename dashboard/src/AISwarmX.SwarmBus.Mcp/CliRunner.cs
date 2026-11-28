using System.Text.Json;
using System.Text.Json.Nodes;
using AISwarmX.SwarmBus.Core.Store;

namespace AISwarmX.SwarmBus.Mcp;

/// <summary>
/// Zero-token CLI runner invoked via <c>--cli &lt;subcommand&gt; [flags]</c>.
/// </summary>
internal static class CliRunner
{
    public static async Task<int> RunAsync(
        SwarmBusStore store,
        string[] cliArgs,
        TextWriter output,
        TextWriter errorOutput,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(cliArgs);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(errorOutput);

        try
        {
            var subcommand = (cliArgs.Length > 0 ? cliArgs[0] : "status").Trim().ToLowerInvariant();
            var flags = ParseFlags(cliArgs.Length > 1 ? cliArgs[1..] : []);
            if (subcommand is "import-legacy" or "import_legacy")
            {
                var legacyDb = GetSingle(flags, "legacy-db", "legacy_db") ?? SwarmBusStore.ResolveLegacyDefaultDbPath();
                var importRes = store.ImportFromLegacyDb(legacyDb);
                var importJson = JsonSerializer.Serialize(
                    new { ok = true, legacyDb, targetDb = store.DatabasePath, result = importRes },
                    SwarmBusToolDispatcher.IndentedJsonOptions);
                await output.WriteLineAsync(importJson.AsMemory(), cancellationToken).ConfigureAwait(false);
                await output.FlushAsync(cancellationToken).ConfigureAwait(false);
                return 0;
            }

            var (toolName, argsObject) = MapSubcommandToTool(subcommand, flags);

            using var doc = JsonDocument.Parse(argsObject.ToJsonString());
            var (payload, isError) = await SwarmBusToolDispatcher
                .ExecuteAsync(store, toolName, doc.RootElement, cancellationToken)
                .ConfigureAwait(false);

            var json = JsonSerializer.Serialize(payload, SwarmBusToolDispatcher.IndentedJsonOptions);
            await output.WriteLineAsync(json.AsMemory(), cancellationToken).ConfigureAwait(false);
            await output.FlushAsync(cancellationToken).ConfigureAwait(false);
            return isError ? 2 : 0;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var errJson = JsonSerializer.Serialize(
                new { ok = false, error = ex.Message },
                SwarmBusToolDispatcher.CompactJsonOptions);
            await errorOutput.WriteLineAsync(errJson.AsMemory(), cancellationToken).ConfigureAwait(false);
            await errorOutput.FlushAsync(cancellationToken).ConfigureAwait(false);
            return 1;
        }
    }

    private static (string ToolName, JsonObject Args) MapSubcommandToTool(
        string subcommand,
        Dictionary<string, List<string>> flags)
    {
        var args = new JsonObject();

        switch (subcommand)
        {
            case "send":
                CopyOptionalString(flags, args, "from", "from");
                CopyOptionalString(flags, args, "to", "to");
                CopyOptionalString(flags, args, "topic", "topic");
                CopyOptionalString(flags, args, "body", "body");
                CopyOptionalString(flags, args, "pr", "pr");
                CopyOptionalString(flags, args, "issue", "issue");
                CopyOptionalString(flags, args, "priority", "priority");
                CopyOptionalString(flags, args, "reply-to", "reply_to");
                CopyOptionalString(flags, args, "state", "state");
                CopyOptionalBool(flags, args, "wake", "wake");
                CopyOptionalString(flags, args, "codex-thread-id", "codex_thread_id");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_send", args);

            case "inbox":
                CopyOptionalString(flags, args, "agent", "agent");
                if (HasFlag(flags, "all"))
                {
                    args["unread_only"] = false;
                }
                else
                {
                    CopyOptionalBool(flags, args, "unread-only", "unread_only");
                }

                CopyOptionalString(flags, args, "limit", "limit");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_inbox", args);

            case "ack":
                CopyOptionalString(flags, args, "message-id", "message_id", "id");
                CopyOptionalString(flags, args, "agent", "agent");
                CopyOptionalString(flags, args, "reply", "reply");
                return ("bus_ack", args);

            case "locks":
                args["action"] = GetSingle(flags, "action") ?? "list";
                CopyOptionalString(flags, args, "agent", "agent");
                CopyPathsArray(flags, args);
                CopyOptionalString(flags, args, "pr", "pr", "pr-number");
                CopyOptionalString(flags, args, "ttl-minutes", "ttl_minutes");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_locks", args);

            case "review-claim":
            case "review_claim":
                args["action"] = GetSingle(flags, "action") ?? "claim";
                CopyOptionalString(flags, args, "agent", "agent");
                CopyOptionalString(flags, args, "pr-number", "pr_number", "pr");
                CopyOptionalString(flags, args, "head-sha", "head_sha", "head", "sha");
                CopyOptionalString(flags, args, "role", "role");
                CopyOptionalString(flags, args, "ttl-minutes", "ttl_minutes");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_review_claim", args);

            case "queue":
                args["action"] = GetSingle(flags, "action") ?? "list";
                CopyOptionalString(flags, args, "agent", "agent");
                CopyOptionalString(flags, args, "owner", "owner");
                CopyOptionalString(flags, args, "issue-number", "issue_number", "issue");
                CopyOptionalString(flags, args, "title", "title");
                CopyOptionalString(flags, args, "priority", "priority");
                CopyOptionalString(flags, args, "risk-class", "risk_class");
                CopyOptionalString(flags, args, "status", "status");
                CopyOptionalString(flags, args, "expected-status", "expected_status");
                CopyOptionalString(flags, args, "expected-owner", "expected_owner");
                CopyOptionalString(flags, args, "claimed-before-utc", "claimed_before_utc");
                CopyOptionalString(flags, args, "meta-key", "meta_key");
                CopyOptionalString(flags, args, "meta-value", "meta_value");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_queue", args);

            case "status":
                CopyOptionalString(flags, args, "limit", "limit");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_status", args);

            case "wait":
                CopyOptionalString(flags, args, "agent", "agent");
                CopyOptionalString(flags, args, "since-id", "since_id");
                CopyOptionalBool(flags, args, "mentions-only", "mentions_only");
                CopyOptionalString(flags, args, "timeout-ms", "timeout_ms");
                CopyOptionalString(flags, args, "limit", "limit");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_wait", args);

            case "start-work":
            case "start_work":
                CopyOptionalString(flags, args, "agent", "agent");
                CopyPathsArray(flags, args);
                CopyOptionalString(flags, args, "pr", "pr", "pr-number");
                CopyOptionalString(flags, args, "ttl-minutes", "ttl_minutes");
                CopyOptionalString(flags, args, "message-limit", "message_limit");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                return ("bus_start_work", args);

            case "sessions":
                args["action"] = GetSingle(flags, "action") ?? "list";
                CopyOptionalString(flags, args, "agent", "agent");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                CopyOptionalString(flags, args, "session-id", "session_id");
                CopyOptionalString(flags, args, "tool", "tool");
                CopyOptionalString(flags, args, "model", "model");
                CopyOptionalString(flags, args, "state", "state");
                CopyOptionalString(flags, args, "activity", "activity");
                CopyOptionalString(flags, args, "issue-number", "issue_number", "issue");
                CopyOptionalString(flags, args, "branch", "branch");
                CopyOptionalString(flags, args, "ttl-minutes", "ttl_minutes");
                return ("bus_sessions", args);

            case "project-sync":
            case "project_sync":
                args["action"] = GetSingle(flags, "action") ?? "get";
                CopyOptionalString(flags, args, "agent", "agent");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                CopyOptionalString(flags, args, "active-goal", "active_goal");
                CopyOptionalString(flags, args, "current-slice", "current_slice");
                CopyOptionalString(flags, args, "decision", "decision");
                CopyDecisionsArray(flags, args);
                CopyOptionalBool(flags, args, "release-lead", "release_lead");
                CopyOptionalString(flags, args, "ttl-minutes", "ttl_minutes");
                CopyOptionalString(flags, args, "session-id", "session_id");
                CopyOptionalString(flags, args, "tool", "tool");
                CopyOptionalString(flags, args, "model", "model");
                CopyOptionalString(flags, args, "state", "state");
                CopyOptionalString(flags, args, "activity", "activity");
                CopyOptionalString(flags, args, "issue-number", "issue_number", "issue");
                CopyOptionalString(flags, args, "branch", "branch");
                return ("bus_project_sync", args);

            case "ideas":
                args["action"] = GetSingle(flags, "action") ?? "list";
                CopyOptionalString(flags, args, "id", "id");
                CopyOptionalString(flags, args, "workspace-id", "workspace_id");
                CopyOptionalString(flags, args, "title", "title");
                CopyOptionalString(flags, args, "details", "details");
                CopyOptionalString(flags, args, "priority", "priority");
                CopyOptionalString(flags, args, "status", "status");
                CopyOptionalString(flags, args, "issue-number", "issue_number", "issue");
                CopyOptionalString(flags, args, "agent", "agent", "created-by", "created_by");
                return ("bus_ideas", args);

            default:
                throw new ArgumentException(
                    $"Unknown CLI command '{subcommand}'. Expected: send, inbox, ack, locks, review-claim, queue, status, wait, start-work, sessions, project-sync, ideas, import-legacy.");
        }
    }

    private static Dictionary<string, List<string>> ParseFlags(string[] tokens)
    {
        var flags = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < tokens.Length; i++)
        {
            var token = tokens[i];
            if (!token.StartsWith("--", StringComparison.Ordinal) || token.Length <= 2)
            {
                throw new ArgumentException($"Unexpected positional CLI argument '{token}'. Flags must start with '--'.");
            }

            var eqIndex = token.IndexOf('=', StringComparison.Ordinal);
            string key;
            string value;
            if (eqIndex > 2)
            {
                key = token[2..eqIndex];
                value = token[(eqIndex + 1)..];
            }
            else
            {
                key = token[2..];
                if (i + 1 < tokens.Length && !tokens[i + 1].StartsWith("--", StringComparison.Ordinal))
                {
                    value = tokens[++i];
                }
                else
                {
                    value = "true";
                }
            }

            if (!flags.TryGetValue(key, out var list))
            {
                list = [];
                flags[key] = list;
            }

            list.Add(value);
        }

        return flags;
    }

    private static string? GetSingle(Dictionary<string, List<string>> flags, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (flags.TryGetValue(key, out var list) && list.Count > 0)
            {
                return list[^1];
            }
        }

        return null;
    }

    private static bool HasFlag(Dictionary<string, List<string>> flags, string key) =>
        flags.TryGetValue(key, out var list) &&
        list.Count > 0 &&
        !string.Equals(list[^1], "false", StringComparison.OrdinalIgnoreCase);

    private static void CopyOptionalString(
        Dictionary<string, List<string>> flags,
        JsonObject target,
        string primaryKey,
        string targetProp,
        params string[] aliases)
    {
        var keys = new string[aliases.Length + 1];
        keys[0] = primaryKey;
        Array.Copy(aliases, 0, keys, 1, aliases.Length);

        var value = GetSingle(flags, keys);
        if (value is not null)
        {
            target[targetProp] = value;
        }
    }

    private static void CopyOptionalBool(
        Dictionary<string, List<string>> flags,
        JsonObject target,
        string flagKey,
        string targetProp)
    {
        var value = GetSingle(flags, flagKey);
        if (value is not null && bool.TryParse(value.Trim(), out var parsed))
        {
            target[targetProp] = parsed;
        }
    }

    private static void CopyPathsArray(Dictionary<string, List<string>> flags, JsonObject target)
    {
        if (!flags.TryGetValue("paths", out var rawList) || rawList.Count == 0)
        {
            return;
        }

        var arr = new JsonArray();
        foreach (var raw in rawList)
        {
            foreach (var part in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                arr.Add(part);
            }
        }

        target["paths"] = arr;
    }

    private static void CopyDecisionsArray(Dictionary<string, List<string>> flags, JsonObject target)
    {
        if (!flags.TryGetValue("decisions", out var rawList) || rawList.Count == 0)
        {
            return;
        }

        var arr = new JsonArray();
        foreach (var raw in rawList)
        {
            var trimmed = raw.Trim();
            if (trimmed.StartsWith('[') && trimmed.EndsWith(']'))
            {
                try
                {
                    using var doc = JsonDocument.Parse(trimmed);
                    if (doc.RootElement.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var elem in doc.RootElement.EnumerateArray())
                        {
                            var text = elem.ValueKind == JsonValueKind.String ? elem.GetString() : elem.ToString();
                            if (!string.IsNullOrWhiteSpace(text))
                            {
                                arr.Add(text.Trim());
                            }
                        }

                        continue;
                    }
                }
                catch (JsonException)
                {
                    // Fall back to literal string entry.
                }
            }

            if (!string.IsNullOrWhiteSpace(trimmed))
            {
                arr.Add(trimmed);
            }
        }

        target["decisions"] = arr;
    }
}
