using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Xunit;

namespace AISwarmX.SwarmBus.Tests;

public sealed class McpServerProcessTests
{
    [Fact]
    public async Task StdioJsonRpcFullRoundtripWorksAgainstIsolatedV2Database()
    {
        using var fx = new TempBusFixture();
        var v2DbPath = Path.Combine(fx.RootPath, "swarm-bus-v2.db");

        using var process = StartMcpProcess(v2DbPath);
        var stderrTask = process.StandardError.ReadToEndAsync();

        try
        {
            // 1. initialize
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 1,
                method = "initialize",
                @params = new { protocolVersion = "2024-11-05", capabilities = new { } }
            });
            using (var initDoc = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                var result = initDoc.RootElement.GetProperty("result");
                Assert.Equal("2024-11-05", result.GetProperty("protocolVersion").GetString());
                Assert.Equal("aiswarmx-swarm-bus-mcp", result.GetProperty("serverInfo").GetProperty("name").GetString());
            }

            // 2. notifications/initialized (must not emit a response)
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                method = "notifications/initialized"
            });

            // 3. tools/list -> 12 tools
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 2,
                method = "tools/list"
            });
            using (var listDoc = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                var tools = listDoc.RootElement.GetProperty("result").GetProperty("tools");
                Assert.Equal(12, tools.GetArrayLength());
                var names = tools.EnumerateArray().Select(t => t.GetProperty("name").GetString()!).ToHashSet(StringComparer.Ordinal);
                Assert.Contains("bus_send", names);
                Assert.Contains("bus_inbox", names);
                Assert.Contains("bus_ack", names);
                Assert.Contains("bus_locks", names);
                Assert.Contains("bus_review_claim", names);
                Assert.Contains("bus_queue", names);
                Assert.Contains("bus_status", names);
                Assert.Contains("bus_wait", names);
                Assert.Contains("bus_start_work", names);
                Assert.Contains("bus_sessions", names);
                Assert.Contains("bus_project_sync", names);
                Assert.Contains("bus_ideas", names);
            }

            // 4. bus_start_work
            using (var startDoc = await CallToolAsync(process, 3, "bus_start_work", new
            {
                agent = "codex",
                paths = new[] { "src/Core" },
                pr = 56
            }))
            {
                Assert.True(startDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.Equal("src/Core", startDoc.RootElement.GetProperty("claimedPaths")[0].GetString());
            }

            // 5. bus_send
            long sentId;
            using (var sendDoc = await CallToolAsync(process, 4, "bus_send", new
            {
                from = "claude-opus",
                to = "codex",
                topic = "slice2-ready",
                body = "Check MCP stdio server"
            }))
            {
                Assert.True(sendDoc.RootElement.GetProperty("ok").GetBoolean());
                sentId = sendDoc.RootElement.GetProperty("id").GetInt64();
            }

            // 6. bus_wait
            using (var waitDoc = await CallToolAsync(process, 5, "bus_wait", new
            {
                agent = "codex",
                since_id = 0,
                timeout_ms = 2000
            }))
            {
                Assert.True(waitDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.False(waitDoc.RootElement.GetProperty("timedOut").GetBoolean());
                Assert.Equal(1, waitDoc.RootElement.GetProperty("messages").GetArrayLength());
                Assert.Equal(sentId, waitDoc.RootElement.GetProperty("messages")[0].GetProperty("id").GetInt64());
            }

            // 7. bus_ack
            using (var ackDoc = await CallToolAsync(process, 6, "bus_ack", new
            {
                message_id = sentId,
                agent = "codex",
                reply = "Acknowledged via MCP"
            }))
            {
                Assert.True(ackDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.Equal(sentId, ackDoc.RootElement.GetProperty("ackedMessageId").GetInt64());
                Assert.Equal("claude-opus", ackDoc.RootElement.GetProperty("replyMessage").GetProperty("to").GetString());
            }

            // 8. bus_queue lifecycle: upsert -> next -> resume -> guarded done -> resume (lease_lost)
            using (var upsertDoc = await CallToolAsync(process, 7, "bus_queue", new
            {
                action = "upsert",
                issue_number = 56,
                title = "AISwarmX #56 Slice 2",
                priority = 0,
                risk_class = "M"
            }))
            {
                Assert.True(upsertDoc.RootElement.GetProperty("ok").GetBoolean());
            }

            using (var nextDoc = await CallToolAsync(process, 8, "bus_queue", new
            {
                action = "next",
                agent = "codex"
            }))
            {
                Assert.True(nextDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.Equal(56, nextDoc.RootElement.GetProperty("item").GetProperty("issueNumber").GetInt32());
            }

            using (var resumeDoc = await CallToolAsync(process, 9, "bus_queue", new
            {
                action = "resume",
                issue_number = 56,
                owner = "codex"
            }))
            {
                Assert.True(resumeDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.True(resumeDoc.RootElement.GetProperty("resumed").GetBoolean());
            }

            using (var doneDoc = await CallToolAsync(process, 10, "bus_queue", new
            {
                action = "done",
                issue_number = 56,
                expected_status = "claimed",
                expected_owner = "codex"
            }))
            {
                Assert.True(doneDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.True(doneDoc.RootElement.GetProperty("changed").GetBoolean());
                Assert.Equal("done", doneDoc.RootElement.GetProperty("item").GetProperty("status").GetString());
            }

            using (var lostDoc = await CallToolAsync(process, 11, "bus_queue", new
            {
                action = "resume",
                issue_number = 56,
                owner = "codex"
            }, expectIsError: true))
            {
                Assert.False(lostDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.Equal("lease_lost", lostDoc.RootElement.GetProperty("reason").GetString());
            }

            // 9. bus_sessions, bus_project_sync, bus_ideas via MCP stdio
            using (var sessDoc = await CallToolAsync(process, 12, "bus_sessions", new
            {
                action = "register",
                agent = "codex",
                workspace_id = "mobaflow",
                session_id = "thread-mobaflow-1",
                tool = "codex-cli",
                model = "gpt-6.2-sol",
                state = "building",
                activity = "Implementing #68"
            }))
            {
                Assert.True(sessDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.Equal("thread-mobaflow-1", sessDoc.RootElement.GetProperty("session").GetProperty("sessionId").GetString());
                Assert.Equal("building", sessDoc.RootElement.GetProperty("session").GetProperty("effectiveState").GetString());
            }

            using (var ideaAddDoc = await CallToolAsync(process, 13, "bus_ideas", new
            {
                action = "add",
                workspace_id = "mobaflow",
                title = "Add virtual master dashboard card",
                priority = 1,
                agent = "claude-opus"
            }))
            {
                Assert.True(ideaAddDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.Equal("queued", ideaAddDoc.RootElement.GetProperty("idea").GetProperty("status").GetString());
            }

            using (var syncDoc = await CallToolAsync(process, 14, "bus_project_sync", new
            {
                action = "claim-turn",
                workspace_id = "mobaflow",
                agent = "claude-opus",
                active_goal = "Issue #68 Slice 1",
                current_slice = "SwarmBus v2.1",
                decision = "Use SQLite-backed session registry"
            }))
            {
                Assert.True(syncDoc.RootElement.GetProperty("ok").GetBoolean());
                Assert.Equal("claude-opus", syncDoc.RootElement.GetProperty("projectState").GetProperty("leadAgent").GetString());
                Assert.Single(syncDoc.RootElement.GetProperty("sessions").EnumerateArray());
                Assert.Single(syncDoc.RootElement.GetProperty("queuedIdeas").EnumerateArray());
            }

            process.StandardInput.Close();
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, process.ExitCode);

            var stderr = await stderrTask;
            Assert.Equal(string.Empty, stderr.Trim());
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
    }

    [Fact]
    public async Task StdioServerHandlesConcurrentPingCancellationAndProtocolValidation()
    {
        using var fx = new TempBusFixture();
        var v2DbPath = Path.Combine(fx.RootPath, "swarm-bus-v2.db");

        using var process = StartMcpProcess(v2DbPath);
        var stderrTask = process.StandardError.ReadToEndAsync();

        try
        {
            // 1. initialize with unsupported protocolVersion falls back to 2024-11-05.
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 1,
                method = "initialize",
                @params = new { protocolVersion = "2099-01-01", capabilities = new { } }
            });
            using (var initUnsupported = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                Assert.Equal("2024-11-05", initUnsupported.RootElement.GetProperty("result").GetProperty("protocolVersion").GetString());
            }

            // 2. initialize with supported 2024-11-05 negotiates 2024-11-05.
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 2,
                method = "initialize",
                @params = new { protocolVersion = "2024-11-05", capabilities = new { } }
            });
            using (var initSupported = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                Assert.Equal("2024-11-05", initSupported.RootElement.GetProperty("result").GetProperty("protocolVersion").GetString());
            }

            // 3. Invalid jsonrpc version -> -32600 Invalid Request preserving valid id.
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "1.0",
                id = 3,
                method = "ping"
            });
            using (var badVersion = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                Assert.Equal(3, badVersion.RootElement.GetProperty("id").GetInt32());
                Assert.Equal(-32600, badVersion.RootElement.GetProperty("error").GetProperty("code").GetInt32());
            }

            // 4. Invalid id type (boolean) -> -32600 Invalid Request with id: null.
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = true,
                method = "ping"
            });
            using (var badId = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                Assert.Equal(JsonValueKind.Null, badId.RootElement.GetProperty("id").ValueKind);
                Assert.Equal(-32600, badId.RootElement.GetProperty("error").GetProperty("code").GetInt32());
            }

            // 5. Unknown tool in tools/call -> -32602 Invalid params.
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 4,
                method = "tools/call",
                @params = new { name = "nonexistent_tool", arguments = new { } }
            });
            using (var unknownTool = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                Assert.Equal(4, unknownTool.RootElement.GetProperty("id").GetInt32());
                Assert.Equal(-32602, unknownTool.RootElement.GetProperty("error").GetProperty("code").GetInt32());
            }

            // 6. Saturate all 4 wait slots (15s): 5th bus_wait immediately returns -32001, ping STILL succeeds unblocked,
            //    and notifications/cancelled + stdin EOF exit promptly without waiting 15s.
            for (var waitId = 100; waitId < 104; waitId++)
            {
                await SendJsonRpcAsync(process.StandardInput, new
                {
                    jsonrpc = "2.0",
                    id = waitId,
                    method = "tools/call",
                    @params = new
                    {
                        name = "bus_wait",
                        arguments = new { agent = "codex", since_id = 0, timeout_ms = 15000 }
                    }
                });
            }

            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 104,
                method = "tools/call",
                @params = new
                {
                    name = "bus_wait",
                    arguments = new { agent = "codex", since_id = 0, timeout_ms = 15000 }
                }
            });
            using (var overflowResponse = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                Assert.Equal(104, overflowResponse.RootElement.GetProperty("id").GetInt32());
                Assert.Equal(-32001, overflowResponse.RootElement.GetProperty("error").GetProperty("code").GetInt32());
            }

            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 105,
                method = "ping"
            });

            using (var pingResponse = await ReadJsonRpcResponseAsync(process.StandardOutput))
            {
                Assert.Equal(105, pingResponse.RootElement.GetProperty("id").GetInt32());
                Assert.True(pingResponse.RootElement.TryGetProperty("result", out _));
            }

            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                method = "notifications/cancelled",
                @params = new { requestId = 100, reason = "test cancel" }
            });

            process.StandardInput.Close();
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, process.ExitCode);

            var trailing = await process.StandardOutput.ReadToEndAsync();
            Assert.Equal(string.Empty, trailing.Trim());

            var stderr = await stderrTask;
            Assert.Equal(string.Empty, stderr.Trim());
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
    }

    [Fact]
    public async Task OneShotPipeClosingStdinImmediatelyAfterFastRequestStillDeliversReplyAndPersistsState()
    {
        using var fx = new TempBusFixture();
        var v2DbPath = Path.Combine(fx.RootPath, "swarm-bus-v2.db");

        using var process = StartMcpProcess(v2DbPath);
        var stderrTask = process.StandardError.ReadToEndAsync();

        try
        {
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 77,
                method = "tools/call",
                @params = new
                {
                    name = "bus_send",
                    arguments = new
                    {
                        from = "codex",
                        to = "claude-opus",
                        topic = "one-shot-pipe",
                        body = "Persisted despite immediate stdin EOF"
                    }
                }
            });

            // Close stdin immediately before reading the reply to exercise one-shot piping (`echo ... | mcp`).
            process.StandardInput.Close();

            using var sendDoc = await ReadJsonRpcResponseAsync(process.StandardOutput);
            Assert.Equal(77, sendDoc.RootElement.GetProperty("id").GetInt32());
            var result = sendDoc.RootElement.GetProperty("result");
            Assert.False(result.GetProperty("isError").GetBoolean());
            var payloadText = result.GetProperty("content")[0].GetProperty("text").GetString()!;
            using var payloadDoc = JsonDocument.Parse(payloadText);
            Assert.True(payloadDoc.RootElement.GetProperty("ok").GetBoolean());

            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, process.ExitCode);

            var stderr = await stderrTask;
            Assert.Equal(string.Empty, stderr.Trim());

            var verifyStore = new AISwarmX.SwarmBus.Core.Store.SwarmBusStore(v2DbPath);
            var inbox = verifyStore.GetInbox("claude-opus", unreadOnly: true, limit: 10);
            var msg = Assert.Single(inbox.Messages);
            Assert.Equal("one-shot-pipe", msg.Topic);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
    }

    [Fact]
    public async Task ToolSideIoExceptionReturnsToolErrorEnvelopeOnConnectedStdout()
    {
        using var fx = new TempBusFixture();
        // Blocking regular file used as parent directory triggers IOException inside EnsureDirectoryExists() during bus_status.
        var fileAsParent = Path.Combine(fx.RootPath, "file-parent");
        File.WriteAllText(fileAsParent, "not-a-directory");
        var unreachableDbPath = Path.Combine(fileAsParent, "swarm-bus-v2.db");

        using var process = StartMcpProcess(unreachableDbPath);
        var stderrTask = process.StandardError.ReadToEndAsync();

        try
        {
            await SendJsonRpcAsync(process.StandardInput, new
            {
                jsonrpc = "2.0",
                id = 88,
                method = "tools/call",
                @params = new
                {
                    name = "bus_status",
                    arguments = new { }
                }
            });

            using var errDoc = await ReadJsonRpcResponseAsync(process.StandardOutput);
            Assert.Equal(88, errDoc.RootElement.GetProperty("id").GetInt32());
            var result = errDoc.RootElement.GetProperty("result");
            Assert.True(result.GetProperty("isError").GetBoolean());
            var payloadText = result.GetProperty("content")[0].GetProperty("text").GetString()!;
            using var payloadDoc = JsonDocument.Parse(payloadText);
            Assert.False(payloadDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.False(string.IsNullOrWhiteSpace(payloadDoc.RootElement.GetProperty("error").GetString()));

            process.StandardInput.Close();
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, process.ExitCode);

            var stderr = await stderrTask;
            Assert.Contains("tools/call error", stderr);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
    }

    [Fact]
    public async Task CliModeExecutesStartWorkStatusQueueResumeAndReturnsExitCode2OnGuardMismatch()
    {
        using var fx = new TempBusFixture();
        var v2DbPath = Path.Combine(fx.RootPath, "swarm-bus-v2.db");

        var startOut = await RunCliAsync(v2DbPath, "--cli", "start-work", "--agent", "codex", "--paths", "src/Mcp,src/Core", "--pr", "56");
        Assert.Equal(0, startOut.ExitCode);
        using (var startDoc = JsonDocument.Parse(startOut.StdOut))
        {
            Assert.True(startDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal(2, startDoc.RootElement.GetProperty("claimedPaths").GetArrayLength());
        }

        var statusOut = await RunCliAsync(v2DbPath, "--cli", "status", "--limit", "5");
        Assert.Equal(0, statusOut.ExitCode);
        using (var statusDoc = JsonDocument.Parse(statusOut.StdOut))
        {
            Assert.Equal(2, statusDoc.RootElement.GetProperty("activeLocks").GetArrayLength());
        }

        var upsertOut = await RunCliAsync(v2DbPath, "--cli", "queue", "--action", "upsert", "--issue-number", "56", "--title", "Slice 2", "--priority", "0", "--risk-class", "M");
        Assert.Equal(0, upsertOut.ExitCode);
        var nextOut = await RunCliAsync(v2DbPath, "--cli", "queue", "--action", "next", "--agent", "codex");
        Assert.Equal(0, nextOut.ExitCode);

        // Watchdog exact CLI resume syntax: --cli queue --action resume --issue-number N --owner ...
        var resumeOk = await RunCliAsync(v2DbPath, "--cli", "queue", "--action", "resume", "--issue-number", "56", "--owner", "codex");
        Assert.Equal(0, resumeOk.ExitCode);
        using (var resumeDoc = JsonDocument.Parse(resumeOk.StdOut))
        {
            Assert.True(resumeDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.True(resumeDoc.RootElement.GetProperty("resumed").GetBoolean());
            Assert.Equal(56, resumeDoc.RootElement.GetProperty("item").GetProperty("issueNumber").GetInt32());
        }

        // Guard/lease mismatch writes JSON to stdout and returns exit code 2.
        var resumeLost = await RunCliAsync(v2DbPath, "--cli", "queue", "--action", "resume", "--issue-number", "56", "--owner", "wrong-owner");
        Assert.Equal(2, resumeLost.ExitCode);
        using (var lostDoc = JsonDocument.Parse(resumeLost.StdOut))
        {
            Assert.False(lostDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal("lease_lost", lostDoc.RootElement.GetProperty("reason").GetString());
        }

        var invalidOut = await RunCliAsync(v2DbPath, "--cli", "nonexistent-cmd");
        Assert.Equal(1, invalidOut.ExitCode);
        Assert.Contains("Unknown CLI command", invalidOut.StdErr);
    }

    [Fact]
    public async Task CliVirtualMasterSessionsProjectSyncAndIdeasRoundtripWorks()
    {
        using var fx = new TempBusFixture();
        var v2DbPath = Path.Combine(fx.RootPath, "swarm-bus-v2.db");

        var regOut = await RunCliAsync(
            v2DbPath,
            "--cli", "sessions",
            "--action", "register",
            "--workspace-id", "sentix",
            "--agent", "codex",
            "--session-id", "codex-sentix-99",
            "--tool", "codex-cli",
            "--model", "gpt-6.2-sol",
            "--state", "planning",
            "--activity", "Reviewing Slice 1");
        Assert.Equal(0, regOut.ExitCode);
        using (var regDoc = JsonDocument.Parse(regOut.StdOut))
        {
            Assert.True(regDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal("sentix", regDoc.RootElement.GetProperty("workspaceId").GetString());
            Assert.Equal("codex-sentix-99", regDoc.RootElement.GetProperty("session").GetProperty("sessionId").GetString());
        }

        var ideaAddOut = await RunCliAsync(
            v2DbPath,
            "--cli", "ideas",
            "--action", "add",
            "--workspace-id", "sentix",
            "--title", "Support 3x3 wake routing",
            "--details", "Resolve per-workspace Codex thread",
            "--priority", "0",
            "--agent", "antigravity-gemini");
        Assert.Equal(0, ideaAddOut.ExitCode);
        long ideaId;
        using (var ideaAddDoc = JsonDocument.Parse(ideaAddOut.StdOut))
        {
            Assert.True(ideaAddDoc.RootElement.GetProperty("ok").GetBoolean());
            ideaId = ideaAddDoc.RootElement.GetProperty("idea").GetProperty("id").GetInt64();
        }

        var ideaUpdateOut = await RunCliAsync(
            v2DbPath,
            "--cli", "ideas",
            "--action", "update",
            "--id", ideaId.ToString(),
            "--workspace-id", "sentix",
            "--status", "promoted",
            "--issue", "68");
        Assert.Equal(0, ideaUpdateOut.ExitCode);
        using (var ideaUpdateDoc = JsonDocument.Parse(ideaUpdateOut.StdOut))
        {
            Assert.True(ideaUpdateDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal("promoted", ideaUpdateDoc.RootElement.GetProperty("idea").GetProperty("status").GetString());
            Assert.Equal(68, ideaUpdateDoc.RootElement.GetProperty("idea").GetProperty("issueNumber").GetInt32());
        }

        var wrongWsOut = await RunCliAsync(
            v2DbPath,
            "--cli", "ideas",
            "--action", "update",
            "--id", ideaId.ToString(),
            "--workspace-id", "mobaflow",
            "--status", "done");
        Assert.Equal(2, wrongWsOut.ExitCode);
        using (var wrongWsDoc = JsonDocument.Parse(wrongWsOut.StdOut))
        {
            Assert.False(wrongWsDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal("workspace_mismatch", wrongWsDoc.RootElement.GetProperty("reason").GetString());
        }

        var claimOut = await RunCliAsync(
            v2DbPath,
            "--cli", "project-sync",
            "--action", "claim-turn",
            "--workspace-id", "sentix",
            "--agent", "claude-opus",
            "--active-goal", "Sentix SwarmBus rollout",
            "--current-slice", "Slice 1",
            "--decision", "Use agent_sessions for 3x3 routing");
        Assert.Equal(0, claimOut.ExitCode);
        using (var claimDoc = JsonDocument.Parse(claimOut.StdOut))
        {
            Assert.True(claimDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.Equal("claude-opus", claimDoc.RootElement.GetProperty("projectState").GetProperty("leadAgent").GetString());
            Assert.True(claimDoc.RootElement.GetProperty("projectState").GetProperty("isLeadActive").GetBoolean());
        }

        var updateOut = await RunCliAsync(
            v2DbPath,
            "--cli", "project_sync",
            "--action", "update",
            "--workspace-id", "sentix",
            "--agent", "claude-opus",
            "--release-lead", "true");
        Assert.Equal(0, updateOut.ExitCode);
        using (var updateDoc = JsonDocument.Parse(updateOut.StdOut))
        {
            Assert.True(updateDoc.RootElement.GetProperty("ok").GetBoolean());
            Assert.False(updateDoc.RootElement.GetProperty("projectState").GetProperty("isLeadActive").GetBoolean());
            Assert.Equal("claude-opus", updateDoc.RootElement.GetProperty("previousLeadAgent").GetString());
        }
    }

    [Fact]
    public async Task CliImportLegacyImportsMessagesIntoV2DatabaseZeroDataLoss()
    {
        using var fx = new TempBusFixture();
        var legacyDbPath = Path.Combine(fx.RootPath, "legacy-swarm-bus.db");
        var v2DbPath = Path.Combine(fx.RootPath, "swarm-bus-v2.db");

        TempBusFixture.ExecuteRawSql(legacyDbPath, """
            CREATE TABLE messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at_utc TEXT NOT NULL,
              sender TEXT NOT NULL,
              recipient TEXT NOT NULL,
              topic TEXT NOT NULL,
              body TEXT NOT NULL,
              pr_number INTEGER,
              issue_number INTEGER,
              priority TEXT NOT NULL DEFAULT 'normal',
              reply_to_id INTEGER,
              status TEXT NOT NULL DEFAULT 'unread',
              acked_by TEXT,
              acked_at_utc TEXT
            );
            CREATE TABLE locks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              agent TEXT NOT NULL,
              path_pattern TEXT NOT NULL UNIQUE,
              pr_number INTEGER,
              claimed_at_utc TEXT NOT NULL,
              expires_at_utc TEXT NOT NULL
            );
            INSERT INTO messages (created_at_utc, sender, recipient, topic, body, status)
            VALUES ('2026-11-28T05:00:00.000Z', 'antigravity-gemini', 'codex', 'legacy-handoff', 'Migrated message', 'acked');
            """);

        var importOut = await RunCliAsync(v2DbPath, "--cli", "import-legacy", "--legacy-db", legacyDbPath);
        Assert.Equal(0, importOut.ExitCode);
        using var importDoc = JsonDocument.Parse(importOut.StdOut);
        Assert.True(importDoc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal(1, importDoc.RootElement.GetProperty("result").GetProperty("messagesImported").GetInt32());

        var statusOut = await RunCliAsync(v2DbPath, "--cli", "status", "--workspace-id", "*");
        Assert.Equal(0, statusOut.ExitCode);
        using var statusDoc = JsonDocument.Parse(statusOut.StdOut);
        Assert.Single(statusDoc.RootElement.GetProperty("recentMessages").EnumerateArray());
    }

    private static Process StartMcpProcess(string v2DbPath, params string[] args)
    {
        var startInfo = CreateMcpStartInfo(v2DbPath, args);
        return Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start AISwarmX.SwarmBus.Mcp process.");
    }

    private static async Task<(int ExitCode, string StdOut, string StdErr)> RunCliAsync(string v2DbPath, params string[] args)
    {
        using var proc = StartMcpProcess(v2DbPath, args);
        try
        {
            var stdoutTask = proc.StandardOutput.ReadToEndAsync();
            var stderrTask = proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            return (proc.ExitCode, await stdoutTask, await stderrTask);
        }
        finally
        {
            if (!proc.HasExited)
            {
                proc.Kill(entireProcessTree: true);
            }
        }
    }

    private static ProcessStartInfo CreateMcpStartInfo(string v2DbPath, params string[] args)
    {
        var mcpDll = Path.Combine(AppContext.BaseDirectory, "AISwarmX.SwarmBus.Mcp.dll");
        if (!File.Exists(mcpDll))
        {
            throw new FileNotFoundException($"Compiled MCP assembly not found at '{mcpDll}'.", mcpDll);
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardInputEncoding = new UTF8Encoding(false),
            StandardOutputEncoding = new UTF8Encoding(false),
            StandardErrorEncoding = new UTF8Encoding(false),
            UseShellExecute = false,
            CreateNoWindow = true
        };

        startInfo.ArgumentList.Add(mcpDll);
        foreach (var arg in args)
        {
            startInfo.ArgumentList.Add(arg);
        }

        startInfo.Environment["MOBADEV_SWARM_BUS_DB"] = "";
        startInfo.Environment["MOBADEV_SWARM_BUS_V2_DB"] = v2DbPath;
        startInfo.Environment["MOBADEV_CODEX_APP_SERVER_URL"] = "";
        return startInfo;
    }

    private static async Task SendJsonRpcAsync(StreamWriter stdin, object message)
    {
        var json = JsonSerializer.Serialize(message);
        await stdin.WriteLineAsync(json);
        await stdin.FlushAsync();
    }

    private static async Task<JsonDocument> ReadJsonRpcResponseAsync(StreamReader stdout)
    {
        var line = await stdout.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(string.IsNullOrWhiteSpace(line), "Expected JSON-RPC response line on stdout.");
        return JsonDocument.Parse(line!);
    }

    private static async Task<JsonDocument> CallToolAsync(
        Process process,
        int id,
        string name,
        object arguments,
        bool expectIsError = false)
    {
        await SendJsonRpcAsync(process.StandardInput, new
        {
            jsonrpc = "2.0",
            id,
            method = "tools/call",
            @params = new { name, arguments }
        });

        using var rpcDoc = await ReadJsonRpcResponseAsync(process.StandardOutput);
        Assert.Equal(id, rpcDoc.RootElement.GetProperty("id").GetInt32());
        var result = rpcDoc.RootElement.GetProperty("result");
        Assert.Equal(expectIsError, result.GetProperty("isError").GetBoolean());
        var text = result.GetProperty("content")[0].GetProperty("text").GetString()!;
        return JsonDocument.Parse(text);
    }
}
