using AISwarmX.SwarmBus.Core.Contracts;
using AISwarmX.SwarmBus.Core.Store;
using AISwarmX.SwarmBus.Core.Wake;
using Xunit;

namespace AISwarmX.SwarmBus.Tests;

public sealed class VirtualMasterTests
{
    [Fact]
    public async Task WorkspaceScopedCodexWakeResolvesRegisteredThreadAndAllowsExplicitOverrideAndFallback()
    {
        using var fx = new TempBusFixture();
        var clock = new TestTimeProvider(new DateTimeOffset(2026, 11, 28, 10, 0, 0, TimeSpan.Zero));
        var dispatched = new List<SwarmWakeContext>();

        var store = fx.CreateStore(new SwarmBusStoreOptions
        {
            TimeProvider = clock,
            WakeDispatcher = (ctx, _) =>
            {
                dispatched.Add(ctx);
                return Task.FromResult(new SwarmWakeResult(
                    Attempted: true,
                    Ok: true,
                    Target: ctx.Recipient,
                    ThreadId: ctx.CodexThreadId ?? "fallback-env-thread",
                    Reason: "steer"));
            }
        });

        await store.UpsertSessionAsync(new AgentSessionUpsertRequest(
            WorkspaceId: "mobaflow",
            Agent: "codex",
            SessionId: "thread-mobaflow",
            Tool: "codex-cli",
            Model: "gpt-6.2-sol",
            State: "idle"));

        await store.UpsertSessionAsync(new AgentSessionUpsertRequest(
            WorkspaceId: "sentix",
            Agent: "codex",
            SessionId: "thread-sentix",
            Tool: "codex-cli",
            Model: "gpt-6.2-sol",
            State: "building",
            Activity: "Editing Sentix"));

        await store.UpsertSessionAsync(new AgentSessionUpsertRequest(
            WorkspaceId: "aiswarmx",
            Agent: "codex",
            SessionId: "thread-aiswarmx",
            Tool: "codex-cli",
            Model: "gpt-6.2-sol",
            State: "testing"));

        // 1. Registered workspace wakes its own thread (sync + async)
        var sentixWake = store.SendMessage(new SwarmSendRequest(
            From: "claude-opus",
            To: "codex",
            Topic: "sentix-task",
            Body: "Run Sentix build",
            Wake: true,
            WorkspaceId: "sentix"));
        Assert.Equal("ok", sentixWake.WakeState);
        Assert.Equal("thread-sentix", sentixWake.WakeResults![0].ThreadId);

        var mobaflowWake = await store.SendMessageAsync(new SwarmSendRequest(
            From: "antigravity-gemini",
            To: "codex",
            Topic: "mobaflow-task",
            Body: "Check MOBAflow",
            Wake: true,
            WorkspaceId: "mobaflow"));
        Assert.Equal("ok", mobaflowWake.WakeState);
        Assert.Equal("thread-mobaflow", mobaflowWake.WakeResults![0].ThreadId);

        var aiswarmxWake = await store.SendMessageAsync(new SwarmSendRequest(
            From: "claude-opus",
            To: "all",
            Topic: "aiswarmx-broadcast",
            Body: "Broadcast to AISwarmX",
            Wake: true,
            WorkspaceId: "aiswarmx"));
        Assert.Equal("ok", aiswarmxWake.WakeState);
        Assert.Equal("thread-aiswarmx", aiswarmxWake.WakeResults![0].ThreadId);

        // 2. Explicit codex_thread_id always overrides workspace registration
        var explicitWake = await store.SendMessageAsync(new SwarmSendRequest(
            From: "claude-opus",
            To: "codex",
            Topic: "override",
            Body: "Use explicit thread",
            Wake: true,
            CodexThreadId: "thread-explicit-override",
            WorkspaceId: "sentix"));
        Assert.Equal("thread-explicit-override", explicitWake.WakeResults![0].ThreadId);

        // 3. Unregistered workspace passes null to wake dispatcher so CodexWakeDispatcher fallback applies
        var unregisteredWake = await store.SendMessageAsync(new SwarmSendRequest(
            From: "claude-opus",
            To: "codex",
            Topic: "unregistered",
            Body: "Fallback test",
            Wake: true,
            WorkspaceId: "unknown-repo"));
        Assert.Null(dispatched[^1].CodexThreadId);
        Assert.Equal("fallback-env-thread", unregisteredWake.WakeResults![0].ThreadId);

        // 4. Stale (>72h) session registration is ignored for wake routing
        clock.Advance(TimeSpan.FromHours(SwarmBusConstants.DefaultSessionRegistrationMaxAgeHours + 1));
        var staleWake = await store.SendMessageAsync(new SwarmSendRequest(
            From: "claude-opus",
            To: "codex",
            Topic: "stale-session",
            Body: "Should fall back when registration is older than 72h",
            Wake: true,
            WorkspaceId: "sentix"));
        Assert.Null(dispatched[^1].CodexThreadId);
        Assert.Equal("fallback-env-thread", staleWake.WakeResults![0].ThreadId);
    }

    [Fact]
    public async Task OmniEntryLeadOnInputHandoverPreservesGoalCapsDecisionsAndEnforcesLease()
    {
        using var fx = new TempBusFixture();
        var clock = new TestTimeProvider(new DateTimeOffset(2026, 11, 28, 12, 0, 0, TimeSpan.Zero));
        var store = fx.CreateStore(new SwarmBusStoreOptions { TimeProvider = clock });

        await store.SendMessageAsync(new SwarmSendRequest(
            From: "watchdog",
            To: "antigravity-gemini",
            Topic: "handoff",
            Body: "Ready for Gemini review",
            WorkspaceId: "mobadev"));

        // 1. claude-opus claims initial turn
        var turn1 = store.ClaimProjectTurn(new ProjectTurnClaimRequest(
            WorkspaceId: "mobadev",
            Agent: "claude-opus",
            ActiveGoal: "Issue #68 Virtual Master",
            CurrentSlice: "Slice 1",
            Decision: "D0: Isolated SQLite tables",
            TtlMinutes: 30,
            SessionId: "claude-sess-1",
            Tool: "claude-code",
            Model: "claude-opus-5-5",
            State: "planning",
            Activity: "Architecting Slice 1",
            IssueNumber: 68,
            Branch: "feat/68-slice1-virtual-master-bus"));

        Assert.True(turn1.Ok);
        Assert.Equal("claim-turn", turn1.Action);
        Assert.Null(turn1.PreviousLeadAgent);
        Assert.NotNull(turn1.ProjectState);
        Assert.Equal("claude-opus", turn1.ProjectState!.LeadAgent);
        Assert.True(turn1.ProjectState.IsLeadActive);
        Assert.Equal("Issue #68 Virtual Master", turn1.ProjectState.ActiveGoal);
        Assert.Single(turn1.Sessions);
        Assert.Equal("planning", turn1.Sessions[0].EffectiveState);

        // 2. antigravity-gemini takes over while claude-opus lease is still active (omits ActiveGoal -> preserved)
        clock.Advance(TimeSpan.FromMinutes(5));
        var turn2 = await store.ClaimProjectTurnAsync(new ProjectTurnClaimRequest(
            WorkspaceId: "mobadev",
            Agent: "antigravity-gemini",
            CurrentSlice: "Slice 1 UX Check",
            Decision: "D1: Preserve goal across baton pass",
            TtlMinutes: 20));

        Assert.Equal("claude-opus", turn2.PreviousLeadAgent);
        Assert.Equal("antigravity-gemini", turn2.ProjectState!.LeadAgent);
        Assert.Equal("Issue #68 Virtual Master", turn2.ProjectState.ActiveGoal);
        Assert.Equal("Slice 1 UX Check", turn2.ProjectState.CurrentSlice);
        Assert.Equal(["D0: Isolated SQLite tables", "D1: Preserve goal across baton pass"], turn2.ProjectState.Decisions);
        Assert.Single(turn2.UnreadMessages);

        // 3. Another agent cannot release antigravity-gemini's active baton (fails closed without mutating UpdatedBy)
        var foreignRelease = await store.UpdateProjectStateAsync(new ProjectStateUpdateRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            ReleaseLead: true));
        Assert.False(foreignRelease.Ok);
        Assert.Equal("lead_held_by_other", foreignRelease.Reason);
        Assert.Equal("antigravity-gemini", foreignRelease.ProjectState!.LeadAgent);
        Assert.Equal("antigravity-gemini", foreignRelease.ProjectState.UpdatedBy);
        Assert.True(foreignRelease.ProjectState.IsLeadActive);
        Assert.Null(foreignRelease.PreviousLeadAgent);

        // 4. Own agent can release baton cleanly
        var ownRelease = await store.UpdateProjectStateAsync(new ProjectStateUpdateRequest(
            WorkspaceId: "mobadev",
            Agent: "Antigravity-Gemini",
            ReleaseLead: true));
        Assert.Null(ownRelease.ProjectState!.LeadAgent);
        Assert.False(ownRelease.ProjectState.IsLeadActive);
        Assert.Equal("antigravity-gemini", ownRelease.PreviousLeadAgent);

        // 5. codex claims turn and appends 22 decisions -> deduplicates consecutive & caps at 20
        var turn3 = await store.ClaimProjectTurnAsync(new ProjectTurnClaimRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            Decision: "D1: Preserve goal across baton pass",
            TtlMinutes: 15));
        Assert.Null(turn3.PreviousLeadAgent);

        for (var i = 2; i <= 25; i++)
        {
            await store.ClaimProjectTurnAsync(new ProjectTurnClaimRequest(
                WorkspaceId: "mobadev",
                Agent: "codex",
                Decision: $"D{i}: Decision step {i}",
                TtlMinutes: 15));
        }

        // Duplicate consecutive decision is ignored
        var dupSync = await store.ClaimProjectTurnAsync(new ProjectTurnClaimRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            Decision: "D25: Decision step 25",
            TtlMinutes: 15));

        Assert.Equal(SwarmBusConstants.MaxProjectDecisions, dupSync.ProjectState!.Decisions.Count);
        Assert.Equal("D6: Decision step 6", dupSync.ProjectState.Decisions[0]);
        Assert.Equal("D25: Decision step 25", dupSync.ProjectState.Decisions[^1]);

        // 6. Lease expiry turns IsLeadActive false and allows anyone to release or take over with PreviousLeadAgent == null
        clock.Advance(TimeSpan.FromMinutes(16));
        var afterExpiry = await store.GetProjectSyncAsync("mobadev", "claude-opus");
        Assert.False(afterExpiry.ProjectState!.IsLeadActive);

        var expiredRelease = await store.UpdateProjectStateAsync(new ProjectStateUpdateRequest(
            WorkspaceId: "mobadev",
            Agent: "claude-opus",
            Decisions: ["  Final decision 1  ", "", "Final decision 2"],
            ReleaseLead: true));
        Assert.Null(expiredRelease.ProjectState!.LeadAgent);
        Assert.Null(expiredRelease.PreviousLeadAgent);
        Assert.Equal(["Final decision 1", "Final decision 2"], expiredRelease.ProjectState.Decisions);
    }

    [Fact]
    public async Task SessionHeartbeatTtlExpiresEffectiveStateToIdleWhilePreservingMetadata()
    {
        using var fx = new TempBusFixture();
        var clock = new TestTimeProvider(new DateTimeOffset(2026, 11, 28, 14, 0, 0, TimeSpan.Zero));
        var store = fx.CreateStore(new SwarmBusStoreOptions { TimeProvider = clock });

        var reg = store.UpsertSession(new AgentSessionUpsertRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            SessionId: "thread-abc",
            Tool: "codex-cli",
            Model: "gpt-6.2-sol",
            State: "building",
            Activity: "Writing VirtualMasterTests",
            IssueNumber: 68,
            Branch: "feat/68-slice1-virtual-master-bus",
            TtlMinutes: 10), action: "register");

        Assert.True(reg.Ok);
        Assert.Equal("register", reg.Action);
        Assert.Equal("building", reg.Session!.State);
        Assert.Equal("building", reg.Session.EffectiveState);
        Assert.Equal(clock.GetUtcNow().AddMinutes(10), reg.Session.ActivityExpiresUtc);

        // Lightweight heartbeat omits session_id, tool, model -> preserved
        clock.Advance(TimeSpan.FromMinutes(4));
        var hb = await store.UpsertSessionAsync(new AgentSessionUpsertRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            State: "testing",
            Activity: "Running dotnet test",
            TtlMinutes: 10), action: "heartbeat");

        Assert.Equal("heartbeat", hb.Action);
        Assert.Equal("thread-abc", hb.Session!.SessionId);
        Assert.Equal("codex-cli", hb.Session.Tool);
        Assert.Equal("gpt-6.2-sol", hb.Session.Model);
        Assert.Equal("testing", hb.Session.State);
        Assert.Equal("testing", hb.Session.EffectiveState);

        // Advance past 10m TTL -> EffectiveState degrades to idle without mutating stored metadata
        clock.Advance(TimeSpan.FromMinutes(10));
        var list = await store.ListSessionsAsync("mobadev");
        var expired = Assert.Single(list.Sessions);
        Assert.Equal("testing", expired.State);
        Assert.Equal("idle", expired.EffectiveState);
        Assert.Equal("thread-abc", expired.SessionId);
        Assert.Equal("gpt-6.2-sol", expired.Model);

        // Stateless heartbeat after TTL expiry stays idle and drops stale activity
        var statelessHb = await store.UpsertSessionAsync(new AgentSessionUpsertRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            TtlMinutes: 10), action: "heartbeat");
        Assert.Equal("idle", statelessHb.Session!.State);
        Assert.Equal("idle", statelessHb.Session.EffectiveState);
        Assert.Null(statelessHb.Session.Activity);
        Assert.Null(statelessHb.Session.ActivityExpiresUtc);
        Assert.Equal("thread-abc", statelessHb.Session.SessionId);

        // Re-entering active state without explicit activity does not resurrect expired activity
        var resumedBuilding = await store.UpsertSessionAsync(new AgentSessionUpsertRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            State: "building",
            TtlMinutes: 10), action: "heartbeat");
        Assert.Equal("building", resumedBuilding.Session!.State);
        Assert.Equal("building", resumedBuilding.Session.EffectiveState);
        Assert.Null(resumedBuilding.Session.Activity);

        // Explicit idle state clears activity and activity_expires_utc
        var idled = await store.UpsertSessionAsync(new AgentSessionUpsertRequest(
            WorkspaceId: "mobadev",
            Agent: "codex",
            State: "idle"), action: "heartbeat");
        Assert.Equal("idle", idled.Session!.State);
        Assert.Equal("idle", idled.Session.EffectiveState);
        Assert.Null(idled.Session.Activity);
        Assert.Null(idled.Session.ActivityExpiresUtc);
        Assert.Equal("thread-abc", idled.Session.SessionId);

        // Unregister removes row
        var unreg = await store.UnregisterSessionAsync("codex", "mobadev");
        Assert.True(unreg.Ok);
        Assert.True(unreg.Changed);
        Assert.Null(unreg.Session);
        Assert.Empty((await store.ListSessionsAsync("mobadev")).Sessions);
    }

    [Fact]
    public async Task QuickCaptureIdeasOrderByPriorityPromoteAndEnforceWorkspaceIsolation()
    {
        using var fx = new TempBusFixture();
        var store = fx.CreateStore();

        var lowPrio = store.AddIdea(new ProjectIdeaAddRequest(
            WorkspaceId: "mobaflow",
            Title: "Polish tooltip copy",
            CreatedBy: "antigravity-gemini",
            Priority: 3));
        var highPrio = await store.AddIdeaAsync(new ProjectIdeaAddRequest(
            WorkspaceId: "mobaflow",
            Title: "Fix critical wake race",
            CreatedBy: "claude-opus",
            Details: "Inspect lease fencing",
            Priority: 0));
        var medPrio = await store.AddIdeaAsync(new ProjectIdeaAddRequest(
            WorkspaceId: "mobaflow",
            Title: "Add status summary badge",
            CreatedBy: "codex",
            Priority: 1));
        var otherWs = await store.AddIdeaAsync(new ProjectIdeaAddRequest(
            WorkspaceId: "sentix",
            Title: "Sentix separate idea",
            CreatedBy: "codex",
            Priority: 0));

        var mobaflowIdeas = store.ListIdeas("mobaflow");
        Assert.Equal(3, mobaflowIdeas.Ideas.Count);
        Assert.Equal([highPrio.Idea!.Id, medPrio.Idea!.Id, lowPrio.Idea!.Id], mobaflowIdeas.Ideas.Select(i => i.Id).ToArray());

        // Promote idea with issue number
        var promoted = await store.UpdateIdeaAsync(new ProjectIdeaUpdateRequest(
            Id: highPrio.Idea.Id,
            WorkspaceId: "mobaflow",
            Status: "promoted",
            IssueNumber: 71));
        Assert.True(promoted.Ok);
        Assert.True(promoted.Changed);
        Assert.Equal("promoted", promoted.Idea!.Status);
        Assert.Equal(71, promoted.Idea.IssueNumber);

        // Cross-workspace isolation guard
        var mismatch = await store.UpdateIdeaAsync(new ProjectIdeaUpdateRequest(
            Id: otherWs.Idea!.Id,
            WorkspaceId: "mobaflow",
            Status: "done"));
        Assert.False(mismatch.Ok);
        Assert.False(mismatch.Changed);
        Assert.Equal("workspace_mismatch", mismatch.Reason);

        var notFound = await store.UpdateIdeaAsync(new ProjectIdeaUpdateRequest(
            Id: 99999,
            WorkspaceId: "mobaflow",
            Status: "done"));
        Assert.False(notFound.Ok);
        Assert.Equal("not_found", notFound.Reason);

        var queuedOnly = await store.ListIdeasAsync("mobaflow", status: "queued");
        Assert.Equal(2, queuedOnly.Ideas.Count);
        Assert.DoesNotContain(queuedOnly.Ideas, i => i.Id == highPrio.Idea.Id);
    }

    [Fact]
    public async Task BusStatusIncludesSessionsProjectsAndQueuedIdeaCount()
    {
        using var fx = new TempBusFixture();
        var store = fx.CreateStore();

        await store.UpsertSessionAsync(new AgentSessionUpsertRequest("mobaflow", "codex", SessionId: "t1", State: "building"));
        await store.UpsertSessionAsync(new AgentSessionUpsertRequest("sentix", "claude-opus", SessionId: "c1", State: "planning"));
        await store.ClaimProjectTurnAsync(new ProjectTurnClaimRequest("mobaflow", "codex", ActiveGoal: "Goal A"));
        await store.ClaimProjectTurnAsync(new ProjectTurnClaimRequest("sentix", "claude-opus", ActiveGoal: "Goal B"));
        await store.AddIdeaAsync(new ProjectIdeaAddRequest("mobaflow", "Idea 1", "codex"));
        var promoted = await store.AddIdeaAsync(new ProjectIdeaAddRequest("mobaflow", "Idea 2", "codex"));
        await store.UpdateIdeaAsync(new ProjectIdeaUpdateRequest(promoted.Idea!.Id, WorkspaceId: "mobaflow", Status: "promoted"));
        await store.AddIdeaAsync(new ProjectIdeaAddRequest("sentix", "Idea 3", "claude-opus"));

        var mobaflowStatus = await store.GetStatusAsync(limit: 10, workspaceId: "mobaflow");
        Assert.NotNull(mobaflowStatus.Sessions);
        Assert.NotNull(mobaflowStatus.Projects);
        Assert.Single(mobaflowStatus.Sessions!);
        Assert.Single(mobaflowStatus.Projects!);
        Assert.Equal(1, mobaflowStatus.QueuedIdeaCount);

        var allStatus = await store.GetStatusAsync(limit: 10, workspaceId: "*");
        Assert.Equal(2, allStatus.Sessions!.Count);
        Assert.Equal(2, allStatus.Projects!.Count);
        Assert.Equal(2, allStatus.QueuedIdeaCount);
    }
}
