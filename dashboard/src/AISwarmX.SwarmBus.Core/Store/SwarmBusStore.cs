using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using AISwarmX.SwarmBus.Core.Contracts;
using AISwarmX.SwarmBus.Core.Locking;

namespace AISwarmX.SwarmBus.Core.Store;

/// <summary>
/// Configuration options for <see cref="SwarmBusStore"/>.
/// </summary>
public sealed class SwarmBusStoreOptions
{
    /// <summary>
    /// Optional time provider for deterministic testing of TTL expirations and wake leases.
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>
    /// Optional wake dispatcher invoked outside the SQLite write transaction when a wake target matches.
    /// Implementations should honor the supplied <see cref="CancellationToken"/> and be idempotent under retry/timeout;
    /// if an implementation ignores cancellation, <see cref="SwarmBusStore"/> stops awaiting after
    /// <see cref="WakeDispatchTimeout"/> and marks the wake attempt failed.
    /// When omitted, wake attempts remain in <c>pending</c> state for the 120s lease window until resolved.
    /// </summary>
    public Func<SwarmWakeContext, CancellationToken, Task<SwarmWakeResult>>? WakeDispatcher { get; init; }

    /// <summary>
    /// Maximum retry attempts when SQLite returns <c>SQLITE_BUSY</c> (5) or <c>SQLITE_LOCKED</c> (6), clamped to <c>1..10</c>.
    /// </summary>
    public int MaxBusyRetries { get; init; } = 5;

    /// <summary>
    /// Timeout applied to each wake dispatch attempt. Defaults to 15 seconds.
    /// </summary>
    public TimeSpan WakeDispatchTimeout { get; init; } = TimeSpan.FromSeconds(15);
}

/// <summary>
/// WAL-mode SQLite persistence store for MOBAi Swarm Bus messages, hierarchical path locks,
/// review claims, work queue items, and workspace metadata.
/// </summary>
public sealed partial class SwarmBusStore
{
    private const int SqliteBusyErrorCode = 5;
    private const int SqliteLockedErrorCode = 6;
    private const string IsoFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";
    private const string V2DbEnvVar = "MOBADEV_SWARM_BUS_V2_DB";
    private const string LegacyDbEnvVar = "MOBADEV_SWARM_BUS_DB";
    private const int MaxCanonicalLinkHops = 16;

    private const string AllWildcard = "*";
    private const string AllRecipient = "all";
    private const string NormalPriority = "normal";
    private const string CodeWipMetaKey = "code_wip";
    private const string ColWorkspaceId = "workspace_id";
    private const string ColStatus = "status";
    private const string AppFolderName = "MOBAdev";

    private const string StatusUnread = "unread";
    private const string StatusAcked = "acked";
    private const string StatusClaimed = "claimed";
    private const string StatusCompleted = "completed";
    private const string StatusReleased = "released";
    private const string StatusQueued = "queued";
    private const string StatusDone = "done";
    private const string StatusRemoved = "removed";

    private const string WakeStatePending = "pending";
    private const string WakeStateOk = "ok";
    private const string WakeStateFailed = "failed";

    private const string ActionClaim = "claim";
    private const string ActionComplete = "complete";
    private const string ActionRelease = "release";
    private const string ActionList = "list";
    private const string ActionUpsert = "upsert";

    private const string TableMessages = "messages";
    private const string TableLocks = "locks";
    private const string TableReviewClaims = "review_claims";
    private const string TableWorkQueue = "work_queue";
    private const string TableMeta = "meta";
    private const string TableAgentSessions = "agent_sessions";
    private const string TableProjectState = "project_state";
    private const string TableProjectIdeas = "project_ideas";

    private const string ParamWorkspaceId = "@workspaceId";
    private const string ParamAllWorkspaces = "@allWorkspaces";
    private const string ParamAllRecipients = "@allRecipients";
    private const string ParamUnreadOnly = "@unreadOnly";
    private const string ParamAgent = "@agent";
    private const string ParamPrNumber = "@prNumber";
    private const string ParamIssueNumber = "@issueNumber";
    private const string ParamClaimedAt = "@claimedAt";
    private const string ParamAckedAt = "@ackedAt";
    private const string ParamExpiresAt = "@expiresAt";
    private const string ParamUpdatedAt = "@updatedAt";
    private const string ParamCompletedAt = "@completedAt";
    private const string ParamHeadSha = "@headSha";
    private const string ParamRole = "@role";
    private const string ParamStatus = "@status";
    private const string ParamId = "@id";
    private const string ParamLimit = "@limit";
    private const string ParamKey = "@key";
    private const string ParamValue = "@value";
    private const string ParamSender = "@sender";
    private const string ParamRecipient = "@recipient";
    private const string ParamTopic = "@topic";
    private const string ParamBody = "@body";
    private const string ParamPriority = "@priority";
    private const string ParamReplyToId = "@replyToId";
    private const string ParamAckedBy = "@ackedBy";
    private const string ParamState = "@state";
    private const string ParamWakeOk = "@wakeOk";
    private const string ParamWakeState = "@wakeState";
    private const string ParamWakeExpires = "@wakeExpires";
    private const string ParamTitle = "@title";
    private const string ParamRiskClass = "@riskClass";
    private const string ParamClaimedBy = "@claimedBy";
    private const string ParamLeaseExpires = "@leaseExpires";
    private const string ParamCreatedAt = "@createdAt";
    private const string ParamPathPattern = "@pathPattern";

    private const string DeleteLockByIdSql = "DELETE FROM locks WHERE id = @id";

    private readonly string _dbPath;
    private readonly TimeProvider _timeProvider;
    private readonly Func<SwarmWakeContext, CancellationToken, Task<SwarmWakeResult>>? _wakeDispatcher;
    private readonly int _maxBusyRetries;
    private readonly TimeSpan _wakeDispatchTimeout;
    private readonly SemaphoreSlim _initGate = new(1, 1);
    private volatile bool _initialized;

    [GeneratedRegex("^[0-9a-fA-F]{40}$", RegexOptions.CultureInvariant)]
    private static partial Regex HeadShaRegex();

    private enum SendTxMode
    {
        Inserted,
        Deduplicated,
        RetryWake
    }

    private sealed record SendTxInput(
        string WorkspaceId,
        string Sender,
        string Recipient,
        string Topic,
        string Body,
        int? PrNumber,
        int? IssueNumber,
        long? ReplyToId,
        string? State,
        string Priority,
        bool WakeTargetMatches,
        bool HasExplicitCodexThreadId = false);

    private sealed record SendTxResult(
        SendTxMode Mode,
        long Id,
        DateTimeOffset CreatedAt,
        string? Body = null,
        string? WakeState = null,
        bool WakeInProgress = false,
        string? WakeLeaseToken = null,
        string? ResolvedCodexThreadId = null);

    private sealed record MatchedMessageRow(
        long Id,
        string Body,
        string Status,
        int? WakeOk,
        string? WakeState,
        DateTimeOffset? WakeAttemptExpiresAtUtc);

    private sealed record WakeResolutionContext(
        string FallbackBody,
        bool WakeTargetMatches,
        DateTimeOffset Now,
        string WakeExpiresIso);

    private sealed record LockUpsertEntry(
        string WorkspaceId,
        string Agent,
        string PathPattern,
        int? PrNumber,
        string ClaimedAtIso,
        string ExpiresAtIso);

    private sealed record LegacyMessageRow(
        long Id,
        string WorkspaceId,
        string CreatedAtUtc,
        string Sender,
        string Recipient,
        string Topic,
        string Body,
        int? PrNumber,
        int? IssueNumber,
        string Priority,
        long? ReplyToId,
        string Status,
        string? AckedBy,
        string? AckedAtUtc,
        string? State,
        int? WakeOk,
        string? WakeState,
        string? WakeAttemptExpiresAtUtc);

    private sealed record LegacyLockRow(
        string WorkspaceId,
        string Agent,
        string PathPattern,
        int? PrNumber,
        string ClaimedAtUtc,
        string ExpiresAtUtc);

    private sealed record LegacyReviewClaimRow(
        string WorkspaceId,
        int PrNumber,
        string HeadSha,
        string Role,
        string Agent,
        string ClaimedAtUtc,
        string Status,
        string? ExpiresAtUtc,
        string? CompletedAtUtc);

    private sealed record LegacyWorkQueueRow(
        string WorkspaceId,
        int IssueNumber,
        string Title,
        int Priority,
        string? RiskClass,
        string Status,
        string? ClaimedBy,
        string? ClaimedAtUtc,
        string? LeaseExpiresUtc,
        string UpdatedAtUtc);

    private sealed record LegacyMetaRow(
        string WorkspaceId,
        string Key,
        string Value,
        string UpdatedAtUtc);

    private sealed record LegacyDbSnapshot(
        IReadOnlyList<LegacyMessageRow> Messages,
        IReadOnlyList<LegacyLockRow> Locks,
        IReadOnlyList<LegacyReviewClaimRow> ReviewClaims,
        IReadOnlyList<LegacyWorkQueueRow> WorkQueueItems,
        IReadOnlyList<LegacyMetaRow> MetaEntries);

    public SwarmBusStore(string? dbPath = null, SwarmBusStoreOptions? options = null)
    {
        var resolvedPath = string.IsNullOrWhiteSpace(dbPath) ? ResolveDefaultDbPath() : NormalizePathForComparison(dbPath);
        ThrowIfProtectedLegacyPath(resolvedPath);

        _dbPath = resolvedPath;
        var resolved = options ?? new SwarmBusStoreOptions();
        _timeProvider = resolved.TimeProvider;
        _wakeDispatcher = resolved.WakeDispatcher;
        _maxBusyRetries = Math.Clamp(resolved.MaxBusyRetries, 1, 10);
        _wakeDispatchTimeout = resolved.WakeDispatchTimeout < TimeSpan.FromMilliseconds(50)
            ? TimeSpan.FromMilliseconds(50)
            : resolved.WakeDispatchTimeout;
    }

    public string DatabasePath => _dbPath;

    /// <summary>
    /// Resolves the default v2 Swarm Bus database path (<c>MOBADEV_SWARM_BUS_V2_DB</c> or <c>%LOCALAPPDATA%\MOBAdev\swarm-bus-v2.db</c>),
    /// keeping the live Node MCP <c>swarm-bus.db</c> and <c>MOBADEV_SWARM_BUS_DB</c> untouched.
    /// </summary>
    public static string ResolveDefaultDbPath()
    {
        var overridePath = Environment.GetEnvironmentVariable(V2DbEnvVar);
        if (!string.IsNullOrWhiteSpace(overridePath))
        {
            return NormalizePathForComparison(overridePath);
        }

        return Path.Combine(ResolveLocalAppDataDir(), AppFolderName, SwarmBusConstants.DefaultDbFileName);
    }

    /// <summary>
    /// Resolves the legacy single-repo Swarm Bus database path (<c>%LOCALAPPDATA%\MOBAdev\swarm-bus.db</c>).
    /// </summary>
    public static string ResolveLegacyDefaultDbPath() =>
        Path.Combine(ResolveLocalAppDataDir(), AppFolderName, SwarmBusConstants.LegacyDbFileName);

    private static string NormalizePathForComparison(string path) =>
        TrimTrailingSeparatorsUnlessRoot(Path.GetFullPath(path.Trim()));

    private static string TrimTrailingSeparatorsUnlessRoot(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath);
        return string.Equals(fullPath, root, StringComparison.Ordinal)
            ? fullPath
            : fullPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
    }

    internal static string ResolveCanonicalPhysicalPath(string rawPath) =>
        ResolveCanonicalPhysicalPathCore(rawPath, MaxCanonicalLinkHops, resolveLinkTarget: null);

    internal static string ResolveCanonicalPhysicalPath(string rawPath, Func<FileSystemInfo, string?> resolveLinkTarget) =>
        ResolveCanonicalPhysicalPathCore(rawPath, MaxCanonicalLinkHops, resolveLinkTarget);

    private static string ResolveCanonicalPhysicalPathCore(
        string rawPath,
        int remainingHops,
        Func<FileSystemInfo, string?>? resolveLinkTarget)
    {
        var full = NormalizePathForComparison(rawPath);
        if (remainingHops <= 0)
        {
            return full;
        }

        var root = Path.GetPathRoot(full);
        if (string.IsNullOrEmpty(root))
        {
            return full;
        }

        var relative = full[root.Length..];
        var segments = relative.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);
        var resolved = root;
        var hitMissingSegment = false;

        foreach (var segment in segments)
        {
            if (hitMissingSegment)
            {
                resolved = Path.Combine(resolved, segment);
                continue;
            }

            resolved = ResolveSegment(resolved, segment, remainingHops, resolveLinkTarget, out var existsOnDisk);
            hitMissingSegment = !existsOnDisk;
        }

        return TrimTrailingSeparatorsUnlessRoot(resolved);
    }

    private static string ResolveSegment(
        string parentDir,
        string segment,
        int remainingHops,
        Func<FileSystemInfo, string?>? resolveLinkTarget,
        out bool existsOnDisk)
    {
        var candidate = Path.Combine(parentDir, segment);
        var linkTarget = TryResolveLinkTarget(candidate, resolveLinkTarget, out existsOnDisk);
        if (linkTarget is null)
        {
            return candidate;
        }

        return ResolveCanonicalPhysicalPathCore(linkTarget, remainingHops - 1, resolveLinkTarget);
    }

    private static string? TryResolveLinkTarget(
        string candidate,
        Func<FileSystemInfo, string?>? resolveLinkTarget,
        out bool existsOnDisk)
    {
        FileSystemInfo? info = File.Exists(candidate)
            ? new FileInfo(candidate)
            : Directory.Exists(candidate)
                ? new DirectoryInfo(candidate)
                : null;

        if (info is null)
        {
            existsOnDisk = false;
            return null;
        }

        try
        {
            existsOnDisk = true;
            if (resolveLinkTarget is not null)
            {
                var overriddenTarget = resolveLinkTarget(info);
                return overriddenTarget is null ? null : StripExtendedDrivePrefix(overriddenTarget);
            }

            var resolvedTarget = info.ResolveLinkTarget(returnFinalTarget: true);
            return resolvedTarget is null ? null : StripExtendedDrivePrefix(resolvedTarget.FullName);
        }
        catch (Exception ex) when (ex is DirectoryNotFoundException or FileNotFoundException)
        {
            // Confirmed TOCTOU disappearance between Exists() and link-target read.
            existsOnDisk = false;
            return null;
        }
    }

    private static string StripExtendedDrivePrefix(string path)
    {
        // Strip Windows \\?\C:\ prefix if returned by ResolveLinkTarget so Path.GetFullPath/comparisons stay uniform.
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal) && path.Length > 6 && path[5] == ':')
        {
            return path[4..];
        }

        return path;
    }

    private static bool PathsEqual(
        string leftPath,
        string rightPath,
        Func<FileSystemInfo, string?>? resolveLinkTarget = null)
    {
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return string.Equals(
            ResolveCanonicalPhysicalPathCore(leftPath, MaxCanonicalLinkHops, resolveLinkTarget),
            ResolveCanonicalPhysicalPathCore(rightPath, MaxCanonicalLinkHops, resolveLinkTarget),
            comparison);
    }

    internal static void ValidateTargetNotProtectedLegacyPath(
        string targetDbPath,
        string legacyPath,
        Func<FileSystemInfo, string?>? resolveLinkTarget = null)
    {
        if (PathsEqual(targetDbPath, legacyPath, resolveLinkTarget))
        {
            throw new InvalidOperationException(
                $"SwarmBusStore cannot open the live legacy database '{targetDbPath}' in writable mode. Use '{SwarmBusConstants.DefaultDbFileName}' or '{V2DbEnvVar}', and import via ImportFromLegacyDb.");
        }
    }

    private static void ThrowIfProtectedLegacyPath(string targetDbPath)
    {
        ValidateTargetNotProtectedLegacyPath(targetDbPath, ResolveLegacyDefaultDbPath());

        var legacyEnvPath = Environment.GetEnvironmentVariable(LegacyDbEnvVar);
        if (!string.IsNullOrWhiteSpace(legacyEnvPath) && PathsEqual(targetDbPath, legacyEnvPath))
        {
            throw new InvalidOperationException(
                $"SwarmBusStore cannot open '{LegacyDbEnvVar}' ('{targetDbPath}') in writable mode. Use '{V2DbEnvVar}' for v2 storage and ImportFromLegacyDb for read-only migration.");
        }
    }

    private static string ResolveLocalAppDataDir()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (!string.IsNullOrWhiteSpace(localAppData))
        {
            return localAppData;
        }

        return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "AppData", "Local");
    }

    /// <summary>
    /// Explicitly ensures the SQLite database directory exists, WAL mode is active, and v2 tables/columns are initialized.
    /// </summary>
    public void EnsureInitialized()
    {
        if (_initialized)
        {
            return;
        }

        _initGate.Wait();
        try
        {
            if (_initialized)
            {
                return;
            }

            EnsureDirectoryExists();
            ExecuteWithBusyRetry(() =>
            {
                InitializeDatabaseOnConnection();
                return 0;
            });

            _initialized = true;
        }
        finally
        {
            _initGate.Release();
        }
    }

    /// <summary>
    /// Asynchronously ensures the SQLite database directory exists, WAL mode is active, and v2 tables/columns are initialized.
    /// </summary>
    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized)
        {
            return;
        }

        await _initGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
            {
                return;
            }

            EnsureDirectoryExists();
            await ExecuteWithBusyRetryAsync(_ =>
            {
                InitializeDatabaseOnConnection();
                return Task.FromResult(0);
            }, cancellationToken).ConfigureAwait(false);

            _initialized = true;
        }
        finally
        {
            _initGate.Release();
        }
    }

    /// <summary>
    /// Reads an existing legacy single-repo <c>swarm-bus.db</c> strictly in <c>Mode=ReadOnly</c> (never mutating the source file)
    /// and imports its messages, locks, review claims, work queue items, and metadata into this v2 store idempotently.
    /// </summary>
    public LegacyImportResult ImportFromLegacyDb(string legacyDbPath)
    {
        var fullLegacyPath = ValidateLegacySourcePath(legacyDbPath, _dbPath);
        var snapshot = ReadLegacySnapshotWithRetry(fullLegacyPath);
        return ExecuteInImmediateTransaction(connection => WriteLegacyImportInTx(connection, fullLegacyPath, snapshot));
    }

    /// <summary>
    /// Asynchronously reads an existing legacy single-repo <c>swarm-bus.db</c> strictly in <c>Mode=ReadOnly</c>
    /// and imports its data into this v2 store idempotently.
    /// </summary>
    public async Task<LegacyImportResult> ImportFromLegacyDbAsync(string legacyDbPath, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var fullLegacyPath = ValidateLegacySourcePath(legacyDbPath, _dbPath);
        var snapshot = await ExecuteWithBusyRetryAsync(
            _ => Task.FromResult(ReadLegacySnapshot(fullLegacyPath)),
            cancellationToken).ConfigureAwait(false);

        return await ExecuteInImmediateTransactionAsync(
            connection => WriteLegacyImportInTx(connection, fullLegacyPath, snapshot),
            cancellationToken).ConfigureAwait(false);
    }

    public SwarmSendResult SendMessage(SwarmSendRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var input = BuildSendTxInput(request);
        var txResult = ExecuteInImmediateTransaction(connection => SendOrDeduplicateInTx(connection, input));
        var effectiveCodexThreadId = string.IsNullOrWhiteSpace(request.CodexThreadId)
            ? txResult.ResolvedCodexThreadId
            : request.CodexThreadId;

        return txResult.Mode switch
        {
            SendTxMode.Deduplicated => CreateDeduplicatedSendResult(txResult, input),
            SendTxMode.RetryWake when _wakeDispatcher is null => CreatePendingRetrySendResult(txResult, input),
            SendTxMode.RetryWake => RunWakeSync(txResult, input, effectiveCodexThreadId, isRetry: true),
            _ when input.WakeTargetMatches && _wakeDispatcher is not null => RunWakeSync(txResult, input, effectiveCodexThreadId, isRetry: false),
            _ => CreateInsertedSendResult(txResult, input, [], input.WakeTargetMatches ? WakeStatePending : null)
        };
    }

    public async Task<SwarmSendResult> SendMessageAsync(SwarmSendRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var input = BuildSendTxInput(request);
        var txResult = await ExecuteInImmediateTransactionAsync(
            connection => SendOrDeduplicateInTx(connection, input),
            cancellationToken).ConfigureAwait(false);
        var effectiveCodexThreadId = string.IsNullOrWhiteSpace(request.CodexThreadId)
            ? txResult.ResolvedCodexThreadId
            : request.CodexThreadId;

        return txResult.Mode switch
        {
            SendTxMode.Deduplicated => CreateDeduplicatedSendResult(txResult, input),
            SendTxMode.RetryWake => await HandleRetryWakeAsync(
                txResult,
                input,
                effectiveCodexThreadId,
                cancellationToken).ConfigureAwait(false),
            _ => await HandleInsertedMessageAsync(
                txResult,
                input,
                effectiveCodexThreadId,
                cancellationToken).ConfigureAwait(false)
        };
    }

    /// <summary>
    /// Completes an in-flight wake attempt for <paramref name="messageId"/> only if the row is still <c>'pending'</c>
    /// and its <c>wake_attempt_expires_at_utc</c> matches <paramref name="expectedLeaseExpiresAtUtc"/> (fencing token).
    /// </summary>
    public bool CompleteWakeAttempt(long messageId, bool ok, DateTimeOffset expectedLeaseExpiresAtUtc) =>
        CompleteWakeAttemptCore(messageId, ok, FormatUtc(expectedLeaseExpiresAtUtc));

    private bool CompleteWakeAttemptCore(long messageId, bool ok, string expectedLeaseIso)
    {
        return ExecuteInImmediateTransaction(connection =>
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                UPDATE messages
                SET wake_state = @wakeState,
                    wake_ok = @wakeOk,
                    wake_attempt_expires_at_utc = NULL
                WHERE id = @id
                  AND wake_state = 'pending'
                  AND wake_attempt_expires_at_utc = @expectedLease
                """;
            cmd.Parameters.AddWithValue(ParamWakeState, ok ? WakeStateOk : WakeStateFailed);
            cmd.Parameters.AddWithValue(ParamWakeOk, ok ? 1 : 0);
            cmd.Parameters.AddWithValue(ParamId, messageId);
            cmd.Parameters.AddWithValue("@expectedLease", expectedLeaseIso);
            return cmd.ExecuteNonQuery() > 0;
        });
    }

    private Task<bool> CompleteWakeAttemptAsync(long messageId, bool ok, string expectedLeaseIso, CancellationToken cancellationToken)
    {
        return ExecuteInImmediateTransactionAsync(connection =>
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                UPDATE messages
                SET wake_state = @wakeState,
                    wake_ok = @wakeOk,
                    wake_attempt_expires_at_utc = NULL
                WHERE id = @id
                  AND wake_state = 'pending'
                  AND wake_attempt_expires_at_utc = @expectedLease
                """;
            cmd.Parameters.AddWithValue(ParamWakeState, ok ? WakeStateOk : WakeStateFailed);
            cmd.Parameters.AddWithValue(ParamWakeOk, ok ? 1 : 0);
            cmd.Parameters.AddWithValue(ParamId, messageId);
            cmd.Parameters.AddWithValue("@expectedLease", expectedLeaseIso);
            return cmd.ExecuteNonQuery() > 0;
        }, cancellationToken);
    }

    public SwarmInboxResult GetInbox(
        string agent = AllRecipient,
        bool unreadOnly = true,
        int limit = 20,
        string? workspaceId = null)
    {
        var (normalizedAgent, clampedLimit, allWorkspaces, normalizedWorkspace, includeAllRecipients) =
            NormalizeInboxQuery(agent, limit, workspaceId);

        var messages = ExecuteRead(connection =>
            ReadInboxMessages(connection, allWorkspaces, normalizedWorkspace, includeAllRecipients, normalizedAgent, unreadOnly, clampedLimit));

        return new SwarmInboxResult(
            Agent: normalizedAgent,
            WorkspaceId: normalizedWorkspace,
            UnreadOnly: unreadOnly,
            Count: messages.Count,
            Messages: messages);
    }

    public async Task<SwarmInboxResult> GetInboxAsync(
        string agent = AllRecipient,
        bool unreadOnly = true,
        int limit = 20,
        string? workspaceId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (normalizedAgent, clampedLimit, allWorkspaces, normalizedWorkspace, includeAllRecipients) =
            NormalizeInboxQuery(agent, limit, workspaceId);

        var messages = await ExecuteReadAsync(
            connection => ReadInboxMessages(connection, allWorkspaces, normalizedWorkspace, includeAllRecipients, normalizedAgent, unreadOnly, clampedLimit),
            cancellationToken).ConfigureAwait(false);

        return new SwarmInboxResult(
            Agent: normalizedAgent,
            WorkspaceId: normalizedWorkspace,
            UnreadOnly: unreadOnly,
            Count: messages.Count,
            Messages: messages);
    }

    /// <summary>
    /// Zero-token blocking wait for unread Swarm Bus messages newer than <see cref="SwarmWaitRequest.SinceId"/>.
    /// Uses <see cref="FileSystemWatcher"/> on the SQLite database/WAL directory with a 250ms fallback poll.
    /// </summary>
    public async Task<SwarmWaitResult> WaitForMessagesAsync(SwarmWaitRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var (normalizedAgent, sinceId, timeoutMs, clampedLimit, allWorkspaces, normalizedWorkspace, includeAllRecipients) =
            ValidateWaitRequest(request);

        var (immediate, hasMore) = await ExecuteReadAsync(
            connection => ReadWaitMessagesBatch(
                connection,
                allWorkspaces,
                normalizedWorkspace,
                includeAllRecipients,
                normalizedAgent,
                clampedLimit,
                sinceId),
            cancellationToken).ConfigureAwait(false);

        if (immediate.Count > 0)
        {
            return new SwarmWaitResult(
                Ok: true,
                WorkspaceId: normalizedWorkspace,
                TimedOut: false,
                LatestId: Math.Max(sinceId, immediate[^1].Id),
                Messages: immediate,
                HasMore: hasMore);
        }

        return await WaitForNewMessagesLoopAsync(
            normalizedAgent,
            sinceId,
            timeoutMs,
            clampedLimit,
            allWorkspaces,
            normalizedWorkspace,
            includeAllRecipients,
            cancellationToken).ConfigureAwait(false);
    }

    public SwarmAckResult AckMessage(long messageId, string agent, string? reply = null)
    {
        if (messageId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(messageId), "bus_ack requires positive \"message_id\".");
        }

        var normalizedAgent = RequireNonEmpty(agent, "bus_ack requires non-empty \"agent\".", nameof(agent));
        var trimmedReply = string.IsNullOrWhiteSpace(reply) ? null : reply.Trim();

        var ackInfo = ExecuteInImmediateTransaction(connection =>
            MarkMessageAckedInTx(connection, messageId, normalizedAgent));

        SwarmSendResult? replyResult = null;
        if (trimmedReply is not null)
        {
            replyResult = SendMessage(new SwarmSendRequest(
                From: normalizedAgent,
                To: ackInfo.OrigSender,
                Topic: $"re:{ackInfo.OrigTopic}",
                Body: trimmedReply,
                PrNumber: ackInfo.PrNumber,
                IssueNumber: ackInfo.IssueNumber,
                ReplyToId: messageId,
                WorkspaceId: ackInfo.WorkspaceId));
        }

        return new SwarmAckResult(
            Ok: true,
            AckedMessageId: messageId,
            AckedBy: normalizedAgent,
            AckedAtUtc: ackInfo.AckedAt,
            ReplyMessage: replyResult);
    }

    public async Task<SwarmAckResult> AckMessageAsync(
        long messageId,
        string agent,
        string? reply = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (messageId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(messageId), "bus_ack requires positive \"message_id\".");
        }

        var normalizedAgent = RequireNonEmpty(agent, "bus_ack requires non-empty \"agent\".", nameof(agent));
        var trimmedReply = string.IsNullOrWhiteSpace(reply) ? null : reply.Trim();

        var ackInfo = await ExecuteInImmediateTransactionAsync(
            connection => MarkMessageAckedInTx(connection, messageId, normalizedAgent),
            cancellationToken).ConfigureAwait(false);

        SwarmSendResult? replyResult = null;
        if (trimmedReply is not null)
        {
            replyResult = await SendMessageAsync(new SwarmSendRequest(
                From: normalizedAgent,
                To: ackInfo.OrigSender,
                Topic: $"re:{ackInfo.OrigTopic}",
                Body: trimmedReply,
                PrNumber: ackInfo.PrNumber,
                IssueNumber: ackInfo.IssueNumber,
                ReplyToId: messageId,
                WorkspaceId: ackInfo.WorkspaceId), cancellationToken).ConfigureAwait(false);
        }

        return new SwarmAckResult(
            Ok: true,
            AckedMessageId: messageId,
            AckedBy: normalizedAgent,
            AckedAtUtc: ackInfo.AckedAt,
            ReplyMessage: replyResult);
    }

    public LockClaimResult ClaimLocks(LockClaimRequest request)
    {
        var (agent, workspaceId, cleanPaths, ttlMinutes) = ValidateLockClaimRequest(request);
        return ExecuteInImmediateTransaction(connection => ClaimLocksInTx(connection, agent, workspaceId, cleanPaths, request.PrNumber, ttlMinutes));
    }

    public Task<LockClaimResult> ClaimLocksAsync(LockClaimRequest request, CancellationToken cancellationToken = default)
    {
        var (agent, workspaceId, cleanPaths, ttlMinutes) = ValidateLockClaimRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => ClaimLocksInTx(connection, agent, workspaceId, cleanPaths, request.PrNumber, ttlMinutes),
            cancellationToken);
    }

    public LockReleaseResult ReleaseLocks(LockReleaseRequest request)
    {
        var (agent, cleanPaths, workspaceId) = ValidateLockReleaseRequest(request);
        return ExecuteInImmediateTransaction(connection => ReleaseLocksInTx(connection, agent, cleanPaths, request.PrNumber, workspaceId));
    }

    public Task<LockReleaseResult> ReleaseLocksAsync(LockReleaseRequest request, CancellationToken cancellationToken = default)
    {
        var (agent, cleanPaths, workspaceId) = ValidateLockReleaseRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => ReleaseLocksInTx(connection, agent, cleanPaths, request.PrNumber, workspaceId),
            cancellationToken);
    }

    public IReadOnlyList<PathLockLease> ListLocks(string? workspaceId = null)
    {
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        return ExecuteInImmediateTransaction(connection => ListLocksInTx(connection, allWorkspaces, normalizedWorkspace));
    }

    public Task<IReadOnlyList<PathLockLease>> ListLocksAsync(string? workspaceId = null, CancellationToken cancellationToken = default)
    {
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        return ExecuteInImmediateTransactionAsync(
            connection => ListLocksInTx(connection, allWorkspaces, normalizedWorkspace),
            cancellationToken);
    }

    /// <summary>
    /// Single-roundtrip startup: optionally claims <see cref="SwarmStartWorkRequest.Paths"/>,
    /// retrieves unread inbox messages for <see cref="SwarmStartWorkRequest.Agent"/>,
    /// and returns active foreign locks in the same workspace.
    /// </summary>
    public SwarmStartWorkResult StartWork(SwarmStartWorkRequest request)
    {
        var (agent, workspaceId, cleanPaths, ttlMinutes, messageLimit) = ValidateStartWorkRequest(request);
        return ExecuteInImmediateTransaction(
            connection => ExecuteStartWorkInTx(connection, agent, workspaceId, cleanPaths, request.PrNumber, ttlMinutes, messageLimit));
    }

    /// <summary>
    /// Asynchronously executes single-roundtrip <see cref="StartWork"/>.
    /// </summary>
    public Task<SwarmStartWorkResult> StartWorkAsync(SwarmStartWorkRequest request, CancellationToken cancellationToken = default)
    {
        var (agent, workspaceId, cleanPaths, ttlMinutes, messageLimit) = ValidateStartWorkRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => ExecuteStartWorkInTx(connection, agent, workspaceId, cleanPaths, request.PrNumber, ttlMinutes, messageLimit),
            cancellationToken);
    }

    public ReviewClaimResult ReviewClaim(ReviewClaimRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var action = string.IsNullOrWhiteSpace(request.Action) ? ActionClaim : request.Action.Trim().ToLowerInvariant();
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);

        return action switch
        {
            ActionClaim => ClaimReviewSlot(request, workspaceId),
            ActionComplete => CompleteReviewSlot(request, workspaceId),
            ActionRelease => ReleaseReviewSlot(request, workspaceId),
            ActionList => ListReviewSlots(request),
            _ => throw new ArgumentException($"Unsupported bus_review_claim action '{action}'. Use: claim, complete, release, list.", nameof(request))
        };
    }

    public Task<ReviewClaimResult> ReviewClaimAsync(ReviewClaimRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var action = string.IsNullOrWhiteSpace(request.Action) ? ActionClaim : request.Action.Trim().ToLowerInvariant();
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);

        return action switch
        {
            ActionClaim => ClaimReviewSlotAsync(request, workspaceId, cancellationToken),
            ActionComplete => CompleteReviewSlotAsync(request, workspaceId, cancellationToken),
            ActionRelease => ReleaseReviewSlotAsync(request, workspaceId, cancellationToken),
            ActionList => ListReviewSlotsAsync(request, cancellationToken),
            _ => throw new ArgumentException($"Unsupported bus_review_claim action '{action}'. Use: claim, complete, release, list.", nameof(request))
        };
    }

    public WorkQueueMutationResult QueueUpsert(WorkQueueUpsertRequest request)
    {
        var parsed = ValidateQueueUpsertRequest(request);
        var result = ExecuteInImmediateTransaction(connection => ExecuteQueueUpsertInTx(connection, parsed));
        if (result is null)
        {
            throw new ArgumentException("bus_queue upsert requires non-empty \"title\".", nameof(request));
        }

        return result;
    }

    public async Task<WorkQueueMutationResult> QueueUpsertAsync(WorkQueueUpsertRequest request, CancellationToken cancellationToken = default)
    {
        var parsed = ValidateQueueUpsertRequest(request);
        var result = await ExecuteInImmediateTransactionAsync(
            connection => ExecuteQueueUpsertInTx(connection, parsed),
            cancellationToken).ConfigureAwait(false);
        if (result is null)
        {
            throw new ArgumentException("bus_queue upsert requires non-empty \"title\".", nameof(request));
        }

        return result;
    }

    public WorkQueueNextResult QueueNext(string agent, string? workspaceId = null)
    {
        var normalizedAgent = RequireNonEmpty(agent, "bus_queue next requires non-empty \"agent\".", nameof(agent));
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        return ExecuteInImmediateTransaction(connection => ExecuteQueueNextInTx(connection, normalizedAgent, normalizedWorkspace));
    }

    public Task<WorkQueueNextResult> QueueNextAsync(string agent, string? workspaceId = null, CancellationToken cancellationToken = default)
    {
        var normalizedAgent = RequireNonEmpty(agent, "bus_queue next requires non-empty \"agent\".", nameof(agent));
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        return ExecuteInImmediateTransactionAsync(
            connection => ExecuteQueueNextInTx(connection, normalizedAgent, normalizedWorkspace),
            cancellationToken);
    }

    /// <summary>
    /// Strictly resume-only: renews an active claimed lease for <paramref name="issueNumber"/> held by <paramref name="owner"/>.
    /// Returns <c>Ok = false, Resumed = false, Reason = "lease_lost"</c> when no matching active lease is held; never claims new work.
    /// </summary>
    public WorkQueueNextResult QueueResume(int issueNumber, string owner, string? workspaceId = null)
    {
        var (normalizedIssue, normalizedOwner, normalizedWorkspace) = ValidateQueueResumeArgs(issueNumber, owner, workspaceId);
        return ExecuteInImmediateTransaction(connection =>
            ExecuteQueueResumeInTx(connection, normalizedIssue, normalizedOwner, normalizedWorkspace));
    }

    /// <summary>
    /// Asynchronously renews an active claimed lease for <paramref name="issueNumber"/> held by <paramref name="owner"/> without claiming new work.
    /// </summary>
    public Task<WorkQueueNextResult> QueueResumeAsync(
        int issueNumber,
        string owner,
        string? workspaceId = null,
        CancellationToken cancellationToken = default)
    {
        var (normalizedIssue, normalizedOwner, normalizedWorkspace) = ValidateQueueResumeArgs(issueNumber, owner, workspaceId);
        return ExecuteInImmediateTransactionAsync(
            connection => ExecuteQueueResumeInTx(connection, normalizedIssue, normalizedOwner, normalizedWorkspace),
            cancellationToken);
    }

    public WorkQueueMutationResult QueueRelease(int issueNumber, string? agent = null, string? workspaceId = null)
    {
        var (normalizedWorkspace, normalizedAgent) = ValidateQueueReleaseArgs(issueNumber, agent, workspaceId);
        return ExecuteInImmediateTransaction(connection => ExecuteQueueReleaseInTx(connection, issueNumber, normalizedAgent, normalizedWorkspace));
    }

    public Task<WorkQueueMutationResult> QueueReleaseAsync(
        int issueNumber,
        string? agent = null,
        string? workspaceId = null,
        CancellationToken cancellationToken = default)
    {
        var (normalizedWorkspace, normalizedAgent) = ValidateQueueReleaseArgs(issueNumber, agent, workspaceId);
        return ExecuteInImmediateTransactionAsync(
            connection => ExecuteQueueReleaseInTx(connection, issueNumber, normalizedAgent, normalizedWorkspace),
            cancellationToken);
    }

    public WorkQueueMutationResult QueueDone(
        int issueNumber,
        string? workspaceId = null,
        string? expectedStatus = null,
        string? expectedOwner = null,
        DateTimeOffset? claimedBeforeUtc = null) =>
        QueueDone(new WorkQueueDoneRequest(
            IssueNumber: issueNumber,
            WorkspaceId: workspaceId,
            ExpectedStatus: expectedStatus,
            ExpectedOwner: expectedOwner,
            ClaimedBeforeUtc: claimedBeforeUtc));

    public WorkQueueMutationResult QueueDone(WorkQueueDoneRequest request)
    {
        var parsed = ValidateQueueDoneRequest(request);
        return ExecuteInImmediateTransaction(connection => ExecuteQueueDoneInTx(connection, parsed));
    }

    public Task<WorkQueueMutationResult> QueueDoneAsync(
        int issueNumber,
        string? workspaceId = null,
        string? expectedStatus = null,
        string? expectedOwner = null,
        DateTimeOffset? claimedBeforeUtc = null,
        CancellationToken cancellationToken = default) =>
        QueueDoneAsync(
            new WorkQueueDoneRequest(
                IssueNumber: issueNumber,
                WorkspaceId: workspaceId,
                ExpectedStatus: expectedStatus,
                ExpectedOwner: expectedOwner,
                ClaimedBeforeUtc: claimedBeforeUtc),
            cancellationToken);

    public Task<WorkQueueMutationResult> QueueDoneAsync(
        WorkQueueDoneRequest request,
        CancellationToken cancellationToken = default)
    {
        var parsed = ValidateQueueDoneRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => ExecuteQueueDoneInTx(connection, parsed),
            cancellationToken);
    }

    public WorkQueueListResult QueueList(string? status = null, string? workspaceId = null)
    {
        var (normalizedWorkspace, statusFilter) = ValidateQueueListArgs(status, workspaceId);
        return ExecuteReadTransaction(connection => ReadQueueListSnapshot(connection, normalizedWorkspace, statusFilter));
    }

    public Task<WorkQueueListResult> QueueListAsync(
        string? status = null,
        string? workspaceId = null,
        CancellationToken cancellationToken = default)
    {
        var (normalizedWorkspace, statusFilter) = ValidateQueueListArgs(status, workspaceId);
        return ExecuteReadTransactionAsync(
            connection => ReadQueueListSnapshot(connection, normalizedWorkspace, statusFilter),
            cancellationToken);
    }

    public void SyncMeta(string key, string value, string? workspaceId = null)
    {
        var normalizedKey = RequireNonEmpty(key, "bus_queue sync-meta requires non-empty \"meta_key\".", nameof(key));
        ArgumentNullException.ThrowIfNull(value);
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);

        ExecuteInImmediateTransaction(connection =>
        {
            UpsertMetaInTx(connection, normalizedWorkspace, normalizedKey, value, FormatUtc(_timeProvider.GetUtcNow()));
            return 0;
        });
    }

    public Task SyncMetaAsync(string key, string value, string? workspaceId = null, CancellationToken cancellationToken = default)
    {
        var normalizedKey = RequireNonEmpty(key, "bus_queue sync-meta requires non-empty \"meta_key\".", nameof(key));
        ArgumentNullException.ThrowIfNull(value);
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);

        return ExecuteInImmediateTransactionAsync(connection =>
        {
            UpsertMetaInTx(connection, normalizedWorkspace, normalizedKey, value, FormatUtc(_timeProvider.GetUtcNow()));
            return 0;
        }, cancellationToken);
    }

    public SwarmBusStatusSnapshot GetStatus(int limit = 15, string? workspaceId = null)
    {
        var clampedLimit = Math.Clamp(limit, 1, 50);
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        return ExecuteInImmediateTransaction(connection => BuildStatusSnapshotInTx(connection, allWorkspaces, normalizedWorkspace, clampedLimit));
    }

    public Task<SwarmBusStatusSnapshot> GetStatusAsync(int limit = 15, string? workspaceId = null, CancellationToken cancellationToken = default)
    {
        var clampedLimit = Math.Clamp(limit, 1, 50);
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        return ExecuteInImmediateTransactionAsync(
            connection => BuildStatusSnapshotInTx(connection, allWorkspaces, normalizedWorkspace, clampedLimit),
            cancellationToken);
    }

    private sealed record ParsedQueueUpsert(
        string WorkspaceId,
        int IssueNumber,
        string? Title,
        int Priority,
        string? RiskClass,
        string Status,
        bool IsTerminalWithoutTitle);

    private void EnsureDirectoryExists()
    {
        var directory = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private void InitializeDatabaseOnConnection()
    {
        using var connection = OpenConnectionCore();
        ExecuteNonQuery(connection, "PRAGMA journal_mode = WAL;");

        RunInImmediateTransactionCore(connection, () =>
        {
            MigrateAndCreateSchema(connection);
            return 0;
        });
    }

    private static string ValidateLegacySourcePath(string legacyDbPath, string targetDbPath)
    {
        var path = RequireNonEmpty(legacyDbPath, "Legacy database path must not be empty.", nameof(legacyDbPath));
        var fullPath = NormalizePathForComparison(path);
        if (PathsEqual(fullPath, targetDbPath))
        {
            throw new ArgumentException(
                $"Legacy source database path '{fullPath}' cannot be the same as the target v2 database path.",
                nameof(legacyDbPath));
        }

        ThrowIfProtectedLegacyPath(targetDbPath);

        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException($"Legacy Swarm Bus database not found: {fullPath}", fullPath);
        }

        return fullPath;
    }

    private LegacyDbSnapshot ReadLegacySnapshotWithRetry(string fullLegacyPath) =>
        ExecuteWithBusyRetry(() => ReadLegacySnapshot(fullLegacyPath));

    private static LegacyDbSnapshot ReadLegacySnapshot(string fullLegacyPath)
    {
        var cs = new SqliteConnectionStringBuilder
        {
            DataSource = fullLegacyPath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Default,
            Pooling = false,
            DefaultTimeout = 1
        }.ToString();

        using var conn = new SqliteConnection(cs);
        conn.Open();
        ExecuteNonQuery(conn, "PRAGMA busy_timeout = 1000;");

        return RunInReadTransactionCore(conn, () => new LegacyDbSnapshot(
            Messages: ReadLegacyMessages(conn),
            Locks: ReadLegacyLocks(conn),
            ReviewClaims: ReadLegacyReviewClaims(conn),
            WorkQueueItems: ReadLegacyWorkQueue(conn),
            MetaEntries: ReadLegacyMeta(conn)));
    }

    private static List<LegacyMessageRow> ReadLegacyMessages(SqliteConnection conn)
    {
        if (!TableExists(conn, TableMessages))
        {
            return [];
        }

        var cols = GetTableColumns(conn, TableMessages);
        var hasWorkspace = cols.Contains(ColWorkspaceId);
        var hasState = cols.Contains("state");
        var hasWakeOk = cols.Contains("wake_ok");
        var hasWakeState = cols.Contains("wake_state");
        var hasWakeExpires = cols.Contains("wake_attempt_expires_at_utc");

        var rows = new List<LegacyMessageRow>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT id, created_at_utc, sender, recipient, topic, body,
                   pr_number, issue_number, priority, reply_to_id, status, acked_by, acked_at_utc
            FROM messages
            ORDER BY id ASC
            """;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var id = reader.GetInt64(0);
            var createdAt = GetStringOrDefault(reader, 1, FormatUtc(DateTimeOffset.UtcNow));
            var sender = GetStringOrDefault(reader, 2, "unknown");
            var recipient = GetStringOrDefault(reader, 3, AllRecipient);
            var topic = GetStringOrDefault(reader, 4, "legacy");
            var body = GetStringOrDefault(reader, 5, string.Empty);
            var pr = GetNullableInt32(reader, 6);
            var issue = GetNullableInt32(reader, 7);
            var priority = NormalizePriority(GetNullableString(reader, 8));
            var replyTo = GetNullableInt64(reader, 9);
            var status = NormalizeMessageStatus(GetNullableString(reader, 10));
            var ackedBy = GetNullableString(reader, 11);
            var ackedAt = GetNullableString(reader, 12);

            rows.Add(new LegacyMessageRow(
                Id: id,
                WorkspaceId: SwarmBusConstants.DefaultWorkspaceId,
                CreatedAtUtc: createdAt,
                Sender: sender,
                Recipient: recipient,
                Topic: topic,
                Body: body,
                PrNumber: pr,
                IssueNumber: issue,
                Priority: priority,
                ReplyToId: replyTo,
                Status: status,
                AckedBy: ackedBy,
                AckedAtUtc: ackedAt,
                State: null,
                WakeOk: null,
                WakeState: null,
                WakeAttemptExpiresAtUtc: null));
        }

        if (!hasWorkspace && !hasState && !hasWakeOk && !hasWakeState && !hasWakeExpires)
        {
            return rows;
        }

        return EnrichLegacyMessageExtras(conn, rows, hasWorkspace, hasState, hasWakeOk, hasWakeState, hasWakeExpires);
    }

    private static List<LegacyMessageRow> EnrichLegacyMessageExtras(
        SqliteConnection conn,
        List<LegacyMessageRow> baseRows,
        bool hasWorkspace,
        bool hasState,
        bool hasWakeOk,
        bool hasWakeState,
        bool hasWakeExpires)
    {
        var extrasById = new Dictionary<long, (string WorkspaceId, string? State, int? WakeOk, string? WakeState, string? WakeExpires)>();
        var workspaceById = hasWorkspace ? ReadColumnById(conn, "SELECT id, workspace_id FROM messages") : [];
        var stateById = hasState ? ReadColumnById(conn, "SELECT id, state FROM messages") : [];
        var wakeOkById = hasWakeOk ? ReadNullableIntColumnById(conn, "SELECT id, wake_ok FROM messages") : [];
        var wakeStateById = hasWakeState ? ReadColumnById(conn, "SELECT id, wake_state FROM messages") : [];
        var wakeExpiresById = hasWakeExpires ? ReadColumnById(conn, "SELECT id, wake_attempt_expires_at_utc FROM messages") : [];

        foreach (var row in baseRows)
        {
            var ws = workspaceById.TryGetValue(row.Id, out var rawWs)
                ? PathPatternMatcher.NormalizeWorkspaceId(rawWs)
                : SwarmBusConstants.DefaultWorkspaceId;
            stateById.TryGetValue(row.Id, out var st);
            int? wakeOk = wakeOkById.TryGetValue(row.Id, out var wOk) ? wOk : null;
            wakeStateById.TryGetValue(row.Id, out var rawWakeState);
            wakeExpiresById.TryGetValue(row.Id, out var rawWakeExp);
            extrasById[row.Id] = (ws, st, wakeOk, NormalizeLegacyWakeState(rawWakeState, wakeOk), rawWakeExp);
        }

        return baseRows
            .Select(r =>
            {
                var ex = extrasById[r.Id];
                return r with
                {
                    WorkspaceId = ex.WorkspaceId,
                    State = ex.State,
                    WakeOk = ex.WakeOk,
                    WakeState = ex.WakeState,
                    WakeAttemptExpiresAtUtc = ex.WakeExpires
                };
            })
            .ToList();
    }

    private static Dictionary<long, string?> ReadColumnById(SqliteConnection conn, string sql)
    {
        var map = new Dictionary<long, string?>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            map[reader.GetInt64(0)] = GetNullableString(reader, 1);
        }

        return map;
    }

    private static Dictionary<long, int?> ReadNullableIntColumnById(SqliteConnection conn, string sql)
    {
        var map = new Dictionary<long, int?>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            map[reader.GetInt64(0)] = GetNullableInt32(reader, 1);
        }

        return map;
    }

    private static List<LegacyLockRow> ReadLegacyLocks(SqliteConnection conn)
    {
        if (!TableExists(conn, TableLocks))
        {
            return [];
        }

        var hasWorkspace = GetTableColumns(conn, TableLocks).Contains(ColWorkspaceId);
        var rows = new List<LegacyLockRow>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = hasWorkspace
            ? "SELECT workspace_id, agent, path_pattern, pr_number, claimed_at_utc, expires_at_utc FROM locks ORDER BY id ASC"
            : "SELECT 'default', agent, path_pattern, pr_number, claimed_at_utc, expires_at_utc FROM locks ORDER BY id ASC";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var agent = GetNullableString(reader, 1);
            var pathPattern = GetNullableString(reader, 2);
            if (string.IsNullOrWhiteSpace(agent) || string.IsNullOrWhiteSpace(pathPattern))
            {
                continue;
            }

            rows.Add(new LegacyLockRow(
                WorkspaceId: PathPatternMatcher.NormalizeWorkspaceId(GetNullableString(reader, 0)),
                Agent: agent.Trim(),
                PathPattern: pathPattern.Trim(),
                PrNumber: GetNullableInt32(reader, 3),
                ClaimedAtUtc: GetStringOrDefault(reader, 4, FormatUtc(DateTimeOffset.UtcNow)),
                ExpiresAtUtc: GetStringOrDefault(reader, 5, FormatUtc(DateTimeOffset.UtcNow))));
        }

        return rows;
    }

    private static List<LegacyReviewClaimRow> ReadLegacyReviewClaims(SqliteConnection conn)
    {
        if (!TableExists(conn, TableReviewClaims))
        {
            return [];
        }

        var cols = GetTableColumns(conn, TableReviewClaims);
        var hasWorkspace = cols.Contains(ColWorkspaceId);
        var hasStatus = cols.Contains(ColStatus);
        var hasExpires = cols.Contains("expires_at_utc");
        var hasCompleted = cols.Contains("completed_at_utc");

        var rows = new List<LegacyReviewClaimRow>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = hasWorkspace
            ? "SELECT id, workspace_id, pr_number, head_sha, role, agent, claimed_at_utc FROM review_claims ORDER BY id ASC"
            : "SELECT id, 'default', pr_number, head_sha, role, agent, claimed_at_utc FROM review_claims ORDER BY id ASC";
        var statusById = hasStatus ? ReadColumnById(conn, "SELECT id, status FROM review_claims") : [];
        var expiresById = hasExpires ? ReadColumnById(conn, "SELECT id, expires_at_utc FROM review_claims") : [];
        var completedById = hasCompleted ? ReadColumnById(conn, "SELECT id, completed_at_utc FROM review_claims") : [];

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var id = reader.GetInt64(0);
            var prNumber = reader.GetInt32(2);
            var rawSha = GetNullableString(reader, 3);
            var rawRole = GetNullableString(reader, 4);
            var agent = GetNullableString(reader, 5);
            if (prNumber <= 0 || string.IsNullOrWhiteSpace(rawSha) || !HeadShaRegex().IsMatch(rawSha.Trim()) ||
                string.IsNullOrWhiteSpace(rawRole) || string.IsNullOrWhiteSpace(agent))
            {
                continue;
            }

            statusById.TryGetValue(id, out var rawStatus);
            expiresById.TryGetValue(id, out var expiresAt);
            completedById.TryGetValue(id, out var completedAt);

            rows.Add(new LegacyReviewClaimRow(
                WorkspaceId: PathPatternMatcher.NormalizeWorkspaceId(GetNullableString(reader, 1)),
                PrNumber: prNumber,
                HeadSha: rawSha.Trim().ToLowerInvariant(),
                Role: rawRole.Trim().ToLowerInvariant(),
                Agent: agent.Trim(),
                ClaimedAtUtc: GetStringOrDefault(reader, 6, FormatUtc(DateTimeOffset.UtcNow)),
                Status: NormalizeLegacyReviewClaimStatus(rawStatus),
                ExpiresAtUtc: expiresAt,
                CompletedAtUtc: completedAt));
        }

        return rows;
    }

    private static List<LegacyWorkQueueRow> ReadLegacyWorkQueue(SqliteConnection conn)
    {
        if (!TableExists(conn, TableWorkQueue))
        {
            return [];
        }

        var hasWorkspace = GetTableColumns(conn, TableWorkQueue).Contains(ColWorkspaceId);
        var rows = new List<LegacyWorkQueueRow>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = hasWorkspace
            ? "SELECT workspace_id, issue_number, title, priority, risk_class, status, claimed_by, claimed_at_utc, lease_expires_utc, updated_at_utc FROM work_queue ORDER BY id ASC"
            : "SELECT 'default', issue_number, title, priority, risk_class, status, claimed_by, claimed_at_utc, lease_expires_utc, updated_at_utc FROM work_queue ORDER BY id ASC";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var issueNumber = reader.GetInt32(1);
            var title = GetNullableString(reader, 2);
            if (issueNumber <= 0 || string.IsNullOrWhiteSpace(title))
            {
                continue;
            }

            var normalizedStatus = NormalizeLegacyWorkQueueStatus(GetNullableString(reader, 5));
            var claimedBy = normalizedStatus == StatusClaimed ? GetNullableString(reader, 6) : null;
            var claimedAt = normalizedStatus == StatusClaimed ? GetNullableString(reader, 7) : null;
            var leaseExpires = normalizedStatus == StatusClaimed ? GetNullableString(reader, 8) : null;

            rows.Add(new LegacyWorkQueueRow(
                WorkspaceId: PathPatternMatcher.NormalizeWorkspaceId(GetNullableString(reader, 0)),
                IssueNumber: issueNumber,
                Title: title.Trim(),
                Priority: reader.IsDBNull(3) ? 2 : reader.GetInt32(3),
                RiskClass: GetNullableString(reader, 4),
                Status: normalizedStatus,
                ClaimedBy: claimedBy,
                ClaimedAtUtc: claimedAt,
                LeaseExpiresUtc: leaseExpires,
                UpdatedAtUtc: GetStringOrDefault(reader, 9, FormatUtc(DateTimeOffset.UtcNow))));
        }

        return rows;
    }

    private static List<LegacyMetaRow> ReadLegacyMeta(SqliteConnection conn)
    {
        if (!TableExists(conn, TableMeta))
        {
            return [];
        }

        var hasWorkspace = GetTableColumns(conn, TableMeta).Contains(ColWorkspaceId);
        var rows = new List<LegacyMetaRow>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = hasWorkspace
            ? "SELECT workspace_id, key, value, updated_at_utc FROM meta"
            : "SELECT 'default', key, value, updated_at_utc FROM meta";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var key = GetNullableString(reader, 1);
            var value = GetNullableString(reader, 2);
            if (string.IsNullOrWhiteSpace(key) || value is null)
            {
                continue;
            }

            rows.Add(new LegacyMetaRow(
                WorkspaceId: PathPatternMatcher.NormalizeWorkspaceId(GetNullableString(reader, 0)),
                Key: key.Trim(),
                Value: value,
                UpdatedAtUtc: GetStringOrDefault(reader, 3, FormatUtc(DateTimeOffset.UtcNow))));
        }

        return rows;
    }

    private static LegacyImportResult WriteLegacyImportInTx(SqliteConnection connection, string sourcePath, LegacyDbSnapshot snapshot)
    {
        var msgCount = ImportMessagesInTx(connection, snapshot.Messages);
        var lockCount = ImportLocksInTx(connection, snapshot.Locks);
        var claimCount = ImportReviewClaimsInTx(connection, snapshot.ReviewClaims);
        var queueCount = ImportWorkQueueInTx(connection, snapshot.WorkQueueItems);
        var metaCount = ImportMetaInTx(connection, snapshot.MetaEntries);

        return new LegacyImportResult(
            SourcePath: sourcePath,
            MessagesImported: msgCount,
            LocksImported: lockCount,
            ReviewClaimsImported: claimCount,
            WorkQueueItemsImported: queueCount,
            MetaEntriesImported: metaCount);
    }

    private static int ImportMessagesInTx(SqliteConnection connection, IReadOnlyList<LegacyMessageRow> rows)
    {
        var imported = 0;
        foreach (var row in rows)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO messages (
                    id, workspace_id, created_at_utc, sender, recipient, topic, body,
                    pr_number, issue_number, priority, reply_to_id, status,
                    acked_by, acked_at_utc, state, wake_ok, wake_state, wake_attempt_expires_at_utc)
                VALUES (
                    @id, @workspaceId, @createdAt, @sender, @recipient, @topic, @body,
                    @prNumber, @issueNumber, @priority, @replyToId, @status,
                    @ackedBy, @ackedAt, @state, @wakeOk, @wakeState, @wakeExpires)
                ON CONFLICT(id) DO NOTHING
                """;
            cmd.Parameters.AddWithValue(ParamId, row.Id);
            cmd.Parameters.AddWithValue(ParamWorkspaceId, row.WorkspaceId);
            cmd.Parameters.AddWithValue(ParamCreatedAt, row.CreatedAtUtc);
            cmd.Parameters.AddWithValue(ParamSender, row.Sender);
            cmd.Parameters.AddWithValue(ParamRecipient, row.Recipient);
            cmd.Parameters.AddWithValue(ParamTopic, row.Topic);
            cmd.Parameters.AddWithValue(ParamBody, row.Body);
            cmd.Parameters.AddWithValue(ParamPrNumber, (object?)row.PrNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamIssueNumber, (object?)row.IssueNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamPriority, row.Priority);
            cmd.Parameters.AddWithValue(ParamReplyToId, (object?)row.ReplyToId ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamStatus, row.Status);
            cmd.Parameters.AddWithValue(ParamAckedBy, (object?)row.AckedBy ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamAckedAt, (object?)row.AckedAtUtc ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamState, (object?)row.State ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamWakeOk, (object?)row.WakeOk ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamWakeState, (object?)row.WakeState ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamWakeExpires, (object?)row.WakeAttemptExpiresAtUtc ?? DBNull.Value);
            imported += cmd.ExecuteNonQuery();
        }

        return imported;
    }

    private static int ImportLocksInTx(SqliteConnection connection, IReadOnlyList<LegacyLockRow> rows)
    {
        var imported = 0;
        foreach (var row in rows)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO locks (workspace_id, agent, path_pattern, pr_number, claimed_at_utc, expires_at_utc)
                VALUES (@workspaceId, @agent, @pathPattern, @prNumber, @claimedAt, @expiresAt)
                ON CONFLICT(workspace_id, path_pattern) DO NOTHING
                """;
            cmd.Parameters.AddWithValue(ParamWorkspaceId, row.WorkspaceId);
            cmd.Parameters.AddWithValue(ParamAgent, row.Agent);
            cmd.Parameters.AddWithValue(ParamPathPattern, row.PathPattern);
            cmd.Parameters.AddWithValue(ParamPrNumber, (object?)row.PrNumber ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamClaimedAt, row.ClaimedAtUtc);
            cmd.Parameters.AddWithValue(ParamExpiresAt, row.ExpiresAtUtc);
            imported += cmd.ExecuteNonQuery();
        }

        return imported;
    }

    private static int ImportReviewClaimsInTx(SqliteConnection connection, IReadOnlyList<LegacyReviewClaimRow> rows)
    {
        var imported = 0;
        foreach (var row in rows)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO review_claims (workspace_id, pr_number, head_sha, role, agent, claimed_at_utc, status, expires_at_utc, completed_at_utc)
                VALUES (@workspaceId, @prNumber, @headSha, @role, @agent, @claimedAt, @status, @expiresAt, @completedAt)
                ON CONFLICT(workspace_id, pr_number, head_sha, role) DO NOTHING
                """;
            cmd.Parameters.AddWithValue(ParamWorkspaceId, row.WorkspaceId);
            cmd.Parameters.AddWithValue(ParamPrNumber, row.PrNumber);
            cmd.Parameters.AddWithValue(ParamHeadSha, row.HeadSha);
            cmd.Parameters.AddWithValue(ParamRole, row.Role);
            cmd.Parameters.AddWithValue(ParamAgent, row.Agent);
            cmd.Parameters.AddWithValue(ParamClaimedAt, row.ClaimedAtUtc);
            cmd.Parameters.AddWithValue(ParamStatus, row.Status);
            cmd.Parameters.AddWithValue(ParamExpiresAt, (object?)row.ExpiresAtUtc ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamCompletedAt, (object?)row.CompletedAtUtc ?? DBNull.Value);
            imported += cmd.ExecuteNonQuery();
        }

        return imported;
    }

    private static int ImportWorkQueueInTx(SqliteConnection connection, IReadOnlyList<LegacyWorkQueueRow> rows)
    {
        var imported = 0;
        foreach (var row in rows)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO work_queue (workspace_id, issue_number, title, priority, risk_class, status, claimed_by, claimed_at_utc, lease_expires_utc, updated_at_utc)
                VALUES (@workspaceId, @issueNumber, @title, @priority, @riskClass, @status, @claimedBy, @claimedAt, @leaseExpires, @updatedAt)
                ON CONFLICT(workspace_id, issue_number) DO NOTHING
                """;
            cmd.Parameters.AddWithValue(ParamWorkspaceId, row.WorkspaceId);
            cmd.Parameters.AddWithValue(ParamIssueNumber, row.IssueNumber);
            cmd.Parameters.AddWithValue(ParamTitle, row.Title);
            cmd.Parameters.AddWithValue(ParamPriority, row.Priority);
            cmd.Parameters.AddWithValue(ParamRiskClass, (object?)row.RiskClass ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamStatus, row.Status);
            cmd.Parameters.AddWithValue(ParamClaimedBy, (object?)row.ClaimedBy ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamClaimedAt, (object?)row.ClaimedAtUtc ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamLeaseExpires, (object?)row.LeaseExpiresUtc ?? DBNull.Value);
            cmd.Parameters.AddWithValue(ParamUpdatedAt, row.UpdatedAtUtc);
            imported += cmd.ExecuteNonQuery();
        }

        return imported;
    }

    private static int ImportMetaInTx(SqliteConnection connection, IReadOnlyList<LegacyMetaRow> rows)
    {
        var imported = 0;
        foreach (var row in rows)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO meta (workspace_id, key, value, updated_at_utc)
                VALUES (@workspaceId, @key, @value, @updatedAt)
                ON CONFLICT(workspace_id, key) DO NOTHING
                """;
            cmd.Parameters.AddWithValue(ParamWorkspaceId, row.WorkspaceId);
            cmd.Parameters.AddWithValue(ParamKey, row.Key);
            cmd.Parameters.AddWithValue(ParamValue, row.Value);
            cmd.Parameters.AddWithValue(ParamUpdatedAt, row.UpdatedAtUtc);
            imported += cmd.ExecuteNonQuery();
        }

        return imported;
    }

    private static string NormalizeMessageStatus(string? rawStatus)
    {
        var candidate = rawStatus?.Trim().ToLowerInvariant();
        return candidate is StatusUnread or StatusAcked ? candidate : StatusUnread;
    }

    private static string? NormalizeLegacyWakeState(string? rawWakeState, int? wakeOk)
    {
        var candidate = rawWakeState?.Trim().ToLowerInvariant();
        if (candidate is WakeStatePending or WakeStateOk or WakeStateFailed)
        {
            return candidate;
        }

        return wakeOk switch
        {
            1 => WakeStateOk,
            0 => WakeStateFailed,
            _ => null
        };
    }

    private static string NormalizeLegacyReviewClaimStatus(string? rawStatus)
    {
        var candidate = rawStatus?.Trim().ToLowerInvariant();
        return candidate is StatusClaimed or StatusCompleted or StatusReleased
            ? candidate
            : StatusReleased;
    }

    private static string NormalizeLegacyWorkQueueStatus(string? rawStatus)
    {
        var candidate = rawStatus?.Trim().ToLowerInvariant();
        return candidate switch
        {
            StatusQueued or StatusClaimed or StatusDone or StatusRemoved => candidate,
            "completed" or "closed" => StatusDone,
            "deleted" or "cancelled" => StatusRemoved,
            _ => StatusQueued
        };
    }

    private static SendTxInput BuildSendTxInput(SwarmSendRequest request)
    {
        var sender = RequireNonEmpty(request.From, "bus_send requires non-empty \"from\".", nameof(request));
        var recipient = RequireNonEmpty(request.To, "bus_send requires non-empty \"to\".", nameof(request));
        var topic = RequireNonEmpty(request.Topic, "bus_send requires non-empty \"topic\".", nameof(request));
        var body = RequireNonEmpty(request.Body, "bus_send requires non-empty \"body\".", nameof(request));

        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var state = string.IsNullOrWhiteSpace(request.State) ? null : request.State.Trim();
        var priority = NormalizePriority(request.Priority);
        var recLower = recipient.ToLowerInvariant();
        var wakeTargetMatches = request.Wake && (recLower.Contains("codex", StringComparison.Ordinal) || recLower == AllRecipient);

        return new SendTxInput(
            WorkspaceId: workspaceId,
            Sender: sender,
            Recipient: recipient,
            Topic: topic,
            Body: body,
            PrNumber: request.PrNumber,
            IssueNumber: request.IssueNumber,
            ReplyToId: request.ReplyToId,
            State: state,
            Priority: priority,
            WakeTargetMatches: wakeTargetMatches,
            HasExplicitCodexThreadId: !string.IsNullOrWhiteSpace(request.CodexThreadId));
    }

    private SwarmSendResult RunWakeSync(SendTxResult txResult, SendTxInput input, string? rawCodexThreadId, bool isRetry) =>
        isRetry
            ? Task.Run(() => HandleRetryWakeAsync(txResult, input, rawCodexThreadId, CancellationToken.None)).GetAwaiter().GetResult()
            : Task.Run(() => HandleInsertedMessageAsync(txResult, input, rawCodexThreadId, CancellationToken.None)).GetAwaiter().GetResult();

    private static SwarmSendResult CreateDeduplicatedSendResult(SendTxResult txResult, SendTxInput input) =>
        new(
            Ok: true,
            Id: txResult.Id,
            WorkspaceId: input.WorkspaceId,
            State: input.State,
            Deduplicated: true,
            WakeInProgress: txResult.WakeInProgress,
            WakeState: txResult.WakeState);

    private static SwarmSendResult CreatePendingRetrySendResult(SendTxResult txResult, SendTxInput input) =>
        new(
            Ok: true,
            Id: txResult.Id,
            WorkspaceId: input.WorkspaceId,
            State: input.State,
            Deduplicated: true,
            WakeRetried: true,
            WakeInProgress: true,
            WakeState: WakeStatePending);

    private static SwarmSendResult CreateInsertedSendResult(
        SendTxResult txResult,
        SendTxInput input,
        IReadOnlyList<SwarmWakeResult> wakeResults,
        string? wakeState) =>
        new(
            Ok: true,
            Id: txResult.Id,
            WorkspaceId: input.WorkspaceId,
            CreatedAtUtc: txResult.CreatedAt,
            From: input.Sender,
            To: input.Recipient,
            Topic: input.Topic,
            PrNumber: input.PrNumber,
            IssueNumber: input.IssueNumber,
            Priority: input.Priority,
            State: input.State,
            WakeState: wakeState,
            WakeResults: wakeResults);

    private async Task<SwarmSendResult> HandleRetryWakeAsync(
        SendTxResult txResult,
        SendTxInput input,
        string? rawCodexThreadId,
        CancellationToken cancellationToken)
    {
        if (_wakeDispatcher is null)
        {
            return CreatePendingRetrySendResult(txResult, input);
        }

        var context = BuildWakeContext(txResult.Id, input, txResult.Body ?? input.Body, rawCodexThreadId);
        var (wakeRes, fenceApplied) = await DispatchWakeAsync(context, txResult.WakeLeaseToken!, cancellationToken).ConfigureAwait(false);
        var finalState = fenceApplied ? (wakeRes.Ok ? WakeStateOk : WakeStateFailed) : WakeStatePending;

        return new SwarmSendResult(
            Ok: true,
            Id: txResult.Id,
            WorkspaceId: input.WorkspaceId,
            State: input.State,
            Deduplicated: true,
            WakeRetried: true,
            WakeState: finalState,
            WakeResults: [wakeRes]);
    }

    private async Task<SwarmSendResult> HandleInsertedMessageAsync(
        SendTxResult txResult,
        SendTxInput input,
        string? rawCodexThreadId,
        CancellationToken cancellationToken)
    {
        if (!input.WakeTargetMatches || _wakeDispatcher is null)
        {
            return CreateInsertedSendResult(txResult, input, [], input.WakeTargetMatches ? WakeStatePending : null);
        }

        var context = BuildWakeContext(txResult.Id, input, input.Body, rawCodexThreadId);
        var (wakeRes, fenceApplied) = await DispatchWakeAsync(context, txResult.WakeLeaseToken!, cancellationToken).ConfigureAwait(false);
        var finalWakeState = fenceApplied ? (wakeRes.Ok ? WakeStateOk : WakeStateFailed) : WakeStatePending;
        return CreateInsertedSendResult(txResult, input, [wakeRes], finalWakeState);
    }

    private static SwarmWakeContext BuildWakeContext(
        long messageId,
        SendTxInput input,
        string effectiveBody,
        string? rawCodexThreadId)
    {
        var codexThreadId = string.IsNullOrWhiteSpace(rawCodexThreadId) ? null : rawCodexThreadId.Trim();
        var prTag = input.PrNumber.HasValue ? $" (PR #{input.PrNumber.Value})" : string.Empty;
        var wakeMsg = $"[SwarmBus #{messageId}] {input.Sender} -> {input.Recipient}{prTag} [{input.Topic}]: {effectiveBody}";

        return new SwarmWakeContext(
            MessageId: messageId,
            WorkspaceId: input.WorkspaceId,
            Sender: input.Sender,
            Recipient: input.Recipient,
            PrNumber: input.PrNumber,
            Topic: input.Topic,
            Body: effectiveBody,
            CodexThreadId: codexThreadId,
            FormattedMessage: wakeMsg);
    }

    private static (string Agent, int Limit, bool AllWorkspaces, string WorkspaceId, bool AllRecipients) NormalizeInboxQuery(
        string agent,
        int limit,
        string? workspaceId)
    {
        var normalizedAgent = string.IsNullOrWhiteSpace(agent) ? AllRecipient : agent.Trim();
        var clampedLimit = Math.Clamp(limit, 1, 100);
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(workspaceId);
        var includeAllRecipients = normalizedAgent.Equals(AllRecipient, StringComparison.OrdinalIgnoreCase) ||
                                   normalizedAgent == AllWildcard;
        return (normalizedAgent, clampedLimit, allWorkspaces, normalizedWorkspace, includeAllRecipients);
    }

    private static (
        string Agent,
        long SinceId,
        int TimeoutMs,
        int Limit,
        bool AllWorkspaces,
        string WorkspaceId,
        bool AllRecipients) ValidateWaitRequest(SwarmWaitRequest request)
    {
        var normalizedAgent = RequireNonEmpty(request.Agent, "bus_wait requires non-empty \"agent\".", nameof(request));
        var sinceId = Math.Max(0, request.SinceId);
        var timeoutMs = Math.Clamp(request.TimeoutMs, SwarmBusConstants.MinWaitTimeoutMs, SwarmBusConstants.MaxWaitTimeoutMs);
        var clampedLimit = Math.Clamp(request.Limit, 1, 100);
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(request.WorkspaceId);
        var includeAllRecipients = !request.MentionsOnly &&
                                   (normalizedAgent.Equals(AllRecipient, StringComparison.OrdinalIgnoreCase) ||
                                    normalizedAgent == AllWildcard);

        return (normalizedAgent, sinceId, timeoutMs, clampedLimit, allWorkspaces, normalizedWorkspace, includeAllRecipients);
    }

    private async Task<SwarmWaitResult> WaitForNewMessagesLoopAsync(
        string normalizedAgent,
        long sinceId,
        int timeoutMs,
        int clampedLimit,
        bool allWorkspaces,
        string normalizedWorkspace,
        bool includeAllRecipients,
        CancellationToken cancellationToken)
    {
        var dbDir = Path.GetDirectoryName(_dbPath);
        var dbFileName = Path.GetFileName(_dbPath);
        using var signal = new SemaphoreSlim(0, 1);
        FileSystemWatcher? watcher = null;

        try
        {
            if (!string.IsNullOrEmpty(dbDir) && Directory.Exists(dbDir) && !string.IsNullOrEmpty(dbFileName))
            {
                try
                {
                    watcher = new FileSystemWatcher(dbDir, $"{dbFileName}*")
                    {
                        NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size | NotifyFilters.FileName,
                        IncludeSubdirectories = false
                    };
                    static bool IsShmFile(string? name) =>
                        !string.IsNullOrEmpty(name) && name.EndsWith("-shm", StringComparison.OrdinalIgnoreCase);

                    void OnFsEvent(object _, FileSystemEventArgs e)
                    {
                        if (!IsShmFile(e.Name))
                        {
                            TryReleaseWaitSignal(signal);
                        }
                    }

                    void OnFsRename(object _, RenamedEventArgs e)
                    {
                        if (!IsShmFile(e.Name))
                        {
                            TryReleaseWaitSignal(signal);
                        }
                    }

                    watcher.Changed += OnFsEvent;
                    watcher.Created += OnFsEvent;
                    watcher.Renamed += OnFsRename;
                    watcher.EnableRaisingEvents = true;
                }
                catch (Exception ex) when (ex is IOException or PlatformNotSupportedException or UnauthorizedAccessException)
                {
                    watcher?.Dispose();
                    watcher = null;
                }
            }

            // Re-check immediately after arming FileSystemWatcher to close the race window.
            var (recheck, recheckHasMore) = await ExecuteReadAsync(
                connection => ReadWaitMessagesBatch(
                    connection,
                    allWorkspaces,
                    normalizedWorkspace,
                    includeAllRecipients,
                    normalizedAgent,
                    clampedLimit,
                    sinceId),
                cancellationToken).ConfigureAwait(false);
            if (recheck.Count > 0)
            {
                return new SwarmWaitResult(
                    Ok: true,
                    WorkspaceId: normalizedWorkspace,
                    TimedOut: false,
                    LatestId: Math.Max(sinceId, recheck[^1].Id),
                    Messages: recheck,
                    HasMore: recheckHasMore);
            }

            var deadlineTick = Environment.TickCount64 + timeoutMs;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var remainingMs = (int)Math.Max(0, deadlineTick - Environment.TickCount64);
                if (remainingMs <= 0)
                {
                    break;
                }

                var sliceMs = Math.Min(remainingMs, SwarmBusConstants.WaitFallbackPollMs);
                await signal.WaitAsync(sliceMs, cancellationToken).ConfigureAwait(false);

                var (messages, hasMore) = await ExecuteReadAsync(
                    connection => ReadWaitMessagesBatch(
                        connection,
                        allWorkspaces,
                        normalizedWorkspace,
                        includeAllRecipients,
                        normalizedAgent,
                        clampedLimit,
                        sinceId),
                    cancellationToken).ConfigureAwait(false);
                if (messages.Count > 0)
                {
                    return new SwarmWaitResult(
                        Ok: true,
                        WorkspaceId: normalizedWorkspace,
                        TimedOut: false,
                        LatestId: Math.Max(sinceId, messages[^1].Id),
                        Messages: messages,
                        HasMore: hasMore);
                }
            }

            return new SwarmWaitResult(
                Ok: true,
                WorkspaceId: normalizedWorkspace,
                TimedOut: true,
                LatestId: sinceId,
                Messages: [],
                HasMore: false);
        }
        finally
        {
            if (watcher is not null)
            {
                try
                {
                    watcher.EnableRaisingEvents = false;
                }
                catch
                {
                    // Ignore watcher shutdown error.
                }

                watcher.Dispose();
            }
        }
    }

    private static void TryReleaseWaitSignal(SemaphoreSlim signal)
    {
        try
        {
            if (signal.CurrentCount == 0)
            {
                signal.Release();
            }
        }
        catch (ObjectDisposedException)
        {
            // Wait loop already exited.
        }
        catch (SemaphoreFullException)
        {
            // Signal already queued.
        }
    }

    private static (List<SwarmMessageEnvelope> Messages, bool HasMore) ReadWaitMessagesBatch(
        SqliteConnection connection,
        bool allWorkspaces,
        string workspaceId,
        bool includeAllRecipients,
        string normalizedAgent,
        int clampedLimit,
        long sinceId)
    {
        var rows = ReadInboxMessages(
            connection,
            allWorkspaces,
            workspaceId,
            includeAllRecipients,
            normalizedAgent,
            unreadOnly: true,
            limit: clampedLimit + 1,
            sinceId: sinceId,
            ascending: true);

        var hasMore = rows.Count > clampedLimit;
        if (hasMore)
        {
            rows.RemoveRange(clampedLimit, rows.Count - clampedLimit);
        }

        return (rows, hasMore);
    }

    private static List<SwarmMessageEnvelope> ReadInboxMessages(
        SqliteConnection connection,
        bool allWorkspaces,
        string workspaceId,
        bool includeAllRecipients,
        string normalizedAgent,
        bool unreadOnly,
        int limit,
        long sinceId = 0,
        bool ascending = false)
    {
        using var cmd = connection.CreateCommand();
        var orderDirection = ascending ? "ASC" : "DESC";
        cmd.CommandText = $"""
            SELECT id, workspace_id, created_at_utc, sender, recipient, topic, body,
                   pr_number, issue_number, priority, reply_to_id, status,
                   acked_by, acked_at_utc, state, wake_ok, wake_state, wake_attempt_expires_at_utc
            FROM messages
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
              AND (@allRecipients = 1 OR lower(recipient) = lower(@agent) OR lower(recipient) = 'all')
              AND (@unreadOnly = 0 OR status = 'unread')
              AND (@sinceId <= 0 OR id > @sinceId)
            ORDER BY id {orderDirection}
            LIMIT @limit
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamAllRecipients, includeAllRecipients ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamAgent, normalizedAgent);
        cmd.Parameters.AddWithValue(ParamUnreadOnly, unreadOnly ? 1 : 0);
        cmd.Parameters.AddWithValue("@sinceId", sinceId);
        cmd.Parameters.AddWithValue(ParamLimit, limit);
        return ReadMessages(cmd);
    }

    private (string WorkspaceId, string OrigSender, string OrigTopic, int? PrNumber, int? IssueNumber, DateTimeOffset AckedAt) MarkMessageAckedInTx(
        SqliteConnection connection,
        long messageId,
        string normalizedAgent)
    {
        using var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = """
            SELECT id, workspace_id, sender, topic, pr_number, issue_number
            FROM messages
            WHERE id = @id
            """;
        selectCmd.Parameters.AddWithValue(ParamId, messageId);

        using var reader = selectCmd.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Message #{messageId} not found.");
        }

        var workspaceId = GetStringOrDefault(reader, 1, SwarmBusConstants.DefaultWorkspaceId);
        var origSender = reader.GetString(2);
        var origTopic = reader.GetString(3);
        var prNumber = GetNullableInt32(reader, 4);
        var issueNumber = GetNullableInt32(reader, 5);
        reader.Close();

        var ackedAt = _timeProvider.GetUtcNow();
        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = """
            UPDATE messages
            SET status = 'acked', acked_by = @agent, acked_at_utc = @ackedAt
            WHERE id = @id
            """;
        updateCmd.Parameters.AddWithValue(ParamAgent, normalizedAgent);
        updateCmd.Parameters.AddWithValue(ParamAckedAt, FormatUtc(ackedAt));
        updateCmd.Parameters.AddWithValue(ParamId, messageId);
        updateCmd.ExecuteNonQuery();

        return (workspaceId, origSender, origTopic, prNumber, issueNumber, ackedAt);
    }

    private static (string Agent, string WorkspaceId, List<string> CleanPaths, int TtlMinutes) ValidateLockClaimRequest(LockClaimRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var agent = RequireNonEmpty(request.Agent, "bus_locks claim requires non-empty \"agent\".", nameof(request));
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var cleanPaths = (request.Paths ?? [])
            .Select(PathPatternMatcher.CanonicalizeForStorage)
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (cleanPaths.Count == 0)
        {
            throw new ArgumentException("bus_locks claim requires non-empty \"paths\".", nameof(request));
        }

        var ttlMinutes = Math.Clamp(request.TtlMinutes, SwarmBusConstants.MinLockTtlMinutes, SwarmBusConstants.MaxLockTtlMinutes);
        return (agent, workspaceId, cleanPaths, ttlMinutes);
    }

    private static (string Agent, string WorkspaceId, List<string> CleanPaths, int TtlMinutes, int MessageLimit) ValidateStartWorkRequest(
        SwarmStartWorkRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var agent = RequireNonEmpty(request.Agent, "bus_start_work requires non-empty \"agent\".", nameof(request));
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var cleanPaths = (request.Paths ?? [])
            .Select(PathPatternMatcher.CanonicalizeForStorage)
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        var ttlMinutes = Math.Clamp(request.TtlMinutes, SwarmBusConstants.MinLockTtlMinutes, SwarmBusConstants.MaxLockTtlMinutes);
        var messageLimit = Math.Clamp(request.MessageLimit, 1, 100);
        return (agent, workspaceId, cleanPaths, ttlMinutes, messageLimit);
    }

    private SwarmStartWorkResult ExecuteStartWorkInTx(
        SqliteConnection connection,
        string agent,
        string workspaceId,
        List<string> cleanPaths,
        int? prNumber,
        int ttlMinutes,
        int messageLimit)
    {
        bool ok;
        IReadOnlyList<string> claimedPaths;
        IReadOnlyList<LockConflict> conflicts;
        IReadOnlyList<PathLockLease> locks;

        if (cleanPaths.Count > 0)
        {
            var claimResult = ClaimLocksInTx(connection, agent, workspaceId, cleanPaths, prNumber, ttlMinutes);
            ok = claimResult.Ok;
            claimedPaths = claimResult.ClaimedPaths;
            conflicts = claimResult.Conflicts;
            locks = claimResult.Locks;
        }
        else
        {
            PruneExpiredLocksInTx(connection, _timeProvider.GetUtcNow());
            ok = true;
            claimedPaths = [];
            conflicts = [];
            locks = ReadLocksInWorkspace(connection, workspaceId);
        }

        var includeAllRecipients = agent.Equals(AllRecipient, StringComparison.OrdinalIgnoreCase) || agent == AllWildcard;
        var unreadMessages = ReadInboxMessages(
            connection,
            allWorkspaces: false,
            workspaceId,
            includeAllRecipients,
            agent,
            unreadOnly: true,
            messageLimit);

        var foreignLocks = locks
            .Where(l => !string.Equals(l.Agent, agent, StringComparison.OrdinalIgnoreCase))
            .ToList();

        return new SwarmStartWorkResult(
            Ok: ok,
            WorkspaceId: workspaceId,
            ClaimedPaths: claimedPaths,
            Conflicts: conflicts,
            Locks: locks,
            UnreadMessages: unreadMessages,
            ForeignLocks: foreignLocks);
    }

    private LockClaimResult ClaimLocksInTx(
        SqliteConnection connection,
        string agent,
        string workspaceId,
        List<string> cleanPaths,
        int? prNumber,
        int ttlMinutes)
    {
        var now = _timeProvider.GetUtcNow();
        var nowIso = FormatUtc(now);
        var expiresIso = FormatUtc(now.AddMinutes(ttlMinutes));

        PruneExpiredLocksInTx(connection, now);

        var existingLocks = ReadLocksInWorkspace(connection, workspaceId);
        var conflicts = FindLockConflicts(cleanPaths, existingLocks, agent, workspaceId);
        if (conflicts.Count > 0)
        {
            return new LockClaimResult(
                Ok: false,
                WorkspaceId: workspaceId,
                ClaimedPaths: [],
                Conflicts: conflicts,
                Locks: existingLocks);
        }

        foreach (var reqPath in cleanPaths)
        {
            UpsertLockPathInTx(connection, existingLocks, new LockUpsertEntry(
                WorkspaceId: workspaceId,
                Agent: agent,
                PathPattern: reqPath,
                PrNumber: prNumber,
                ClaimedAtIso: nowIso,
                ExpiresAtIso: expiresIso));
        }

        return new LockClaimResult(
            Ok: true,
            WorkspaceId: workspaceId,
            ClaimedPaths: cleanPaths,
            Conflicts: [],
            Locks: ReadLocksInWorkspace(connection, workspaceId));
    }

    private static (string? Agent, HashSet<string> CleanPaths, string WorkspaceId) ValidateLockReleaseRequest(LockReleaseRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var agent = string.IsNullOrWhiteSpace(request.Agent) ? null : request.Agent.Trim();
        var cleanPaths = (request.Paths ?? [])
            .Select(PathPatternMatcher.NormalizePath)
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .ToHashSet(StringComparer.Ordinal);
        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);

        if (agent is null && cleanPaths.Count == 0 && !request.PrNumber.HasValue)
        {
            throw new ArgumentException("bus_locks release requires \"agent\", \"paths\", or \"prNumber\".", nameof(request));
        }

        return (agent, cleanPaths, workspaceId);
    }

    private LockReleaseResult ReleaseLocksInTx(
        SqliteConnection connection,
        string? agent,
        HashSet<string> cleanPaths,
        int? prNumber,
        string workspaceId)
    {
        PruneExpiredLocksInTx(connection, _timeProvider.GetUtcNow());

        var candidates = ReadLocksInWorkspace(connection, workspaceId);
        var idsToDelete = candidates
            .Where(c => MatchesReleaseFilter(c, agent, prNumber, cleanPaths))
            .Select(c => c.Id)
            .ToList();

        DeleteLocksByIdInTx(connection, idsToDelete);

        return new LockReleaseResult(
            Ok: true,
            WorkspaceId: workspaceId,
            RemovedCount: idsToDelete.Count,
            RemainingLocks: ReadLocksInWorkspace(connection, workspaceId));
    }

    private IReadOnlyList<PathLockLease> ListLocksInTx(SqliteConnection connection, bool allWorkspaces, string normalizedWorkspace)
    {
        PruneExpiredLocksInTx(connection, _timeProvider.GetUtcNow());
        return allWorkspaces
            ? ReadAllLocks(connection)
            : ReadLocksInWorkspace(connection, normalizedWorkspace);
    }

    private static List<LockConflict> FindLockConflicts(
        IReadOnlyList<string> cleanPaths,
        IReadOnlyList<PathLockLease> existingLocks,
        string agent,
        string workspaceId)
    {
        return cleanPaths
            .SelectMany(reqPath => existingLocks
                .Where(ex => !ex.Agent.Equals(agent, StringComparison.OrdinalIgnoreCase) &&
                             PathPatternMatcher.LocksOverlap(workspaceId, reqPath, ex.WorkspaceId, ex.PathPattern))
                .Select(ex => new LockConflict(
                    RequestedPath: reqPath,
                    ConflictingAgent: ex.Agent,
                    ConflictingPath: ex.PathPattern,
                    ConflictingPr: ex.PrNumber,
                    ExpiresAtUtc: ex.ExpiresAtUtc,
                    WorkspaceId: workspaceId)))
            .ToList();
    }

    private static void UpsertLockPathInTx(
        SqliteConnection connection,
        IReadOnlyList<PathLockLease> existingLocks,
        LockUpsertEntry entry)
    {
        var normalizedReq = PathPatternMatcher.NormalizePath(entry.PathPattern);
        var staleIds = existingLocks
            .Where(l => string.Equals(PathPatternMatcher.NormalizePath(l.PathPattern), normalizedReq, StringComparison.Ordinal) &&
                        !string.Equals(l.PathPattern, entry.PathPattern, StringComparison.Ordinal))
            .Select(l => l.Id)
            .ToList();

        DeleteLocksByIdInTx(connection, staleIds);

        using var upsert = connection.CreateCommand();
        upsert.CommandText = """
            INSERT INTO locks (workspace_id, agent, path_pattern, pr_number, claimed_at_utc, expires_at_utc)
            VALUES (@workspaceId, @agent, @pathPattern, @prNumber, @claimedAt, @expiresAt)
            ON CONFLICT(workspace_id, path_pattern) DO UPDATE SET
                agent = excluded.agent,
                pr_number = excluded.pr_number,
                claimed_at_utc = excluded.claimed_at_utc,
                expires_at_utc = excluded.expires_at_utc
            """;
        upsert.Parameters.AddWithValue(ParamWorkspaceId, entry.WorkspaceId);
        upsert.Parameters.AddWithValue(ParamAgent, entry.Agent);
        upsert.Parameters.AddWithValue(ParamPathPattern, entry.PathPattern);
        upsert.Parameters.AddWithValue(ParamPrNumber, (object?)entry.PrNumber ?? DBNull.Value);
        upsert.Parameters.AddWithValue(ParamClaimedAt, entry.ClaimedAtIso);
        upsert.Parameters.AddWithValue(ParamExpiresAt, entry.ExpiresAtIso);
        upsert.ExecuteNonQuery();
    }

    private static bool MatchesReleaseFilter(
        PathLockLease candidate,
        string? agent,
        int? prNumber,
        HashSet<string> cleanPaths)
    {
        if (agent is not null && !candidate.Agent.Equals(agent, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (prNumber.HasValue && candidate.PrNumber != prNumber.Value)
        {
            return false;
        }

        return cleanPaths.Count == 0 || cleanPaths.Contains(PathPatternMatcher.NormalizePath(candidate.PathPattern));
    }

    private static void DeleteLocksByIdInTx(SqliteConnection connection, IReadOnlyList<long> ids)
    {
        foreach (var id in ids)
        {
            using var delCmd = connection.CreateCommand();
            delCmd.CommandText = DeleteLockByIdSql;
            delCmd.Parameters.AddWithValue(ParamId, id);
            delCmd.ExecuteNonQuery();
        }
    }

    private static ParsedQueueUpsert ValidateQueueUpsertRequest(WorkQueueUpsertRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.IssueNumber <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "bus_queue upsert requires positive integer \"issue_number\".");
        }

        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var incomingStatus = string.IsNullOrWhiteSpace(request.Status) ? StatusQueued : request.Status.Trim().ToLowerInvariant();
        if (!SwarmBusConstants.ValidQueueStatuses.Contains(incomingStatus))
        {
            throw new ArgumentException($"Invalid work_queue status '{incomingStatus}'. Expected: queued, claimed, done, removed.", nameof(request));
        }

        var rawTitle = string.IsNullOrWhiteSpace(request.Title) ? null : request.Title.Trim();
        var isTerminalWithoutTitle = rawTitle is null && incomingStatus is StatusDone or StatusRemoved;
        if (rawTitle is null && !isTerminalWithoutTitle)
        {
            throw new ArgumentException("bus_queue upsert requires non-empty \"title\".", nameof(request));
        }

        var priority = request.Priority ?? 2;
        var riskClass = string.IsNullOrWhiteSpace(request.RiskClass) ? null : request.RiskClass.Trim().ToUpperInvariant();
        return new ParsedQueueUpsert(workspaceId, request.IssueNumber, rawTitle, priority, riskClass, incomingStatus, isTerminalWithoutTitle);
    }

    private WorkQueueMutationResult? ExecuteQueueUpsertInTx(SqliteConnection connection, ParsedQueueUpsert parsed)
    {
        var nowIso = FormatUtc(_timeProvider.GetUtcNow());
        if (parsed.IsTerminalWithoutTitle)
        {
            return TryMarkDoneOrRemovedWithoutTitleInTx(connection, parsed.WorkspaceId, parsed.IssueNumber, parsed.Status, nowIso);
        }

        return UpsertQueueItemInTx(connection, parsed, nowIso);
    }

    private static WorkQueueMutationResult? TryMarkDoneOrRemovedWithoutTitleInTx(
        SqliteConnection connection,
        string workspaceId,
        int issueNumber,
        string incomingStatus,
        string nowIso)
    {
        var existing = ReadWorkQueueItem(connection, workspaceId, issueNumber);
        if (existing is null)
        {
            return null;
        }

        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = """
            UPDATE work_queue
            SET status = @status,
                claimed_by = NULL,
                claimed_at_utc = NULL,
                lease_expires_utc = NULL,
                updated_at_utc = @updatedAt
            WHERE workspace_id = @workspaceId AND issue_number = @issueNumber
            """;
        updateCmd.Parameters.AddWithValue(ParamStatus, incomingStatus);
        updateCmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        updateCmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        updateCmd.Parameters.AddWithValue(ParamIssueNumber, issueNumber);
        updateCmd.ExecuteNonQuery();

        return new WorkQueueMutationResult(
            Ok: true,
            Action: ActionUpsert,
            WorkspaceId: workspaceId,
            IssueNumber: issueNumber,
            Changed: true,
            Item: ReadWorkQueueItem(connection, workspaceId, issueNumber));
    }

    private static WorkQueueMutationResult UpsertQueueItemInTx(
        SqliteConnection connection,
        ParsedQueueUpsert parsed,
        string nowIso)
    {
        using var upsertCmd = connection.CreateCommand();
        upsertCmd.CommandText = """
            INSERT INTO work_queue (workspace_id, issue_number, title, priority, risk_class, status, updated_at_utc)
            VALUES (@workspaceId, @issueNumber, @title, @priority, @riskClass, @status, @updatedAt)
            ON CONFLICT(workspace_id, issue_number) DO UPDATE SET
                title = excluded.title,
                priority = excluded.priority,
                risk_class = excluded.risk_class,
                status = CASE
                    WHEN excluded.status IN ('done', 'removed') THEN excluded.status
                    WHEN work_queue.status = 'claimed' THEN 'claimed'
                    ELSE excluded.status
                END,
                claimed_by = CASE WHEN excluded.status IN ('done', 'removed') THEN NULL ELSE work_queue.claimed_by END,
                claimed_at_utc = CASE WHEN excluded.status IN ('done', 'removed') THEN NULL ELSE work_queue.claimed_at_utc END,
                lease_expires_utc = CASE WHEN excluded.status IN ('done', 'removed') THEN NULL ELSE work_queue.lease_expires_utc END,
                updated_at_utc = excluded.updated_at_utc
            """;
        upsertCmd.Parameters.AddWithValue(ParamWorkspaceId, parsed.WorkspaceId);
        upsertCmd.Parameters.AddWithValue(ParamIssueNumber, parsed.IssueNumber);
        upsertCmd.Parameters.AddWithValue(ParamTitle, parsed.Title!);
        upsertCmd.Parameters.AddWithValue(ParamPriority, parsed.Priority);
        upsertCmd.Parameters.AddWithValue(ParamRiskClass, (object?)parsed.RiskClass ?? DBNull.Value);
        upsertCmd.Parameters.AddWithValue(ParamStatus, parsed.Status);
        upsertCmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        upsertCmd.ExecuteNonQuery();

        return new WorkQueueMutationResult(
            Ok: true,
            Action: ActionUpsert,
            WorkspaceId: parsed.WorkspaceId,
            IssueNumber: parsed.IssueNumber,
            Changed: true,
            Item: ReadWorkQueueItem(connection, parsed.WorkspaceId, parsed.IssueNumber));
    }

    private WorkQueueNextResult ExecuteQueueNextInTx(SqliteConnection connection, string normalizedAgent, string normalizedWorkspace)
    {
        var now = _timeProvider.GetUtcNow();
        var nowIso = FormatUtc(now);
        var leaseExpiresIso = FormatUtc(now.AddHours(SwarmBusConstants.DefaultQueueLeaseHours));

        var resumed = TryResumeOwnClaimInTx(connection, normalizedWorkspace, normalizedAgent, now, nowIso, leaseExpiresIso);
        if (resumed is not null)
        {
            return resumed;
        }

        var (baseCodeWip, activeClaimedCodeCount, effectiveCodeWip) = ComputeEffectiveCodeWipInTx(connection, normalizedWorkspace, now);
        var restrictToLowRisk = effectiveCodeWip >= SwarmBusConstants.CodeWipClaimLimit;
        var candidate = SelectNextCandidate(connection, normalizedWorkspace, now, restrictToLowRisk);

        if (candidate is null)
        {
            return new WorkQueueNextResult(
                Ok: true,
                WorkspaceId: normalizedWorkspace,
                Resumed: false,
                CodeWip: baseCodeWip,
                ActiveClaimedCodeCount: activeClaimedCodeCount,
                EffectiveCodeWip: effectiveCodeWip,
                Item: null,
                Reason: restrictToLowRisk
                    ? $"effective_code_wip ({effectiveCodeWip}) >= {SwarmBusConstants.CodeWipClaimLimit} and no eligible L-class work item is queued."
                    : "No queued or expired-lease work items available.");
        }

        var claimed = ClaimQueueItemInTx(connection, candidate.Id, normalizedAgent, nowIso, leaseExpiresIso);
        return new WorkQueueNextResult(
            Ok: true,
            WorkspaceId: normalizedWorkspace,
            Resumed: false,
            CodeWip: baseCodeWip,
            ActiveClaimedCodeCount: activeClaimedCodeCount,
            EffectiveCodeWip: effectiveCodeWip,
            Item: claimed);
    }

    private static (int IssueNumber, string Owner, string WorkspaceId) ValidateQueueResumeArgs(
        int issueNumber,
        string owner,
        string? workspaceId)
    {
        if (issueNumber <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(issueNumber), "bus_queue resume requires positive integer \"issue_number\".");
        }

        var normalizedOwner = RequireNonEmpty(owner, "bus_queue resume requires non-empty \"owner\".", nameof(owner));
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        return (issueNumber, normalizedOwner, normalizedWorkspace);
    }

    private WorkQueueNextResult ExecuteQueueResumeInTx(
        SqliteConnection connection,
        int issueNumber,
        string normalizedOwner,
        string normalizedWorkspace)
    {
        var now = _timeProvider.GetUtcNow();
        var nowIso = FormatUtc(now);
        var leaseExpiresIso = FormatUtc(now.AddHours(SwarmBusConstants.DefaultQueueLeaseHours));

        long? matchedId = null;
        using (var selectCmd = connection.CreateCommand())
        {
            selectCmd.CommandText = """
                SELECT id
                FROM work_queue
                WHERE workspace_id = @workspaceId
                  AND issue_number = @issueNumber
                  AND status = 'claimed'
                  AND lower(claimed_by) = lower(@owner)
                  AND lease_expires_utc > @nowIso
                LIMIT 1
                """;
            selectCmd.Parameters.AddWithValue(ParamWorkspaceId, normalizedWorkspace);
            selectCmd.Parameters.AddWithValue(ParamIssueNumber, issueNumber);
            selectCmd.Parameters.AddWithValue("@owner", normalizedOwner);
            selectCmd.Parameters.AddWithValue("@nowIso", nowIso);
            var scalar = selectCmd.ExecuteScalar();
            if (scalar is not null && scalar != DBNull.Value)
            {
                matchedId = Convert.ToInt64(scalar, CultureInfo.InvariantCulture);
            }
        }

        if (matchedId.HasValue)
        {
            using var updateCmd = connection.CreateCommand();
            updateCmd.CommandText = """
                UPDATE work_queue
                SET lease_expires_utc = @leaseExpires, updated_at_utc = @updatedAt
                WHERE id = @id
                  AND status = 'claimed'
                  AND lower(claimed_by) = lower(@owner)
                  AND lease_expires_utc > @nowIso
                """;
            updateCmd.Parameters.AddWithValue(ParamLeaseExpires, leaseExpiresIso);
            updateCmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
            updateCmd.Parameters.AddWithValue(ParamId, matchedId.Value);
            updateCmd.Parameters.AddWithValue("@owner", normalizedOwner);
            updateCmd.Parameters.AddWithValue("@nowIso", nowIso);

            if (updateCmd.ExecuteNonQuery() > 0)
            {
                return new WorkQueueNextResult(
                    Ok: true,
                    WorkspaceId: normalizedWorkspace,
                    Resumed: true,
                    CodeWip: 0,
                    ActiveClaimedCodeCount: 0,
                    EffectiveCodeWip: 0,
                    Item: ReadWorkQueueItemById(connection, matchedId.Value));
            }
        }

        return new WorkQueueNextResult(
            Ok: false,
            WorkspaceId: normalizedWorkspace,
            Resumed: false,
            CodeWip: 0,
            ActiveClaimedCodeCount: 0,
            EffectiveCodeWip: 0,
            Item: null,
            Reason: "lease_lost");
    }

    private static (string WorkspaceId, string? Agent) ValidateQueueReleaseArgs(int issueNumber, string? agent, string? workspaceId)
    {
        if (issueNumber <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(issueNumber), "bus_queue release requires positive integer \"issue_number\".");
        }

        return (PathPatternMatcher.NormalizeWorkspaceId(workspaceId), string.IsNullOrWhiteSpace(agent) ? null : agent.Trim());
    }

    private WorkQueueMutationResult ExecuteQueueReleaseInTx(
        SqliteConnection connection,
        int issueNumber,
        string? normalizedAgent,
        string normalizedWorkspace)
    {
        var nowIso = FormatUtc(_timeProvider.GetUtcNow());
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE work_queue
            SET status = 'queued', claimed_by = NULL, claimed_at_utc = NULL, lease_expires_utc = NULL, updated_at_utc = @updatedAt
            WHERE workspace_id = @workspaceId
              AND issue_number = @issueNumber
              AND status = 'claimed'
              AND (@agent IS NULL OR lower(claimed_by) = lower(@agent))
            """;
        cmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, normalizedWorkspace);
        cmd.Parameters.AddWithValue(ParamIssueNumber, issueNumber);
        cmd.Parameters.AddWithValue(ParamAgent, (object?)normalizedAgent ?? DBNull.Value);

        var changed = cmd.ExecuteNonQuery() > 0;
        return new WorkQueueMutationResult(
            Ok: true,
            Action: ActionRelease,
            WorkspaceId: normalizedWorkspace,
            IssueNumber: issueNumber,
            Changed: changed,
            Item: ReadWorkQueueItem(connection, normalizedWorkspace, issueNumber));
    }

    private sealed record ParsedQueueDone(
        string WorkspaceId,
        int IssueNumber,
        string? ExpectedStatus,
        string? ExpectedOwner,
        string? ClaimedBeforeIso,
        bool HasGuards);

    private static ParsedQueueDone ValidateQueueDoneRequest(WorkQueueDoneRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.IssueNumber <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "bus_queue done requires positive integer \"issue_number\".");
        }

        var workspaceId = PathPatternMatcher.NormalizeWorkspaceId(request.WorkspaceId);
        var expectedStatus = string.IsNullOrWhiteSpace(request.ExpectedStatus)
            ? null
            : request.ExpectedStatus.Trim().ToLowerInvariant();
        if (expectedStatus is not null && !SwarmBusConstants.ValidQueueStatuses.Contains(expectedStatus))
        {
            throw new ArgumentException(
                $"Invalid expected_status '{expectedStatus}'. Expected: queued, claimed, done, removed.",
                nameof(request));
        }

        var expectedOwner = string.IsNullOrWhiteSpace(request.ExpectedOwner) ? null : request.ExpectedOwner.Trim();
        var claimedBeforeIso = request.ClaimedBeforeUtc.HasValue ? FormatUtc(request.ClaimedBeforeUtc.Value) : null;
        var hasGuards = expectedStatus is not null || expectedOwner is not null || claimedBeforeIso is not null;

        return new ParsedQueueDone(workspaceId, request.IssueNumber, expectedStatus, expectedOwner, claimedBeforeIso, hasGuards);
    }

    private WorkQueueMutationResult ExecuteQueueDoneInTx(SqliteConnection connection, ParsedQueueDone parsed)
    {
        var nowIso = FormatUtc(_timeProvider.GetUtcNow());
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE work_queue
            SET status = 'done', lease_expires_utc = NULL, updated_at_utc = @updatedAt
            WHERE workspace_id = @workspaceId
              AND issue_number = @issueNumber
              AND (@expectedStatus IS NULL OR status = @expectedStatus)
              AND (@expectedOwner IS NULL OR lower(claimed_by) = lower(@expectedOwner))
              AND (@claimedBeforeUtc IS NULL OR (claimed_at_utc IS NOT NULL AND claimed_at_utc < @claimedBeforeUtc))
            """;
        cmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, parsed.WorkspaceId);
        cmd.Parameters.AddWithValue(ParamIssueNumber, parsed.IssueNumber);
        cmd.Parameters.AddWithValue("@expectedStatus", (object?)parsed.ExpectedStatus ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@expectedOwner", (object?)parsed.ExpectedOwner ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@claimedBeforeUtc", (object?)parsed.ClaimedBeforeIso ?? DBNull.Value);

        var changed = cmd.ExecuteNonQuery() > 0;
        var item = ReadWorkQueueItem(connection, parsed.WorkspaceId, parsed.IssueNumber);

        if (!changed && parsed.HasGuards && item is not null)
        {
            return new WorkQueueMutationResult(
                Ok: false,
                Action: StatusDone,
                WorkspaceId: parsed.WorkspaceId,
                IssueNumber: parsed.IssueNumber,
                Changed: false,
                Item: item,
                Reason: "guard_mismatch");
        }

        return new WorkQueueMutationResult(
            Ok: true,
            Action: StatusDone,
            WorkspaceId: parsed.WorkspaceId,
            IssueNumber: parsed.IssueNumber,
            Changed: changed,
            Item: item);
    }

    private static (string WorkspaceId, string? StatusFilter) ValidateQueueListArgs(string? status, string? workspaceId)
    {
        var normalizedWorkspace = PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        var statusFilter = string.IsNullOrWhiteSpace(status) ? null : status.Trim().ToLowerInvariant();
        if (statusFilter is not null && !SwarmBusConstants.ValidQueueStatuses.Contains(statusFilter))
        {
            throw new ArgumentException($"Invalid status '{statusFilter}'. Expected: queued, claimed, done, removed.", nameof(status));
        }

        return (normalizedWorkspace, statusFilter);
    }

    private static WorkQueueListResult ReadQueueListSnapshot(SqliteConnection connection, string normalizedWorkspace, string? statusFilter)
    {
        var items = ReadWorkQueueItems(connection, normalizedWorkspace, statusFilter);
        var meta = ReadMetaMap(connection, normalizedWorkspace);
        var codeWip = meta.TryGetValue(CodeWipMetaKey, out var rawWip) && int.TryParse(rawWip, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : 0;

        return new WorkQueueListResult(
            Ok: true,
            WorkspaceId: normalizedWorkspace,
            CodeWip: codeWip,
            Meta: meta,
            Items: items);
    }

    private static void UpsertMetaInTx(SqliteConnection connection, string workspaceId, string key, string value, string nowIso)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO meta (workspace_id, key, value, updated_at_utc)
            VALUES (@workspaceId, @key, @value, @updatedAt)
            ON CONFLICT(workspace_id, key) DO UPDATE SET
                value = excluded.value,
                updated_at_utc = excluded.updated_at_utc
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamKey, key);
        cmd.Parameters.AddWithValue(ParamValue, value);
        cmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        cmd.ExecuteNonQuery();
    }

    private static WorkQueueNextResult? TryResumeOwnClaimInTx(
        SqliteConnection connection,
        string workspaceId,
        string normalizedAgent,
        DateTimeOffset now,
        string nowIso,
        string leaseExpiresIso)
    {
        var ownActive = ReadWorkQueueItems(connection, workspaceId, StatusClaimed)
            .Where(item => string.Equals(item.ClaimedBy, normalizedAgent, StringComparison.OrdinalIgnoreCase) &&
                           item.LeaseExpiresUtc.HasValue &&
                           item.LeaseExpiresUtc.Value > now)
            .OrderBy(item => item.Priority)
            .ThenBy(item => item.Id)
            .FirstOrDefault();

        if (ownActive is null)
        {
            return null;
        }

        using var renewCmd = connection.CreateCommand();
        renewCmd.CommandText = """
            UPDATE work_queue
            SET lease_expires_utc = @leaseExpires, updated_at_utc = @updatedAt
            WHERE id = @id
            """;
        renewCmd.Parameters.AddWithValue(ParamLeaseExpires, leaseExpiresIso);
        renewCmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        renewCmd.Parameters.AddWithValue(ParamId, ownActive.Id);
        renewCmd.ExecuteNonQuery();

        return new WorkQueueNextResult(
            Ok: true,
            WorkspaceId: workspaceId,
            Resumed: true,
            CodeWip: 0,
            ActiveClaimedCodeCount: 0,
            EffectiveCodeWip: 0,
            Item: ReadWorkQueueItemById(connection, ownActive.Id));
    }

    private static (int BaseCodeWip, int ActiveClaimedCodeCount, int EffectiveCodeWip) ComputeEffectiveCodeWipInTx(
        SqliteConnection connection,
        string workspaceId,
        DateTimeOffset now)
    {
        var baseCodeWip = ReadMetaInt(connection, workspaceId, CodeWipMetaKey);
        var activeClaimedCodeCount = ReadWorkQueueItems(connection, workspaceId, StatusClaimed)
            .Count(item => IsUnexpired(item.LeaseExpiresUtc, now) && !IsLowRisk(item.RiskClass));

        return (baseCodeWip, activeClaimedCodeCount, baseCodeWip + activeClaimedCodeCount);
    }

    private static WorkQueueItem? SelectNextCandidate(
        SqliteConnection connection,
        string workspaceId,
        DateTimeOffset now,
        bool restrictToLowRisk)
    {
        return ReadWorkQueueItems(connection, workspaceId, statusFilter: null)
            .Where(item => IsQueueEligible(item, now) && (!restrictToLowRisk || IsLowRisk(item.RiskClass)))
            .OrderBy(item => item.Priority)
            .ThenBy(item => item.Id)
            .FirstOrDefault();
    }

    private static bool IsQueueEligible(WorkQueueItem item, DateTimeOffset now) =>
        string.Equals(item.Status, StatusQueued, StringComparison.Ordinal) ||
        (string.Equals(item.Status, StatusClaimed, StringComparison.Ordinal) &&
         item.LeaseExpiresUtc.HasValue &&
         item.LeaseExpiresUtc.Value <= now);

    private static bool IsUnexpired(DateTimeOffset? expiresAt, DateTimeOffset now) =>
        !expiresAt.HasValue || expiresAt.Value > now;

    private static bool IsLowRisk(string? riskClass) =>
        string.Equals(riskClass?.Trim(), "L", StringComparison.OrdinalIgnoreCase);

    private static WorkQueueItem? ClaimQueueItemInTx(
        SqliteConnection connection,
        long itemId,
        string agent,
        string nowIso,
        string leaseExpiresIso)
    {
        using var claimCmd = connection.CreateCommand();
        claimCmd.CommandText = """
            UPDATE work_queue
            SET status = 'claimed',
                claimed_by = @agent,
                claimed_at_utc = @claimedAt,
                lease_expires_utc = @leaseExpires,
                updated_at_utc = @updatedAt
            WHERE id = @id
            """;
        claimCmd.Parameters.AddWithValue(ParamAgent, agent);
        claimCmd.Parameters.AddWithValue(ParamClaimedAt, nowIso);
        claimCmd.Parameters.AddWithValue(ParamLeaseExpires, leaseExpiresIso);
        claimCmd.Parameters.AddWithValue(ParamUpdatedAt, nowIso);
        claimCmd.Parameters.AddWithValue(ParamId, itemId);
        claimCmd.ExecuteNonQuery();

        return ReadWorkQueueItemById(connection, itemId);
    }

    private SwarmBusStatusSnapshot BuildStatusSnapshotInTx(
        SqliteConnection connection,
        bool allWorkspaces,
        string normalizedWorkspace,
        int clampedLimit)
    {
        var now = _timeProvider.GetUtcNow();
        PruneExpiredLocksInTx(connection, now);

        return new SwarmBusStatusSnapshot(
            GeneratedAtUtc: now,
            WorkspaceId: normalizedWorkspace,
            TotalUnread: ReadTotalUnreadCount(connection, allWorkspaces, normalizedWorkspace),
            UnreadByRecipient: ReadUnreadByRecipient(connection, allWorkspaces, normalizedWorkspace),
            ActiveLocks: allWorkspaces ? ReadAllLocks(connection) : ReadLocksInWorkspace(connection, normalizedWorkspace),
            RecentMessages: ReadRecentMessages(connection, allWorkspaces, normalizedWorkspace, clampedLimit),
            Sessions: ReadAgentSessionsInTx(connection, allWorkspaces, normalizedWorkspace, now),
            Projects: ReadProjectStatesInTx(connection, allWorkspaces, normalizedWorkspace, now),
            QueuedIdeaCount: CountQueuedIdeasInTx(connection, allWorkspaces, normalizedWorkspace));
    }

    private static List<RecipientUnreadCount> ReadUnreadByRecipient(SqliteConnection connection, bool allWorkspaces, string workspaceId)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT recipient, COUNT(*) AS unread_count
            FROM messages
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
              AND status = 'unread'
            GROUP BY recipient
            ORDER BY unread_count DESC, recipient ASC
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);

        var result = new List<RecipientUnreadCount>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result.Add(new RecipientUnreadCount(
                Recipient: reader.GetString(0),
                UnreadCount: reader.GetInt32(1)));
        }

        return result;
    }

    private static int ReadTotalUnreadCount(SqliteConnection connection, bool allWorkspaces, string workspaceId)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*)
            FROM messages
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
              AND status = 'unread'
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);

        var scalar = cmd.ExecuteScalar();
        return scalar is not null && scalar != DBNull.Value
            ? Convert.ToInt32(scalar, CultureInfo.InvariantCulture)
            : 0;
    }

    private static List<SwarmMessageEnvelope> ReadRecentMessages(
        SqliteConnection connection,
        bool allWorkspaces,
        string workspaceId,
        int limit)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, created_at_utc, sender, recipient, topic, body,
                   pr_number, issue_number, priority, reply_to_id, status,
                   acked_by, acked_at_utc, state, wake_ok, wake_state, wake_attempt_expires_at_utc
            FROM messages
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
            ORDER BY id DESC
            LIMIT @limit
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamLimit, limit);
        return ReadMessages(cmd);
    }

    private ReviewClaimResult ClaimReviewSlot(ReviewClaimRequest request, string workspaceId)
    {
        var (agent, prNumber, headSha, role, ttlMinutes) = ValidateClaimReviewSlotRequest(request);
        return ExecuteInImmediateTransaction(connection => ClaimReviewSlotInTx(connection, workspaceId, agent, prNumber, headSha, role, ttlMinutes));
    }

    private Task<ReviewClaimResult> ClaimReviewSlotAsync(ReviewClaimRequest request, string workspaceId, CancellationToken cancellationToken)
    {
        var (agent, prNumber, headSha, role, ttlMinutes) = ValidateClaimReviewSlotRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => ClaimReviewSlotInTx(connection, workspaceId, agent, prNumber, headSha, role, ttlMinutes),
            cancellationToken);
    }

    private static (string Agent, int PrNumber, string HeadSha, string Role, int TtlMinutes) ValidateClaimReviewSlotRequest(ReviewClaimRequest request)
    {
        var agent = RequireNonEmpty(request.Agent, "bus_review_claim claim requires non-empty \"agent\".", nameof(request));
        var (prNumber, headSha, role) = ValidateReviewClaimTarget(request);
        var rawTtl = request.TtlMinutes ?? SwarmBusConstants.DefaultReviewClaimTtlMinutes;
        var ttlMinutes = Math.Clamp(rawTtl, SwarmBusConstants.MinReviewClaimTtlMinutes, SwarmBusConstants.MaxReviewClaimTtlMinutes);
        return (agent, prNumber, headSha, role, ttlMinutes);
    }

    private ReviewClaimResult ClaimReviewSlotInTx(
        SqliteConnection connection,
        string workspaceId,
        string agent,
        int prNumber,
        string headSha,
        string role,
        int ttlMinutes)
    {
        var now = _timeProvider.GetUtcNow();
        var claimedAtIso = FormatUtc(now);
        var expiresAtIso = FormatUtc(now.AddMinutes(ttlMinutes));

        var existing = ReadReviewClaim(connection, workspaceId, prNumber, headSha, role);
        if (existing is null)
        {
            return InsertNewReviewClaimInTx(connection, workspaceId, prNumber, headSha, role, agent, claimedAtIso, expiresAtIso);
        }

        if (string.Equals(existing.Status, StatusCompleted, StringComparison.Ordinal))
        {
            return CreateBlockedReviewClaimResult(existing, workspaceId, "already_completed", StatusCompleted, alreadyClaimedBySelf: null);
        }

        if (IsActiveReviewClaim(existing, now))
        {
            return CreateBlockedReviewClaimResult(
                existing,
                workspaceId,
                "already_claimed",
                StatusClaimed,
                alreadyClaimedBySelf: existing.Agent.Equals(agent, StringComparison.OrdinalIgnoreCase));
        }

        return ReclaimReviewSlotInTx(connection, existing, workspaceId, headSha, agent, claimedAtIso, expiresAtIso);
    }

    private static bool IsActiveReviewClaim(ReviewClaim existing, DateTimeOffset now) =>
        string.Equals(existing.Status, StatusClaimed, StringComparison.Ordinal) &&
        existing.ExpiresAtUtc.HasValue &&
        existing.ExpiresAtUtc.Value > now;

    private static ReviewClaimResult CreateBlockedReviewClaimResult(
        ReviewClaim existing,
        string workspaceId,
        string reason,
        string status,
        bool? alreadyClaimedBySelf) =>
        new(
            Ok: true,
            Action: ActionClaim,
            WorkspaceId: workspaceId,
            Claimed: false,
            Reason: reason,
            AlreadyClaimedBySelf: alreadyClaimedBySelf,
            Holder: existing.Agent,
            PrNumber: existing.PrNumber,
            HeadSha: existing.HeadSha,
            Role: existing.Role,
            Status: status,
            Claim: existing);

    private static ReviewClaimResult InsertNewReviewClaimInTx(
        SqliteConnection connection,
        string workspaceId,
        int prNumber,
        string headSha,
        string role,
        string agent,
        string claimedAtIso,
        string expiresAtIso)
    {
        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
            INSERT INTO review_claims (workspace_id, pr_number, head_sha, role, agent, claimed_at_utc, status, expires_at_utc, completed_at_utc)
            VALUES (@workspaceId, @prNumber, @headSha, @role, @agent, @claimedAt, 'claimed', @expiresAt, NULL)
            """;
        insertCmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        insertCmd.Parameters.AddWithValue(ParamPrNumber, prNumber);
        insertCmd.Parameters.AddWithValue(ParamHeadSha, headSha);
        insertCmd.Parameters.AddWithValue(ParamRole, role);
        insertCmd.Parameters.AddWithValue(ParamAgent, agent);
        insertCmd.Parameters.AddWithValue(ParamClaimedAt, claimedAtIso);
        insertCmd.Parameters.AddWithValue(ParamExpiresAt, expiresAtIso);
        insertCmd.ExecuteNonQuery();

        return new ReviewClaimResult(
            Ok: true,
            Action: ActionClaim,
            WorkspaceId: workspaceId,
            Claimed: true,
            Reclaimed: false,
            PrNumber: prNumber,
            HeadSha: headSha,
            Role: role,
            Status: StatusClaimed,
            Claim: ReadReviewClaim(connection, workspaceId, prNumber, headSha, role));
    }

    private static ReviewClaimResult ReclaimReviewSlotInTx(
        SqliteConnection connection,
        ReviewClaim existing,
        string workspaceId,
        string headSha,
        string agent,
        string claimedAtIso,
        string expiresAtIso)
    {
        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = """
            UPDATE review_claims
            SET head_sha = @headSha,
                agent = @agent,
                status = 'claimed',
                claimed_at_utc = @claimedAt,
                expires_at_utc = @expiresAt,
                completed_at_utc = NULL
            WHERE id = @id
            """;
        updateCmd.Parameters.AddWithValue(ParamHeadSha, headSha);
        updateCmd.Parameters.AddWithValue(ParamAgent, agent);
        updateCmd.Parameters.AddWithValue(ParamClaimedAt, claimedAtIso);
        updateCmd.Parameters.AddWithValue(ParamExpiresAt, expiresAtIso);
        updateCmd.Parameters.AddWithValue(ParamId, existing.Id);
        updateCmd.ExecuteNonQuery();

        return new ReviewClaimResult(
            Ok: true,
            Action: ActionClaim,
            WorkspaceId: workspaceId,
            Claimed: true,
            Reclaimed: true,
            PreviousHolder: existing.Agent,
            PreviousStatus: existing.Status,
            PrNumber: existing.PrNumber,
            HeadSha: headSha,
            Role: existing.Role,
            Status: StatusClaimed,
            Claim: ReadReviewClaim(connection, workspaceId, existing.PrNumber, headSha, existing.Role));
    }

    private ReviewClaimResult CompleteReviewSlot(ReviewClaimRequest request, string workspaceId)
    {
        var (agent, prNumber, headSha, role) = ValidateHolderMutationRequest(request);
        return ExecuteInImmediateTransaction(connection => CompleteReviewSlotInTx(connection, workspaceId, agent, prNumber, headSha, role));
    }

    private Task<ReviewClaimResult> CompleteReviewSlotAsync(ReviewClaimRequest request, string workspaceId, CancellationToken cancellationToken)
    {
        var (agent, prNumber, headSha, role) = ValidateHolderMutationRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => CompleteReviewSlotInTx(connection, workspaceId, agent, prNumber, headSha, role),
            cancellationToken);
    }

    private ReviewClaimResult ReleaseReviewSlot(ReviewClaimRequest request, string workspaceId)
    {
        var (agent, prNumber, headSha, role) = ValidateHolderMutationRequest(request);
        return ExecuteInImmediateTransaction(connection => ReleaseReviewSlotInTx(connection, workspaceId, agent, prNumber, headSha, role));
    }

    private Task<ReviewClaimResult> ReleaseReviewSlotAsync(ReviewClaimRequest request, string workspaceId, CancellationToken cancellationToken)
    {
        var (agent, prNumber, headSha, role) = ValidateHolderMutationRequest(request);
        return ExecuteInImmediateTransactionAsync(
            connection => ReleaseReviewSlotInTx(connection, workspaceId, agent, prNumber, headSha, role),
            cancellationToken);
    }

    private static (string Agent, int PrNumber, string HeadSha, string Role) ValidateHolderMutationRequest(ReviewClaimRequest request)
    {
        var agent = RequireNonEmpty(request.Agent, "bus_review_claim requires non-empty \"agent\".", nameof(request));
        var (prNumber, headSha, role) = ValidateReviewClaimTarget(request);
        return (agent, prNumber, headSha, role);
    }

    private ReviewClaimResult CompleteReviewSlotInTx(
        SqliteConnection connection,
        string workspaceId,
        string agent,
        int prNumber,
        string headSha,
        string role)
    {
        var now = _timeProvider.GetUtcNow();
        var existing = RequireOwnedReviewClaim(connection, workspaceId, prNumber, headSha, role, agent);

        if (string.Equals(existing.Status, StatusCompleted, StringComparison.Ordinal))
        {
            return new ReviewClaimResult(
                Ok: true,
                Action: ActionComplete,
                WorkspaceId: workspaceId,
                PrNumber: prNumber,
                HeadSha: headSha,
                Role: role,
                Status: StatusCompleted,
                Claim: existing);
        }

        EnsureActiveClaimLease(existing, now, prNumber, headSha, role);

        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = """
            UPDATE review_claims
            SET status = 'completed',
                expires_at_utc = NULL,
                completed_at_utc = @completedAt
            WHERE id = @id
            """;
        updateCmd.Parameters.AddWithValue(ParamCompletedAt, FormatUtc(now));
        updateCmd.Parameters.AddWithValue(ParamId, existing.Id);
        updateCmd.ExecuteNonQuery();

        var updated = ReadReviewClaim(connection, workspaceId, prNumber, headSha, role);
        return new ReviewClaimResult(
            Ok: true,
            Action: ActionComplete,
            WorkspaceId: workspaceId,
            PrNumber: prNumber,
            HeadSha: headSha,
            Role: role,
            Status: StatusCompleted,
            Claim: updated);
    }

    private ReviewClaimResult ReleaseReviewSlotInTx(
        SqliteConnection connection,
        string workspaceId,
        string agent,
        int prNumber,
        string headSha,
        string role)
    {
        var now = _timeProvider.GetUtcNow();
        var existing = RequireOwnedReviewClaim(connection, workspaceId, prNumber, headSha, role, agent);

        if (string.Equals(existing.Status, StatusCompleted, StringComparison.Ordinal))
        {
            return new ReviewClaimResult(
                Ok: true,
                Action: ActionRelease,
                WorkspaceId: workspaceId,
                Released: false,
                Reason: "already_completed",
                PrNumber: prNumber,
                HeadSha: headSha,
                Role: role,
                Status: StatusCompleted,
                Claim: existing);
        }

        EnsureActiveClaimLease(existing, now, prNumber, headSha, role);

        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = """
            UPDATE review_claims
            SET status = 'released',
                expires_at_utc = NULL,
                completed_at_utc = NULL
            WHERE id = @id
            """;
        updateCmd.Parameters.AddWithValue(ParamId, existing.Id);
        updateCmd.ExecuteNonQuery();

        var updated = ReadReviewClaim(connection, workspaceId, prNumber, headSha, role);
        return new ReviewClaimResult(
            Ok: true,
            Action: ActionRelease,
            WorkspaceId: workspaceId,
            Released: true,
            PrNumber: prNumber,
            HeadSha: headSha,
            Role: role,
            Status: StatusReleased,
            Claim: updated);
    }

    private static ReviewClaim RequireOwnedReviewClaim(
        SqliteConnection connection,
        string workspaceId,
        int prNumber,
        string headSha,
        string role,
        string agent)
    {
        var existing = ReadReviewClaim(connection, workspaceId, prNumber, headSha, role)
            ?? throw new InvalidOperationException($"No review claim found for PR #{prNumber} ({headSha}, {role}).");

        if (!existing.Agent.Equals(agent, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Review claim for PR #{prNumber} ({headSha}, {role}) is held by '{existing.Agent}', not '{agent}'.");
        }

        return existing;
    }

    private static void EnsureActiveClaimLease(ReviewClaim existing, DateTimeOffset now, int prNumber, string headSha, string role)
    {
        if (!string.Equals(existing.Status, StatusClaimed, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Review claim for PR #{prNumber} ({headSha}, {role}) is '{existing.Status}' and not active.");
        }

        if (!existing.ExpiresAtUtc.HasValue || existing.ExpiresAtUtc.Value <= now)
        {
            throw new InvalidOperationException($"Review claim for PR #{prNumber} ({headSha}, {role}) has expired.");
        }
    }

    private ReviewClaimResult ListReviewSlots(ReviewClaimRequest request)
    {
        var (allWorkspaces, normalizedWorkspace, headSha, role) = ValidateListReviewSlotsRequest(request);
        var claims = ExecuteRead(connection => ReadReviewClaimsList(connection, allWorkspaces, normalizedWorkspace, request.PrNumber, headSha, role));
        return new ReviewClaimResult(Ok: true, Action: ActionList, WorkspaceId: normalizedWorkspace, Claims: claims);
    }

    private async Task<ReviewClaimResult> ListReviewSlotsAsync(ReviewClaimRequest request, CancellationToken cancellationToken)
    {
        var (allWorkspaces, normalizedWorkspace, headSha, role) = ValidateListReviewSlotsRequest(request);
        var claims = await ExecuteReadAsync(
            connection => ReadReviewClaimsList(connection, allWorkspaces, normalizedWorkspace, request.PrNumber, headSha, role),
            cancellationToken).ConfigureAwait(false);
        return new ReviewClaimResult(Ok: true, Action: ActionList, WorkspaceId: normalizedWorkspace, Claims: claims);
    }

    private static (bool AllWorkspaces, string WorkspaceId, string? HeadSha, string? Role) ValidateListReviewSlotsRequest(ReviewClaimRequest request)
    {
        var (allWorkspaces, normalizedWorkspace) = ResolveWorkspaceFilter(request.WorkspaceId);

        if (request.PrNumber.HasValue && request.PrNumber.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "bus_review_claim list \"pr_number\" must be a positive integer.");
        }

        var headSha = NormalizeHeadSha(request.HeadSha, required: false, nameof(request));
        var role = NormalizeOptionalReviewRole(request.Role, nameof(request));
        return (allWorkspaces, normalizedWorkspace, headSha, role);
    }

    private static List<ReviewClaim> ReadReviewClaimsList(
        SqliteConnection connection,
        bool allWorkspaces,
        string normalizedWorkspace,
        int? prNumber,
        string? headSha,
        string? role)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, pr_number, head_sha, role, agent, claimed_at_utc, status, expires_at_utc, completed_at_utc
            FROM review_claims
            WHERE (@allWorkspaces = 1 OR workspace_id = @workspaceId)
              AND (@prNumber IS NULL OR pr_number = @prNumber)
              AND (@headSha IS NULL OR lower(head_sha) = @headSha)
              AND (@role IS NULL OR role = @role)
            ORDER BY id DESC
            """;
        cmd.Parameters.AddWithValue(ParamAllWorkspaces, allWorkspaces ? 1 : 0);
        cmd.Parameters.AddWithValue(ParamWorkspaceId, normalizedWorkspace);
        cmd.Parameters.AddWithValue(ParamPrNumber, (object?)prNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue(ParamHeadSha, (object?)headSha ?? DBNull.Value);
        cmd.Parameters.AddWithValue(ParamRole, (object?)role ?? DBNull.Value);

        var results = new List<ReviewClaim>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(MapReviewClaim(reader));
        }

        return results;
    }

    private SendTxResult SendOrDeduplicateInTx(SqliteConnection connection, SendTxInput input)
    {
        var now = _timeProvider.GetUtcNow();
        var createdAtIso = FormatUtc(now);
        var wakeExpiresIso = FormatUtc(now.AddSeconds(SwarmBusConstants.DefaultWakeAttemptTtlSeconds));

        AutoAckParentMessageInTx(connection, input, createdAtIso);

        var resolvedCodexThreadId = input.WakeTargetMatches && !input.HasExplicitCodexThreadId
            ? ResolveRegisteredCodexThreadIdInTx(connection, input.WorkspaceId)
            : null;

        if (input.State is not null)
        {
            var dedup = TryDeduplicateLatestMessageInTx(connection, input, now, wakeExpiresIso);
            if (dedup is not null)
            {
                return dedup.Mode == SendTxMode.RetryWake
                    ? dedup with { ResolvedCodexThreadId = resolvedCodexThreadId }
                    : dedup;
            }
        }

        if (input.Sender.Equals("watchdog", StringComparison.OrdinalIgnoreCase) && input.PrNumber.HasValue)
        {
            SupersedeWatchdogAlertsInTx(connection, input.WorkspaceId, input.PrNumber.Value, createdAtIso);
        }

        var id = InsertMessageInTx(connection, input, createdAtIso, wakeExpiresIso);
        return new SendTxResult(
            SendTxMode.Inserted,
            id,
            now,
            WakeLeaseToken: input.WakeTargetMatches ? wakeExpiresIso : null,
            ResolvedCodexThreadId: resolvedCodexThreadId);
    }

    private static SendTxResult? TryDeduplicateLatestMessageInTx(
        SqliteConnection connection,
        SendTxInput input,
        DateTimeOffset now,
        string wakeExpiresIso)
    {
        using var latestCmd = connection.CreateCommand();
        latestCmd.CommandText = """
            SELECT id, body, status, state, wake_ok, wake_state, wake_attempt_expires_at_utc
            FROM messages
            WHERE workspace_id = @workspaceId
              AND lower(sender) = lower(@sender)
              AND lower(recipient) = lower(@recipient)
              AND topic = @topic
              AND COALESCE(pr_number, -1) = COALESCE(@prNumber, -1)
              AND COALESCE(issue_number, -1) = COALESCE(@issueNumber, -1)
            ORDER BY id DESC
            LIMIT 1
            """;
        latestCmd.Parameters.AddWithValue(ParamWorkspaceId, input.WorkspaceId);
        latestCmd.Parameters.AddWithValue(ParamSender, input.Sender);
        latestCmd.Parameters.AddWithValue(ParamRecipient, input.Recipient);
        latestCmd.Parameters.AddWithValue(ParamTopic, input.Topic);
        latestCmd.Parameters.AddWithValue(ParamPrNumber, (object?)input.PrNumber ?? DBNull.Value);
        latestCmd.Parameters.AddWithValue(ParamIssueNumber, (object?)input.IssueNumber ?? DBNull.Value);

        using var reader = latestCmd.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        var latestState = GetNullableString(reader, 3);
        if (!string.Equals(latestState, input.State, StringComparison.Ordinal))
        {
            return null;
        }

        var matched = new MatchedMessageRow(
            Id: reader.GetInt64(0),
            Body: GetStringOrDefault(reader, 1, input.Body),
            Status: GetStringOrDefault(reader, 2, StatusUnread),
            WakeOk: GetNullableInt32(reader, 4),
            WakeState: GetNullableString(reader, 5),
            WakeAttemptExpiresAtUtc: GetNullableUtc(reader, 6));
        reader.Close();

        return ResolveMatchedStateRow(
            connection,
            matched,
            new WakeResolutionContext(input.Body, input.WakeTargetMatches, now, wakeExpiresIso));
    }

    private static void SupersedeWatchdogAlertsInTx(
        SqliteConnection connection,
        string workspaceId,
        int prNumber,
        string createdAtIso)
    {
        using var supersedeCmd = connection.CreateCommand();
        supersedeCmd.CommandText = """
            UPDATE messages
            SET status = 'acked', acked_by = 'auto-superseded', acked_at_utc = @ackedAt
            WHERE workspace_id = @workspaceId
              AND lower(sender) = 'watchdog'
              AND pr_number = @prNumber
              AND status = 'unread'
            """;
        supersedeCmd.Parameters.AddWithValue(ParamAckedAt, createdAtIso);
        supersedeCmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        supersedeCmd.Parameters.AddWithValue(ParamPrNumber, prNumber);
        supersedeCmd.ExecuteNonQuery();
    }

    private static void AutoAckParentMessageInTx(
        SqliteConnection connection,
        SendTxInput input,
        string createdAtIso)
    {
        if (!input.ReplyToId.HasValue)
        {
            return;
        }

        using var autoAckCmd = connection.CreateCommand();
        autoAckCmd.CommandText = """
            UPDATE messages
            SET status = 'acked',
                acked_by = @sender,
                acked_at_utc = @ackedAt
            WHERE workspace_id = @workspaceId
              AND id = @replyToId
              AND status = 'unread'
              AND (lower(recipient) = lower(@sender) OR lower(recipient) = 'all')
            """;
        autoAckCmd.Parameters.AddWithValue(ParamSender, input.Sender);
        autoAckCmd.Parameters.AddWithValue(ParamAckedAt, createdAtIso);
        autoAckCmd.Parameters.AddWithValue(ParamWorkspaceId, input.WorkspaceId);
        autoAckCmd.Parameters.AddWithValue(ParamReplyToId, input.ReplyToId.Value);
        autoAckCmd.ExecuteNonQuery();
    }

    private static long InsertMessageInTx(
        SqliteConnection connection,
        SendTxInput input,
        string createdAtIso,
        string wakeExpiresIso)
    {
        var isHeadless = SwarmBusConstants.HeadlessRecipients.Contains(input.Recipient);
        var initialStatus = isHeadless ? StatusAcked : StatusUnread;
        var initialAckedBy = isHeadless ? "auto-headless" : null;
        var initialAckedAt = isHeadless ? createdAtIso : null;
        int? initialWakeOk = input.WakeTargetMatches ? 0 : null;
        var initialWakeState = input.WakeTargetMatches ? WakeStatePending : null;
        var initialWakeExpires = input.WakeTargetMatches ? wakeExpiresIso : null;

        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = """
            INSERT INTO messages (
                workspace_id, created_at_utc, sender, recipient, topic, body,
                pr_number, issue_number, priority, reply_to_id, status,
                acked_by, acked_at_utc, state, wake_ok, wake_state, wake_attempt_expires_at_utc)
            VALUES (
                @workspaceId, @createdAt, @sender, @recipient, @topic, @body,
                @prNumber, @issueNumber, @priority, @replyToId, @status,
                @ackedBy, @ackedAt, @state, @wakeOk, @wakeState, @wakeExpires);
            SELECT last_insert_rowid();
            """;
        insertCmd.Parameters.AddWithValue(ParamWorkspaceId, input.WorkspaceId);
        insertCmd.Parameters.AddWithValue(ParamCreatedAt, createdAtIso);
        insertCmd.Parameters.AddWithValue(ParamSender, input.Sender);
        insertCmd.Parameters.AddWithValue(ParamRecipient, input.Recipient);
        insertCmd.Parameters.AddWithValue(ParamTopic, input.Topic);
        insertCmd.Parameters.AddWithValue(ParamBody, input.Body);
        insertCmd.Parameters.AddWithValue(ParamPrNumber, (object?)input.PrNumber ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamIssueNumber, (object?)input.IssueNumber ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamPriority, input.Priority);
        insertCmd.Parameters.AddWithValue(ParamReplyToId, (object?)input.ReplyToId ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamStatus, initialStatus);
        insertCmd.Parameters.AddWithValue(ParamAckedBy, (object?)initialAckedBy ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamAckedAt, (object?)initialAckedAt ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamState, (object?)input.State ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamWakeOk, (object?)initialWakeOk ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamWakeState, (object?)initialWakeState ?? DBNull.Value);
        insertCmd.Parameters.AddWithValue(ParamWakeExpires, (object?)initialWakeExpires ?? DBNull.Value);

        return Convert.ToInt64(insertCmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private static SendTxResult ResolveMatchedStateRow(
        SqliteConnection connection,
        MatchedMessageRow latest,
        WakeResolutionContext ctx)
    {
        if (!ctx.WakeTargetMatches)
        {
            return new SendTxResult(SendTxMode.Deduplicated, latest.Id, ctx.Now, WakeState: latest.WakeState);
        }

        var curWakeState = NormalizeWakeState(latest.WakeState, latest.WakeOk);
        if (string.Equals(curWakeState, WakeStateOk, StringComparison.Ordinal))
        {
            return new SendTxResult(SendTxMode.Deduplicated, latest.Id, ctx.Now, WakeState: WakeStateOk);
        }

        var hasUnexpiredPending = string.Equals(curWakeState, WakeStatePending, StringComparison.Ordinal) &&
                                  latest.WakeAttemptExpiresAtUtc.HasValue &&
                                  latest.WakeAttemptExpiresAtUtc.Value > ctx.Now;
        if (hasUnexpiredPending)
        {
            return new SendTxResult(SendTxMode.Deduplicated, latest.Id, ctx.Now, WakeState: WakeStatePending, WakeInProgress: true);
        }

        if (string.Equals(latest.Status, StatusUnread, StringComparison.Ordinal))
        {
            ReserveWakeRetryInTx(connection, latest.Id, ctx.WakeExpiresIso);
            return new SendTxResult(
                SendTxMode.RetryWake,
                latest.Id,
                ctx.Now,
                Body: string.IsNullOrWhiteSpace(latest.Body) ? ctx.FallbackBody : latest.Body,
                WakeLeaseToken: ctx.WakeExpiresIso);
        }

        return new SendTxResult(SendTxMode.Deduplicated, latest.Id, ctx.Now, WakeState: curWakeState);
    }

    private static string? NormalizeWakeState(string? wakeState, int? wakeOk)
    {
        if (!string.IsNullOrEmpty(wakeState))
        {
            return wakeState;
        }

        return wakeOk switch
        {
            1 => WakeStateOk,
            0 => WakeStateFailed,
            _ => null
        };
    }

    private static void ReserveWakeRetryInTx(SqliteConnection connection, long messageId, string wakeExpiresIso)
    {
        using var reserveCmd = connection.CreateCommand();
        reserveCmd.CommandText = """
            UPDATE messages
            SET wake_state = 'pending',
                wake_attempt_expires_at_utc = @wakeExpires,
                wake_ok = 0
            WHERE id = @id
            """;
        reserveCmd.Parameters.AddWithValue(ParamWakeExpires, wakeExpiresIso);
        reserveCmd.Parameters.AddWithValue(ParamId, messageId);
        reserveCmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Invokes the configured wake dispatcher bounded by <see cref="SwarmBusStoreOptions.WakeDispatchTimeout"/>.
    /// Uses <see cref="Task.WaitAsync(CancellationToken)"/> so a non-cooperative dispatcher cannot block indefinitely.
    /// </summary>
    private async Task<(SwarmWakeResult Result, bool FenceApplied)> DispatchWakeAsync(
        SwarmWakeContext context,
        string expectedLeaseIso,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_wakeDispatchTimeout);

        SwarmWakeResult wakeRes;
        try
        {
            wakeRes = await _wakeDispatcher!(context, timeoutCts.Token)
                .WaitAsync(timeoutCts.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await CompleteWakeAttemptAsync(context.MessageId, ok: false, expectedLeaseIso, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex) when (ex is OperationCanceledException or TimeoutException)
        {
            wakeRes = new SwarmWakeResult(
                Attempted: true,
                Ok: false,
                Target: context.Recipient,
                ThreadId: context.CodexThreadId,
                Reason: "timeout",
                Error: $"Wake dispatch timed out after {_wakeDispatchTimeout.TotalSeconds:0.##}s.");
        }
        catch (Exception ex)
        {
            wakeRes = new SwarmWakeResult(Attempted: true, Ok: false, Target: context.Recipient, ThreadId: context.CodexThreadId, Error: ex.Message);
        }

        var fenceApplied = await CompleteWakeAttemptAsync(context.MessageId, wakeRes.Ok, expectedLeaseIso, cancellationToken).ConfigureAwait(false);
        if (!fenceApplied)
        {
            wakeRes = new SwarmWakeResult(
                Attempted: true,
                Ok: false,
                Target: context.Recipient,
                ThreadId: context.CodexThreadId,
                Reason: "stale_lease_fenced",
                Error: "Wake lease expired or was superseded before completion.");
        }

        return (wakeRes, fenceApplied);
    }

    private static (bool AllWorkspaces, string NormalizedWorkspace) ResolveWorkspaceFilter(string? workspaceId)
    {
        var allWorkspaces = string.Equals(workspaceId?.Trim(), AllWildcard, StringComparison.Ordinal);
        var normalized = allWorkspaces ? AllWildcard : PathPatternMatcher.NormalizeWorkspaceId(workspaceId);
        return (allWorkspaces, normalized);
    }

    private static (int PrNumber, string HeadSha, string Role) ValidateReviewClaimTarget(ReviewClaimRequest request)
    {
        if (!request.PrNumber.HasValue || request.PrNumber.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "bus_review_claim requires positive integer \"pr_number\".");
        }

        var headSha = NormalizeHeadSha(request.HeadSha, required: true, nameof(request))!;
        var role = (request.Role ?? string.Empty).Trim().ToLowerInvariant();
        if (!SwarmBusConstants.ValidReviewRoles.Contains(role))
        {
            throw new ArgumentException(
                $"bus_review_claim requires \"role\" to be one of: {string.Join(", ", SwarmBusConstants.ValidReviewRoles)}.",
                nameof(request));
        }

        return (request.PrNumber.Value, headSha, role);
    }

    private static string? NormalizeOptionalReviewRole(string? rawRole, string paramName)
    {
        if (string.IsNullOrWhiteSpace(rawRole))
        {
            return null;
        }

        var role = rawRole.Trim().ToLowerInvariant();
        if (!SwarmBusConstants.ValidReviewRoles.Contains(role))
        {
            throw new ArgumentException(
                $"bus_review_claim list \"role\" must be one of: {string.Join(", ", SwarmBusConstants.ValidReviewRoles)}.",
                paramName);
        }

        return role;
    }

    private static string? NormalizeHeadSha(string? rawSha, bool required, string paramName)
    {
        var trimmed = (rawSha ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            if (required)
            {
                throw new ArgumentException("bus_review_claim requires a 40-character hexadecimal \"head_sha\".", paramName);
            }

            return null;
        }

        if (!HeadShaRegex().IsMatch(trimmed))
        {
            throw new ArgumentException("bus_review_claim \"head_sha\" must be a 40-character hexadecimal SHA.", paramName);
        }

        return trimmed.ToLowerInvariant();
    }

    private static string NormalizePriority(string? rawPriority)
    {
        var candidate = (rawPriority ?? NormalPriority).Trim().ToLowerInvariant();
        return SwarmBusConstants.ValidPriorities.Contains(candidate) ? candidate : NormalPriority;
    }

    private static string RequireNonEmpty(string? value, string errorMessage, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException(errorMessage, paramName);
        }

        return value.Trim();
    }

    private static void PruneExpiredLocksInTx(SqliteConnection connection, DateTimeOffset now)
    {
        var expiredIds = new List<long>();
        using (var selectCmd = connection.CreateCommand())
        {
            selectCmd.CommandText = "SELECT id, expires_at_utc FROM locks";
            using var reader = selectCmd.ExecuteReader();
            while (reader.Read())
            {
                var id = reader.GetInt64(0);
                var expiresAt = GetNullableUtc(reader, 1) ?? DateTimeOffset.MinValue;
                if (expiresAt <= now)
                {
                    expiredIds.Add(id);
                }
            }
        }

        DeleteLocksByIdInTx(connection, expiredIds);
    }

    private static List<PathLockLease> ReadLocksInWorkspace(SqliteConnection connection, string workspaceId)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, agent, path_pattern, pr_number, claimed_at_utc, expires_at_utc
            FROM locks
            WHERE workspace_id = @workspaceId
            ORDER BY id ASC
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        return ReadLocksFromCommand(cmd);
    }

    private static List<PathLockLease> ReadAllLocks(SqliteConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, agent, path_pattern, pr_number, claimed_at_utc, expires_at_utc
            FROM locks
            ORDER BY id ASC
            """;
        return ReadLocksFromCommand(cmd);
    }

    private static List<PathLockLease> ReadLocksFromCommand(SqliteCommand cmd)
    {
        var locks = new List<PathLockLease>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            locks.Add(new PathLockLease(
                Id: reader.GetInt64(0),
                Agent: reader.GetString(2),
                PathPattern: reader.GetString(3),
                PrNumber: GetNullableInt32(reader, 4),
                ClaimedAtUtc: ParseUtcOrMin(reader.GetString(5)),
                ExpiresAtUtc: ParseUtcOrMin(reader.GetString(6)),
                WorkspaceId: GetStringOrDefault(reader, 1, SwarmBusConstants.DefaultWorkspaceId)));
        }

        return locks;
    }

    private static List<SwarmMessageEnvelope> ReadMessages(SqliteCommand cmd)
    {
        var messages = new List<SwarmMessageEnvelope>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            messages.Add(MapMessageEnvelope(reader));
        }

        return messages;
    }

    private static SwarmMessageEnvelope MapMessageEnvelope(SqliteDataReader reader) =>
        new(
            Id: reader.GetInt64(0),
            CreatedAtUtc: ParseUtcOrMin(reader.GetString(2)),
            Sender: reader.GetString(3),
            Recipient: reader.GetString(4),
            Topic: reader.GetString(5),
            Body: reader.GetString(6),
            PrNumber: GetNullableInt32(reader, 7),
            IssueNumber: GetNullableInt32(reader, 8),
            Priority: GetStringOrDefault(reader, 9, NormalPriority),
            ReplyToId: GetNullableInt64(reader, 10),
            Status: GetStringOrDefault(reader, 11, StatusUnread),
            AckedBy: GetNullableString(reader, 12),
            AckedAtUtc: GetNullableUtc(reader, 13),
            State: GetNullableString(reader, 14),
            WakeOk: GetNullableBool(reader, 15),
            WakeState: GetNullableString(reader, 16),
            WakeAttemptExpiresAtUtc: GetNullableUtc(reader, 17),
            WorkspaceId: GetStringOrDefault(reader, 1, SwarmBusConstants.DefaultWorkspaceId));

    private static ReviewClaim? ReadReviewClaim(SqliteConnection connection, string workspaceId, int prNumber, string headSha, string role)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, pr_number, head_sha, role, agent, claimed_at_utc, status, expires_at_utc, completed_at_utc
            FROM review_claims
            WHERE workspace_id = @workspaceId
              AND pr_number = @prNumber
              AND lower(head_sha) = @headSha
              AND role = @role
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamPrNumber, prNumber);
        cmd.Parameters.AddWithValue(ParamHeadSha, headSha);
        cmd.Parameters.AddWithValue(ParamRole, role);

        using var reader = cmd.ExecuteReader();
        return reader.Read() ? MapReviewClaim(reader) : null;
    }

    private static ReviewClaim MapReviewClaim(SqliteDataReader reader) =>
        new(
            Id: reader.GetInt64(0),
            PrNumber: reader.GetInt32(2),
            HeadSha: reader.GetString(3).ToLowerInvariant(),
            Role: reader.GetString(4),
            Agent: reader.GetString(5),
            ClaimedAtUtc: ParseUtcOrMin(reader.GetString(6)),
            Status: GetStringOrDefault(reader, 7, StatusClaimed),
            ExpiresAtUtc: GetNullableUtc(reader, 8),
            CompletedAtUtc: GetNullableUtc(reader, 9),
            WorkspaceId: GetStringOrDefault(reader, 1, SwarmBusConstants.DefaultWorkspaceId));

    private static WorkQueueItem? ReadWorkQueueItem(SqliteConnection connection, string workspaceId, int issueNumber)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, issue_number, title, priority, risk_class, status,
                   claimed_by, claimed_at_utc, lease_expires_utc, updated_at_utc
            FROM work_queue
            WHERE workspace_id = @workspaceId AND issue_number = @issueNumber
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamIssueNumber, issueNumber);

        using var reader = cmd.ExecuteReader();
        return reader.Read() ? MapWorkQueueItem(reader) : null;
    }

    private static WorkQueueItem? ReadWorkQueueItemById(SqliteConnection connection, long id)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, issue_number, title, priority, risk_class, status,
                   claimed_by, claimed_at_utc, lease_expires_utc, updated_at_utc
            FROM work_queue
            WHERE id = @id
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue(ParamId, id);

        using var reader = cmd.ExecuteReader();
        return reader.Read() ? MapWorkQueueItem(reader) : null;
    }

    private static List<WorkQueueItem> ReadWorkQueueItems(SqliteConnection connection, string workspaceId, string? statusFilter)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, workspace_id, issue_number, title, priority, risk_class, status,
                   claimed_by, claimed_at_utc, lease_expires_utc, updated_at_utc
            FROM work_queue
            WHERE workspace_id = @workspaceId
              AND (@status IS NULL OR status = @status)
            ORDER BY priority ASC, id ASC
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamStatus, (object?)statusFilter ?? DBNull.Value);

        var items = new List<WorkQueueItem>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            items.Add(MapWorkQueueItem(reader));
        }

        return items;
    }

    private static WorkQueueItem MapWorkQueueItem(SqliteDataReader reader) =>
        new(
            Id: reader.GetInt64(0),
            IssueNumber: reader.GetInt32(2),
            Title: reader.GetString(3),
            Priority: reader.GetInt32(4),
            RiskClass: GetNullableString(reader, 5),
            Status: reader.GetString(6),
            ClaimedBy: GetNullableString(reader, 7),
            ClaimedAtUtc: GetNullableUtc(reader, 8),
            LeaseExpiresUtc: GetNullableUtc(reader, 9),
            UpdatedAtUtc: ParseUtcOrMin(reader.GetString(10)),
            WorkspaceId: GetStringOrDefault(reader, 1, SwarmBusConstants.DefaultWorkspaceId));

    private static Dictionary<string, string> ReadMetaMap(SqliteConnection connection, string workspaceId)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT key, value
            FROM meta
            WHERE workspace_id = @workspaceId
            ORDER BY key ASC
            """;
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            map[reader.GetString(0)] = reader.GetString(1);
        }

        return map;
    }

    private static int ReadMetaInt(SqliteConnection connection, string workspaceId, string key)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT value FROM meta WHERE workspace_id = @workspaceId AND key = @key LIMIT 1";
        cmd.Parameters.AddWithValue(ParamWorkspaceId, workspaceId);
        cmd.Parameters.AddWithValue(ParamKey, key);
        var raw = cmd.ExecuteScalar() as string;
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : 0;
    }

    private static void MigrateAndCreateSchema(SqliteConnection connection)
    {
        EnsureMessagesTable(connection);
        EnsureLocksTable(connection);
        EnsureReviewClaimsTable(connection);
        EnsureWorkQueueTable(connection);
        EnsureMetaTable(connection);
        EnsureVirtualMasterSchema(connection);
    }

    private static void EnsureMessagesTable(SqliteConnection connection)
    {
        ExecuteNonQuery(connection, """
            CREATE TABLE IF NOT EXISTS messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id TEXT NOT NULL DEFAULT 'default',
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
              acked_at_utc TEXT,
              state TEXT,
              wake_ok INTEGER,
              wake_state TEXT CHECK(wake_state IN ('pending','ok','failed')),
              wake_attempt_expires_at_utc TEXT
            );
            """);

        EnsureTableColumns(connection, TableMessages, [
            (ColWorkspaceId, "ALTER TABLE messages ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'default';"),
            ("state", "ALTER TABLE messages ADD COLUMN state TEXT;"),
            ("wake_ok", "ALTER TABLE messages ADD COLUMN wake_ok INTEGER;"),
            ("wake_state", "ALTER TABLE messages ADD COLUMN wake_state TEXT;"),
            ("wake_attempt_expires_at_utc", "ALTER TABLE messages ADD COLUMN wake_attempt_expires_at_utc TEXT;")
        ]);

        ExecuteNonQuery(connection, """
            CREATE INDEX IF NOT EXISTS idx_messages_recipient_status
              ON messages (recipient, status, id DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_workspace_recipient_status
              ON messages (workspace_id, recipient, status, id DESC);
            """);
    }

    private static void EnsureLocksTable(SqliteConnection connection)
    {
        ExecuteNonQuery(connection, """
            CREATE TABLE IF NOT EXISTS locks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id TEXT NOT NULL DEFAULT 'default',
              agent TEXT NOT NULL,
              path_pattern TEXT NOT NULL,
              pr_number INTEGER,
              claimed_at_utc TEXT NOT NULL,
              expires_at_utc TEXT NOT NULL,
              UNIQUE(workspace_id, path_pattern)
            );
            """);

        ExecuteNonQuery(connection, """
            CREATE INDEX IF NOT EXISTS idx_locks_expires
              ON locks (expires_at_utc);
            """);

        if (GetTableColumns(connection, TableLocks).Contains(ColWorkspaceId))
        {
            ExecuteNonQuery(connection, """
                CREATE INDEX IF NOT EXISTS idx_locks_workspace
                  ON locks (workspace_id, expires_at_utc);
                """);
        }
    }

    private static void EnsureReviewClaimsTable(SqliteConnection connection)
    {
        ExecuteNonQuery(connection, """
            CREATE TABLE IF NOT EXISTS review_claims (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id TEXT NOT NULL DEFAULT 'default',
              pr_number INTEGER NOT NULL,
              head_sha TEXT NOT NULL,
              role TEXT NOT NULL,
              agent TEXT NOT NULL,
              claimed_at_utc TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'claimed' CHECK(status IN ('claimed','completed','released')),
              expires_at_utc TEXT,
              completed_at_utc TEXT,
              UNIQUE(workspace_id, pr_number, head_sha, role)
            );
            """);

        EnsureTableColumns(connection, TableReviewClaims, [
            (ColStatus, "ALTER TABLE review_claims ADD COLUMN status TEXT NOT NULL DEFAULT 'claimed';"),
            ("expires_at_utc", "ALTER TABLE review_claims ADD COLUMN expires_at_utc TEXT;"),
            ("completed_at_utc", "ALTER TABLE review_claims ADD COLUMN completed_at_utc TEXT;")
        ]);
    }

    private static void EnsureWorkQueueTable(SqliteConnection connection)
    {
        ExecuteNonQuery(connection, """
            CREATE TABLE IF NOT EXISTS work_queue (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id TEXT NOT NULL DEFAULT 'default',
              issue_number INTEGER NOT NULL,
              title TEXT NOT NULL,
              priority INTEGER NOT NULL,
              risk_class TEXT,
              status TEXT NOT NULL CHECK(status IN ('queued','claimed','done','removed')),
              claimed_by TEXT,
              claimed_at_utc TEXT,
              lease_expires_utc TEXT,
              updated_at_utc TEXT NOT NULL,
              UNIQUE(workspace_id, issue_number)
            );
            """);

        if (GetTableColumns(connection, TableWorkQueue).Contains(ColWorkspaceId))
        {
            ExecuteNonQuery(connection, """
                CREATE INDEX IF NOT EXISTS idx_work_queue_status_priority
                  ON work_queue (workspace_id, status, priority ASC, id ASC);
                """);
        }
    }

    private static void EnsureMetaTable(SqliteConnection connection)
    {
        ExecuteNonQuery(connection, """
            CREATE TABLE IF NOT EXISTS meta (
              workspace_id TEXT NOT NULL DEFAULT 'default',
              key TEXT NOT NULL,
              value TEXT NOT NULL,
              updated_at_utc TEXT NOT NULL,
              PRIMARY KEY (workspace_id, key)
            );
            """);
    }

    private static bool TableExists(SqliteConnection connection, string tableName)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name LIMIT 1";
        cmd.Parameters.AddWithValue("@name", tableName);
        return cmd.ExecuteScalar() is not null;
    }

    private static HashSet<string> GetTableColumns(SqliteConnection connection, string tableName)
    {
        var pragmaSql = tableName switch
        {
            TableMessages => "PRAGMA table_info(messages);",
            TableLocks => "PRAGMA table_info(locks);",
            TableReviewClaims => "PRAGMA table_info(review_claims);",
            TableWorkQueue => "PRAGMA table_info(work_queue);",
            TableMeta => "PRAGMA table_info(meta);",
            TableAgentSessions => "PRAGMA table_info(agent_sessions);",
            TableProjectState => "PRAGMA table_info(project_state);",
            TableProjectIdeas => "PRAGMA table_info(project_ideas);",
            _ => throw new ArgumentOutOfRangeException(nameof(tableName), $"Unsupported SwarmBus table '{tableName}'.")
        };

        var cols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = pragmaSql;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            cols.Add(reader.GetString(1));
        }

        return cols;
    }

    private static void EnsureTableColumns(SqliteConnection connection, string tableName, (string Name, string AlterSql)[] columnDefs)
    {
        var existingCols = GetTableColumns(connection, tableName);
        foreach (var (colName, alterSql) in columnDefs)
        {
            if (!existingCols.Contains(colName))
            {
                ExecuteNonQuery(connection, alterSql);
            }
        }
    }

    private T ExecuteRead<T>(Func<SqliteConnection, T> action)
    {
        EnsureInitialized();
        return ExecuteWithBusyRetry(() =>
        {
            using var connection = OpenConnectionCore();
            return action(connection);
        });
    }

    private async Task<T> ExecuteReadAsync<T>(Func<SqliteConnection, T> action, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await ExecuteWithBusyRetryAsync(_ =>
        {
            using var connection = OpenConnectionCore();
            return Task.FromResult(action(connection));
        }, cancellationToken).ConfigureAwait(false);
    }

    private T ExecuteReadTransaction<T>(Func<SqliteConnection, T> action)
    {
        EnsureInitialized();
        return ExecuteWithBusyRetry(() =>
        {
            using var connection = OpenConnectionCore();
            return RunInReadTransactionCore(connection, () => action(connection));
        });
    }

    private async Task<T> ExecuteReadTransactionAsync<T>(Func<SqliteConnection, T> action, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await ExecuteWithBusyRetryAsync(_ =>
        {
            using var connection = OpenConnectionCore();
            return Task.FromResult(RunInReadTransactionCore(connection, () => action(connection)));
        }, cancellationToken).ConfigureAwait(false);
    }

    private T ExecuteInImmediateTransaction<T>(Func<SqliteConnection, T> action)
    {
        EnsureInitialized();
        return ExecuteWithBusyRetry(() =>
        {
            using var connection = OpenConnectionCore();
            return RunInImmediateTransactionCore(connection, () => action(connection));
        });
    }

    private async Task<T> ExecuteInImmediateTransactionAsync<T>(Func<SqliteConnection, T> action, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await ExecuteWithBusyRetryAsync(_ =>
        {
            using var connection = OpenConnectionCore();
            return Task.FromResult(RunInImmediateTransactionCore(connection, () => action(connection)));
        }, cancellationToken).ConfigureAwait(false);
    }

    private SqliteConnection OpenConnectionCore()
    {
        ThrowIfProtectedLegacyPath(_dbPath);

        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = _dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Default,
            Pooling = false,
            DefaultTimeout = 1
        }.ToString();

        var connection = new SqliteConnection(connectionString);
        try
        {
            connection.Open();
            // CRITICAL: always set busy_timeout FIRST before any WAL or write pragma.
            ExecuteNonQuery(connection, "PRAGMA busy_timeout = 1000;");
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    private static T RunInReadTransactionCore<T>(SqliteConnection connection, Func<T> action)
    {
        ExecuteNonQuery(connection, "BEGIN;");
        try
        {
            var result = action();
            ExecuteNonQuery(connection, "COMMIT;");
            return result;
        }
        catch
        {
            TryRollback(connection);
            throw;
        }
    }

    private static T RunInImmediateTransactionCore<T>(SqliteConnection connection, Func<T> action)
    {
        ExecuteNonQuery(connection, "BEGIN IMMEDIATE;");
        try
        {
            var result = action();
            ExecuteNonQuery(connection, "COMMIT;");
            return result;
        }
        catch
        {
            TryRollback(connection);
            throw;
        }
    }

    private static void TryRollback(SqliteConnection connection)
    {
        try
        {
            ExecuteNonQuery(connection, "ROLLBACK;");
        }
        catch
        {
            // Ignore rollback error if transaction is already closed.
        }
    }

    private T ExecuteWithBusyRetry<T>(Func<T> operation)
    {
        for (var attempt = 0; attempt <= _maxBusyRetries; attempt++)
        {
            try
            {
                return operation();
            }
            catch (SqliteException ex) when (IsBusyOrLocked(ex) && attempt < _maxBusyRetries)
            {
                Thread.Sleep(ComputeRetryDelayMs(attempt));
            }
        }

        throw new InvalidOperationException("Busy retry loop terminated unexpectedly.");
    }

    private async Task<T> ExecuteWithBusyRetryAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt <= _maxBusyRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (SqliteException ex) when (IsBusyOrLocked(ex) && attempt < _maxBusyRetries)
            {
                await Task.Delay(ComputeRetryDelayMs(attempt), cancellationToken).ConfigureAwait(false);
            }
        }

        throw new InvalidOperationException("Async busy retry loop terminated unexpectedly.");
    }

    private static int ComputeRetryDelayMs(int attempt)
    {
        var baseDelayMs = Math.Min(250, 15 * Math.Pow(1.5, attempt));
        var jitterMs = Random.Shared.Next(1, 25);
        return (int)baseDelayMs + jitterMs;
    }

    private static bool IsBusyOrLocked(SqliteException ex) =>
        ex.SqliteErrorCode is SqliteBusyErrorCode or SqliteLockedErrorCode;

    private static void ExecuteNonQuery(SqliteConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static string GetStringOrDefault(SqliteDataReader reader, int ordinal, string fallback) =>
        reader.IsDBNull(ordinal) ? fallback : reader.GetString(ordinal);

    private static string? GetNullableString(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int? GetNullableInt32(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    private static long? GetNullableInt64(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);

    private static bool? GetNullableBool(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal) != 0;

    private static DateTimeOffset? GetNullableUtc(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : ParseUtcNullable(reader.GetString(ordinal));

    private static string FormatUtc(DateTimeOffset utc) =>
        utc.ToUniversalTime().ToString(IsoFormat, CultureInfo.InvariantCulture);

    private static DateTimeOffset? ParseUtcNullable(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return DateTimeOffset.TryParse(
            raw,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsed)
            ? parsed
            : null;
    }

    private static DateTimeOffset ParseUtcOrMin(string? raw) =>
        ParseUtcNullable(raw) ?? DateTimeOffset.MinValue;
}
