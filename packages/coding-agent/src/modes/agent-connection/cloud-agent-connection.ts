import type { AgentMessage, ThinkingLevel } from "@earendil-works/pi-agent-core";
import {
	type AssistantMessage,
	getModels,
	type ImageContent,
	type ServiceTier,
	type Transport,
	type Usage,
} from "@earendil-works/pi-ai";
import type { AgentSessionMessageReceipt, AgentSessionMessageSafetyStatus } from "../../core/agent-messages.js";
import type { AgentAutonomousStatus } from "../../core/autonomous.js";
import type { BashResult } from "../../core/bash-executor.js";
import type { CompactionResult } from "../../core/compaction/index.js";
import type { ContextTreeNode } from "../../core/context-tree.js";
import type {
	AgentCronJob,
	AgentHeartbeatDeliveryMode,
	AgentHeartbeatManagementAction,
	AgentHeartbeatUpdateAction,
} from "../../core/cron-jobs.js";
import { emptyGoalState } from "../../core/goals.js";
import type { RefinementResult } from "../../core/refinement/index.js";
import type { RlmMaxDepthStatus, SetRlmMaxDepthResult } from "../../core/rlm-max-depth.js";
import type { DeleteSessionFileResult } from "../../core/session-file-actions.js";
import type { SessionStats } from "../../core/session-stats.js";
import {
	type CloudAgentClient,
	type CloudAgentCreateOptions,
	CloudAgentHttpError,
	type CloudAgentSnapshotDto,
	type CloudSseEvent,
} from "./cloud-agent-client.js";
import {
	type AgentConnection,
	type AgentConnectionBeforeSessionInvalidateListener,
	type AgentConnectionEvent,
	type AgentConnectionEventListener,
	type AgentConnectionExecuteBashOptions,
	type AgentConnectionExtensionUiResponse,
	type AgentConnectionForkOptions,
	type AgentConnectionHeartbeat,
	type AgentConnectionModel,
	type AgentConnectionModelCatalog,
	type AgentConnectionModelCycleResult,
	type AgentConnectionNavigateTreeOptions,
	type AgentConnectionNavigateTreeResult,
	type AgentConnectionNewSessionOptions,
	AgentConnectionPromptAdmissionError,
	type AgentConnectionPromptOptions,
	type AgentConnectionQueueMode,
	type AgentConnectionQueueState,
	type AgentConnectionResourceSnapshot,
	type AgentConnectionRlmChildAgentSnapshot,
	type AgentConnectionSavedSessionInfo,
	type AgentConnectionSavedSessionScope,
	type AgentConnectionScopedModel,
	type AgentConnectionSessionContext,
	type AgentConnectionSessionEvent,
	type AgentConnectionSessionHeader,
	type AgentConnectionSessionListCallbacks,
	type AgentConnectionSessionTreeNode,
	type AgentConnectionSessionWatcher,
	type AgentConnectionSideQuestionTurn,
	type AgentConnectionSlashCommand,
	type AgentConnectionSnapshot,
	type AgentConnectionState,
	type AgentConnectionSwitchSessionOptions,
	type AgentConnectionToolDefinition,
	type AgentConnectionUserMessage,
} from "./types.js";

const CLOUD_MESSAGE_MARKER = /^\[cloud-agent-message-id:[^\]]+\]\s*/;
const RECONNECT_DELAY_MS = 1_000;
const CHILD_REFRESH_MS = 1_500;

export interface CloudAgentConnectionOptions extends CloudAgentCreateOptions {
	attachOnly?: boolean;
}

interface LiveTool {
	id: string;
	name: string;
	args: Record<string, unknown>;
	output: string;
	waitsForOutputItem: boolean;
}

export class CloudAgentConnection implements AgentConnection {
	private readonly listeners = new Set<AgentConnectionEventListener>();
	private readonly beforeSessionInvalidateListeners = new Set<AgentConnectionBeforeSessionInvalidateListener>();
	private readonly abortController = new AbortController();
	private readonly modelCatalog: AgentConnectionModel[];
	private readonly liveAssistants = new Map<string, AssistantMessage>();
	private readonly liveTools = new Map<string, LiveTool>();
	private readonly completedItemIds = new Set<string>();
	private readonly childSnapshots = new Map<string, AgentConnectionRlmChildAgentSnapshot>();
	private messages: AgentMessage[] = [];
	private snapshot: CloudAgentSnapshotDto;
	private queue: AgentConnectionQueueState = { steering: [], followUp: [] };
	private disposed = false;
	private turnActive = false;
	private eventPump: Promise<void> | undefined;
	private childPump: Promise<void> | undefined;
	private readyPromise: Promise<void>;
	private resolveReady!: () => void;
	private rejectReady!: (error: Error) => void;
	private readySettled = false;

	private constructor(
		private readonly client: CloudAgentClient,
		snapshot: CloudAgentSnapshotDto,
	) {
		this.snapshot = snapshot;
		this.modelCatalog = cloudModels(snapshot.model);
		this.readyPromise = new Promise<void>((resolve, reject) => {
			this.resolveReady = resolve;
			this.rejectReady = reject;
		});
	}

	static async connect(client: CloudAgentClient, options: CloudAgentConnectionOptions): Promise<CloudAgentConnection> {
		let snapshot: CloudAgentSnapshotDto;
		try {
			snapshot = await client.getAgent(options.fleetId, options.agentId);
		} catch (error) {
			if (!(error instanceof CloudAgentHttpError) || error.status !== 404 || options.attachOnly) {
				throw error;
			}
			await client.createAgent(options);
			snapshot = await waitForSession(client, options.fleetId, options.agentId);
		}
		if (snapshot.status === "stopped" || snapshot.status === "stopping") {
			throw new Error(
				`Cloud agent ${options.fleetId}/${options.agentId} is ${snapshot.status}; choose a new fleet or agent id`,
			);
		}
		if (!snapshot.sessionId) {
			snapshot = await waitForSession(client, options.fleetId, options.agentId);
		}

		const connection = new CloudAgentConnection(client, snapshot);
		await connection.start();
		return connection;
	}

	subscribe(listener: AgentConnectionEventListener): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	onBeforeSessionInvalidate(listener: AgentConnectionBeforeSessionInvalidateListener): () => void {
		this.beforeSessionInvalidateListeners.add(listener);
		return () => this.beforeSessionInvalidateListeners.delete(listener);
	}

	async getState(): Promise<AgentConnectionState> {
		return this.connectionState();
	}

	async getInitialSnapshot(): Promise<AgentConnectionSnapshot> {
		return this.connectionSnapshot();
	}

	async getMessages(): Promise<AgentMessage[]> {
		return [...this.messages];
	}

	async getSessionHeader(): Promise<AgentConnectionSessionHeader | undefined> {
		return {
			type: "session",
			id: this.snapshot.sessionId ?? this.snapshot.workflowId,
			timestamp: new Date().toISOString(),
			cwd: "/workspace",
			rlmDepth: this.snapshot.parentAgentId ? 1 : 0,
		};
	}

	async getCommands(): Promise<AgentConnectionSlashCommand[]> {
		return [];
	}

	async getResourceSnapshot(): Promise<AgentConnectionResourceSnapshot> {
		return emptyResourceSnapshot();
	}

	async getModelCatalog(): Promise<AgentConnectionModelCatalog> {
		return { models: [...this.modelCatalog], configuredProviders: ["openai"] };
	}

	async getAvailableModels(): Promise<AgentConnectionModel[]> {
		return [...this.modelCatalog];
	}

	async getSessionStats(): Promise<SessionStats> {
		const userMessages = this.messages.filter((message) => message.role === "user").length;
		const assistantMessages = this.messages.filter((message) => message.role === "assistant").length;
		const toolResults = this.messages.filter((message) => message.role === "toolResult").length;
		const usage = this.messages.reduce((total, message) => {
			if (message.role === "assistant") addUsage(total, message.usage);
			return total;
		}, emptyUsage());
		return {
			sessionFile: undefined,
			sessionId: this.snapshot.sessionId ?? this.snapshot.workflowId,
			userMessages,
			assistantMessages,
			toolCalls: this.messages.reduce(
				(count, message) =>
					message.role === "assistant"
						? count + message.content.filter((content) => content.type === "toolCall").length
						: count,
				0,
			),
			toolResults,
			totalMessages: this.messages.length,
			tokens: {
				input: usage.input,
				output: usage.output,
				cacheRead: usage.cacheRead,
				cacheWrite: usage.cacheWrite,
				total: usage.totalTokens,
			},
			cost: usage.cost.total,
			contextUsage: this.connectionState().contextUsage,
		};
	}

	async getContextTree(): Promise<ContextTreeNode> {
		const usage = (await this.getSessionStats()).tokens;
		const rootUsage = usageFromTokens(usage);
		return {
			id: "root",
			label: this.snapshot.agentId,
			status: "active",
			model: { provider: "openai", id: this.snapshot.model },
			ownUsage: rootUsage,
			totalUsage: { ...rootUsage, cost: { ...rootUsage.cost } },
			contextUsage: this.connectionState().contextUsage,
			children: [...this.childSnapshots.values()].map((child) => ({
				id: child.id,
				label: child.label,
				status: child.status,
				model: child.model ? { provider: "openai", id: child.model } : undefined,
				ownUsage: emptyUsage(),
				totalUsage: emptyUsage(),
				children: [],
			})),
		};
	}

	async getSessionContext(): Promise<AgentConnectionSessionContext> {
		return {
			messages: [...this.messages],
			thinkingLevel: this.connectionState().thinkingLevel,
			serviceTier: null,
			model: { provider: "openai", modelId: this.snapshot.model },
		};
	}

	async getSessionTree(): Promise<{ tree: AgentConnectionSessionTreeNode[]; leafId: string | null }> {
		return { tree: [], leafId: null };
	}

	async listSavedSessions(
		_scope: AgentConnectionSavedSessionScope,
		_callbacks?: AgentConnectionSessionListCallbacks,
	): Promise<AgentConnectionSavedSessionInfo[]> {
		return [];
	}

	async getQueue(): Promise<AgentConnectionQueueState> {
		return { steering: [...this.queue.steering], followUp: [...this.queue.followUp] };
	}

	async clearQueue(): Promise<AgentConnectionQueueState> {
		this.queue = { steering: [], followUp: [] };
		return this.getQueue();
	}

	async abortAndClearQueue(): Promise<AgentConnectionQueueState> {
		const queue = await this.clearQueue();
		await this.abort();
		return queue;
	}

	async listCronJobs(_options: { includeInactive?: boolean } = {}): Promise<AgentCronJob[]> {
		return [];
	}

	async listHeartbeats(): Promise<AgentConnectionHeartbeat[]> {
		return [];
	}

	async manageHeartbeat(
		_activeSessionId: string,
		_jobId: string,
		_action: AgentHeartbeatManagementAction,
	): Promise<AgentCronJob> {
		return this.unsupported("Heartbeat management");
	}

	async addCronJob(_schedule: string, _prompt: string): Promise<AgentCronJob> {
		return this.unsupported("Cron jobs");
	}

	async cancelCronJob(_jobId: string): Promise<AgentCronJob> {
		return this.unsupported("Cron jobs");
	}

	async getHeartbeat(): Promise<AgentCronJob | undefined> {
		return undefined;
	}

	async setHeartbeat(
		_schedule: string,
		_instruction: string,
		_deliveryMode?: AgentHeartbeatDeliveryMode,
	): Promise<AgentCronJob> {
		return this.unsupported("Heartbeats");
	}

	async updateHeartbeat(_action: AgentHeartbeatUpdateAction): Promise<AgentCronJob | undefined> {
		return this.unsupported("Heartbeats");
	}

	async sendAgentMessage(targetActiveSessionId: string, message: string): Promise<AgentSessionMessageReceipt> {
		const accepted = await this.client.sendMessage(this.snapshot.fleetId, targetActiveSessionId, message);
		return {
			id: accepted.messageId ?? `${Date.now()}`,
			source: "agent_message",
			target: {
				activeSessionId: targetActiveSessionId,
				sessionId: targetActiveSessionId,
				sessionName: targetActiveSessionId,
			},
			from: {
				activeSessionId: this.snapshot.agentId,
				sessionId: this.snapshot.sessionId,
				sessionName: this.snapshot.agentId,
			},
			message,
			deliveryStatus: "queued",
			queuedAt: new Date().toISOString(),
		};
	}

	async getAgentMessageStatus(): Promise<AgentSessionMessageSafetyStatus> {
		return messageSafetyStatus(false);
	}

	async pauseAgentMessages(): Promise<AgentSessionMessageSafetyStatus> {
		return this.unsupported("Pausing fleet messages");
	}

	async resumeAgentMessages(): Promise<AgentSessionMessageSafetyStatus> {
		return messageSafetyStatus(false);
	}

	async clearAgentMessages(): Promise<number> {
		return 0;
	}

	async getUserMessagesForForking(): Promise<AgentConnectionUserMessage[]> {
		return this.messages.flatMap((message, index) =>
			message.role === "user" ? [{ entryId: `cloud-user-${index}`, text: messageText(message.content) }] : [],
		);
	}

	async getLastAssistantText(): Promise<string | undefined> {
		for (let index = this.messages.length - 1; index >= 0; index--) {
			const message = this.messages[index];
			if (message.role !== "assistant") continue;
			const text = messageText(message.content);
			if (text) return text;
		}
		return undefined;
	}

	async getSystemPrompt(): Promise<string> {
		return "The cloud control plane owns this managed agent's system instructions.";
	}

	async getToolDefinition(name: string): Promise<AgentConnectionToolDefinition | undefined> {
		const labels: Record<string, string> = {
			shell: "Remote shell",
			command_execution: "Remote shell",
			mcp_call: "MCP call",
			"native.spawn_agent": "Spawn native subagent",
			"native.send_input": "Message native subagent",
			"native.wait": "Wait for native subagents",
			"native.resume_agent": "Resume native subagent",
			"native.close_agent": "Close native subagent",
			"native.agent_message": "Native agent message",
			web_search_call: "Web search",
		};
		const label = name.startsWith("fleet.") ? `Fleet · ${name.slice("fleet.".length)}` : labels[name];
		return label
			? {
					name,
					label,
					description: `${label} executed by the remote managed agent`,
					parameters: { type: "object" },
				}
			: undefined;
	}

	async setSessionEntryLabel(_entryId: string, _label: string | undefined): Promise<void> {
		return this.unsupported("Transcript labels");
	}

	async respondToExtensionUiRequest(_requestId: string, _response: AgentConnectionExtensionUiResponse): Promise<void> {
		return this.unsupported("Extension UI requests");
	}

	async prompt(message: string, options: AgentConnectionPromptOptions = {}): Promise<void> {
		if ((options.images?.length ?? 0) > 0) {
			throw new AgentConnectionPromptAdmissionError(
				"Image prompts are not available through the cloud MVP yet",
				"unsupported",
			);
		}
		if (options.signal?.aborted) {
			throw new AgentConnectionPromptAdmissionError("Prompt admission was cancelled", "cancelled");
		}
		try {
			await this.client.sendMessage(this.snapshot.fleetId, this.snapshot.agentId, message, options.signal);
		} catch (error) {
			throw new AgentConnectionPromptAdmissionError("Cloud prompt admission failed", "unknown", {
				cause: error,
			});
		}
		const userMessage = createUserMessage(message);
		this.messages.push(userMessage);
		this.emitSessionEvent({ type: "message_start", message: userMessage });
		this.emitSessionEvent({ type: "message_end", message: userMessage });
		this.snapshot = { ...this.snapshot, status: "running" };
	}

	async promptAndWait(message: string, options?: AgentConnectionPromptOptions): Promise<void> {
		await this.prompt(message, options);
		await this.waitForIdle();
	}

	async startSideQuestion(
		_id: string,
		_question: string,
		_previousTurns?: AgentConnectionSideQuestionTurn[],
	): Promise<void> {
		return this.unsupported("Side conversations");
	}

	async abortSideQuestion(_id: string): Promise<boolean> {
		return false;
	}

	async steer(message: string, images?: ImageContent[]): Promise<void> {
		await this.prompt(message, { images, streamingBehavior: "steer", queueIfBusy: true });
	}

	async followUp(message: string, images?: ImageContent[]): Promise<void> {
		await this.prompt(message, { images, streamingBehavior: "followUp", queueIfBusy: true });
	}

	async abort(): Promise<void> {
		await this.client.cancelTurn(this.snapshot.fleetId, this.snapshot.agentId);
	}

	async cancelRlmChild(childId: string): Promise<boolean> {
		const child = this.childSnapshots.get(childId);
		if (child?.executionKind === "managed-native") return false;
		await this.client.stopAgent(this.snapshot.fleetId, child?.activeSessionId ?? childId);
		return true;
	}

	async waitForIdle(): Promise<void> {
		while (!this.disposed) {
			const snapshot = await this.client.getAgent(this.snapshot.fleetId, this.snapshot.agentId);
			this.snapshot = snapshot;
			if (snapshot.status === "idle" || snapshot.status === "failed" || snapshot.status === "stopped") return;
			await delay(500, this.abortController.signal);
		}
	}

	async waitForHeadlessCompletion(): Promise<AgentAutonomousStatus> {
		await this.waitForIdle();
		return {
			enabled: false,
			continuationsUsed: 0,
			turnsUsed: 0,
			tokensUsed: 0,
			limits: { maxContinuations: 0, maxTurns: 0, maxTokens: 0, timeoutMs: 0 },
			gates: { commands: [], maxRetries: 0, timeoutMs: 0 },
			gateAttempts: {},
		};
	}

	async executeBash(_command: string, _options?: AgentConnectionExecuteBashOptions): Promise<void> {
		return this.unsupported("Direct bang-shell commands");
	}

	async executeBashAndWait(_command: string): Promise<BashResult> {
		return this.unsupported("Direct bang-shell commands");
	}

	async abortBash(): Promise<void> {}

	async setModel(_provider: string, _modelId: string): Promise<AgentConnectionModel> {
		return this.unsupported("Changing a managed agent model in place");
	}

	async cycleModel(
		_direction: "forward" | "backward" = "forward",
	): Promise<AgentConnectionModelCycleResult | undefined> {
		return this.unsupported("Changing a managed agent model in place");
	}

	async setScopedModels(_scopedModels: AgentConnectionScopedModel[]): Promise<void> {
		return this.unsupported("Model scopes");
	}

	async setThinkingLevel(_level: ThinkingLevel): Promise<void> {
		return this.unsupported("Changing managed-agent reasoning in place");
	}

	async setServiceTier(_serviceTier: ServiceTier): Promise<void> {
		return this.unsupported("Service-tier changes");
	}

	async cycleThinkingLevel(): Promise<ThinkingLevel | undefined> {
		return this.unsupported("Changing managed-agent reasoning in place");
	}

	async setTransport(_transport: Transport): Promise<void> {
		return this.unsupported("Provider transport changes");
	}

	async setSteeringMode(_mode: AgentConnectionQueueMode): Promise<void> {}

	async setFollowUpMode(_mode: AgentConnectionQueueMode): Promise<void> {}

	async setAutoCompactionEnabled(_enabled: boolean): Promise<void> {
		return this.unsupported("Client-side compaction settings");
	}

	async setAutoRetryEnabled(_enabled: boolean): Promise<void> {
		return this.unsupported("Client-side retry settings");
	}

	async compact(_customInstructions?: string): Promise<CompactionResult> {
		return this.unsupported("Manual client-side compaction");
	}

	async refine(_options?: {
		instructions?: string;
		rollbackId?: string;
		global?: boolean;
	}): Promise<RefinementResult> {
		return this.unsupported("Harness refinement");
	}

	async abortCompaction(): Promise<void> {}

	async abortBranchSummary(): Promise<void> {}

	async abortRetry(): Promise<void> {}

	async reload(): Promise<void> {
		await this.refreshFromTranscript(true);
	}

	async newSession(_options?: AgentConnectionNewSessionOptions): Promise<{ cancelled: boolean }> {
		return this.unsupported("Creating another cloud session from this chat");
	}

	async switchSession(
		_sessionPath: string,
		_options?: AgentConnectionSwitchSessionOptions,
	): Promise<{ cancelled: boolean }> {
		return this.unsupported("Switching cloud sessions from this chat");
	}

	async fork(
		_entryId: string,
		_options?: AgentConnectionForkOptions,
	): Promise<{ cancelled: boolean; selectedText?: string }> {
		return this.unsupported("Forking managed-agent transcripts");
	}

	async navigateTree(
		_targetId: string,
		_options?: AgentConnectionNavigateTreeOptions,
	): Promise<AgentConnectionNavigateTreeResult> {
		return this.unsupported("Transcript tree navigation");
	}

	async importFromJsonl(_inputPath: string, _cwdOverride?: string): Promise<{ cancelled: boolean }> {
		return this.unsupported("JSONL import");
	}

	async exportToHtml(_outputPath?: string): Promise<string> {
		return this.unsupported("HTML export");
	}

	async exportToJsonl(_outputPath?: string): Promise<string> {
		return this.unsupported("JSONL export");
	}

	async setSessionName(_name: string): Promise<void> {
		return this.unsupported("Renaming cloud agents");
	}

	async getRlmMaxDepthStatus(): Promise<RlmMaxDepthStatus> {
		return { maxDepth: 1, source: "default" };
	}

	async setRlmMaxDepth(_maxDepth: number, _options?: { global?: boolean }): Promise<SetRlmMaxDepthResult> {
		return this.unsupported("Changing cloud recursion depth from the client");
	}

	async renameSavedSession(_sessionPath: string, _name: string): Promise<void> {
		return this.unsupported("Renaming cloud sessions");
	}

	async deleteSavedSession(_sessionPath: string): Promise<DeleteSessionFileResult> {
		return this.unsupported("Deleting cloud sessions from the local catalog");
	}

	async watchSession(activeSessionId: string): Promise<AgentConnectionSessionWatcher | undefined> {
		const child = [...this.childSnapshots.values()].find(
			(candidate) => candidate.activeSessionId === activeSessionId || candidate.id === activeSessionId,
		);
		if (child?.executionKind === "managed-native") return undefined;
		let connection: CloudAgentConnection;
		try {
			connection = await CloudAgentConnection.connect(this.client, {
				fleetId: this.snapshot.fleetId,
				agentId: activeSessionId,
				instructions: "attach to durable cloud agent",
				model: child?.model ?? this.snapshot.model,
				reasoningEffort: this.snapshot.reasoningEffort,
				attachOnly: true,
			});
		} catch {
			return undefined;
		}
		return {
			getMessages: () => connection.getMessages(),
			getCommands: () => connection.getCommands(),
			subscribe: (listener) => connection.subscribe(listener),
			getToolDefinition: (name) => connection.getToolDefinition(name),
			close: () => connection.dispose(),
		};
	}

	async dispose(): Promise<void> {
		if (this.disposed) return;
		this.disposed = true;
		this.abortController.abort();
		await Promise.allSettled(
			[this.eventPump, this.childPump].filter((task): task is Promise<void> => task !== undefined),
		);
		this.listeners.clear();
		this.beforeSessionInvalidateListeners.clear();
	}

	private async start(): Promise<void> {
		this.eventPump = this.runEventPump();
		this.childPump = this.runChildPump();
		await this.readyPromise;
	}

	private async runEventPump(): Promise<void> {
		let connectedOnce = false;
		while (!this.disposed) {
			try {
				if (connectedOnce) {
					this.emit({ type: "connection_status", status: "reconnecting" });
				}
				for await (const event of this.client.streamEvents(
					this.snapshot.fleetId,
					this.snapshot.agentId,
					this.abortController.signal,
				)) {
					if (event.event === "cloud.connected") {
						await this.refreshFromTranscript(connectedOnce);
						if (connectedOnce) this.emit({ type: "connection_status", status: "connected" });
						connectedOnce = true;
						this.settleReady();
						continue;
					}
					if (event.event === "cloud.error") {
						throw new Error(eventString(event.data, "error") ?? "Cloud event stream failed");
					}
					await this.handleCloudEvent(event);
				}
				if (!this.disposed) throw new Error("Cloud event stream ended");
			} catch (error) {
				if (this.disposed || this.abortController.signal.aborted) return;
				if (!connectedOnce && !this.readySettled) {
					this.rejectReady(error instanceof Error ? error : new Error(String(error)));
					this.readySettled = true;
					return;
				}
				this.emit({
					type: "connection_status",
					status: "reconnecting",
					error: error instanceof Error ? error.message : String(error),
				});
				await delay(RECONNECT_DELAY_MS, this.abortController.signal).catch(() => undefined);
			}
		}
	}

	private async runChildPump(): Promise<void> {
		while (!this.disposed) {
			await this.refreshChildren().catch(() => undefined);
			await delay(CHILD_REFRESH_MS, this.abortController.signal).catch(() => undefined);
		}
	}

	private async refreshFromTranscript(emitResync: boolean): Promise<void> {
		const transcript = await this.client.getTranscript(
			this.snapshot.fleetId,
			this.snapshot.agentId,
			this.abortController.signal,
		);
		for (const listener of [...this.beforeSessionInvalidateListeners]) listener();
		this.snapshot = transcript.snapshot;
		this.messages = transcript.items.flatMap((item) => itemToMessages(item, this.currentModel()));
		this.completedItemIds.clear();
		for (const item of transcript.items) {
			const id = eventString(item, "id");
			if (id && eventString(item, "status") !== "in_progress") this.completedItemIds.add(id);
		}
		this.liveAssistants.clear();
		this.liveTools.clear();
		for (const [id, child] of this.childSnapshots) {
			if (child.executionKind === "managed-native") this.childSnapshots.delete(id);
		}
		for (const child of managedSubagentsFromItems(transcript.items)) {
			this.childSnapshots.set(child.id, child);
		}
		this.turnActive = transcript.snapshot.status === "running";
		await this.refreshChildren();
		if (emitResync) this.emit({ type: "session_resynced", snapshot: this.connectionSnapshot() });
	}

	private async refreshChildren(): Promise<void> {
		const root = await this.client.getAgent(
			this.snapshot.fleetId,
			this.snapshot.agentId,
			this.abortController.signal,
		);
		this.snapshot = root;
		const results = await Promise.allSettled(
			root.childAgentIds.map((childId) => this.client.getAgent(root.fleetId, childId, this.abortController.signal)),
		);
		for (const result of results) {
			if (result.status !== "fulfilled") continue;
			const child = childSnapshot(result.value);
			const previous = this.childSnapshots.get(child.id);
			if (JSON.stringify(previous) === JSON.stringify(child)) continue;
			this.childSnapshots.set(child.id, child);
			this.emitSessionEvent({ type: "rlm_child_update", child });
		}
	}

	private async handleCloudEvent(event: CloudSseEvent): Promise<void> {
		const type = eventString(event.data, "type") ?? event.event;
		const itemId = eventString(event.data, "item_id");
		if (itemId && this.completedItemIds.has(itemId)) return;

		switch (type) {
			case "session.turn.created":
			case "session.turn.in_progress":
				this.beginTurn();
				break;
			case "session.turn.output_text.added":
				if (itemId) this.ensureLiveAssistant(itemId, "text");
				break;
			case "session.turn.output_text.delta":
				if (itemId) this.appendAssistantContent(itemId, "text", eventString(event.data, "delta") ?? "");
				break;
			case "session.turn.output_text.done":
				if (itemId) this.finishAssistantContent(itemId, "text", eventString(event.data, "text") ?? "");
				break;
			case "session.turn.reasoning_summary_text.added":
				if (itemId) this.ensureLiveAssistant(itemId, "thinking");
				break;
			case "session.turn.reasoning_summary_text.delta":
				if (itemId) this.appendAssistantContent(itemId, "thinking", eventString(event.data, "delta") ?? "");
				break;
			case "session.turn.reasoning_summary_text.done":
				if (itemId) this.finishAssistantContent(itemId, "thinking", eventString(event.data, "text") ?? "");
				break;
			case "session.turn.item.added": {
				const item = eventRecord(event.data, "item");
				if (item) {
					this.updateManagedSubagentFromItem(item);
					this.beginTool(item);
				}
				break;
			}
			case "agent.output.command_execution_output.delta":
				if (itemId) this.updateToolOutput(itemId, eventString(event.data, "delta") ?? "");
				break;
			case "session.turn.item.done": {
				const item = eventRecord(event.data, "item");
				if (item) this.finishItem(item);
				break;
			}
			case "session.turn.completed":
			case "session.idle":
				this.finishTurn("stop");
				break;
			case "session.turn.cancelled":
				this.finishTurn("aborted", "Operation cancelled");
				break;
			case "session.turn.failed":
			case "session.failed":
				this.finishTurn("error", eventError(event.data));
				break;
			case "session.environment.failed":
				this.finishTurn("error", "Remote sandbox connection failed");
				break;
			case "session.subagent.created":
			case "session.subagent.closed":
				this.updateManagedSubagent(event.data, type === "session.subagent.closed");
				break;
		}
	}

	private updateManagedSubagent(data: Record<string, unknown>, closed: boolean): void {
		const value = eventRecord(data, "subagent");
		if (!value) return;
		const id = eventString(value, "id");
		if (!id) return;
		const key = `managed-native:${id}`;
		const previous = this.childSnapshots.get(key);
		const openedAt = eventNumber(value, "opened_at");
		const closedAt = eventNumber(value, "closed_at");
		const child: AgentConnectionRlmChildAgentSnapshot = {
			id: key,
			executionKind: "managed-native",
			activeSessionId: closed ? undefined : id,
			label: previous?.label ?? nativeSubagentLabel(id),
			model: previous?.model,
			status: closed ? "done" : (previous?.status ?? "running"),
			durationMs:
				openedAt !== undefined && closedAt !== undefined ? Math.max(0, closedAt - openedAt) * 1_000 : undefined,
			answerPreview: previous?.answerPreview,
			recap: closed
				? "native subagent closed"
				: previous?.status === "done"
					? "native subagent idle"
					: "native subagent running",
			sessionDir: "/workspace",
			activity: closed || previous?.status === "done" ? undefined : { kind: "executing" },
		};
		this.childSnapshots.set(key, child);
		this.emitSessionEvent({ type: "rlm_child_update", child });
	}

	private updateManagedSubagentFromItem(item: Record<string, unknown>): void {
		const child = applyManagedSubagentItem(this.childSnapshots, item);
		if (child) this.emitSessionEvent({ type: "rlm_child_update", child });
	}

	private beginTurn(): void {
		if (this.turnActive) return;
		this.turnActive = true;
		this.snapshot = { ...this.snapshot, status: "running" };
		this.emitSessionEvent({ type: "agent_start" });
		this.emitSessionEvent({ type: "turn_start" });
	}

	private finishTurn(reason: "stop" | "error" | "aborted", errorMessage?: string): void {
		if (reason !== "stop" && this.liveAssistants.size === 0) {
			const id = `cloud-error-${Date.now()}`;
			const message = this.ensureLiveAssistant(id, "text");
			message.content = [{ type: "text", text: errorMessage ?? "Cloud agent turn failed" }];
			message.stopReason = reason;
			message.errorMessage = errorMessage;
		}
		let lastAssistant: AssistantMessage | undefined;
		for (const [id, message] of this.liveAssistants) {
			message.stopReason = reason;
			if (errorMessage) message.errorMessage = errorMessage;
			this.emitSessionEvent({ type: "message_end", message });
			this.messages.push(message);
			this.completedItemIds.add(id);
			lastAssistant = message;
		}
		this.liveAssistants.clear();
		if (lastAssistant) this.emitSessionEvent({ type: "turn_end", message: lastAssistant, toolResults: [] });
		if (this.turnActive) this.emitSessionEvent({ type: "agent_end", messages: lastAssistant ? [lastAssistant] : [] });
		this.turnActive = false;
		this.snapshot = {
			...this.snapshot,
			status: reason === "error" ? "failed" : "idle",
			lastError: errorMessage,
		};
	}

	private ensureLiveAssistant(itemId: string, contentType: "text" | "thinking"): AssistantMessage {
		const existing = this.liveAssistants.get(itemId);
		if (existing) return existing;
		this.beginTurn();
		const message = createAssistantMessage(this.currentModel(), contentType);
		this.liveAssistants.set(itemId, message);
		this.emitSessionEvent({ type: "message_start", message: { ...message, content: [...message.content] } });
		return message;
	}

	private appendAssistantContent(itemId: string, contentType: "text" | "thinking", delta: string): void {
		const message = this.ensureLiveAssistant(itemId, contentType);
		const block = message.content[0];
		if (block?.type === "text" && contentType === "text") block.text += delta;
		if (block?.type === "thinking" && contentType === "thinking") block.thinking += delta;
		this.emitSessionEvent({
			type: "message_update",
			message: { ...message, content: [...message.content] },
			assistantMessageEvent:
				contentType === "text"
					? { type: "text_delta", contentIndex: 0, delta, partial: message }
					: { type: "thinking_delta", contentIndex: 0, delta, partial: message },
		});
	}

	private finishAssistantContent(itemId: string, contentType: "text" | "thinking", text: string): void {
		const message = this.ensureLiveAssistant(itemId, contentType);
		const block = message.content[0];
		if (block?.type === "text" && contentType === "text") block.text = text;
		if (block?.type === "thinking" && contentType === "thinking") block.thinking = text;
		this.emitSessionEvent({
			type: "message_update",
			message: { ...message, content: [...message.content] },
			assistantMessageEvent:
				contentType === "text"
					? { type: "text_end", contentIndex: 0, content: text, partial: message }
					: { type: "thinking_end", contentIndex: 0, content: text, partial: message },
		});
	}

	private beginTool(item: Record<string, unknown>): void {
		const itemType = eventString(item, "type");
		if (!itemType || itemType === "message" || itemType === "reasoning" || itemType.endsWith("_output")) return;
		const id = eventString(item, "call_id") ?? eventString(item, "id");
		if (!id || this.liveTools.has(id)) return;
		const name = toolName(itemType, item);
		const args = toolArguments(item);
		const tool: LiveTool = {
			id,
			name,
			args,
			output: "",
			waitsForOutputItem: itemType === "function_call",
		};
		this.liveTools.set(id, tool);
		const message = createToolCallMessage(this.currentModel(), tool);
		this.messages.push(message);
		this.emitSessionEvent({ type: "message_start", message });
		this.emitSessionEvent({ type: "message_end", message });
		this.emitSessionEvent({ type: "tool_execution_start", toolCallId: id, toolName: name, args });
	}

	private updateToolOutput(itemId: string, delta: string): void {
		const tool = this.liveTools.get(itemId);
		if (!tool) return;
		tool.output += delta;
		this.emitSessionEvent({
			type: "tool_execution_update",
			toolCallId: tool.id,
			toolName: tool.name,
			args: tool.args,
			partialResult: { content: [{ type: "text", text: tool.output }] },
		});
	}

	private finishItem(item: Record<string, unknown>): void {
		const itemType = eventString(item, "type");
		const itemId = eventString(item, "id");
		if (itemId) this.completedItemIds.add(itemId);
		if (itemType === "message" && eventString(item, "role") === "assistant" && itemId) {
			const message = this.liveAssistants.get(itemId);
			if (message) {
				this.emitSessionEvent({ type: "message_end", message });
				this.messages.push(message);
				this.liveAssistants.delete(itemId);
			}
			return;
		}
		if (itemType === "function_call_output") {
			const callId = eventString(item, "call_id");
			if (callId) this.finishTool(callId, item);
			return;
		}
		const callId = eventString(item, "call_id") ?? itemId;
		const tool = callId ? this.liveTools.get(callId) : undefined;
		if (tool && !tool.waitsForOutputItem) this.finishTool(tool.id, item);
	}

	private finishTool(callId: string, item: Record<string, unknown>): void {
		const tool = this.liveTools.get(callId);
		if (!tool) return;
		const output = tool.output || itemOutput(item) || "Completed";
		this.emitSessionEvent({
			type: "tool_execution_end",
			toolCallId: tool.id,
			toolName: tool.name,
			result: { content: [{ type: "text", text: output }], details: item },
			isError: eventString(item, "status") === "failed",
		});
		this.liveTools.delete(callId);
	}

	private connectionState(): AgentConnectionState {
		const model = this.currentModel();
		return {
			activeSessionId: this.snapshot.agentId,
			cwd: "/workspace",
			model,
			thinkingLevel: toThinkingLevel(this.snapshot.reasoningEffort),
			serviceTier: null,
			availableThinkingLevels: ["off", "low", "medium", "high", "xhigh", "max"],
			isStreaming: this.turnActive || ["running", "recovering", "provisioning"].includes(this.snapshot.status),
			isCompacting: false,
			isBashRunning: false,
			retryAttempt: 0,
			steeringMode: "all",
			followUpMode: "all",
			sessionFile: undefined,
			sessionId: this.snapshot.sessionId ?? this.snapshot.workflowId,
			sessionName: `${this.snapshot.fleetId}/${this.snapshot.agentId}`,
			sessionDir: "/workspace",
			leafId: null,
			autoCompactionEnabled: true,
			messageCount: this.messages.length,
			sessionActions: {
				queuedCount: this.snapshot.pendingMessages,
				steering: this.queue.steering,
				followUps: this.queue.followUp,
			},
			compactionCount: 0,
			goal: emptyGoalState(),
			scopedModels: [{ model, thinkingLevel: toThinkingLevel(this.snapshot.reasoningEffort) }],
			activeToolNames: [
				"shell",
				"mcp_call",
				"native.spawn_agent",
				"native.send_input",
				"native.wait",
				"native.resume_agent",
				"native.close_agent",
				"native.agent_message",
			],
			contextUsage: { tokens: null, contextWindow: model.contextWindow, percent: null },
			recap: cloudRecap(this.snapshot),
		};
	}

	private connectionSnapshot(): AgentConnectionSnapshot {
		return {
			state: this.connectionState(),
			messages: [...this.messages],
			sessionContext: {
				messages: [...this.messages],
				thinkingLevel: this.connectionState().thinkingLevel,
				serviceTier: null,
				model: { provider: "openai", modelId: this.snapshot.model },
			},
			parent: this.snapshot.parentAgentId
				? { activeSessionId: this.snapshot.parentAgentId, childId: this.snapshot.agentId }
				: undefined,
			children: [...this.childSnapshots.values()],
			replay: {
				status: "partial",
				toSequence: 0,
				reason: "Managed Agents streams are live-only; items restore durable output",
			},
		};
	}

	private currentModel(): AgentConnectionModel {
		return this.modelCatalog.find((model) => model.id === this.snapshot.model) ?? this.modelCatalog[0];
	}

	private emitSessionEvent(event: AgentConnectionSessionEvent): void {
		this.emit({ type: "session_event", event });
	}

	private emit(event: AgentConnectionEvent): void {
		for (const listener of [...this.listeners]) {
			void Promise.resolve(listener(event)).catch(() => undefined);
		}
	}

	private settleReady(): void {
		if (this.readySettled) return;
		this.readySettled = true;
		this.resolveReady();
	}

	private unsupported(feature: string): never {
		throw new Error(`${feature} is not available in the cloud MVP yet`);
	}
}

function cloudModels(current: string): AgentConnectionModel[] {
	const preferred = [current, "gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol"];
	const models = getModels("openai");
	const selected: AgentConnectionModel[] = [];
	for (const id of preferred) {
		const model = models.find((candidate) => candidate.id === id);
		if (model && !selected.some((candidate) => candidate.id === model.id)) selected.push(model);
	}
	if (selected.length === 0) throw new Error(`Prime Agent has no OpenAI model metadata for ${current}`);
	return selected;
}

function createUserMessage(text: string): AgentMessage {
	return { role: "user", content: text, timestamp: Date.now() };
}

function createAssistantMessage(model: AgentConnectionModel, contentType: "text" | "thinking"): AssistantMessage {
	return {
		role: "assistant",
		content: [contentType === "text" ? { type: "text", text: "" } : { type: "thinking", thinking: "" }],
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: emptyUsage(),
		stopReason: "stop",
		timestamp: Date.now(),
	};
}

function createToolCallMessage(model: AgentConnectionModel, tool: LiveTool): AssistantMessage {
	return {
		role: "assistant",
		content: [{ type: "toolCall", id: tool.id, name: tool.name, arguments: tool.args }],
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: emptyUsage(),
		stopReason: "toolUse",
		timestamp: Date.now(),
	};
}

function itemToMessages(item: Record<string, unknown>, model: AgentConnectionModel): AgentMessage[] {
	if (eventString(item, "type") !== "message") return [];
	const role = eventString(item, "role");
	const text = itemContentText(item);
	const timestamp = itemTimestamp(item);
	if (role === "user") {
		return [{ role: "user", content: text.replace(CLOUD_MESSAGE_MARKER, ""), timestamp }];
	}
	if (role === "assistant") {
		return [
			{
				role: "assistant",
				content: [{ type: "text", text }],
				api: model.api,
				provider: model.provider,
				model: model.id,
				usage: emptyUsage(),
				stopReason: eventString(item, "status") === "failed" ? "error" : "stop",
				timestamp,
			},
		];
	}
	return [];
}

function itemContentText(item: Record<string, unknown>): string {
	const content = item.content;
	if (!Array.isArray(content)) return "";
	return content
		.flatMap((part) => {
			if (typeof part !== "object" || part === null || Array.isArray(part)) return [];
			const record = part as Record<string, unknown>;
			const text = eventString(record, "text") ?? eventString(record, "encrypted_content");
			return text ? [text] : [];
		})
		.join("");
}

function itemTimestamp(item: Record<string, unknown>): number {
	for (const key of ["created_at", "completed_at", "timestamp"]) {
		const value = item[key];
		if (typeof value === "number" && Number.isFinite(value)) return value < 10_000_000_000 ? value * 1_000 : value;
	}
	return Date.now();
}

function childSnapshot(snapshot: CloudAgentSnapshotDto): AgentConnectionRlmChildAgentSnapshot {
	const status =
		snapshot.status === "failed"
			? "error"
			: snapshot.status === "stopped"
				? "cancelled"
				: snapshot.status === "idle" && snapshot.lastOutput
					? "done"
					: snapshot.status === "provisioning"
						? "queued"
						: "running";
	return {
		id: snapshot.agentId,
		executionKind: "durable-fleet",
		activeSessionId: snapshot.agentId,
		sessionName: snapshot.agentId,
		model: snapshot.model,
		label: snapshot.agentId,
		status,
		answerPreview: snapshot.lastOutput?.slice(0, 240),
		recap: cloudRecap(snapshot),
		sessionDir: "/workspace",
		activity: status === "running" ? { kind: "executing" } : undefined,
		error: snapshot.lastError,
	};
}

function managedSubagentsFromItems(items: readonly Record<string, unknown>[]): AgentConnectionRlmChildAgentSnapshot[] {
	const snapshots = new Map<string, AgentConnectionRlmChildAgentSnapshot>();
	for (const item of items) applyManagedSubagentItem(snapshots, item);
	return [...snapshots.values()];
}

function applyManagedSubagentItem(
	snapshots: Map<string, AgentConnectionRlmChildAgentSnapshot>,
	item: Record<string, unknown>,
): AgentConnectionRlmChildAgentSnapshot | undefined {
	const itemType = eventString(item, "type");
	if (itemType === "spawn_agent_call") {
		const id = eventString(item, "spawned_agent_id");
		if (!id) return undefined;
		const key = `managed-native:${id}`;
		const previous = snapshots.get(key);
		const child: AgentConnectionRlmChildAgentSnapshot = {
			id: key,
			executionKind: "managed-native",
			activeSessionId: id,
			label: previous?.label ?? nativeSubagentLabel(id),
			model: eventString(item, "model") ?? previous?.model,
			status: "running",
			answerPreview: previous?.answerPreview,
			recap: "native subagent running",
			sessionDir: "/workspace",
			activity: { kind: "executing" },
		};
		snapshots.set(key, child);
		return child;
	}

	if (itemType === "agent_message") {
		const author = eventString(item, "author");
		const recipient = eventString(item, "recipient");
		const authorKey = author ? `managed-native:${author}` : undefined;
		const recipientKey = recipient ? `managed-native:${recipient}` : undefined;
		const previous = (authorKey && snapshots.get(authorKey)) || (recipientKey && snapshots.get(recipientKey));
		if (!previous) return undefined;
		const fromChild = authorKey === previous.id;
		const child: AgentConnectionRlmChildAgentSnapshot = {
			...previous,
			status: fromChild ? "done" : "running",
			answerPreview: fromChild ? itemContentText(item).slice(0, 240) : previous.answerPreview,
			recap: fromChild ? "native subagent idle" : "native subagent running",
			activity: fromChild ? undefined : { kind: "executing" },
		};
		snapshots.set(previous.id, child);
		return child;
	}

	if (itemType === "send_input_call" || itemType === "resume_agent_call" || itemType === "close_agent_call") {
		const id = eventString(item, "agent_id") ?? eventString(item, "target_agent_id");
		if (!id) return undefined;
		const key = `managed-native:${id}`;
		const previous = snapshots.get(key);
		if (!previous) return undefined;
		const closed = itemType === "close_agent_call";
		const child: AgentConnectionRlmChildAgentSnapshot = {
			...previous,
			activeSessionId: closed ? undefined : id,
			status: closed ? "done" : "running",
			recap: closed ? "native subagent closed" : "native subagent running",
			activity: closed ? undefined : { kind: "executing" },
		};
		snapshots.set(key, child);
		return child;
	}

	return undefined;
}

function nativeSubagentLabel(id: string): string {
	return `native ${id.slice(-8)}`;
}

function cloudRecap(snapshot: CloudAgentSnapshotDto): string {
	const location = snapshot.podName ? ` · ${snapshot.podName}` : "";
	const pending = snapshot.pendingMessages > 0 ? ` · ${snapshot.pendingMessages} queued` : "";
	const generation = snapshot.sessionGeneration > 1 ? ` · generation ${snapshot.sessionGeneration}` : "";
	return `cloud ${snapshot.status}${generation}${pending}${location}`;
}

function toolName(itemType: string, item: Record<string, unknown>): string {
	if (itemType === "function_call") return eventString(item, "name") ?? "function";
	if (itemType === "command_execution") return "shell";
	if (itemType === "mcp_call") {
		const server = eventString(item, "server_label") ?? eventString(item, "server");
		const name = eventString(item, "name") ?? eventString(item, "tool_name");
		return server && name ? `${server}.${name}` : (name ?? "mcp_call");
	}
	const nativeNames: Record<string, string> = {
		spawn_agent_call: "native.spawn_agent",
		send_input_call: "native.send_input",
		wait_for_agents_call: "native.wait",
		resume_agent_call: "native.resume_agent",
		close_agent_call: "native.close_agent",
		agent_message: "native.agent_message",
	};
	if (nativeNames[itemType]) return nativeNames[itemType];
	return itemType;
}

function toolArguments(item: Record<string, unknown>): Record<string, unknown> {
	const rawArguments = item.arguments;
	if (typeof rawArguments === "object" && rawArguments !== null && !Array.isArray(rawArguments)) {
		return rawArguments as Record<string, unknown>;
	}
	if (typeof rawArguments === "string") {
		try {
			const parsed = JSON.parse(rawArguments) as unknown;
			if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
				return parsed as Record<string, unknown>;
			}
		} catch {
			return { arguments: rawArguments };
		}
	}
	const itemType = eventString(item, "type");
	if (
		itemType &&
		[
			"spawn_agent_call",
			"send_input_call",
			"wait_for_agents_call",
			"resume_agent_call",
			"close_agent_call",
			"agent_message",
		].includes(itemType)
	) {
		const fields = [
			"spawned_agent_id",
			"sender_agent_id",
			"agent_id",
			"target_agent_id",
			"agent_ids",
			"author",
			"recipient",
			"prompt",
			"model",
			"reasoning_effort",
		] as const;
		const projected: Record<string, unknown> = {};
		for (const field of fields) {
			if (item[field] !== undefined) projected[field] = item[field];
		}
		const message = itemContentText(item);
		if (message) projected.message = message;
		return projected;
	}
	const command = eventString(item, "command") ?? eventString(item, "cmd");
	return command ? { command } : { item };
}

function itemOutput(item: Record<string, unknown>): string {
	for (const key of ["output", "result", "error"]) {
		const value = item[key];
		if (typeof value === "string") return value;
		if (value !== undefined) return JSON.stringify(value);
	}
	return itemContentText(item);
}

function eventString(record: Record<string, unknown>, key: string): string | undefined {
	return typeof record[key] === "string" ? record[key] : undefined;
}

function eventRecord(record: Record<string, unknown>, key: string): Record<string, unknown> | undefined {
	const value = record[key];
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function eventNumber(record: Record<string, unknown>, key: string): number | undefined {
	const value = record[key];
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function eventError(record: Record<string, unknown>): string {
	const error = eventRecord(record, "error");
	const code = error && eventString(error, "code");
	const message = (error && eventString(error, "message")) ?? eventString(record, "error") ?? "turn failed";
	if (code === "internal_error") {
		return `OpenAI managed turn failed (${code}): ${message} Send a new message to recover into a fresh managed session; the failed message is not replayed automatically.`;
	}
	return `Cloud agent turn failed${code ? ` (${code})` : ""}: ${message}`;
}

function toThinkingLevel(effort: string): ThinkingLevel {
	if (effort === "none") return "off";
	if (["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(effort)) {
		return effort as ThinkingLevel;
	}
	return "medium";
}

function emptyUsage(): Usage {
	return {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 0,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

function addUsage(target: Usage, value: Usage): void {
	target.input += value.input;
	target.output += value.output;
	target.cacheRead += value.cacheRead;
	target.cacheWrite += value.cacheWrite;
	target.totalTokens += value.totalTokens;
	target.cost.input += value.cost.input;
	target.cost.output += value.cost.output;
	target.cost.cacheRead += value.cost.cacheRead;
	target.cost.cacheWrite += value.cost.cacheWrite;
	target.cost.total += value.cost.total;
}

function usageFromTokens(tokens: SessionStats["tokens"]): Usage {
	return {
		input: tokens.input,
		output: tokens.output,
		cacheRead: tokens.cacheRead,
		cacheWrite: tokens.cacheWrite,
		totalTokens: tokens.total,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

function messageText(content: unknown): string {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	return content
		.flatMap((part) => {
			if (typeof part !== "object" || part === null || Array.isArray(part)) return [];
			const record = part as Record<string, unknown>;
			if (record.type === "text" && typeof record.text === "string") return [record.text];
			if (record.type === "thinking" && typeof record.thinking === "string") return [record.thinking];
			return [];
		})
		.join("");
}

function messageSafetyStatus(paused: boolean): AgentSessionMessageSafetyStatus {
	return {
		paused,
		maxMessageChars: 50_000,
		maxPendingPerSession: 100,
		rateLimitCapacity: 100,
		rateLimitRefillMs: 1_000,
	};
}

function emptyResourceSnapshot(): AgentConnectionResourceSnapshot {
	return {
		contextFiles: [],
		skills: [],
		prompts: [],
		extensions: [],
		themes: [],
		diagnostics: { skills: [], prompts: [], extensions: [], themes: [] },
	};
}

async function waitForSession(
	client: CloudAgentClient,
	fleetId: string,
	agentId: string,
): Promise<CloudAgentSnapshotDto> {
	const deadline = Date.now() + 10 * 60 * 1_000;
	while (Date.now() < deadline) {
		const snapshot = await client.getAgent(fleetId, agentId);
		if (snapshot.sessionId) return snapshot;
		if (snapshot.status === "failed") throw new Error(snapshot.lastError ?? "Cloud agent provisioning failed");
		await delay(500);
	}
	throw new Error("Timed out waiting for the cloud managed-agent session");
}

function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		if (signal?.aborted) {
			reject(signal.reason);
			return;
		}
		const timer = setTimeout(resolve, milliseconds);
		signal?.addEventListener(
			"abort",
			() => {
				clearTimeout(timer);
				reject(signal.reason);
			},
			{ once: true },
		);
	});
}
