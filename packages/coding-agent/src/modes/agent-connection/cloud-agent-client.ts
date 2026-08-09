export interface CloudAgentSnapshotDto {
	agentId: string;
	fleetId: string;
	status: string;
	workflowId: string;
	model: string;
	reasoningEffort: string;
	parentAgentId?: string;
	sessionId?: string;
	environmentId?: string;
	podName?: string;
	pendingMessages: number;
	currentMessageId?: string;
	lastOutput?: string;
	lastError?: string;
	childAgentIds: string[];
}

export interface CloudTranscriptDto {
	snapshot: CloudAgentSnapshotDto;
	items: Record<string, unknown>[];
	truncated: boolean;
}

export interface CloudAcceptedDto {
	workflowId: string;
	agentId: string;
	fleetId: string;
	status: string;
	messageId?: string;
}

export interface CloudAgentCreateOptions {
	fleetId: string;
	agentId: string;
	instructions: string;
	model: string;
	reasoningEffort: string;
	textVerbosity?: "low" | "medium" | "high";
	repoUrl?: string;
}

export interface CloudSseEvent {
	event: string;
	data: Record<string, unknown>;
}

export class CloudAgentHttpError extends Error {
	constructor(
		message: string,
		readonly status: number,
	) {
		super(message);
		this.name = "CloudAgentHttpError";
	}
}

export class CloudAgentClient {
	readonly baseUrl: string;

	constructor(baseUrl: string) {
		this.baseUrl = baseUrl.replace(/\/+$/, "");
	}

	async health(signal?: AbortSignal): Promise<boolean> {
		try {
			const response = await fetch(`${this.baseUrl}/healthz`, { signal });
			return response.ok;
		} catch {
			return false;
		}
	}

	async getAgent(fleetId: string, agentId: string, signal?: AbortSignal): Promise<CloudAgentSnapshotDto> {
		const payload = await this.request(`/v1/agents/${encodeURIComponent(fleetId)}/${encodeURIComponent(agentId)}`, {
			signal,
		});
		return parseAgentSnapshot(payload);
	}

	async createAgent(options: CloudAgentCreateOptions, signal?: AbortSignal): Promise<CloudAcceptedDto> {
		const payload = await this.request("/v1/agents", {
			method: "POST",
			signal,
			body: JSON.stringify({
				fleet_id: options.fleetId,
				agent_id: options.agentId,
				instructions: options.instructions,
				model: options.model,
				reasoning_effort: options.reasoningEffort,
				text_verbosity: options.textVerbosity ?? "low",
				...(options.repoUrl ? { repo_url: options.repoUrl } : {}),
			}),
		});
		return parseAccepted(payload);
	}

	async sendMessage(fleetId: string, agentId: string, body: string, signal?: AbortSignal): Promise<CloudAcceptedDto> {
		const payload = await this.request(
			`/v1/agents/${encodeURIComponent(fleetId)}/${encodeURIComponent(agentId)}/messages`,
			{
				method: "POST",
				signal,
				body: JSON.stringify({ body }),
			},
		);
		return parseAccepted(payload);
	}

	async cancelTurn(fleetId: string, agentId: string, signal?: AbortSignal): Promise<void> {
		await this.request(`/v1/agents/${encodeURIComponent(fleetId)}/${encodeURIComponent(agentId)}/cancel`, {
			method: "POST",
			signal,
		});
	}

	async stopAgent(fleetId: string, agentId: string, signal?: AbortSignal): Promise<void> {
		await this.request(`/v1/agents/${encodeURIComponent(fleetId)}/${encodeURIComponent(agentId)}/stop`, {
			method: "POST",
			signal,
			body: JSON.stringify({ reason: "requested from Prime Agent cloud UI" }),
		});
	}

	async getTranscript(fleetId: string, agentId: string, signal?: AbortSignal): Promise<CloudTranscriptDto> {
		const payload = await this.request(
			`/v1/agents/${encodeURIComponent(fleetId)}/${encodeURIComponent(agentId)}/transcript`,
			{ signal },
		);
		const record = requireRecord(payload, "transcript");
		const rawItems = record.items;
		if (!Array.isArray(rawItems)) {
			throw new Error("Cloud transcript did not include an items array");
		}
		return {
			snapshot: parseAgentSnapshot(record.snapshot),
			items: rawItems.map((item) => requireRecord(item, "transcript item")),
			truncated: record.truncated === true,
		};
	}

	async *streamEvents(fleetId: string, agentId: string, signal?: AbortSignal): AsyncGenerator<CloudSseEvent> {
		const response = await fetch(
			`${this.baseUrl}/v1/agents/${encodeURIComponent(fleetId)}/${encodeURIComponent(agentId)}/events`,
			{
				headers: { Accept: "text/event-stream" },
				signal,
			},
		);
		if (!response.ok) {
			throw new CloudAgentHttpError(await responseError(response), response.status);
		}
		if (!response.body) {
			throw new Error("Cloud event stream returned no response body");
		}

		const decoder = new TextDecoder();
		let pending = "";
		let eventName = "message";
		let dataLines: string[] = [];
		for await (const chunk of response.body) {
			pending += decoder.decode(chunk, { stream: true });
			let newline = pending.indexOf("\n");
			while (newline >= 0) {
				const rawLine = pending.slice(0, newline);
				pending = pending.slice(newline + 1);
				const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
				if (line === "") {
					if (dataLines.length > 0) {
						yield {
							event: eventName,
							data: parseEventData(dataLines.join("\n")),
						};
					}
					eventName = "message";
					dataLines = [];
				} else if (line.startsWith("event:")) {
					eventName = line.slice("event:".length).trim();
				} else if (line.startsWith("data:")) {
					dataLines.push(line.slice("data:".length).trimStart());
				}
				newline = pending.indexOf("\n");
			}
		}
	}

	private async request(path: string, init: RequestInit): Promise<unknown> {
		const headers = new Headers(init.headers);
		if (init.body !== undefined) {
			headers.set("Content-Type", "application/json");
		}
		const response = await fetch(`${this.baseUrl}${path}`, { ...init, headers });
		if (!response.ok) {
			throw new CloudAgentHttpError(await responseError(response), response.status);
		}
		if (response.status === 204) {
			return {};
		}
		return (await response.json()) as unknown;
	}
}

function parseAgentSnapshot(value: unknown): CloudAgentSnapshotDto {
	const record = requireRecord(value, "agent snapshot");
	return {
		agentId: requireString(record.agent_id, "agent_id"),
		fleetId: requireString(record.fleet_id, "fleet_id"),
		status: requireString(record.status, "status"),
		workflowId: requireString(record.workflow_id, "workflow_id"),
		model: optionalString(record.model) ?? "gpt-5.6-sol",
		reasoningEffort: optionalString(record.reasoning_effort) ?? "medium",
		parentAgentId: optionalString(record.parent_agent_id),
		sessionId: optionalString(record.session_id),
		environmentId: optionalString(record.environment_id),
		podName: optionalString(record.pod_name),
		pendingMessages: optionalNumber(record.pending_messages) ?? 0,
		currentMessageId: optionalString(record.current_message_id),
		lastOutput: optionalString(record.last_output),
		lastError: optionalString(record.last_error),
		childAgentIds: Array.isArray(record.child_agent_ids)
			? record.child_agent_ids.filter((item): item is string => typeof item === "string")
			: [],
	};
}

function parseAccepted(value: unknown): CloudAcceptedDto {
	const record = requireRecord(value, "accepted response");
	return {
		workflowId: requireString(record.workflow_id, "workflow_id"),
		agentId: requireString(record.agent_id, "agent_id"),
		fleetId: requireString(record.fleet_id, "fleet_id"),
		status: requireString(record.status, "status"),
		messageId: optionalString(record.message_id),
	};
}

function parseEventData(value: string): Record<string, unknown> {
	try {
		return requireRecord(JSON.parse(value) as unknown, "SSE event data");
	} catch (error) {
		throw new Error("Cloud event stream returned invalid JSON", { cause: error });
	}
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new Error(`Cloud ${label} was not an object`);
	}
	return value as Record<string, unknown>;
}

function requireString(value: unknown, field: string): string {
	if (typeof value !== "string") {
		throw new Error(`Cloud response field ${field} was not a string`);
	}
	return value;
}

function optionalString(value: unknown): string | undefined {
	return typeof value === "string" ? value : undefined;
}

function optionalNumber(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

async function responseError(response: Response): Promise<string> {
	try {
		const payload = requireRecord((await response.json()) as unknown, "error response");
		const detail = optionalString(payload.detail);
		return detail ?? `Cloud agent API returned ${response.status}`;
	} catch {
		return `Cloud agent API returned ${response.status}`;
	}
}
