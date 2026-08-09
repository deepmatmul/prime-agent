import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { CloudAgentClient } from "../src/modes/agent-connection/cloud-agent-client.js";
import { CloudAgentConnection } from "../src/modes/agent-connection/cloud-agent-connection.js";

const snapshot = {
	agent_id: "lead",
	fleet_id: "fleet",
	status: "idle",
	workflow_id: "agent/fleet/lead",
	model: "gpt-5.6-luna",
	reasoning_effort: "low",
	parent_agent_id: null,
	session_id: "session_test",
	environment_id: "env_test",
	pod_name: "agent-lead",
	pending_messages: 0,
	current_message_id: null,
	last_output: null,
	last_error: null,
	child_agent_ids: [],
	archived_session_ids: ["session_old"],
	session_generation: 2,
};

describe("CloudAgentClient", () => {
	let server: Server | undefined;

	afterEach(async () => {
		if (!server) return;
		await new Promise<void>((resolve, reject) => {
			server?.close((error) => (error ? reject(error) : resolve()));
		});
		server = undefined;
	});

	it("hydrates persisted items before translating live managed-agent events", async () => {
		server = createServer((request, response) => {
			if (request.url === "/healthz") {
				response.writeHead(200, { "content-type": "application/json" });
				response.end('{"status":"ok"}');
				return;
			}
			if (request.url === "/v1/agents/fleet/lead") {
				response.writeHead(200, { "content-type": "application/json" });
				response.end(JSON.stringify(snapshot));
				return;
			}
			if (request.url === "/v1/agents/fleet/lead/transcript") {
				response.writeHead(200, { "content-type": "application/json" });
				response.end(
					JSON.stringify({
						snapshot,
						items: [
							{
								id: "user_1",
								type: "message",
								role: "user",
								status: "completed",
								content: [{ type: "input_text", text: "[cloud-agent-message-id:test]\nexisting" }],
							},
						],
						truncated: false,
					}),
				);
				return;
			}
			if (request.url === "/v1/agents/fleet/lead/events") {
				response.writeHead(200, {
					"content-type": "text/event-stream",
					"cache-control": "no-cache",
				});
				response.write('event: cloud.connected\ndata: {"session_id":"session_test"}\n\n');
				setTimeout(() => {
					response.write('data: {"type":"session.turn.created","event_id":"evt_1"}\n\n');
					response.write('data: {"type":"session.turn.output_text.added","item_id":"assistant_1"}\n\n');
					response.write(
						'data: {"type":"session.turn.output_text.delta","item_id":"assistant_1","delta":"hello"}\n\n',
					);
					response.write(
						'data: {"type":"session.turn.output_text.done","item_id":"assistant_1","text":"hello"}\n\n',
					);
					response.write(
						'data: {"type":"session.turn.item.done","item":{"id":"assistant_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hello"}]}}\n\n',
					);
					response.write('data: {"type":"session.turn.completed","event_id":"evt_6"}\n\n');
				}, 50);
				return;
			}
			response.writeHead(404);
			response.end();
		});
		const address = await listen(server);
		const client = new CloudAgentClient(`http://127.0.0.1:${address.port}`);
		await expect(client.getAgent("fleet", "lead")).resolves.toMatchObject({
			archivedSessionIds: ["session_old"],
			sessionGeneration: 2,
		});
		const connection = await CloudAgentConnection.connect(client, {
			fleetId: "fleet",
			agentId: "lead",
			instructions: "test",
			model: "gpt-5.6-luna",
			reasoningEffort: "low",
			attachOnly: true,
		});
		const eventTypes: string[] = [];
		connection.subscribe((event) => {
			if (event.type === "session_event") eventTypes.push(event.event.type);
		});

		await waitUntil(async () => (await connection.getLastAssistantText()) === "hello");
		expect((await connection.getMessages())[0]).toMatchObject({ role: "user", content: "existing" });
		expect(await connection.getLastAssistantText()).toBe("hello");
		expect(eventTypes).toContain("message_update");
		expect(eventTypes).toContain("agent_end");
		await connection.dispose();
	});
});

function listen(server: Server): Promise<{ port: number }> {
	return new Promise((resolve, reject) => {
		server.once("error", reject);
		server.listen(0, "127.0.0.1", () => {
			const address = server.address();
			if (!address || typeof address === "string") {
				reject(new Error("Test server did not bind a TCP port"));
				return;
			}
			resolve({ port: address.port });
		});
	});
}

async function waitUntil(predicate: () => Promise<boolean>): Promise<void> {
	const deadline = Date.now() + 2_000;
	while (Date.now() < deadline) {
		if (await predicate()) return;
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
	throw new Error("Timed out waiting for cloud connection event");
}
