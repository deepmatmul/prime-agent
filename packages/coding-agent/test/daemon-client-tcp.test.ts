import { createServer, type Server } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { DaemonClient } from "../src/modes/daemon/daemon-client.js";

describe("DaemonClient TCP transport", () => {
	let server: Server | undefined;

	afterEach(async () => {
		if (!server?.listening) return;
		await new Promise<void>((resolve, reject) => server!.close((error) => (error ? reject(error) : resolve())));
	});

	it("carries the normal daemon protocol over a host and port", async () => {
		server = createServer((socket) => {
			socket.write(
				`${JSON.stringify({
					type: "daemon_hello",
					protocol: { name: "prime-agent.daemon", version: 7 },
					schemaRevision: 14,
					serverCapabilities: [],
				})}\n`,
			);
			let buffered = "";
			socket.setEncoding("utf8");
			socket.on("data", (chunk: string) => {
				buffered += chunk;
				const newline = buffered.indexOf("\n");
				if (newline < 0) return;
				const message = JSON.parse(buffered.slice(0, newline)) as {
					id: string;
					command: { type: string };
				};
				socket.write(
					`${JSON.stringify({
						id: message.id,
						type: "response",
						command: message.command.type,
						success: true,
						data: { sessions: [] },
					})}\n`,
				);
			});
		});
		await new Promise<void>((resolve, reject) => {
			server!.once("error", reject);
			server!.listen(0, "127.0.0.1", resolve);
		});
		const address = server.address();
		if (!address || typeof address === "string") throw new Error("TCP test server has no port");

		const client = new DaemonClient({ host: "127.0.0.1", port: address.port, label: "test remote" });
		await client.connect();
		await client.waitForHello();
		await expect(client.request({ type: "list" })).resolves.toMatchObject({
			success: true,
			data: { sessions: [] },
		});
		client.close();
	});
});
