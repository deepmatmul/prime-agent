import { describe, expect, it } from "vitest";
import { formatCommandHelp, formatTopLevelHelp } from "../src/cli/command-registry.js";
import { cloudPodPortForwardArgs, cloudPortForwardArgs, parseCloudCommandArgs } from "../src/cloud-command.js";

describe("cloud command", () => {
	it("parses a durable Luna fleet launch", () => {
		const options = parseCloudCommandArgs(
			[
				"--url",
				"http://127.0.0.1:19090",
				"--fleet",
				"demo",
				"--agent",
				"lead",
				"--model",
				"gpt-5.6-luna",
				"--effort",
				"low",
				"inspect the repo",
			],
			"/tmp/example-project",
		);

		expect(options).toMatchObject({
			url: "http://127.0.0.1:19090",
			urlIsExplicit: true,
			fleetId: "demo",
			agentId: "lead",
			model: "gpt-5.6-luna",
			reasoningEffort: "low",
			initialMessage: "inspect the repo",
		});
	});

	it("derives a stable fleet id from the working directory", () => {
		const options = parseCloudCommandArgs([], "/tmp/My Project");
		expect(options.fleetId).toBe("prime-my-project");
	});

	it("forwards to the Kubernetes Service port", () => {
		expect(
			cloudPortForwardArgs({
				context: "dev-eks",
				namespace: "cloud-agent-mvp",
				service: "cloud-agent-api",
				localPort: 18080,
			}),
		).toEqual([
			"--context",
			"dev-eks",
			"--namespace",
			"cloud-agent-mvp",
			"port-forward",
			"service/cloud-agent-api",
			"18080:80",
		]);
	});

	it("forwards the exact Prime daemon protocol from its EKS pod", () => {
		expect(
			cloudPodPortForwardArgs({
				context: "dev-eks",
				namespace: "cloud-agent-mvp",
				pod: "agent-demo-lead",
				localPort: 23456,
				remotePort: 7447,
			}),
		).toEqual([
			"--context",
			"dev-eks",
			"--namespace",
			"cloud-agent-mvp",
			"port-forward",
			"pod/agent-demo-lead",
			"23456:7447",
		]);
	});

	it("advertises cloud mode in public help", () => {
		expect(formatTopLevelHelp()).toContain("cloud");
		expect(formatCommandHelp(["cloud"])).toContain("--fleet <id>");
	});
});
