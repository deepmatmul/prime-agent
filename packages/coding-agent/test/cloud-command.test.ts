import { describe, expect, it } from "vitest";
import { formatCommandHelp, formatTopLevelHelp } from "../src/cli/command-registry.js";
import { parseCloudCommandArgs } from "../src/cloud-command.js";

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

	it("advertises cloud mode in public help", () => {
		expect(formatTopLevelHelp()).toContain("cloud");
		expect(formatCommandHelp(["cloud"])).toContain("--fleet <id>");
	});
});
