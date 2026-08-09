import { type ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { basename } from "node:path";
import chalk from "chalk";
import { getAgentDir } from "./config.js";
import { createAgentSessionServices } from "./core/agent-session-services.js";
import { SessionManager } from "./core/session-manager.js";
import { CloudAgentClient } from "./modes/agent-connection/cloud-agent-client.js";
import { CloudAgentConnection } from "./modes/agent-connection/cloud-agent-connection.js";
import { InteractiveMode } from "./modes/interactive/interactive-mode.js";
import { createInteractiveModeUiServicesFromServices } from "./modes/interactive/interactive-mode-services.js";
import { ClientPromptStashStore } from "./modes/interactive/prompt-stash-state.js";
import { initTheme, preloadCodeHighlighter, stopThemeWatcher } from "./modes/interactive/theme/theme.js";

const DEFAULT_CLOUD_URL = "http://127.0.0.1:18080";
const DEFAULT_NAMESPACE = "cloud-agent-mvp";
const DEFAULT_SERVICE = "cloud-agent-api";
const DEFAULT_SERVICE_PORT = 80;
const IDENTIFIER = /^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$/;
const REASONING_EFFORTS = new Set(["none", "minimal", "low", "medium", "high", "xhigh", "max"]);

interface CloudCommandOptions {
	url: string;
	urlIsExplicit: boolean;
	fleetId: string;
	agentId: string;
	model: string;
	reasoningEffort: string;
	instructions: string;
	repoUrl?: string;
	initialMessage?: string;
	attachOnly: boolean;
	newFleet: boolean;
	kubeContext?: string;
	namespace: string;
	service: string;
	help: boolean;
}

interface CloudEndpoint {
	url: string;
	close(): Promise<void>;
}

export async function runCloudCommand(args: string[]): Promise<void> {
	const options = parseCloudCommandArgs(args, process.cwd());
	if (options.help) {
		console.log(cloudHelp());
		return;
	}
	validateOptions(options);

	const endpoint = await ensureCloudEndpoint(options);
	const client = new CloudAgentClient(endpoint.url);
	if (!(await client.health(AbortSignal.timeout(5_000)))) {
		await endpoint.close();
		throw new Error(`Cloud agent API is not healthy at ${endpoint.url}`);
	}

	const cwd = process.cwd();
	const agentDir = getAgentDir();
	const sessionManager = SessionManager.inMemory(cwd);
	const services = await createAgentSessionServices({
		cwd,
		agentDir,
		resourceLoaderOptions: {
			noExtensions: true,
			noSkills: true,
			noPromptTemplates: true,
			noContextFiles: true,
		},
	});
	for (const diagnostic of services.diagnostics) {
		const color = diagnostic.type === "error" ? chalk.red : diagnostic.type === "warning" ? chalk.yellow : chalk.dim;
		console.error(color(diagnostic.message));
	}

	let connection: CloudAgentConnection | undefined;
	try {
		connection = await CloudAgentConnection.connect(client, {
			fleetId: options.newFleet ? withUniqueSuffix(options.fleetId) : options.fleetId,
			agentId: options.agentId,
			instructions: options.instructions,
			model: options.model,
			reasoningEffort: options.reasoningEffort,
			repoUrl: options.repoUrl,
			attachOnly: options.attachOnly,
		});
		const state = await connection.getState();
		initTheme(services.settingsManager.getTheme(), true);
		const interactiveMode = new InteractiveMode({
			agentConnection: connection,
			uiServices: createInteractiveModeUiServicesFromServices({ services, sessionManager }),
			bindLocalSessionExtensions: false,
			promptStashStore: new ClientPromptStashStore(),
			promptStashSessionId: state.sessionId,
			initialMessage: options.initialMessage,
			startupNotice: `Remote fleet ${state.sessionName} · ${endpoint.url}`,
			returnToAgentsView: false,
			onShutdown: () => endpoint.close(),
		});
		await preloadCodeHighlighter();
		await interactiveMode.run();
	} finally {
		await connection?.dispose();
		await endpoint.close();
		stopThemeWatcher();
	}
}

export function parseCloudCommandArgs(args: string[], cwd: string): CloudCommandOptions {
	const envUrl = process.env.PRIME_AGENT_CLOUD_URL;
	const options: CloudCommandOptions = {
		url: envUrl ?? DEFAULT_CLOUD_URL,
		urlIsExplicit: envUrl !== undefined,
		fleetId: defaultFleetId(cwd),
		agentId: "lead",
		model: process.env.PRIME_AGENT_CLOUD_MODEL ?? "gpt-5.6-luna",
		reasoningEffort: process.env.PRIME_AGENT_CLOUD_EFFORT ?? "low",
		instructions:
			"Act as the root coding and research coordinator. Work recursively, delegate independent subtasks early, and synthesize child results into a verified outcome.",
		attachOnly: false,
		newFleet: false,
		kubeContext: process.env.PRIME_AGENT_CLOUD_KUBE_CONTEXT,
		namespace: process.env.PRIME_AGENT_CLOUD_NAMESPACE ?? DEFAULT_NAMESPACE,
		service: process.env.PRIME_AGENT_CLOUD_SERVICE ?? DEFAULT_SERVICE,
		help: false,
	};
	const positional: string[] = [];
	for (let index = 0; index < args.length; index++) {
		const arg = args[index];
		switch (arg) {
			case "--help":
			case "-h":
				options.help = true;
				break;
			case "--url":
				options.url = requiredValue(args, ++index, arg);
				options.urlIsExplicit = true;
				break;
			case "--fleet":
				options.fleetId = requiredValue(args, ++index, arg);
				break;
			case "--agent":
				options.agentId = requiredValue(args, ++index, arg);
				break;
			case "--model":
				options.model = requiredValue(args, ++index, arg);
				break;
			case "--effort":
				options.reasoningEffort = requiredValue(args, ++index, arg);
				break;
			case "--instructions":
				options.instructions = requiredValue(args, ++index, arg);
				break;
			case "--repo":
				options.repoUrl = requiredValue(args, ++index, arg);
				break;
			case "--kube-context":
				options.kubeContext = requiredValue(args, ++index, arg);
				break;
			case "--namespace":
				options.namespace = requiredValue(args, ++index, arg);
				break;
			case "--service":
				options.service = requiredValue(args, ++index, arg);
				break;
			case "--attach":
				options.attachOnly = true;
				break;
			case "--new":
				options.newFleet = true;
				break;
			default:
				if (arg.startsWith("-")) throw new Error(`Unknown cloud option: ${arg}`);
				positional.push(arg);
		}
	}
	options.initialMessage = positional.length > 0 ? positional.join(" ") : undefined;
	return options;
}

async function ensureCloudEndpoint(options: CloudCommandOptions): Promise<CloudEndpoint> {
	const directClient = new CloudAgentClient(options.url);
	if (await directClient.health(AbortSignal.timeout(1_500))) {
		return { url: options.url, close: async () => {} };
	}
	if (options.urlIsExplicit) {
		throw new Error(`Cloud agent API is not reachable at ${options.url}`);
	}

	const parsedUrl = new URL(options.url);
	if (parsedUrl.hostname !== "127.0.0.1" && parsedUrl.hostname !== "localhost") {
		throw new Error(`Automatic EKS port-forwarding requires a loopback URL, received ${options.url}`);
	}
	const localPort = Number(parsedUrl.port || (parsedUrl.protocol === "https:" ? 443 : 80));
	const tunnel = await startPortForward({
		context: options.kubeContext,
		namespace: options.namespace,
		service: options.service,
		localPort,
	});
	return {
		url: options.url,
		close: async () => {
			if (tunnel.exitCode === null && !tunnel.killed) tunnel.kill("SIGTERM");
		},
	};
}

function startPortForward(options: {
	context?: string;
	namespace: string;
	service: string;
	localPort: number;
}): Promise<ChildProcessWithoutNullStreams> {
	const args = cloudPortForwardArgs(options);
	const process = spawn("kubectl", args, { stdio: "pipe" });
	return new Promise((resolve, reject) => {
		let settled = false;
		let errorText = "";
		const timeout = setTimeout(() => finish(new Error("Timed out starting the EKS API port-forward")), 30_000);
		const finish = (error?: Error) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			if (error) {
				if (process.exitCode === null) process.kill("SIGTERM");
				reject(error);
			} else {
				resolve(process);
			}
		};
		process.once("error", (error) => finish(error));
		process.once("exit", (code) => {
			finish(new Error(`kubectl port-forward exited with ${code ?? "unknown"}: ${errorText.trim()}`));
		});
		process.stderr.setEncoding("utf8");
		process.stderr.on("data", (chunk: string) => {
			errorText = `${errorText}${chunk}`.slice(-4_000);
			if (chunk.includes("Forwarding from")) finish();
		});
		process.stdout.setEncoding("utf8");
		process.stdout.on("data", (chunk: string) => {
			if (chunk.includes("Forwarding from")) finish();
		});
	});
}

export function cloudPortForwardArgs(options: {
	context?: string;
	namespace: string;
	service: string;
	localPort: number;
}): string[] {
	return [
		...(options.context ? ["--context", options.context] : []),
		"--namespace",
		options.namespace,
		"port-forward",
		`service/${options.service}`,
		`${options.localPort}:${DEFAULT_SERVICE_PORT}`,
	];
}

function validateOptions(options: CloudCommandOptions): void {
	if (!IDENTIFIER.test(options.fleetId)) throw new Error(`Invalid cloud fleet id: ${options.fleetId}`);
	if (!IDENTIFIER.test(options.agentId)) throw new Error(`Invalid cloud agent id: ${options.agentId}`);
	if (!REASONING_EFFORTS.has(options.reasoningEffort)) {
		throw new Error(`Invalid cloud reasoning effort: ${options.reasoningEffort}`);
	}
	const url = new URL(options.url);
	if (url.protocol !== "http:" && url.protocol !== "https:") {
		throw new Error("Cloud URL must use http or https");
	}
	if (options.repoUrl) {
		const repo = new URL(options.repoUrl);
		if (repo.protocol !== "https:" || repo.username || repo.password) {
			throw new Error("Cloud repository URL must be credential-free HTTPS");
		}
	}
}

function requiredValue(args: string[], index: number, flag: string): string {
	const value = args[index];
	if (!value || value.startsWith("--")) throw new Error(`${flag} requires a value`);
	return value;
}

function defaultFleetId(cwd: string): string {
	const slug = basename(cwd)
		.toLowerCase()
		.replace(/[^a-z0-9-]+/g, "-")
		.replace(/^-+|-+$/g, "")
		.slice(0, 48);
	return `prime-${slug || "workspace"}`;
}

function withUniqueSuffix(fleetId: string): string {
	const suffix = Date.now().toString(36).slice(-8);
	return `${fleetId.slice(0, 64 - suffix.length - 1)}-${suffix}`;
}

function cloudHelp(): string {
	return `Prime Agent cloud UI

Usage:
  prime-agent cloud [options] [initial prompt]

Options:
  --fleet <id>          Durable fleet id (default: derived from cwd)
  --agent <id>          Agent id (default: lead)
  --new                 Add a unique suffix and start a fresh fleet
  --attach              Require the agent to already exist
  --model <id>          Managed Agents model (default: gpt-5.6-luna)
  --effort <level>      Reasoning effort (default: low)
  --instructions <text> Root-agent instructions
  --repo <https-url>    Repository cloned into the shared workspace
  --url <url>           Cloud harness URL (default: http://127.0.0.1:18080)
  --kube-context <name> kubectl context used for automatic port-forwarding
  --namespace <name>    EKS namespace (default: cloud-agent-mvp)
  --service <name>      API service (default: cloud-agent-api)

When the default local URL is unavailable, this command opens and owns a kubectl
port-forward automatically. Closing the UI detaches from the durable remote agent;
it does not stop the Temporal workflow or sandbox.`;
}
