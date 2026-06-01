# gemini-cli-openai-proxy

<p align="center">
  <a href="https://github.com/FlerAlex/gemini_proxy/actions">
    <img src="https://github.com/FlerAlex/gemini_proxy/actions/workflows/release.yml/badge.svg" alt="Release Status" />
  </a>
  <a href="https://crates.io/crates/gemini_proxy">
    <img src="https://img.shields.io/crates/v/gemini_proxy.svg" alt="Crates.io" />
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License" />
  </a>
  <a href="#">
    <img src="https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg" alt="Platform" />
  </a>
</p>

A high-performance, stateless OpenAI-compatible API gateway proxy for Google's official `gemini-cli`. 

This proxy translates incoming `/v1/chat/completions` (streaming/non-streaming) and `/v1/models` REST payloads into isolated, headless `gemini-cli` subprocess executions and streams Server-Sent Events (SSE) directly back to any standard client (e.g., Open WebUI, VS Code Continue, Emacs gptel).

---

## Why Rust? (Architectural Comparison with Node.js)

When translating an architecture from a JavaScript/Node.js ecosystem into a systems-programming language like Rust, we made fundamental structural shifts under the hood to maximize performance, safety, and simplicity.

| Metric / Aspect | Traditional Node.js Proxy | Our Rust Proxy Design |
| :--- | :--- | :--- |
| **Integration Model** | Downloads `gemini-cli` source as a Git submodule and imports internal code directly. Runs server and Gemini logic in the same JS memory bubble. | Acts as an independent "process manager." Spawns your already-installed `gemini-cli` as an isolated background process, communicating via stdin/stdout pipes. |
| **Footprint & Deployment** | Requires Node.js, `npm install` (hundreds of MB of `node_modules`), and TS compilation. Idles at **40MB - 100MB RAM**. | Single, hyper-lean standalone binary. No `node_modules` required to run the server. Idles at **3MB - 5MB RAM** with zero GC latency. |
| **Protocol Translation** | Bypasses translation by importing CLI code directly, calling Google's SDK directly inside JS. | Speaks OpenAI REST on the front end (to Open WebUI/Emacs) and cleanly maps those payloads into headless `gemini` prompt executions using `stream-json` line-by-line streams on the back end. |

In short, while a Node.js proxy is a heavily integrated script, this Rust version is a lightweight, universal, secure system daemon that wraps the CLI from the outside!

---

## Features

- **Stateless & Parallel:** Spawns a lightweight, headless subprocess per-request. Allows 100% concurrent execution with zero thread-locking or session contamination.
- **Secure Sandbox Execution:** Runs all subprocesses inside the system temporary directory (`temp_dir`) to prevent the LLM from attempting local filesystem or codebase actions.
- **Workspace Trust Bypass:** Automatically overrides headless directory trust checks (`--skip-trust`).
- **Secure Network Binding:** Defaults strictly to `127.0.0.1` (localhost), with optional binding override via environment variables.
- **Real-time Performance Profiling:** Logs process spawn duration, Time-to-First-Token (TTFT), and average generation speed (`tokens/second`) directly to stdout for every query.

---

## Requirements

1. **`gemini`** (Official Google Gemini CLI binary installed in your system PATH).
2. **Active GCP Authentication:**
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```

---

## Supported Models

The proxy exposes the active Vertex AI endpoint matrix:

| Model ID | Target Endpoint | Description |
| :--- | :--- | :--- |
| **`gemini-cli`** | `gemini-3-flash` | Default fast, general-purpose model |
| **`gemini-3-flash`** | `gemini-3-flash` | Standard high-speed generation model |
| **`gemini-3.1-flash-lite`** | `gemini-3.1-flash-lite` | Ultra-fast lightweight model |
| **`gemini-1.5-pro`** | `gemini-1.5-pro` | Stable legacy high-reasoning Pro model |
| **`gemini-2.5-pro`** | `gemini-2.5-pro` | Advanced high-reasoning Pro model |
| **`gemini-3.1-pro-preview`** | `gemini-3.1-pro-preview` | Live cutting-edge high-reasoning Pro model |

---

## Building & Running

### 1. Run with secure defaults (localhost only)
```bash
cargo run --release
```
Server boots instantly on `http://127.0.0.1:8765`.

### 2. Run with custom interface and port
If you need to connect from containerized applications (like Dockerized Open WebUI) without exposing the port to your physical LAN, bind the proxy to your private virtual Docker bridge gateway IP:
```bash
BIND_ADDRESS=172.17.0.1 PORT=8080 cargo run --release
```

---

## Client Integration

### Open WebUI (Docker)

Start your Open WebUI container with `OPENAI_API_BASE_URL` pointing to your host gateway:

```bash
docker run -d -p 3000:8080 \
  -e OPENAI_API_BASE_URL="http://host.docker.internal:8765/v1" \
  -e OPENAI_API_KEY="local" \
  --name open-webui \
  ghcr.io/open-webui/open-webui:main
```

1. Open Open WebUI (`http://localhost:3000`).
2. Go to **Admin Settings** -> **Connections** -> **OpenAI API**.
3. Click the circular **refresh arrow icon** next to the URL/Key fields.
4. Open WebUI will fetch the supported model list. Select your target Gemini model from the dropdown menu and start chatting.

### Emacs gptel

If you use Emacs, you can configure `gptel` to use this local proxy as a custom OpenAI provider. Add the following Elisp configuration to your init file:

```elisp
(use-package gptel
  :config
  (gptel-make-openai "GeminiProxy"
    :host "127.0.0.1:8765"
    :key "sk-dummy"
    :stream t
    :models '(gemini-cli
              gemini-3-flash
              gemini-3.1-flash-lite
              gemini-1.5-pro
              gemini-2.5-pro
              gemini-3.1-pro-preview)))
```

---

## Running as a macOS Background Daemon (`launchd`)

On macOS, the native and most robust way to run this proxy persistently in the background is through a user-level **LaunchAgent** using `launchd`. This ensures the proxy starts automatically whenever you log in.

### 1. Compile and Install the Binary
Build and install the binary directly using `cargo install` (this compiles in release mode and puts the binary inside `~/.cargo/bin/`):
```bash
cargo install --path . --force
```

### 2. Create the LaunchAgent Plist Configuration
Create a configuration file at `~/Library/LaunchAgents/com.user.gemini-proxy.plist`:

```bash
touch ~/Library/LaunchAgents/com.user.gemini-proxy.plist
```

Open this file and paste the following XML (replace `YOUR_USERNAME` with your actual macOS username, and verify your `GOOGLE_CLOUD_PROJECT` name):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.user.gemini-proxy</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/YOUR_USERNAME/.cargo/bin/gemini_proxy</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/opt/homebrew/bin</string>
        <key>GOOGLE_CLOUD_PROJECT</key>
        <string>YOUR_GCP_PROJECT_ID</string>
        <key>BIND_ADDRESS</key>
        <string>127.0.0.1</string>
        <key>PORT</key>
        <string>8765</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/gemini-proxy.out.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/gemini-proxy.err.log</string>
</dict>
</plist>
```

### 3. Load and Manage the Daemon

- **Load and Start** the daemon (enabling auto-start on login):
  ```bash
  launchctl load ~/Library/LaunchAgents/com.user.gemini-proxy.plist
  ```

- **Stop** the daemon from running:
  ```bash
  launchctl unload ~/Library/LaunchAgents/com.user.gemini-proxy.plist
  ```

- **Check logs** to verify performance and inspect requests:
  ```bash
  tail -f /tmp/gemini-proxy.out.log
  ```

### 4. Log Rotation & Disk Space Management (Optional)

To ensure these logs never fill up your filesystem, you can register them with macOS's native `newsyslog` utility for automatic, size-based log rotation and compression:

1. Create a newsyslog configuration file (requires `sudo`):
   ```bash
   sudo touch /etc/newsyslog.d/gemini-proxy.conf
   ```

2. Open the file and paste this rule (e.g., via `sudo nano /etc/newsyslog.d/gemini-proxy.conf`):
   ```text
   # logfilename                      mode count size  when flags
   /tmp/gemini-proxy.*.log            644  3     5000  *    J
   ```

*This rule tells macOS to automatically rotate the logs the moment they exceed **5 MB (5000 KB)**, compress them with high-efficiency `bzip2` compression (turning a 5MB text log into ~200KB), and keep only the last **3 historical backups** before automatically purging older ones.*

---

## Acknowledgments & Credits

This project was inspired by the original Node.js implementation: [Intelligent-Internet/gemini-cli-mcp-openai-bridge](https://github.com/Intelligent-Internet/gemini-cli-mcp-openai-bridge). Our Rust rewrite focuses on minimizing resource consumption, adding security-isolated sandboxing, providing native parallelization, and enabling high-resolution terminal performance profiling.



