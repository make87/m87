<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./.github/assets/logo-dark.svg">
  <img alt="make87" src="./.github/assets/logo-light.svg" width="200">
</picture>

# m87

**Secure, outbound-only access to physical devices — a native-feeling development, debugging, and software deployment experience.**

[![E2E Tests](https://github.com/make87/make87/actions/workflows/e2e-tests.yml/badge.svg)](https://github.com/make87/make87/actions/workflows/e2e-tests.yml)
[![Latest release](https://img.shields.io/github/v/release/make87/make87?sort=semver)](https://github.com/make87/make87/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0%20%7C%20AGPL--3.0-blue)](./LICENSE.md)
[![Stars](https://img.shields.io/github/stars/make87/make87)](https://github.com/make87/make87/stargazers)

</div>

---

**m87 is make87's command line and device runtime** for connecting to, debugging, and deploying software to distributed hardware fleets — all over a single outbound connection and without VPNs or inbound firewall rules.

In this repo:
- **`m87` command** = what you type in your terminal
- **m87 runtime** = the on-device process that maintains the outbound connection and executes actions

---

## ✨ What Makes m87 Different

m87 isn't *just* remote access — it's designed so **working with real devices feels like local development and deployment**:

* **Outbound-only access:** works behind NATs / firewalls without opening inbound ports.
* **Identity-based access — no SSH keys to copy:** sign in through your organization's identity provider (e.g. Google) and grant or revoke access **by role**. Share devices and fleets with your team through your org instead of distributing and rotating keys.
* **Native dev experience:** shell, port/sockets forwarding, logs, and live debugging feel like you're working locally.
* **Deployment-ready:** one command line that transitions from access to orchestrating software deployments across fleets.

If you've ever SSH'd into an embedded device only to run into network traps or scaling pain, m87 makes those workflows easy and repeatable.

---

## ⚡ See it in action

Once a device is connected, everything below runs over that single outbound connection — the device could be behind a carrier NAT on the other side of the planet and these still work as if it were on your desk.

```bash
m87 devices list                       # see every device you can reach
```

**Work on the device like it's local** — drop into a shell, or run one-off commands:

```bash
m87 <device> shell                     # interactive shell on the device
m87 <device> exec -- uptime            # run a single command, get the output
m87 <device> docker ps                 # inspect running containers
m87 <device> metrics                   # live CPU / memory / disk
```

**Reach services running on the device** — forward a remote port to localhost, then open it in your browser or hit it with curl:

```bash
m87 <device> forward 8080              # localhost:8080 → device:8080
m87 <device> forward 3000:80           # localhost:3000 → device:80
```

**Move files both ways** — copy artifacts up, or live-sync your code as you edit it:

```bash
m87 cp ./firmware.bin <device>:/tmp/   # SCP-style copy to the device
m87 sync --watch ./src <device>:/app   # rsync-style sync that follows your edits
```

**Deploy and watch software** — ship containers and stream logs as they run:

```bash
m87 <device> docker compose up -d      # deploy with familiar tooling
m87 <device> logs -f                   # follow live logs from the device
```

**Use your own SSH tooling** — enable host resolution and `ssh` (and `scp`, IDE remote dev, etc.) just work:

```bash
m87 ssh enable
ssh <device>.m87                       # native ssh over the m87 tunnel
```

New to m87? Jump to the [Quick Start](#-quick-start-try-in-5-minutes) to install and connect your first device.

---

## 🚀 Quick Start (Try in 5 minutes)

### 1. Install the m87 command line

Install the `m87` command on your **developer machine**. (You'll run the m87 runtime on the edge device in step 3.)

#### ⚡ One-liner (recommended)

Installs the latest version to `$HOME/.local/bin`:

```bash
curl -fsSL https://get.make87.com | sh
```

<details>
<summary>Other install options</summary>

**From releases**

Download a pre-built binary from the [releases page](https://github.com/make87/m87/releases) and place it in your `$PATH` (e.g., `$HOME/.local/bin` or `/usr/local/bin`).

**From source**

Build the binary and move it to a location in your `$PATH`:

```bash
git clone https://github.com/make87/m87.git
cd make87
cargo build --release
cp target/release/m87 $HOME/.local/bin/
```

**Via Docker (no local install)**

Run m87 from a container without installing anything locally. Useful for CI pipelines or keeping your system clean.

```bash
# Build the image
git clone https://github.com/make87/m87.git
cd make87
docker build -f m87-client/Dockerfile -t m87 .

# Run commands (config persists in ~/.config/m87)
docker run -it --rm \
  --user "$(id -u):$(id -g)" \
  -v "$HOME/.config/m87:/.config/m87" \
  -e HOME=/ \
  m87 login
```

For convenience, add an alias to your shell rc:

```bash
alias m87='docker run -it --rm --user "$(id -u):$(id -g)" -v "$HOME/.config/m87:/.config/m87" -e HOME=/ m87'
```

</details>

### 2. Set up your developer machine

Login to create your account (opens browser for OAuth):

```bash
m87 login
```

### 3. Set up the edge device

On the edge device, start the **m87 runtime** (the on-device process):

```bash
m87 runtime run --email you@example.com
```

This registers the device (printing a request ID) and waits for approval. Once approved, the runtime starts automatically.

### 4. Approve the device

On your developer machine, approve the pending device:

```bash
m87 devices approve <request-id>
```

*(You can also approve via the web UI.)*

Now you're connected — no inbound access, no firewall rules, and no VPN required.

---

## 🧪 Testing

This repo has three layers of tests; the right command depends on what
you're trying to verify.

### Unit tests (fast, no external dependencies)

```bash
cargo test --workspace
```

Covers the time parser, event aggregator, status summary, deployment
manager state machine, and so on. ~200 tests, completes in a few seconds.
Use this on every change.

### E2E compile gate (no Docker, no test execution)

```bash
cargo e2e-check
```

Just typechecks the e2e crate against the current `m87-client` /
`m87-server` / `m87-shared` API. Catches drift before you spend a full
e2e run on it. CI runs this on every PR.

### Full e2e suite (Docker required)

```bash
cargo e2e
```

Spins up real `m87-server`, MongoDB, and runtime containers and exercises
the CLI surface end-to-end (`deploy`, `logs`, `status`, `job trigger`,
lifecycle, etc.). Around 60 tests; takes 5–10 minutes on a typical
laptop.

Defaults to running 4 tests in parallel (each owns its own container
set). Override per-machine if needed:

```bash
RUST_TEST_THREADS=1 cargo e2e      # resource-constrained host (~25 min)
RUST_TEST_THREADS=8 cargo e2e      # beefy CI runner
```

Run a single test:

```bash
cargo e2e -- --nocapture deployment::test_deploy_service_lands_in_spec
```

Requires:

* Docker daemon running and accessible
* Rust toolchain (build of the e2e harness uses `testcontainers`)

CI runs this on PRs that touch `m87-client/**`, `m87-server/**`,
`m87-shared/**`, or `tests/**`. See `.github/workflows/e2e-tests.yml`.

---

## 🧱 Core Concepts

### 🛠 Development & Debugging

Use native OS tools and IDEs as if the device were local:

```bash
# Run shell
m87 <device> shell

# Forward a port for a debugging server
m87 <device> forward 8080:localhost:3000
```

### 📦 Software Deployment

Deploy containers and services using familiar commands:

```bash
m87 <device> docker compose up -d
```

(*More deployment commands and flags coming soon.*)

---

## 📚 Detailed Docs

Full documentation, examples, and tutorials are available here:
[m87-client/](./m87-client/)

---

## 🧪 Building from Source

Requires:

* Rust 1.85+
* Git

```bash
git clone https://github.com/make87/m87.git
cd make87
cargo build --release
```

---

## 🤝 Contributing

Contributions, bug reports, and feedback are welcome! Whether you're a tinkerer, an early adopter, or looking to integrate m87 into your stack:

1. Open issues for ideas and bugs
2. Submit PRs — we review quickly

Let's build a better developer experience for physical systems.

---

## 📜 License

* [m87-client](./m87-client/), [m87-shared](./m87-shared/): **Apache-2.0**
* [m87-server](./m87-server/): **AGPL-3.0-or-later**

---

## ⭐ If This Excites You

Give the repo a ⭐ and share your feedback — every star helps drive adoption and signals to others that this tool is worth exploring.