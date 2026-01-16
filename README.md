# m87

**Secure, outbound-only access to physical devices — with a native-feeling development, debugging, and software deployment experience.**

**m87 is make87's command line and device runtime** for connecting to, debugging, and deploying software to distributed hardware fleets — all over a single outbound connection and without VPNs or inbound firewall rules.

In this repo:
- **`m87` command** = what you type in your terminal
- **m87 runtime** = the on-device process that maintains the outbound connection and executes actions

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

Once approved, you can interact with your device:

```bash
m87 devices list
m87 <device> shell
m87 <device> docker ps
```

Now you're connected — no inbound access, no firewall rules, and no VPN required.

👀 Try this next:

* forward a local port to a remote service
* run an IDE remote development session

---

## ✨ What Makes m87 Different

m87 isn't *just* remote access — it's designed so **working with real devices feels like local development and deployment**:

* **Outbound-only access:** works behind NATs / firewalls without opening inbound ports.
* **Native dev experience:** shell, port/sockets forwarding, logs, and live debugging feel like you're working locally.
* **Deployment-ready:** one command line that transitions from access to orchestrating software deployments across fleets.

If you've ever SSH'd into an embedded device only to run into network traps or scaling pain, m87 makes those workflows easy and repeatable.

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