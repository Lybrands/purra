# Private Mem0 SDK extension

The managed client calls Mem0's native text and embedding protocols through PurrA's current operation. The pinned SDK constructors do not accept these instances directly. This extension adds instance injection without registering global factories, replacing host modules, or changing extraction and retrieval algorithms.

`upstream.json` pins the official input versions and SHA-256 hashes. The extension ships inside `purra-mem0`, under a private path, with Apache-2.0 attribution. It is not a republished official `mem0ai` distribution.

- Python: install `mem0ai==2.0.19`, then run `python integrations/mem0/python/scripts/sync_sdk.py /path/to/mem0/memory/main.py`. Commit the generated private module. `--check` verifies exact reproduction. Other SDK modules remain provided by the pinned official dependency.
- TypeScript: run `npm ci` and `npm run build` inside `integrations/mem0/typescript`. The build verifies `mem0ai@3.1.7`, generates the private JavaScript module and ships it in `dist`. Consumers need no official Mem0 package for the managed path. The official SDK remains a development dependency and an optional host choice for raw clients.

Only the managed factory uses the injection entry. The private SDK's `from_config` / `fromConfig` methods are not public PurrA entry points. Reset retains injected providers. Debug binding labels distinguish injected providers from upstream default model settings, which are not evidence of actual model calls.

An SDK update requires reviewing the input hash, transformation, dependencies, license and native protocol call sites. Run both native-boundary and full SDK checks, then install wheels/tarballs outside the source tree with LangChain imports prohibited and compile the public TypeScript consumer with `skipLibCheck: false`.

TypeScript creation waits for storage initialization. Failed initialization releases its history handle and SDK-owned local vector handle where the SDK exposes one. Python releases history and owned Qdrant handles; caller-supplied Qdrant clients remain caller-owned. Constructors of third-party backends remain responsible for handles they allocate before throwing. Successful SDK instances remain host-owned; this change does not add a general cross-backend shutdown API.

Deterministic SDK fixtures use local Qdrant/SQLite and substitute model callbacks. They do not validate live model providers, remote vector backends or downstream applications.
