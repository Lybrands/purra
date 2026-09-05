# Mem0 attribution and local modifications

This package includes a private module derived from Mem0 (https://github.com/mem0ai/mem0), copyright Mem0 contributors, licensed under Apache-2.0 (MEM0-LICENSE).

Python uses mem0ai 2.0.19 memory/main.py; TypeScript uses mem0ai 3.1.7 dist/oss/index.mjs. PurrA modifies constructor provider injection and, in TypeScript, preserves injected providers across reset. The extension also identifies injected providers and releases SDK-owned local handles on failed initialization; the TypeScript managed factory awaits initialization before returning. These are PurrA-owned private copies, not replacements for or modifications to the installed official mem0ai package. The SDK algorithms remain upstream-owned.

The repository integrations/mem0/sdk/upstream.json records input hashes. Python scripts/sync_sdk.py verifies/regenerates the checked-in private module; TypeScript scripts/build-sdk.mjs generates dist/sdk-memory.js during the package build. No patching or source generation happens when a consumer imports the package.

Generated source is reviewed by verifying the pinned input and the small transformation, then exercising SDK contract and installed-package tests. SDK upgrades require an explicit hash/contract review. Native provider clients are not constructed on the managed path. The private SDK is not part of the public API.
