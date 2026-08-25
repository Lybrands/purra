# Examples

These examples use deterministic local model gateways and in-memory adapters.
They need no credentials and are not production persistence examples.

## Python

From the repository root:

```bash
python -m pip install -e .
python examples/python/quickstart.py
```

See [`python/quickstart.py`](python/quickstart.py).

## TypeScript

From the repository root:

```bash
cd typescript
npm ci
npm run example
```

See [`../typescript/examples/quickstart.ts`](../typescript/examples/quickstart.ts).

Both examples run the same flow: the model requests a read-only lookup tool,
the host executes it, and the model returns a final answer through a canonical
Run.
