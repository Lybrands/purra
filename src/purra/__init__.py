"""Business-agnostic, independently runnable PurrA.

Use :class:`purra.api.AgentCore` as the high-level event-stream entry
point. Applications and domains implement or register only the ports and
capabilities declared by this package; Core never imports them back.
"""

__all__: list[str] = []
