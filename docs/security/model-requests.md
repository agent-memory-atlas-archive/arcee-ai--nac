# Model request security

All model inference clients disable HTTP redirects so prompts, credentials, and request bodies are not replayed to a redirect destination. This applies to every backend, not only Arcee. Extra headers are validated centrally for every backend and cannot override `Host`, `Authorization`, `Proxy-Authorization`, or `x-api-key` in any letter case; backend-selected credentials remain authoritative. Invalid header names and values are also rejected before dispatch.

Custom API-key endpoints are ordinary model configuration: any absolute public HTTPS base URL with a host is accepted without a separate host allowlist. Embedded URL userinfo is rejected, and plaintext HTTP remains limited to loopback and private-network destinations. Managed Arcee and ChatGPT Codex credentials keep their provider-specific issuer and origin binding.
