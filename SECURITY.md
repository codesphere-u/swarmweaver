# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

## Security Model & Guarantees

1. **Cryptographic Checkpoint Integrity:** `CheckpointJournal` links every record via a SHA-256 hash chain (`GENESIS_HASH -> prev_hash -> entry_hash`). Before replaying any cached task output on `resume=True`, `SwarmOrchestrator` executes `verify_integrity(raise_on_error=True)` to detect payload tampering, sequence discontinuities, or truncated journals.
2. **Cryptographic Lease Tokens:** `FileScopeLeaseManager` signs every `LeaseGrant` using a per-instance cryptographic salt and SHA-256 digest, enforcing TTL expiration and scope containment.
3. **Zero External Runtime Dependencies:** `swarmweaver` relies exclusively on the Python 3.11+ standard library at runtime, eliminating third-party supply-chain attack vectors.

## Reporting a Vulnerability

Please report security vulnerabilities to **Akandwanaho Ivan Alvin** (`codesphere-u@users.noreply.github.com`) or via GitHub Private Vulnerability Reporting on `https://github.com/codesphere-u/swarmweaver/security`.
