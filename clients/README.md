# Clients

This directory contains the client boundaries represented within God's Freedom Ark.

Each client receives a permanent client identifier and an independent directory:

```text
clients/
├── C001-<client-name>/
├── C002-<client-name>/
└── ...
```

A client directory may contain:

- client metadata
- client-level data contracts
- documentation for the client's canonical data estate
- independent engagements performed for that client

## Ownership Principle

Clients own canonical source and shared data.

Individual engagements own their implementations, processing state, and project-specific outputs.

Engagements should consume shared client data through explicit data contracts rather than depending on another engagement's internal implementation.

## Naming

Client directories follow:

```text
C###-<client-slug>
```

For example:

```text
C001-northstar-retail
```

The `C###` identifier is permanent. The human-readable client name may change without changing the identifier.
