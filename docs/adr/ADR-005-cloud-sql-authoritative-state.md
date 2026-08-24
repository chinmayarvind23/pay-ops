# ADR-005: Cloud SQL owns authoritative incident state

PostgreSQL stores incidents, transitions, approvals, action audit, and checkpoint metadata. Redis remains ephemeral.
