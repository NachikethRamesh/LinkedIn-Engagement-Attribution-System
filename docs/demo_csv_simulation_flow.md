# CSV Simulation Flow (Deprecated)

CSV-based simulation flow is disabled.

Current supported flow:
1. Run LinkedIn org URL ingestion in real mode.
2. Run identity resolution, intent scoring, and opportunity attribution.
3. Run writeback targets with real `endpoint_url` values.

If you need CSV-backed simulation again, it must be reintroduced explicitly in code and config.
