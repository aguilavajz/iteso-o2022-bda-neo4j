## 2024-10-21 - [Hardcoded Database Secrets]
**Vulnerability:** Hardcoded database fallback password `T4t13584#` for Neo4j connection in both `main.py` and `gds.py`.
**Learning:** Found multiple entry points using the same hardcoded credential logic. We should not fall back to static passwords for local environments.
**Prevention:** Rely entirely on environment variables like `os.getenv("NEO4J_PASSWORD")` and ensure local developers set these up.
