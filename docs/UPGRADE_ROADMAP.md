# Upgrade Roadmap

This repository has been upgraded to better reflect production-minded data engineering practices and stronger job-market positioning.

## Improvements Added

### Engineering Foundations
- Centralized project settings in `config/settings.py`
- Environment-aware paths instead of hardcoded local machine paths
- Reusable database and AWS configuration objects
- Local runtime directory creation for logs and output folders

### Reliability and Quality
- Automated `pytest` test suite for core ingestion modules
- GitHub Actions CI workflow for linting and tests
- `ruff` linting configuration in `pyproject.toml`
- Dedicated `requirements.txt` and `requirements-dev.txt`

### Developer Experience
- `.env.example` for safe environment bootstrapping
- `docker-compose.yml` for reproducible PostgreSQL setup
- Cleaner pipeline scripts that all read from shared settings
- Airflow DAG updated to resolve the repository path dynamically

## Why These Changes Matter for Hiring

These changes make the project easier to explain as more than a one-off ETL demo:

- It is easier for another engineer to clone and run
- Core logic is now testable and CI-validated
- Configuration is cleaner and more portable
- The repo shows awareness of maintainability, reproducibility, and deployment hygiene

That combination signals stronger readiness for data engineering or analytics engineering roles.

## Best Next Upgrades

If we continue improving this project, the highest-value next steps are:

1. Move staging and mart transformations into dbt or a SQL-first modeling layer.
2. Replace full-table `replace` loads with incremental or merge-based loading.
3. Add integration tests against a real PostgreSQL instance in CI.
4. Add pipeline run metadata such as run ids, task durations, and freshness checks.
5. Containerize Airflow alongside PostgreSQL for a full local orchestration stack.
6. Add a small business-facing dashboard or Streamlit app for live project demos.

## Suggested Resume / Interview Framing

You can describe this project as:

Built an end-to-end Malaysian CPI analytics pipeline using Python, PostgreSQL, Airflow, AWS S3, and Power BI, with layered warehouse modeling, automated testing, centralized configuration, and CI-driven quality checks.
