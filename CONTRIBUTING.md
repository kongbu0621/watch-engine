# Contributing

Keep the engine domain-neutral. Business scraping, product interpretation, provider-specific
notifications, credentials, and deployment secrets belong in downstream adapters.

Before submitting a change, update the requirements, architecture, implementation, and adoption
documents when their contracts are affected, then run:

```bash
python -m pytest
python -m ruff check .
python -m mypy src
python -m build
python -m pip_audit --local --progress-spinner=off
```

Use synthetic fixtures. Do not include personal information, credentials, private downstream
repository names, production URLs, database files, or complete captured responses in commits,
issues, logs, or pull requests. Report vulnerabilities through `SECURITY.md`.
