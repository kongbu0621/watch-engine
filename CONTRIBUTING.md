# Contributing

English | [简体中文](CONTRIBUTING.zh-CN.md)

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

When changing a human-facing English Markdown document, update its sibling `.zh-CN.md` version in
the same change. Keep public API names, state values, commands, paths, and important engineering
terms in English inside the Chinese text so they remain directly searchable and traceable to code.
