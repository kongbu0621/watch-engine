from __future__ import annotations

import re
import shlex
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[1]
GENERATED_MARKDOWN_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "build",
    "dist",
}


def _is_repository_source_markdown(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    return not any(
        part in GENERATED_MARKDOWN_PARTS or part.endswith(".egg-info")
        for part in relative.parts
    )


MARKDOWN_FILES = tuple(
    sorted(path for path in ROOT.rglob("*.md") if _is_repository_source_markdown(path))
)
REQUIRED_PROGRAM_DOCUMENTS = (
    "module-requirements.zh-CN.md",
    "architecture.zh-CN.md",
    "implementation.zh-CN.md",
)
REQUIRED_REUSABLE_MODULE_DOCUMENT = "adoption-guide.zh-CN.md"
REQUIRED_GOVERNANCE_DOCUMENTS = ("SECURITY.zh-CN.md", "DATA-GOVERNANCE.zh-CN.md")
REPOSITORY_BLOB_URL = "https://github.com/kongbu0621/watch-engine/blob/main"


def test_required_document_layers_exist_and_are_discoverable() -> None:
    chinese_readme = (ROOT / "README.zh-CN.md").read_text(encoding="utf-8")
    for name in (*REQUIRED_PROGRAM_DOCUMENTS, REQUIRED_REUSABLE_MODULE_DOCUMENT):
        assert (ROOT / "docs" / name).is_file()
        assert f"(docs/{name})" in chinese_readme
    for name in REQUIRED_GOVERNANCE_DOCUMENTS:
        assert (ROOT / name).is_file()
        assert f"({name})" in chinese_readme


def test_every_english_markdown_document_has_a_linked_chinese_version() -> None:
    english_documents = tuple(
        path
        for path in MARKDOWN_FILES
        if not path.name.endswith(".zh-CN.md")
    )
    assert english_documents

    for english in english_documents:
        chinese = english.with_name(f"{english.stem}.zh-CN.md")
        assert chinese.is_file(), f"missing Chinese version for {english.relative_to(ROOT)}"

        english_content = english.read_text(encoding="utf-8")
        chinese_content = chinese.read_text(encoding="utf-8")
        relative = english.relative_to(ROOT)
        absolute_chinese = (
            f"]({REPOSITORY_BLOB_URL}/{relative.with_name(chinese.name).as_posix()})"
        )
        assert (
            f"]({chinese.name})" in english_content
            or absolute_chinese in english_content
        )
        assert f"[English]({english.name})" in chinese_content


def test_chinese_repository_guidance_preserves_key_english_terms() -> None:
    chinese_guidance = (ROOT / "AGENTS.zh-CN.md").read_text(encoding="utf-8")
    for term in (
        "Observer",
        "TransitionPolicy",
        "Trigger",
        "EventSink",
        "Authority",
        "Observation",
        "Outbox",
        "at-least-once",
    ):
        assert term in chinese_guidance


def test_chinese_documents_retain_searchable_english_terms() -> None:
    english_term = re.compile(r"[A-Za-z][A-Za-z0-9_.-]+")
    chinese_text = re.compile(r"[\u3400-\u9fff]")
    chinese_documents = tuple(
        path for path in MARKDOWN_FILES if path.name.endswith(".zh-CN.md")
    )
    assert chinese_documents
    for document in chinese_documents:
        content = document.read_text(encoding="utf-8")
        assert chinese_text.search(content), (
            f"Chinese document contains no Chinese text: {document.relative_to(ROOT)}"
        )
        assert english_term.search(content), (
            f"Chinese document lost all English terminology: {document.relative_to(ROOT)}"
        )

    required_terms = {
        "README.zh-CN.md": ("Observer", "Authority", "TransitionPolicy", "Outbox", "EventSink"),
        "SECURITY.zh-CN.md": ("SQLite", "Observer", "TransitionPolicy", "EventSink"),
        "DATA-GOVERNANCE.zh-CN.md": ("Observation", "Authority", "WatchEvent", "EventSink"),
        "AGENTS.zh-CN.md": ("Observer", "Authority", "TransitionPolicy", "Outbox", "at-least-once"),
        "CONTRIBUTING.zh-CN.md": ("Core", "Adapter", "Architecture", "Implementation"),
        "docs/module-requirements.zh-CN.md": ("Public API", "Observer", "Authority", "Outbox"),
        "docs/architecture.zh-CN.md": ("Runtime", "TransitionPolicy", "Authority", "EventSink"),
        "docs/implementation.zh-CN.md": ("Public API", "SQLite", "OutboxDispatcher", "CI"),
        "docs/adoption-guide.zh-CN.md": (
            "Observer",
            "TransitionPolicy",
            "EventSink",
            "WatchDefinition",
        ),
    }
    for relative_path, terms in required_terms.items():
        content = (ROOT / relative_path).read_text(encoding="utf-8")
        for term in terms:
            assert term in content, f"{term} missing from {relative_path}"


def test_bilingual_requirement_is_traceable_from_requirement_to_implementation() -> None:
    requirements = (ROOT / "docs/module-requirements.zh-CN.md").read_text(encoding="utf-8")
    implementation = (ROOT / "docs/implementation.zh-CN.md").read_text(encoding="utf-8")
    assert "NFR-08 中文文档可用性" in requirements
    assert "NFR-08 中文文档可用性" in implementation
    assert ".zh-CN.md" in requirements
    assert ".zh-CN.md" in implementation


def test_sdist_manifest_keeps_chinese_counterparts_of_packaged_documents() -> None:
    selected: set[Path] = set()
    for raw_line in (ROOT / "MANIFEST.in").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        command, *arguments = shlex.split(line)
        if command == "include":
            for pattern in arguments:
                selected.update(
                    path.relative_to(ROOT)
                    for path in ROOT.glob(pattern)
                    if path.is_file() and path.suffix == ".md"
                )
        elif command == "recursive-include":
            directory, *patterns = arguments
            for pattern in patterns:
                selected.update(
                    path.relative_to(ROOT)
                    for path in (ROOT / directory).rglob(pattern)
                    if path.is_file() and path.suffix == ".md"
                )

    english_documents = {
        path for path in selected if not path.name.endswith(".zh-CN.md")
    }
    assert english_documents
    for english in english_documents:
        chinese = english.with_name(f"{english.stem}.zh-CN.md")
        assert chinese in selected, f"{english} is packaged without {chinese}"


def test_pypi_readme_uses_portable_absolute_repository_links() -> None:
    link_pattern = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    targets = link_pattern.findall(readme)
    assert targets
    for target in targets:
        assert target.startswith(("https://", "#")), (
            f"PyPI README contains a repository-relative link: {target}"
        )
        if target.startswith("https://github.com/"):
            assert target.startswith(f"{REPOSITORY_BLOB_URL}/")


def test_readmes_explain_purpose_and_reuse_decision_up_front() -> None:
    english = (ROOT / "README.md").read_text(encoding="utf-8")
    chinese = (ROOT / "README.zh-CN.md").read_text(encoding="utf-8")

    for required in (
        "## What this module is for",
        "| Reuse question |",
        "Reuse this module when:",
        "Do not use this module when:",
    ):
        assert required in english

    for required in (
        "## 这个模块有什么用",
        "| 复用判断 |",
        "适合复用这个模块的情况：",
        "不适合使用这个模块的情况：",
    ):
        assert required in chinese

    first_english_detail = english.index("## Core concepts")
    first_chinese_detail = chinese.index("## 核心概念")
    assert english.index("## What this module is for") < first_english_detail
    assert chinese.index("## 这个模块有什么用") < first_chinese_detail


def test_relative_markdown_links_resolve() -> None:
    link_pattern = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
    missing: list[tuple[str, str]] = []
    for markdown in MARKDOWN_FILES:
        for target in link_pattern.findall(markdown.read_text(encoding="utf-8")):
            if "://" in target or target.startswith("#"):
                continue
            relative_path = target.split("#", 1)[0]
            if not (markdown.parent / relative_path).resolve().exists():
                missing.append((str(markdown.relative_to(ROOT)), target))
    assert missing == []


def test_python_documentation_blocks_are_syntax_valid() -> None:
    block_pattern = re.compile(r"```python\n(.*?)```", re.DOTALL)
    for markdown in MARKDOWN_FILES:
        blocks = block_pattern.findall(markdown.read_text(encoding="utf-8"))
        for index, block in enumerate(blocks, start=1):
            compile(block, f"{markdown}:python-block-{index}", "exec")


def test_public_docs_do_not_contain_operational_identifiers() -> None:
    patterns = {
        "email address": re.compile(
            r"(?i)[a-z0-9._%+-]+@(?!example\.(?:com|org|net))[a-z0-9.-]+\.[a-z]{2,}"
        ),
        "IPv4 address": re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])"),
        "deployment path": re.compile(r"/(?:home|root|opt|var/lib)/[^\s`]+"),
    }
    for markdown in MARKDOWN_FILES:
        content = markdown.read_text(encoding="utf-8")
        for label, pattern in patterns.items():
            assert pattern.search(content) is None, (
                f"{label} leaked in {markdown.relative_to(ROOT)}"
            )


def test_candidate_version_and_release_status_are_consistent() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    version = project["version"]
    assert version == "0.2.0"

    readme = (ROOT / "README.zh-CN.md").read_text(encoding="utf-8")
    adoption = (ROOT / "docs/adoption-guide.zh-CN.md").read_text(encoding="utf-8")
    implementation = (ROOT / "docs/implementation.zh-CN.md").read_text(encoding="utf-8")
    architecture = (ROOT / "docs/architecture.zh-CN.md").read_text(encoding="utf-8")

    for document in (readme, adoption, implementation):
        assert version in document
        assert "未发布" in document or "尚未发布" in document
        assert "v0.1.0" in document

    assert f"@v{version}" in adoption
    assert "git+https://github.com/kongbu0621/watch-engine.git@v0.1.0" not in adoption
    assert "load_watch_event_schema()" in architecture
    assert "Event v1" in architecture
    assert "v0.1.0" in architecture


def test_dependency_audit_covers_installed_development_environment() -> None:
    command = "python -m pip_audit --local --progress-spinner=off"
    for relative_path in (
        ".github/workflows/ci.yml",
        "AGENTS.md",
        "CONTRIBUTING.md",
        "README.md",
        "README.zh-CN.md",
        "docs/implementation.zh-CN.md",
    ):
        content = (ROOT / relative_path).read_text(encoding="utf-8")
        assert command in content, f"local dependency audit missing from {relative_path}"

    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "python -m pip_audit . --progress-spinner=off" not in workflow

    configuration = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert "setuptools>=83" in configuration["build-system"]["requires"]
    development_dependencies = configuration["project"]["optional-dependencies"]["dev"]
    assert "setuptools>=83" in development_dependencies
    assert "mypy>=1.11,<3" in development_dependencies
    assert "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7" in workflow
    assert "actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97 # v7" in workflow

    implementation = (ROOT / "docs/implementation.zh-CN.md").read_text(encoding="utf-8")
    for expected in (
        "`mypy>=1.11,<3`",
        "`checkout` v7",
        "3d3c42e5aac5ba805825da76410c181273ba90b1",
        "`setup-python` v7",
        "5fda3b95a4ea91299a34e894583c3862153e4b97",
    ):
        assert expected in implementation


def test_supported_python_and_package_validation_are_enforced_in_ci() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    configuration = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    for version in ("3.11", "3.12", "3.13", "3.14"):
        assert f'"{version}"' in workflow
    assert "twine>=6,<7" in configuration["project"]["optional-dependencies"]["dev"]
    assert "python -m twine check dist/*" in workflow
    assert "python scripts/verify_sdist_bilingual.py dist/*.tar.gz" in workflow
