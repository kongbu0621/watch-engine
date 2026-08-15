from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[1]
MARKDOWN_FILES = tuple(sorted((*ROOT.glob("*.md"), *ROOT.glob("docs/*.md"))))
REQUIRED_PROGRAM_DOCUMENTS = (
    "module-requirements.zh-CN.md",
    "architecture.zh-CN.md",
    "implementation.zh-CN.md",
)
REQUIRED_REUSABLE_MODULE_DOCUMENT = "adoption-guide.zh-CN.md"
REQUIRED_GOVERNANCE_DOCUMENTS = ("SECURITY.zh-CN.md", "DATA-GOVERNANCE.zh-CN.md")


def test_required_document_layers_exist_and_are_discoverable() -> None:
    chinese_readme = (ROOT / "README.zh-CN.md").read_text(encoding="utf-8")
    for name in (*REQUIRED_PROGRAM_DOCUMENTS, REQUIRED_REUSABLE_MODULE_DOCUMENT):
        assert (ROOT / "docs" / name).is_file()
        assert f"(docs/{name})" in chinese_readme
    for name in REQUIRED_GOVERNANCE_DOCUMENTS:
        assert (ROOT / name).is_file()
        assert f"({name})" in chinese_readme


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
    assert "setuptools>=83" in configuration["project"]["optional-dependencies"]["dev"]
    assert "actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09 # v5" in workflow
    assert "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1 # v6" in workflow
