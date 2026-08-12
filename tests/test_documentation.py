from __future__ import annotations

import re
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


def test_public_docs_do_not_name_private_adopters_or_product_targets() -> None:
    forbidden = ("apple-refurb-monitor", "apple-cn-refurb", "mac studio")
    for markdown in MARKDOWN_FILES:
        content = markdown.read_text(encoding="utf-8").lower()
        for value in forbidden:
            assert value not in content, f"{value!r} leaked in {markdown.relative_to(ROOT)}"
