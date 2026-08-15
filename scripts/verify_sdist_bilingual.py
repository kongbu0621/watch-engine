from __future__ import annotations

import sys
import tarfile
from pathlib import PurePosixPath


def verify_sdist(path: str) -> None:
    with tarfile.open(path, mode="r:gz") as archive:
        markdown = {
            PurePosixPath(member.name)
            for member in archive.getmembers()
            if member.isfile() and member.name.endswith(".md")
        }

    if not markdown:
        raise SystemExit(f"{path}: source distribution contains no Markdown documents")

    missing: list[str] = []
    pairs = 0
    for english in sorted(markdown):
        if english.name.endswith(".zh-CN.md"):
            continue
        chinese = english.with_name(f"{english.stem}.zh-CN.md")
        if chinese not in markdown:
            missing.append(f"{english} -> {chinese}")
        else:
            pairs += 1

    if missing:
        raise SystemExit(
            f"{path}: English Markdown missing Chinese sdist counterpart: "
            + "; ".join(missing)
        )
    if pairs == 0:
        raise SystemExit(f"{path}: source distribution contains no bilingual pair")


def main(arguments: list[str]) -> None:
    if len(arguments) != 1:
        raise SystemExit("usage: verify_sdist_bilingual.py DIST.tar.gz")
    verify_sdist(arguments[0])


if __name__ == "__main__":
    main(sys.argv[1:])
