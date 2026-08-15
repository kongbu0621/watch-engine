# 贡献指南

[English](CONTRIBUTING.md) | 简体中文

保持 Engine 领域无关。业务抓取、产品语义解释、Provider-specific Notification、Credential 和
Deployment Secret 都属于下游 Adapter，不得进入 Core。

提交变更前，如果 Contract 受到影响，必须同步更新需求、Architecture、Implementation 和
Adoption 文档，然后运行：

```bash
python -m pytest
python -m ruff check .
python -m mypy src
python -m build
python -m twine check dist/*
python scripts/verify_sdist_bilingual.py dist/*.tar.gz
python -m pip_audit --local --progress-spinner=off
```

测试数据必须为 Synthetic Fixture（合成测试数据）。不得在 Commit、Issue、Log 或 Pull Request 中
包含 Personal Information、Credential、私有下游仓库名、Production URL、Database File 或完整抓取
响应。安全漏洞应通过[安全策略](SECURITY.zh-CN.md)所述的私有渠道报告。

修改仓库任意源码目录中由本仓库维护且面向人的英文 Markdown 文档时，必须在同一变更中同步修改
同目录的 `.zh-CN.md` 中文版本。中文文件必须包含有实际说明价值的中文正文，不能只是使用中文文件名
的英文副本。中文正文应保留 Public API 名、状态值、Command、Path 和关键英文工程术语，使其可以
直接搜索并追溯到代码。
