# Data Governance and Privacy

## Data minimization rule

`watch-engine` is designed for operational state, not personal data. Do not put credentials,
tokens, cookies, private keys, personal information, precise user identifiers, complete HTTP
responses, or regulated/sensitive data in `watch_id`, Observation `state`/`evidence`/`error`, or
WatchEvent `subject`/`payload`.

The runtime redacts messages from caught exceptions and stores only their type. This protects
against accidental secrets embedded in exception text. Deliberately supplied model fields cannot
be classified automatically; adopters must minimize or pseudonymize them before calling the API.

## Storage and retention

- JSON fields are limited to 1 MiB encoded size and error fields to 2,048 characters.
- POSIX SQLite database, WAL, and SHM files are owner-only (`0600`); use a `0700` parent directory.
- `purge_before(cutoff, watch_id=...)` removes old terminal event history and non-authoritative
  observations while preserving undelivered events and current Authority.
- `delete_watch(watch_id)` refuses outstanding delivery by default. The
  `allow_undelivered=True` override is intentionally destructive.
- `compact_storage()` performs WAL checkpoint and `VACUUM`; run it only after other database owners
  stop. Secure deletion cannot guarantee erasure from SSD remapping, snapshots, backups, journal
  exports, or external logs.

Adopters must define a documented retention period, schedule purging, protect and expire backups,
and test restoration and deletion. The library does not silently choose a legal retention period.

## Public repository and cross-border considerations

Repository examples and test fixtures must be synthetic and anonymized. Never commit production
databases, `.env` files, certificates, keys, customer identifiers, or incident payloads.

For use in mainland China, the adopter—not this general-purpose library—must determine whether the
Personal Information Protection Law, Data Security Law, Cybersecurity Law, sector rules, consent,
localization, security assessment, or cross-border transfer requirements apply. Avoid collecting
personal information where operational state is sufficient, and obtain qualified legal advice for
regulated or cross-border deployments.

Authoritative references (review the current text at deployment time):

- [Personal Information Protection Law](https://www.stats.gov.cn/gk/tjfg/xgfxfg/202503/t20250310_1958923.html)
- [Data Security Law](https://www.npc.gov.cn/npc/c2/c30834/202106/t20210610_311888.html)
- [Cybersecurity Law, amended text effective 2026](https://www.cac.gov.cn/2025-12/29/c_1768735112911946.htm)
- [CAC cross-border data policy guidance](https://www.cac.gov.cn/2025-10/31/c_1763633376984070.htm)

This document is engineering guidance, not legal advice.
