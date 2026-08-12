# Security Policy

English | [简体中文](SECURITY.zh-CN.md)

## Supported version

Security fixes are provided for the latest released `0.1.x` version. Users should upgrade to the
latest patch release before reporting a defect already fixed on `main`.

## Reporting a vulnerability

Do not disclose credentials, personal information, exploit details, or vulnerable production
data in a public issue. Use a
[private GitHub security advisory](https://github.com/kongbu0621/watch-engine/security/advisories/new).
If private reporting is unavailable, open a minimal public issue requesting a private contact
channel, without technical details.

Include affected version, impact, reproduction conditions, and a redacted proof of concept. The
maintainers will acknowledge the report when capacity permits; no fixed response-time SLA is
promised by this community project.

## Deployment baseline

- Run with a dedicated unprivileged account and an owner-only (`0700`) state directory.
- Do not place the SQLite database on a shared or untrusted filesystem.
- Keep database, WAL, and SHM files at `0600`; `SQLiteStore` enforces this on POSIX systems.
- Do not log complete observations, events, exception objects, environment variables, or database
  rows.
- Keep Python, dependencies, and the host operating system patched.
- Backups must use the same or stronger access controls and an explicit deletion schedule.

`watch-engine` is a library, not a security boundary. Observer, Policy, Sink, process supervision,
secret management, network controls, and downstream authorization remain the adopter's
responsibility.
