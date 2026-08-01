# Security Policy

## Supported versions

Before the first release, security fixes are applied to `main`. After releases
begin, fixes are provided for the latest released version. This project is alpha
software and may introduce breaking changes while addressing security issues.

## Reporting a vulnerability

Use GitHub private vulnerability reporting when **Report a vulnerability** is
available on the repository's **Security** tab. If it is unavailable, open a
minimal public issue asking the maintainer for a private contact channel. Do not
include exploit details, credentials, tokens, or sensitive data in a public issue.

Include the affected version, a minimal reproduction, impact, and any suggested
remediation. Reports will be acknowledged as soon as practical. A fix and
coordinated disclosure timeline will be agreed after the issue is reproduced and
assessed.

## Deployment responsibility

The package provides protocol routes but does not choose an authentication system,
TLS termination, object-ownership policy, or per-client rate limiter. The standalone
factory provides bounded request bodies and active runs; route-only installations
must supply an equivalent request-size boundary. Review the README security section
before exposing the API to untrusted clients.
