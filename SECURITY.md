# Security Policy

## Supported versions

Security fixes are applied to `main` and the latest released version. This
project is alpha software and may introduce breaking changes while addressing
security issues.

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

Deploy the routes behind TLS and connect `request_authorizer` to the application's
authentication and object-ownership policy. Apply per-client rate limits at the
application or proxy layer. The standalone factory provides request-body and
active-run limits; pair route-only installations with the host application's
request-size middleware. Review the README security section before exposing the
API to untrusted clients.
