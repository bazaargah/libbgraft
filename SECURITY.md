# Security Policy

## Supported versions

libbgraft is pre-1.0. Security fixes land on the latest commit of the
default branch and are backported to the most recent tagged release
only when one exists.

## Reporting a vulnerability

Please report security issues **privately**, not as a public issue:

- Open a private vulnerability report against the repository (GitHub →
  Security → "Report a vulnerability"), or
- email the maintainers directly if private reporting is unavailable.

Include the commit SHA, a description of the affected component
(protocol parser, TLS transport, WAL, elections, snapshot transfer), and
— if possible — a reproduction. Consensus-layer findings (term
manipulation, log truncation from outside the cluster, frame-parser
memory safety) and any way to bypass TLS peer verification are treated
with highest priority.

Expect an acknowledgment within a few business days. Fixes ship in the
next release; coordinated disclosure is welcome — ask before publishing
details.
