# Security policy

## Scope

This policy covers the `tarssh-rs` source tree, container image, Compose file,
and observability assets published in this repository.

`tarssh-rs` is a deliberately exposed tarpit. It is not an SSH server. Do not
use it for authentication, administration, credential collection, payload
storage, or network scanning.

## Reporting a vulnerability

Do not open a public issue for a vulnerability. Use GitHub's private
**Report a vulnerability** link on the repository Security tab. Include:

- affected commit, release, or image digest;
- deployment and reproduction conditions;
- impact and, when possible, a minimal reproduction;
- suggested mitigation.

Do not include credentials, client payloads, or personal data in a report.

Reports receive an acknowledgement when reviewed. Disclosure timing is
coordinated with the reporter and depends on severity and available fixes.

## Deployment reports

For configuration mistakes or ordinary bugs, open a GitHub issue. Remove
secrets and client data before posting logs. The service intentionally keeps
application logs out of Docker and sends only bounded, best-effort events to
Alloy.

## Supported releases

Security fixes target the latest release and the default branch. Release tags
must point to reviewed default-branch commits. Deploy image digests, not
mutable tags, when reproducibility matters.
