# 0006 — Region override uses AWS_DEFAULT_REGION, not IONFLOW_REGION

Status: accepted (2026-07-26)

## Context

Every other config override follows the `IONFLOW_*` convention
(`IONFLOW_BUCKET`, `IONFLOW_PREFIX`, `IONFLOW_ENDPOINT`), but region is
overridden by `AWS_DEFAULT_REGION` (`config.py`). It reads like drift.

## Decision

Deliberate deference: region is an AWS-chain concern like credentials, so the
standard AWS variable wins. Anything already configured for the AWS CLI/SDK
(shell profiles, CI images) sets the right region for ionflow with no extra
variable, and there is never a ionflow-vs-AWS disagreement about which
region a boto3 client resolves.

## Consequences

- No `IONFLOW_REGION`; don't add one.
- The TOML `region` key remains the project-level setting; the AWS env var is
  the only override.
