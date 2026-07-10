# 0006 — Region override uses AWS_DEFAULT_REGION, not MICROFLOW_REGION

Status: accepted (2026-07-10)

## Context

Every other config override follows the `MICROFLOW_*` convention
(`MICROFLOW_BUCKET`, `MICROFLOW_PREFIX`, `MICROFLOW_ENDPOINT`), but region is
overridden by `AWS_DEFAULT_REGION` (`config.py`). It reads like drift.

## Decision

Deliberate deference: region is an AWS-chain concern like credentials, so the
standard AWS variable wins. Anything already configured for the AWS CLI/SDK
(shell profiles, CI images) sets the right region for microflow with no extra
variable, and there is never a microflow-vs-AWS disagreement about which
region a boto3 client resolves.

## Consequences

- No `MICROFLOW_REGION`; don't add one.
- The TOML `region` key remains the project-level setting; the AWS env var is
  the only override.
