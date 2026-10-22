"""Declared artifact reuse before executing a producer or requiring its payloads."""

from __future__ import annotations

import logging
from typing import Any, Literal

from botocore.exceptions import BotoCoreError, ClientError

from .artifact import Artifact, Artifacts, MissingPayload
from .storage import execute_transfer, plan_transfer, remote_config
from .store import Store

Reuse = Literal["auto", "local", "never"]
logger = logging.getLogger(__name__)


def lookup(
    schema: type[Artifact], values: dict[str, Any], registry: Artifacts, policy: Reuse
) -> tuple[Artifact, str, int] | None:
    """Look up one declared ID; only auto authorizes a verified output download."""
    if policy == "never":
        return None
    artifact_id = schema.identity(**values)
    try:
        ref = registry.get(artifact_id, schema)
    except KeyError:
        pass
    else:
        if registry.availability(artifact_id=artifact_id).get(artifact_id, {}).get("local") and registry.has_payload(
            ref
        ):
            return ref, "local", 0
    if policy == "local" or not registry.store.cfg.remote:
        return None
    try:
        remote = Store.of(remote_config(registry.store.cfg))
        ref = Artifacts(remote).get(artifact_id, schema)
        if not Artifacts(remote).availability(artifact_id=artifact_id).get(artifact_id, {}).get("local"):
            return None
        plan = plan_transfer(
            remote, registry.store, artifact_ids=[artifact_id], include_artifacts=[artifact_id], direction="down"
        )
    except (KeyError, MissingPayload):
        return None
    except (BotoCoreError, ClientError, OSError) as error:
        logger.warning("artifact identity lookup unavailable; computing %s locally: %s", artifact_id, error)
        return None
    result = execute_transfer(plan)
    return registry.get(artifact_id, schema, snapshot=ref.snapshot()), "s3", result["transferred_bytes"]
