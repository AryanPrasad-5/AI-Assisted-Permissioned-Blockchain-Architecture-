                                                                    

                                                                           
                                                                         
                                                                 
   

from __future__ import annotations

import copy
from typing import Any


class TransitionValidationError(ValueError):
                                                                           

    def __init__(self, category: str, message: str):
        self.category = category
        super().__init__(message)


def _config(block: dict) -> dict:
    try:
        return block["data"]["data"][0]["payload"]["data"]["config"]
    except (KeyError, IndexError, TypeError) as exc:
        raise TransitionValidationError("incomplete_evidence", f"configuration block is incomplete: {exc}") from exc


def _groups(config: dict) -> tuple[dict, dict, dict]:
    try:
        channel_group = config["channel_group"]
        orderer = channel_group["groups"]["Orderer"]
        values = orderer["values"]
        return channel_group, orderer, values
    except (KeyError, TypeError) as exc:
        raise TransitionValidationError("incomplete_evidence", f"configuration lacks the expected Orderer groups: {exc}") from exc


def _normalise_expected_change(before: dict, after: dict) -> tuple[dict, dict]:
                                                                             
    before_norm = copy.deepcopy(before)
    after_norm = copy.deepcopy(after)
    _, _, before_values = _groups(before_norm)
    _, _, after_values = _groups(after_norm)

    if "sequence" in before_norm or "sequence" in after_norm:
        after_norm["sequence"] = before_norm.get("sequence")
    after_values["BatchSize"]["value"]["max_message_count"] = before_values["BatchSize"]["value"]["max_message_count"]
    after_values["BatchSize"]["version"] = before_values["BatchSize"]["version"]
    return before_norm, after_norm


def validate_batch_size_transition(
    before_block: dict,
    after_block: dict,
    target_max_message_count: int,
    *,
    expected_sequence_increment: int = 1,
    expected_batch_size_increment: int = 1,
) -> dict[str, Any]:
                                                           
    before = _config(before_block)
    after = _config(after_block)
    before_group, before_orderer, before_values = _groups(before)
    after_group, after_orderer, after_values = _groups(after)

    try:
        checks: dict[str, Any] = {
            "configuration_sequence_increment": int(after.get("sequence", 0)) == int(before.get("sequence", 0)) + expected_sequence_increment,
            "channel_group_version_unchanged": after_group.get("version") == before_group.get("version"),
            "orderer_group_version_unchanged": after_orderer.get("version") == before_orderer.get("version"),
            "batch_size_version_expected_increment": int(after_values["BatchSize"]["version"]) == int(before_values["BatchSize"]["version"]) + expected_batch_size_increment,
            "batch_timeout_version_unchanged": after_values["BatchTimeout"].get("version") == before_values["BatchTimeout"].get("version"),
            "target_max_message_count": int(after_values["BatchSize"]["value"]["max_message_count"]) == int(target_max_message_count),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise TransitionValidationError("incomplete_evidence", f"version or target evidence is incomplete: {exc}") from exc

    if not checks["configuration_sequence_increment"]:
        raise TransitionValidationError("unexpected_configuration_sequence", "configuration sequence did not increment exactly once")
    if not checks["channel_group_version_unchanged"]:
        raise TransitionValidationError("unexpected_channel_group_version", "channel_group version changed unexpectedly")
    if not checks["orderer_group_version_unchanged"]:
        raise TransitionValidationError("unexpected_orderer_group_version", "Orderer group version changed unexpectedly")
    if not checks["batch_size_version_expected_increment"]:
        raise TransitionValidationError("unexpected_batch_size_version", "BatchSize version is not the expected increment")
    if not checks["batch_timeout_version_unchanged"]:
        raise TransitionValidationError("unexpected_unrelated_version", "BatchTimeout version changed unexpectedly")
    if not checks["target_max_message_count"]:
        raise TransitionValidationError("wrong_target_value", "committed MaxMessageCount does not equal the requested value")

    before_norm, after_norm = _normalise_expected_change(before, after)
    before_norm_group, before_norm_orderer, _ = _groups(before_norm)
    after_norm_group, after_norm_orderer, _ = _groups(after_norm)
    if before_norm_orderer.get("values") != after_norm_orderer.get("values"):
        raise TransitionValidationError("unexpected_unrelated_value", "an unrelated configuration value changed")
    if before_norm_orderer.get("policies") != after_norm_orderer.get("policies"):
        raise TransitionValidationError("unexpected_unrelated_policy", "an unrelated Orderer policy changed")
    if before_norm_orderer.get("groups") != after_norm_orderer.get("groups"):
        raise TransitionValidationError("unexpected_unrelated_group", "an unrelated Orderer organization/group changed")
    if before_norm_group != after_norm_group or before_norm != after_norm:
        raise TransitionValidationError("unexpected_unrelated_configuration", "an unrelated channel configuration field changed")

    checks.update({
        "unrelated_values_unchanged": True,
        "unrelated_policies_unchanged": True,
        "unrelated_groups_unchanged": True,
        "unrelated_configuration_unchanged": True,
    })
    return checks
