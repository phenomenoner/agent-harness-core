"""Owner-selected AHC child admission limits; no vendor usage guarantees."""

from ahc.domain.sessions import ContractError


def validate_limits(value):
    if (type(value) is not dict or set(value) not in (
            {"max_children", "max_concurrency"},
            {"max_children", "max_concurrency", "max_artifacts", "artifact_bytes"})
            or type(value["max_children"]) is not int
            or value["max_children"] < 0
            or type(value["max_concurrency"]) is not int
            or value["max_concurrency"] < 1):
        raise ContractError("child limits require nonnegative max_children and positive max_concurrency")
    if "max_artifacts" in value:
        count, size = value["max_artifacts"], value["artifact_bytes"]
        if (type(count) is not int or count < 0
                or type(size) is not int or size < 0
                or (count == 0) != (size == 0)):
            raise ContractError("child artifact count/bytes require matched bounded allocation")
    return dict(value)


def parse_child_limits(raw):
    return ({"child_limits": validate_limits(raw["child_limits"])}
            if "child_limits" in raw else {})


def owner_limits(owner):
    config = getattr(owner, "config", None)
    raw = {} if config is None else config.to_dict()
    return validate_limits(raw.get("child_limits", {"max_children": 16, "max_concurrency": 3}))
