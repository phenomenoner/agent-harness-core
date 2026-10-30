"""Opt-in local task navigation; configuration parsing performs no writes."""

from ahc.application.canvas_turn_observer import CanvasTurnObserver
from ahc.domain.sessions import ContractError


def parse_canvas(raw):
    if "symbolic_canvas" not in raw:
        return {}
    value = raw["symbolic_canvas"]
    if (type(value) is not dict or set(value) != {"enabled", "min_turns", "trigger_mode"}
            or type(value["enabled"]) is not bool
            or type(value["min_turns"]) is not int or not 1 <= value["min_turns"] <= 16
            or value["trigger_mode"] not in ("qualified", "all")):
        raise ContractError("symbolic_canvas configuration invalid")
    return {"symbolic_canvas": dict(value)}


def compose_canvas(config, store):
    value = config.to_dict().get("symbolic_canvas")
    if value is None or not value["enabled"] or not config.backend["enabled"]:
        return None
    return CanvasTurnObserver(store, config.session, min_turns=value["min_turns"],
                              trigger_mode=value["trigger_mode"])
