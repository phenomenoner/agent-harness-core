"""Compatibility exports for strict intent, goal, and direction-signal values."""

from .judgment_goal import GOAL_SCHEMA, GoalContract
from .judgment_intent import INTENT_SCHEMA, PROVENANCES, IntentFrame, Premise
from .judgment_signal import SIGNAL_KINDS, SIGNAL_SCHEMA, DirectionSignal

__all__ = [
    "DirectionSignal",
    "GOAL_SCHEMA",
    "GoalContract",
    "INTENT_SCHEMA",
    "IntentFrame",
    "PROVENANCES",
    "Premise",
    "SIGNAL_KINDS",
    "SIGNAL_SCHEMA",
]
