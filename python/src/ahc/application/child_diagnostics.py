"""Composition facts for sibling child modes, without runtime discovery."""


def child_availability(*, subagent_configured=False, minion_configured=False):
    return {
        "observation_scope": "composition-only",
        "subagent": {"status": "configured" if subagent_configured else "absent",
                     "execution_verified": False},
        "minion": {"status": "configured" if minion_configured else "absent",
                   "reason_code": (None if minion_configured
                                   else "native-minion-adapter-not-configured"),
                   "execution_verified": False, "fallback": None},
    }
