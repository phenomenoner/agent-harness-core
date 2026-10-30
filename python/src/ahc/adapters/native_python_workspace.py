"""Native process admission over the shared persistent Python workspace protocol."""

from ahc.domain.sessions import ContractError
from ahc.domain.execution_budgets import NATIVE_LIFETIME_MAX_SECONDS
from .native_process_runner import NativeProcessRunner
from .wsl_python_workspace import WslPythonWorkspace


class NativePythonWorkspace(WslPythonWorkspace):
    MAX_LIFETIME_SECONDS = NATIVE_LIFETIME_MAX_SECONDS

    @staticmethod
    def _validate_runner(runner, user):
        if type(runner) is not NativeProcessRunner or user is not None:
            raise ContractError("native workspace requires an explicit native owner")
