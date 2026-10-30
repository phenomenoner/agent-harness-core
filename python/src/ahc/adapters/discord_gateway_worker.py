"""Single socket-owner thread; foreground retains explicit start and verified close."""

import threading

from ahc.domain.sessions import ContractError


class DiscordGatewayWorker:
    def __init__(self, driver, *, setup=None):
        if not all(callable(getattr(driver, name, None)) for name in ("step", "status", "close")):
            raise ContractError("Discord worker driver invalid")
        self._driver = driver
        self._setup = setup
        self._menu = None
        self._stop, self._lock = threading.Event(), threading.Lock()
        self._thread = None
        self._menu_thread = None
        self._status = {"state": "new"}
        self._close_verified = False

    def status(self):
        with self._lock:
            return {**self._status, **({"command_menu": self._menu} if self._menu else {})}

    def _publish(self, value):
        with self._lock:
            self._status = dict(value)

    def start(self):
        if self._thread is not None or self._stop.is_set():
            raise ContractError("Discord worker already started or closed")
        if self._setup is not None:
            self._menu_thread = threading.Thread(target=self._configure_menu,
                                                 name="ahc-discord-menu", daemon=True)
            self._menu_thread.start()
        self._thread = threading.Thread(target=self._run, name="ahc-discord-gateway", daemon=True)
        self._thread.start()

    def _configure_menu(self):
        try:
            configured = self._setup(cancelled=self._stop.is_set, wait=self._stop.wait)
        except Exception:
            configured = False
        with self._lock:
            self._menu = "configured" if configured else "unverified"

    def _run(self):
        failed = False
        try:
            while not self._stop.is_set():
                self._driver.step(cancelled=self._stop.is_set)
                status = self._driver.status()
                self._publish(status)
                if status["state"] in {"closed", "paused"}:
                    break
                if self._stop.wait(0.05):
                    break
        except Exception:
            failed = True
            self._publish({"state": "failed"})
        finally:
            try:
                self._driver.close()
                self._close_verified = True
                if not failed and self._stop.is_set():
                    self._publish({**self.status(), "state": "closed"})
            except Exception:
                self._publish({"state": "close-unverified"})

    def close(self):
        self._stop.set()
        if self._thread is None:
            self._driver.close()
            self._close_verified = True
            self._publish({"state": "closed"})
            return
        self._thread.join(timeout=15)
        if self._menu_thread:
            self._menu_thread.join(timeout=5)
            if self._menu_thread.is_alive():
                raise ContractError("Discord menu shutdown unverified")
        if self._thread.is_alive() or not self._close_verified:
            raise ContractError("Discord worker shutdown unverified")
