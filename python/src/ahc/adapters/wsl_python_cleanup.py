"""Close owned clients and reconcile recorded units; always release the local lease."""


def cleanup_workspace(self):
    try:
        if self._transport is not None:
            self._transport.close()
    finally:
        self._transport = None
        try:
            close_runner = getattr(self._runner, "close", None)
            if callable(close_runner):
                close_runner()  # Closes a WslStreamRunner pending attachment, if any.
        finally:
            try:
                if self._lease is not None:
                    self._custody.recover(self._lease, self._runner)
            finally:
                if self._lease is not None:
                    self._lease.close()
                self._lease = None
                self._closed = True
