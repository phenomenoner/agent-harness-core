"""Optional bounded JSON WebSocket transport for Discord Gateway v10."""

import json
import logging
import math
from urllib.parse import urlsplit

from ahc.adapters.channel_text import _invalid_constant, _unique_object
from ahc.domain.sessions import ContractError


class DiscordGatewayClosed(ContractError):
    def __init__(self, code):
        if type(code) is not int or not 1000 <= code <= 4999:
            raise ContractError("Discord close code invalid")
        self.code = code
        super().__init__("Discord Gateway peer closed")


def _peer_close_code(error):
    try:
        from websockets.exceptions import ConnectionClosed
    except ImportError:
        return None
    if isinstance(error, ConnectionClosed) and error.rcvd is not None:
        code = error.rcvd.code
        if type(code) is int and 1000 <= code <= 4999:
            return code
    return None


def _connect(url):
    try:
        from websockets.sync.client import connect
    except ImportError:
        raise ContractError("Discord WebSocket dependency unavailable") from None
    logger = logging.Logger("ahc.discord.private-transport", level=logging.CRITICAL + 1)
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    return connect(url, proxy=None, compression=None, open_timeout=10, close_timeout=2,
                   ping_interval=None, max_size=1048576, max_queue=16, logger=logger)


class DiscordWebSocket:
    def __init__(self, url="wss://gateway.discord.gg", *, connect=_connect):
        if type(url) is not str or len(url) > 512 or not callable(connect):
            raise ContractError("Discord Gateway URL invalid")
        try:
            parsed = urlsplit(url)
            valid = (parsed.scheme == "wss" and parsed.hostname
                     and (parsed.hostname == "gateway.discord.gg"
                          or parsed.hostname.endswith(".discord.gg"))
                     and not parsed.username and not parsed.password and not parsed.port
                     and parsed.path in ("", "/") and not parsed.query and not parsed.fragment)
        except ValueError:
            valid = False
        if not valid:
            raise ContractError("Discord Gateway URL invalid")
        try:
            self._connection = connect(url.rstrip("/") + "/?v=10&encoding=json")
        except Exception:
            raise ContractError("Discord Gateway connection unavailable") from None

    def send(self, frame):
        if self._connection is None:
            raise ContractError("Discord Gateway connection closed")
        try:
            if type(frame) is not dict:
                raise ValueError()
            text = json.dumps(frame, allow_nan=False, ensure_ascii=True, separators=(",", ":"))
            if len(text.encode()) > 4096:
                raise ValueError()
        except (ValueError, TypeError, RecursionError):
            raise ContractError("Discord Gateway outgoing frame invalid") from None
        try:
            self._connection.send(text)
        except Exception as error:
            code = _peer_close_code(error)
            self.disconnect()
            if code is not None:
                raise DiscordGatewayClosed(code) from None
            raise ContractError("Discord Gateway send unverified") from None

    def receive(self, timeout=0):
        if (type(timeout) not in (int, float) or not math.isfinite(timeout)
                or not 0 <= timeout <= 10 or self._connection is None):
            raise ContractError("Discord Gateway receive state invalid")
        try:
            text = self._connection.recv(timeout=timeout)
        except TimeoutError:
            return None
        except Exception as error:
            code = _peer_close_code(error)
            self.disconnect()
            if code is not None:
                raise DiscordGatewayClosed(code) from None
            raise ContractError("Discord Gateway receive unavailable") from None
        try:
            if type(text) is not str or len(text.encode()) > 1048576:
                raise ValueError()
            result = json.loads(text, object_pairs_hook=_unique_object,
                                parse_constant=_invalid_constant)
            if type(result) is not dict:
                raise ValueError()
            return result
        except (ValueError, TypeError, RecursionError, UnicodeError):
            self.disconnect()
            raise ContractError("Discord Gateway incoming frame invalid") from None

    def close(self):
        self._close(resumable=False)

    def disconnect(self):
        """Leave the session eligible for Resume; normal stop still uses close()."""
        self._close(resumable=True)

    def _close(self, *, resumable):
        if self._connection is not None:
            try:
                if resumable:
                    # Discord invalidates sessions on 1000/1001. An application
                    # close permits Resume; it does not guarantee server retention.
                    self._connection.close(code=4000)
                else:
                    self._connection.close()
            except Exception:
                raise ContractError("Discord Gateway close unverified") from None
            self._connection = None
