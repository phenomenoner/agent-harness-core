"""Account/route-bound Gateway checkpoint and inbox transaction; never stores a bot token."""

from urllib.parse import urlsplit

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_discord_resume (
account_id TEXT PRIMARY KEY, routes_json TEXT NOT NULL, session_id TEXT NOT NULL,
resume_url TEXT NOT NULL, sequence INTEGER NOT NULL)"""


class DiscordResume:
    def __init__(self, inbox):
        self.inbox, self.store = inbox, inbox.store
        self._routes = canonical_json([r.to_dict() for r in sorted(
            inbox.routes, key=lambda r: (r.channel_id, r.user_id))]).decode()

    @staticmethod
    def _validate(session_id, url, sequence):
        if (type(session_id) is not str or not 1 <= len(session_id) <= 256
                or type(url) is not str or len(url) > 512
                or type(sequence) is not int or not 0 <= sequence < 2**63):
            raise ContractError("Discord resume state invalid")
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
            raise ContractError("Discord resume URL invalid")

    def _read(self, connection):
        if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                  "AND name='runtime_discord_resume'").fetchone():
            return None
        row = connection.execute("SELECT * FROM runtime_discord_resume WHERE account_id=?",
                                 (self.inbox.account_id,)).fetchone()
        if row is None:
            return None
        if row["routes_json"] != self._routes:
            raise ContractError("Discord resume route binding changed")
        self._validate(row["session_id"], row["resume_url"], row["sequence"])
        return {key: row[key] for key in ("session_id", "resume_url", "sequence")}

    def read(self):
        with self.store._read() as connection:
            return self._read(connection)

    def save_ready(self, session_id, resume_url, sequence, *, expected):
        self._validate(session_id, resume_url, sequence)

        def write(connection):
            connection.execute(_TABLE)
            if self._read(connection) != expected:
                raise ContractError("Discord resume checkpoint changed")
            for route in self.inbox.routes:
                current = self.store._load_current(connection, route.session.ref.virtual_session_id)
                if current.session != route.session or current.status != "active":
                    raise ContractError("Discord resume owner is not active")
            connection.execute("INSERT INTO runtime_discord_resume VALUES(?,?,?,?,?) "
                "ON CONFLICT(account_id) DO UPDATE SET session_id=excluded.session_id, "
                "resume_url=excluded.resume_url, sequence=excluded.sequence",
                (self.inbox.account_id, self._routes, session_id, resume_url, sequence))

        self.store._mutate(write)

    def admit(self, frame, *, expected):
        def write(connection):
            current = self._read(connection)
            sequence = frame.get("s") if type(frame) is dict else None
            if (current is None or current != expected or type(sequence) is not int
                    or not current["sequence"] < sequence < 2**63
                    or type(frame.get("op")) is not int or frame["op"] != 0):
                raise ContractError("Discord dispatch checkpoint invalid")
            for route in self.inbox.routes:
                owner = self.store._load_current(connection, route.session.ref.virtual_session_id)
                if owner.session != route.session or owner.status != "active":
                    raise ContractError("Discord dispatch owner is not active")
            result = self.inbox(frame, _transaction=connection)
            connection.execute("UPDATE runtime_discord_resume SET sequence=? WHERE account_id=?",
                               (sequence, self.inbox.account_id))
            return result

        return self.store._mutate(write)

    def clear(self, *, expected):
        def write(connection):
            if self._read(connection) != expected:
                raise ContractError("Discord resume checkpoint changed")
            if expected is not None:
                connection.execute("DELETE FROM runtime_discord_resume WHERE account_id=?",
                                   (self.inbox.account_id,))
        self.store._mutate(write)


class DiscordResumeSession:
    """Single-driver checkpoint view; CAS prevents silently borrowing a newer writer's state."""

    def __init__(self, repository):
        self.repository = repository
        self.current = repository.read()

    def checkpoint(self, frame):
        if frame.get("op") == 9 and frame.get("d") is False:
            self.repository.clear(expected=self.current)
            self.current = None
        elif frame.get("t") == "READY":
            data = frame["d"]
            self.repository.save_ready(data["session_id"], data["resume_gateway_url"], frame["s"],
                                       expected=self.current)
            self.current = {"session_id": data["session_id"],
                            "resume_url": data["resume_gateway_url"], "sequence": frame["s"]}
        else:
            self.admit(frame)

    def admit(self, frame):
        result = self.repository.admit(frame, expected=self.current)
        self.current = {**self.current, "sequence": frame["s"]}
        return result
