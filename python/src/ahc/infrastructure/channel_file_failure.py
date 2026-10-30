"""One durable import failure notice; retained files are not replayed on restart."""

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json

from .runtime_channel_store import _exists


def record_failure(store, event):
    controls = getattr(store, "channel_controls", None)
    if controls is None:
        return

    def write(c):
        if event.route not in controls.routes:
            raise ContractError("file failure route mismatch")
        if (_exists(c, "channel_held") and c.execute(
                "SELECT 1 FROM channel_held WHERE operation_id=?",
                (event.operation_id,)).fetchone()):
            return
        c.execute("CREATE TABLE IF NOT EXISTS runtime_channel_file_failures "
                  "(operation_id TEXT PRIMARY KEY)")
        if c.execute("SELECT 1 FROM runtime_channel_file_failures WHERE operation_id=?",
                     (event.operation_id,)).fetchone():
            return
        # A submission already accepted is not an import failure to be retried or replaced.
        if (_exists(c, "runtime_submissions") and c.execute(
                "SELECT 1 FROM runtime_submissions WHERE operation_id=?",
                (event.operation_id,)).fetchone()):
            return
        c.execute("INSERT INTO runtime_channel_file_failures VALUES(?)", (event.operation_id,))
        controls._record_control(
            c, event, digest_json(event.to_dict()), controls._preferences(c, event.route),
            "附件下載或匯入未完成，這則工作尚未執行。請重新傳送附件；原紀錄已保留，不會自動重播。",
            False,
        )

    store._mutate(write)
