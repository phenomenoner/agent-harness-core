"""Model scheduling follows the active parent's exact channel route."""

from dataclasses import replace

from test_channel_controls import make, reply
from test_workspace_tools import Port

from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_schedule import WorkspaceScheduleTools
from ahc.application.workspace_tools import WorkspaceTools


def test_active_model_can_create_list_and_retire_chat_schedule(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    event = submit("schedule this")
    request = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    workspace = RuntimeService(store, None).open_workspace(current, "work", Port)
    tools = WorkspaceTools(workspace, scheduling=WorkspaceScheduleTools(controls))
    args = {"expected": workspace.identity.to_dict(), "command": "add every 10m report"}
    try:
        assert "workspace.schedule" in tools.capabilities()
        assert tools.invoke("workspace.schedule", args)["disposition"] == "rejected"
        store.begin(store.read_current("session"), request)
        result = tools.invoke("workspace.schedule", args)
        assert result["disposition"] == "observed", result
        assert "將回覆至本聊天室" in result["result"]["reply"]
        listed = reply(store, submit("/cron list"))
        assert "report" in listed
        with store._read() as c:
            schedule_id = c.execute("SELECT schedule_id FROM runtime_schedules").fetchone()[0]
        retired = tools.invoke("workspace.schedule", dict(args, command="delete " + schedule_id))
        assert "已刪除" in retired["result"]["reply"]
        stale = dict(args, expected=replace(workspace.identity, revision=99).to_dict())
        assert tools.invoke("workspace.schedule", stale)["disposition"] == "rejected"
        controls.routes = ()
        assert tools.invoke("workspace.schedule", args)["disposition"] == "rejected"
    finally:
        workspace.close()


def test_capabilities_reads_current_operating_context_without_model(tmp_path):
    store, _, _, _, controls, _, submit = make(tmp_path)
    doc = controls.config.to_dict()
    doc["host_paths"] = [{"host_root": "D:/", "backend_root": "/mnt/d"}]
    doc["operating_context"] = {"memory_coverage": "qualified archive",
                                "skills_coverage": "4 active"}
    text = reply(store, submit("/capabilities"))
    assert "/mnt/d" in text and "qualified archive" in text and "4 active" in text
    assert "/cron" in text and "子任務" in text


def test_background_model_keeps_schedule_tools_bound_to_original_chat(tmp_path):
    from test_background_schedule_runtime import occurrence
    from ahc.infrastructure.background_schedule_dispatch import BackgroundScheduleDispatch

    store, current, template, route, controls, _, submit = make(tmp_path)
    occurrence(store, current, submit)
    binding = BackgroundScheduleDispatch(store).claim_one(current.session)
    session = binding["worker_session"]
    current = store.read_current(session.ref.virtual_session_id)
    workspace = RuntimeService(store, None).open_workspace(current, binding["workspace_id"], Port)
    request = replace(template, session=session, workspace_scope=binding["workspace_id"],
                      operation_id=binding["worker_operation_id"],
                      input_text=binding["source_payload"]["input_text"])
    tools = WorkspaceScheduleTools(controls)
    args = {"expected": workspace.identity.to_dict(), "command": "list"}
    try:
        store.begin(store.read_current(session.ref.virtual_session_id), request)
        result = tools.invoke(workspace, args)
        assert result["disposition"] == "observed", result
        assert "background fixture" in result["result"]["reply"]
        controls.routes = ()
        assert tools.invoke(workspace, args)["disposition"] == "rejected"
    finally:
        workspace.close()
