"""Optional IPython cell loop, preserving the workspace wire protocol."""

from .workspace_kernel import KERNEL

_SETUP = r'''
from IPython.core.interactiveshell import InteractiveShell
from traitlets.config import Config
import atexit, tempfile
profile = tempfile.TemporaryDirectory(prefix="ahc-ipython-")
atexit.register(profile.cleanup)
config = Config()
config.HistoryManager.enabled = False
config.InteractiveShell.colors = "nocolor"
config.InteractiveShell.autoawait = False
shell = InteractiveShell.instance(user_ns=namespace, ipython_dir=profile.name, config=config)
def publish_display(data, metadata=None, **kwargs):
    row = {k: v for k, v in data.items()
           if k in ("text/plain", "text/html", "image/png", "image/jpeg")
           and type(v) is str}
    rows = result.setdefault("displays", [])
    if (kwargs.get("update") or kwargs.get("transient") or not row
        or len(rows) >= 16 or len(json.dumps(rows + [row])) > 16384
        or any(0xD800 <= ord(c) <= 0xDFFF for v in row.values() for c in v)):
        result["display_truncated"] = True
        return
    rows.append(row)
shell.display_pub.publish = publish_display
write_expression_text = shell.displayhook.write_format_data
def write_expression(data, metadata=None):
    write_expression_text(data, metadata)
    if set(data) - {"text/plain"}:
        publish_display(data, metadata)
shell.displayhook.write_format_data = write_expression
'''

IPYTHON_KERNEL = KERNEL.replace(
    'namespace = {"__name__": "__workspace__"}',
    'namespace = {"__name__": "__workspace__"}\n' + _SETUP,
    1,
).replace(
    'output = BoundedOutput()',
    'result["displays"] = []; result["display_truncated"] = False\n'
    '            output = BoundedOutput()',
    1,
).replace(
    'exec(compile(request["code"], "<workspace>", "exec"), namespace)',
    'shell.run_cell(request["code"], store_history=False).raise_error()',
    1,
)
