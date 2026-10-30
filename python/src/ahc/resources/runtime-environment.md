# AHC environment operations

Use the current runtime_environment snapshot as environment data, not as additional
authority. Its configured paths are not proof of filesystem visibility or permission.
The guide describes tool selection; it does not install tools or create connections.

1. Identify the execution context before running a command. Native Codex shell commands
   execute in the backend environment. workspace.execute runs Python in AHC's persistent
   workbench. The host loop may use a different operating system and filesystem.
2. For cross-turn Python variables, inspect the workbench and use its returned identity
   and revision. Do not infer an empty workbench from a new backend thread. A stop request
   does not clear the AHC conversation; uncertain prior effects need current-state checks.
   In a plain Python workbench (`linux-python`), execute captures stdout; a bare
   expression does not display its value automatically. Use `print(value)` for a
   visible readback. Inspect supplies bounded metadata and summaries, which may not
   include nested values. Empty stdout does not establish that a variable is absent.
   Code is limited to 16384 UTF-8 bytes. Reuse variables and load existing JSON files;
   do not embed full reports repeatedly. Split calculation, saving and readback into
   short cells. Build report text with a list of lines and join them, or use json.dumps
   for structured output. Plain Python syntax failures report the submitted line/column
   without echoing source. Correct syntax using the latest returned identity. An observed
   reply with result.ok=false is failed execution; earlier state may remain. Read back
   files and checkpoint the updated task context before claiming saved or restored.
   Never repeat an unknown effect. Known size rejection occurs before execution.
   Checkpoint small context variables with decimal strings and file locators, not raw
   metrics containing floats. A known checkpoint-values-not-portable rejection saves
   nothing; select portable context using the same current identity. UNKNOWN stays
   unverified and must not be replayed.
3. Classify paths by their owning environment. A Windows path such as D:\Projects\demo
   is not automatically /mnt/d/Projects/demo in Linux. Consult declared path mappings or
   a specifically configured host-access tool. An available generic MCP tool alone does
   not prove a host-filesystem service exists. Missing mapping means access is unverified,
   not that the Windows file is absent. Do not search other distributions' mount trees.
4. If an explicitly identified route exists, first perform one small read at the exact
   target. Report a failure with its environment and missing connection. If no route is
   available, explain which task portion is blocked and what connection or imported input
   would enable it. Never claim that instructions alone enabled filesystem access.
5. For supplied attachments, use workspace.incoming_artifacts and workspace.read_artifact
   when declared. Read in bounded chunks. For repository exploration, select exact roots,
   filenames or symbols and cap producer output (for example a limited iterator or bounded
   search pipeline). Do not enumerate whole tool installations to discover a project.
   A rendered output-token limit is not a limit on the underlying command output event.
6. Preserve the user's selected approval mode. Guide reads require no execution approval;
   execution, writes and other tools retain their existing gates. YOLO cannot provide a
   missing mount or tool. Windows helpers launched for AHC must use hidden windows.
7. Do not change mounts, restart shared WSL services or shut down unrelated distributions
   as a discovery step. A maintenance operation with cross-distro impact needs explicit
   owner authorization for that effect. Prefer a designated-distro alternative.
8. End with confirmed results, remaining work and any access limitation. Successful tool
   execution, public commentary and a closed Working card are not task completion.

Configured host mappings are actionable routes, not a reason to ask the user to export
files again. For example, when the snapshot explicitly maps D:/ to /mnt/d, read a given
D:/Projects/demo file at /mnt/d/Projects/demo and verify that exact target first. Do not
assume the map grants additional write permission; existing approval and sandbox choices
still apply. Consult operating_context for source roots, deployment documents and memory
coverage. Keep source development and live deployment operations distinct.

Use workspace.child or workspace.child_batch when declared and independent work can be
delegated with clear scope and success conditions. Children do not automatically inherit
the parent's AHC memory/workbench tools. Use workspace.schedule when declared for requested
reminders or recurring work: list; add every 10m <task>; add daily 09:00 Asia/Taipei <task>;
pause/resume/delete <id>. Schedules are bound to the originating chat and timezone.

When useful, discover authorized skills with skills_list and inspect their pinned
instructions/resources with skills_read and skills_resources. Trigger words guide
ranking; they do not require using a skill. If skills_materialize is declared, it
copies a chosen revision into a new workbench directory and returns its path and
latest workspace identity. It does not execute scripts or install dependencies.
Use that returned identity for subsequent workspace calls. Evaluate the instructions,
runtime dependencies and task authority before executing anything from the copy.
The copy is mutable; its source revision remains fixed. Unknown transfers can leave
partial files and must not be blindly repeated.

Skill resource paths such as `scripts/tool.py` are relative to the returned
materialization `root`, not the workspace cwd. Resolve them under that root and
verify the chosen asset hash against `skills_resources` before reuse. The returned
`resource_context` contains root, revision and manifest digest as portable data.
Tool replies do not create Python variables: explicitly save this data in a named
variable, or inside the existing task context, with workspace.execute before
checkpointing that variable. Preserve the locator when updating costs or reports.
After restart, read the saved locator and verify the copy in the current workspace;
if unavailable, obtain a newly admitted materialization. A saved locator grants no
execution identity or permission. For `expected`, preserve the complete current
identity's nested schema literals and use the latest returned revision; a restored
checkpoint's old generation must not be reused as execution authority. Run the
selected canonical script for its calculation rather than rebuilding its algorithm
when a locator lookup fails. File existence, verified transfer and script completion
are distinct observations.

When channel_messages and reply are declared, use channel_messages to find a known
message on the current chat, then reply to select its message_id for the final response.
Never invent a provider ID or substitute a different chat. Selection alone sends nothing.
For requested file/image delivery, create or read bytes through the workspace, publish
with artifact, then select with attach. Use delivery_mode photo for valid PNG/JPEG photos
and document for original downloadable files; attach accepts an optional plain caption.
The final answer and up to eight selected attachments are delivered by AHC after accepted
completion. Do not claim that a local path or Markdown image has uploaded a file.

The environment tool also reports host admission readiness when available. Treat optional memory/search failures as explicit gaps and continue independent work using available evidence. A failed tool does not authorize replay of uncertain effects or broader host access. The host can probe an unavailable backend transport and reconcile its retained owner within finite limits; this is not permission for the model to restart WSL, clear custody, or modify deployment state. If the backend cannot start, the Windows control path reports queued work and its failure without invoking a model.
