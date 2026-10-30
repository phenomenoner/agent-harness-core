"""Model-directed discovery of authorized, generation-pinned skill revisions."""

import base64
import json

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest
from ahc.domain.workspace import WorkspaceIdentity


class WorkspaceSkillTools:
    """Relevance is advisory; identity, availability and disclosure limits are not."""

    def __init__(self, repository):
        self.repository = repository
        self._bound_request = None

    def bind_request(self, request, workspace):
        if workspace.identity.session != request.session:
            raise ContractError("skill request workspace owner mismatch")
        self._bound_request = (request.session, request.operation_id,
                               workspace.identity.workspace_id, workspace.identity.generation)

    def invoke(self, workspace, name, arguments, *, available_tools):
        from .workspace_skill_materialize import SkillTransferIncomplete, materialize_skill

        try:
            fields = (
                {"expected", "query", "offset", "limit"}
                if name == "workspace.skills_list"
                else {"expected", "revision_digest"} if name == "workspace.skills_materialize"
                else {"expected", "revision_digest", "path", "offset", "limit"}
            )
            if (
                type(arguments) is not dict
                or "expected" not in arguments
                or set(arguments) - fields
            ):
                raise ContractError("skill discovery fields invalid")
            expected = WorkspaceIdentity.from_dict(arguments["expected"])
            if workspace.closed or expected != workspace.identity:
                raise ContractError("skill discovery workspace mismatch")
            offset = arguments.get("offset", 0)
            listing = name in ("workspace.skills_list", "workspace.skills_resources")
            limit = arguments.get("limit", 16 if listing else 4096)
            maximum = 32 if listing else 4096
            if (
                type(offset) is not int
                or not 0 <= offset <= 16_777_216
                or type(limit) is not int
                or not 1 <= limit <= maximum
            ):
                raise ContractError("skill discovery bounds invalid")
            revisions = None
            if self._bound_request is not None:
                session, operation, workspace_id, generation = self._bound_request
                if (session != expected.session or workspace_id != expected.workspace_id
                        or generation != expected.generation):
                    raise ContractError("skill operation catalog workspace generation mismatch")
                from ahc.infrastructure.scoped_skill_trials import read_operation_catalog
                revisions = read_operation_catalog(self.repository, session, operation)
            if revisions is None:
                revisions, _ = self.repository.pin_revision_availability(expected.session)
            authorized = tuple(
                item
                for item in revisions
                if (not item.agent_allowlist or expected.session.agent_id in item.agent_allowlist)
                and set(item.required_tools).issubset(available_tools)
            )
            if name == "workspace.skills_list":
                result = self._list(authorized, arguments.get("query", ""), offset, limit)
            elif name in ("workspace.skills_read", "workspace.skills_resources",
                          "workspace.skills_materialize"):
                requested = arguments.get("revision_digest")
                digest(requested, "revision_digest")
                item = next(
                    (item for item in authorized if item.revision_digest == requested), None
                )
                if item is None:
                    raise ContractError("skill revision is not available to this owner")
                if name == "workspace.skills_materialize":
                    result = materialize_skill(workspace, self.repository, item)
                elif name == "workspace.skills_resources":
                    if "path" in arguments:
                        raise ContractError("resource listing does not take a path")
                    entries = []
                    for asset in item.assets[offset:offset + limit]:
                        row = asset.to_dict()
                        if len(json.dumps(entries + [row]).encode("utf-8")) > 20000:
                            break
                        entries.append(row)
                    end = offset + len(entries)
                    result = {"resources": entries, "revision_digest": requested,
                              "total": len(item.assets),
                              "next_offset": end if end < len(item.assets) else None}
                else:
                    path = arguments.get("path", "SKILL.md")
                    content, encoding = item.body, "utf-8"
                    if path != "SKILL.md":
                        if type(path) is not str:
                            raise ContractError("resource path invalid")
                        raw = self.repository.read_asset(requested, path)
                        try:
                            content = raw.decode("utf-8")
                        except UnicodeError:
                            content, encoding = raw, "base64"
                    end = min(offset + limit, len(content))
                    chunk = content[offset:end]
                    if encoding == "utf-8":
                        while len(json.dumps(chunk).encode("utf-8")) > 24000:
                            end = offset + (end - offset) // 2
                            chunk = content[offset:end]
                    else:
                        chunk = base64.b64encode(chunk).decode("ascii")
                    result = {"skill_name": item.skill_name, "revision_digest": requested,
                              "path": path, "encoding": encoding, "text": chunk,
                              "offset": offset, "total_chars": len(content),
                              "offset_unit": "chars" if encoding == "utf-8" else "bytes",
                              "next_offset": end if end < len(content) else None}
            else:
                raise ContractError("skill discovery operation invalid")
            return {"disposition": "observed", "unresolved_effects": False, "result": result}
        except SkillTransferIncomplete:
            return {"disposition": "unknown", "unresolved_effects": True,
                    "reason": "skill-materialization-unverified-partial-copy-retained"}
        except (ContractError, OSError, UnicodeError):
            return {
                "disposition": "rejected",
                "unresolved_effects": False,
                "reason": "skill-discovery-unavailable-or-invalid",
            }

    @staticmethod
    def _list(revisions, query, offset, limit):
        if (
            type(query) is not str
            or len(query) > 256
            or any(0xD800 <= ord(c) <= 0xDFFF for c in query)
        ):
            raise ContractError("skill discovery query invalid")
        terms = query.casefold().split()

        def rank(item):
            text = " ".join((item.skill_name, item.description, *item.triggers)).casefold()
            return (-sum(term in text for term in terms), item.skill_name, item.revision_digest)

        ordered = sorted(revisions, key=rank)
        entries = []
        for item in ordered[offset : offset + limit]:
            row = {
                "skill_name": item.skill_name,
                "revision_digest": item.revision_digest,
                "description": item.description[:256],
                "roles": list(item.roles),
                "required_tools": list(item.required_tools),
            }
            if len(json.dumps(entries + [row]).encode("utf-8")) > 20_000:
                break
            entries.append(row)
        end = offset + len(entries)
        if not entries and offset < len(ordered):
            raise ContractError("skill metadata exceeds disclosure bound")
        return {
            "skills": entries,
            "total": len(ordered),
            "next_offset": end if end < len(ordered) else None,
            "selection": "model-discretion",
            "query_is_filter": False,
        }
