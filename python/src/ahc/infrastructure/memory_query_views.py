"""Bounded additional topic view for questions with a named conversational actor."""
import re

_ACTOR = re.compile(r"\b(?i:does|did|should|must|can|will|would)\s+[A-Z][A-Za-z0-9_-]{1,31}\s+")
_LEADING = re.compile(r"^(?i:how|what|which|why|when|where)\s+")


def query_views(query):
    """Keep exact entity-bearing input; optionally add its actor-free topic.

    A personal name can dominate sentence similarity and trigram relevance.
    The second view only broadens candidates. Original scope/digest hydration
    and original query evidence remain mandatory and unchanged.
    """
    reduced, count = _ACTOR.subn("", query.strip(), count=1)
    if not count:
        return [query]
    reduced = _LEADING.sub("", reduced).strip()
    if len(re.findall(r"[A-Za-z]{3,}", reduced)) < 3:
        return [query]
    return [query, reduced] if reduced != query else [query]
