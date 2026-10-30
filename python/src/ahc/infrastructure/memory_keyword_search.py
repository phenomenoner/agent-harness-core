"""Rebuildable scope-checked lexical projection, independent of vector freshness."""

import json
import re


def prepare(db):
    db.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS memory_keywords USING fts5(summary, tokenize='trigram')"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS memory_keyword_progress(scope TEXT PRIMARY KEY, "
        "cursor INTEGER NOT NULL, complete INTEGER NOT NULL)"
    )
    db.execute(
        "CREATE TRIGGER IF NOT EXISTS memory_keywords_insert AFTER INSERT ON records BEGIN\n"
        "        INSERT INTO memory_keywords(rowid,summary) VALUES("
        "new.id,json_extract(new.observation,'$.summary'));\n"
        "        END")


def build(db, scope, limit):
    prepare(db)
    prior = db.execute(
        "SELECT cursor FROM memory_keyword_progress WHERE scope=?", (scope,)
    ).fetchone()
    cursor = prior[0] if prior else 0
    rows = db.execute(
        "SELECT id,observation FROM records WHERE scope=? AND id>? ORDER BY id LIMIT ?",
        (scope, cursor, limit),
    ).fetchall()
    for row in rows:
        db.execute(
            "INSERT OR REPLACE INTO memory_keywords(rowid,summary) VALUES(?,?)",
            (row[0], json.loads(row[1])["summary"]),
        )
    if rows:
        cursor = rows[-1][0]
    db.execute(
        "INSERT INTO memory_keyword_progress VALUES(?,?,?) ON CONFLICT(scope) DO UPDATE "
        "SET cursor=excluded.cursor,complete=excluded.complete",
        (scope, cursor, int(not rows)),
    )
    return len(rows)


def ready(db, scope):
    if not db.execute(
        "SELECT 1 FROM sqlite_master WHERE name='memory_keyword_progress'"
    ).fetchone():
        return False
    row = db.execute(
        "SELECT complete FROM memory_keyword_progress WHERE scope=?", (scope,)
    ).fetchone()
    return bool(row and row[0] == 1)


def _query_terms(query):
    # The trigram tokenizer needs fragments of unsegmented CJK questions.
    # Split mixed-script words too, e.g. "Python安裝步驟".
    stop = set("the and for from with which what when where who why how does did "
               "are was were have has had this that these those into about".split())
    terms, unfiltered = [], []
    for word in re.findall(r"[^\W_]+", query, flags=re.UNICODE):
        for token in re.findall(r"[\u3400-\u9fff]+|[^\u3400-\u9fff]+", word):
            acronym = token.isupper()
            literal = token.lower()
            token = literal
            if not acronym and token.isascii() and token.isalpha() and token not in stop:
                # FTS trigram phrases otherwise miss ordinary inflections.
                if token.endswith("ies") and len(token) > 5:
                    token = token[:-3] + "y"
                elif token.endswith("s") and not token.endswith("ss") and len(token) > 4:
                    token = token[:-1]
                elif token.endswith("ing") and len(token) > 6:
                    token = token[:-3]
                    if token[-1:] == token[-2:-1]:
                        token = token[:-1]
            if re.fullmatch(r"[\u3400-\u9fff]+", token):
                pieces = (token[i:i + 3] for i in range(len(token) - 2))
            else:
                pieces = (variant[i:i + 32] for variant in dict.fromkeys((literal, token))
                          for i in range(0, len(variant), 32))
            for term in pieces:
                if len(term) < 3:
                    continue
                if term not in unfiltered and len(unfiltered) < 48:
                    unfiltered.append(term)
                if token in stop and not acronym:
                    continue
                if term not in terms:
                    terms.append(term)
                if len(terms) == 48:
                    return terms
    # Function words can also be identifiers or proper names. They must still
    # work as standalone queries, especially when vectors are unavailable.
    return terms or unfiltered


def search(db, query, scope, limit):
    # Rank locators first. Keeping every equal-boundary score preserves the
    # original global newest-id tie rule without sorting original payloads.
    import heapq
    terms = _query_terms(query)
    if not terms:
        return []
    match = " OR ".join('"' + term.replace('"', '""') + '"' for term in terms)
    cursor = db.execute(
        "SELECT r.id,f.rank AS lexical_score "
        "FROM memory_keywords f JOIN records r ON r.id=f.rowid "
        "WHERE memory_keywords MATCH ? AND f.rank MATCH 'bm25()' "
        "AND r.scope=? AND r.retired=0 ORDER BY f.rank",
        (match, scope),
    )
    best = []
    try:
        for row in cursor:
            identifier, score = row[0], row[1]
            if len(best) == limit and score > -best[0][0]:
                break
            item = (-score, identifier)
            if len(best) < limit:
                heapq.heappush(best, item)
            elif item > best[0]:
                heapq.heapreplace(best, item)
    finally:
        cursor.close()
    identifiers = [identifier for score,identifier in
                   sorted(best, key=lambda item: (-item[0], -item[1]))]
    if not identifiers:
        return []
    # A concurrent retirement is checked again before canonical hydration.
    placeholders = ",".join("?" for _ in identifiers)
    rows = db.execute(
        "SELECT * FROM records WHERE scope=? AND retired=0 "
        "AND id IN (" + placeholders + ")",
        (scope, *identifiers),
    ).fetchall()
    by_id = {row["id"]: row for row in rows}
    return [by_id[identifier] for identifier in identifiers if identifier in by_id]
