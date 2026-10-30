"""Bounded local JSON/Mermaid task navigation, independent of memory providers."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
import sys

MAX_INPUT = 1024 * 1024


def text(value, bound, name):
    if (type(value) is not str or not value.strip() or len(value) > bound
            or any(0xD800 <= ord(c) <= 0xDFFF or ord(c) == 0 for c in value)):
        raise ValueError(name + ' must be bounded nonempty text')
    return value


def unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate JSON key')
        value[key] = item
    return value


def nonfinite(_):
    raise ValueError('non-finite JSON value')


def quoted(value):
    # Mermaid supports decimal entities. Untrusted text cannot break the quote
    # or inject new statements, HTML markup, markdown strings or click commands.
    value = ' '.join(value.split())
    return '"' + ''.join(c if c.isalnum() or c in ' _-.,:/' else f'#{ord(c)};'
                         for c in value) + '"'


def resolve_path(path, *, allow_missing=False):
    try:
        try:
            return path.resolve(strict=True)
        except FileNotFoundError:
            if not allow_missing:
                raise
            return path.resolve(strict=False)
    except RuntimeError as error:
        # Python 3.12 reports symlink loops as RuntimeError, unlike newer
        # pathlib versions. Normalize this path failure, not arbitrary bugs.
        raise ValueError('path resolution failed: ' + str(error)) from error


def reference(value, base):
    value = text(value, 240, 'reference')
    path = PurePosixPath(value)
    if ('\\' in value or ':' in value or path.is_absolute()
            or PureWindowsPath(value).is_absolute() or '..' in path.parts):
        raise ValueError('reference must be a contained relative path')
    resolved = resolve_path(base / value, allow_missing=True)
    if not resolved.is_relative_to(base):
        raise ValueError('reference escapes base directory')
    return {'path': value, 'exists': resolved.is_file()}


def build(raw, base):
    if len(raw) > MAX_INPUT:
        raise ValueError('input exceeds 1 MiB')
    source = json.loads(raw.decode('utf-8-sig'), object_pairs_hook=unique,
                        parse_constant=nonfinite)
    if type(source) is not dict:
        raise ValueError('input must be an object')
    task = text(source.get('task_id'), 128, 'task_id')
    choices = [key for key in ('nodes', 'steps', 'events') if key in source]
    if len(choices) != 1:
        raise ValueError('supply exactly one nodes/steps/events array')
    rows, edges = source[choices[0]], source.get('edges', [])
    if type(rows) is not list or not 1 <= len(rows) <= 256:
        raise ValueError('nodes must contain 1..256 entries')
    if type(edges) is not list or len(edges) > 1024:
        raise ValueError('edges exceed bound')
    nodes, mapping, generated, warnings = [], {}, set(), []
    for row in rows:
        if type(row) is not dict:
            raise ValueError('node must be an object')
        source_id = text(row.get('id'), 128, 'node id')
        node_id = 'n_' + hashlib.sha256(source_id.encode()).hexdigest()[:24]
        if source_id in mapping or node_id in generated:
            raise ValueError('duplicate source or generated node ID')
        mapping[source_id] = node_id
        generated.add(node_id)
        label = text(row.get('label', source_id), 512, 'label')
        state = text(row.get('state', 'unknown'), 64, 'state')
        refs = row.get('refs', [])
        if type(refs) is not list:
            raise ValueError('refs must be an array')
        refs = refs + ([row['result_ref']] if 'result_ref' in row else [])
        if len(refs) > 32:
            raise ValueError('too many node references')
        resolved = [reference(r, base) for r in refs]
        for ref in resolved:
            if not ref['exists']:
                warnings.append({'kind': 'missing_ref', 'node_id': node_id, 'path': ref['path']})
        nodes.append({'source_id': source_id, 'node_id': node_id, 'label': label,
                      'state': state, 'refs': resolved})
    joined = []
    for edge in edges:
        if type(edge) is not list or len(edge) not in (2, 3):
            raise ValueError('edge must be [from,to,label?]')
        start, end = (text(value, 128, 'edge endpoint') for value in edge[:2])
        label = text(edge[2], 128, 'edge label') if len(edge) == 3 else None
        if start not in mapping or end not in mapping:
            warnings.append({'kind': 'unknown_edge_endpoint', 'from': start, 'to': end})
            continue
        joined.append({'from': mapping[start], 'to': mapping[end], 'label': label})
    lines = ['flowchart TD']
    for node in nodes:
        lines.append(f"  {node['node_id']}[{quoted(node['label'] + ' (' + node['state'] + ')')}]")
    for edge in joined:
        arrow = '-->' if edge['label'] is None else '-->|' + quoted(edge['label']) + '|'
        lines.append(f"  {edge['from']} {arrow} {edge['to']}")
    return {'schema': 'ahc.symbolic-canvas.v1', 'task_id': task, 'nodes': nodes,
            'edges': joined, 'warnings': warnings, 'source_sha256': hashlib.sha256(raw).hexdigest(),
            'stats': {'nodes': len(nodes), 'edges': len(joined),
                      'missing_refs': sum(w['kind'] == 'missing_ref' for w in warnings),
                      'unknown_edges': sum(w['kind'] == 'unknown_edge_endpoint' for w in warnings)},
            'mermaid': '\n'.join(lines) + '\n', 'evidence_content_verified': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['build'])
    parser.add_argument('--from-file', required=True)
    parser.add_argument('--base-dir', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--mermaid-out', required=True)
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)
    created = []
    try:
        base = resolve_path(Path(args.base_dir))
        if not base.is_dir():
            raise ValueError('base directory unavailable')
        source = Path(args.from_file)
        if not source.is_file():
            raise ValueError('input must be a regular file')
        with source.open('rb') as stream:
            raw = stream.read(MAX_INPUT + 1)
        result = build(raw, base)
        destinations = [Path(args.out), Path(args.mermaid_out)]
        if resolve_path(destinations[0], allow_missing=True) == resolve_path(destinations[1], allow_missing=True):
            raise ValueError('outputs must be distinct')
        for destination in destinations:
            if destination.exists() or destination.is_symlink() or not destination.parent.is_dir():
                raise ValueError('output exists or parent directory unavailable')
        encoded = json.dumps(result, ensure_ascii=False, indent=2) + '\n'
        for destination, content in zip(destinations, [encoded, result['mermaid']], strict=True):
            with destination.open('x', encoding='utf-8', newline='\n') as stream:
                created.append(str(destination))
                stream.write(content)
        print(json.dumps({'ok': True, 'stats': result['stats'], 'created_outputs': created,
                          'warnings': result['warnings'][:16],
                          'warnings_truncated': len(result['warnings']) > 16,
                          'mermaid_sha256': hashlib.sha256(result['mermaid'].encode()).hexdigest()}))
        return 0
    except (OSError, ValueError, TypeError, RecursionError) as error:
        print(json.dumps({'ok': False, 'error': str(error)[:256], 'created_outputs': created}))
        return 2


if __name__ == '__main__':
    sys.exit(main())
