"""Bounded, dependency-free channel text formatting and provider payloads."""

import html
import re
from urllib.parse import urlsplit

TELEGRAM_MAX_CHARS = 4096
DISCORD_MAX_CHARS = 4096
DISCORD_CONTENT_MAX_CHARS = 2000

_HEADING_RE = re.compile(r"^ {0,3}#{1,6}(?:[ \t]+(.*)|[ \t]*)$")
_QUOTE_RE = re.compile(r"^ {0,3}>[ \t]?(.*)$")
_UNORDERED_RE = re.compile(r"^ {0,3}([-+*])[ \t]+(.*)$")
_ORDERED_RE = re.compile(r"^ {0,3}([0-9]{1,9})[.)][ \t]+(.*)$")
_FENCE_RE = re.compile(r"^ {0,3}```(?:[ \t]*(.*))?$")
_FENCE_CLOSE_RE = re.compile(r"^ {0,3}```[ \t]*$")


def _validate_text(text):
    if type(text) is not str or any(0xD800 <= ord(char) <= 0xDFFF for char in text):
        raise ValueError("channel text invalid")
    return text


def _utf16_length(text):
    return len(text.encode("utf-16-le")) // 2


def _escape_text(value):
    return html.escape(value, quote=True)


def _safe_http_url(value):
    if (type(value) is not str or not value or any(
            char.isspace() or ord(char) < 0x20 or char in '<>"' for char in value)):
        return False
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def _find_unescaped(value, needle, start):
    index = start
    while True:
        index = value.find(needle, index)
        if index < 0:
            return None
        backslashes = 0
        prior = index - 1
        while prior >= 0 and value[prior] == "\\":
            backslashes += 1
            prior -= 1
        if backslashes % 2 == 0:
            return index
        index += len(needle)


def _find_run_end(value, start, char):
    end = start
    while end < len(value) and value[end] == char:
        end += 1
    return end


def _find_triple_close(value, start):
    cursor = start
    while True:
        index = value.find("***", cursor)
        if index < 0:
            return None
        if index == 0 or value[index - 1] != "\\":
            return index
        cursor = index + 3


def _find_bold_close(value, start):
    cursor = start
    while True:
        index = value.find("**", cursor)
        if index < 0:
            return None
        if index and value[index - 1] == "\\":
            cursor = index + 2
            continue
        run_end = _find_run_end(value, index, "*")
        close = run_end - 2 if run_end - index >= 3 else index
        if close > start:
            return close
        cursor = run_end


def _find_italic_close(value, start, delimiter):
    cursor = start
    while True:
        index = _find_unescaped(value, delimiter, cursor)
        if index is None:
            return None
        if delimiter == "_":
            if index + 1 < len(value) and value[index + 1].isalnum():
                cursor = index + 1
                continue
        if delimiter == "*" and index + 1 < len(value) and value[index + 1] == "*":
            cursor = index + 1
            continue
        if index > start:
            return index
        cursor = index + 1


def _try_link(value, index, depth):
    if value[index] != "[":
        return None
    label_end = _find_unescaped(value, "]", index + 1)
    if label_end is None or label_end + 1 >= len(value) or value[label_end + 1] != "(":
        return None
    url_end = _find_unescaped(value, ")", label_end + 2)
    if url_end is None:
        return None
    label = value[index + 1:label_end]
    url = value[label_end + 2:url_end]
    consumed = url_end + 1 - index
    literal = value[index:index + consumed]
    if not _safe_http_url(url):
        return _escape_text(literal), consumed
    return (
        f'<a href="{html.escape(url, quote=True)}">'
        f"{_render_inline(label, depth + 1)}</a>",
        consumed,
    )


def _render_inline(value, depth=0):
    if depth > 32:
        return _escape_text(value)
    output = []
    index = 0
    while index < len(value):
        if value[index] == "\\" and index + 1 < len(value):
            output.append(_escape_text(value[index + 1]))
            index += 2
            continue
        if value.startswith("```", index):
            run_end = _find_run_end(value, index, "`")
            output.append(_escape_text(value[index:run_end]))
            index = run_end
            continue
        if value[index] == "`":
            close = _find_unescaped(value, "`", index + 1)
            if close is not None:
                output.append(f"<code>{_escape_text(value[index + 1:close])}</code>")
                index = close + 1
                continue
            output.append("`")
            index += 1
            continue
        link = _try_link(value, index, depth)
        if link is not None:
            rendered, consumed = link
            output.append(rendered)
            index += consumed
            continue
        if value.startswith("***", index):
            close = _find_triple_close(value, index + 3)
            if close is not None and close > index + 3:
                inner = _render_inline(value[index + 3:close], depth + 1)
                output.append(f"<b><i>{inner}</i></b>")
                index = close + 3
                continue
        if value.startswith("**", index):
            close = _find_bold_close(value, index + 2)
            if close is not None:
                inner = value[index + 2:close]
                if inner:
                    output.append(f"<b>{_render_inline(inner, depth + 1)}</b>")
                    index = close + 2
                    continue
            output.append("**")
            index += 2
            continue
        if value[index] in "*_" and not value.startswith(value[index] * 2, index):
            delimiter = value[index]
            if delimiter == "_" and index and value[index - 1].isalnum():
                output.append(_escape_text(delimiter))
                index += 1
                continue
            close = _find_italic_close(value, index + 1, delimiter)
            if close is not None:
                inner = value[index + 1:close]
                if inner and not inner.isspace():
                    output.append(f"<i>{_render_inline(inner, depth + 1)}</i>")
                    index = close + 1
                    continue
            output.append(_escape_text(delimiter))
            index += 1
            continue
        output.append(_escape_text(value[index]))
        index += 1
    return "".join(output)


def _fenced_block(lines, start):
    opening = _FENCE_RE.fullmatch(lines[start])
    if opening is None:
        return None
    for index in range(start + 1, len(lines)):
        if _FENCE_CLOSE_RE.fullmatch(lines[index]):
            info = (opening.group(1) or "").strip()
            language = info.split()[0] if info else ""
            body = "\n".join(lines[start + 1:index])
            escaped = _escape_text(body)
            if language:
                rendered = (f'<pre><code class="language-{html.escape(language, quote=True)}">'
                            f"{escaped}</code></pre>")
            else:
                rendered = f"<pre>{escaped}</pre>"
            return rendered, index + 1
    return "unclosed", len(lines)


def _render_blocks(text):
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    blocks = []
    paragraph = []

    def flush_paragraph():
        if paragraph:
            blocks.append(_render_inline("\n".join(paragraph)))
            paragraph.clear()

    index = 0
    while index < len(lines):
        fenced = _fenced_block(lines, index)
        if fenced is not None:
            flush_paragraph()
            rendered, next_index = fenced
            if rendered == "unclosed":
                blocks.append(_escape_text("\n".join(lines[index:])))
                break
            blocks.append(rendered)
            index = next_index
            continue
        heading = _HEADING_RE.fullmatch(lines[index])
        if heading is not None:
            flush_paragraph()
            blocks.append(f"<b>{_render_inline(heading.group(1) or '')}</b>")
            index += 1
            continue
        quote = _QUOTE_RE.fullmatch(lines[index])
        if quote is not None:
            flush_paragraph()
            quoted = []
            while index < len(lines):
                current = _QUOTE_RE.fullmatch(lines[index])
                if current is None:
                    break
                quoted.append(current.group(1))
                index += 1
            blocks.append(f"<blockquote>{_render_inline(chr(10).join(quoted))}</blockquote>")
            continue
        unordered = _UNORDERED_RE.fullmatch(lines[index])
        ordered = _ORDERED_RE.fullmatch(lines[index])
        if unordered is not None or ordered is not None:
            flush_paragraph()
            items = []
            is_ordered = ordered is not None
            while index < len(lines):
                current = (_ORDERED_RE.fullmatch(lines[index]) if is_ordered
                           else _UNORDERED_RE.fullmatch(lines[index]))
                if current is None:
                    break
                prefix = f"{current.group(1)}." if is_ordered else "•"
                items.append(f"{prefix} {_render_inline(current.group(2))}")
                index += 1
            blocks.append("\n".join(items))
            continue
        if not lines[index].strip():
            flush_paragraph()
            index += 1
            continue
        paragraph.append(lines[index])
        index += 1
    flush_paragraph()
    return "\n\n".join(blocks)


def telegram_text(text):
    """Return a Telegram wire fragment, falling back to full plain text if needed."""
    text = _validate_text(text)
    if len(text) > TELEGRAM_MAX_CHARS:
        raise ValueError("telegram source text exceeds Python character limit")
    rendered = _render_blocks(text)
    if _utf16_length(rendered) <= TELEGRAM_MAX_CHARS:
        return {"text": rendered, "parse_mode": "HTML"}
    if _utf16_length(text) <= TELEGRAM_MAX_CHARS:
        return {"text": text}
    raise ValueError("telegram text exceeds UTF-16 limit after plain fallback")


def discord_text(text):
    """Return one Discord content or embed payload while preserving the source text."""
    text = _validate_text(text)
    if len(text) > DISCORD_MAX_CHARS:
        raise ValueError("discord source text exceeds embed limit")
    mentions = {"allowed_mentions": {"parse": []}}
    if len(text) <= DISCORD_CONTENT_MAX_CHARS:
        return {"content": text, **mentions}
    return {"embeds": [{"description": text}], **mentions}


__all__ = ["discord_text", "telegram_text"]
