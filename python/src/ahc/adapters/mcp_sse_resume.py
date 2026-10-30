"""Bounded resume metadata from complete SSE events, never a replayed POST."""


class ResumeStream(Exception):
    def __init__(self, cursor, delay_ms):
        self.cursor, self.delay_ms = cursor, delay_ms


def resume_metadata(raw):
    cursor, delay = None, 0
    for block in raw.replace(b"\r\n", b"\n").split(b"\n\n")[:-1]:
        for line in block.split(b"\n"):
            if line.startswith(b"id:"):
                value = line[3:].removeprefix(b" ")
                if len(value) > 1024 or any(not 33 <= char <= 126 for char in value):
                    raise ValueError("unsupported SSE cursor")
                cursor = value.decode("ascii") or None
            if line.startswith(b"retry:"):
                value = line[6:].removeprefix(b" ")
                if value.isdigit():
                    if len(value) > 9:
                        raise ValueError("SSE retry exceeds request lifetime")
                    delay = int(value)
    return cursor, delay
