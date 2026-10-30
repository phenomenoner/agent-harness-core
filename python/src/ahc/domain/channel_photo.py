"""Strict, bounded image validation for channel-native photo delivery."""

import zlib

from .sessions import ContractError

MAX_PHOTO_BYTES = 1_048_576
MAX_PHOTO_DIMENSION = 10_000
MAX_PHOTO_RATIO = 20
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_JPEG_SIGNATURE = b"\xff\xd8\xff"
_JPEG_SOF_MARKERS = frozenset({
    0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
    0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
})
_JPEG_STANDALONE_MARKERS = frozenset({0x01, *range(0xD0, 0xD9)})


def _reject():
    raise ContractError("photo content invalid")


def _png_dimensions(content):
    if len(content) < len(_PNG_SIGNATURE) or not content.startswith(_PNG_SIGNATURE):
        _reject()
    position = len(_PNG_SIGNATURE)
    dimensions = None
    has_idat = False
    while position < len(content):
        if position + 12 > len(content):
            _reject()
        length = int.from_bytes(content[position:position + 4], "big")
        chunk_type = content[position + 4:position + 8]
        chunk_end = position + 12 + length
        if chunk_end > len(content):
            _reject()
        chunk_data = content[position + 8:position + 8 + length]
        expected_crc = int.from_bytes(content[position + 8 + length:chunk_end], "big")
        if zlib.crc32(chunk_type + chunk_data) & 0xFFFFFFFF != expected_crc:
            _reject()
        if position == len(_PNG_SIGNATURE) and chunk_type != b"IHDR":
            _reject()
        if chunk_type == b"IHDR":
            if dimensions is not None or length != 13:
                _reject()
            dimensions = (int.from_bytes(chunk_data[0:4], "big"),
                          int.from_bytes(chunk_data[4:8], "big"))
        elif chunk_type == b"IDAT":
            if dimensions is None:
                _reject()
            has_idat = True
        elif chunk_type == b"IEND":
            if (length != 0 or dimensions is None or not has_idat
                    or chunk_end != len(content)):
                _reject()
            return dimensions
        position = chunk_end
    _reject()


def _jpeg_dimensions(content):
    if not content.startswith(_JPEG_SIGNATURE) or not content.endswith(b"\xff\xd9"):
        _reject()
    position = 2
    while position < len(content):
        if content[position] != 0xFF:
            _reject()
        while position < len(content) and content[position] == 0xFF:
            position += 1
        if position >= len(content):
            _reject()
        marker = content[position]
        position += 1
        if marker == 0x00:
            _reject()
        if marker in _JPEG_STANDALONE_MARKERS:
            continue
        if position + 2 > len(content):
            _reject()
        segment_length = int.from_bytes(content[position:position + 2], "big")
        if segment_length < 2 or position + segment_length > len(content):
            _reject()
        if marker in _JPEG_SOF_MARKERS:
            if (segment_length < 11 or content[position + 2] not in (8, 12, 16)
                    or content[position + 7] not in (1, 3, 4)
                    or segment_length != 8 + 3 * content[position + 7]):
                _reject()
            height = int.from_bytes(content[position + 3:position + 5], "big")
            width = int.from_bytes(content[position + 5:position + 7], "big")
            return width, height
        position += segment_length
    _reject()


def validate_photo(content, media_type):
    """Validate a bounded PNG/JPEG and return its ``(width, height)``."""

    if (type(content) is not bytes or not 0 < len(content) <= MAX_PHOTO_BYTES
            or type(media_type) is not str):
        _reject()
    if media_type == "image/png":
        width, height = _png_dimensions(content)
    elif media_type == "image/jpeg":
        width, height = _jpeg_dimensions(content)
    else:
        _reject()
    if (type(width) is not int or type(height) is not int
            or not 0 < width <= MAX_PHOTO_DIMENSION
            or not 0 < height <= MAX_PHOTO_DIMENSION
            or width + height > MAX_PHOTO_DIMENSION
            or max(width, height) > MAX_PHOTO_RATIO * min(width, height)):
        _reject()
    return width, height


__all__ = [
    "MAX_PHOTO_BYTES",
    "MAX_PHOTO_DIMENSION",
    "MAX_PHOTO_RATIO",
    "validate_photo",
]
