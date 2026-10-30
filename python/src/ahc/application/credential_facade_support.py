"""Exception and result-safety helpers used by the issued façade."""

from collections.abc import Mapping
from dataclasses import fields, is_dataclass


def resolve_provider(provider, request):
    try:
        return provider.resolve(request), False, None
    except SystemExit:
        return None, False, "system-exit"
    except KeyboardInterrupt:
        return None, False, "keyboard-interrupt"
    except GeneratorExit:
        return None, False, "generator-exit"
    except BaseException:
        return None, True, None


def invoke_admission(admission, request):
    try:
        return admission(request), False, None
    except SystemExit:
        return None, False, "system-exit"
    except KeyboardInterrupt:
        return None, False, "keyboard-interrupt"
    except GeneratorExit:
        return None, False, "generator-exit"
    except BaseException:
        return None, True, None


def read_cancellation(probe):
    try:
        state = probe.is_cancelled()
    except SystemExit:
        return None, False, "system-exit"
    except KeyboardInterrupt:
        return None, False, "keyboard-interrupt"
    except GeneratorExit:
        return None, False, "generator-exit"
    except BaseException:
        return None, True, None
    if type(state) is not bool:
        return None, True, None
    return state, False, None


def invoke_consumer(consumer, material, cancellation_probe):
    try:
        return consumer(material, cancellation_probe), False, None
    except SystemExit:
        return None, False, "system-exit"
    except KeyboardInterrupt:
        return None, False, "keyboard-interrupt"
    except GeneratorExit:
        return None, False, "generator-exit"
    except BaseException:
        return None, True, None


def raise_process_control(kind):
    if kind == "system-exit":
        raise SystemExit(1) from None
    if kind == "keyboard-interrupt":
        raise KeyboardInterrupt() from None
    if kind == "generator-exit":
        raise GeneratorExit() from None
    raise RuntimeError("unsupported process-control kind")


def contains_material(value, material, material_view_type, seen=None):
    if type(value) is bytes:
        return value == material
    if type(value) in (bytearray, memoryview):
        return bytes(value) == material
    if isinstance(value, material_view_type):
        return True
    if type(value) is str:
        try:
            return value == material.decode("utf-8")
        except UnicodeDecodeError:
            return False
    if value is None or type(value) in (bool, int, float):
        return False

    actual_seen = set() if seen is None else seen
    identity = id(value)
    if identity in actual_seen:
        return False
    actual_seen.add(identity)

    if is_dataclass(value) and not isinstance(value, type):
        return any(
            contains_material(getattr(value, field.name), material, material_view_type, actual_seen)
            for field in fields(value)
        )
    if isinstance(value, Mapping):
        return any(
            contains_material(item, material, material_view_type, actual_seen)
            for pair in value.items()
            for item in pair
        )
    if type(value) in (tuple, list, set, frozenset):
        return any(
            contains_material(item, material, material_view_type, actual_seen) for item in value
        )
    return False
