"""Deterministic, non-live M2.5 provider and cancellation test doubles."""

from __future__ import annotations

from collections.abc import Mapping

from ahc.domain.credential_values import CredentialUseRequest


class FakeCredentialProvider:
    """Explicit fake custody provider; never discovers or contacts a host source."""

    def __init__(
        self,
        material_by_ref: Mapping[str, bytes],
        *,
        events: list[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        copied: dict[str, bytes] = {}
        for reference, material in material_by_ref.items():
            if type(reference) is not str or type(material) is not bytes or not material:
                raise TypeError(
                    "fake credential rows require string references and non-empty bytes"
                )
            copied[reference] = bytes(material)
        self.__material_by_ref = copied
        self.__error = error
        self.__events = events
        self.resolve_count = 0
        self.__requests: list[str] = []

    @property
    def requests(self) -> tuple[str, ...]:
        return tuple(self.__requests)

    def resolve(self, request: CredentialUseRequest) -> bytes | None:
        if type(request) is not CredentialUseRequest:
            raise TypeError("request must be a CredentialUseRequest")
        self.resolve_count += 1
        self.__requests.append(request.credential_ref)
        if self.__events is not None:
            self.__events.append("resolve")
        if self.__error is not None:
            raise self.__error
        material = self.__material_by_ref.get(request.credential_ref)
        return None if material is None else bytes(material)

    def __repr__(self) -> str:
        return f"<fake-credential-provider rows={len(self.__material_by_ref)} redacted>"


class AbsentCredentialProvider:
    def __init__(self) -> None:
        self.resolve_count = 0

    def resolve(self, request: CredentialUseRequest) -> None:
        if type(request) is not CredentialUseRequest:
            raise TypeError("request must be a CredentialUseRequest")
        self.resolve_count += 1
        return None

    def __repr__(self) -> str:
        return "<absent-credential-provider>"


class MutableCancellationProbe:
    def __init__(self, *, cancelled: bool = False) -> None:
        if type(cancelled) is not bool:
            raise TypeError("cancelled must be a strict boolean")
        self.__cancelled = cancelled

    def cancel(self) -> None:
        self.__cancelled = True

    def is_cancelled(self) -> bool:
        return self.__cancelled


__all__ = [
    "AbsentCredentialProvider",
    "FakeCredentialProvider",
    "MutableCancellationProbe",
]
