"""Composition-private M2.5 provider, consumer, and material protocols."""

from __future__ import annotations

from typing import Protocol, TypeVar

from ahc.domain.credential_values import CredentialUseRequest

_DomainResult = TypeVar("_DomainResult")


class _CredentialMaterialView:
    __slots__ = ("__material",)

    def __init__(self, material: bytes) -> None:
        if type(material) is not bytes or not material:
            raise TypeError("credential material must be non-empty bytes")
        self.__material = material

    def reveal(self) -> bytes:
        """Expose bytes only inside the trusted, composition-bound consumer."""

        return self.__material

    def __repr__(self) -> str:
        return "<credential-material:redacted>"

    __str__ = __repr__


class _CredentialProvider(Protocol):
    def resolve(self, request: CredentialUseRequest) -> bytes | None: ...


class _CancellationProbe(Protocol):
    def is_cancelled(self) -> bool: ...


class _CredentialConsumer(Protocol[_DomainResult]):
    def __call__(
        self,
        material: _CredentialMaterialView,
        cancellation_probe: _CancellationProbe,
    ) -> _DomainResult: ...


__all__: list[str] = []
