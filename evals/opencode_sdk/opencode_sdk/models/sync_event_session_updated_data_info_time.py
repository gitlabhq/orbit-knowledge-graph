from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="SyncEventSessionUpdatedDataInfoTime")


@_attrs_define
class SyncEventSessionUpdatedDataInfoTime:
    """
    Attributes:
        created (float | None):
        updated (float | None):
        compacting (float | None):
        archived (float | None):
    """

    created: float | None
    updated: float | None
    compacting: float | None
    archived: float | None
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        created: float | None
        created = self.created

        updated: float | None
        updated = self.updated

        compacting: float | None
        compacting = self.compacting

        archived: float | None
        archived = self.archived

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "created": created,
                "updated": updated,
                "compacting": compacting,
                "archived": archived,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_created(data: object) -> float | None:
            if data is None:
                return data
            return cast("float | None", data)

        created = _parse_created(d.pop("created"))

        def _parse_updated(data: object) -> float | None:
            if data is None:
                return data
            return cast("float | None", data)

        updated = _parse_updated(d.pop("updated"))

        def _parse_compacting(data: object) -> float | None:
            if data is None:
                return data
            return cast("float | None", data)

        compacting = _parse_compacting(d.pop("compacting"))

        def _parse_archived(data: object) -> float | None:
            if data is None:
                return data
            return cast("float | None", data)

        archived = _parse_archived(d.pop("archived"))

        sync_event_session_updated_data_info_time = cls(
            created=created,
            updated=updated,
            compacting=compacting,
            archived=archived,
        )

        sync_event_session_updated_data_info_time.additional_properties = d
        return sync_event_session_updated_data_info_time

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
