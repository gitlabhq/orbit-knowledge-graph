from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.permission_rule import PermissionRule
    from ..models.sync_event_session_updated_data_info_revert_type_0 import (
        SyncEventSessionUpdatedDataInfoRevertType0,
    )
    from ..models.sync_event_session_updated_data_info_share import (
        SyncEventSessionUpdatedDataInfoShare,
    )
    from ..models.sync_event_session_updated_data_info_summary_type_0 import (
        SyncEventSessionUpdatedDataInfoSummaryType0,
    )
    from ..models.sync_event_session_updated_data_info_time import (
        SyncEventSessionUpdatedDataInfoTime,
    )


T = TypeVar("T", bound="SyncEventSessionUpdatedDataInfo")


@_attrs_define
class SyncEventSessionUpdatedDataInfo:
    """
    Attributes:
        id (None | str):
        slug (None | str):
        project_id (None | str):
        workspace_id (None | str):
        directory (None | str):
        parent_id (None | str):
        summary (None | SyncEventSessionUpdatedDataInfoSummaryType0):
        title (None | str):
        version (None | str):
        permission (list[PermissionRule] | None):
        revert (None | SyncEventSessionUpdatedDataInfoRevertType0):
        share (SyncEventSessionUpdatedDataInfoShare | Unset):
        time (SyncEventSessionUpdatedDataInfoTime | Unset):
    """

    id: None | str
    slug: None | str
    project_id: None | str
    workspace_id: None | str
    directory: None | str
    parent_id: None | str
    summary: None | SyncEventSessionUpdatedDataInfoSummaryType0
    title: None | str
    version: None | str
    permission: list[PermissionRule] | None
    revert: None | SyncEventSessionUpdatedDataInfoRevertType0
    share: SyncEventSessionUpdatedDataInfoShare | Unset = UNSET
    time: SyncEventSessionUpdatedDataInfoTime | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.sync_event_session_updated_data_info_revert_type_0 import (
            SyncEventSessionUpdatedDataInfoRevertType0,
        )
        from ..models.sync_event_session_updated_data_info_summary_type_0 import (
            SyncEventSessionUpdatedDataInfoSummaryType0,
        )

        id: None | str
        id = self.id

        slug: None | str
        slug = self.slug

        project_id: None | str
        project_id = self.project_id

        workspace_id: None | str
        workspace_id = self.workspace_id

        directory: None | str
        directory = self.directory

        parent_id: None | str
        parent_id = self.parent_id

        summary: dict[str, Any] | None
        if isinstance(self.summary, SyncEventSessionUpdatedDataInfoSummaryType0):
            summary = self.summary.to_dict()
        else:
            summary = self.summary

        title: None | str
        title = self.title

        version: None | str
        version = self.version

        permission: list[dict[str, Any]] | None
        if isinstance(self.permission, list):
            permission = []
            for componentsschemas_permission_ruleset_item_data in self.permission:
                componentsschemas_permission_ruleset_item = (
                    componentsschemas_permission_ruleset_item_data.to_dict()
                )
                permission.append(componentsschemas_permission_ruleset_item)

        else:
            permission = self.permission

        revert: dict[str, Any] | None
        if isinstance(self.revert, SyncEventSessionUpdatedDataInfoRevertType0):
            revert = self.revert.to_dict()
        else:
            revert = self.revert

        share: dict[str, Any] | Unset = UNSET
        if not isinstance(self.share, Unset):
            share = self.share.to_dict()

        time: dict[str, Any] | Unset = UNSET
        if not isinstance(self.time, Unset):
            time = self.time.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "slug": slug,
                "projectID": project_id,
                "workspaceID": workspace_id,
                "directory": directory,
                "parentID": parent_id,
                "summary": summary,
                "title": title,
                "version": version,
                "permission": permission,
                "revert": revert,
            }
        )
        if share is not UNSET:
            field_dict["share"] = share
        if time is not UNSET:
            field_dict["time"] = time

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.permission_rule import PermissionRule
        from ..models.sync_event_session_updated_data_info_revert_type_0 import (
            SyncEventSessionUpdatedDataInfoRevertType0,
        )
        from ..models.sync_event_session_updated_data_info_share import (
            SyncEventSessionUpdatedDataInfoShare,
        )
        from ..models.sync_event_session_updated_data_info_summary_type_0 import (
            SyncEventSessionUpdatedDataInfoSummaryType0,
        )
        from ..models.sync_event_session_updated_data_info_time import (
            SyncEventSessionUpdatedDataInfoTime,
        )

        d = dict(src_dict)

        def _parse_id(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        id = _parse_id(d.pop("id"))

        def _parse_slug(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        slug = _parse_slug(d.pop("slug"))

        def _parse_project_id(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        project_id = _parse_project_id(d.pop("projectID"))

        def _parse_workspace_id(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        workspace_id = _parse_workspace_id(d.pop("workspaceID"))

        def _parse_directory(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        directory = _parse_directory(d.pop("directory"))

        def _parse_parent_id(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        parent_id = _parse_parent_id(d.pop("parentID"))

        def _parse_summary(data: object) -> None | SyncEventSessionUpdatedDataInfoSummaryType0:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                summary_type_0 = SyncEventSessionUpdatedDataInfoSummaryType0.from_dict(data)

                return summary_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast("None | SyncEventSessionUpdatedDataInfoSummaryType0", data)

        summary = _parse_summary(d.pop("summary"))

        def _parse_title(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        title = _parse_title(d.pop("title"))

        def _parse_version(data: object) -> None | str:
            if data is None:
                return data
            return cast("None | str", data)

        version = _parse_version(d.pop("version"))

        def _parse_permission(data: object) -> list[PermissionRule] | None:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                permission_type_0 = []
                _permission_type_0 = data
                for componentsschemas_permission_ruleset_item_data in _permission_type_0:
                    componentsschemas_permission_ruleset_item = PermissionRule.from_dict(
                        componentsschemas_permission_ruleset_item_data
                    )

                    permission_type_0.append(componentsschemas_permission_ruleset_item)

                return permission_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast("list[PermissionRule] | None", data)

        permission = _parse_permission(d.pop("permission"))

        def _parse_revert(data: object) -> None | SyncEventSessionUpdatedDataInfoRevertType0:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                revert_type_0 = SyncEventSessionUpdatedDataInfoRevertType0.from_dict(data)

                return revert_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast("None | SyncEventSessionUpdatedDataInfoRevertType0", data)

        revert = _parse_revert(d.pop("revert"))

        _share = d.pop("share", UNSET)
        share: SyncEventSessionUpdatedDataInfoShare | Unset
        if isinstance(_share, Unset):
            share = UNSET
        else:
            share = SyncEventSessionUpdatedDataInfoShare.from_dict(_share)

        _time = d.pop("time", UNSET)
        time: SyncEventSessionUpdatedDataInfoTime | Unset
        if isinstance(_time, Unset):
            time = UNSET
        else:
            time = SyncEventSessionUpdatedDataInfoTime.from_dict(_time)

        sync_event_session_updated_data_info = cls(
            id=id,
            slug=slug,
            project_id=project_id,
            workspace_id=workspace_id,
            directory=directory,
            parent_id=parent_id,
            summary=summary,
            title=title,
            version=version,
            permission=permission,
            revert=revert,
            share=share,
            time=time,
        )

        sync_event_session_updated_data_info.additional_properties = d
        return sync_event_session_updated_data_info

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
