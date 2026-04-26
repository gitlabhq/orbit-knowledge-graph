from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.event_command_executed import EventCommandExecuted
    from ..models.event_file_edited import EventFileEdited
    from ..models.event_file_watcher_updated import EventFileWatcherUpdated
    from ..models.event_global_disposed import EventGlobalDisposed
    from ..models.event_installation_update_available import EventInstallationUpdateAvailable
    from ..models.event_installation_updated import EventInstallationUpdated
    from ..models.event_lsp_client_diagnostics import EventLspClientDiagnostics
    from ..models.event_lsp_updated import EventLspUpdated
    from ..models.event_mcp_browser_open_failed import EventMcpBrowserOpenFailed
    from ..models.event_mcp_tools_changed import EventMcpToolsChanged
    from ..models.event_message_part_delta import EventMessagePartDelta
    from ..models.event_message_part_removed import EventMessagePartRemoved
    from ..models.event_message_part_updated import EventMessagePartUpdated
    from ..models.event_message_removed import EventMessageRemoved
    from ..models.event_message_updated import EventMessageUpdated
    from ..models.event_permission_asked import EventPermissionAsked
    from ..models.event_permission_replied import EventPermissionReplied
    from ..models.event_project_updated import EventProjectUpdated
    from ..models.event_pty_created import EventPtyCreated
    from ..models.event_pty_deleted import EventPtyDeleted
    from ..models.event_pty_exited import EventPtyExited
    from ..models.event_pty_updated import EventPtyUpdated
    from ..models.event_question_asked import EventQuestionAsked
    from ..models.event_question_rejected import EventQuestionRejected
    from ..models.event_question_replied import EventQuestionReplied
    from ..models.event_server_connected import EventServerConnected
    from ..models.event_server_instance_disposed import EventServerInstanceDisposed
    from ..models.event_session_compacted import EventSessionCompacted
    from ..models.event_session_created import EventSessionCreated
    from ..models.event_session_deleted import EventSessionDeleted
    from ..models.event_session_diff import EventSessionDiff
    from ..models.event_session_error import EventSessionError
    from ..models.event_session_idle import EventSessionIdle
    from ..models.event_session_status import EventSessionStatus
    from ..models.event_session_updated import EventSessionUpdated
    from ..models.event_todo_updated import EventTodoUpdated
    from ..models.event_tui_command_execute import EventTuiCommandExecute
    from ..models.event_tui_prompt_append import EventTuiPromptAppend
    from ..models.event_tui_session_select import EventTuiSessionSelect
    from ..models.event_tui_toast_show import EventTuiToastShow
    from ..models.event_vcs_branch_updated import EventVcsBranchUpdated
    from ..models.event_workspace_failed import EventWorkspaceFailed
    from ..models.event_workspace_ready import EventWorkspaceReady
    from ..models.event_workspace_restore import EventWorkspaceRestore
    from ..models.event_workspace_status import EventWorkspaceStatus
    from ..models.event_worktree_failed import EventWorktreeFailed
    from ..models.event_worktree_ready import EventWorktreeReady
    from ..models.sync_event_message_part_removed import SyncEventMessagePartRemoved
    from ..models.sync_event_message_part_updated import SyncEventMessagePartUpdated
    from ..models.sync_event_message_removed import SyncEventMessageRemoved
    from ..models.sync_event_message_updated import SyncEventMessageUpdated
    from ..models.sync_event_session_created import SyncEventSessionCreated
    from ..models.sync_event_session_deleted import SyncEventSessionDeleted
    from ..models.sync_event_session_updated import SyncEventSessionUpdated


T = TypeVar("T", bound="GlobalEvent")


@_attrs_define
class GlobalEvent:
    """
    Attributes:
        directory (str):
        payload (EventCommandExecuted | EventFileEdited | EventFileWatcherUpdated | EventGlobalDisposed |
            EventInstallationUpdateAvailable | EventInstallationUpdated | EventLspClientDiagnostics | EventLspUpdated |
            EventMcpBrowserOpenFailed | EventMcpToolsChanged | EventMessagePartDelta | EventMessagePartRemoved |
            EventMessagePartUpdated | EventMessageRemoved | EventMessageUpdated | EventPermissionAsked |
            EventPermissionReplied | EventProjectUpdated | EventPtyCreated | EventPtyDeleted | EventPtyExited |
            EventPtyUpdated | EventQuestionAsked | EventQuestionRejected | EventQuestionReplied | EventServerConnected |
            EventServerInstanceDisposed | EventSessionCompacted | EventSessionCreated | EventSessionDeleted |
            EventSessionDiff | EventSessionError | EventSessionIdle | EventSessionStatus | EventSessionUpdated |
            EventTodoUpdated | EventTuiCommandExecute | EventTuiPromptAppend | EventTuiSessionSelect | EventTuiToastShow |
            EventVcsBranchUpdated | EventWorkspaceFailed | EventWorkspaceReady | EventWorkspaceRestore |
            EventWorkspaceStatus | EventWorktreeFailed | EventWorktreeReady | SyncEventMessagePartRemoved |
            SyncEventMessagePartUpdated | SyncEventMessageRemoved | SyncEventMessageUpdated | SyncEventSessionCreated |
            SyncEventSessionDeleted | SyncEventSessionUpdated):
        project (str | Unset):
        workspace (str | Unset):
    """

    directory: str
    payload: (
        EventCommandExecuted
        | EventFileEdited
        | EventFileWatcherUpdated
        | EventGlobalDisposed
        | EventInstallationUpdateAvailable
        | EventInstallationUpdated
        | EventLspClientDiagnostics
        | EventLspUpdated
        | EventMcpBrowserOpenFailed
        | EventMcpToolsChanged
        | EventMessagePartDelta
        | EventMessagePartRemoved
        | EventMessagePartUpdated
        | EventMessageRemoved
        | EventMessageUpdated
        | EventPermissionAsked
        | EventPermissionReplied
        | EventProjectUpdated
        | EventPtyCreated
        | EventPtyDeleted
        | EventPtyExited
        | EventPtyUpdated
        | EventQuestionAsked
        | EventQuestionRejected
        | EventQuestionReplied
        | EventServerConnected
        | EventServerInstanceDisposed
        | EventSessionCompacted
        | EventSessionCreated
        | EventSessionDeleted
        | EventSessionDiff
        | EventSessionError
        | EventSessionIdle
        | EventSessionStatus
        | EventSessionUpdated
        | EventTodoUpdated
        | EventTuiCommandExecute
        | EventTuiPromptAppend
        | EventTuiSessionSelect
        | EventTuiToastShow
        | EventVcsBranchUpdated
        | EventWorkspaceFailed
        | EventWorkspaceReady
        | EventWorkspaceRestore
        | EventWorkspaceStatus
        | EventWorktreeFailed
        | EventWorktreeReady
        | SyncEventMessagePartRemoved
        | SyncEventMessagePartUpdated
        | SyncEventMessageRemoved
        | SyncEventMessageUpdated
        | SyncEventSessionCreated
        | SyncEventSessionDeleted
        | SyncEventSessionUpdated
    )
    project: str | Unset = UNSET
    workspace: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.event_command_executed import EventCommandExecuted
        from ..models.event_file_edited import EventFileEdited
        from ..models.event_file_watcher_updated import EventFileWatcherUpdated
        from ..models.event_global_disposed import EventGlobalDisposed
        from ..models.event_installation_update_available import EventInstallationUpdateAvailable
        from ..models.event_installation_updated import EventInstallationUpdated
        from ..models.event_lsp_client_diagnostics import EventLspClientDiagnostics
        from ..models.event_lsp_updated import EventLspUpdated
        from ..models.event_mcp_browser_open_failed import EventMcpBrowserOpenFailed
        from ..models.event_mcp_tools_changed import EventMcpToolsChanged
        from ..models.event_message_part_delta import EventMessagePartDelta
        from ..models.event_message_part_removed import EventMessagePartRemoved
        from ..models.event_message_part_updated import EventMessagePartUpdated
        from ..models.event_message_removed import EventMessageRemoved
        from ..models.event_message_updated import EventMessageUpdated
        from ..models.event_permission_asked import EventPermissionAsked
        from ..models.event_permission_replied import EventPermissionReplied
        from ..models.event_project_updated import EventProjectUpdated
        from ..models.event_pty_created import EventPtyCreated
        from ..models.event_pty_deleted import EventPtyDeleted
        from ..models.event_pty_exited import EventPtyExited
        from ..models.event_pty_updated import EventPtyUpdated
        from ..models.event_question_asked import EventQuestionAsked
        from ..models.event_question_rejected import EventQuestionRejected
        from ..models.event_question_replied import EventQuestionReplied
        from ..models.event_server_connected import EventServerConnected
        from ..models.event_server_instance_disposed import EventServerInstanceDisposed
        from ..models.event_session_compacted import EventSessionCompacted
        from ..models.event_session_created import EventSessionCreated
        from ..models.event_session_deleted import EventSessionDeleted
        from ..models.event_session_diff import EventSessionDiff
        from ..models.event_session_error import EventSessionError
        from ..models.event_session_idle import EventSessionIdle
        from ..models.event_session_status import EventSessionStatus
        from ..models.event_session_updated import EventSessionUpdated
        from ..models.event_todo_updated import EventTodoUpdated
        from ..models.event_tui_command_execute import EventTuiCommandExecute
        from ..models.event_tui_prompt_append import EventTuiPromptAppend
        from ..models.event_tui_session_select import EventTuiSessionSelect
        from ..models.event_tui_toast_show import EventTuiToastShow
        from ..models.event_vcs_branch_updated import EventVcsBranchUpdated
        from ..models.event_workspace_failed import EventWorkspaceFailed
        from ..models.event_workspace_ready import EventWorkspaceReady
        from ..models.event_workspace_restore import EventWorkspaceRestore
        from ..models.event_workspace_status import EventWorkspaceStatus
        from ..models.event_worktree_failed import EventWorktreeFailed
        from ..models.event_worktree_ready import EventWorktreeReady
        from ..models.sync_event_message_part_removed import SyncEventMessagePartRemoved
        from ..models.sync_event_message_part_updated import SyncEventMessagePartUpdated
        from ..models.sync_event_message_removed import SyncEventMessageRemoved
        from ..models.sync_event_message_updated import SyncEventMessageUpdated
        from ..models.sync_event_session_created import SyncEventSessionCreated
        from ..models.sync_event_session_updated import SyncEventSessionUpdated

        directory = self.directory

        payload: dict[str, Any]
        if (
            isinstance(self.payload, EventProjectUpdated)
            or isinstance(self.payload, EventServerInstanceDisposed)
            or isinstance(self.payload, EventServerConnected)
            or isinstance(self.payload, EventGlobalDisposed)
            or isinstance(self.payload, EventFileEdited)
            or isinstance(self.payload, EventFileWatcherUpdated)
            or isinstance(self.payload, EventLspClientDiagnostics)
            or isinstance(self.payload, EventLspUpdated)
            or isinstance(self.payload, EventInstallationUpdated)
            or isinstance(self.payload, EventInstallationUpdateAvailable)
            or isinstance(self.payload, EventMessagePartDelta)
            or isinstance(self.payload, EventPermissionAsked)
            or isinstance(self.payload, EventPermissionReplied)
            or isinstance(self.payload, EventSessionDiff)
            or isinstance(self.payload, EventSessionError)
            or isinstance(self.payload, EventQuestionAsked)
            or isinstance(self.payload, EventQuestionReplied)
            or isinstance(self.payload, EventQuestionRejected)
            or isinstance(self.payload, EventTodoUpdated)
            or isinstance(self.payload, EventSessionStatus)
            or isinstance(self.payload, EventSessionIdle)
            or isinstance(self.payload, EventSessionCompacted)
            or isinstance(self.payload, EventTuiPromptAppend)
            or isinstance(self.payload, EventTuiCommandExecute)
            or isinstance(self.payload, EventTuiToastShow)
            or isinstance(self.payload, EventTuiSessionSelect)
            or isinstance(self.payload, EventMcpToolsChanged)
            or isinstance(self.payload, EventMcpBrowserOpenFailed)
            or isinstance(self.payload, EventCommandExecuted)
            or isinstance(self.payload, EventVcsBranchUpdated)
            or isinstance(self.payload, EventWorktreeReady)
            or isinstance(self.payload, EventWorktreeFailed)
            or isinstance(self.payload, EventPtyCreated)
            or isinstance(self.payload, EventPtyUpdated)
            or isinstance(self.payload, EventPtyExited)
            or isinstance(self.payload, EventPtyDeleted)
            or isinstance(self.payload, EventWorkspaceReady)
            or isinstance(self.payload, EventWorkspaceFailed)
            or isinstance(self.payload, EventWorkspaceRestore)
            or isinstance(self.payload, EventWorkspaceStatus)
            or isinstance(self.payload, EventMessageUpdated)
            or isinstance(self.payload, EventMessageRemoved)
            or isinstance(self.payload, EventMessagePartUpdated)
            or isinstance(self.payload, EventMessagePartRemoved)
            or isinstance(self.payload, EventSessionCreated)
            or isinstance(self.payload, EventSessionUpdated)
            or isinstance(self.payload, EventSessionDeleted)
            or isinstance(self.payload, SyncEventMessageUpdated)
            or isinstance(self.payload, SyncEventMessageRemoved)
            or isinstance(self.payload, SyncEventMessagePartUpdated)
            or isinstance(self.payload, SyncEventMessagePartRemoved)
            or isinstance(self.payload, SyncEventSessionCreated)
            or isinstance(self.payload, SyncEventSessionUpdated)
        ):
            payload = self.payload.to_dict()
        else:
            payload = self.payload.to_dict()

        project = self.project

        workspace = self.workspace

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "directory": directory,
                "payload": payload,
            }
        )
        if project is not UNSET:
            field_dict["project"] = project
        if workspace is not UNSET:
            field_dict["workspace"] = workspace

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.event_command_executed import EventCommandExecuted
        from ..models.event_file_edited import EventFileEdited
        from ..models.event_file_watcher_updated import EventFileWatcherUpdated
        from ..models.event_global_disposed import EventGlobalDisposed
        from ..models.event_installation_update_available import EventInstallationUpdateAvailable
        from ..models.event_installation_updated import EventInstallationUpdated
        from ..models.event_lsp_client_diagnostics import EventLspClientDiagnostics
        from ..models.event_lsp_updated import EventLspUpdated
        from ..models.event_mcp_browser_open_failed import EventMcpBrowserOpenFailed
        from ..models.event_mcp_tools_changed import EventMcpToolsChanged
        from ..models.event_message_part_delta import EventMessagePartDelta
        from ..models.event_message_part_removed import EventMessagePartRemoved
        from ..models.event_message_part_updated import EventMessagePartUpdated
        from ..models.event_message_removed import EventMessageRemoved
        from ..models.event_message_updated import EventMessageUpdated
        from ..models.event_permission_asked import EventPermissionAsked
        from ..models.event_permission_replied import EventPermissionReplied
        from ..models.event_project_updated import EventProjectUpdated
        from ..models.event_pty_created import EventPtyCreated
        from ..models.event_pty_deleted import EventPtyDeleted
        from ..models.event_pty_exited import EventPtyExited
        from ..models.event_pty_updated import EventPtyUpdated
        from ..models.event_question_asked import EventQuestionAsked
        from ..models.event_question_rejected import EventQuestionRejected
        from ..models.event_question_replied import EventQuestionReplied
        from ..models.event_server_connected import EventServerConnected
        from ..models.event_server_instance_disposed import EventServerInstanceDisposed
        from ..models.event_session_compacted import EventSessionCompacted
        from ..models.event_session_created import EventSessionCreated
        from ..models.event_session_deleted import EventSessionDeleted
        from ..models.event_session_diff import EventSessionDiff
        from ..models.event_session_error import EventSessionError
        from ..models.event_session_idle import EventSessionIdle
        from ..models.event_session_status import EventSessionStatus
        from ..models.event_session_updated import EventSessionUpdated
        from ..models.event_todo_updated import EventTodoUpdated
        from ..models.event_tui_command_execute import EventTuiCommandExecute
        from ..models.event_tui_prompt_append import EventTuiPromptAppend
        from ..models.event_tui_session_select import EventTuiSessionSelect
        from ..models.event_tui_toast_show import EventTuiToastShow
        from ..models.event_vcs_branch_updated import EventVcsBranchUpdated
        from ..models.event_workspace_failed import EventWorkspaceFailed
        from ..models.event_workspace_ready import EventWorkspaceReady
        from ..models.event_workspace_restore import EventWorkspaceRestore
        from ..models.event_workspace_status import EventWorkspaceStatus
        from ..models.event_worktree_failed import EventWorktreeFailed
        from ..models.event_worktree_ready import EventWorktreeReady
        from ..models.sync_event_message_part_removed import SyncEventMessagePartRemoved
        from ..models.sync_event_message_part_updated import SyncEventMessagePartUpdated
        from ..models.sync_event_message_removed import SyncEventMessageRemoved
        from ..models.sync_event_message_updated import SyncEventMessageUpdated
        from ..models.sync_event_session_created import SyncEventSessionCreated
        from ..models.sync_event_session_deleted import SyncEventSessionDeleted
        from ..models.sync_event_session_updated import SyncEventSessionUpdated

        d = dict(src_dict)
        directory = d.pop("directory")

        def _parse_payload(
            data: object,
        ) -> (
            EventCommandExecuted
            | EventFileEdited
            | EventFileWatcherUpdated
            | EventGlobalDisposed
            | EventInstallationUpdateAvailable
            | EventInstallationUpdated
            | EventLspClientDiagnostics
            | EventLspUpdated
            | EventMcpBrowserOpenFailed
            | EventMcpToolsChanged
            | EventMessagePartDelta
            | EventMessagePartRemoved
            | EventMessagePartUpdated
            | EventMessageRemoved
            | EventMessageUpdated
            | EventPermissionAsked
            | EventPermissionReplied
            | EventProjectUpdated
            | EventPtyCreated
            | EventPtyDeleted
            | EventPtyExited
            | EventPtyUpdated
            | EventQuestionAsked
            | EventQuestionRejected
            | EventQuestionReplied
            | EventServerConnected
            | EventServerInstanceDisposed
            | EventSessionCompacted
            | EventSessionCreated
            | EventSessionDeleted
            | EventSessionDiff
            | EventSessionError
            | EventSessionIdle
            | EventSessionStatus
            | EventSessionUpdated
            | EventTodoUpdated
            | EventTuiCommandExecute
            | EventTuiPromptAppend
            | EventTuiSessionSelect
            | EventTuiToastShow
            | EventVcsBranchUpdated
            | EventWorkspaceFailed
            | EventWorkspaceReady
            | EventWorkspaceRestore
            | EventWorkspaceStatus
            | EventWorktreeFailed
            | EventWorktreeReady
            | SyncEventMessagePartRemoved
            | SyncEventMessagePartUpdated
            | SyncEventMessageRemoved
            | SyncEventMessageUpdated
            | SyncEventSessionCreated
            | SyncEventSessionDeleted
            | SyncEventSessionUpdated
        ):
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_0 = EventProjectUpdated.from_dict(data)

                return payload_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_1 = EventServerInstanceDisposed.from_dict(data)

                return payload_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_2 = EventServerConnected.from_dict(data)

                return payload_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_3 = EventGlobalDisposed.from_dict(data)

                return payload_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_4 = EventFileEdited.from_dict(data)

                return payload_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_5 = EventFileWatcherUpdated.from_dict(data)

                return payload_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_6 = EventLspClientDiagnostics.from_dict(data)

                return payload_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_7 = EventLspUpdated.from_dict(data)

                return payload_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_8 = EventInstallationUpdated.from_dict(data)

                return payload_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_9 = EventInstallationUpdateAvailable.from_dict(data)

                return payload_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_10 = EventMessagePartDelta.from_dict(data)

                return payload_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_11 = EventPermissionAsked.from_dict(data)

                return payload_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_12 = EventPermissionReplied.from_dict(data)

                return payload_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_13 = EventSessionDiff.from_dict(data)

                return payload_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_14 = EventSessionError.from_dict(data)

                return payload_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_15 = EventQuestionAsked.from_dict(data)

                return payload_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_16 = EventQuestionReplied.from_dict(data)

                return payload_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_17 = EventQuestionRejected.from_dict(data)

                return payload_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_18 = EventTodoUpdated.from_dict(data)

                return payload_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_19 = EventSessionStatus.from_dict(data)

                return payload_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_20 = EventSessionIdle.from_dict(data)

                return payload_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_21 = EventSessionCompacted.from_dict(data)

                return payload_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_22 = EventTuiPromptAppend.from_dict(data)

                return payload_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_23 = EventTuiCommandExecute.from_dict(data)

                return payload_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_24 = EventTuiToastShow.from_dict(data)

                return payload_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_25 = EventTuiSessionSelect.from_dict(data)

                return payload_type_25
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_26 = EventMcpToolsChanged.from_dict(data)

                return payload_type_26
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_27 = EventMcpBrowserOpenFailed.from_dict(data)

                return payload_type_27
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_28 = EventCommandExecuted.from_dict(data)

                return payload_type_28
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_29 = EventVcsBranchUpdated.from_dict(data)

                return payload_type_29
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_30 = EventWorktreeReady.from_dict(data)

                return payload_type_30
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_31 = EventWorktreeFailed.from_dict(data)

                return payload_type_31
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_32 = EventPtyCreated.from_dict(data)

                return payload_type_32
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_33 = EventPtyUpdated.from_dict(data)

                return payload_type_33
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_34 = EventPtyExited.from_dict(data)

                return payload_type_34
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_35 = EventPtyDeleted.from_dict(data)

                return payload_type_35
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_36 = EventWorkspaceReady.from_dict(data)

                return payload_type_36
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_37 = EventWorkspaceFailed.from_dict(data)

                return payload_type_37
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_38 = EventWorkspaceRestore.from_dict(data)

                return payload_type_38
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_39 = EventWorkspaceStatus.from_dict(data)

                return payload_type_39
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_40 = EventMessageUpdated.from_dict(data)

                return payload_type_40
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_41 = EventMessageRemoved.from_dict(data)

                return payload_type_41
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_42 = EventMessagePartUpdated.from_dict(data)

                return payload_type_42
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_43 = EventMessagePartRemoved.from_dict(data)

                return payload_type_43
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_44 = EventSessionCreated.from_dict(data)

                return payload_type_44
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_45 = EventSessionUpdated.from_dict(data)

                return payload_type_45
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_46 = EventSessionDeleted.from_dict(data)

                return payload_type_46
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_47 = SyncEventMessageUpdated.from_dict(data)

                return payload_type_47
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_48 = SyncEventMessageRemoved.from_dict(data)

                return payload_type_48
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_49 = SyncEventMessagePartUpdated.from_dict(data)

                return payload_type_49
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_50 = SyncEventMessagePartRemoved.from_dict(data)

                return payload_type_50
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_51 = SyncEventSessionCreated.from_dict(data)

                return payload_type_51
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                payload_type_52 = SyncEventSessionUpdated.from_dict(data)

                return payload_type_52
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            payload_type_53 = SyncEventSessionDeleted.from_dict(data)

            return payload_type_53

        payload = _parse_payload(d.pop("payload"))

        project = d.pop("project", UNSET)

        workspace = d.pop("workspace", UNSET)

        global_event = cls(
            directory=directory,
            payload=payload,
            project=project,
            workspace=workspace,
        )

        global_event.additional_properties = d
        return global_event

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
