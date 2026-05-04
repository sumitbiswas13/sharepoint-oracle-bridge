"""
SharePoint Data Models
Normalised representations of SharePoint entities.
Adapter-agnostic — both mock and real Graph adapters produce these shapes.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from enum import Enum


class ContentType(str, Enum):
    LIST        = "list"
    LIST_ITEM   = "list_item"
    DOCUMENT    = "document"
    SITE        = "site"
    PERMISSION  = "permission"


class SyncStatus(str, Enum):
    PENDING   = "pending"
    SYNCED    = "synced"
    FAILED    = "failed"
    SKIPPED   = "skipped"
    DELETED   = "deleted"


@dataclass
class SPSite:
    site_id:      str
    display_name: str
    web_url:      str
    description:  str = ""
    created_at:   Optional[datetime] = None
    modified_at:  Optional[datetime] = None
    owner_email:  Optional[str] = None
    storage_used: int = 0


@dataclass
class SPList:
    list_id:      str
    site_id:      str
    display_name: str
    description:  str = ""
    item_count:   int = 0
    created_at:   Optional[datetime] = None
    modified_at:  Optional[datetime] = None
    list_type:    str = "genericList"   # genericList | documentLibrary | calendar
    hidden:       bool = False


@dataclass
class SPColumn:
    name:          str
    display_name:  str
    column_type:   str    # text | number | dateTime | boolean | choice | lookup | person
    required:      bool = False
    indexed:       bool = False
    choices:       list[str] = field(default_factory=list)


@dataclass
class SPListItem:
    item_id:      str
    list_id:      str
    site_id:      str
    etag:         str                    # Change detection — etag changes on every update
    fields:       dict[str, Any] = field(default_factory=dict)
    created_at:   Optional[datetime] = None
    modified_at:  Optional[datetime] = None
    created_by:   Optional[str] = None
    modified_by:  Optional[str] = None
    deleted:      bool = False


@dataclass
class SPDocument:
    doc_id:        str
    list_id:       str
    site_id:       str
    file_name:     str
    file_type:     str       # docx | pdf | xlsx | txt | other
    file_size:     int       # bytes
    web_url:       str
    etag:          str
    extracted_text: Optional[str] = None
    page_count:    int = 0
    created_at:    Optional[datetime] = None
    modified_at:   Optional[datetime] = None
    created_by:    Optional[str] = None


@dataclass
class SPPermission:
    permission_id: str
    site_id:       str
    resource_type: str       # site | list | item
    resource_id:   str
    principal:     str       # email or group name
    role:          str       # owner | member | visitor | read | write
    granted_at:    Optional[datetime] = None


@dataclass
class DeltaToken:
    """Tracks the Graph API delta token for incremental sync."""
    resource_id:   str        # list_id or site_id
    resource_type: str        # list | site
    token:         str        # opaque token from Graph API
    last_sync_at:  datetime = field(default_factory=datetime.utcnow)
    items_synced:  int = 0


@dataclass
class WebhookSubscription:
    """A SharePoint webhook subscription registered with Graph API."""
    subscription_id:  str
    resource:         str        # /sites/{id}/lists/{id}
    notification_url: str        # Our FastAPI endpoint
    expiry:           datetime
    client_state:     str        # Validation secret
    created_at:       datetime = field(default_factory=datetime.utcnow)


@dataclass
class WebhookNotification:
    """Incoming notification from SharePoint webhook."""
    subscription_id:  str
    client_state:     str
    expiry:           str
    resource:         str
    site_url:         str
    tenant_id:        str
    received_at:      datetime = field(default_factory=datetime.utcnow)


@dataclass
class SyncResult:
    """Result of a sync operation."""
    operation:      str           # full_sync | delta_sync | webhook_sync | push_to_sharepoint
    resource_id:    str
    resource_type:  str
    items_found:    int = 0
    items_created:  int = 0
    items_updated:  int = 0
    items_deleted:  int = 0
    items_failed:   int = 0
    items_skipped:  int = 0
    delta_token:    Optional[str] = None
    errors:         list[str] = field(default_factory=list)
    duration_ms:    float = 0.0
    started_at:     datetime = field(default_factory=datetime.utcnow)

    @property
    def total_processed(self) -> int:
        return self.items_created + self.items_updated + self.items_deleted

    @property
    def success_rate(self) -> float:
        total = self.total_processed + self.items_failed
        return round(self.total_processed / max(total, 1) * 100, 1)
