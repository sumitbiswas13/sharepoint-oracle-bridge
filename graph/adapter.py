"""
Microsoft Graph Adapter
Connects to SharePoint via Microsoft Graph API.
Supports full sync, delta sync (changed items only), and webhook subscriptions.

Real adapter: needs GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET env vars.
Mock adapter: generates realistic SharePoint data — no credentials needed.
"""

import json
import random
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Iterator
from graph.models import (
    SPSite, SPList, SPListItem, SPDocument, SPPermission,
    DeltaToken, WebhookSubscription, SyncResult, SPColumn
)


class GraphAdapterBase(ABC):
    @abstractmethod
    def get_sites(self) -> list[SPSite]: pass

    @abstractmethod
    def get_lists(self, site_id: str) -> list[SPList]: pass

    @abstractmethod
    def get_list_items(self, site_id: str, list_id: str, delta_token: Optional[str] = None) -> tuple[list[SPListItem], str]: pass

    @abstractmethod
    def get_documents(self, site_id: str, list_id: str) -> list[SPDocument]: pass

    @abstractmethod
    def get_permissions(self, site_id: str) -> list[SPPermission]: pass

    @abstractmethod
    def create_list_item(self, site_id: str, list_id: str, fields: dict) -> SPListItem: pass

    @abstractmethod
    def update_list_item(self, site_id: str, list_id: str, item_id: str, fields: dict) -> bool: pass

    @abstractmethod
    def subscribe_webhook(self, site_id: str, list_id: str, notification_url: str) -> WebhookSubscription: pass

    @abstractmethod
    def refresh_webhook(self, subscription_id: str) -> bool: pass


# ── Mock adapter ──────────────────────────────────────────────

class MockGraphAdapter(GraphAdapterBase):
    """
    Realistic mock of the Microsoft Graph API.
    Generates deterministic SharePoint data for demos and testing.
    Simulates delta tokens — call get_list_items twice to see change detection work.
    """

    SITES_DATA = [
        {"id":"site-001","name":"Acme Corp Intranet",  "url":"https://acme.sharepoint.com/sites/intranet",  "owner":"admin@acme.com"},
        {"id":"site-002","name":"HR Portal",           "url":"https://acme.sharepoint.com/sites/hr",        "owner":"hr@acme.com"},
        {"id":"site-003","name":"Engineering Hub",     "url":"https://acme.sharepoint.com/sites/engineering","owner":"cto@acme.com"},
        {"id":"site-004","name":"Finance Workspace",   "url":"https://acme.sharepoint.com/sites/finance",   "owner":"cfo@acme.com"},
    ]

    LISTS_DATA = {
        "site-001": [
            {"id":"list-001","name":"Company Announcements","type":"genericList",   "count":24},
            {"id":"list-002","name":"Policy Documents",      "type":"documentLibrary","count":18},
            {"id":"list-003","name":"IT Help Desk Tickets",  "type":"genericList",   "count":142},
        ],
        "site-002": [
            {"id":"list-004","name":"Employee Directory",    "type":"genericList",   "count":87},
            {"id":"list-005","name":"Job Postings",          "type":"genericList",   "count":12},
            {"id":"list-006","name":"HR Policies Library",   "type":"documentLibrary","count":34},
        ],
        "site-003": [
            {"id":"list-007","name":"Sprint Backlog",        "type":"genericList",   "count":203},
            {"id":"list-008","name":"Architecture Docs",     "type":"documentLibrary","count":56},
            {"id":"list-009","name":"Incident Register",     "type":"genericList",   "count":31},
        ],
        "site-004": [
            {"id":"list-010","name":"Budget Tracker",        "type":"genericList",   "count":48},
            {"id":"list-011","name":"Vendor Contracts",      "type":"documentLibrary","count":22},
        ],
    }

    SAMPLE_FIELDS = {
        "genericList": [
            {"Title":"Q4 Planning Complete","Status":"Approved","Priority":"High",    "AssignedTo":"alice@acme.com","DueDate":"2024-12-15"},
            {"Title":"Server Migration",    "Status":"InProgress","Priority":"Critical","AssignedTo":"bob@acme.com",  "DueDate":"2024-11-30"},
            {"Title":"Security Audit",      "Status":"Pending",  "Priority":"Medium", "AssignedTo":"carol@acme.com","DueDate":"2025-01-10"},
            {"Title":"Budget Review",       "Status":"Complete", "Priority":"Low",    "AssignedTo":"dave@acme.com", "DueDate":"2024-12-01"},
            {"Title":"Vendor Evaluation",   "Status":"Pending",  "Priority":"High",   "AssignedTo":"emma@acme.com", "DueDate":"2025-01-20"},
        ],
        "documentLibrary": [
            {"Title":"Q3 Financial Report.pdf",        "FileSize":245760,  "Author":"cfo@acme.com"},
            {"Title":"Employee Handbook 2024.docx",    "FileSize":184320,  "Author":"hr@acme.com"},
            {"Title":"Architecture Decision Record.docx","FileSize":92160, "Author":"cto@acme.com"},
            {"Title":"Security Policy v2.pdf",         "FileSize":163840,  "Author":"security@acme.com"},
            {"Title":"Vendor Contract Template.docx",  "FileSize":71680,   "Author":"legal@acme.com"},
        ],
    }

    PERMISSIONS_DATA = [
        {"principal":"admin@acme.com",     "role":"owner"},
        {"principal":"hr@acme.com",        "role":"member"},
        {"principal":"Engineering Team",   "role":"member"},
        {"principal":"Finance Team",       "role":"visitor"},
        {"principal":"all-staff@acme.com", "role":"read"},
    ]

    # Delta state — tracks which items have "changed" between calls
    _delta_state: dict[str, int] = {}

    def get_sites(self) -> list[SPSite]:
        return [
            SPSite(
                site_id=s["id"],
                display_name=s["name"],
                web_url=s["url"],
                description=f"SharePoint site for {s['name']}",
                created_at=datetime(2022, 1, 15),
                modified_at=datetime.utcnow() - timedelta(days=random.randint(1, 30)),
                owner_email=s["owner"],
                storage_used=random.randint(100_000_000, 2_000_000_000),
            )
            for s in self.SITES_DATA
        ]

    def get_lists(self, site_id: str) -> list[SPList]:
        lists_raw = self.LISTS_DATA.get(site_id, [])
        return [
            SPList(
                list_id=l["id"],
                site_id=site_id,
                display_name=l["name"],
                description=f"SharePoint list: {l['name']}",
                item_count=l["count"],
                created_at=datetime(2022, 3, 1),
                modified_at=datetime.utcnow() - timedelta(hours=random.randint(1, 72)),
                list_type=l["type"],
            )
            for l in lists_raw
        ]

    def get_list_items(
        self,
        site_id: str,
        list_id: str,
        delta_token: Optional[str] = None,
    ) -> tuple[list[SPListItem], str]:
        """
        Returns (items, new_delta_token).
        On first call (no token): returns all items.
        On subsequent calls (with token): returns only changed items.
        This is the delta sync mechanism — massive bandwidth saving in production.
        """
        # Find list type
        site_lists = self.LISTS_DATA.get(site_id, [])
        list_info  = next((l for l in site_lists if l["id"] == list_id), None)
        list_type  = list_info["type"] if list_info else "genericList"
        item_count = list_info["count"] if list_info else 5

        # Generate items
        rng      = random.Random(f"{site_id}{list_id}")
        samples  = self.SAMPLE_FIELDS.get(list_type, self.SAMPLE_FIELDS["genericList"])
        users    = ["alice@acme.com","bob@acme.com","carol@acme.com","dave@acme.com","emma@acme.com"]
        all_items = []

        for i in range(min(item_count, 20)):
            sample = samples[i % len(samples)].copy()
            item_id = f"item-{list_id}-{i+1:04d}"
            etag    = hashlib.md5(f"{item_id}-v{self._delta_state.get(item_id,1)}".encode()).hexdigest()[:8]
            all_items.append(SPListItem(
                item_id=item_id,
                list_id=list_id,
                site_id=site_id,
                etag=etag,
                fields=sample,
                created_at=datetime.utcnow() - timedelta(days=rng.randint(1, 90)),
                modified_at=datetime.utcnow() - timedelta(hours=rng.randint(1, 168)),
                created_by=rng.choice(users),
                modified_by=rng.choice(users),
            ))

        # Delta logic — if token provided, simulate only returning changed items
        if delta_token:
            changed_count = rng.randint(1, max(1, len(all_items) // 4))
            items = rng.sample(all_items, changed_count)
            for item in items:
                self._delta_state[item.item_id] = self._delta_state.get(item.item_id, 1) + 1
        else:
            items = all_items

        # Generate new delta token (opaque string encoding the sync state)
        new_token = hashlib.sha256(
            f"{list_id}-{datetime.utcnow().isoformat()}-{len(items)}".encode()
        ).hexdigest()[:32]

        return items, new_token

    def get_documents(self, site_id: str, list_id: str) -> list[SPDocument]:
        site_lists = self.LISTS_DATA.get(site_id, [])
        list_info  = next((l for l in site_lists if l["id"] == list_id), None)
        if not list_info or list_info["type"] != "documentLibrary":
            return []

        samples = self.SAMPLE_FIELDS["documentLibrary"]
        docs    = []
        for i, s in enumerate(samples[:5]):
            fname     = s["Title"]
            ftype     = "pdf" if fname.endswith(".pdf") else "docx"
            doc_id    = f"doc-{list_id}-{i+1:03d}"
            docs.append(SPDocument(
                doc_id=doc_id,
                list_id=list_id,
                site_id=site_id,
                file_name=fname,
                file_type=ftype,
                file_size=s["FileSize"],
                web_url=f"https://acme.sharepoint.com/sites/docs/{fname.replace(' ','%20')}",
                etag=hashlib.md5(doc_id.encode()).hexdigest()[:8],
                extracted_text=self._mock_extracted_text(fname, ftype),
                page_count=random.randint(2, 25),
                created_at=datetime(2024, 1, 15),
                modified_at=datetime.utcnow() - timedelta(days=random.randint(1, 60)),
                created_by=s["Author"],
            ))
        return docs

    def get_permissions(self, site_id: str) -> list[SPPermission]:
        perms = []
        for i, p in enumerate(self.PERMISSIONS_DATA):
            perms.append(SPPermission(
                permission_id=f"perm-{site_id}-{i+1:03d}",
                site_id=site_id,
                resource_type="site",
                resource_id=site_id,
                principal=p["principal"],
                role=p["role"],
                granted_at=datetime(2022, 6, 1),
            ))
        return perms

    def create_list_item(self, site_id: str, list_id: str, fields: dict) -> SPListItem:
        """Bidirectional — Oracle data pushed to SharePoint."""
        new_id = f"item-{list_id}-new-{datetime.utcnow().strftime('%H%M%S')}"
        return SPListItem(
            item_id=new_id,
            list_id=list_id,
            site_id=site_id,
            etag=hashlib.md5(new_id.encode()).hexdigest()[:8],
            fields=fields,
            created_at=datetime.utcnow(),
            modified_at=datetime.utcnow(),
            created_by="oracle-bridge@acme.com",
        )

    def update_list_item(self, site_id: str, list_id: str, item_id: str, fields: dict) -> bool:
        """Bidirectional — Oracle update pushed back to SharePoint."""
        self._delta_state[item_id] = self._delta_state.get(item_id, 1) + 1
        return True

    def subscribe_webhook(self, site_id: str, list_id: str, notification_url: str) -> WebhookSubscription:
        sub_id = hashlib.sha256(f"{site_id}{list_id}{notification_url}".encode()).hexdigest()[:16]
        return WebhookSubscription(
            subscription_id=sub_id,
            resource=f"/sites/{site_id}/lists/{list_id}/items",
            notification_url=notification_url,
            expiry=datetime.utcnow() + timedelta(days=29),   # Graph max = 29 days
            client_state=hashlib.md5(sub_id.encode()).hexdigest()[:12],
        )

    def refresh_webhook(self, subscription_id: str) -> bool:
        return True   # In real adapter: PATCH to extend expiry

    def _mock_extracted_text(self, filename: str, ftype: str) -> str:
        texts = {
            "Q3 Financial Report.pdf":         "Total revenue Q3 2024: $4.2M. Operating expenses: $2.8M. EBITDA: $1.4M. Headcount: 87 FTE.",
            "Employee Handbook 2024.docx":     "Welcome to Acme Corp. This handbook outlines our policies on leave, conduct, benefits, and remote work.",
            "Architecture Decision Record.docx":"ADR-042: Adopt Oracle as primary transactional database. Rationale: existing payroll system, team expertise.",
            "Security Policy v2.pdf":          "All systems must use MFA. Passwords minimum 12 characters. Annual penetration testing required.",
            "Vendor Contract Template.docx":   "This agreement is entered into between Acme Corp and Vendor. Payment terms: Net 30. Liability cap: $500,000.",
        }
        return texts.get(filename, f"Extracted text from {filename}. Document contains {random.randint(200,2000)} words.")


# ── Real Graph adapter stub ───────────────────────────────────

class RealGraphAdapter(GraphAdapterBase):
    """
    Production Microsoft Graph API adapter.
    Requires app registration in Azure AD with:
      - Sites.Read.All (or Sites.ReadWrite.All for bidirectional)
      - Files.Read.All
    Swap MockGraphAdapter for RealGraphAdapter — no other code changes.
    """

    GRAPH_BASE = "https://graph.microsoft.com/v1.0"

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self._tenant_id    = tenant_id
        self._client_id    = client_id
        self._client_secret = client_secret
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    def _get_token(self) -> str:
        if self._token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._token
        import httpx
        resp = httpx.post(
            f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._client_id,
                "client_secret": self._client_secret,
                "scope":         "https://graph.microsoft.com/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token        = data["access_token"]
        self._token_expiry = datetime.utcnow() + timedelta(seconds=data["expires_in"] - 60)
        return self._token

    def _get(self, path: str, params: dict = None) -> dict:
        import httpx
        resp = httpx.get(
            f"{self.GRAPH_BASE}{path}",
            headers={"Authorization": f"Bearer {self._get_token()}"},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, body: dict) -> dict:
        import httpx
        resp = httpx.patch(
            f"{self.GRAPH_BASE}{path}",
            headers={"Authorization": f"Bearer {self._get_token()}", "Content-Type": "application/json"},
            json=body, timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict) -> dict:
        import httpx
        resp = httpx.post(
            f"{self.GRAPH_BASE}{path}",
            headers={"Authorization": f"Bearer {self._get_token()}", "Content-Type": "application/json"},
            json=body, timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_sites(self) -> list[SPSite]:
        data = self._get("/sites", params={"search": "*"})
        sites = []
        for s in data.get("value", []):
            sites.append(SPSite(
                site_id=s["id"],
                display_name=s.get("displayName",""),
                web_url=s.get("webUrl",""),
                description=s.get("description",""),
                created_at=datetime.fromisoformat(s["createdDateTime"].rstrip("Z")) if "createdDateTime" in s else None,
                modified_at=datetime.fromisoformat(s["lastModifiedDateTime"].rstrip("Z")) if "lastModifiedDateTime" in s else None,
            ))
        return sites

    def get_lists(self, site_id: str) -> list[SPList]:
        data = self._get(f"/sites/{site_id}/lists")
        lists = []
        for l in data.get("value", []):
            if l.get("list",{}).get("hidden", False):
                continue
            lists.append(SPList(
                list_id=l["id"],
                site_id=site_id,
                display_name=l.get("displayName",""),
                description=l.get("description",""),
                list_type=l.get("list",{}).get("template","genericList"),
                created_at=datetime.fromisoformat(l["createdDateTime"].rstrip("Z")) if "createdDateTime" in l else None,
            ))
        return lists

    def get_list_items(self, site_id: str, list_id: str, delta_token: Optional[str] = None) -> tuple[list[SPListItem], str]:
        if delta_token:
            data = self._get(f"/sites/{site_id}/lists/{list_id}/items/delta",
                            params={"$deltatoken": delta_token})
        else:
            data = self._get(f"/sites/{site_id}/lists/{list_id}/items/delta",
                            params={"$expand": "fields"})

        items = []
        for raw in data.get("value", []):
            deleted = "@removed" in raw
            items.append(SPListItem(
                item_id=raw["id"],
                list_id=list_id,
                site_id=site_id,
                etag=raw.get("@odata.etag",""),
                fields=raw.get("fields",{}),
                deleted=deleted,
                created_at=datetime.fromisoformat(raw["createdDateTime"].rstrip("Z")) if "createdDateTime" in raw else None,
                modified_at=datetime.fromisoformat(raw["lastModifiedDateTime"].rstrip("Z")) if "lastModifiedDateTime" in raw else None,
            ))

        next_link   = data.get("@odata.nextLink","")
        delta_link  = data.get("@odata.deltaLink","")
        new_token   = delta_link.split("$deltatoken=")[-1] if "$deltatoken=" in delta_link else ""
        return items, new_token

    def get_documents(self, site_id: str, list_id: str) -> list[SPDocument]:
        data = self._get(f"/sites/{site_id}/lists/{list_id}/items",
                         params={"$expand": "driveItem"})
        docs = []
        for raw in data.get("value", []):
            di = raw.get("driveItem",{})
            if not di:
                continue
            fname = di.get("name","")
            ftype = fname.rsplit(".",1)[-1].lower() if "." in fname else "other"
            docs.append(SPDocument(
                doc_id=di.get("id",""),
                list_id=list_id,
                site_id=site_id,
                file_name=fname,
                file_type=ftype,
                file_size=di.get("size",0),
                web_url=di.get("webUrl",""),
                etag=di.get("eTag",""),
            ))
        return docs

    def get_permissions(self, site_id: str) -> list[SPPermission]:
        data = self._get(f"/sites/{site_id}/permissions")
        perms = []
        for p in data.get("value", []):
            for role in p.get("roles", []):
                grantee = p.get("grantedToV2",{})
                principal = (grantee.get("user",{}).get("email") or
                             grantee.get("group",{}).get("displayName") or "unknown")
                perms.append(SPPermission(
                    permission_id=p["id"],
                    site_id=site_id,
                    resource_type="site",
                    resource_id=site_id,
                    principal=principal,
                    role=role,
                ))
        return perms

    def create_list_item(self, site_id: str, list_id: str, fields: dict) -> SPListItem:
        data = self._post(f"/sites/{site_id}/lists/{list_id}/items", {"fields": fields})
        return SPListItem(
            item_id=data["id"],
            list_id=list_id,
            site_id=site_id,
            etag=data.get("@odata.etag",""),
            fields=fields,
        )

    def update_list_item(self, site_id: str, list_id: str, item_id: str, fields: dict) -> bool:
        self._patch(f"/sites/{site_id}/lists/{list_id}/items/{item_id}/fields", fields)
        return True

    def subscribe_webhook(self, site_id: str, list_id: str, notification_url: str) -> WebhookSubscription:
        import secrets
        client_state = secrets.token_hex(12)
        expiry       = (datetime.utcnow() + timedelta(days=29)).strftime("%Y-%m-%dT%H:%M:%SZ")
        data = self._post("/subscriptions", {
            "changeType":       "updated,deleted,added",
            "notificationUrl":  notification_url,
            "resource":         f"/sites/{site_id}/lists/{list_id}/items",
            "expirationDateTime": expiry,
            "clientState":      client_state,
        })
        return WebhookSubscription(
            subscription_id=data["id"],
            resource=data["resource"],
            notification_url=notification_url,
            expiry=datetime.fromisoformat(data["expirationDateTime"].rstrip("Z")),
            client_state=client_state,
        )

    def refresh_webhook(self, subscription_id: str) -> bool:
        new_expiry = (datetime.utcnow() + timedelta(days=29)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._patch(f"/subscriptions/{subscription_id}", {"expirationDateTime": new_expiry})
        return True
