"""Pydantic model for GitHub repository data."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

__all__: list[str] = ["RepositoryModel"]


class RepositoryModel(BaseModel):
    """Validated repository record, ready for DB insertion.

    Fields mirror the ``repositories`` DuckDB table.  Class methods provide
    convenient constructors from GitHub's GraphQL and REST payloads.
    """

    id: str
    name: str
    full_name: str
    owner_login: str | None = None
    owner_type: str | None = None
    description: str | None = None
    url: str | None = None
    homepage_url: str | None = None
    primary_language: str | None = None
    topics: list[str] = Field(default_factory=list)
    license_spdx: str | None = None
    is_archived: bool = False
    is_fork: bool = False
    is_template: bool = False
    stars: int = 0
    forks: int = 0
    watchers: int = 0
    open_issues: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    pushed_at: datetime | None = None
    disk_usage_kb: int | None = None

    # Enrichment fields (populated after REST calls)
    readme_length_chars: int | None = None
    readme_h2_sections: int | None = None
    readme_has_badges: bool | None = None
    readme_has_demo_gif: bool | None = None
    readme_has_install: bool | None = None
    contributors_count: int | None = None
    releases_count: int | None = None
    latest_release_tag: str | None = None
    latest_release_at: datetime | None = None

    @classmethod
    def from_graphql(cls, node: dict) -> RepositoryModel:
        """Construct a model from a raw GraphQL search node.

        Args:
            node: A single element from ``search.nodes`` in the GraphQL
                response.

        Returns:
            A validated ``RepositoryModel`` instance.
        """
        topics_raw = node.get("repositoryTopics", {}).get("nodes", [])
        topics = [t["topic"]["name"] for t in topics_raw if t.get("topic")]

        primary_lang = node.get("primaryLanguage")
        license_info = node.get("licenseInfo")
        owner = node.get("owner", {})

        return cls(
            id=node["id"],
            name=node["name"],
            full_name=node["nameWithOwner"],
            owner_login=owner.get("login"),
            owner_type=owner.get("__typename"),
            description=node.get("description"),
            url=node.get("url"),
            homepage_url=node.get("homepageUrl"),
            primary_language=primary_lang["name"] if primary_lang else None,
            topics=topics,
            license_spdx=license_info.get("spdxId") if license_info else None,
            is_archived=node.get("isArchived", False),
            is_fork=node.get("isFork", False),
            is_template=node.get("isTemplate", False),
            stars=node.get("stargazerCount", 0),
            forks=node.get("forkCount", 0),
            watchers=node.get("watchers", {}).get("totalCount", 0),
            open_issues=node.get("issues", {}).get("totalCount", 0),
            created_at=node.get("createdAt"),
            updated_at=node.get("updatedAt"),
            pushed_at=node.get("pushedAt"),
            disk_usage_kb=node.get("diskUsage"),
        )
