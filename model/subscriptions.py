from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class PageInfo(BaseModel):
    totalResults: int
    resultsPerPage: int


class ResourceId(BaseModel):
    kind: str
    channelId: str


class Thumbnail(BaseModel):
    url: str


class Snippet(BaseModel):
    publishedAt: datetime
    title: str
    description: str
    resourceId: ResourceId
    channelId: str
    thumbnails: dict[str, Thumbnail]


class Subscription(BaseModel):
    kind: str
    etag: str
    id: str
    snippet: Snippet


class Subscriptions(BaseModel):
    kind: str
    etag: str
    nextPageToken: Optional[str] = None
    pageInfo: PageInfo
    items: list[Subscription]
