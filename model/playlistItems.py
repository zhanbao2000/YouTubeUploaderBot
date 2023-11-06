from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Thumbnail(BaseModel):
    url: str
    width: int
    height: int


class ResourceId(BaseModel):
    kind: str
    videoId: str


class Snippet(BaseModel):
    publishedAt: datetime
    channelId: str
    title: str
    description: str
    thumbnails: dict[str, Thumbnail]
    channelTitle: str
    playlistId: str
    position: int
    resourceId: ResourceId
    videoOwnerChannelTitle: Optional[str] = None
    videoOwnerChannelId: Optional[str] = None


class PlaylistItem(BaseModel):
    kind: str
    etag: str
    id: str
    snippet: Snippet


class PageInfo(BaseModel):
    totalResults: int
    resultsPerPage: int


class PlaylistItems(BaseModel):
    kind: str
    etag: str
    nextPageToken: Optional[str] = None
    items: list[PlaylistItem]
    pageInfo: PageInfo
