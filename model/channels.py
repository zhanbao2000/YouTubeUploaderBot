from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class PageInfo(BaseModel):
    totalResults: int
    resultsPerPage: int


class Thumbnail(BaseModel):
    url: str
    width: int
    height: int


class Localized(BaseModel):
    title: str
    description: str


class Snippet(BaseModel):
    title: str
    description: str
    customUrl: str
    publishedAt: datetime
    thumbnails: dict[str, Thumbnail]
    localized: Localized
    country: Optional[str] = None
    defaultLanguage: Optional[str] = None


class RelatedPlaylists(BaseModel):
    likes: str
    uploads: str


class ContentDetails(BaseModel):
    relatedPlaylists: RelatedPlaylists


class Statistics(BaseModel):
    viewCount: str
    subscriberCount: str
    hiddenSubscriberCount: bool
    videoCount: str


class Channel(BaseModel):
    kind: str
    etag: str
    id: str
    snippet: Snippet
    contentDetails: ContentDetails
    statistics: Statistics


class Channels(BaseModel):
    kind: str
    etag: str
    pageInfo: PageInfo
    items: list[Channel]
