from typing import Optional

from pydantic import BaseModel


class Thumbnail(BaseModel):
    url: str
    width: int
    height: int


class Localized(BaseModel):
    title: str
    description: str


class Snippet(BaseModel):
    publishedAt: str
    channelId: str
    title: str
    description: str
    thumbnails: dict[str, Thumbnail]
    channelTitle: str
    tags: Optional[list[str]] = None
    categoryId: str
    liveBroadcastContent: str
    localized: Localized
    defaultAudioLanguage: Optional[str] = None
    defaultLanguage: Optional[str] = None


class Statistics(BaseModel):
    viewCount: Optional[str] = None
    likeCount: Optional[str] = None
    favoriteCount: str
    commentCount: Optional[str] = None


class Video(BaseModel):
    kind: str
    etag: str
    id: str
    snippet: Snippet
    statistics: Statistics


class PageInfo(BaseModel):
    totalResults: int
    resultsPerPage: int


class Videos(BaseModel):
    kind: str
    etag: str
    items: list[Video] = []
    pageInfo: PageInfo
