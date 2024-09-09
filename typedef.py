from asyncio import Queue
from enum import Enum
from typing import Optional

from yt_dlp.utils import YoutubeDLError


class IncompleteTranscodingError(YoutubeDLError):
    msg = 'Transcoding for this video is not yet complete on YouTube servers.'


class VideoTooShortError(RuntimeError):
    pass


class UniqueQueue(Queue):
    """A queue that ensures all items are unique (set-like). It supports the `in` operator to check for item existence."""

    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self._set = set()

    def __contains__(self, item):
        return item in self._set

    def _put(self, item):
        if item not in self._set:
            super()._put(item)  # noqa
            self._set.add(item)

    def _get(self):
        item = super()._get()  # noqa
        self._set.remove(item)
        return item


class Task(object):
    def __init__(self, url: str, chat_id: Optional[int] = None, message_id: Optional[int] = None):
        self.url = url
        self.chat_id = chat_id
        self.message_id = message_id

    # override __hash__() and __eq__() so that set() only considers url when deduplicating

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, other):
        if isinstance(other, Task):
            return self.url == other.url
        if isinstance(other, str):
            return self.url == other
        return False


class AddResult(int, Enum):
    SUCCESS = 1
    DUPLICATE_DATABASE = 2  # already in the database, regardless of the status
    DUPLICATE_DATABASE_UPLOADED = 3  # already in the database, and successfully uploaded
    DUPLICATE_DATABASE_FAILED = 4  # already in the database, but failed to upload
    DUPLICATE_QUEUE = 5  # already in video_queue
    DUPLICATE_RETRY = 6  # already in retry_tasks, but not ready to retry
    DUPLICATE_CURRENT = 7  # worker is currently processing this video


class RetryReason(str, Enum):
    LIVE_NOT_STARTED = 'this live has not yet started'
    NETWORK_ERROR = 'a network error occurs when upload this video'
    INCOMPLETE_TRANSCODING = 'transcoding for this video is not yet complete on YouTube servers'


class VideoStatus(int, Enum):
    # positive values are available or can be made available
    TOO_SHORT = 2
    AVAILABLE = 1

    # zero means error on uploading, usually this is because the file size exceeds the Telegram limit
    ERROR_ON_UPLOADING = 0

    # negative values are unavailable from YouTube
    UNAVAILABLE = -1
    VIDEO_PRIVATE = -2
    VIDEO_DELETED = -3
    ACCOUNT_TERMINATED = -4
    ACCOUNT_CLOSED = -5
    NUDITY_OR_SEXUAL_CONTENT = -6
    MEMBERS_ONLY = -7


HashTag = {
    VideoStatus.AVAILABLE: '#已重新可用',
    VideoStatus.ERROR_ON_UPLOADING: '#上传失败',
    VideoStatus.UNAVAILABLE: '#不能观看',
    VideoStatus.VIDEO_PRIVATE: '#私享视频',
    VideoStatus.VIDEO_DELETED: '#已删除',
    VideoStatus.ACCOUNT_TERMINATED: '#账号已终止',
    VideoStatus.ACCOUNT_CLOSED: '#账号已关闭',
    VideoStatus.NUDITY_OR_SEXUAL_CONTENT: '#裸露或色情内容',
    VideoStatus.MEMBERS_ONLY: '#会员限定'
}
