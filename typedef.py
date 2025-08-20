from asyncio import Queue, QueueFull
from enum import Enum
from time import time
from typing import NamedTuple, Optional

from yt_dlp.utils import YoutubeDLError


class IncompleteTranscodingError(YoutubeDLError):
    msg = 'Transcoding for this video is not yet complete on YouTube servers.'


class VideoTooShortError(RuntimeError):
    def __init__(self, video_info: dict):
        self.video_info = video_info
        super().__init__()


class UniqueQueue(Queue):
    """A queue that ensures all items are unique (set-like). It supports the `in` operator to check for item existence."""

    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self._set = set()

    def __contains__(self, item):
        return item in self._set

    def _put(self, item):
        if item not in self._set:
            super()._put(item)
            self._set.add(item)

    def _get(self):
        item = super()._get()
        self._set.remove(item)
        return item

    def put_left_nowait(self, item):
        """Put an item into the front of the queue."""
        if self.full():
            raise QueueFull
        if item not in self._set:
            self._queue.appendleft(item)
            self._set.add(item)
            self._unfinished_tasks += 1
            self._finished.clear()
            self._wakeup_next(self._getters)


class ProgressStatus(object):
    """A super class to collect progress information"""

    def __init__(self):
        self.in_progress = False
        self.finished = False
        self.transferred = 0
        self.total = 0
        self.start_ts = 0.

    def update(self, *args, **kwargs) -> None:
        if self.start_ts == 0.:
            self.start_ts = time()

    def get_percentage(self) -> float:
        return self.transferred / self.total * 100 if self.total else 0.

    def get_elapsed(self) -> float:
        return time() - self.start_ts if self.start_ts else 0.

    def get_avg_speed(self) -> float:
        return self.transferred / self.get_elapsed() if self.get_elapsed() else 0.

    def get_avg_speed_formatted(self) -> str:
        bytes_per_second = self.get_avg_speed()
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_per_second < 1024:
                result = f'{bytes_per_second:.1f} {unit}'
                break
            bytes_per_second /= 1024
        else:
            result = f'{bytes_per_second:.1f} PB'
        return f'{result}/s'

    def get_eta(self) -> str:
        remain_bytes = self.total - self.transferred
        speed = self.get_avg_speed()

        if speed == 0.:
            return 'N/A'

        remain_seconds = int(remain_bytes / speed)
        days, remain_seconds = divmod(remain_seconds, 60 * 60 * 24)
        hours, remain_seconds = divmod(remain_seconds, 60 * 60)
        minutes, seconds = divmod(remain_seconds, 60)

        if days > 0:
            return f'{days}d{hours}h{minutes}m{seconds}s'
        elif hours > 0:
            return f'{hours}h{minutes}m{seconds}s'
        elif minutes > 0:
            return f'{minutes}m{seconds}s'
        else:
            return f'{seconds}s'


class DownloadProgressStatus(ProgressStatus):
    """A class to collect download progress information from yt-dlp"""

    def __init__(self):
        super().__init__()
        self.title = 'Unknown'

    def update(self, video_info: dict) -> None:
        super().update()
        if video_info['status'] == 'downloading':
            self.in_progress = True
            self.transferred = video_info.get('downloaded_bytes', 0)
            self.total = video_info.get('total_bytes', 0) or video_info.get('total_bytes_estimate', 0)
            self.title = video_info.get('info_dict', {}).get('title', 'Unknown')
        else:
            self.in_progress = False
            self.finished = True


class UploadProgressStatus(ProgressStatus):
    """A class to collect upload progress information from pyrogram"""

    def update(self, transferred: int, total: int) -> None:
        super().update()
        self.in_progress = True
        self.transferred = transferred
        self.total = total
        if transferred == total:
            self.in_progress = False
            self.finished = True


class Channel(NamedTuple):
    name: str
    url: str


class Task(object):
    def __init__(
            self,
            url: str,
            chat_id: Optional[int] = None,
            message_id: Optional[int] = None,
            use_cookies: Optional[bool] = False,
    ):
        self.url = url
        self.chat_id = chat_id
        self.message_id = message_id
        self.use_cookies = use_cookies

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
    BLOCKLIST = 8  # video is in blocklist


class RetryReason(str, Enum):
    LIVE_NOT_STARTED = 'this live has not yet started'
    NETWORK_ERROR = 'a network error occurs when upload this video'
    INCOMPLETE_TRANSCODING = 'transcoding for this video is not yet complete on YouTube servers'
    LOGIN_REQUIRED = 'login is required to access this video'


class VideoStatus(int, Enum):
    # positive values are available or can be made available
    UNAVAILABLE_RECORD = 4
    MEMBERS_ONLY = 3
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

    VIOLATE_TOS = -8


HashTag = {
    VideoStatus.AVAILABLE: '#已重新可用',
    VideoStatus.ERROR_ON_UPLOADING: '#上传失败',
    VideoStatus.UNAVAILABLE: '#不能观看',
    VideoStatus.VIDEO_PRIVATE: '#私享视频',
    VideoStatus.VIDEO_DELETED: '#已删除',
    VideoStatus.ACCOUNT_TERMINATED: '#账号已终止',
    VideoStatus.ACCOUNT_CLOSED: '#账号已关闭',
    VideoStatus.NUDITY_OR_SEXUAL_CONTENT: '#裸露或色情内容',
    VideoStatus.MEMBERS_ONLY: '#会员限定',
    VideoStatus.VIOLATE_TOS: '#违反服务条款'
}
