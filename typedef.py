from enum import Enum
from typing import NamedTuple, Optional

from yt_dlp.utils import YoutubeDLError


class IncompleteTranscodingError(YoutubeDLError):
    msg = 'Transcoding for this video is not yet complete on YouTube servers.'


class VideoTooShortError(RuntimeError):
    pass


class RetryReason(str, Enum):
    LIVE_NOT_STARTED = 'this live has not yet started'
    NETWORK_ERROR = 'a network error occurs when upload this video'
    INCOMPLETE_TRANSCODING = 'transcoding for this video is not yet complete on YouTube servers'


class Task(NamedTuple):
    url: str
    chat_id: Optional[int]
    message_id: Optional[int]


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
