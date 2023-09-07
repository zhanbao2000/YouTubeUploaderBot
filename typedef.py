from enum import Enum
from typing import NamedTuple


class RetryReason(str, Enum):
    LIVE_NOT_STARTED = 'this live has not yet started'
    NETWORK_ERROR = 'a network error occurs when upload this video'
    INCONCLUSIVE_FORMAT = 'this video currently only contains inconclusive formats'


class Task(NamedTuple):
    url: str
    chat_id: int
    message_id: int
