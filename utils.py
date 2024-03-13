from collections import deque
from os import getpid
from platform import system
from re import sub, search
from time import time
from typing import Optional, TypeVar, Generator

from httpx import AsyncClient, AsyncHTTPTransport, Request
from psutil import Process
from pyrogram import filters
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message, MessageEntity

from config import SUPERUSERS, PROXY

T = TypeVar('T')


class APIUsageCounter:
    def __init__(self):
        self.timestamps = deque()

    def add_timestamp(self):
        self.timestamps.append(time())
        # Perform cleanup every 1000 timestamps
        if len(self.timestamps) % 1000 == 0:
            self._cleanup()

    def count_last_minute(self):
        return self._count_last_seconds(60)

    def count_last_hour(self):
        return self._count_last_seconds(60 * 60)

    def count_last_day(self):
        return self._count_last_seconds(60 * 60 * 24)

    def _count_last_seconds(self, seconds):
        cutoff = time() - seconds
        return sum(1 for ts in self.timestamps if ts >= cutoff)

    def _cleanup(self):
        cutoff = time() - 60 * 60 * 24  # One day ago
        while self.timestamps and self.timestamps[0] < cutoff:
            self.timestamps.popleft()


def convert_date(date: str) -> str:
    """convert date string to human-readable format"""
    return f'{date[:4]}年{date[4:6]}月{date[6:8]}日'


def create_message_link(chat_id: int, message_id: int) -> str:
    """create a link to a message in a channel"""
    channel_id = str(chat_id).removeprefix('-100')
    return f'https://t.me/c/{channel_id}/{message_id}'


def create_video_link(video_id: str) -> str:
    """create a video link according to video id"""
    return f'https://www.youtube.com/watch?v={video_id}'


def format_file_size(byte: int) -> str:
    """convert file size to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if byte < 1024:
            return f'{byte:.2f}{unit}'
        byte /= 1024
    return f'{byte:.2f}PB'


def escape_color(text: str) -> str:
    """escape color codes"""
    return sub(r'\x1b\[[0-9;]*m', '', text)


def slide_window(lst: list[T], window_size: int) -> Generator[list[T], None, None]:
    """generator function that yields window_size elements at a time"""
    for start in range(0, len(lst), window_size):
        yield lst[start:start + window_size]


async def on_googleapi_call(request: Request):
    if request.url.host.endswith('googleapis.com'):
        counter.add_timestamp()


def get_client(proxies: Optional[str] = None, timeout: float = 15, retries: int = 5, **kwargs) -> AsyncClient:
    """get a httpx client"""
    return AsyncClient(
        event_hooks={'request': [on_googleapi_call]},
        proxies=proxies or PROXY,
        timeout=timeout,
        transport=AsyncHTTPTransport(retries=retries) if retries else None,
        **kwargs
    )


def get_args(message: Message) -> str:
    """get arguments from a message"""
    _, *args = message.command
    return ''.join(args)


def offset_text_link_entities(entities: list[MessageEntity], offset: int) -> list[MessageEntity]:
    """offset message entities"""
    for entity in entities:
        if entity.type is MessageEntityType.TEXT_LINK:
            entity.offset += offset
    return entities


def escape_hashtag(caption: str) -> str:
    """delete hashtags from caption"""
    title_index = caption.find('标题')
    if title_index != -1:
        return caption[title_index:]
    return caption


def get_memory_usage():
    """get memory usage, using psutil library"""
    process = Process(getpid())
    return process.memory_info()


def get_swap_usage() -> int:
    """get swap usage, using smaps"""
    if system() != 'Linux':
        return 0

    with open(f'/proc/{getpid()}/smaps', 'r') as file:
        # awk '/^Swap:/ {SWAP+=$2}END{print SWAP" KB"}' /proc/{pid}/smaps
        swap = sum(
            int(search(r'\d+', line).group())
            for line in file
            if line.startswith('Swap:')
        )

    return swap * 1024


is_superuser = filters.chat(SUPERUSERS)
counter = APIUsageCounter()
