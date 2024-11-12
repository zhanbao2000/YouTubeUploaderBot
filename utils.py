from collections import deque
from datetime import datetime
from os import getpid
from platform import system
from re import sub, search
from time import time, localtime, strftime
from typing import Optional, TypeVar, Generator

from httpx import AsyncClient, AsyncHTTPTransport, Request
from psutil import Process
from pyrogram import filters
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message, MessageEntity

from config import SUPERUSERS, PROXY_HTTPX
from typedef import Channel

T = TypeVar('T')
START_TIME = time()


class APIUsageCounter(object):
    def __init__(self):
        self.timestamps = deque()

    def add_timestamp(self):
        self.timestamps.append(time())
        # clean up timestamps every 1000 requests
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


def create_message_link(chat_id: int, message_id: int) -> str:
    """create a Telegram message link to a message in a channel"""
    channel_id = str(chat_id).removeprefix('-100')
    return f'https://t.me/c/{channel_id}/{message_id}'


def create_video_link(video_id: str) -> str:
    """create a YouTube video link according to video id"""
    return f'https://www.youtube.com/watch?v={video_id}'


def create_video_link_markdown(video_id: str, title: str = '') -> str:
    """create a YouTube video link in Markdown format"""
    return f'[{title or video_id}]({create_video_link(video_id)})'


def create_channel_link_markdown(channel: Channel) -> str:
    """create a YouTube channel link in Markdown format"""
    return f'[{channel.name}]({channel.url})'


def now_datetime() -> str:
    """return current time and date, format: 2020-02-20 11:45:14"""
    time_local = localtime(time())
    return strftime('%Y-%m-%d %H:%M:%S', time_local)


def get_uptime() -> str:
    """return uptime of the bot"""
    duration = int(time() - START_TIME)
    return format_duration(duration)


def format_date(date: str) -> str:
    """convert date string to human-readable format, 20200220 -> 2020年02月20日"""
    return f'{date[:4]}年{date[4:6]}月{date[6:8]}日'


def format_file_size(byte: int) -> str:
    """convert file size to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if byte < 1024:
            return f'{byte:.2f}{unit}'
        byte /= 1024
    return f'{byte:.2f}PB'


def format_duration(duration: int) -> str:
    """convert duration to human-readable format"""
    years, duration = divmod(duration, 60 * 60 * 24 * 365)
    days, duration = divmod(duration, 60 * 60 * 24)
    hours, duration = divmod(duration, 60 * 60)
    minutes, seconds = divmod(duration, 60)

    if years > 0:
        return f'{years}年{days}天{hours}小时{minutes}分钟{seconds}秒'
    elif days > 0:
        return f'{days}天{hours}小时{minutes}分钟{seconds}秒'
    elif hours > 0:
        return f'{hours}小时{minutes}分钟{seconds}秒'
    elif minutes > 0:
        return f'{minutes}分钟{seconds}秒'
    else:
        return f'{seconds}秒'


def remove_color_codes(text: str) -> str:
    """remove color codes"""
    return sub(r'\x1b\[[0-9;]*m', '', text)


def batched(lst: list[T], n: int) -> Generator[list[T], None, None]:
    """
    generator function that yields n elements at a time

    roughly equivalent to ``itertools.batched()``, which is available in Python 3.12
    """
    for start in range(0, len(lst), n):
        yield lst[start:start + n]


async def on_googleapi_call(request: Request):
    """hook function that records the timestamp of a Google API call"""
    if request.url.host.endswith('googleapis.com'):
        counter.add_timestamp()


def get_client(proxies: Optional[str] = None, timeout: float = 15, retries: int = 5, **kwargs) -> AsyncClient:
    """get a httpx client"""
    return AsyncClient(
        event_hooks={'request': [on_googleapi_call]},
        proxies=proxies or PROXY_HTTPX,
        timeout=timeout,
        transport=AsyncHTTPTransport(retries=retries) if retries else None,
        **kwargs
    )


def get_args(message: Message) -> str:
    """get arguments from a message"""
    _, *args = message.command
    return ''.join(args)


def offset_text_link_entities(entities: list[MessageEntity], offset: int) -> list[MessageEntity]:
    """offset text link entities by a certain amount of characters"""
    for entity in entities:
        if entity.type is MessageEntityType.TEXT_LINK:
            entity.offset += offset
    return entities


def remove_hashtags_from_caption(caption: str) -> str:
    """remove hashtags from caption"""
    title_index = caption.find('标题')
    if title_index != -1:
        return caption[title_index:]
    return caption


def find_channel_in_message(message: Message) -> Channel:
    """find channel name and URL in a message"""
    # do NOT use str.find() because some of the characters in message.caption may be emojis (UTF-16-LE), which have a length more than 1
    match = search(r'\n频道：(.*?)\n时长', message.caption)

    for entity in message.caption_entities:
        text = message.caption[entity.offset:entity.offset + entity.length]
        if text == match.group(1) and entity.type is MessageEntityType.TEXT_LINK:
            return Channel(text, entity.url)


def get_next_retry_ts(error_message: str) -> float:
    """get next retry timestamp from error message"""
    match = search(r'in (\d+) (minute|hour|day|year)', error_message)

    if match:
        amount, unit = match.groups()
        if unit == 'minute':
            return time() + int(amount) * 60
        elif unit == 'hour':
            return time() + int(amount) * 60 * 60
        elif unit == 'day':
            return time() + int(amount) * 24 * 60 * 60
        elif unit == 'year':
            return time() + int(amount) * 365 * 24 * 60 * 60
    elif 'in a few moments' in error_message:
        return time() + 24 * 60 * 60

    return time()


def is_ready(timestamp: float) -> bool:
    """check if the task is ready to retry"""
    return timestamp <= time()


def get_memory_usage():
    """get memory usage, using psutil library"""
    process = Process(getpid())
    return process.memory_info()


def get_swap_usage() -> int:
    """get swap usage, using smaps, if system is not Linux, return 0"""
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


def parse_upload_timestamp(video_info: dict) -> datetime:
    """parse upload timestamp to datetime object"""
    if video_info.get('release_timestamp') is not None:
        return datetime.fromtimestamp(video_info['release_timestamp'])
    elif video_info.get('release_date') is not None:
        return datetime.strptime(video_info['release_date'], '%Y%m%d')
    elif video_info.get('upload_date') is not None:
        return datetime.strptime(video_info['upload_date'], '%Y%m%d')
    raise ValueError('No upload timestamp found in video_info')


def join_list(separator: list[T], *lists: list[T]) -> Generator[T, None, None]:
    """
    join multiple lists with a separator

    Example:
      sep=[x, y], *lists=[a, b], [c], [d, e]
      => [a, b, x, y, c, x, y, d, e]
    """
    length = len(lists)
    for index, lst in enumerate(lists, start=1):
        if not lst:  # skip empty list
            continue
        yield from lst
        if index != length:  # do not add separator after the last list
            yield from separator


is_superuser = filters.chat(SUPERUSERS)
counter = APIUsageCounter()
