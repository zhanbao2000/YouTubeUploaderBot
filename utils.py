from re import sub
from typing import Optional

from httpx import AsyncClient, AsyncHTTPTransport

from config import SUPERUSERS


def convert_date(date: str) -> str:
    """convert date string to human-readable format"""
    return f'{date[:4]}年{date[4:6]}月{date[6:8]}日'


def create_message_link(chat_id: int, message_id: int) -> str:
    """create a link to a message in a channel"""
    channel_id = str(chat_id).removeprefix('-100')
    return f'https://t.me/c/{channel_id}/{message_id}'


def format_file_size(byte: int) -> str:
    """convert file size to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if byte < 1024:
            return f'{byte:.2f}{unit}'
        byte /= 1024
    return f'{byte:.2f}PB'


def escape_markdown(text: str) -> str:
    """escape markdown characters"""
    escape_chars = r'\*_\[\]()~`>#+-=|{}.!'
    return sub(r'([' + escape_chars + '])', r'\\\1', str(text))


def escape_color(text: str) -> str:
    """escape color codes"""
    return sub(r'\x1b\[[0-9;]*m', '', text)


def is_superuser(chat_id: int) -> bool:
    """check if a user is a superuser"""
    return chat_id in SUPERUSERS


def get_client(proxies: Optional[str] = None, timeout: float = 15, retries: int = 5, **kwargs) -> AsyncClient:
    """get a httpx client"""
    return AsyncClient(
        proxies=proxies,
        timeout=timeout,
        transport=AsyncHTTPTransport(retries=retries) if retries else None,
        **kwargs
    )
