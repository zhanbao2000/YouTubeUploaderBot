import re
from io import BytesIO
from pathlib import Path
from typing import Optional

from yt_dlp import YoutubeDL

from config import DOWNLOAD_ROOT, GCP_APIKEY
from utils import format_file_size, convert_date, get_client, escape_markdown


class Format(object):

    def __init__(self, info: dict):
        self.format_id: int = int(info.get('format_id'))
        self.format_note: str = info.get('format_note')
        self.filesize: int = info.get('filesize') or info.get('filesize_approx') or 0
        self.bitrate: float = info.get('vbr') or info.get('tbr') or info.get('abr') or 0


class VideoFormat(Format):

    def __init__(self, info: dict):
        super().__init__(info)
        self.extension: str = info.get('video_ext')
        self.resolution: str = info.get('resolution')
        self.fps: float = info.get('fps') or 0

    def __str__(self) -> str:
        return (
            f'(Video id={self.format_id} {self.format_note})'
            f' {self.extension}'
            f' {self.resolution} {int(self.fps)}fps {int(self.bitrate)}kbps'
            f' {format_file_size(self.filesize)}'
        )


class AudioFormat(Format):

    def __init__(self, info: dict):
        super().__init__(info)
        self.extension: str = info.get('audio_ext')

    def __str__(self) -> str:
        return (
            f'(Audio id={self.format_id} {self.format_note})'
            f' {self.extension}'
            f' {int(self.bitrate)}kbps {format_file_size(self.filesize)}'
        )


class DownloadManager(object):

    def __init__(self, url: str):
        self.url = url
        self.video_id = get_video_id(url)
        self.file = Path(f'{DOWNLOAD_ROOT}/{self.video_id}.mp4')

        if not self.video_id:
            raise RuntimeError(f'Cannot get video id from url:\n{url}')

    def _get_base_ydl_options(self) -> dict:
        """get base ydl options"""
        return {
            'ffmpeg_location': 'bin',
            'concurrent_fragment_downloads': 16,
            'outtmpl': str(self.file),
            'skip_download': True,
            'socket_timeout': 90
        }

    def _download(self, ydl_options: dict) -> dict:
        """download the video and return the info dict"""
        with YoutubeDL(ydl_options) as ydl:
            info_dict = ydl.extract_info(self.url, download=False)
            print_formats_list(info_dict)

            ydl.params.update({
                'skip_download': False,
            })
            ydl.download([self.url])

            return info_dict

    def get_video_info(self) -> dict:
        """get video info directly without download the video"""
        ydl_options = self._get_base_ydl_options()
        ydl_options.update({
            'format': 'bv+ba/b',
            'format_sort': ['size:2000M'],
            'merge_output_format': 'mp4',
        })

        with YoutubeDL(ydl_options) as ydl:
            return ydl.extract_info(self.url, download=False)

    def download_max_size_2000mb(self) -> dict:
        """download the largest video but no bigger than 2000 MB"""
        ydl_options = self._get_base_ydl_options()
        ydl_options.update({
            'format': 'bv+ba/b',
            'format_sort': ['size:2000M'],
            'merge_output_format': 'mp4',
        })

        return self._download(ydl_options)

    def download_max_size_1600mb(self) -> dict:
        """download the largest video but no bigger than 1600 MB"""
        ydl_options = self._get_base_ydl_options()
        ydl_options.update({
            'format_sort': ['size:1600M'],
            'merge_output_format': 'mp4',
        })

        return self._download(ydl_options)


def get_video_caption(video_info: dict) -> str:
    """get the caption generated from video info"""

    # This text will be parsed as ParseMode.MARKDOWN_V2 directly,
    # so we must first escape any Markdown characters in title, uploader, etc.
    # Carefully add new characters to this text, make sure they are safe.
    # If you do want to add Markdown tags, please understand what you are doing.

    title = escape_markdown(video_info['title'])
    url = video_info['webpage_url']
    duration_string = escape_markdown(video_info['duration_string'])
    uploader = escape_markdown(video_info['uploader'])
    uploader_url = video_info['uploader_url']
    upload_date = convert_date(video_info['upload_date'])
    resolution = escape_markdown(video_info['resolution'])
    fps = escape_markdown(video_info['fps'])
    vcodec = escape_markdown(video_info['vcodec'].split('.', 1)[0])
    acodec = escape_markdown(video_info['acodec'].split('.', 1)[0])
    asr = int(video_info['asr'] or 0)
    vbr = int(video_info['vbr'] or 0)
    abr = int(video_info['abr'] or 0)

    return (f'标题：[{title}]({url})\n'
            f'频道：[{uploader}]({uploader_url})\n'
            f'时长：{duration_string}\n'
            f'视频质量：{resolution} {fps}fps {vbr}kbps {vcodec}\n'
            f'音频质量：{asr}KHz {abr}kbps {acodec}\n'
            f'上传时间：{upload_date}')


def get_video_id(url: str) -> Optional[str]:
    """get video id from url"""
    match = re.search(r'(?:v=|/)([0-9A-Za-z_-]{11}).*', url)
    return match.group(1) if match else None


def get_playlist_id(url: str) -> Optional[str]:
    """get playlist id from url"""
    match = re.search(r'(?<=list=)[^&]+', url)
    return match.group(0) if match else None


def print_formats_list(info_dict: dict) -> None:
    """print the formats list of the video"""
    video_formats, audio_formats = get_formats_list(info_dict['formats'])

    print('Video:')
    for video_format in video_formats:
        print(video_format)

    print('Audio:')
    for audio_format in audio_formats:
        print(audio_format)


def get_formats_list(formats: list[dict]) -> tuple[list[VideoFormat], list[AudioFormat]]:
    """get sorted formats of video and audio (by bitrate, descending)"""
    audio_formats = []
    video_formats = []

    for info in formats:
        if not info.get('format_id').isdigit():  # storyboards
            continue
        if 'video_ext' in info and info['video_ext'] != 'none' and 'format_note' in info:
            video_formats.append(VideoFormat(info))
        elif 'audio_ext' in info and info['audio_ext'] != 'none' and 'format_note' in info:
            audio_formats.append(AudioFormat(info))

    return (
        sorted(video_formats, key=lambda x: x.bitrate, reverse=True),
        sorted(audio_formats, key=lambda x: x.bitrate, reverse=True),
    )


async def get_thumbnail(url: str) -> BytesIO:
    """get thumbnail of a video"""
    async with get_client() as client:
        r = await client.get(url)
        return BytesIO(r.content)


async def is_video_available_online(video_id: str) -> bool:
    """check if a video is available"""
    video_params = {
        'part': 'snippet,statistics',
        'id': video_id,
        'key': GCP_APIKEY
    }
    async with get_client() as client:
        resp = await client.get('https://youtube.googleapis.com/youtube/v3/videos', params=video_params)
        video_info = resp.json()

    return 'items' in video_info and len(video_info['items']) > 0


async def get_all_video_urls_from_playlist(playlist_id) -> list[str]:
    """get all video urls from a playlist"""
    api_url = 'https://www.googleapis.com/youtube/v3/playlistItems'
    params = {
        'part': 'snippet',
        'playlistId': playlist_id,
        'maxResults': 50,
        'key': GCP_APIKEY,
    }

    result = []

    async with get_client() as client:
        while True:
            r = await client.get(api_url, params=params)
            resp_dict = r.json()
            items = resp_dict.get('items', [])

            for item in items:
                video_id = item['snippet']['resourceId']['videoId']
                result.append(f'https://www.youtube.com/watch?v={video_id}')

            if 'nextPageToken' in resp_dict:
                params['pageToken'] = resp_dict['nextPageToken']
            else:
                break

    return result
