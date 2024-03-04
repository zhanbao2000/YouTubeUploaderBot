import re
from io import BytesIO
from pathlib import Path
from time import time
from typing import Optional

from yt_dlp import YoutubeDL
from yt_dlp.utils import YoutubeDLError

from config import PROXY, DOWNLOAD_ROOT, GCP_APIKEY, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN
from model.channels import Channels, Channel
from model.playlistItems import PlaylistItems
from model.subscriptions import Subscriptions
from model.videos import Videos
from typedef import VideoStatus, IncompleteTranscodingError
from utils import format_file_size, convert_date, get_client, create_video_link


class Format(object):

    def __init__(self, format_info: dict):
        self.format_id: int = int(format_info.get('format_id'))
        self.format_note: str = format_info.get('format_note')
        self.filesize: int = format_info.get('filesize') or format_info.get('filesize_approx') or 0
        self.bitrate: float = format_info.get('vbr') or format_info.get('tbr') or format_info.get('abr') or 0


class VideoFormat(Format):

    def __init__(self, format_info: dict):
        super().__init__(format_info)
        self.extension: str = format_info.get('video_ext')
        self.resolution: str = format_info.get('resolution')
        self.fps: float = format_info.get('fps') or 0

    def __str__(self) -> str:
        return (
            f'(Video id={self.format_id} {self.format_note})'
            f' {self.extension}'
            f' {self.resolution} {int(self.fps)}fps {int(self.bitrate)}kbps'
            f' {format_file_size(self.filesize)}'
        )


class AudioFormat(Format):

    def __init__(self, format_info: dict):
        super().__init__(format_info)
        self.extension: str = format_info.get('audio_ext')

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
            'proxy': PROXY,
            'ffmpeg_location': 'bin',
            'concurrent_fragment_downloads': 16,
            'outtmpl': str(self.file),
            'skip_download': True,
            'socket_timeout': 90
        }

    def _download(self, ydl_options: dict) -> dict:
        """download the video and return the video info"""
        with YoutubeDL(ydl_options) as ydl:
            video_info = ydl.extract_info(self.url, download=False)
            video_formats, audio_formats = get_formats_list(video_info['formats'])

            # check if all formats have filesize == 0
            if (
                    all(format_.filesize == 0 for format_ in video_formats) or
                    all(format_.filesize == 0 for format_ in audio_formats)
            ):
                raise IncompleteTranscodingError

            # check if duration less than minimum duration
            if video_info['duration'] <= 300:  # 5 min
                raise RuntimeError(f'Too small video, duration: {video_info["duration"]}')

            ydl.params.update({
                'skip_download': False,
            })
            ydl.download([self.url])

            return video_info

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

    def get_video_status(self) -> VideoStatus:
        """get remote video status"""
        try:
            self.get_video_info()
        except YoutubeDLError as e:
            msg = e.msg
            if 'Sign in if you\'ve been granted access to this video' in msg:
                result = VideoStatus.VIDEO_PRIVATE
            elif 'This video is private. If the owner of this video has granted you access, please sign in' in msg:
                result = VideoStatus.VIDEO_PRIVATE
            elif 'This video has been removed by the uploader' in msg:
                result = VideoStatus.VIDEO_DELETED
            elif 'This video is no longer available because the YouTube account associated with this video has been terminated' in msg:
                result = VideoStatus.ACCOUNT_TERMINATED
            # elif '' in msg:
            #     result = VideoStatus.ACCOUNT_CLOSED
            elif 'This video has been removed for violating YouTube\'s policy on nudity or sexual content' in msg:
                result = VideoStatus.NUDITY_OR_SEXUAL_CONTENT
            elif 'Join this channel to get access to members-only content like this video, and other exclusive perks' in msg:
                result = VideoStatus.MEMBERS_ONLY
            elif 'This video is unavailable' in msg:
                result = VideoStatus.UNAVAILABLE
            elif 'Video unavailable' in msg:
                result = VideoStatus.UNAVAILABLE
            else:
                result = VideoStatus.UNAVAILABLE
        else:
            result = VideoStatus.AVAILABLE

        return result

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


class YouTubeOAuth20Provider(object):

    def __init__(self):
        self.access_token = ''
        self.expire_at = 0.

    async def refresh_token(self) -> None:
        """refresh OAuth 2.0 token"""
        params = {
            'grant_type': 'refresh_token',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'refresh_token': REFRESH_TOKEN
        }
        async with get_client() as client:
            resp = await client.post('https://oauth2.googleapis.com/token', data=params)
            resp_dict = resp.json()

        self.access_token = resp_dict['access_token']
        self.expire_at = time() + resp_dict['expires_in']

    async def get_access_token_safe(self) -> str:
        """get access token ensure it is available"""
        if not self.access_token or time() >= self.expire_at:
            await self.refresh_token()
        return self.access_token


def get_video_caption(video_info: dict) -> str:
    """get the caption generated from video info"""
    title = video_info['title']
    url = video_info['webpage_url']
    duration_string = video_info['duration_string']
    uploader = video_info['uploader']
    uploader_url = video_info['uploader_url']
    upload_date = convert_date(video_info['upload_date'])
    resolution = video_info['resolution']
    fps = video_info['fps']
    vcodec = video_info['vcodec'].split('.', 1)[0]
    acodec = video_info['acodec'].split('.', 1)[0]
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


def get_channel_id(url: str) -> Optional[str]:
    """get channel id from url"""
    match = re.search(r'(?<=channel/)[^/]+', url)
    return match.group(0) if match else None


def print_formats_list(video_info: dict) -> None:
    """print the formats list of the video"""
    video_formats, audio_formats = get_formats_list(video_info['formats'])

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

    for format_info in formats:
        if not format_info.get('format_id').isdigit():  # storyboards
            continue
        if 'video_ext' in format_info and format_info['video_ext'] != 'none' and 'format_note' in format_info:
            video_formats.append(VideoFormat(format_info))
        elif 'audio_ext' in format_info and format_info['audio_ext'] != 'none' and 'format_note' in format_info:
            audio_formats.append(AudioFormat(format_info))

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
    params = {
        'part': 'snippet,statistics',
        'id': video_id,
        'key': GCP_APIKEY
    }
    async with get_client() as client:
        resp = await client.get('https://youtube.googleapis.com/youtube/v3/videos', params=params)
        videos = Videos(**resp.json())

    return len(videos.items) > 0


async def is_video_available_online_batch(video_ids: list[str]) -> dict[str, bool]:
    """check if a list of videos are available"""
    if len(video_ids) > 50:
        raise ValueError('The number of video IDs should not exceed 50.')

    params = {
        'part': 'snippet,statistics',
        'id': ','.join(video_ids),
        'key': GCP_APIKEY
    }
    async with get_client() as client:
        resp = await client.get('https://youtube.googleapis.com/youtube/v3/videos', params=params)
        videos = Videos(**resp.json())

    result = {video_id: False for video_id in video_ids}
    for video in videos.items:
        result[video.id] = True

    return result


async def get_all_video_urls_from_playlist(playlist_id: str, filter_str: str = '', limit: int = 0) -> list[str]:
    """get all video urls from a playlist"""
    params = {
        'part': 'snippet',
        'playlistId': playlist_id,
        'maxResults': 50,
        'key': GCP_APIKEY,
    }

    result = []

    async with get_client() as client:
        while True:
            resp = await client.get('https://www.googleapis.com/youtube/v3/playlistItems', params=params)
            playlist_items = PlaylistItems(**resp.json())

            for playlist_item in playlist_items.items:
                video_id = playlist_item.snippet.resourceId.videoId

                if filter_str in playlist_item.snippet.title:
                    result.append(f'https://www.youtube.com/watch?v={video_id}')

                if limit and len(result) >= limit:
                    return result

            if playlist_items.nextPageToken and not limit:  # with limit != 0 will ignore next page
                params['pageToken'] = playlist_items.nextPageToken
            else:
                break

    return result


async def get_all_my_subscription_channel_ids() -> list[str]:
    """get all channel ids of my subscriptions"""
    params = {
        'part': 'snippet',
        'mine': True,
        'maxResults': 50,
        'key': GCP_APIKEY,
    }
    headers = {
        'Authorization': f'Bearer {await access_token_provider.get_access_token_safe()}',
        'Accept': 'application/json',
    }

    result = []

    async with get_client() as client:
        while True:
            resp = await client.get('https://www.googleapis.com/youtube/v3/subscriptions', params=params, headers=headers)
            subscriptions = Subscriptions(**resp.json())

            items = subscriptions.items

            for item in items:
                channel_id = item.snippet.resourceId.channelId
                result.append(channel_id)

            if subscriptions.nextPageToken:
                params['pageToken'] = subscriptions.nextPageToken
            else:
                break

    return result


async def get_channel_info(channel_id: str) -> Optional[Channel]:
    """get channel info"""
    params = {
        'part': 'snippet,contentDetails,statistics',
        'id': channel_id,
        'key': GCP_APIKEY
    }
    async with get_client() as client:
        resp = await client.get('https://www.googleapis.com/youtube/v3/channels', params=params)
        channels = Channels(**resp.json())

    if len(channels.items) > 0:
        return channels.items[0]
    return None


async def get_channel_uploads_playlist_id(channel_id: str) -> str:
    """get the playlist consisting of all the videos uploaded by the channel"""
    params = {
        'part': 'snippet,contentDetails,statistics',
        'id': channel_id,
        'key': GCP_APIKEY
    }
    async with get_client() as client:
        resp = await client.get('https://www.googleapis.com/youtube/v3/channels', params=params)
        channels = Channels(**resp.json())

    if len(channels.items) > 0:
        return channels.items[0].contentDetails.relatedPlaylists.uploads
    return ''


async def get_channel_uploads_playlist_id_batch(channel_ids: list[str]) -> dict[str, str]:
    """get the playlist consisting of all the videos uploaded by the channel"""
    if len(channel_ids) > 50:
        raise ValueError('The number of channel IDs should not exceed 50.')

    params = {
        'part': 'snippet,contentDetails,statistics',
        'id': ','.join(channel_ids),
        'maxResults': 50,
        'key': GCP_APIKEY
    }
    async with get_client() as client:
        resp = await client.get('https://www.googleapis.com/youtube/v3/channels', params=params)
        channels = Channels(**resp.json())

    return {
        channel.id: channel.contentDetails.relatedPlaylists.uploads
        for channel in channels.items
    }


async def get_all_stream_urls_from_holoinfo(limit: int = 100, max_upcoming_hours: int = -1) -> list[str]:
    """get all video urls from holoinfo"""
    params = {
        'status': 'past',
        'type': 'stream',
        'topic': 'asmr',
        'limit': limit,
        'max_upcoming_hours': max_upcoming_hours,
    }
    headers = {
        'User-Agent': 'YouTubeUploaderBot/1.0 (contact: https://github.com/zhanbao2000/YouTubeUploaderBot)',
        'Sec-Fetch-Site': 'same-origin'
    }

    async with get_client() as client:
        resp = await client.get('https://holoinfo.me/dex/videos', headers=headers, params=params)

        if resp.status_code != 200:
            return []

    return [create_video_link(video_info['id']) for video_info in resp.json()]


async def get_channel_id_by_link(url: str) -> Optional[str]:
    """get channel id from webpage"""
    async with get_client(follow_redirects=True) as client:
        r = await client.get(url)

        if r.status_code != 200:
            return None

    match = re.search(r'(https?://www\.youtube\.com/channel/([a-zA-Z0-9_-]{24}))', r.text)
    return get_channel_id(match.group(0)) if match else None


access_token_provider = YouTubeOAuth20Provider()
