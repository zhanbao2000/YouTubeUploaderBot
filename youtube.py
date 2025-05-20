import re
from io import BytesIO
from pathlib import Path
from subprocess import run, DEVNULL
from time import time
from typing import Optional, Callable

from PIL import Image, ImageDraw, ImageFont
from yt_dlp import YoutubeDL
from yt_dlp.utils import YoutubeDLError

from config import CLIENT_ID, CLIENT_SECRET, DOWNLOAD_ROOT, GCP_APIKEY, REFRESH_TOKEN
from model.channels import Channel, Channels
from model.playlistItems import PlaylistItems
from model.subscriptions import Subscriptions
from model.videos import Videos
from typedef import IncompleteTranscodingError, VideoStatus, VideoTooShortError
from utils import format_date, format_duration_without_unit, format_file_size, get_client, get_proxy_yt_dlp


class Format(object):

    def __init__(self, format_info: dict):
        self.format_id: str = format_info.get('format_id')
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

    def __init__(self, url: str, progress_hooks: list[Callable[[dict], None]] = None):
        self.url = url
        # the default value of progress_hooks passed to yt-dlp is []
        # however, we need to use this trick to avoid issues with mutable default parameters
        self.progress_hooks = progress_hooks or []
        self.video_id = get_video_id(url)
        self.file = Path(f'{DOWNLOAD_ROOT}/{self.video_id}.mp4')

        if not self.video_id:
            raise RuntimeError(f'Cannot get video id from url:\n{url}')

    def _get_base_ydl_options(self) -> dict:
        """get base ydl options"""
        return {
            'player_client': 'web',
            'getpot_bgutil_baseurl': 'http://127.0.0.1:4416',
            'proxy': get_proxy_yt_dlp(),
            'ffmpeg_location': 'bin',
            'concurrent_fragment_downloads': 16,
            'outtmpl': str(self.file),
            'skip_download': True,
            'socket_timeout': 90,
            'progress_hooks': self.progress_hooks,
        }

    def _get_ydl_options_with_cookies(self) -> dict:
        """get ydl options with cookies"""
        # If downloading can be done WITHOUT using cookies,
        # then try NOT to use cookies to avoid them becoming invalid.
        ydl_options = self._get_base_ydl_options()
        ydl_options.update({
            'cookiefile': 'cookies.txt',  # original dump name: cookies.私人.txt
        })
        return ydl_options

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
                raise VideoTooShortError(video_info)

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
            'format_sort': ['lang', 'size:2000M'],
            'merge_output_format': 'mp4',
        })

        with YoutubeDL(ydl_options) as ydl:
            return ydl.extract_info(self.url, download=False)

    def get_video_status(self) -> VideoStatus:
        """get video status from YouTube"""
        try:
            self.get_video_info()
        except YoutubeDLError as e:
            msg = e.msg
            if 'Sign in if you\'ve been granted access to this video' in msg:
                result = VideoStatus.VIDEO_PRIVATE
            elif 'This video is private' in msg:
                result = VideoStatus.VIDEO_PRIVATE
            elif 'This video has been removed by the uploader' in msg:
                result = VideoStatus.VIDEO_DELETED
            elif 'This video is no longer available because the YouTube account associated with this video has been terminated' in msg:
                result = VideoStatus.ACCOUNT_TERMINATED
            elif 'This video is no longer available because the uploader has closed their YouTube account' in msg:
                result = VideoStatus.ACCOUNT_CLOSED
            elif 'This video has been removed for violating YouTube\'s policy on nudity or sexual content' in msg:
                result = VideoStatus.NUDITY_OR_SEXUAL_CONTENT
            elif 'This video has been removed for violating YouTube\'s Terms of Service' in msg:
                result = VideoStatus.VIOLATE_TOS
            elif 'Join this channel to get access to members-only content like this video, and other exclusive perks' in msg:
                result = VideoStatus.MEMBERS_ONLY
            elif 'This video is available to this channel\'s members on level' in msg:
                result = VideoStatus.MEMBERS_ONLY
            elif 'This video is unavailable' in msg:
                result = VideoStatus.UNAVAILABLE
            else:
                result = VideoStatus.UNAVAILABLE
        else:
            result = VideoStatus.AVAILABLE

        return result

    def download_max_size(self, max_size_mb: int, use_cookies: bool = False) -> dict:
        """download the largest video but no bigger than given size"""
        if use_cookies:
            ydl_options = self._get_ydl_options_with_cookies()
        else:
            ydl_options = self._get_base_ydl_options()

        ydl_options.update({
            'format': 'bv+ba/b',
            'format_sort': ['lang', f'size:{max_size_mb or 1}M'],  # at least 1 MB
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
    """generate caption from video info"""
    title = video_info['title']
    url = video_info['webpage_url']
    duration_string = video_info['duration_string']
    uploader = video_info['uploader']
    uploader_url = video_info['uploader_url']
    upload_date = format_date(video_info['upload_date'])
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
    """extract video id from url"""
    match = re.search(r'(?:v=|/)([0-9A-Za-z_-]{11}).*', url)
    return match.group(1) if match else None


def get_playlist_id(url: str) -> Optional[str]:
    """extract playlist id from url"""
    match = re.search(r'(?<=list=)[^&]+', url)
    return match.group(0) if match else None


def get_channel_id(url: str) -> Optional[str]:
    """extract channel id from url"""
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
        if format_info.get('format_note') == 'storyboard':
            continue
        if 'video_ext' in format_info and format_info['video_ext'] != 'none' and 'format_note' in format_info:
            video_formats.append(VideoFormat(format_info))
        elif 'audio_ext' in format_info and format_info['audio_ext'] != 'none' and 'format_note' in format_info:
            audio_formats.append(AudioFormat(format_info))

    return (
        sorted(video_formats, key=lambda x: x.bitrate, reverse=True),
        sorted(audio_formats, key=lambda x: x.bitrate, reverse=True),
    )


async def get_thumbnail(url: str, width: int, height: int) -> BytesIO:
    """download thumbnail of a video, as BytesIO object, convert it to video size"""
    async with get_client() as client:
        r = await client.get(url)
        data = BytesIO(r.content)

    image = Image.open(data)

    if image.mode in ('RGBA', 'LA'):
        background = Image.new('RGB', image.size, (255, 255, 255))
        background.paste(image, mask=image.split()[3] if image.mode == 'RGBA' else image.split()[1])
        image = background
    elif image.mode != 'RGB':
        image = image.convert('RGB')
    # else it is a jpg image, we don't need to convert it

    # STEP 1: crop the image to remove black borders, use 5 as threshold
    gray_image = image.convert('L')
    mask = gray_image.point(lambda x: 255 if x > 5 else 0)
    bbox = mask.getbbox()
    if bbox:
        image = image.crop(bbox)

    # STEP 2: resize the origin image to fit the video width or height
    img_ratio = image.width / image.height
    target_ratio = width / height

    if img_ratio > target_ratio:
        new_width = width
        new_height = int(width / img_ratio)
    else:
        new_height = height
        new_width = int(height * img_ratio)

    # STEP 3: paste the resized image a black background with the same size as the video
    image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
    offset_x = (width - new_width) // 2
    offset_y = (height - new_height) // 2
    target_image = Image.new('RGB', (width, height), (0, 0, 0))  # black background
    target_image.paste(image, (offset_x, offset_y))

    jpeg_data = BytesIO()
    target_image.save(jpeg_data, 'JPEG', quality=90)

    return jpeg_data


def get_captures(video: Path, video_info: dict) -> BytesIO:
    """generate video captures (4x3) from video file"""
    temp_path = Path(DOWNLOAD_ROOT)

    interval = video_info['duration'] / 13
    ratio = video_info['width'] / video_info['height']
    target_frame_size = (480, int(480 / ratio))  # single frame size
    grid_width = target_frame_size[0] * 4  # final grid width
    grid_height = target_frame_size[1] * 3  # final grid height
    grid_image = Image.new('RGB', (grid_width, grid_height))
    font = ImageFont.load_default(size=40)
    result = BytesIO()

    for index in range(12):
        # generate frames
        time_position = int((index + 1) * interval)
        timestamp = format_duration_without_unit(time_position)
        output_file = temp_path / f'frame_{index:02d}.png'

        run([
            'bin/ffmpeg',
            '-ss', str(time_position),
            '-i', str(video),
            '-vframes', '1',
            '-q:v', '2',
            '-y',
            str(output_file)
        ], check=True, stdout=DEVNULL, stderr=DEVNULL)

        # paste resized frame to grid image
        image = Image.open(output_file).resize(target_frame_size)

        draw = ImageDraw.Draw(image)
        draw.text((8, 5), timestamp, fill=(255, 255, 255, 255), font=font, stroke_width=1, stroke_fill=(0, 0, 0, 255))

        row, col = index // 4, index % 4
        x = col * target_frame_size[0]
        y = row * target_frame_size[1]
        grid_image.paste(image, (x, y))

    grid_image.save(result, format='PNG')

    for file in temp_path.glob('frame_*.png'):
        file.unlink(missing_ok=True)

    return result


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

                if filter_str.lower() in playlist_item.snippet.title.lower():
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

            for item in subscriptions.items:
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


async def get_channel_id_by_link(url: str) -> Optional[str]:
    """extract channel id from webpage"""
    async with get_client(follow_redirects=True) as client:
        r = await client.get(url)

        if r.status_code != 200:
            return None

    match = re.search(r'(https?://www\.youtube\.com/channel/([a-zA-Z0-9_-]{24}))', r.text)
    return get_channel_id(match.group(0)) if match else None


access_token_provider = YouTubeOAuth20Provider()
