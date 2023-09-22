from asyncio import Queue, QueueEmpty, AbstractEventLoop
from pathlib import Path
from traceback import format_exc
from typing import Iterable, Optional

from aiogram import Bot
from aiogram.types import Message, ParseMode
from aiogram.types.base import Integer
from yt_dlp.utils import YoutubeDLError

from config import DOWNLOAD_ROOT, CHAT_ID
from database import insert_uploaded, is_in_database, get_upload_message_id, update_available, is_available, get_all_video_ids
from typedef import Task, RetryReason
from utils import format_file_size, create_message_link, escape_color, slide_window
from youtube import DownloadManager, get_video_caption, get_video_id, get_thumbnail, is_video_available_online_batch


class VideoWorker(object):
    def __init__(self, loop: AbstractEventLoop, bot: Bot):
        self.is_working = False
        self.loop = loop
        self.bot = bot
        self.video_queue: Queue[Task] = Queue()
        self.current_task = Optional[Task]
        self.current_running_transfer_files = 0  # total files transferred from startup
        self.current_running_transfer_size = 0  # total size transferred from startup
        self.current_running_retry_list: set[str] = set()  # save links with download error

    def get_pending_tasks_count(self) -> int:
        """return pending tasks = waiting + current"""
        return self.video_queue.qsize() + self.is_working

    def get_queue_size(self) -> int:
        """return waiting tasks = queue size"""
        return self.video_queue.qsize()

    async def add_task(self, task: Task) -> None:
        """add a new task"""
        await self.video_queue.put(task)

    async def add_task_batch(self, urls: Iterable[str], chat_id: Integer, message_id: Integer) -> int:
        """add many new tasks"""
        count_task_added = 0
        for url in urls:

            if is_in_database(get_video_id(url)):
                continue

            await self.video_queue.put(Task(url, chat_id, message_id))
            count_task_added += 1

        return count_task_added

    async def reply(self, text: str, **kwargs) -> Message:
        """reply to the message which triggered current task"""
        return await self.bot.send_message(
            chat_id=self.current_task.chat_id,
            reply_to_message_id=self.current_task.message_id,
            text=text,
            **kwargs
        )

    async def clear_download_folder(self) -> None:
        """clear the download folder and reply to the user"""
        for file in Path(DOWNLOAD_ROOT).glob('*'):
            file.unlink()
            await self.reply(f'deleted {file.name}')

    async def clear_queue(self) -> None:
        """immediately clear all waiting tasks, will not cancel current task"""
        while not self.video_queue.empty():
            try:
                self.video_queue.get_nowait()
            except QueueEmpty:
                continue

    async def reply_duplicate(self, video_id: str) -> None:
        """inform the user that this video had been uploaded"""
        if video_message_id := get_upload_message_id(video_id):  # video_message_id == 0
            await self.reply(f'this video had been [uploaded]({create_message_link(CHAT_ID, video_message_id)})', parse_mode='Markdown')
        else:  # video_message_id != 0
            await self.reply('this video used to be tried to upload, but failed')

    async def reply_task_done(self) -> None:
        """inform the user that this task had been done"""
        if self.get_queue_size():
            await self.reply(f'task finished\npending task(s): {self.get_queue_size()}')
        else:
            await self.reply('all tasks finished')

    async def download_video(self, dm: DownloadManager) -> dict:
        """download the video, return the video info"""
        video_info = await self.loop.run_in_executor(None, dm.download_max_size_2000mb)

        if (filesize := dm.file.stat().st_size) >= 2e9:
            dm.file.unlink()
            await self.reply(f'file too big: {format_file_size(filesize)}\ntry downloading smaller format')
            video_info = await self.loop.run_in_executor(None, dm.download_max_size_1600mb)
            # if this format is still too big, the video_id will be recorded in db with message_id = 0

        return video_info

    async def upload_video(self, file: Path, video_info: dict) -> Message:
        """upload the video and its thumbnail"""
        with open(file, 'rb') as video:
            video_message = await self.bot.send_video(
                chat_id=CHAT_ID, video=video,
                supports_streaming=True, duration=video_info['duration'],
                width=video_info['width'], height=video_info['height']
            )
            await self.bot.send_photo(
                chat_id=CHAT_ID, photo=await get_thumbnail(video_info['thumbnail']),
                caption=get_video_caption(video_info), parse_mode=ParseMode.MARKDOWN_V2,
                reply_to_message_id=video_message.message_id
            )  # use ParseMode.MARKDOWN_V2 to safely parse video titles containing markdown characters

            self.current_running_transfer_files += 1
            self.current_running_transfer_size += file.stat().st_size

            return video_message

    async def on_download_error(self, dm: DownloadManager, e: YoutubeDLError) -> None:
        """handle download error"""
        msg = escape_color(e.msg)
        network_error_tokens = (
            'The read operation timed out',
            'Connection reset by peer',
            'HTTP Error 503: Service Unavailable',
            'The handshake operation timed out'
        )
        live_not_started_error_tokens = (
            'Premieres in',
            'This live event will begin in'
        )

        retry_reason = ''

        if any(token in msg for token in network_error_tokens):
            retry_reason = RetryReason.NETWORK_ERROR
        elif any(token in msg for token in live_not_started_error_tokens):
            retry_reason = RetryReason.LIVE_NOT_STARTED
        elif 'Inconclusive download format' in msg:
            retry_reason = RetryReason.INCONCLUSIVE_FORMAT

        if retry_reason:
            self.current_running_retry_list.add(dm.url)
            await self.reply(f'{retry_reason}: {dm.video_id}\n{msg}\nthis url has been saved to retry list, you can retry it later')
        else:
            await self.reply(f'error on uploading this video: {dm.video_id}\n{msg}\n')

    async def work(self) -> None:
        """main work loop"""
        while True:
            self.current_task = await self.video_queue.get()
            video_message_id = None
            dm = DownloadManager(self.current_task.url)

            if is_in_database(dm.video_id):
                await self.reply_duplicate(dm.video_id)
                continue

            self.is_working = True

            try:
                video_info = await self.download_video(dm)
                video_message = await self.upload_video(dm.file, video_info)
                video_message_id = video_message.message_id

            except YoutubeDLError as e:
                await self.on_download_error(dm, e)

            except Exception:  # noqa
                await self.reply(f'error on uploading this video: {dm.video_id}\n{format_exc()}')

            else:
                await self.reply_task_done()

            finally:
                if dm.file.exists():
                    # if file exists, but upload failed, message_id will be 0
                    # make sure video_id is not in database before insert
                    insert_uploaded(dm.video_id, video_message_id)
                    dm.file.unlink()

                await self.clear_download_folder()
                self.is_working = False
                self.video_queue.task_done()


class VideoChecker(object):
    def __init__(self, message: Message):
        self.message = message
        self.video_ids = get_all_video_ids()

        self.count_all = len(self.video_ids)
        self.count_progress = 0
        self.count_become_available = 0
        self.count_become_unavailable = 0
        self.count_all_available = 0
        self.count_all_unavailable = 0

    async def handle_become_available(self, video_id: str):
        self.count_become_available += 1
        update_available(video_id, True)
        await self.reply_change(video_id, 'detected a video is available again')

    async def handle_become_not_available(self, video_id: str):
        self.count_become_unavailable += 1
        update_available(video_id, False)
        await self.reply_change(video_id, 'detected new unavailable video')

    async def reply_change(self, video_id: str, text: str):
        message_link = create_message_link(CHAT_ID, get_upload_message_id(video_id))
        await self.message.reply(f'{text}: [{video_id}]({message_link})', parse_mode='Markdown')

    async def check_progress(self) -> None:
        self.count_progress += 1
        if self.count_progress % 1000 == 0:
            await self.message.reply(f'progress: {self.count_progress}/{self.count_all}')

    async def check_video(self, video_id: str, video_available_online: bool, video_available_local: bool):
        if video_available_online:
            self.count_all_available += 1
        else:
            self.count_all_unavailable += 1

        if video_available_online and not video_available_local:
            await self.handle_become_available(video_id)
        elif not video_available_online and video_available_local:
            await self.handle_become_not_available(video_id)

    async def check_videos(self):
        for batch_video_ids in slide_window(self.video_ids, 50):
            batch_availability = await is_video_available_online_batch(batch_video_ids)

            for video_id in batch_video_ids:
                video_available_online = batch_availability[video_id]
                video_available_local = is_available(video_id)

                await self.check_progress()
                await self.check_video(video_id, video_available_online, video_available_local)
