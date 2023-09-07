from asyncio import Queue, get_event_loop, QueueEmpty, AbstractEventLoop
from pathlib import Path
from traceback import format_exc

from aiogram import Bot, Dispatcher
from aiogram.bot.api import TelegramAPIServer
from aiogram.types import BotCommand, Message, ParseMode
from aiogram.types.base import Integer
from aiogram.utils import executor
from yt_dlp.utils import YoutubeDLError

from config import BOT_TOKEN, PROXY, DOWNLOAD_ROOT, CHAT_ID
from database import (
    insert_uploaded, is_in_database, is_available, update_available,
    get_db_size, get_all_video_ids, get_upload_message_id, get_unavailable_videos_count
)
from utils import format_file_size, create_message_link, is_superuser
from youtube import (
    DownloadManager, is_video_available_online, get_video_caption,
    get_video_id, get_playlist_id, get_all_video_urls_from_playlist, get_thumbnail
)

local_server = TelegramAPIServer.from_base('http://localhost:8083')
bot = Bot(token=BOT_TOKEN, proxy=PROXY, server=local_server)
dp = Dispatcher(bot)


class VideoWorker(object):
    def __init__(self, loop: AbstractEventLoop):
        self.is_working = False
        self.loop = loop
        self.video_queue = Queue()
        self.current_chat_id = None  # the chat which triggered this task, for replying
        self.current_message_id = None  # the message which triggered this task, for replying
        self.current_running_transfer_files = 0  # total files transferred from startup
        self.current_running_transfer_size = 0  # total size transferred from startup
        self.current_running_retry_list: list[str] = []  # save links with download error (only network error)

    def get_pending_tasks_count(self) -> int:
        """return pending tasks = waiting + current"""
        return self.video_queue.qsize() + self.is_working

    def get_queue_size(self) -> int:
        """return waiting tasks = queue size"""
        return self.video_queue.qsize()

    def add_retry_link(self, url: str, e: YoutubeDLError) -> bool:
        """add a link to retry list if this link has network error"""
        network_error_tokens = (
            'The read operation timed out',
            'Connection reset by peer',
            'HTTP Error 503: Service Unavailable',
            'The handshake operation timed out'
        )

        if any(token in e.msg for token in network_error_tokens):
            self.current_running_retry_list.append(url)
            return True
        return False

    async def add_task(self, url: str, chat_id: Integer, message_id: Integer) -> None:
        """add a new task"""
        await self.video_queue.put((url, chat_id, message_id))

    async def add_task_batch(self, url_list: list[str], chat_id: Integer, message_id: Integer) -> int:
        """add many new tasks"""
        count_task_added = 0
        for url in url_list:

            if is_in_database(get_video_id(url)):
                continue

            await self.video_queue.put((url, chat_id, message_id))
            count_task_added += 1

        return count_task_added

    async def reply(self, text: str, **kwargs) -> Message:
        """reply to the message which triggered current task"""
        return await bot.send_message(chat_id=self.current_chat_id, reply_to_message_id=self.current_message_id, text=text, **kwargs)

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
            video_message = await bot.send_video(
                chat_id=CHAT_ID, video=video,
                supports_streaming=True, duration=video_info['duration'],
                width=video_info['width'], height=video_info['height']
            )
            await bot.send_photo(
                chat_id=CHAT_ID, photo=await get_thumbnail(video_info['thumbnail']),
                caption=get_video_caption(video_info), parse_mode=ParseMode.MARKDOWN_V2,
                reply_to_message_id=video_message.message_id
            )  # use ParseMode.MARKDOWN_V2 to safely parse video titles containing markdown characters

            self.current_running_transfer_files += 1
            self.current_running_transfer_size += file.stat().st_size

            return video_message

    async def on_download_error(self, dm: DownloadManager, e: YoutubeDLError) -> None:
        """handle download error"""
        text_retry = 'this url has been saved to retry list, you can retry it later'
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

        if any(token in e.msg for token in network_error_tokens):
            self.current_running_retry_list.append(dm.url)
            await self.reply(f'a network error occurs when upload this video: {dm.video_id}\n{e.msg}\n{text_retry}')
        elif any(token in e.msg for token in live_not_started_error_tokens):
            self.current_running_retry_list.append(dm.url)
            await self.reply(f'this live has not yet started: {dm.video_id}\n{e.msg}\n{text_retry}')
        elif 'Inconclusive download format' in e.msg:
            self.current_running_retry_list.append(dm.url)
            await self.reply(f'this video currently only contains inconclusive formats: {dm.video_id}\n{e.msg}\n{text_retry}')
        else:
            await self.reply(f'error on uploading this video: {dm.video_id}\n{e.msg}\n')

    async def work(self) -> None:
        """main work loop"""
        while True:
            url, self.current_chat_id, self.current_message_id = await self.video_queue.get()
            video_message_id = None
            dm = DownloadManager(url)

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


@dp.message_handler(commands=['start'])
async def _(message: Message):
    await message.reply('hello')


@dp.message_handler(commands=['check'])
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    video_ids = get_all_video_ids()
    count_all = len(video_ids)
    count_become_available = 0
    count_become_not_available = 0
    count_all_available = 0
    count_all_not_available = 0

    await message.reply(f'start checking, task(s): {count_all}')

    for count_progress, video_id in enumerate(video_ids, start=1):

        if count_progress % 100 == 0:
            await message.reply(f'progress: {count_progress}/{count_all}')

        if not await is_video_available_online(video_id):
            count_all_not_available += 1

            if is_available(video_id):
                count_become_not_available += 1
                update_available(video_id, False)
                message_link = create_message_link(CHAT_ID, get_upload_message_id(video_id))
                await message.reply(f'detected new unavailable video: [{video_id}]({message_link})', parse_mode='Markdown')

        else:  # available online
            count_all_available += 1

            if not is_available(video_id):
                count_become_available += 1
                update_available(video_id, True)
                message_link = create_message_link(CHAT_ID, get_upload_message_id(video_id))
                await message.reply(f'detected a video is available again: [{video_id}]({message_link})', parse_mode='Markdown')

    await message.reply(f'all checking tasks finished\n'
                        f'total videos: {count_all}\n'
                        f'total 🟢: {count_all_available}\n'
                        f'total 🔴: {count_all_not_available}\n'
                        f'🟢 -> 🔴: {count_become_not_available}\n'
                        f'🔴 -> 🟢: {count_become_available}')


@dp.message_handler(commands=['cancel'])
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    await message.reply(f'{worker.get_queue_size()} task(s) cancelled')
    await worker.clear_queue()


@dp.message_handler(commands=['stat'])
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    await message.reply(f'statistics:\n'
                        f'transfer file(s): {worker.current_running_transfer_files}\n'
                        f'transfer size: {format_file_size(worker.current_running_transfer_size)}\n'
                        f'pending task(s): {worker.get_pending_tasks_count()}\n'
                        f'retry list size: {len(worker.current_running_retry_list)}\n'
                        f'database size: {get_db_size()}\n'
                        f'saved unavailable video(s): {get_unavailable_videos_count()}')


@dp.message_handler(commands=['add_list'])
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    if not (playlist_id := get_playlist_id(message.get_args())):
        return

    video_urls = await get_all_video_urls_from_playlist(playlist_id)
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.message_id)

    await message.reply(f'{count_urls} video(s) in this list\n'
                        f'{count_urls - count_urls_filtered} video(s) skipped\n'
                        f'{count_urls_filtered} task(s) added')


@dp.message_handler(commands=['retry'])
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    count_urls = len(worker.current_running_retry_list)

    count_urls_filtered = await worker.add_task_batch(worker.current_running_retry_list, message.chat.id, message.message_id)
    worker.current_running_retry_list.clear()

    await message.reply(f'{count_urls} video(s) in current retry list\n'
                        f'{count_urls - count_urls_filtered} video(s) skipped\n'
                        f'{count_urls_filtered} task(s) added')


@dp.message_handler(regexp=r'(?:v=|/)([0-9A-Za-z_-]{11}).*')
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    url = str(message.text)
    video_id = get_video_id(url)

    if is_in_database(video_id):

        if video_message_id := get_upload_message_id(video_id):  # video_message_id != 0
            await message.reply(f'this video had been [uploaded]({create_message_link(CHAT_ID, video_message_id)})',
                                parse_mode='Markdown')
        else:  # video_message_id == 0
            await message.reply('this video used to be tried to upload, but failed')

        return

    await worker.add_task(url, message.chat.id, message.message_id)
    await message.reply(f'task added\ntotal task(s): {worker.get_pending_tasks_count()}')


async def on_startup(dp_: Dispatcher) -> None:
    await dp_.bot.set_my_commands([
        BotCommand('start', 'hello'),
        BotCommand('check', 'check all videos whether they are still available'),
        BotCommand('cancel', 'cancel all waiting tasks'),
        BotCommand('stat', 'show statistics'),
        BotCommand('add_list', 'add all videos in a playlist'),
        BotCommand('retry', 'retry all videos with network error'),
    ])


if __name__ == '__main__':
    downloads_dir = Path(DOWNLOAD_ROOT)
    if not downloads_dir.exists():
        downloads_dir.mkdir()

    global_loop = get_event_loop()
    worker = VideoWorker(global_loop)
    global_loop.create_task(worker.work())

    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
