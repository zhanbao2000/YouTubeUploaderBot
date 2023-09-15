from asyncio import get_event_loop
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.bot.api import TelegramAPIServer
from aiogram.types import BotCommand, Message
from aiogram.utils import executor

from config import BOT_TOKEN, PROXY, DOWNLOAD_ROOT, CHAT_ID
from database import (
    is_in_database, is_available, update_available,
    get_db_size, get_all_video_ids, get_upload_message_id, get_unavailable_videos_count
)
from typedef import Task
from utils import format_file_size, create_message_link, is_superuser
from worker import VideoWorker
from youtube import (
    is_video_available_online, get_video_id, get_playlist_id,
    get_all_video_urls_from_playlist, get_all_stream_urls_from_holoinfo
)

local_server = TelegramAPIServer.from_base('http://localhost:8083')
bot = Bot(token=BOT_TOKEN, proxy=PROXY, server=local_server)
dp = Dispatcher(bot)


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


@dp.message_handler(commands=['clear'])
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    await message.reply(f'{worker.get_queue_size()} pending task(s) cancelled\n'
                        f'{len(worker.current_running_retry_list)} retry task(s) cancelled')
    await worker.clear_queue()
    worker.current_running_retry_list.clear()


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


@dp.message_handler(commands=['add_holoinfo'])
async def _(message: Message):
    if not is_superuser(message.chat.id):
        return

    video_urls = await get_all_stream_urls_from_holoinfo()
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.message_id)

    await message.reply(f'{count_urls} video(s) fetched from holoinfo\n'
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

    await worker.add_task(Task(url, message.chat.id, message.message_id))
    await message.reply(f'task added\ntotal task(s): {worker.get_pending_tasks_count()}')


async def on_startup(dp_: Dispatcher) -> None:
    await dp_.bot.set_my_commands([
        BotCommand('start', 'hello'),
        BotCommand('check', 'check all videos whether they are still available'),
        BotCommand('clear', 'clear both retry and pending tasks'),
        BotCommand('stat', 'show statistics'),
        BotCommand('add_list', 'add all videos in a playlist'),
        BotCommand('add_holoinfo', 'add 100 videos from holoinfo'),
        BotCommand('retry', 'retry all videos with network error'),
    ])


if __name__ == '__main__':
    downloads_dir = Path(DOWNLOAD_ROOT)
    if not downloads_dir.exists():
        downloads_dir.mkdir()

    global_loop = get_event_loop()
    worker = VideoWorker(global_loop, bot)
    global_loop.create_task(worker.work())

    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
