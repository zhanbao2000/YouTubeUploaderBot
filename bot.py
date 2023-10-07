from asyncio import get_event_loop
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.bot.api import TelegramAPIServer
from aiogram.types import BotCommand, Message
from aiogram.utils import executor

from config import BOT_TOKEN, PROXY, DOWNLOAD_ROOT, CHAT_ID
from database import is_in_database, get_db_size, get_upload_message_id, get_unavailable_videos_count
from typedef import Task
from utils import format_file_size, create_message_link, superuser_required, slide_window
from worker import VideoWorker, VideoChecker, SchedulerManager
from youtube import (
    get_video_id, get_playlist_id, get_channel_id,
    get_all_video_urls_from_playlist, get_all_stream_urls_from_holoinfo,
    get_all_my_subscription_channel_ids, get_channel_uploads_playlist_id_batch, get_channel_uploads_playlist_id
)

local_server = TelegramAPIServer.from_base('http://localhost:8083')
bot = Bot(token=BOT_TOKEN, proxy=PROXY, server=local_server)
dp = Dispatcher(bot)


@dp.message_handler(commands=['start'])
async def _(message: Message):
    await message.reply('hello')


@dp.message_handler(commands=['check'])
@superuser_required
async def _(message: Message):
    checker = VideoChecker(message)
    await message.reply(f'start checking, task(s): {checker.count_all}')
    await checker.check_videos()
    await message.reply(f'all checking tasks finished\n'
                        f'total videos: {checker.count_all}\n'
                        f'total 🟢: {checker.count_all_available}\n'
                        f'total 🔴: {checker.count_all_unavailable}\n'
                        f'🟢 -> 🔴: {checker.count_become_unavailable}\n'
                        f'🔴 -> 🟢: {checker.count_become_available}')


@dp.message_handler(commands=['clear'])
@superuser_required
async def _(message: Message):
    await message.reply(f'{worker.get_queue_size()} pending task(s) cancelled\n'
                        f'{len(worker.current_running_retry_list)} retry task(s) cancelled')
    await worker.clear_queue()
    worker.current_running_retry_list.clear()


@dp.message_handler(commands=['stat'])
@superuser_required
async def _(message: Message):
    await message.reply(f'statistics:\n'
                        f'transfer file(s): {worker.current_running_transfer_files}\n'
                        f'transfer size: {format_file_size(worker.current_running_transfer_size)}\n'
                        f'pending task(s): {worker.get_pending_tasks_count()}\n'
                        f'retry list size: {len(worker.current_running_retry_list)}\n'
                        f'database size: {get_db_size()}\n'
                        f'saved unavailable video(s): {get_unavailable_videos_count()}\n'
                        f'notify on success: {worker.current_running_reply_on_success}\n'
                        f'notify on failure: {worker.current_running_reply_on_failure}')


@dp.message_handler(commands=['add_list'])
@superuser_required
async def _(message: Message):
    if not (playlist_id := get_playlist_id(message.get_args())):
        return

    video_urls = await get_all_video_urls_from_playlist(playlist_id)
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.message_id)

    await message.reply(f'{count_urls} video(s) in this list\n'
                        f'{count_urls - count_urls_filtered} video(s) skipped\n'
                        f'{count_urls_filtered} task(s) added')


@dp.message_handler(commands=['add_holoinfo'])
@superuser_required
async def _(message: Message):
    video_urls = await get_all_stream_urls_from_holoinfo()
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.message_id)

    await message.reply(f'{count_urls} video(s) fetched from holoinfo\n'
                        f'{count_urls - count_urls_filtered} video(s) skipped\n'
                        f'{count_urls_filtered} task(s) added')


@dp.message_handler(commands=['add_channel'])
@superuser_required
async def _(message: Message):
    if not (channel_id := get_channel_id(message.get_args())):
        return

    playlist_id = await get_channel_uploads_playlist_id(channel_id)
    video_urls = await get_all_video_urls_from_playlist(playlist_id, 'ASMR')
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.message_id)

    await message.reply(f'{count_urls} ASMR video(s) in this channel\n'
                        f'{count_urls - count_urls_filtered} video(s) skipped\n'
                        f'{count_urls_filtered} task(s) added')


@dp.message_handler(commands=['add_subscription'])
@superuser_required
async def _(message: Message):
    video_urls = []
    channel_ids = await get_all_my_subscription_channel_ids()

    await message.reply(f'get {len(channel_ids)} channels')

    for batch_channel_ids in slide_window(channel_ids, 50):
        playlist_ids = await get_channel_uploads_playlist_id_batch(batch_channel_ids)

        for playlist_id in playlist_ids.values():
            video_urls.extend(await get_all_video_urls_from_playlist(playlist_id, 'ASMR', 5))

    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.message_id)

    await message.reply(f'{count_urls} video(s) in recent feeds\n'
                        f'{count_urls - count_urls_filtered} video(s) skipped\n'
                        f'{count_urls_filtered} task(s) added')


@dp.message_handler(commands=['retry'])
@superuser_required
async def _(message: Message):
    count_urls = len(worker.current_running_retry_list)

    count_urls_filtered = await worker.add_task_batch(worker.current_running_retry_list, message.chat.id, message.message_id)
    worker.current_running_retry_list.clear()

    await message.reply(f'{count_urls} video(s) in current retry list\n'
                        f'{count_urls - count_urls_filtered} video(s) skipped\n'
                        f'{count_urls_filtered} task(s) added')


@dp.message_handler(commands=['toggle_reply_on_success'])
@superuser_required
async def _(message: Message):
    worker.current_running_reply_on_success = not worker.current_running_reply_on_success
    await message.reply(f'current notification setting\n'
                        f'on success: {worker.current_running_reply_on_success}\n'
                        f'on failure: {worker.current_running_reply_on_failure}')


@dp.message_handler(commands=['toggle_reply_on_failure'])
@superuser_required
async def _(message: Message):
    worker.current_running_reply_on_failure = not worker.current_running_reply_on_failure
    await message.reply(f'current notification setting\n'
                        f'on success: {worker.current_running_reply_on_success}\n'
                        f'on failure: {worker.current_running_reply_on_failure}')


@dp.message_handler(regexp=r'(?:v=|/)([0-9A-Za-z_-]{11}).*')
@superuser_required
async def _(message: Message):
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
        BotCommand('add_channel', 'all the videos uploaded by the channel'),
        BotCommand('add_subscription', 'add recent subscription feeds'),
        BotCommand('retry', 'retry all videos with network error'),
        BotCommand('toggle_reply_on_success', 'change success notification setting'),
        BotCommand('toggle_reply_on_failure', 'change failure notification setting'),
    ])


if __name__ == '__main__':
    downloads_dir = Path(DOWNLOAD_ROOT)
    if not downloads_dir.exists():
        downloads_dir.mkdir()

    global_loop = get_event_loop()
    worker = VideoWorker(global_loop, bot)
    global_loop.create_task(worker.work())
    scheduler_manager = SchedulerManager(worker, bot)
    scheduler_manager.start()

    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
