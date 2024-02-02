from pathlib import Path
from textwrap import dedent

from pyrogram import Client, filters, idle
from pyrogram.types import BotCommand, Message

from config import API_ID, API_HASH, BOT_TOKEN, DOWNLOAD_ROOT, CHAT_ID
from database import (
    is_in_database, get_upload_message_id, insert_extra_subscription,
    get_backup_videos_count, get_unavailable_videos_count, get_extra_subscriptions_count,
    get_all_extra_subscription_channel_ids,
)
from typedef import Task
from utils import format_file_size, create_message_link, slide_window, is_superuser, get_args, counter
from worker import VideoWorker, VideoChecker, SchedulerManager
from youtube import (
    get_video_id, get_playlist_id, get_channel_id,
    get_all_video_urls_from_playlist, get_all_stream_urls_from_holoinfo,
    get_channel_info, get_all_my_subscription_channel_ids, get_channel_uploads_playlist_id_batch,
    get_channel_id_by_link, get_channel_uploads_playlist_id,
)

app = Client('YouTubeUploaderBot', api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


@app.on_message(filters.command('hello'))
async def hello(_, message: Message):
    await message.reply_text('hello', quote=True)


@app.on_message(filters.command('check') & is_superuser)
async def check(_, message: Message):
    checker = VideoChecker(message, app)
    await message.reply_text(f'start checking, task(s): {checker.count_all}', quote=True)
    await checker.check_videos()
    await message.reply_text(
        text=f'all checking tasks finished\n'
             f'total videos: {checker.count_all}\n'
             f'total 🟢: {checker.count_all_available}\n'
             f'total 🔴: {checker.count_all_unavailable}\n'
             f'🟢 -> 🔴: {checker.count_become_unavailable}\n'
             f'🔴 -> 🟢: {checker.count_become_available}',
        quote=True
    )


@app.on_message(filters.command('retry') & is_superuser)
async def retry(_, message: Message):
    count_urls = len(worker.current_running_retry_list)

    count_urls_filtered = await worker.add_task_batch(worker.current_running_retry_list, message.chat.id, message.id)
    worker.current_running_retry_list.clear()

    await message.reply_text(
        text=f'{count_urls} video(s) in current retry list\n'
             f'{count_urls - count_urls_filtered} video(s) skipped\n'
             f'{count_urls_filtered} task(s) added',
        quote=True
    )


@app.on_message(filters.command('stat') & is_superuser)
async def stat(_, message: Message):
    await message.reply_text(
        text=dedent(f'''\
             statistics:

             transfer
               transfer files: {worker.current_running_transfer_files}
               transfer size: {format_file_size(worker.current_running_transfer_size)}

             tasks
               pending tasks: {worker.get_pending_tasks_count()}
               retry list size: {len(worker.current_running_retry_list)}

             video count
               backup videos count: {get_backup_videos_count()}
               extra subscription count: {get_extra_subscriptions_count()}
               saved unavailable videos: {get_unavailable_videos_count()}

             switch
               notify on success: {worker.current_running_reply_on_success}
               notify on failure: {worker.current_running_reply_on_failure}
               scheduler status: {scheduler_manager.get_running_status()}

             API calls
               google api calls (1m): {counter.count_last_minute()}
               google api calls (1h): {counter.count_last_hour()}
               google api calls (1d): {counter.count_last_day()}'''),
        quote=True
    )


@app.on_message(filters.command('clear') & is_superuser)
async def clear(_, message: Message):
    await message.reply_text(
        text=f'{worker.get_queue_size()} pending task(s) cancelled\n'
             f'{len(worker.current_running_retry_list)} retry task(s) cancelled',
        quote=True
    )
    await worker.clear_queue()
    worker.current_running_retry_list.clear()


@app.on_message(filters.command('add_list') & is_superuser)
async def add_list(_, message: Message):
    url = get_args(message)
    if not (playlist_id := get_playlist_id(url)):
        return

    video_urls = await get_all_video_urls_from_playlist(playlist_id)
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.id)

    await message.reply_text(
        text=f'{count_urls} video(s) in this list\n'
             f'{count_urls - count_urls_filtered} video(s) skipped\n'
             f'{count_urls_filtered} task(s) added',
        quote=True
    )


@app.on_message(filters.command('add_holoinfo') & is_superuser)
async def add_holoinfo(_, message: Message):
    video_urls = await get_all_stream_urls_from_holoinfo()
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.id)

    await message.reply_text(
        text=f'{count_urls} video(s) fetched from holoinfo\n'
             f'{count_urls - count_urls_filtered} video(s) skipped\n'
             f'{count_urls_filtered} task(s) added',
        quote=True
    )


@app.on_message(filters.command('add_channel') & is_superuser)
async def add_channel(_, message: Message):
    url = get_args(message)
    if not url or not (channel_id := get_channel_id(url) or await get_channel_id_by_link(url)):
        return

    playlist_id = await get_channel_uploads_playlist_id(channel_id)
    video_urls = await get_all_video_urls_from_playlist(playlist_id, 'ASMR')
    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.id)

    await message.reply_text(
        text=f'{count_urls} ASMR video(s) in this channel\n'
             f'{count_urls - count_urls_filtered} video(s) skipped\n'
             f'{count_urls_filtered} task(s) added',
        quote=True
    )


@app.on_message(filters.command('add_subscription') & is_superuser)
async def add_subscription(_, message: Message):
    video_urls = []
    channel_ids = await get_all_my_subscription_channel_ids() + get_all_extra_subscription_channel_ids()

    await message.reply_text(f'get {len(channel_ids)} channels ({get_extra_subscriptions_count()} in extra)', quote=True)

    for batch_channel_ids in slide_window(channel_ids, 50):
        playlist_ids = await get_channel_uploads_playlist_id_batch(batch_channel_ids)

        for playlist_id in playlist_ids.values():
            video_urls.extend(await get_all_video_urls_from_playlist(playlist_id, 'ASMR', 5))

    count_urls = len(video_urls)

    count_urls_filtered = await worker.add_task_batch(video_urls, message.chat.id, message.id)

    await message.reply_text(
        text=f'{count_urls} video(s) in recent feeds\n'
             f'{count_urls - count_urls_filtered} video(s) skipped\n'
             f'{count_urls_filtered} task(s) added',
        quote=True
    )


@app.on_message(filters.command('add_extra_subscription') & is_superuser)
async def add_extra_subscription(_, message: Message):
    url = get_args(message)
    if not url or not (channel_id := get_channel_id(url) or await get_channel_id_by_link(url)):
        return

    if not (channel := await get_channel_info(channel_id)):
        await message.reply_text('this channel does not exist', quote=True)

    elif not insert_extra_subscription(channel_id):
        await message.reply_text('this channel has been added', quote=True)

    else:
        await message.reply_text(
            text=f'channel added\n'
                 f'[{channel.snippet.title}](https://www.youtube.com/channel/{channel_id})',
            quote=True
        )


@app.on_message(filters.command('toggle_reply_on_success') & is_superuser)
async def toggle_reply_on_success(_, message: Message):
    worker.current_running_reply_on_success = not worker.current_running_reply_on_success
    await message.reply_text(
        text=f'current notification setting\n'
             f'on success: {worker.current_running_reply_on_success}\n'
             f'on failure: {worker.current_running_reply_on_failure}',
        quote=True
    )


@app.on_message(filters.command('toggle_reply_on_failure') & is_superuser)
async def toggle_reply_on_failure(_, message: Message):
    worker.current_running_reply_on_failure = not worker.current_running_reply_on_failure
    await message.reply_text(
        text=f'current notification setting\n'
             f'on success: {worker.current_running_reply_on_success}\n'
             f'on failure: {worker.current_running_reply_on_failure}',
        quote=True
    )


@app.on_message(filters.command('toggle_scheduler') & is_superuser)
async def toggle_scheduler(_, message: Message):
    if scheduler_manager.get_running_status():
        scheduler_manager.pause()
        await message.reply_text('scheduler paused', quote=True)
    else:
        scheduler_manager.resume()
        await message.reply_text('scheduler resumed', quote=True)


@app.on_message(filters.regex(r'(?:youtube\.com/(?:watch\?v=|live/)|youtu\.be/)([0-9A-Za-z_-]{11})') & is_superuser)
async def youtube_url_regex(_, message: Message):
    url = str(message.text)
    video_id = get_video_id(url)

    if is_in_database(video_id):

        if video_message_id := get_upload_message_id(video_id):  # video_message_id != 0
            await message.reply_text(
                text=f'this video had been [uploaded]({create_message_link(CHAT_ID, video_message_id)})',
                quote=True
            )
        else:  # video_message_id == 0
            await message.reply_text('this video used to be tried to upload, but failed', quote=True)

        return

    await worker.add_task(Task(url, message.chat.id, message.id))
    await message.reply_text(f'task added\ntotal task(s): {worker.get_pending_tasks_count()}', quote=True)


if __name__ == '__main__':
    app.start()

    app.set_bot_commands([
        BotCommand(command='hello', description='hello'),
        BotCommand(command='check', description='check all videos whether they are still available'),
        BotCommand(command='retry', description='retry all videos with network error'),
        BotCommand(command='stat', description='show statistics'),
        BotCommand(command='clear', description='clear both retry and pending tasks'),
        BotCommand(command='add_list', description='add all videos in a playlist'),
        BotCommand(command='add_holoinfo', description='add 100 videos from holoinfo'),
        BotCommand(command='add_channel', description='all the videos uploaded by the channel'),
        BotCommand(command='add_subscription', description='add recent subscription feeds'),
        BotCommand(command='add_extra_subscription', description='add channel into separated subscription list'),
        BotCommand(command='toggle_reply_on_success', description='change success notification setting'),
        BotCommand(command='toggle_reply_on_failure', description='change failure notification setting'),
        BotCommand(command='toggle_scheduler', description='change scheduler status'),
    ])

    downloads_dir = Path(DOWNLOAD_ROOT)
    if not downloads_dir.exists():
        downloads_dir.mkdir()

    worker = VideoWorker(app)
    app.loop.create_task(worker.work())
    scheduler_manager = SchedulerManager(worker, app)

    idle()
    app.stop(block=False)
