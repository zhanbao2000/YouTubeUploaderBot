from pathlib import Path
from textwrap import dedent

from pyrogram import Client, filters, idle
from pyrogram.types import BotCommand, Message

from config import API_ID, API_HASH, BOT_TOKEN, DOWNLOAD_ROOT, CHAT_ID
from database import (
    is_in_database, get_upload_message_id, insert_extra_subscription,
    get_backup_videos_count, get_unavailable_videos_count, get_extra_subscriptions_count,
    get_all_extra_subscription_channel_ids, get_backup_videos_total_size, get_backup_videos_total_duration,
)
from typedef import Task
from utils import (
    format_file_size, format_duration, create_message_link, slide_window, is_superuser, get_args, counter,
    get_memory_usage, get_swap_usage
)
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
    await app.send_message(CHAT_ID, text=checker.generate_check_report())
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
    count_urls = len(worker.session_retry_list)

    count_urls_filtered = await worker.add_task_batch(worker.session_retry_list, message.chat.id, message.id)
    worker.session_retry_list.clear()

    await message.reply_text(
        text=f'{count_urls} video(s) in current retry list\n'
             f'{count_urls - count_urls_filtered} video(s) skipped\n'
             f'{count_urls_filtered} task(s) added',
        quote=True
    )


@app.on_message(filters.command('stat') & is_superuser)
async def stat(_, message: Message):
    memory_usage = get_memory_usage()
    await message.reply_text(
        text=dedent(f'''\
             statistics:

             transfer
               transfer files: {worker.session_transfer_files}
               transfer size: {format_file_size(worker.session_transfer_size)}

             tasks
               pending tasks: {worker.get_pending_tasks_count()}
               retry list size: {len(worker.session_retry_list)}

             video count
               backup videos count: {get_backup_videos_count()}
               extra subscription count: {get_extra_subscriptions_count()}
               saved unavailable videos: {get_unavailable_videos_count()}
               total size: {format_file_size(get_backup_videos_total_size())}
               total duration: {format_duration(get_backup_videos_total_duration())}

             settings
               notify on success: {worker.session_reply_on_success}
               notify on failure: {worker.session_reply_on_failure}
               scheduler status: {scheduler_manager.get_running_status()}
               download max size: {worker.session_download_max_size} MB

             API calls
               google api calls (1m): {counter.count_last_minute()}
               google api calls (1h): {counter.count_last_hour()}
               google api calls (1d): {counter.count_last_day()}
    
             memory usage
               RSS: {format_file_size(memory_usage.rss)}
               VMS: {format_file_size(memory_usage.vms)}
               Text: {format_file_size(memory_usage.text)}
               Lib: {format_file_size(memory_usage.lib)}
               Data: {format_file_size(memory_usage.data)}
               Dirty: {format_file_size(memory_usage.dirty)}
               Swap: {format_file_size(get_swap_usage())}'''),
        quote=True
    )


@app.on_message(filters.command('clear') & is_superuser)
async def clear(_, message: Message):
    await message.reply_text(
        text=f'{worker.get_queue_size()} pending task(s) cancelled\n'
             f'{len(worker.session_retry_list)} retry task(s) cancelled',
        quote=True
    )
    await worker.clear_queue()
    worker.session_retry_list.clear()


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


@app.on_message(filters.command('set_download_max_size') & is_superuser)
async def set_download_max_size(_, message: Message):
    size = get_args(message)
    if not size or not size.isdigit():
        return

    if int(size) < 100:
        await message.reply_text('size must be greater than 100 MB', quote=True)
        return

    worker.session_download_max_size = int(size)
    await message.reply_text(f'max size set to {worker.session_download_max_size} MB', quote=True)


@app.on_message(filters.command('toggle_reply_on_success') & is_superuser)
async def toggle_reply_on_success(_, message: Message):
    worker.session_reply_on_success = not worker.session_reply_on_success
    await message.reply_text(
        text=f'current notification setting\n'
             f'on success: {worker.session_reply_on_success}\n'
             f'on failure: {worker.session_reply_on_failure}',
        quote=True
    )


@app.on_message(filters.command('toggle_reply_on_failure') & is_superuser)
async def toggle_reply_on_failure(_, message: Message):
    worker.session_reply_on_failure = not worker.session_reply_on_failure
    await message.reply_text(
        text=f'current notification setting\n'
             f'on success: {worker.session_reply_on_success}\n'
             f'on failure: {worker.session_reply_on_failure}',
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
        BotCommand(command='retry', description='retry all videos in retry list'),
        BotCommand(command='stat', description='show statistics'),
        BotCommand(command='clear', description='clear both retry and pending tasks'),
        BotCommand(command='add_list', description='add all videos in a playlist'),
        BotCommand(command='add_holoinfo', description='add 100 videos from holoinfo'),
        BotCommand(command='add_channel', description='all the videos uploaded by the channel'),
        BotCommand(command='add_subscription', description='add recent subscription feeds'),
        BotCommand(command='add_extra_subscription', description='add channel into separated subscription list'),
        BotCommand(command='set_download_max_size', description='set max size of downloading video'),
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
