from pathlib import Path
from textwrap import dedent

from pyrogram import Client, filters, idle
from pyrogram.types import BotCommand, Message

from config import API_HASH, API_ID, BOT_TOKEN, CHAT_ID, DOWNLOAD_ROOT, PROXY_TELEGRAM
from database import (
    delete_extra_subscription,
    get_all_extra_subscription_channel_ids,
    get_backup_videos_count,
    get_backup_videos_total_duration,
    get_backup_videos_total_size,
    get_extra_subscriptions_count,
    get_unavailable_videos_count,
    get_upload_message_id,
    insert_blocklist,
    insert_extra_subscription,
    is_in_blocklist,
)
from typedef import AddResult
from utils import (
    batched,
    counter,
    create_message_link,
    create_video_link_markdown,
    format_duration,
    format_file_size,
    get_args,
    get_memory_usage,
    get_swap_usage,
    get_uptime,
    is_ready,
    is_superuser,
    kill_self,
    not_a_command,
)
from worker import SchedulerManager, VideoChecker, VideoWorker
from youtube import (
    get_all_my_subscription_channel_ids,
    get_all_video_urls_from_playlist,
    get_channel_id,
    get_channel_id_by_link,
    get_channel_info,
    get_channel_uploads_playlist_id,
    get_channel_uploads_playlist_id_batch,
    get_playlist_id,
    get_video_id,
)

app = Client('YouTubeUploaderBot', api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, proxy=PROXY_TELEGRAM)


@app.on_message(filters.command('hello'))
async def hello(_, message: Message):
    await message.reply_text('hello', quote=True)


@app.on_message(filters.command('check') & is_superuser)
async def check(_, message: Message):
    checker = VideoChecker(message, app)
    await message.reply_text(f'start checking, task(s): {checker.count_all}', quote=True)
    await checker.check_videos()
    await app.send_message(CHAT_ID, text=checker.generate_check_report(), disable_web_page_preview=True)
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
    count_urls = len(worker.retry_tasks)
    count_urls_filtered = await worker.add_task_retry(message.chat.id, message.id)

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
             
             uptime: {get_uptime()}

             session upload
               uploaded files: {worker.session_uploaded_files}
               uploaded size: {format_file_size(worker.session_uploaded_size)}

             session tasks
               pending tasks: {worker.get_pending_tasks_count()}
               retry list size: {len(worker.retry_tasks)}
               retry ready: {sum(is_ready(next_retry_ts) for next_retry_ts in worker.retry_tasks.values())}
               
             current task
               {worker.generate_current_task_status()}

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
        quote=True,
        disable_web_page_preview=True
    )


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
    channel_ids = list(set(channel_ids))

    await message.reply_text(f'get {len(channel_ids)} channels ({get_extra_subscriptions_count()} in extra)', quote=True)

    for batch_channel_ids in batched(channel_ids, 50):
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


@app.on_message(filters.command('add_blocklist') & is_superuser)
async def add_blocklist(_, message: Message):
    url = get_args(message)
    if not (video_id := get_video_id(url)):
        return

    if is_in_blocklist(video_id):
        await message.reply_text('this video is already in the blocklist', quote=True)
        return

    insert_blocklist(video_id)
    await message.reply_text(
        f'successfully added to blocklist: {create_video_link_markdown(video_id)}',
        quote=True
    )


@app.on_message(filters.command('remove_invalid_subscription') & is_superuser)
async def remove_invalid_subscription(_, message: Message):
    channel_ids_extra_subscription = get_all_extra_subscription_channel_ids()
    channel_ids_my_subscription = await get_all_my_subscription_channel_ids()

    count_duplicated = sum(
        1
        for channel_id in channel_ids_extra_subscription
        if channel_id in channel_ids_my_subscription and delete_extra_subscription(channel_id)
    )

    playlist_ids = dict()  # channel_id -> playlist_id
    for channel_ids in batched(channel_ids_extra_subscription, 50):
        playlist_ids.update(await get_channel_uploads_playlist_id_batch(channel_ids))

    # invalid subscriptions do not have a corresponding key (channel_id) in dict playlist_ids
    count_banned = sum(
        1
        for channel_id in channel_ids_extra_subscription
        if channel_id not in playlist_ids and delete_extra_subscription(channel_id)
    )

    await message.reply_text(
        f'{count_duplicated} duplicated subscription(s) removed, '
        f'{count_banned} banned subscription(s) removed',
        quote=True
    )


@app.on_message(filters.command('clear') & is_superuser)
async def clear(_, message: Message):
    await message.reply_text(
        text=f'{worker.video_queue.qsize()} pending task(s) cancelled\n'
             f'{len(worker.retry_tasks)} retry task(s) cancelled',
        quote=True
    )
    await worker.clear_queue()
    worker.retry_tasks.clear()


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


@app.on_message(not_a_command & filters.regex(r'(?:youtube\.com/(?:watch\?v=|live/)|youtu\.be/)([0-9A-Za-z_-]{11})') & is_superuser)
async def youtube_url_regex(_, message: Message):
    url = str(message.text)
    add_result = await worker.add_task(url, message.chat.id, message.id)

    if add_result is AddResult.SUCCESS:
        reply = f'task added\ntotal task(s): {worker.get_pending_tasks_count()}'
    elif add_result is AddResult.DUPLICATE_DATABASE_UPLOADED:
        video_message_id = get_upload_message_id(get_video_id(url))
        reply = f'this video has been uploaded\n{create_message_link(CHAT_ID, video_message_id)}'
    elif add_result is AddResult.DUPLICATE_DATABASE_FAILED:
        reply = 'this video has been uploaded but failed'
    elif add_result is AddResult.DUPLICATE_QUEUE:
        reply = 'this video is already in the queue'
    elif add_result is AddResult.DUPLICATE_RETRY:
        reply = 'this video is already in the retry list, but not ready to retry'
    elif add_result is AddResult.DUPLICATE_CURRENT:
        reply = 'this video is currently being processed'
    elif add_result is AddResult.BLOCKLIST:
        reply = 'this video has been blocked by bot admin for some reasons'
    else:
        reply = f'unknown add result {add_result}'

    await message.reply_text(reply, quote=True)


if __name__ == '__main__':
    app.start()

    app.set_bot_commands([
        BotCommand(command='hello', description='hello'),
        BotCommand(command='check', description='check all videos whether they are still available'),
        BotCommand(command='retry', description='retry all videos in retry list'),
        BotCommand(command='stat', description='show statistics'),
        BotCommand(command='add_list', description='add all videos in a playlist'),
        BotCommand(command='add_channel', description='all the videos uploaded by the channel'),
        BotCommand(command='add_subscription', description='add recent subscription feeds'),
        BotCommand(command='add_extra_subscription', description='add channel into separated subscription list'),
        BotCommand(command='add_blocklist', description='add video into blocklist'),
        BotCommand(command='remove_invalid_subscription', description='remove all duplicated and banned subscriptions'),
        BotCommand(command='clear', description='clear both retry and pending tasks'),
        BotCommand(command='set_download_max_size', description='set max size of downloading video'),
        BotCommand(command='toggle_reply_on_success', description='change success notification setting'),
        BotCommand(command='toggle_reply_on_failure', description='change failure notification setting'),
        BotCommand(command='toggle_scheduler', description='change scheduler status'),
    ])

    downloads_dir = Path(DOWNLOAD_ROOT)
    if not downloads_dir.exists():
        downloads_dir.mkdir()

    worker = VideoWorker(app)
    app.loop.create_task(worker.start())
    scheduler_manager = SchedulerManager(worker, app)

    idle()

    app.stop(block=False)
    kill_self()  # if a download is in progress (by yt-dlp), CTRL+C will not stop the program, so we need to kill self manually
