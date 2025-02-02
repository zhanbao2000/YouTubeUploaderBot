from asyncio import QueueEmpty, Task as AsyncTask, create_task, get_running_loop, sleep, to_thread
from collections import defaultdict
from pathlib import Path
from traceback import format_exc
from typing import Iterable, Optional

from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import STATE_PAUSED
from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.types import Message
from pytz import timezone
from yt_dlp.utils import YoutubeDLError

from config import CHAT_ID, DOWNLOAD_ROOT, SUPERUSERS
from database import (
    get_all_extra_subscription_channel_ids,
    get_all_video_ids,
    get_backup_videos_total_duration,
    get_backup_videos_total_size,
    get_status,
    get_upload_message_id,
    insert_video,
    is_in_database,
    update_status,
)
from typedef import (
    AddResult,
    Channel,
    HashTag,
    IncompleteTranscodingError,
    RetryReason,
    Task,
    UniqueQueue,
    VideoStatus,
    VideoTooShortError,
)
from utils import (
    batched,
    create_channel_link_markdown,
    create_message_link,
    create_video_link,
    create_video_link_markdown,
    find_channel_in_message,
    format_duration,
    format_file_size,
    format_timestamp,
    get_next_retry_ts,
    is_ready,
    join_list,
    now_datetime,
    offset_text_link_entities,
    remove_color_codes,
    remove_hashtags_from_caption,
)
from youtube import (
    DownloadManager,
    get_all_my_subscription_channel_ids,
    get_all_video_urls_from_playlist,
    get_channel_uploads_playlist_id_batch,
    get_thumbnail,
    get_video_caption,
    get_video_id,
    is_video_available_online_batch,
)


class VideoWorker(object):
    def __init__(self, app: Client):
        self.is_working = False
        self.app = app

        self.video_queue: UniqueQueue[Task] = UniqueQueue()
        self.current_task: Optional[Task] = None
        self.retry_tasks: dict[str, float] = dict()  # save the urls of the task that need to be retried and the timestamps of their next retries

        self.session_uploaded_files = 0  # total number of files that have been uploaded since startup
        self.session_uploaded_size = 0  # total size of files that have been uploaded since startup
        self.session_download_max_size = 2000  # max size of single format allowed when extracting video info (MB)
        self.session_reply_on_success = True
        self.session_reply_on_failure = True

        self.worker_task: Optional[AsyncTask] = None  # the task of the worker, used to restart the worker

    async def start(self) -> None:
        """start the worker and worker monitor"""
        self.worker_task = create_task(self.work())
        self.worker_task.add_done_callback(self.on_worker_stopped_async_wrapper)

        await self.app.send_message(chat_id=SUPERUSERS[0], text='worker started')

    def on_worker_stopped_async_wrapper(self, task: AsyncTask) -> None:
        """async wrapper for on_worker_stopped"""
        loop = self.app.loop or get_running_loop()
        loop.create_task(self.on_worker_stopped(task))

    async def on_worker_stopped(self, task: AsyncTask) -> None:
        """handle worker stopped event"""
        self.is_working = False

        e = task.exception()
        if isinstance(e, YoutubeDLError):
            msg = remove_color_codes(e.msg)
        else:
            msg = str(e)

        await self.app.send_message(
            chat_id=SUPERUSERS[0],
            text=f'worker stopped, reason:\n{msg}\nworker will restart in 10 seconds'
        )

        await sleep(10)
        await self.start()

    def get_pending_tasks_count(self) -> int:
        """return pending tasks = waiting + current"""
        return self.video_queue.qsize() + self.is_working

    def get_queue_size(self) -> int:
        """return waiting tasks = queue size"""
        return self.video_queue.qsize()

    async def add_task(self, url: str, chat_id: Optional[int], message_id: Optional[int]) -> AddResult:
        """add a new task, skip if the task already exists in any of the queues or the database to avoid duplication"""
        video_id = get_video_id(url)

        if is_in_database(video_id):
            # There is no need to get the video_message_id every time when the video_id already exists in the database.
            # since getting the video_message_id is only for the reply message of the bot, and get_upload_message_id() is slow.
            # So get_upload_message_id() should be skipped when either chat_id or message_id is None.
            if not chat_id or not message_id:
                return AddResult.DUPLICATE_DATABASE
            video_message_id = get_upload_message_id(video_id)
            if video_message_id != 0:
                return AddResult.DUPLICATE_DATABASE_UPLOADED
            else:
                return AddResult.DUPLICATE_DATABASE_FAILED

        if self.is_working and url == self.current_task.url:
            return AddResult.DUPLICATE_CURRENT

        if url in self.video_queue:
            return AddResult.DUPLICATE_QUEUE

        if url in self.retry_tasks:
            if is_ready(self.retry_tasks[url]):
                self.retry_tasks.pop(url)
            else:
                return AddResult.DUPLICATE_RETRY

        await self.video_queue.put(Task(url, chat_id, message_id))
        return AddResult.SUCCESS

    async def add_task_batch(self, urls: Iterable[str], chat_id: Optional[int], message_id: Optional[int]) -> int:
        """add tasks in batch, every task will be added individually"""
        count_task_added = 0

        for url in urls:
            if await self.add_task(url, chat_id, message_id) == AddResult.SUCCESS:
                count_task_added += 1

        return count_task_added

    async def add_task_retry(self, chat_id: Optional[int], message_id: Optional[int]) -> int:
        """retry all ready tasks in retry_tasks."""
        count_task_added = 0

        for url, next_retry_ts in self.retry_tasks.copy().items():  # use copy() to avoid 'RuntimeError: dictionary changed size during iteration'
            if is_ready(next_retry_ts):
                self.retry_tasks.pop(url)  # use pop() before add_task() since add_task() will check if url in self.retry_tasks
                await self.add_task(url, chat_id, message_id)
                count_task_added += 1

        return count_task_added

    async def reply(self, text: str, **kwargs) -> Optional[Message]:
        """reply to the message which triggered current task"""
        # if current_task does not provide chat_id or message_id, ignore
        if self.current_task.chat_id and self.current_task.message_id:
            return await self.app.send_message(
                chat_id=self.current_task.chat_id,
                reply_to_message_id=self.current_task.message_id,
                text=text,
                **kwargs
            )

    async def clear_download_folder(self) -> None:
        """clear the download folder and reply to the user"""
        for file in Path(DOWNLOAD_ROOT).glob('*'):
            file.unlink()
            await self.reply_failure(f'deleted {file.name}')

    async def clear_queue(self) -> None:
        """immediately clear all waiting tasks, will not cancel current task"""
        while not self.video_queue.empty():
            try:
                self.video_queue.get_nowait()
            except QueueEmpty:
                continue

    async def reply_success(self, text: str, **kwargs) -> Optional[Message]:
        """reply when the video is successfully downloaded and uploaded"""
        if self.session_reply_on_success:
            return await self.reply(text, **kwargs)

    async def reply_failure(self, text: str, **kwargs) -> Optional[Message]:
        """reply when the video is failed to download or upload"""
        if self.session_reply_on_failure:
            return await self.reply(text, **kwargs)

    async def reply_task_done(self) -> None:
        """inform the user that this task had been done"""
        if self.get_queue_size():
            await self.reply_success(f'task finished\npending task(s): {self.get_queue_size()}')
        else:
            await self.reply_success('all tasks finished')

    async def download_video(self, dm: DownloadManager, use_cookies: bool = False) -> dict:
        """download the video, return the video info"""
        video_info = await to_thread(dm.download_max_size, self.session_download_max_size, use_cookies)

        # the 2 GB check here is just to prevent the file size exceeds the Telegram limit,
        # it has nothing to do with the `session_download_max_size`
        if (filesize := dm.file.stat().st_size) >= 2e9:
            dm.file.unlink()
            await self.reply_failure(f'{create_video_link_markdown(dm.video_id)}\n'
                                     f'file too big: {format_file_size(filesize)}\n'
                                     f'try downloading smaller format')
            video_info = await to_thread(dm.download_max_size, 1600, use_cookies)  # retry with smaller format
            # if this format is still too big, the video_id will be recorded in db with message_id = 0

        return video_info

    async def upload_video(self, file: Path, video_info: dict) -> Message:
        """upload the video and its thumbnail, return the message of the thumbnail"""
        with open(file, 'rb') as video:
            video_message = await self.app.send_video(
                chat_id=CHAT_ID, video=video,
                supports_streaming=True, duration=video_info['duration'],
                width=video_info['width'], height=video_info['height']
            )
            thumbnail_message = await self.app.send_photo(
                chat_id=CHAT_ID, photo=await get_thumbnail(video_info['thumbnail']),
                caption=get_video_caption(video_info), reply_to_message_id=video_message.id
            )

            self.session_uploaded_files += 1
            self.session_uploaded_size += file.stat().st_size

            return thumbnail_message

    async def on_too_short_video(self, dm: DownloadManager, e: VideoTooShortError) -> None:
        """handle error of VideoTooShortError"""
        video_info = e.video_info
        duration = video_info['duration']

        insert_video(dm.video_id, 0, 0, video_info, VideoStatus.TOO_SHORT)
        await self.reply_failure(f'error on downloading this video: {create_video_link_markdown(dm.video_id)}\n'
                                 f'Too short video, duration: {duration}\n')

    async def on_error_should_use_cookies(self, dm: DownloadManager, e: YoutubeDLError) -> bool:
        """handle error of video that should use cookies"""
        msg = remove_color_codes(e.msg)
        task = self.current_task

        if 'This video may be inappropriate for some users' in msg and not task.use_cookies:
            task.use_cookies = True
            # add this task into the front of the queue to retry with cookies immediately
            self.video_queue.put_left_nowait(task)
            await self.reply_failure(f'encountered age-restricted video, retrying with cookies immediately: '
                                     f'{create_video_link_markdown(dm.video_id)}')
            return True

        return False

    async def on_error_should_retry(self, dm: DownloadManager, e: YoutubeDLError) -> bool:
        """handle error of video that should retry"""
        msg = remove_color_codes(e.msg)
        network_error_tokens = (
            'The read operation timed out',
            'Connection reset by peer',
            'HTTP Error 403: Forbidden',
            'HTTP Error 503: Service Unavailable',
            'The handshake operation timed out',
            'Read timed out',
            'Remote end closed connection without response',
            'A network error has occurred',
            'bytes read',
            'EOF occurred in violation of protocol',
            'Failed to extract any player response'
        )
        live_not_started_error_tokens = (
            'Premieres in',
            'This live event will begin in',
            'Watch on the latest version of YouTube'
        )
        retry_reason = ''

        if any(token in msg for token in network_error_tokens):
            retry_reason = RetryReason.NETWORK_ERROR
        elif any(token in msg for token in live_not_started_error_tokens):
            retry_reason = RetryReason.LIVE_NOT_STARTED
        elif isinstance(e, IncompleteTranscodingError):
            retry_reason = RetryReason.INCOMPLETE_TRANSCODING
        elif 'Sign in to confirm you’re not a bot' in msg:
            # when encountering LOGIN_REQUIRED, retrying later may alleviate this error
            retry_reason = RetryReason.LOGIN_REQUIRED

        if retry_reason:
            next_retry_ts = get_next_retry_ts(msg)
            self.retry_tasks[dm.url] = next_retry_ts
            await self.reply_failure(f'{retry_reason}: {create_video_link_markdown(dm.video_id)}\n{msg}\n'
                                     f'this url has been saved to retry list, it will retry after {format_timestamp(next_retry_ts)}')
            return True

        return False

    async def on_error_members_only_videos(self, dm: DownloadManager, e: YoutubeDLError) -> bool:
        """handle error of members-only videos"""
        msg = remove_color_codes(e.msg)
        members_only_tokens = (
            'Join this channel to get access to members-only content like this video, and other exclusive perks',
            'This video is available to this channel\'s members on level'
        )

        if any(token in msg for token in members_only_tokens):
            # yt-dlp cannot fetch video_info of members-only videos, so the video_info should be empty
            insert_video(dm.video_id, 0, 0, {}, VideoStatus.MEMBERS_ONLY)
            await self.reply_failure(f'error on downloading this member-only video: '
                                     f'{create_video_link_markdown(dm.video_id)}')
            return True

        return False

    async def on_download_error(self, dm: DownloadManager, e: YoutubeDLError) -> None:
        """handle error of YoutubeDLError"""
        msg = remove_color_codes(e.msg)

        if not any((
            await self.on_error_members_only_videos(dm, e),
            await self.on_error_should_use_cookies(dm, e),
            await self.on_error_should_retry(dm, e)
        )):
            await self.reply_failure(f'error on downloading this video: '
                                     f'{create_video_link_markdown(dm.video_id)}\n{msg}')

    async def on_finish(self, dm: DownloadManager, video_info: Optional[dict], thumbnail_message: Optional[Message]) -> None:
        """handle finish event"""
        if video_info is None:  # download failed
            pass
        elif thumbnail_message is None:  # download successful, upload failed
            insert_video(dm.video_id, 0, dm.file.stat().st_size, video_info, VideoStatus.ERROR_ON_UPLOADING)
        else:  # both download and upload successful
            insert_video(dm.video_id, thumbnail_message.id, dm.file.stat().st_size, video_info, VideoStatus.AVAILABLE)

        dm.file.unlink(missing_ok=True)

        await self.clear_download_folder()
        self.is_working = False
        self.video_queue.task_done()

    async def work(self) -> None:
        """main work loop"""
        while True:
            self.current_task = await self.video_queue.get()
            dm = DownloadManager(self.current_task.url)

            self.is_working = True
            # determine whether the download or upload is successful by setting its initial value to None,
            # in finally block, if the value is still None, it means the download or upload is failed
            video_info: Optional[dict] = None  # for download
            thumbnail_message: Optional[Message] = None  # for upload

            try:
                video_info = await self.download_video(dm, self.current_task.use_cookies)
                thumbnail_message = await self.upload_video(dm.file, video_info)

            except VideoTooShortError as e:
                await self.on_too_short_video(dm, e)

            except YoutubeDLError as e:
                await self.on_download_error(dm, e)

            except Exception:  # noqa
                await self.reply_failure(f'error on uploading this video: {create_video_link_markdown(dm.video_id)}\n'
                                         f'{format_exc().splitlines()[-1]}')

            else:
                await self.reply_task_done()

            finally:
                await self.on_finish(dm, video_info, thumbnail_message)


class VideoChecker(object):
    def __init__(self, message: Message, app: Client, verbose: bool = True):
        self.message = message
        self.app = app
        self.verbose = verbose
        self.video_ids = get_all_video_ids()

        self.count_all = len(self.video_ids)
        self.count_progress = 0
        self.count_become_available = 0
        self.count_become_unavailable = 0
        self.count_all_available = 0
        self.count_all_unavailable = 0
        self.count_all_unavailable_by_reasons: dict[VideoStatus, int] = dict.fromkeys((status for status in VideoStatus), 0)
        self.count_all_unavailable_by_channel: defaultdict[Channel, int] = defaultdict(int)
        self.terminated_or_closed_accounts: set[Channel] = set()

    async def update_video_caption(self, message_id: int, video_status: VideoStatus) -> None:
        message = await self.app.get_messages(CHAT_ID, message_id)
        entities = message.caption_entities
        edited_caption = f'{HashTag[video_status]}\n{remove_hashtags_from_caption(message.caption)}'

        if edited_caption == message.caption:
            return

        await self.app.edit_message_caption(
            chat_id=CHAT_ID, message_id=message_id,
            caption=edited_caption,
            parse_mode=ParseMode.MARKDOWN,
            caption_entities=offset_text_link_entities(entities, len(edited_caption) - len(message.caption))
        )

    def generate_check_report(self) -> str:
        lines = join_list(
            [''],
            [f'#视频有效性检查报告 {now_datetime()}'],
            self.generate_check_report_become_unavailable(),
            self.generate_check_report_account_terminated_or_closed(),
            self.generate_check_report_many_unavailable(),
            self.generate_check_report_stats()
        )
        return '\n'.join(lines)

    def generate_check_report_become_unavailable(self) -> list[str]:
        lines = []

        if self.count_become_unavailable:
            lines.append(f'本次检查中，新增 {self.count_become_unavailable} 个视频已在源站不可观看，其中：')
            for video_status, count in self.count_all_unavailable_by_reasons.items():
                if count:
                    tag = HashTag[video_status].removeprefix('#')
                    lines.append(f'{tag}：{count} 个')

        else:
            lines.append('本次检查中，没有新增已在源站不可观看的视频。')

        return lines

    def generate_check_report_account_terminated_or_closed(self) -> list[str]:
        lines = []

        if self.terminated_or_closed_accounts:
            lines.append('以下 YouTube 账号已终止或已关闭：')
            for channel in self.terminated_or_closed_accounts:
                lines.append(create_channel_link_markdown(channel))

        return lines

    def generate_check_report_many_unavailable(self) -> list[str]:
        lines = []

        # skip channels with less than 10 unavailable videos
        for channel, count in self.count_all_unavailable_by_channel.copy().items():
            if count < 10:
                self.count_all_unavailable_by_channel.pop(channel)

        if self.count_all_unavailable_by_channel:
            lines.append('以下频道有较多视频在源站变得不可观看：')
            channels_sorted_by_count = sorted(self.count_all_unavailable_by_channel.items(), key=lambda _: _[1], reverse=True)
            for channel, count in channels_sorted_by_count:
                lines.append(f'{create_channel_link_markdown(channel)}：{count} 个')

        return lines

    def generate_check_report_stats(self) -> list[str]:
        unavailable_percent = self.count_all_unavailable / self.count_all * 100
        return [
            f'已备份视频总数：{self.count_all} 个',
            f'在源站不可观看：{self.count_all_unavailable} 个（{unavailable_percent:.2f}%）',
            f'已备份视频总大小：{format_file_size(get_backup_videos_total_size())}',
            f'已备份视频总时长：{format_duration(get_backup_videos_total_duration())}',
        ]

    async def handle_become_available(self, video_id: str) -> None:
        await sleep(3)
        self.count_become_available += 1
        update_status(video_id, VideoStatus.AVAILABLE)

        await self.reply_change(video_id, 'detected a video is available again')
        await self.update_video_caption(get_upload_message_id(video_id), VideoStatus.AVAILABLE)

    async def handle_become_unavailable(self, video_id: str) -> None:
        try:
            video_status = await to_thread(DownloadManager(create_video_link(video_id)).get_video_status)
        except YoutubeDLError:
            # if encounter DownloadError (Sign in to confirm ...), skip handling this video, and wait for the next check
            return

        await sleep(3)
        self.count_become_unavailable += 1
        update_status(video_id, video_status)

        message_id = get_upload_message_id(video_id)
        message = await self.app.get_messages(CHAT_ID, message_id)
        channel = find_channel_in_message(message)

        await self.reply_change(video_id, f'detected new unavailable video\n{video_status.name}')
        await self.update_video_caption(message_id, video_status)

        self.count_all_unavailable_by_reasons[video_status] += 1
        self.count_all_unavailable_by_channel[channel] += 1

        if video_status in (VideoStatus.ACCOUNT_TERMINATED, VideoStatus.ACCOUNT_CLOSED):
            self.terminated_or_closed_accounts.add(channel)

    async def reply_change(self, video_id: str, text: str) -> None:
        if not self.verbose:
            return
        message_link = create_message_link(CHAT_ID, get_upload_message_id(video_id))
        await self.message.reply_text(f'{text}: [{video_id}]({message_link})', quote=True)

    async def check_progress(self) -> None:
        self.count_progress += 1
        if self.count_progress % 1000 == 0:
            await self.message.reply_text(f'progress: {self.count_progress}/{self.count_all}', quote=True)

    async def check_video(self, video_id: str, video_available_online: bool, video_status_local: VideoStatus) -> None:
        if video_available_online:
            self.count_all_available += 1
        else:
            self.count_all_unavailable += 1

        if video_available_online and video_status_local is not VideoStatus.AVAILABLE:
            await self.handle_become_available(video_id)
        elif not video_available_online and video_status_local is VideoStatus.AVAILABLE:
            await self.handle_become_unavailable(video_id)

    async def check_videos(self) -> None:
        for batch_video_ids in batched(self.video_ids, 50):
            batch_availability = await is_video_available_online_batch(batch_video_ids)

            for video_id in batch_video_ids:
                video_available_online = batch_availability[video_id]
                video_status_local = get_status(video_id)

                await self.check_progress()
                await self.check_video(video_id, video_available_online, video_status_local)


class SchedulerManager(object):
    def __init__(self, worker: VideoWorker, app: Client):
        self.worker = worker
        self.app = app
        self.scheduler = AsyncIOScheduler()

        beijing_tz = timezone('Asia/Shanghai')

        self.scheduler.add_job(self.add_subscription, 'cron', hour='3', timezone=beijing_tz)
        self.scheduler.add_job(self.retry, 'cron', hour='6,9,12', timezone=beijing_tz)

        self.scheduler.add_listener(self.on_error, EVENT_JOB_ERROR)
        self.scheduler.start()

    def pause(self) -> None:
        """pause scheduler"""
        self.scheduler.pause()

    def resume(self) -> None:
        """resume scheduler"""
        self.scheduler.resume()

    def get_running_status(self) -> bool:
        """return if scheduler is running"""
        return self.scheduler.state != STATE_PAUSED

    def on_error(self, event: JobExecutionEvent) -> None:
        """handle error during execute tasks"""
        job = self.scheduler.get_job(event.job_id)
        exc_class = event.exception.__class__.__name__

        self.app.loop.create_task(self.reply(f'error on execute this job: {job.name}\n'
                                             f'{exc_class}: {event.exception}\n'
                                             f'next run at: {job.next_run_time}'))

    async def reply(self, text: str, **kwargs) -> Message:
        """reply to first admin"""
        return await self.app.send_message(chat_id=SUPERUSERS[0], text=text, **kwargs)

    async def add_subscription(self) -> None:
        """add recent subscription feeds"""
        video_urls = []
        channel_ids = await get_all_my_subscription_channel_ids() + get_all_extra_subscription_channel_ids()
        channel_ids = list(set(channel_ids))

        for batch_channel_ids in batched(channel_ids, 50):
            playlist_ids = await get_channel_uploads_playlist_id_batch(batch_channel_ids)

            for playlist_id in playlist_ids.values():
                video_urls.extend(await get_all_video_urls_from_playlist(playlist_id, 'ASMR', 5))

        await self.worker.add_task_batch(video_urls, None, None)

    async def retry(self) -> None:
        """retry all videos in retry list"""
        await self.worker.add_task_retry(None, None)
