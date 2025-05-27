from sqlite3 import connect, Cursor, IntegrityError

from config import DATABASE_FILE
from typedef import VideoStatus
from utils import parse_upload_timestamp


class Connect:
    def __init__(self, file_path):
        self.file_path = file_path

    def __enter__(self) -> Cursor:
        self.conn = connect(self.file_path)
        self.c = self.conn.cursor()
        return self.c

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self.conn:
            self.conn.commit()
            self.conn.close()

        return not exc_type


def is_in_database(video_id: str) -> bool:
    """check if video_id exists in the database"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT video_id FROM video WHERE video_id = ?', (video_id,))
        return c.fetchone() is not None
    raise RuntimeError


def is_in_blocklist(video_id: str) -> bool:
    """check if video_id exists in the blocklist"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT video_id FROM blocklist WHERE video_id = ?', (video_id,))
        return c.fetchone() is not None
    raise RuntimeError


def insert_video(video_id: str, message_id: int, size: int, video_info: dict, status: VideoStatus) -> None:
    """insert a video_id into the database and record its message_id when the video is uploaded"""
    with Connect(DATABASE_FILE) as c:
        title = video_info.get('title')
        duration = video_info.get('duration')
        upload_ts = parse_upload_timestamp(video_info)
        c.execute('INSERT INTO video (video_id, message_id, title, duration, size, upload_ts, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
                  (video_id, message_id, title, duration, size, upload_ts, status))


def insert_blocklist(video_id: str) -> None:
    """insert a video_id into the blocklist"""
    with Connect(DATABASE_FILE) as c:
        c.execute('INSERT INTO blocklist (video_id) VALUES (?)', (video_id,))


def insert_extra_subscription(channel_id: str) -> bool:
    """insert an extra subscription into database"""
    try:
        with Connect(DATABASE_FILE) as c:
            c.execute('INSERT INTO extra_subscription (channel_id) VALUES (?)', (channel_id,))
        return True
    except IntegrityError:  # channel_id already exists
        return False


def delete_extra_subscription(channel_id: str) -> bool:
    """delete an extra subscription from database"""
    with Connect(DATABASE_FILE) as c:
        c.execute('DELETE FROM extra_subscription WHERE channel_id = ?', (channel_id,))
        return c.rowcount > 0
    raise RuntimeError


def update_status(video_id: str, video_status: VideoStatus) -> None:
    """update the status of a video in the database"""
    with Connect(DATABASE_FILE) as c:
        c.execute("UPDATE video SET status = ?, update_ts = datetime('now','localtime') WHERE video_id = ?",
                  (video_status, video_id,))


def get_status(video_id: str) -> VideoStatus:
    """get the status of a video in the database"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT status FROM video WHERE video_id = ?', (video_id,))
        result = c.fetchone()
    return VideoStatus(result[0])


def get_all_video_ids() -> list[str]:
    """get all uploaded video ids as a list, only for videos that have linked messages"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT video_id FROM video WHERE message_id != 0')
        return [video_id for video_id, in c.fetchall()]
    raise RuntimeError


def get_all_extra_subscription_channel_ids() -> list[str]:
    """get all extra subscription channel ids as a list"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT channel_id FROM extra_subscription')
        return [channel_id for channel_id, in c.fetchall()]
    raise RuntimeError


def get_upload_message_id(video_id: str) -> int:
    """get the message_id of the thumbnail message of the video"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT message_id FROM video WHERE video_id = ?', (video_id,))
        result = c.fetchone()
    return result[0] if result else 0


def get_backup_videos_count() -> int:
    """get the number of videos in the database, only for videos that have linked messages"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT COUNT(*) FROM video WHERE message_id != 0')
        return c.fetchone()[0]
    raise RuntimeError


def get_unavailable_videos_count() -> int:
    """get the number of unavailable videos"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT COUNT(*) FROM video WHERE status < 0')
        return c.fetchone()[0]
    raise RuntimeError


def get_extra_subscriptions_count() -> int:
    """get the number of extra subscriptions"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT COUNT(*) FROM extra_subscription')
        return c.fetchone()[0]
    raise RuntimeError


def get_video_count_by_status(status: VideoStatus) -> int:
    """get the number of videos by specific status"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT COUNT(*) FROM video WHERE status = ?', (status,))
        return c.fetchone()[0]
    raise RuntimeError


def get_backup_videos_total_size() -> int:
    """get the total file size of the backed up videos in the database"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT SUM(size) FROM video')
        return c.fetchone()[0] or 0
    raise RuntimeError


def get_backup_videos_total_duration() -> int:
    """get the total duration of the backed up videos in the database"""
    with Connect(DATABASE_FILE) as c:
        c.execute('SELECT SUM(duration) FROM video')
        return c.fetchone()[0] or 0
    raise RuntimeError
