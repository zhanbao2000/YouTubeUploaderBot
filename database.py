import sqlite3
from typing import Optional

from typedef import VideoStatus


class Connect:

    def __init__(self, file_path):
        self.file_path = file_path

    def __enter__(self) -> sqlite3.Cursor:
        self.conn = sqlite3.connect(self.file_path)
        self.c = self.conn.cursor()
        return self.c

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self.conn:
            self.conn.commit()
            self.conn.close()

        return not exc_type


def is_in_database(video_id: str) -> bool:
    """check if a video is in the database"""
    with Connect('database.dat') as c:
        c.execute('SELECT video_id FROM video WHERE video_id = ?', (video_id,))
        return c.fetchone() is not None


def insert_uploaded(video_id: str, message_id: Optional[int]) -> None:
    """insert a video into database, and record the message_id of the video message"""
    with Connect('database.dat') as c:
        video_status = VideoStatus.AVAILABLE if message_id else VideoStatus.ERROR_ON_UPLOADING
        c.execute('INSERT INTO video (video_id, message_id, status) VALUES (?, ?, ?)',
                  (video_id, message_id, video_status))


def insert_extra_subscription(channel_id: str) -> bool:
    """insert an extra subscription into database"""
    try:
        with Connect('database.dat') as c:
            c.execute('INSERT INTO extra_subscription (channel_id) VALUES (?)', (channel_id,))
        return True
    except sqlite3.IntegrityError:  # channel_id already exists
        return False


def update_status(video_id: str, video_status: VideoStatus) -> None:
    """set a video as unavailable"""
    with Connect('database.dat') as c:
        c.execute("UPDATE video SET status = ?, update_ts = datetime('now','localtime') WHERE video_id = ?",
                  (video_status, video_id,))


def get_status(video_id: str) -> VideoStatus:
    """check if a video is unavailable"""
    with Connect('database.dat') as c:
        c.execute('SELECT status FROM video WHERE video_id = ?', (video_id,))
        result = c.fetchone()
    return VideoStatus(result[0])


def get_all_video_ids() -> list[str]:
    """get all video ids, except for videos that have not been uploaded successfully"""
    with Connect('database.dat') as c:
        c.execute('SELECT video_id FROM video WHERE status != ?', (VideoStatus.ERROR_ON_UPLOADING,))
        return [video_id for video_id, in c.fetchall()]


def get_all_extra_subscription_channel_ids() -> list[str]:
    """get all extra subscription channel ids"""
    with Connect('database.dat') as c:
        c.execute('SELECT channel_id FROM extra_subscription')
        return [channel_id for channel_id, in c.fetchall()]


def get_upload_message_id(video_id: str) -> int:
    """get the message_id of the message of the video"""
    with Connect('database.dat') as c:
        c.execute('SELECT message_id FROM video WHERE video_id = ?', (video_id,))
        result = c.fetchone()
    return result[0] if result else 0


def get_backup_videos_count() -> int:
    """get the number of videos in the database, except for videos that have not been uploaded successfully"""
    with Connect('database.dat') as c:
        c.execute('SELECT COUNT(*) FROM video WHERE status != ?', (VideoStatus.ERROR_ON_UPLOADING,))
        return c.fetchone()[0]


def get_unavailable_videos_count() -> int:
    """get the number of unavailable videos"""
    with Connect('database.dat') as c:
        c.execute('SELECT COUNT(*) FROM video WHERE status < 0')
        return c.fetchone()[0]


def get_extra_subscriptions_count() -> int:
    """get the number of extra subscriptions"""
    with Connect('database.dat') as c:
        c.execute('SELECT COUNT(*) FROM extra_subscription')
        return c.fetchone()[0]
