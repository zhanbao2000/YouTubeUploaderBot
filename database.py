import sqlite3
from typing import Optional


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
        status = 0 if message_id else -1  # upload error, set status to -1
        c.execute('INSERT INTO video (video_id, message_id, status) VALUES (?, ?, ?)', (video_id, message_id, status))


def update_available(video_id: str, available: bool) -> None:
    """set a video as unavailable"""
    with Connect('database.dat') as c:
        status = 0 if available else -2
        c.execute("UPDATE video SET status = ?, delete_ts = datetime('now','localtime') WHERE video_id = ?",
                  (status, video_id,))


def is_available(video_id: str) -> bool:
    """check if a video is unavailable"""
    with Connect('database.dat') as c:
        c.execute('SELECT status FROM video WHERE video_id = ?', (video_id,))
        result = c.fetchone()
    return result[0] != -2 if result else False


def get_all_video_ids() -> list[str]:
    """get all video ids"""
    with Connect('database.dat') as c:
        c.execute('SELECT video_id FROM video')
        return [video_id for video_id, in c.fetchall()]


def get_upload_message_id(video_id: str) -> Optional[int]:
    """get the message_id of the message of the video"""
    with Connect('database.dat') as c:
        c.execute('SELECT message_id FROM video WHERE video_id = ?', (video_id,))
        result = c.fetchone()
    return result[0] if result else None


def get_db_size() -> int:
    """get the size of the database, namely the number of videos in the database"""
    with Connect('database.dat') as c:
        c.execute('SELECT COUNT(*) FROM video')
        return c.fetchone()[0]


def get_unavailable_videos_count() -> int:
    """get the number of unavailable videos"""
    with Connect('database.dat') as c:
        c.execute('SELECT COUNT(*) FROM video WHERE status = -2')
        return c.fetchone()[0]
