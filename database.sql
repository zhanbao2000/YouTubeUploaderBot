DROP TABLE IF EXISTS video;
DROP TABLE IF EXISTS extra_subscription;
VACUUM;

CREATE TABLE video
(
    video_id   TEXT PRIMARY KEY,
    message_id INTEGER NOT NULL,
    title      TEXT    DEFAULT NULL,
    duration   INTEGER DEFAULT 0,
    size       INTEGER DEFAULT 0,
    upload_ts  INTEGER DEFAULT NULL,
    backup_ts  INTEGER DEFAULT (datetime('now', 'localtime')),
    update_ts  INTEGER DEFAULT NULL,
    status     INTEGER DEFAULT 1
    -- 1: ok
    -- 0: error on uploading
    -- other: see VideoStatus in typedef.py
);

CREATE TABLE extra_subscription
(
    channel_id TEXT PRIMARY KEY,
    add_ts     INTEGER DEFAULT (datetime('now', 'localtime'))
);
