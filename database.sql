DROP TABLE IF EXISTS video;
DROP TABLE IF EXISTS extra_subscription;

CREATE TABLE video (
  video_id TEXT PRIMARY KEY,
  message_id INTEGER DEFAULT NULL,
  backup_ts INTEGER NULL DEFAULT (datetime('now','localtime')),
  update_ts INTEGER NULL,
  status INTEGER NOT NULL DEFAULT 1
    -- 1: ok
    -- 0: error on uploading
    -- other: see VideoStatus in typedef.py
);

CREATE TABLE extra_subscription (
    channel_id TEXT PRIMARY KEY,
    add_ts INTEGER NULL DEFAULT (datetime('now','localtime'))
)
