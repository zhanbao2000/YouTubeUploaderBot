DROP TABLE IF EXISTS video;

CREATE TABLE video (
  video_id TEXT PRIMARY KEY,
  message_id INTEGER DEFAULT NULL,
  backup_ts INTEGER NULL DEFAULT (datetime('now','localtime')),
  delete_ts INTEGER NULL,
  status INTEGER NOT NULL DEFAULT 0
    -- 0: ok
    -- -1: error on uploading
    -- -2: unavailable video
);
