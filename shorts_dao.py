import pymysql
import json
from datetime import timezone
from dateutil.parser import isoparse
from dotenv import load_dotenv
import os

load_dotenv()
host = os.getenv("db_host")
port = int(os.getenv("db_port"))
user = os.getenv("db_user")
password = os.getenv("db_password")
database = os.getenv("db_database")

class ShortDAO:
    def __init__(self):
        self.conn = pymysql.connect(
            host = host,
            port = port,
            user = user,
            password = password,
            database = database
        )
        self.cursor = self.conn.cursor()

    # YouTube 기본이 UTC
    @staticmethod
    def to_mysql_datetime_utc(rfc3339_str):
        dt = isoparse(rfc3339_str)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)


    # DB 저장
    def save_shorts_list(self, shorts):
        sql = """
            INSERT INTO youtube_videos
            (video_id, channel_id, title, description, published_at_utc, duration_seconds, is_shorts, view_count, like_count, comment_count, tags_json)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                description = VALUES(description),
                duration_seconds = VALUES(duration_seconds),
                is_shorts = VALUES(is_shorts),
                view_count = VALUES(view_count),
                like_count = VALUES(like_count),
                comment_count = VALUES(comment_count),
                tags_json = VALUES(tags_json),
                updated_at = CURRENT_TIMESTAMP
            """

        rows = []
        for v in shorts:
            rows.append((
                v["video_id"],
                v["channel_id"],
                (v["title"] or "")[:255],
                v.get("description"),
                self.to_mysql_datetime_utc(v["published_at"]),
                v["duration_seconds"],
                1 if v.get("is_shorts") else 0,
                v.get("view_count"),
                v.get("like_count"),
                v.get("comment_count"),
                json.dumps(v.get("tags", []), ensure_ascii=False)
            ))
        
        self.cursor.executemany(sql, rows)
        self.conn.commit()
        return len(rows)


    # 최근업데이트 5건 조회
    def fetch_last_n(self, n=5):
        sql = (
            "SELECT video_id, title, published_at_utc, view_count "
            "FROM youtube_videos "
            "ORDER BY updated_at DESC "
            "LIMIT %s"
        )
        self.cursor.execute(sql, (n,))
        return self.cursor.fetchall()


    def close(self):
        try:
            self.cursor.close()
        finally:
            self.conn.close()
        print("DB 커넥션 종료")
        
        