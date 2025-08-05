from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from isodate import parse_duration
from itertools import islice
from dotenv import load_dotenv
from datetime import datetime, timezone
import os, time

load_dotenv()
API_KEY = os.getenv("api")
YOUTUBE = build("youtube", "v3", developerKey=API_KEY)

# YouTube API videos.list()는 최대 50개 ID까지만 한번에 요청 가능
# 일정 크기 잘라서 리턴
def chunked(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        # 일시 중지 -> 값 반환 후 재개
        yield batch


# YouTube API에서 영상 수집
def collect_video(channel_id, max_pages=3, published_after=None):
    ids = []
    page_token = None
    for _ in range(max_pages):
        resp = YOUTUBE.search().list(
            part="id",
            channelId=channel_id,
            type="video",
            order="date",
            videoDuration="short",  # 4분 미만 동영상만
            maxResults=50,
            pageToken=page_token,
            publishedAfter=published_after  # 특정 날짜 이후 업로드만 필터링
        ).execute()
        ids += [it["id"]["videoId"] for it in resp.get("items", []) if it["id"]["kind"] == "youtube#video"]
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        time.sleep(1)
        #print(ids)
    return ids


# videoId를 이용하여 영상 세부 정보 조회
def fetch_video_details(video_ids):
    results = []
    for batch in chunked(video_ids, 50):
        resp = YOUTUBE.videos().list(
            # 제목,설명,채널ID,태그 / 길이,해상도 / 조회수,좋아요,댓글
            part="snippet,contentDetails,statistics",
            id=",".join(batch)
        ).execute()
        results += resp.get("items", [])
        time.sleep(1)
        #print(results)
    return results


# Shorts 영상 판별
def is_shorts_item(item):
    # 영상 길이
    dur_iso = item["contentDetails"]["duration"]
    seconds = int(parse_duration(dur_iso).total_seconds())

    # 정보 수집
    snip = item["snippet"]
    title = snip.get("title", "") or ""
    desc = snip.get("description", "") or ""
    tags = snip.get("tags", []) or []
    text_blob = f"{title}\n{desc}\n{' '.join(tags)}".lower()
    has_hint = any(k in text_blob for k in ["#shorts", "shorts", "쇼츠"])
    return (seconds <= 61) or has_hint, seconds


# 위 모든 단계를 묶어서 실행
def get_shorts_from_channel(channel_id, max_pages=5, published_after=None):
    try:
        ids = collect_video(channel_id, max_pages, published_after)
        details = fetch_video_details(ids)
        shorts = []
        for it in details:
            ok, seconds = is_shorts_item(it)
            if ok:
                sn = it["snippet"]
                st = it.get("statistics", {})
                shorts.append({
                    "video_id": it["id"],
                    "channel_id": sn.get("channelId"),
                    "title": sn.get("title"),
                    "description": sn.get("description", ""),
                    "published_at": sn.get("publishedAt"),
                    "duration_seconds": seconds,
                    "is_shorts": True,
                    "view_count": int(st.get("viewCount", 0)),
                    "like_count": int(st.get("likeCount", 0)) if "likeCount" in st else None,
                    "comment_count": int(st.get("commentCount", 0)) if "commentCount" in st else None,
                    "tags": sn.get("tags", [])
                })
        return shorts
    
    except HttpError as e:
        raise e
