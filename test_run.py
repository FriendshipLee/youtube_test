from youtube_api import get_shorts_from_channel
from shorts_dao import ShortDAO

def main():
    # 유튜브 API 쇼츠 수집
    channel_id = "UCgsffS7MfKL6YU3r_U3E-aA"
    print(f"[1] 채널 ({channel_id})에서 쇼츠 수집 중...")
    shorts = get_shorts_from_channel(channel_id, max_pages=3)
    print(f" - 수집된 쇼츠 수 : {len(shorts)}")

    # 3개 테스트
    for s in shorts[:3]:
        print(f"  {s['video_id']} | {s['title']} | {s['duration_seconds']}초 | 조회수={s['view_count']}")

    # DB 연동
    dao = ShortDAO()
    inserted = dao.save_shorts_list(shorts)
    print("\n[2] 쇼츠 DB 저장...")
    print(f" - DB 저장 완료 건수 : {inserted}")

    last = dao.fetch_last_n(5)
    print("최근 5건 조회")
    for row in last:
        print(row)

    dao.close()
    print("완료")

if __name__ == "__main__":
    main()