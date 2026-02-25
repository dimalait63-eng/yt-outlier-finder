# app.py — MostPopular Outlier Finder v2 (Ultra Low Quota + SQLite cache)
#
# ✅ No search.list (самое дорогое) — только chart=mostPopular
# ✅ SQLite-кэш: повторные сканы почти не тратят квоту
# ✅ Лимит кандидатов за запуск
# ✅ Исключение Shorts по длительности
# ✅ Выбор страны + (опционально) категории, 404 категории пропускаем
#
# Install:
#   pip install streamlit google-api-python-client python-dotenv pandas
# Run:
#   streamlit run app.py
#
# .env / Streamlit Secrets:
#   YOUTUBE_API_KEY=...

import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------- init ----------------
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()

st.set_page_config(page_title="MostPopular Outliers v2 (Low Quota)", layout="wide")
st.title("MostPopular Outliers v2 (экономно по квоте + кэш)")

if not API_KEY:
    st.error("Не найден YOUTUBE_API_KEY. Добавь в .env или Streamlit Secrets.")
    st.stop()

youtube = build("youtube", "v3", developerKey=API_KEY)

DB_PATH = "cache.db"  # рядом с app.py

DUR_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")

def iso_duration_to_seconds(d: str) -> int:
    m = DUR_RE.fullmatch(d or "")
    if not m:
        return 0
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mi * 60 + s

def age_days(published_at: str) -> float:
    dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    days = (datetime.now(timezone.utc) - dt).total_seconds() / 86400
    return max(days, 0.1)

def fmt_int(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    try:
        return f"{int(x):,}".replace(",", " ")
    except Exception:
        return "—"

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def http_error_text(e: HttpError) -> str:
    try:
        body = e.content.decode("utf-8", errors="ignore")
    except Exception:
        body = str(e)
    return f"{e}\n\n{body}"

# ---------------- SQLite cache ----------------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = db()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS videos_cache (
      video_id TEXT PRIMARY KEY,
      title TEXT,
      channel_id TEXT,
      channel_title TEXT,
      published_at TEXT,
      views INTEGER,
      duration_iso TEXT,
      thumbnail TEXT,
      fetched_at TEXT
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS channels_cache (
      channel_id TEXT PRIMARY KEY,
      subs INTEGER,
      fetched_at TEXT
    )
    """)
    conn.commit()
    conn.close()

init_db()

def get_cached_videos(video_ids: List[str], max_age_hours: int) -> Dict[str, dict]:
    if not video_ids:
        return {}
    conn = db()
    q = f"""
    SELECT video_id, title, channel_id, channel_title, published_at, views, duration_iso, thumbnail, fetched_at
    FROM videos_cache
    WHERE video_id IN ({",".join(["?"]*len(video_ids))})
    """
    rows = conn.execute(q, video_ids).fetchall()
    conn.close()

    out = {}
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_hours * 3600
    for r in rows:
        fetched_at = r[8]
        try:
            ts = datetime.fromisoformat(fetched_at).timestamp()
        except Exception:
            ts = 0
        if ts >= cutoff:
            out[r[0]] = {
                "videoId": r[0],
                "title": r[1],
                "channelId": r[2],
                "channelTitle": r[3],
                "publishedAt": r[4],
                "views": int(r[5]) if r[5] is not None else 0,
                "duration_iso": r[6] or "",
                "thumbnail": r[7],
            }
    return out

def upsert_videos_cache(items: List[dict]):
    if not items:
        return
    conn = db()
    conn.executemany("""
    INSERT INTO videos_cache(video_id, title, channel_id, channel_title, published_at, views, duration_iso, thumbnail, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
      title=excluded.title,
      channel_id=excluded.channel_id,
      channel_title=excluded.channel_title,
      published_at=excluded.published_at,
      views=excluded.views,
      duration_iso=excluded.duration_iso,
      thumbnail=excluded.thumbnail,
      fetched_at=excluded.fetched_at
    """, [
        (
            it["videoId"],
            it.get("title",""),
            it.get("channelId",""),
            it.get("channelTitle",""),
            it.get("publishedAt",""),
            int(it.get("views",0)),
            it.get("duration_iso",""),
            it.get("thumbnail", None),
            now_utc_iso()
        ) for it in items
    ])
    conn.commit()
    conn.close()

def get_cached_subs(channel_ids: List[str], max_age_hours: int) -> Dict[str, Optional[int]]:
    if not channel_ids:
        return {}
    conn = db()
    q = f"""
    SELECT channel_id, subs, fetched_at
    FROM channels_cache
    WHERE channel_id IN ({",".join(["?"]*len(channel_ids))})
    """
    rows = conn.execute(q, channel_ids).fetchall()
    conn.close()

    out = {}
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_hours * 3600
    for cid, subs, fetched_at in rows:
        try:
            ts = datetime.fromisoformat(fetched_at).timestamp()
        except Exception:
            ts = 0
        if ts >= cutoff:
            out[cid] = int(subs) if subs is not None else None
    return out

def upsert_channels_cache(items: Dict[str, Optional[int]]):
    if not items:
        return
    conn = db()
    conn.executemany("""
    INSERT INTO channels_cache(channel_id, subs, fetched_at)
    VALUES (?, ?, ?)
    ON CONFLICT(channel_id) DO UPDATE SET
      subs=excluded.subs,
      fetched_at=excluded.fetched_at
    """, [(cid, items[cid], now_utc_iso()) for cid in items])
    conn.commit()
    conn.close()

# ---------------- API calls ----------------
@st.cache_data(ttl=3600)
def fetch_categories(region_code: str) -> List[Tuple[str, str]]:
    r = youtube.videoCategories().list(part="snippet", regionCode=region_code).execute()
    cats = []
    for it in r.get("items", []):
        sn = it.get("snippet", {})
        if sn.get("assignable") is True:
            cats.append((it["id"], sn.get("title", it["id"])))
    cats.sort(key=lambda x: x[1].lower())
    return cats

def most_popular_video_ids(region_code: str, pages: int, per_page: int, category_id: Optional[str]) -> List[str]:
    ids = []
    page_token = None
    for _ in range(int(pages)):
        params = {
            "part": "snippet",
            "chart": "mostPopular",
            "regionCode": region_code,
            "maxResults": int(per_page),
        }
        if category_id:
            params["videoCategoryId"] = str(category_id)
        if page_token:
            params["pageToken"] = page_token

        r = youtube.videos().list(**params).execute()
        ids.extend([it["id"] for it in r.get("items", [])])

        page_token = r.get("nextPageToken")
        if not page_token:
            break
    return list(dict.fromkeys(ids))

def fetch_videos_api(video_ids: List[str]) -> List[dict]:
    out = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        r = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(chunk)
        ).execute()

        for it in r.get("items", []):
            sn = it.get("snippet", {})
            stt = it.get("statistics", {})
            cd = it.get("contentDetails", {})

            thumbs = sn.get("thumbnails", {})
            thumb_url = None
            for k in ["maxres", "standard", "high", "medium", "default"]:
                if k in thumbs and "url" in thumbs[k]:
                    thumb_url = thumbs[k]["url"]
                    break

            out.append({
                "videoId": it.get("id"),
                "title": sn.get("title", ""),
                "channelId": sn.get("channelId", ""),
                "channelTitle": sn.get("channelTitle", ""),
                "publishedAt": sn.get("publishedAt", ""),
                "views": int(stt.get("viewCount", 0)),
                "duration_iso": cd.get("duration", ""),
                "thumbnail": thumb_url,
            })
    return out

def fetch_channels_subs_api(channel_ids: List[str]) -> Dict[str, Optional[int]]:
    result = {}
    channel_ids = [c for c in channel_ids if c]
    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i:i+50]
        r = youtube.channels().list(part="statistics", id=",".join(chunk)).execute()
        for it in r.get("items", []):
            stt = it.get("statistics", {})
            subs = stt.get("subscriberCount")
            result[it["id"]] = int(subs) if subs is not None else None
    return result

# ---------------- UI ----------------
COMMON_COUNTRIES = [
    ("US", "United States"), ("GB", "United Kingdom"), ("CA", "Canada"), ("AU", "Australia"),
    ("DE", "Germany"), ("FR", "France"), ("NL", "Netherlands"), ("ES", "Spain"), ("IT", "Italy"),
    ("BR", "Brazil"), ("MX", "Mexico"), ("JP", "Japan"), ("KR", "South Korea"), ("IN", "India"),
    ("PL", "Poland"), ("SE", "Sweden"), ("NO", "Norway"), ("TR", "Turkey"),
]

with st.sidebar:
    st.header("Страна")
    region_code = st.selectbox(
        "regionCode",
        options=COMMON_COUNTRIES,
        index=0,
        format_func=lambda x: f"{x[1]} ({x[0]})"
    )[0]

    st.header("Охват")
    # По умолчанию выключено — стабильнее и меньше запросов
    scan_by_categories = st.checkbox("Сканировать по категориям (шире, но иногда 404)", value=False)
    pages = st.slider("Страниц", 1, 10, 3, 1)
    per_page = st.selectbox("Видео на страницу", [10, 25, 50], index=2)

    st.header("Кэш (сильно экономит квоту)")
    video_cache_hours = st.slider("Кэш видео (часов)", 1, 72, 12, 1)
    channel_cache_hours = st.slider("Кэш подписчиков (часов)", 1, 168, 24, 1)

    st.header("Фильтры")
    exclude_shorts = st.checkbox("Убрать Shorts/короткие", value=True)
    min_seconds = st.slider("Мин. длительность (сек)", 60, 3600, 120, 30, disabled=not exclude_shorts)

    max_subs = st.number_input("Макс. подписчики канала", min_value=0, value=10_000, step=500)
    min_views = st.number_input("Мин. просмотры", min_value=0, value=20_000, step=5_000)
    min_ratio = st.number_input("Мин. Views/Subs (если subs видны)", min_value=0.0, value=3.0, step=1.0)

    st.header("Лимиты (защита квоты)")
    max_candidates = st.slider("Макс. кандидатов за запуск", 50, 1000, 300, 50)

    st.header("Вывод")
    sort_mode = st.selectbox("Сортировать по", ["views_per_day", "ratio", "views", "date"], index=0)
    view_mode = st.radio("Вид", ["Карточки", "Таблица"], index=0)

    run = st.button("🔎 Сканировать")

if not run:
    st.write("Выбери страну и фильтры слева → нажми **Сканировать**.")
    st.stop()

errors = []
try:
    video_ids: List[str] = []

    if scan_by_categories:
        cats = fetch_categories(region_code)
        # даём выбрать (по умолчанию чуть-чуть)
        cat_options = [f"{title} ({cid})" for cid, title in cats]
        default_pick = cat_options[: min(8, len(cat_options))]
        selected = st.multiselect("Категории", options=cat_options, default=default_pick)

        selected_ids = []
        for s in selected:
            cid = s.split("(")[-1].split(")")[0].strip()
            selected_ids.append(cid)

        if not selected_ids:
            selected_ids = [cid for cid, _ in cats]

        for cid in selected_ids:
            try:
                video_ids.extend(most_popular_video_ids(region_code, pages, per_page, cid))
            except HttpError as e:
                # ✅ некоторые категории дают 404 — пропускаем
                continue
    else:
        video_ids.extend(most_popular_video_ids(region_code, pages, per_page, None))

    # dedupe + hard limit
    video_ids = list(dict.fromkeys(video_ids))[: int(max_candidates)]

    # ---- videos: cache first
    cached_v = get_cached_videos(video_ids, max_age_hours=int(video_cache_hours))
    missing_v = [vid for vid in video_ids if vid not in cached_v]

    fresh_v_items = []
    if missing_v:
        fresh_v_items = fetch_videos_api(missing_v)
        upsert_videos_cache(fresh_v_items)

    all_v = {**cached_v, **{it["videoId"]: it for it in fresh_v_items}}
    vids = list(all_v.values())

    # ---- channels subs: cache first
    channel_ids = list({v["channelId"] for v in vids if v.get("channelId")})
    cached_c = get_cached_subs(channel_ids, max_age_hours=int(channel_cache_hours))
    missing_c = [cid for cid in channel_ids if cid not in cached_c]

    fresh_subs = {}
    if missing_c:
        fresh_subs = fetch_channels_subs_api(missing_c)
        upsert_channels_cache(fresh_subs)

    subs_map = {**cached_c, **fresh_subs}

    # ---- compute + filter
    rows = []
    for v in vids:
        secs = iso_duration_to_seconds(v.get("duration_iso", ""))
        if exclude_shorts and secs < int(min_seconds):
            continue

        subs = subs_map.get(v.get("channelId", ""))
        days = age_days(v["publishedAt"]) if v.get("publishedAt") else None
        vpd = (v["views"] / days) if days else None

        ratio = None
        if subs and subs > 0:
            ratio = v["views"] / subs

        if v["views"] < int(min_views):
            continue
        if subs is not None and subs > int(max_subs):
            continue
        if ratio is not None and ratio < float(min_ratio):
            continue

        rows.append({
            "title": v["title"],
            "channel": v["channelTitle"],
            "subs": subs,
            "views": v["views"],
            "views_per_day": round(vpd, 2) if vpd is not None else None,
            "ratio": round(ratio, 2) if ratio is not None else None,
            "duration_sec": secs,
            "publishedAt": v["publishedAt"],
            "thumbnail": v["thumbnail"],
            "url": f"https://www.youtube.com/watch?v={v['videoId']}",
        })

except HttpError as e:
    errors.append(http_error_text(e))

if errors:
    st.error("Ошибка API.")
    st.code("\n\n".join(errors))
    st.stop()

if not video_ids:
    st.warning("Не удалось получить кандидатов (попробуй выключить категории или поменять страну).")
    st.stop()

st.caption(f"Кандидатов собрано: {len(video_ids)} (лимит: {int(max_candidates)})")

if not rows:
    st.info(
        "Ничего не найдено под текущие фильтры.\n\n"
        "Важно: mostPopular редко содержит каналы ≤10k.\n\n"
        "Чтобы чаще находило:\n"
        "- min_views 10k–20k\n"
        "- min_ratio 0–1\n"
        "- max_subs 50k (на время проверки)\n"
        "- включи категории и выбери больше (но бывают 404 — мы их уже пропускаем)"
    )
    st.stop()

df = pd.DataFrame(rows)

# sort
if sort_mode == "views_per_day":
    df["_sort"] = df["views_per_day"].fillna(-1)
    df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])
elif sort_mode == "ratio":
    df["_sort"] = df["ratio"].fillna(-1)
    df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])
elif sort_mode == "views":
    df = df.sort_values("views", ascending=False)
else:
    df = df.sort_values("publishedAt", ascending=False)

st.success(f"Найдено: {len(df)}")

if view_mode == "Карточки":
    for _, r in df.iterrows():
        c1, c2 = st.columns([1, 3])
        with c1:
            if r.get("thumbnail"):
                st.image(r["thumbnail"], use_container_width=True)
            else:
                st.write("🖼 Нет превью")
        with c2:
            st.markdown(f"**{r['title']}**")
            st.write(
                f"Канал: {r['channel']}\n\n"
                f"Subs: {fmt_int(r['subs'])} | Views: {fmt_int(r['views'])} | "
                f"Views/day: {r.get('views_per_day','—')} | Ratio: {r.get('ratio','—')}\n\n"
                f"Duration: {fmt_int(r.get('duration_sec'))} sec | Date: {r['publishedAt']}"
            )
            st.markdown(f"[Открыть видео]({r['url']})")
        st.divider()
else:
    show = df[["title", "channel", "subs", "views", "views_per_day", "ratio", "duration_sec", "publishedAt", "url"]]
    st.dataframe(show, use_container_width=True)

csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
st.download_button("⬇️ Скачать CSV", data=csv_bytes, file_name=f"mostpopular_outliers_{region_code}.csv", mime="text/csv")

st.caption(
    "v2 использует SQLite-кэш, поэтому повторные сканы почти не тратят квоту. "
    "Если всё равно пусто при subs≤10k — это норм: mostPopular редко содержит такие каналы."
)
