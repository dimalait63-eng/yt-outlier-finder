# app.py — Outlier Finder v3 (WORKS): MostPopular + Low-Quota Search + Cache
#
# Идея:
# - MostPopular (дёшево) даёт "что сейчас на витрине"
# - Search (дорого) используем МАЛО: 8–12 запросов/запуск, чтобы ловить маленькие каналы
# - Всё кэшируем в SQLite, повторные сканы не жрут квоту
#
# Install:
#   pip install streamlit google-api-python-client python-dotenv pandas
# Run:
#   streamlit run app.py
#
# Secrets/.env:
#   YOUTUBE_API_KEY=...

import os, re, sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- config ----------
BEST_US_KEYS = [
    # Crime / interrogation
    "true crime documentary", "interrogation analysis", "cold case documentary",
    "unsolved mysteries documentary", "bodycam footage full",
    # Psychology / relationships
    "dark psychology", "gaslighting explained", "narcissist documentary",
    "attachment styles explained", "human behavior documentary",
    # Survival / disasters
    "real survival stories", "plane crash documentary", "disaster documentary",
    # Space / science
    "space documentary", "universe explained documentary",
    # Money / scams
    "scam documentary", "fraud documentary",
]

DUR_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")
DB_PATH = "cache.db"

def iso_duration_to_seconds(d: str) -> int:
    m = DUR_RE.fullmatch(d or "")
    if not m: return 0
    h = int(m.group(1) or 0); mi = int(m.group(2) or 0); s = int(m.group(3) or 0)
    return h*3600 + mi*60 + s

def age_days(published_at: str) -> float:
    dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    days = (datetime.now(timezone.utc) - dt).total_seconds()/86400
    return max(days, 0.1)

def fmt_int(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return "—"
    try: return f"{int(x):,}".replace(",", " ")
    except: return "—"

def http_error_text(e: HttpError) -> str:
    try: body = e.content.decode("utf-8", errors="ignore")
    except: body = str(e)
    return f"{e}\n\n{body}"

def now_iso(): return datetime.now(timezone.utc).isoformat()

# ---------- init ----------
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()

st.set_page_config(page_title="Outlier Finder v3", layout="wide")
st.title("Outlier Finder v3 (реально находит маленькие каналы)")

if not API_KEY:
    st.error("Нет YOUTUBE_API_KEY.")
    st.stop()

youtube = build("youtube", "v3", developerKey=API_KEY)

# ---------- DB ----------
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = db()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS videos_cache(
      video_id TEXT PRIMARY KEY,
      title TEXT, channel_id TEXT, channel_title TEXT,
      published_at TEXT, views INTEGER, duration_iso TEXT, thumbnail TEXT,
      fetched_at TEXT
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS channels_cache(
      channel_id TEXT PRIMARY KEY,
      subs INTEGER,
      fetched_at TEXT
    )
    """)
    conn.commit(); conn.close()

init_db()

def get_cached_videos(video_ids: List[str], max_age_hours: int) -> Dict[str, dict]:
    if not video_ids: return {}
    conn = db()
    q = f"SELECT video_id,title,channel_id,channel_title,published_at,views,duration_iso,thumbnail,fetched_at FROM videos_cache WHERE video_id IN ({','.join(['?']*len(video_ids))})"
    rows = conn.execute(q, video_ids).fetchall()
    conn.close()
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_hours*3600
    out = {}
    for r in rows:
        try: ts = datetime.fromisoformat(r[8]).timestamp()
        except: ts = 0
        if ts >= cutoff:
            out[r[0]] = {"videoId":r[0],"title":r[1],"channelId":r[2],"channelTitle":r[3],
                         "publishedAt":r[4],"views":int(r[5] or 0),"duration_iso":r[6] or "",
                         "thumbnail":r[7]}
    return out

def upsert_videos(items: List[dict]):
    if not items: return
    conn = db()
    conn.executemany("""
    INSERT INTO videos_cache(video_id,title,channel_id,channel_title,published_at,views,duration_iso,thumbnail,fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?)
    ON CONFLICT(video_id) DO UPDATE SET
      title=excluded.title, channel_id=excluded.channel_id, channel_title=excluded.channel_title,
      published_at=excluded.published_at, views=excluded.views, duration_iso=excluded.duration_iso,
      thumbnail=excluded.thumbnail, fetched_at=excluded.fetched_at
    """, [(it["videoId"], it.get("title",""), it.get("channelId",""), it.get("channelTitle",""),
           it.get("publishedAt",""), int(it.get("views",0)), it.get("duration_iso",""),
           it.get("thumbnail",None), now_iso()) for it in items])
    conn.commit(); conn.close()

def get_cached_subs(channel_ids: List[str], max_age_hours: int) -> Dict[str, Optional[int]]:
    if not channel_ids: return {}
    conn = db()
    q = f"SELECT channel_id,subs,fetched_at FROM channels_cache WHERE channel_id IN ({','.join(['?']*len(channel_ids))})"
    rows = conn.execute(q, channel_ids).fetchall()
    conn.close()
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_hours*3600
    out = {}
    for cid, subs, fetched_at in rows:
        try: ts = datetime.fromisoformat(fetched_at).timestamp()
        except: ts = 0
        if ts >= cutoff:
            out[cid] = int(subs) if subs is not None else None
    return out

def upsert_subs(items: Dict[str, Optional[int]]):
    if not items: return
    conn = db()
    conn.executemany("""
    INSERT INTO channels_cache(channel_id,subs,fetched_at)
    VALUES (?,?,?)
    ON CONFLICT(channel_id) DO UPDATE SET subs=excluded.subs, fetched_at=excluded.fetched_at
    """, [(cid, items[cid], now_iso()) for cid in items])
    conn.commit(); conn.close()

# ---------- API (cheap) ----------
def most_popular_ids(region: str, pages: int, per_page: int) -> List[str]:
    ids = []
    token = None
    for _ in range(pages):
        params = {"part":"snippet", "chart":"mostPopular", "regionCode":region, "maxResults":per_page}
        if token: params["pageToken"] = token
        r = youtube.videos().list(**params).execute()
        ids.extend([it["id"] for it in r.get("items", [])])
        token = r.get("nextPageToken")
        if not token: break
    return list(dict.fromkeys(ids))

# ---------- API (expensive but limited) ----------
def search_ids(query: str, region: str, lang: str, max_results: int, duration: str, order: str) -> List[str]:
    r = youtube.search().list(
        part="id", q=query, type="video", regionCode=region, relevanceLanguage=lang,
        maxResults=max_results, videoDuration=duration, order=order
    ).execute()
    return [it["id"]["videoId"] for it in r.get("items", [])]

def fetch_videos_api(video_ids: List[str]) -> List[dict]:
    out = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        r = youtube.videos().list(part="snippet,statistics,contentDetails", id=",".join(chunk)).execute()
        for it in r.get("items", []):
            sn = it.get("snippet", {}); stt = it.get("statistics", {}); cd = it.get("contentDetails", {})
            thumbs = sn.get("thumbnails", {})
            thumb = None
            for k in ["maxres","standard","high","medium","default"]:
                if k in thumbs and "url" in thumbs[k]:
                    thumb = thumbs[k]["url"]; break
            out.append({
                "videoId": it.get("id"),
                "title": sn.get("title",""),
                "channelId": sn.get("channelId",""),
                "channelTitle": sn.get("channelTitle",""),
                "publishedAt": sn.get("publishedAt",""),
                "views": int(stt.get("viewCount",0)),
                "duration_iso": cd.get("duration",""),
                "thumbnail": thumb
            })
    return out

def fetch_subs_api(channel_ids: List[str]) -> Dict[str, Optional[int]]:
    res = {}
    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i:i+50]
        r = youtube.channels().list(part="statistics", id=",".join(chunk)).execute()
        for it in r.get("items", []):
            subs = it.get("statistics", {}).get("subscriberCount")
            res[it["id"]] = int(subs) if subs is not None else None
    return res

# ---------- UI ----------
with st.sidebar:
    st.header("Регион")
    region = st.selectbox("Страна", ["US","GB","CA","AU"], index=0)
    lang = st.selectbox("Язык", ["en"], index=0)

    st.header("Источники")
    use_popular = st.checkbox("MostPopular (дёшево)", value=True)
    pop_pages = st.slider("MostPopular pages", 1, 10, 3, 1)
    pop_per_page = st.selectbox("MostPopular per page", [10,25,50], index=2)

    use_search = st.checkbox("Добавить Low-Quota Search (чтобы ловить мелкие каналы)", value=True)
    search_budget = st.slider("Лимит search-запросов за запуск", 1, 20, 10, 1)
    search_max_results = st.selectbox("Search results per key", [5,10,25], index=1)
    duration = st.selectbox("Длина", [("medium (4–20 мин)","medium"),("long (20+ мин)","long")], index=0, format_func=lambda x:x[0])[1]
    order = st.selectbox("Search order", [("viewCount","viewCount"),("relevance","relevance")], index=0, format_func=lambda x:x[0])[1]

    keys_text = st.text_area("Ключи (можешь править)", value="\n".join(BEST_US_KEYS), height=220)

    st.header("Кэш")
    v_cache_h = st.slider("Видео кэш (часы)", 1, 72, 12, 1)
    c_cache_h = st.slider("Каналы кэш (часы)", 1, 168, 24, 1)

    st.header("Фильтры (твоя цель)")
    max_subs = st.number_input("Макс subs", 0, 10_000_000, 10_000, 500)
    min_views = st.number_input("Мин views", 0, 1_000_000_000, 20_000, 5000)
    min_ratio = st.number_input("Мин views/subs", 0.0, 100000.0, 3.0, 1.0)

    st.header("Без Shorts")
    min_seconds = st.slider("Мин длительность (сек)", 120, 3600, 240, 30)

    st.header("Сортировка/вид")
    sort_mode = st.selectbox("Сортировать по", ["views_per_day","ratio","views","date"], index=0)
    view_mode = st.radio("Вид", ["Карточки","Таблица"], index=0)

    run = st.button("🔎 Сканировать")

if not run:
    st.stop()

keys = [k.strip() for k in keys_text.splitlines() if k.strip()]

errors = []
try:
    candidate_ids: List[str] = []

    if use_popular:
        candidate_ids.extend(most_popular_ids(region, pop_pages, pop_per_page))

    if use_search and keys:
        # жёстко ограничиваем количество search запросов
        for kw in keys[: int(search_budget)]:
            try:
                candidate_ids.extend(search_ids(kw, region, lang, int(search_max_results), duration, order))
            except HttpError as e:
                errors.append(http_error_text(e))
                break

    candidate_ids = list(dict.fromkeys(candidate_ids))
    if not candidate_ids:
        st.warning("Нет кандидатов. Проверь источники.")
        st.stop()

    # cache videos
    cached_v = get_cached_videos(candidate_ids, int(v_cache_h))
    missing_v = [vid for vid in candidate_ids if vid not in cached_v]
    fresh_v = []
    if missing_v:
        fresh_v = fetch_videos_api(missing_v)
        upsert_videos(fresh_v)

    vids = list({**cached_v, **{it["videoId"]: it for it in fresh_v}}.values())

    # cache subs
    channel_ids = list({v["channelId"] for v in vids if v.get("channelId")})
    cached_s = get_cached_subs(channel_ids, int(c_cache_h))
    missing_s = [cid for cid in channel_ids if cid not in cached_s]
    fresh_s = {}
    if missing_s:
        fresh_s = fetch_subs_api(missing_s)
        upsert_subs(fresh_s)

    subs_map = {**cached_s, **fresh_s}

    rows = []
    for v in vids:
        secs = iso_duration_to_seconds(v.get("duration_iso",""))
        if secs < int(min_seconds):
            continue

        subs = subs_map.get(v.get("channelId",""))
        days = age_days(v.get("publishedAt","")) if v.get("publishedAt") else None
        vpd = (v["views"]/days) if days else None

        ratio = None
        if subs and subs > 0:
            ratio = v["views"]/subs

        if v["views"] < int(min_views):
            continue
        if subs is not None and subs > int(max_subs):
            continue
        if ratio is not None and ratio < float(min_ratio):
            continue

        rows.append({
            "source": "popular/search",
            "title": v["title"],
            "channel": v["channelTitle"],
            "subs": subs,
            "views": v["views"],
            "views_per_day": round(vpd,2) if vpd is not None else None,
            "ratio": round(ratio,2) if ratio is not None else None,
            "duration_sec": secs,
            "publishedAt": v["publishedAt"],
            "thumbnail": v["thumbnail"],
            "url": f"https://www.youtube.com/watch?v={v['videoId']}"
        })

except HttpError as e:
    errors.append(http_error_text(e))

if errors:
    st.error("Ошибка API (квота/настройки).")
    st.code("\n\n".join(errors))
    st.stop()

if not rows:
    st.info(
        "Пока пусто — это нормально при subs<=10k.\n\n"
        "Сделай так, чтобы пошло:\n"
        "- min_ratio = 0–1\n"
        "- min_views = 10000\n"
        "- search_budget = 10\n"
        "- duration = medium\n"
        "Потом ужесточишь."
    )
    st.stop()

df = pd.DataFrame(rows)
if sort_mode == "views_per_day":
    df["_s"] = df["views_per_day"].fillna(-1); df = df.sort_values("_s", ascending=False).drop(columns=["_s"])
elif sort_mode == "ratio":
    df["_s"] = df["ratio"].fillna(-1); df = df.sort_values("_s", ascending=False).drop(columns=["_s"])
elif sort_mode == "views":
    df = df.sort_values("views", ascending=False)
else:
    df = df.sort_values("publishedAt", ascending=False)

st.success(f"Кандидатов: {len(candidate_ids)} | Найдено outliers: {len(df)}")

if view_mode == "Карточки":
    for _, r in df.iterrows():
        c1, c2 = st.columns([1,3])
        with c1:
            if r.get("thumbnail"):
                st.image(r["thumbnail"], use_container_width=True)
        with c2:
            st.markdown(f"**{r['title']}**")
            st.write(f"Channel: {r['channel']}")
            st.write(f"Subs: {fmt_int(r['subs'])} | Views: {fmt_int(r['views'])} | Views/day: {r.get('views_per_day','—')} | Ratio: {r.get('ratio','—')}")
            st.write(f"Duration: {fmt_int(r.get('duration_sec'))} sec | Date: {r['publishedAt']}")
            st.markdown(f"[Open]({r['url']})")
        st.divider()
else:
    st.dataframe(df[["title","channel","subs","views","views_per_day","ratio","duration_sec","publishedAt","url"]], use_container_width=True)

csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
st.download_button("⬇️ Скачать CSV", data=csv_bytes, file_name=f"outliers_{region}.csv", mime="text/csv")
