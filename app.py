# app.py — Ultra Efficient MostPopular Outlier Finder (NO SEARCH, LOW QUOTA)
#
# ✅ Никаких search.list (который сжирает квоту)
# ✅ Только videos.list(chart=mostPopular) + channels.list
# ✅ Выбор страны (regionCode)
# ✅ Убираем Shorts/короткие (по duration)
# ✅ Фильтр: каналы <= 10k subs + views + ratio + views/day
# ✅ Карточки с обложками + таблица + экспорт CSV
#
# Запуск:
#   pip install streamlit google-api-python-client python-dotenv pandas
#   streamlit run app.py
#
# .env:
#   YOUTUBE_API_KEY=ваш_ключ
#
# Streamlit Cloud:
#   Settings → Secrets:
#   YOUTUBE_API_KEY="ваш_ключ"

import os
import re
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- init ----------
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()

st.set_page_config(page_title="MostPopular Outlier Finder (Low Quota)", layout="wide")
st.title("MostPopular Outlier Finder (без поиска, экономно по квоте)")

if not API_KEY:
    st.error("Не найден YOUTUBE_API_KEY. Добавь в .env или в Streamlit Secrets.")
    st.stop()

youtube = build("youtube", "v3", developerKey=API_KEY)

# ---------- helpers ----------
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

def friendly_http_error(e: HttpError) -> str:
    try:
        content = e.content.decode("utf-8", errors="ignore")
    except Exception:
        content = str(e)
    return f"{e}\n\n{content}"

# ---------- API ----------
@st.cache_data(ttl=3600)
def fetch_categories(region_code: str):
    r = youtube.videoCategories().list(part="snippet", regionCode=region_code).execute()
    cats = []
    for it in r.get("items", []):
        sn = it.get("snippet", {})
        if sn.get("assignable") is True:
            cats.append((it["id"], sn.get("title", it["id"])))
    cats.sort(key=lambda x: x[1].lower())
    return cats

@st.cache_data(ttl=1800)
def most_popular_video_ids(region_code: str, pages: int, per_page: int, category_id: str | None):
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

@st.cache_data(ttl=1800)
def fetch_videos(video_ids: list[str]):
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

@st.cache_data(ttl=1800)
def fetch_channels_subs(channel_ids: list[str]):
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

# ---------- UI ----------
COMMON_COUNTRIES = [
    ("US", "United States"),
    ("GB", "United Kingdom"),
    ("CA", "Canada"),
    ("AU", "Australia"),
    ("DE", "Germany"),
    ("FR", "France"),
    ("NL", "Netherlands"),
    ("ES", "Spain"),
    ("IT", "Italy"),
    ("BR", "Brazil"),
    ("MX", "Mexico"),
    ("JP", "Japan"),
    ("KR", "South Korea"),
    ("IN", "India"),
    ("RU", "Russia"),
    ("UA", "Ukraine"),
    ("PL", "Poland"),
    ("SE", "Sweden"),
    ("NO", "Norway"),
    ("TR", "Turkey"),
]

with st.sidebar:
    st.header("Страна")
    country_choice = st.selectbox(
        "regionCode",
        options=COMMON_COUNTRIES,
        index=0,
        format_func=lambda x: f"{x[1]} ({x[0]})",
    )
    region_code = country_choice[0]

    st.header("Охват (дёшево по квоте)")
    scan_by_categories = st.checkbox("Сканировать по категориям (шире)", value=True)
    pages = st.slider("Страниц на категорию", 1, 10, 3, 1)
    per_page = st.selectbox("Видео на страницу", [10, 25, 50], index=2)

    st.header("Фильтры")
    exclude_shorts = st.checkbox("Убрать Shorts/короткие", value=True)
    min_seconds = st.slider("Мин. длительность (сек)", 60, 3600, 120, 30, disabled=not exclude_shorts)

    # твоя цель: каналы <= 10k
    max_subs = st.number_input("Макс. подписчики канала", min_value=0, value=10_000, step=500)
    min_views = st.number_input("Мин. просмотры", min_value=0, value=50_000, step=10_000)
    min_ratio = st.number_input("Мин. Views/Subs (если subs видны)", min_value=0.0, value=3.0, step=1.0)

    st.header("Сортировка/вид")
    sort_mode = st.selectbox("Сортировать по", ["views_per_day", "ratio", "views", "date"], index=0)
    view_mode = st.radio("Вид", ["Карточки (с обложками)", "Таблица"], index=0)

    run = st.button("🔎 Сканировать")

if not run:
    st.write("Выбери страну слева → нажми **Сканировать**.")
    st.stop()

errors = []
try:
    video_ids = []

    if scan_by_categories:
        cats = fetch_categories(region_code)
        cat_options = [f"{title} ({cid})" for cid, title in cats]
        default_pick = cat_options[: min(10, len(cat_options))]

        selected = st.multiselect(
            "Категории (выбери больше — больше охват)",
            options=cat_options,
            default=default_pick,
        )
        selected_ids = []
        for s in selected:
            cid = s.split("(")[-1].split(")")[0].strip()
            selected_ids.append(cid)

        # если юзер ничего не выбрал — возьмём все (но это дольше)
        if not selected_ids:
            selected_ids = [cid for cid, _ in cats]

        for cid in selected_ids:
            video_ids.extend(most_popular_video_ids(region_code, pages, per_page, cid))
    else:
        video_ids.extend(most_popular_video_ids(region_code, pages, per_page, None))

    video_ids = list(dict.fromkeys(video_ids))

    vids = fetch_videos(video_ids)
    subs_map = fetch_channels_subs(list({v["channelId"] for v in vids}))

    rows = []
    for v in vids:
        secs = iso_duration_to_seconds(v.get("duration_iso", ""))

        if exclude_shorts and secs < int(min_seconds):
            continue

        subs = subs_map.get(v["channelId"])
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
    errors.append(friendly_http_error(e))

if errors:
    st.error("Ошибка API (квота/ключ/настройки проекта).")
    st.code("\n\n".join(errors))
    st.stop()

if not rows:
    st.info(
        "Ничего не найдено под текущие фильтры.\n\n"
        "Важно: mostPopular редко содержит каналы ≤10k.\n\n"
        "Чтобы чаще находило:\n"
        "- снизить min_views до 10k–20k\n"
        "- снизить min_ratio до 0–1\n"
        "- увеличить pages/per_page\n"
        "- выбрать больше категорий"
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

st.success(f"Найдено видео: {len(df)} (из кандидатов: {len(video_ids)})")

if view_mode.startswith("Карточки"):
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
st.download_button(
    "⬇️ Скачать CSV",
    data=csv_bytes,
    file_name=f"mostpopular_outliers_{region_code}.csv",
    mime="text/csv",
)

st.caption(
    "Это максимально экономно по квоте (без search). "
    "Но mostPopular редко включает каналы ≤10k. "
    "Если хочешь реально много outliers маленьких каналов — нужно добавить 'экономный search' (10–15 запросов/день)."
)
