# app.py — YouTube "Most Popular" Outlier Finder (без ключей, выбор страны, без Shorts)
#
# Что делает:
# - Берёт "самые популярные" видео по выбранной стране (YouTube chart=mostPopular)
# - (опционально) по выбранным категориям
# - Убирает Shorts (по длительности)
# - Тянет подписчиков каналов и фильтрует по твоим критериям (subs / views / ratio / views_per_day)
#
# Запуск (локально или на Streamlit Cloud):
#   pip install streamlit google-api-python-client python-dotenv pandas
#   streamlit run app.py
#
# .env (в папке проекта):
#   YOUTUBE_API_KEY=ваш_ключ

import os
import re
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()

st.set_page_config(page_title="YouTube Most Popular Outliers", layout="wide")
st.title("YouTube Most Popular Outliers (без ключей, выбор страны, без Shorts)")

if not API_KEY:
    st.error("Не найден YOUTUBE_API_KEY. Создай файл .env и добавь ключ.")
    st.stop()

youtube = build("youtube", "v3", developerKey=API_KEY)

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
    # "2026-02-25T12:34:56Z"
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

@st.cache_data(ttl=3600)
def fetch_categories(region_code: str):
    # Возвращает список (id, title)
    r = youtube.videoCategories().list(part="snippet", regionCode=region_code).execute()
    cats = []
    for it in r.get("items", []):
        sn = it.get("snippet", {})
        if sn.get("assignable") is True:
            cats.append((it["id"], sn.get("title", it["id"])))
    # на всякий
    cats.sort(key=lambda x: x[1].lower())
    return cats

@st.cache_data(ttl=1800)
def most_popular_video_ids(region_code: str, pages: int, per_page: int, category_id: str | None):
    ids = []
    page_token = None
    for _ in range(pages):
        params = dict(
            part="id",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=per_page,
        )
        if category_id:
            params["videoCategoryId"] = category_id
        if page_token:
            params["pageToken"] = page_token

        r = youtube.videos().list(**params).execute()
        ids.extend([it["id"] for it in r.get("items", [])])
        page_token = r.get("nextPageToken")
        if not page_token:
            break
    # dedupe, preserve order
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
                "likes": int(stt.get("likeCount", 0)) if "likeCount" in stt else None,
                "comments": int(stt.get("commentCount", 0)) if "commentCount" in stt else None,
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
        r = youtube.channels().list(part="statistics,snippet", id=",".join(chunk)).execute()
        for it in r.get("items", []):
            stt = it.get("statistics", {})
            subs = stt.get("subscriberCount")
            result[it["id"]] = int(subs) if subs is not None else None
    return result

# -------- UI --------
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
    st.header("Источник")
    country_choice = st.selectbox(
        "Страна (regionCode)",
        options=COMMON_COUNTRIES,
        index=0,
        format_func=lambda x: f"{x[1]} ({x[0]})"
    )
    region_code = country_choice[0]

    st.caption("Источник данных: YouTube chart=mostPopular. Это не весь YouTube, а трендовый/популярный срез по стране.")

    st.subheader("Объём выборки")
    pages = st.slider("Сколько страниц брать", 1, 10, 3, 1)
    per_page = st.selectbox("Видео на страницу", [10, 25, 50], index=2)
    use_categories = st.checkbox("Сканировать по категориям (шире охват)", value=True)

    st.subheader("Фильтры")
    exclude_shorts = st.checkbox("Исключить Shorts/короткие", value=True)
    min_seconds = st.slider("Мин. длительность (сек)", 60, 1800, 120, 30, disabled=not exclude_shorts)

    min_views = st.number_input("Мин. просмотры", min_value=0, value=50_000, step=10_000)
    max_subs = st.number_input("Макс. подписчики канала", min_value=0, value=200_000, step=10_000)
    min_ratio = st.number_input("Мин. Views/Subs (если subs видны)", min_value=0.0, value=3.0, step=1.0)

    st.subheader("Сортировка/вид")
    sort_mode = st.selectbox("Сортировать по", ["views_per_day", "ratio", "views", "date"], index=0)
    view_mode = st.radio("Вид", ["Карточки (с обложками)", "Таблица"], index=0)

    run = st.button("🔎 Сканировать")

if not run:
    st.write("Выбери страну слева → нажми **Сканировать**.")
    st.stop()

errors = []
try:
    video_ids = []

    if use_categories:
        cats = fetch_categories(region_code)
        # Важно: если выбрать все категории и много страниц — станет тяжело по квоте.
        # Поэтому даём выбрать категории на UI:
        cat_titles = [f"{t} ({cid})" for cid, t in cats]
        selected = st.multiselect(
            "Категории (если пусто — возьмём все assignable)",
            options=cat_titles,
            default=cat_titles[: min(8, len(cat_titles))]  # по умолчанию не всё, чтобы не убить квоту
        )
        # распарсим обратно id
        selected_ids = []
        if selected:
            for s in selected:
                # "... (ID)"
                cid = s.split("(")[-1].split(")")[0].strip()
                selected_ids.append(cid)
        else:
            selected_ids = [cid for cid, _ in cats]

        for cid in selected_ids:
            video_ids.extend(most_popular_video_ids(region_code, pages, per_page, cid))
    else:
        video_ids.extend(most_popular_video_ids(region_code, pages, per_page, None))

    # dedupe
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

        # фильтры
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
            "ratio": round(ratio, 2) if ratio is not None else None,
            "views_per_day": round(vpd, 2) if vpd is not None else None,
            "publishedAt": v["publishedAt"],
            "duration_sec": secs,
            "thumbnail": v["thumbnail"],
            "url": f"https://www.youtube.com/watch?v={v['videoId']}",
        })

except HttpError as e:
    errors.append(str(e))

if errors:
    st.error("Ошибка API (часто квота/ключ/ограничения проекта).")
    st.code("\n\n".join(errors))
    st.stop()

if not rows:
    st.info(
        "Ничего не найдено под текущие фильтры.\n\n"
        "Попробуй:\n"
        "- снизить min_views\n"
        "- поднять max_subs\n"
        "- поставить min_ratio = 0–1 (часто subs скрыты)\n"
        "- увеличить pages/per_page\n"
        "- включить 'по категориям' и выбрать больше категорий"
    )
    st.stop()

df = pd.DataFrame(rows)

# сортировка
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

st.success(f"Найдено видео: {len(df)}")

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
                f"Подписчики: {fmt_int(r['subs'])} | Просмотры: {fmt_int(r['views'])} | "
                f"Views/day: {r.get('views_per_day','—')} | Ratio: {r.get('ratio','—')}\n\n"
                f"Длительность: {fmt_int(r.get('duration_sec'))} сек | Дата: {r['publishedAt']}"
            )
            st.markdown(f"[Открыть видео]({r['url']})")
        st.divider()
else:
    show = df[["title", "channel", "subs", "views", "views_per_day", "ratio", "duration_sec", "publishedAt", "url"]]
    st.dataframe(show, use_container_width=True)

csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
st.download_button("⬇️ Скачать CSV", data=csv_bytes, file_name=f"mostpopular_outliers_{region_code}.csv", mime="text/csv")

st.caption(
    "Важно: chart=mostPopular чаще показывает крупных. Но фильтр по подписчикам вытащит редкие случаи, "
    "когда маленький канал попал в популярное."
)
