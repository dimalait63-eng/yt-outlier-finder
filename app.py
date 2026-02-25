# app.py — YouTube Most Popular Outliers (без ключевых слов, выбор страны, без Shorts)
#
# Идея:
# - Берём YouTube videos.list(chart="mostPopular") по выбранной стране
# - (опционально) расширяем охват, проходя по категориям
# - Тянем данные видео + подписчиков канала
# - Фильтруем по твоим критериям (views, subs, ratio, views/day)
# - Убираем Shorts/короткие по длительности
#
# Запуск:
#   pip install streamlit google-api-python-client python-dotenv pandas
#   streamlit run app.py
#
# .env:
#   YOUTUBE_API_KEY=ваш_ключ

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

st.set_page_config(page_title="YouTube Most Popular Outliers", layout="wide")
st.title("YouTube Most Popular Outliers (без ключей, выбор страны, без Shorts)")

if not API_KEY:
    st.error("Не найден YOUTUBE_API_KEY. Создай файл .env и добавь ключ.")
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
    # Список assignable категорий для региона
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
    # ✅ ВАЖНО: videos.list НЕ поддерживает part="id". Нужно part="snippet" (или другое валидное).
    ids = []
    page_token = None
    for _ in range(pages):
        params = dict(
            part="snippet",
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
    st.header("Источник")
    country_choice = st.selectbox(
        "Страна (regionCode)",
        options=COMMON_COUNTRIES,
        index=0,
        format_func=lambda x: f"{x[1]} ({x[0]})",
    )
    region_code = country_choice[0]

    st.caption("Берём данные из YouTube chart=mostPopular по стране (это не весь YouTube, но хороший 'популярный' срез).")

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

# ---------- run ----------
errors: list[str] = []

try:
    video_ids: list[str] = []

    if use_categories:
        cats = fetch_categories(region_code)
        cat_options = [f"{title} ({cid})" for cid, title in cats]

        # По умолчанию не выбираем ВСЕ, чтобы не убить квоту: можно расширить руками
        default_pick = cat_options[: min(8, len(cat_options))]

        selected = st.multiselect(
            "Категории (если пусто — возьмём все assignable)",
            options=cat_options,
            default=default_pick,
        )

        selected_ids: list[str] = []
        if selected:
            for s in selected:
                cid = s.split("(")[-1].split(")")[0].strip()
                selected_ids.append(cid)
        else:
            selected_ids = [cid for cid, _ in cats]

        for cid in selected_ids:
            video_ids.extend(most_popular_video_ids(region_code, int(pages), int(per_page), cid))
    else:
        video_ids.extend(most_popular_video_ids(region_code, int(pages), int(per_page), None))

    video_ids = list(dict.fromkeys(video_ids))  # dedupe

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
    st.error("Ошибка API (квота/ключ/ограничения проекта или неправильные настройки).")
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
        "- включить 'по категориям' и выбрать больше категорий\n"
        "- уменьшить 'Мин. длительность' (если срез слишком узкий)"
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
                f"Подписчики: {fmt_int(r['subs'])} | "
                f"Просмотры: {fmt_int(r['views'])} | "
                f"Views/day: {r.get('views_per_day','—')} | "
                f"Ratio: {r.get('ratio','—')}\n\n"
                f"Длительность: {fmt_int(r.get('duration_sec'))} сек | "
                f"Дата: {r['publishedAt']}"
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
    "Примечание: chart=mostPopular часто доминируют крупные каналы. "
    "Фильтр по подписчикам вытащит редкие случаи, где маленький канал попал в популярное."
)
