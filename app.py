# app.py — YouTube Outlier Finder (без Shorts)
# Запуск:
#   pip install streamlit google-api-python-client python-dotenv pandas
#   streamlit run app.py
#
# .env (в папке проекта):
#   YOUTUBE_API_KEY=ваш_ключ

import os
from datetime import datetime, timezone, date
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()

st.set_page_config(page_title="YouTube Outlier Finder (без Shorts)", layout="wide")
st.title("YouTube Outlier Finder (локально, без Shorts)")

if not API_KEY:
    st.error("Не найден YOUTUBE_API_KEY. Создай файл .env и добавь ключ.")
    st.stop()

youtube = build("youtube", "v3", developerKey=API_KEY)


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


@st.cache_data(ttl=1800)
def search_video_ids(query: str, max_results: int, published_after_iso: str | None,
                     order: str, duration_filter: str, region_code: str | None,
                     relevance_lang: str | None):
    """
    duration_filter: "long" | "medium" | "any"
    order: "viewCount" | "date" | "relevance"
    """
    params = dict(
        part="id",
        q=query,
        type="video",
        maxResults=max_results,
        order=order,
    )
    if published_after_iso:
        params["publishedAfter"] = published_after_iso

    # ВАЖНО: это убирает Shorts на уровне поиска
    if duration_filter in ("long", "medium"):
        params["videoDuration"] = duration_filter  # long >= ~20 мин, medium 4–20 мин

    # Локализация выдачи (не всегда критично, но помогает)
    if region_code:
        params["regionCode"] = region_code
    if relevance_lang:
        params["relevanceLanguage"] = relevance_lang

    r = youtube.search().list(**params).execute()
    return [it["id"]["videoId"] for it in r.get("items", [])]


@st.cache_data(ttl=1800)
def fetch_videos(video_ids: list[str]):
    out = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
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
                "duration_iso": cd.get("duration", ""),  # ISO 8601
                "thumbnail": thumb_url,
            })
    return out


@st.cache_data(ttl=1800)
def fetch_channels_subs(channel_ids: list[str]):
    result = {}
    channel_ids = [c for c in channel_ids if c]
    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i:i + 50]
        r = youtube.channels().list(part="statistics", id=",".join(chunk)).execute()
        for it in r.get("items", []):
            stt = it.get("statistics", {})
            subs = stt.get("subscriberCount")
            result[it["id"]] = int(subs) if subs is not None else None
    return result


# ===== UI =====
with st.sidebar:
    st.header("Поиск и фильтры")

    keywords_text = st.text_area(
        "Ключевые слова (каждое с новой строки)",
        value='psychology facts\n"dark facts"\ntrue crime documentary',
        height=140,
        help='Можно писать фразы в кавычках, например "dark facts".'
    )

    st.subheader("Как искать")
    duration_filter = st.selectbox(
        "Длина видео (убрать Shorts)",
        options=[
            ("Только long (обычно 20+ минут)", "long"),
            ("Только medium (4–20 минут)", "medium"),
            ("Любая длина (может вернуть Shorts)", "any"),
        ],
        index=0,
        format_func=lambda x: x[0],
    )[1]

    order = st.selectbox(
        "Сортировка на этапе поиска (важно!)",
        options=[
            ("По просмотрам (viewCount) — лучше для вирусняка", "viewCount"),
            ("По релевантности (relevance)", "relevance"),
            ("По новизне (date)", "date"),
        ],
        index=0,
        format_func=lambda x: x[0],
    )[1]

    max_videos_per_kw = st.slider("Видео на ключ (чем больше, тем лучше)", 5, 50, 50, 5)

    days_back = st.slider("Искать за последние N дней", 1, 365, 90, 1)

    st.subheader("Пороговые условия")
    min_views = st.number_input("Мин. просмотры", min_value=0, value=20000, step=5000)
    max_subs = st.number_input("Макс. подписчики канала", min_value=0, value=200000, step=10000)
    min_ratio = st.number_input("Мин. Views/Subs (если subs видны)", min_value=0.0, value=3.0, step=1.0)

    st.subheader("Регион/язык (опционально)")
    region_code = st.text_input("regionCode (например RU, US, GB)", value="RU").strip().upper() or None
    relevance_lang = st.text_input("relevanceLanguage (например ru, en)", value="ru").strip().lower() or None

    st.subheader("Вывод")
    sort_mode = st.selectbox("Сортировка результатов", ["ratio", "views_per_day", "views", "date"], index=1)
    view_mode = st.radio("Вид", ["Карточки (с обложками)", "Таблица"], index=0)

    st.caption("Если результатов мало — увеличь days_back, max_videos_per_kw и снизь min_ratio/min_views.")

    run = st.button("🔎 Сканировать")


if not run:
    st.write("Введи ключевые слова слева → нажми **Сканировать**.")
    st.stop()

keywords = [k.strip() for k in keywords_text.splitlines() if k.strip()]
if not keywords:
    st.warning("Введи хотя бы одно ключевое слово.")
    st.stop()

published_after_iso = (datetime.now(timezone.utc) - pd.Timedelta(days=int(days_back))).isoformat().replace("+00:00", "Z")

# анти-мусор (мягко)
BLOCK_WORDS = ["tiktok", "edit", "эдит", "meme", "прикол", "status", "reels"]
# если всё равно хочешь иногда видеть короткие — выключи через duration_filter="any"

all_rows = []
trend_rows = []
errors = []

try:
    for kw in keywords:
        ids = search_video_ids(
            kw, int(max_videos_per_kw), published_after_iso,
            order=order, duration_filter=duration_filter,
            region_code=region_code, relevance_lang=relevance_lang
        )
        ids = list(dict.fromkeys(ids))  # dedupe

        vids = fetch_videos(ids)
        subs_map = fetch_channels_subs(list({v["channelId"] for v in vids}))

        kw_rows = []
        for v in vids:
            title_l = (v.get("title") or "").lower()
            if any(w in title_l for w in BLOCK_WORDS):
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

            row = {
                "keyword": kw,
                "title": v["title"],
                "channel": v["channelTitle"],
                "subs": subs,
                "views": v["views"],
                "ratio": round(ratio, 2) if ratio is not None else None,
                "views_per_day": round(vpd, 2) if vpd is not None else None,
                "publishedAt": v["publishedAt"],
                "thumbnail": v["thumbnail"],
                "url": f"https://www.youtube.com/watch?v={v['videoId']}",
            }
            all_rows.append(row)
            kw_rows.append(row)

        if kw_rows:
            dfk = pd.DataFrame(kw_rows)
            trend_rows.append({
                "direction(keyword)": kw,
                "videos_found": int(len(dfk)),
                "avg_views_per_day": float(dfk["views_per_day"].fillna(0).mean()),
                "max_views_per_day": float(dfk["views_per_day"].fillna(0).max()),
                "outliers_count": int(dfk["ratio"].notna().sum()),
                "total_views": int(dfk["views"].sum()),
            })

except HttpError as e:
    errors.append(str(e))

if errors:
    st.error("Ошибка API. Часто это квота/ключ/ограничения проекта.")
    st.code("\n\n".join(errors))
    st.stop()

if not all_rows:
    st.info(
        "Ничего не найдено под текущие фильтры.\n\n"
        "Попробуй:\n"
        "- увеличить 'Искать за последние N дней' до 180–365\n"
        "- поставить min_ratio = 0–1 (часто subs скрыты)\n"
        "- поднять max_subs до 500k\n"
        "- order=viewCount (уже стоит) и duration=long/medium\n"
        "- увеличить 'Видео на ключ' до 50"
    )
    st.stop()

df = pd.DataFrame(all_rows)

# сортировка результатов
if sort_mode == "ratio":
    df["_sort"] = df["ratio"].fillna(-1)
    df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])
elif sort_mode == "views_per_day":
    df["_sort"] = df["views_per_day"].fillna(-1)
    df = df.sort_values("_sort", ascending=False).drop(columns=["_sort"])
elif sort_mode == "views":
    df = df.sort_values("views", ascending=False)
else:
    df = df.sort_values("publishedAt", ascending=False)

tab1, tab2 = st.tabs(["🎯 Видео (без Shorts)", "🔥 Популярные направления"])

with tab2:
    if trend_rows:
        dft = pd.DataFrame(trend_rows).sort_values("avg_views_per_day", ascending=False)
        st.subheader("Топ направлений по avg views/day (с учётом твоих фильтров)")
        st.dataframe(dft, use_container_width=True)
    else:
        st.info("Нет данных по направлениям — слишком жёсткие фильтры или мало результатов по ключам.")

with tab1:
    st.subheader(f"Найдено видео: {len(df)}")

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
                st.write(f"Направление: {r['keyword']}")
                st.write(f"Канал: {r['channel']}")
                st.write(
                    f"Подписчики: {fmt_int(r['subs'])} | "
                    f"Просмотры: {fmt_int(r['views'])} | "
                    f"Views/day: {r.get('views_per_day','—')} | "
                    f"Ratio: {r.get('ratio','—')}"
                )
                st.write(f"Дата: {r['publishedAt']}")
                st.markdown(f"[Открыть видео]({r['url']})")
            st.divider()
    else:
        show = df[["keyword", "title", "channel", "subs", "views", "views_per_day", "ratio", "publishedAt", "url"]]
        st.dataframe(show, use_container_width=True)

    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Скачать CSV", data=csv_bytes, file_name="outliers.csv", mime="text/csv")
