# app.py — US Long-Form Outlier Finder (маленькие каналы <=10k, много просмотров)
#
# Что делает:
# 1) Берёт “пиздатый” преднастроенный список ключей (US/EN, заточен под лонги)
# 2) Ищет через YouTube Search API (region=US, lang=en, order=viewCount)
# 3) Убирает Shorts (videoDuration=medium/long + доп. отсев по секундам)
# 4) Тянет views + subs, считает ratio и views/day
# 5) Показывает карточки с обложками + таблицу + “топ направлений”
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

# ----------------- KEYWORDS (US / EN, long-form friendly) -----------------
BEST_US_LONGFORM_KEYWORDS = [
    # TRUE CRIME / INTERROGATION / CASES (очень сильный спрос в US)
    "true crime documentary",
    "unsolved mysteries documentary",
    "interrogation analysis",
    "police interrogation full",
    "criminal psychology documentary",
    "cold case documentary",
    "missing person case documentary",
    "court trial full",
    "bodycam footage full",
    "detective documentary",

    # DARK PSYCHOLOGY / RELATIONSHIPS (высокий CPM + удержание)
    "dark psychology documentary",
    "psychology of manipulation",
    "gaslighting explained",
    "narcissistic personality disorder documentary",
    "how narcissists think",
    "toxic relationship psychology",
    "attachment styles explained",
    "trauma bonding explained",
    "human behavior documentary",
    "cognitive biases explained documentary",

    # SURVIVAL / DISASTERS / ACCIDENTS (CTR и ретеншн)
    "real survival stories documentary",
    "plane crash documentary",
    "maritime disaster documentary",
    "mountain survival documentary",
    "trapped for days documentary",
    "true disaster documentary",
    "industrial disaster documentary",
    "train crash documentary",
    "shipwreck documentary",
    "lost in the wilderness documentary",

    # SPACE / SCIENCE MYSTERIES (evergreen + стабильный трафик)
    "space documentary",
    "black hole documentary",
    "james webb discoveries documentary",
    "nasa discoveries documentary",
    "universe explained documentary",
    "cosmic mysteries documentary",
    "ancient earth documentary",
    "deep ocean documentary",
    "science documentary full",
    "mysteries of the universe documentary",

    # MONEY / SCAMS / INVESTIGATIONS (дорогая реклама + вирусность)
    "scam documentary",
    "fraud documentary",
    "financial crime documentary",
    "how money works documentary",
    "psychology of money documentary",
    "inside the company documentary",
    "business documentary full",
    "tech scandal documentary",
    "cult documentary",
    "conspiracy documentary",

    # HISTORY / DARK HISTORY
    "dark history documentary",
    "untold history documentary",
    "ancient mysteries documentary",
    "war documentary full",
    "cold war documentary",
    "spy documentary",
    "secret missions documentary",
    "forbidden history documentary",
]

# ----------------- Setup -----------------
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()

st.set_page_config(page_title="US Long-Form Outlier Finder", layout="wide")
st.title("US Long-Form Outlier Finder (каналы ≤ 10k, без Shorts)")

if not API_KEY:
    st.error("Не найден YOUTUBE_API_KEY. Добавь в .env или в Streamlit Secrets.")
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

# ----------------- API helpers -----------------
@st.cache_data(ttl=1800)
def search_video_ids(query: str, max_results: int, order: str, duration: str,
                     published_after_iso: str | None, region_code: str, lang: str):
    """
    duration: "medium" | "long"
    order: "viewCount" | "relevance" | "date"
    """
    params = {
        "part": "id",
        "q": query,
        "type": "video",
        "order": order,
        "maxResults": int(max_results),
        "regionCode": region_code,
        "relevanceLanguage": lang,
        "videoDuration": duration,  # ✅ отсев Shorts на уровне поиска
    }
    if published_after_iso:
        params["publishedAfter"] = published_after_iso

    r = youtube.search().list(**params).execute()
    return [it["id"]["videoId"] for it in r.get("items", [])]

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

# ----------------- UI -----------------
with st.sidebar:
    st.header("Источник")
    region_code = st.selectbox("Страна", ["US"], index=0)
    lang = st.selectbox("Язык релевантности", ["en"], index=0)

    st.header("Ключи (можешь редактировать)")
    use_default = st.checkbox("Использовать преднастроенные 'топ' ключи", value=True)
    if use_default:
        keywords_text = st.text_area(
            "Ключевые слова (каждое с новой строки)",
            value="\n".join(BEST_US_LONGFORM_KEYWORDS),
            height=260,
        )
    else:
        keywords_text = st.text_area(
            "Ключевые слова (каждое с новой строки)",
            value='true crime documentary\npsychology of manipulation\nspace documentary',
            height=260,
        )

    st.header("Поиск")
    duration = st.selectbox("Длина (без Shorts)", [("medium (4–20 мин)", "medium"), ("long (20+ мин)", "long")],
                            index=0, format_func=lambda x: x[0])[1]
    order = st.selectbox("Режим поиска", [("viewCount (лучше для вирусняка)", "viewCount"),
                                         ("relevance", "relevance"),
                                         ("date", "date")],
                         index=0, format_func=lambda x: x[0])[1]
    max_videos_per_kw = st.slider("Видео на ключ", 5, 50, 25, 5)
    days_back = st.slider("Искать видео за последние N дней", 1, 365, 365, 1)

    st.header("Фильтры outlier")
    # твоя цель: до 10k
    max_subs = st.number_input("Макс. подписчики канала", min_value=0, value=10_000, step=500)
    min_views = st.number_input("Мин. просмотры", min_value=0, value=20_000, step=5_000)
    min_ratio = st.number_input("Мин. Views/Subs (если subs видны)", min_value=0.0, value=5.0, step=1.0)

    st.header("Отсев Shorts (доп. защита)")
    # medium/long уже режет, но добавим “железобетон”
    min_seconds = st.slider("Мин. длительность (сек)", 120, 3600, 240, 30)

    st.header("Вывод")
    sort_mode = st.selectbox("Сортировать по", ["views_per_day", "ratio", "views", "date"], index=0)
    view_mode = st.radio("Вид", ["Карточки (с обложками)", "Таблица"], index=0)

    run = st.button("🔎 Сканировать")

if not run:
    st.write("Слева выбери параметры → нажми **Сканировать**.")
    st.stop()

keywords = [k.strip() for k in keywords_text.splitlines() if k.strip()]
if not keywords:
    st.warning("Список ключей пустой.")
    st.stop()

published_after_iso = (datetime.now(timezone.utc) - pd.Timedelta(days=int(days_back))).isoformat().replace("+00:00", "Z")

# лёгкий анти-мусор (можешь расширить)
BLOCK_WORDS = ["shorts", "tiktok", "edit", "reels", "meme", "compilation", "reaction"]

errors = []
all_rows = []
trend_rows = []

try:
    for kw in keywords:
        ids = search_video_ids(
            query=kw,
            max_results=int(max_videos_per_kw),
            order=order,
            duration=duration,
            published_after_iso=published_after_iso,
            region_code=region_code,
            lang=lang,
        )
        ids = list(dict.fromkeys(ids))
        if not ids:
            continue

        vids = fetch_videos(ids)
        subs_map = fetch_channels_subs(list({v["channelId"] for v in vids}))

        kw_rows = []
        for v in vids:
            title_l = (v.get("title") or "").lower()
            if any(w in title_l for w in BLOCK_WORDS):
                continue

            secs = iso_duration_to_seconds(v.get("duration_iso", ""))
            if secs < int(min_seconds):
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
            # если subs скрыты (None) — оставляем, но ты можешь легко выключить это
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
                "duration_sec": secs,
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
    errors.append(friendly_http_error(e))

if errors:
    st.error("Ошибка API (ключ/квота/настройки проекта).")
    st.code("\n\n".join(errors))
    st.stop()

if not all_rows:
    st.info(
        "Пусто по текущим фильтрам.\n\n"
        "Чтобы точно пошло:\n"
        "- min_ratio = 0–2\n"
        "- min_views = 10000\n"
        "- days_back = 365\n"
        "- duration = medium\n"
        "- max_videos_per_kw = 50\n"
        "Потом ужесточай."
    )
    st.stop()

df = pd.DataFrame(all_rows)

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

tab1, tab2 = st.tabs(["🎯 Outlier-видео", "🔥 Топ направлений (по твоим фильтрам)"])

with tab2:
    if trend_rows:
        dft = pd.DataFrame(trend_rows).sort_values("avg_views_per_day", ascending=False)
        st.dataframe(dft, use_container_width=True)
    else:
        st.info("Нет данных по направлениям (слишком жёсткие фильтры или мало результатов).")

with tab1:
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
                st.write(f"Ключ: {r['keyword']}")
                st.write(f"Канал: {r['channel']}")
                st.write(
                    f"Subs: {fmt_int(r['subs'])} | Views: {fmt_int(r['views'])} | "
                    f"Views/day: {r.get('views_per_day','—')} | Ratio: {r.get('ratio','—')} | "
                    f"Dur: {fmt_int(r.get('duration_sec'))} sec"
                )
                st.write(f"Дата: {r['publishedAt']}")
                st.markdown(f"[Открыть видео]({r['url']})")
            st.divider()
    else:
        show = df[["keyword","title","channel","subs","views","views_per_day","ratio","duration_sec","publishedAt","url"]]
        st.dataframe(show, use_container_width=True)

    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Скачать CSV", data=csv_bytes, file_name="us_outliers_longform.csv", mime="text/csv")

st.caption(
    "Подсказка: если много 'None' в subs (скрытые подписчики) — ставь сортировку views/day и ratio=0, "
    "а потом вручную отбрасывай мусор."
)
