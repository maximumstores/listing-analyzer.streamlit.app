"""
listing_analyzer.py — Конкурентный анализ Amazon листингов
Отдельное Streamlit приложение.
pip install streamlit requests
Secrets: GOOGLE_API_KEY = "AIza..."
"""

import json
import re
import requests
import streamlit as st

MODELS = {
    "gemini-2.5-flash":       "⚡ Gemini 2.5 Flash  (быстро, дёшево)",
    "gemini-2.5-pro":         "🧠 Gemini 2.5 Pro    (лучшее качество)",
    "gemini-3-flash-preview": "🔥 Gemini 3 Flash    (топ скорость)",
    "gemini-2.5-flash-lite":  "💰 Gemini 2.5 Lite   (минимальная цена)",
}

SCHEMA = '{"summary":"...","title_score":7,"title_gaps":["x"],"title_advantages":["x"],"title_rec":"x","bullets_score":7,"bullets_gaps":["x"],"bullets_advantages":["x"],"bullets_rec":"x","desc_score":7,"desc_gaps":["x"],"desc_advantages":["x"],"desc_rec":"x","photos_score":7,"photos_gaps":["x"],"photos_advantages":["x"],"photos_rec":"x","video_score":7,"video_gaps":["x"],"video_advantages":["x"],"video_rec":"x","aplus_score":7,"aplus_gaps":["x"],"aplus_advantages":["x"],"aplus_rec":"x","missing_chars":[{"name":"x","how_competitors_use":"x","priority":"HIGH"}],"tech_params":[{"param":"x","competitor_value":"x","our_gap":"x"}],"scenarios":[{"scenario":"x","competitors":[],"how_to_add":"x"}],"numbers":[{"metric":"x","competitor_usage":"x","suggested":"x"}],"actions":[{"action":"x","impact":"HIGH","effort":"LOW","details":"x"}]}'

def run_analysis(our_url, competitor_urls, model_id, log):
    api_key = st.secrets.get("GOOGLE_API_KEY", "")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY не найден в secrets")

    active = [u.strip() for u in competitor_urls if u.strip()]
    all_urls = [our_url] + active

    log("f**Модель:** " + model_id)
    log(f"**Листингов:** {len(all_urls)}")
    for i, u in enumerate(all_urls):
        log(f"- {'НАШ' if i==0 else f'Конк.{i}'}: `{u}`")
    log("🌐 Gemini читает страницы через url_context...")

    prompt = f"""Analyze these Amazon product listing pages for a competitive audit.

OUR LISTING: {our_url}
COMPETITORS: {chr(10).join(active) if active else 'none'}

Read each URL using url_context. For each listing extract: full title, all bullet points, description, A+ content, photo count/types, video presence, technical specs, BSR rank, review count/rating, price.

Then give a detailed competitive analysis comparing OUR listing vs competitors.

Return ONLY valid JSON (no markdown, no code blocks, no explanation).
All text in Russian. Max 3 items per array.
Schema: {SCHEMA}"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}:generateContent?key={api_key}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"url_context": {}}],
        "generationConfig": {"maxOutputTokens": 8192},
    }

    resp = requests.post(url, json=body, timeout=120)
    if not resp.ok:
        raise ValueError(f"Gemini API error {resp.status_code}: {resp.text[:300]}")

    data = resp.json()
    raw = ""
    for part in data.get("candidates", [{}])[0].get("content", {}).get("parts", []):
        if "text" in part:
            raw += part["text"]

    log(f"✅ Получено {len(raw)} символов")

    s = raw.strip().replace("```json", "").replace("```", "").strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"JSON не найден. Ответ: {raw[:300]}")
    s = re.sub(r",\s*([}\]])", r"\1", s[start:end+1])
    return json.loads(s)


# ── UI helpers ────────────────────────────────────────────────────────────────
def score_color(s):
    return "🟢" if s >= 8 else ("🟡" if s >= 6 else "🔴")

def badge(p):
    return {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "🟢 LOW"}.get(p, p)

def section(label, score, gaps, advantages, rec):
    c1, c2 = st.columns([4, 1])
    c1.markdown(f"**{label}**")
    c2.markdown(f"{score_color(score)} **{score}/10**")
    st.progress(score / 10)
    if gaps:
        with st.expander(f"⚠️ Пробелы ({len(gaps)})"):
            for g in gaps: st.markdown(f"- {g}")
    if advantages:
        with st.expander(f"🏆 У конкурентов ({len(advantages)})"):
            for a in advantages: st.markdown(f"- {a}")
    if rec:
        st.info(f"💡 {rec}")


# ── App ───────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Анализ листинга", page_icon="🔍", layout="wide")
st.title("🔍 Конкурентный анализ листинга")
st.caption("Gemini читает Amazon страницы напрямую по URL — просто вставь ссылки")

with st.sidebar:
    st.header("⚙️ Модель")
    model_id = st.selectbox("", options=list(MODELS.keys()), format_func=lambda x: MODELS[x])
    st.divider()
    st.caption("1. Вставь ссылки  \n2. Нажми Запустить  \n3. Gemini читает страницы  \n4. Получи анализ")

st.subheader("📎 Ссылки")
our_url = st.text_input("🔵 НАШ листинг", value="https://www.amazon.com/dp/B0D6WBQ7G1")

with st.expander("➕ Конкуренты (до 5)", expanded=True):
    defaults = [
        "",
        "",
        "",
        "",
        "",
    ]
    competitor_urls = [
        st.text_input(f"Конкурент {i+1}", value=defaults[i], key=f"c{i}")
        for i in range(5)
    ]

st.divider()
if st.button("🚀 Запустить анализ", type="primary"):
    lines = []
    ph = st.empty()
    def log(msg):
        lines.append(msg)
        ph.markdown("\n\n".join(lines))

    try:
        r = run_analysis(our_url, competitor_urls, model_id, log)
        st.session_state["result"] = r
        st.success("✅ Готово!")
    except Exception as e:
        st.error(f"Ошибка: {e}")
        st.stop()

if "result" in st.session_state:
    r = st.session_state["result"]
    st.divider()
    st.subheader("📊 Результаты")
    st.info(f"**Резюме:** {r.get('summary','')}")

    # Priority actions
    if r.get("actions"):
        st.subheader("🎯 Приоритетные действия")
        for i, a in enumerate(r["actions"]):
            with st.container(border=True):
                c1, c2, c3 = st.columns([5,1,1])
                c1.markdown(f"**{i+1}. {a.get('action','')}**")
                c2.markdown(badge(a.get("impact","MEDIUM")))
                c3.caption(f"Усилия: {a.get('effort','?')}")
                st.caption(a.get("details",""))

    # Scores
    st.subheader("📝 Разделы")
    t1, t2, t3 = st.tabs(["Текст", "Визуал", "A+"])
    with t1:
        section("Title",       r.get("title_score",0),   r.get("title_gaps",[]),   r.get("title_advantages",[]),   r.get("title_rec",""))
        st.divider()
        section("Bullets",     r.get("bullets_score",0), r.get("bullets_gaps",[]), r.get("bullets_advantages",[]), r.get("bullets_rec",""))
        st.divider()
        section("Description", r.get("desc_score",0),    r.get("desc_gaps",[]),    r.get("desc_advantages",[]),    r.get("desc_rec",""))
    with t2:
        section("Фото",  r.get("photos_score",0), r.get("photos_gaps",[]), r.get("photos_advantages",[]), r.get("photos_rec",""))
        st.divider()
        section("Видео", r.get("video_score",0),  r.get("video_gaps",[]),  r.get("video_advantages",[]),  r.get("video_rec",""))
    with t3:
        section("A+", r.get("aplus_score",0), r.get("aplus_gaps",[]), r.get("aplus_advantages",[]), r.get("aplus_rec",""))

    if r.get("missing_chars"):
        st.subheader("🔍 Отсутствующие характеристики")
        for c in r["missing_chars"]:
            with st.container(border=True):
                col1, col2 = st.columns([5,1])
                col1.markdown(f"**{c.get('name','')}**")
                col1.caption(c.get("how_competitors_use",""))
                col2.markdown(badge(c.get("priority","MEDIUM")))

    if r.get("tech_params"):
        st.subheader("⚙️ Технические параметры")
        for p in r["tech_params"]:
            with st.container(border=True):
                st.markdown(f"**{p.get('param','')}**")
                c1, c2 = st.columns(2)
                c1.caption(f"🏆 {p.get('competitor_value','')}")
                c2.caption(f"→ {p.get('our_gap','')}")

    if r.get("scenarios"):
        st.subheader("🎭 Сценарии использования")
        for s in r["scenarios"]:
            with st.container(border=True):
                st.markdown(f"**{s.get('scenario','')}**")
                if s.get("competitors"): st.caption(f"Упоминают: {', '.join(s['competitors'])}")
                st.success(f"→ {s.get('how_to_add','')}")

    if r.get("numbers"):
        st.subheader("🔢 Цифры и конкретика")
        for n in r["numbers"]:
            with st.container(border=True):
                st.markdown(f"**{n.get('metric','')}**")
                c1, c2 = st.columns(2)
                c1.caption(f"Конкуренты: {n.get('competitor_usage','')}")
                c2.success(f"→ {n.get('suggested','')}")

    with st.expander("🔧 Raw JSON"):
        st.json(r)
