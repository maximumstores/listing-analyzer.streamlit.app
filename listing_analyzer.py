"""
listing_analyzer.py  —  Конкурентный анализ Amazon листингов
Добавить в pages/ существующего amazon-dashboard репо.
Requires: pip install google-generativeai streamlit
Secrets:  GOOGLE_API_KEY = "AIza..."
"""

import json
import streamlit as st
import google.generativeai as genai

# ── Config ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Анализ листинга",
    page_icon="🔍",
    layout="wide",
)

MODELS = {
    "gemini-2.5-flash":        "⚡ Gemini 2.5 Flash  (быстро, дёшево)",
    "gemini-2.5-pro":          "🧠 Gemini 2.5 Pro    (лучшее качество)",
    "gemini-3-flash-preview":  "🔥 Gemini 3 Flash    (топ скорость)",
    "gemini-2.5-flash-lite":   "💰 Gemini 2.5 Lite   (минимальная цена)",
}

ANALYSIS_SCHEMA = """{
  "summary": "краткое резюме на русском",
  "title_score": 7,
  "title_gaps": ["пробел 1", "пробел 2"],
  "title_advantages": ["преимущество конкурентов 1"],
  "title_rec": "рекомендация",
  "bullets_score": 7,
  "bullets_gaps": ["пробел 1"],
  "bullets_advantages": ["преимущество"],
  "bullets_rec": "рекомендация",
  "desc_score": 7,
  "desc_gaps": ["пробел 1"],
  "desc_advantages": ["преимущество"],
  "desc_rec": "рекомендация",
  "photos_score": 7,
  "photos_gaps": ["пробел 1"],
  "photos_advantages": ["преимущество"],
  "photos_rec": "рекомендация",
  "video_score": 7,
  "video_gaps": ["пробел 1"],
  "video_advantages": ["преимущество"],
  "video_rec": "рекомендация",
  "aplus_score": 7,
  "aplus_gaps": ["пробел 1"],
  "aplus_advantages": ["преимущество"],
  "aplus_rec": "рекомендация",
  "missing_chars": [
    {"name": "название", "how_competitors_use": "как используют конкуренты", "priority": "HIGH"}
  ],
  "tech_params": [
    {"param": "параметр", "competitor_value": "значение у конкурентов", "our_gap": "что добавить нам"}
  ],
  "scenarios": [
    {"scenario": "сценарий использования", "competitors": ["ASIN1"], "how_to_add": "как добавить в листинг"}
  ],
  "numbers": [
    {"metric": "метрика", "competitor_usage": "как используют конкуренты", "suggested": "что добавить нам"}
  ],
  "actions": [
    {"action": "действие", "impact": "HIGH", "effort": "LOW", "details": "детали реализации"}
  ]
}"""


# ── Gemini call ───────────────────────────────────────────────────────────────
def run_analysis(our_url: str, competitor_urls: list[str], model_id: str, log) -> dict:
    api_key = st.secrets.get("GOOGLE_API_KEY", "")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY не найден в secrets")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_id)

    active_competitors = [u.strip() for u in competitor_urls if u.strip()]
    all_urls = [our_url] + active_competitors

    log.write(f"**Модель:** {model_id}  \n")
    log.write(f"**Листингов:** {len(all_urls)} (1 наш + {len(active_competitors)} конкурентов)  \n")
    log.write(f"**URLs:**  \n")
    for i, u in enumerate(all_urls):
        role = "НАШ" if i == 0 else f"Конк.{i}"
        log.write(f"- {role}: `{u}`  \n")
    log.write("\n---\n🌐 Gemini читает страницы через url_context...  \n")

    # Build content with url_context tool
    prompt = f"""Analyze these Amazon product listings for a competitive audit.

OUR LISTING (first URL): {our_url}
COMPETITORS: {', '.join(active_competitors) if active_competitors else 'none provided'}

Use url_context to read each listing page and extract:
- Full title
- All bullet points  
- Product description
- A+ content
- Number and types of photos
- Video presence (yes/no)
- Technical specifications
- BSR rank
- Review count and rating

Then provide detailed competitive analysis comparing OUR listing vs competitors.

Return ONLY valid JSON (no markdown, no explanation) following this exact schema.
All text values must be in Russian. Max 3 items per array.

Schema:
{ANALYSIS_SCHEMA}"""

    # Call with url_context tool (correct Gemini format)
    from google.generativeai import types as gtypes
    response = model.generate_content(
        prompt,
        tools=[gtypes.Tool(url_context=gtypes.UrlContext())],
    )

    raw = response.text.strip()
    log.write(f"✅ Получено {len(raw)} символов от Gemini  \n")

    # Parse JSON
    s = raw.replace("```json", "").replace("```", "").strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"JSON не найден в ответе. Начало: {raw[:200]}")
    s = s[start:end+1]
    # fix trailing commas
    import re
    s = re.sub(r",\s*([}\]])", r"\1", s)
    return json.loads(s)


# ── UI helpers ────────────────────────────────────────────────────────────────
def score_color(score: int) -> str:
    if score >= 8: return "🟢"
    if score >= 6: return "🟡"
    return "🔴"

def priority_badge(p: str) -> str:
    return {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "🟢 LOW"}.get(p, p)

def render_section(label: str, score: int, gaps: list, advantages: list, rec: str):
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"**{label}**")
    with col2:
        st.markdown(f"{score_color(score)} **{score}/10**")
    
    st.progress(score / 10)
    
    if gaps:
        with st.expander(f"⚠️ Пробелы ({len(gaps)})"):
            for g in gaps:
                st.markdown(f"- {g}")
    if advantages:
        with st.expander(f"🏆 У конкурентов ({len(advantages)})"):
            for a in advantages:
                st.markdown(f"- {a}")
    if rec:
        st.info(f"💡 {rec}")


# ── Main UI ───────────────────────────────────────────────────────────────────
st.title("🔍 Конкурентный анализ листинга")
st.caption("Gemini читает Amazon страницы напрямую по URL через url_context")

# ── Sidebar: settings ─────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Настройки")
    model_id = st.selectbox(
        "Модель",
        options=list(MODELS.keys()),
        format_func=lambda x: MODELS[x],
        index=0,
    )
    st.divider()
    st.caption("**Как пользоваться:**")
    st.caption("1. Вставь ссылку на НАШ листинг")
    st.caption("2. Добавь ссылки конкурентов (опционально)")
    st.caption("3. Нажми «Запустить анализ»")
    st.caption("4. Gemini прочитает все страницы и выдаст анализ")

# ── Input: URLs ───────────────────────────────────────────────────────────────
st.subheader("📎 Ссылки на листинги")

our_url = st.text_input(
    "🔵 НАШ листинг (обязательно)",
    value="https://www.amazon.com/dp/B0D6WBQ7G1",
    placeholder="https://www.amazon.com/dp/XXXXXXXXXX",
)

with st.expander("➕ Конкуренты (до 5)", expanded=True):
    competitor_urls = []
    defaults = [
        "https://www.amazon.com/dp/B0BTSZNMVR",
        "https://www.amazon.com/dp/B0FWC67C6H",
        "https://www.amazon.com/dp/B0DFW7YGG7",
        "https://www.amazon.com/dp/B00YWU1PG2",
        "https://www.amazon.com/dp/B0C4Z1R735",
    ]
    for i in range(5):
        u = st.text_input(
            f"Конкурент {i+1}",
            value=defaults[i] if i < len(defaults) else "",
            key=f"comp_{i}",
            placeholder="https://www.amazon.com/dp/XXXXXXXXXX (опционально)",
        )
        competitor_urls.append(u)

# ── Run button ────────────────────────────────────────────────────────────────
st.divider()
if st.button("🚀 Запустить анализ", type="primary", disabled=not our_url.strip()):
    if not our_url.strip():
        st.error("Вставь ссылку на НАШ листинг")
    else:
        with st.spinner("Анализирую..."):
            log_area = st.empty()
            log_lines = []

            class LogWriter:
                def write(self, text):
                    log_lines.append(text)
                    log_area.markdown("".join(log_lines))

            log = LogWriter()
            try:
                result = run_analysis(our_url, competitor_urls, model_id, log)
                st.session_state["analysis_result"] = result
                st.success("✅ Анализ завершён!")
            except Exception as e:
                st.error(f"Ошибка: {e}")
                st.stop()

# ── Results ───────────────────────────────────────────────────────────────────
if "analysis_result" in st.session_state:
    r = st.session_state["analysis_result"]

    st.divider()
    st.subheader("📊 Результаты анализа")

    # Summary
    st.info(f"**Резюме:** {r.get('summary', '')}")

    # Priority actions
    actions = r.get("actions", [])
    if actions:
        st.subheader("🎯 Приоритетные действия")
        for i, a in enumerate(actions):
            with st.container(border=True):
                col1, col2, col3 = st.columns([5, 1, 1])
                with col1:
                    st.markdown(f"**{i+1}. {a.get('action', '')}**")
                with col2:
                    st.markdown(priority_badge(a.get("impact", "MEDIUM")))
                with col3:
                    st.caption(f"Усилия: {a.get('effort', '?')}")
                st.caption(a.get("details", ""))

    # Scores
    st.subheader("📝 Оценки по разделам")
    tabs = st.tabs(["Текст", "Визуал", "A+"])

    with tabs[0]:
        render_section("Title",       r.get("title_score",   0), r.get("title_gaps",   []), r.get("title_advantages",   []), r.get("title_rec",   ""))
        st.divider()
        render_section("Bullets",     r.get("bullets_score", 0), r.get("bullets_gaps", []), r.get("bullets_advantages", []), r.get("bullets_rec", ""))
        st.divider()
        render_section("Description", r.get("desc_score",    0), r.get("desc_gaps",    []), r.get("desc_advantages",    []), r.get("desc_rec",    ""))

    with tabs[1]:
        render_section("Фото", r.get("photos_score", 0), r.get("photos_gaps", []), r.get("photos_advantages", []), r.get("photos_rec", ""))
        st.divider()
        render_section("Видео", r.get("video_score", 0), r.get("video_gaps", []), r.get("video_advantages", []), r.get("video_rec", ""))

    with tabs[2]:
        render_section("A+ контент", r.get("aplus_score", 0), r.get("aplus_gaps", []), r.get("aplus_advantages", []), r.get("aplus_rec", ""))

    # Missing characteristics
    missing = r.get("missing_chars", [])
    if missing:
        st.subheader("🔍 Отсутствующие характеристики")
        for c in missing:
            with st.container(border=True):
                col1, col2 = st.columns([5, 1])
                with col1:
                    st.markdown(f"**{c.get('name', '')}**")
                    st.caption(c.get("how_competitors_use", ""))
                with col2:
                    st.markdown(priority_badge(c.get("priority", "MEDIUM")))

    # Tech params
    tech = r.get("tech_params", [])
    if tech:
        st.subheader("⚙️ Технические параметры")
        for p in tech:
            with st.container(border=True):
                st.markdown(f"**{p.get('param', '')}**")
                col1, col2 = st.columns(2)
                with col1:
                    st.caption(f"🏆 Конкуренты: {p.get('competitor_value', '')}")
                with col2:
                    st.caption(f"→ Нам добавить: {p.get('our_gap', '')}")

    # Scenarios
    scenarios = r.get("scenarios", [])
    if scenarios:
        st.subheader("🎭 Сценарии использования")
        for s in scenarios:
            with st.container(border=True):
                st.markdown(f"**{s.get('scenario', '')}**")
                comps = s.get("competitors", [])
                if comps:
                    st.caption(f"Упоминают: {', '.join(comps)}")
                st.success(f"→ {s.get('how_to_add', '')}")

    # Numbers
    numbers = r.get("numbers", [])
    if numbers:
        st.subheader("🔢 Цифры и конкретика")
        cols = st.columns(3)
        for i, n in enumerate(numbers):
            with st.container(border=True):
                st.markdown(f"**{n.get('metric', '')}**")
                st.caption(f"Конкуренты: {n.get('competitor_usage', '')}")
                st.success(f"→ {n.get('suggested', '')}")

    # Raw JSON (debug)
    with st.expander("🔧 Raw JSON (для отладки)"):
        st.json(r)
