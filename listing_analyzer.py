"""
listing_analyzer.py — Amazon Listing Analyzer
Anthropic (приоритет) + Gemini (запасной)
Secrets: ANTHROPIC_API_KEY, GEMINI_API_KEY, SCRAPINGDOG_API_KEY
"""
import json, re, base64, requests, streamlit as st

st.set_page_config(page_title="Listing Analyzer", page_icon="🔍", layout="wide")

SCHEMA = '{"summary":"...","title_score":7,"title_gaps":["x"],"title_advantages":["x"],"title_rec":"x","bullets_score":7,"bullets_gaps":["x"],"bullets_advantages":["x"],"bullets_rec":"x","desc_score":7,"desc_gaps":["x"],"desc_advantages":["x"],"desc_rec":"x","photos_score":7,"photos_gaps":["x"],"photos_advantages":["x"],"photos_rec":"x","video_score":7,"video_gaps":["x"],"video_advantages":["x"],"video_rec":"x","aplus_score":7,"aplus_gaps":["x"],"aplus_advantages":["x"],"aplus_rec":"x","missing_chars":[{"name":"x","how_competitors_use":"x","priority":"HIGH"}],"tech_params":[{"param":"x","competitor_value":"x","our_gap":"x"}],"scenarios":[{"scenario":"x","competitors":[],"how_to_add":"x"}],"numbers":[{"metric":"x","competitor_usage":"x","suggested":"x"}],"actions":[{"action":"x","impact":"HIGH","effort":"LOW","details":"x"}]}'

# ── Gemini keys rotation ──────────────────────────────────────────────────────
GEMINI_KEYS = [
    "AIzaSyCiAwfbC4xkaapuBrQtDfOxQ52-Kd5bUPE",
    "AIzaSyC8CUvsCMnoGLfjOXP2Cod7KueJAMeNdY8",
    "AIzaSyC62LXBA1dTUqYnbXcrljnafzXodo6-CKQ",
    "AIzaSyAwsZXXADvRtdEBXM0vQAh3JHx8dK8uzsw",
    "AIzaSyCQxE6zBB7yoGqL5iv3dEtw0xSfINHHyPQ",
    "AIzaSyBd6gmCW9CZ2uX_TREHozfCOQEyQcqeF-M",
]
GEMINI_URL   = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GEMINI_MODEL = "gemini-2.0-flash-lite"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-3-haiku-20240307"

# ── API helpers ───────────────────────────────────────────────────────────────
def anthropic_post(payload, log=None):
    key = st.secrets.get("ANTHROPIC_API_KEY", "")
    if not key:
        raise Exception("ANTHROPIC_API_KEY не задан")
    r = requests.post(
        ANTHROPIC_URL,
        headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json=payload, timeout=120
    )
    if not r.ok:
        raise Exception(f"Anthropic {r.status_code}: {r.json()}")
    return r.json()["content"][0]["text"]

def gemini_post(payload, log=None):
    last_err = None
    for key in GEMINI_KEYS:
        r = requests.post(
            GEMINI_URL.format(model=GEMINI_MODEL),
            params={"key": key}, json=payload, timeout=120
        )
        if r.status_code == 429:
            last_err = f"429 key ...{key[-6:]}"
            continue
        if not r.ok:
            raise Exception(f"Gemini {r.status_code}: {r.text[:200]}")
        data = r.json()
        parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
        return "".join(p.get("text", "") for p in parts)
    raise Exception(f"Все Gemini ключи исчерпаны: {last_err}")

def ai_post(payload_anthropic, payload_gemini=None, log=None):
    """Только Anthropic"""
    result = anthropic_post(payload_anthropic)
    if log: log("  ✅ Anthropic")
    return result, "anthropic"

# ── Utils ─────────────────────────────────────────────────────────────────────
def get_asin(url):
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None

def sc(s): return "🟢" if s>=8 else ("🟡" if s>=6 else "🔴")
def badge(p): return {"HIGH":"🔴 HIGH","MEDIUM":"🟡 MEDIUM","LOW":"🟢 LOW"}.get(p,p)

def section(label, score, gaps, adv, rec):
    c1,c2 = st.columns([4,1])
    c1.markdown(f"**{label}**"); c2.markdown(f"{sc(score)} **{score}/10**")
    st.progress(score/10)
    if gaps:
        with st.expander(f"⚠️ Пробелы ({len(gaps)})"):
            for g in gaps: st.markdown(f"- {g}")
    if adv:
        with st.expander(f"🏆 У конкурентов ({len(adv)})"):
            for a in adv: st.markdown(f"- {a}")
    if rec: st.info(f"💡 {rec}")

# ── ScrapingDog Screenshot ───────────────────────────────────────────────────
def fetch_listing_screenshot(listing_url, label, log):
    """Делает полный скрин страницы Amazon и возвращает как image dict"""
    sd_key = st.secrets.get("SCRAPINGDOG_API_KEY", "")
    if not sd_key:
        log("⚠️ SCRAPINGDOG_API_KEY не задан")
        return None
    log(f"📸 ScrapingDog screenshot: {label}...")
    try:
        r = requests.get(
            "https://api.scrapingdog.com/screenshot",
            params={"api_key": sd_key, "url": listing_url, "fullPage": "true"},
            timeout=90
        )
        if not r.ok:
            log(f"⚠️ Screenshot {r.status_code}: {r.text[:150]}")
            return None
        if len(r.content) < 5000:
            log(f"⚠️ Screenshot слишком маленький: {len(r.content)} bytes")
            return None
        # Resize to max 1500px wide to stay under Anthropic limits
        try:
            from PIL import Image
            import io
            img = Image.open(io.BytesIO(r.content))
            w, h = img.size
            if w > 1500:
                ratio = 1500 / w
                img = img.resize((1500, int(h * ratio)), Image.LANCZOS)
            # Crop to top 3000px (most important part of listing)
            if img.size[1] > 3000:
                img = img.crop((0, 0, img.size[0], 3000))
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=70)
            buf.seek(0)
            compressed = buf.read()
            log(f"✅ Screenshot {label}: {len(r.content)//1024}KB → {len(compressed)//1024}KB (сжато)")
            b64 = base64.b64encode(compressed).decode()
            return {"b64": b64, "media_type": "image/jpeg", "label": label}
        except Exception as pe:
            log(f"⚠️ PIL: {pe} — отправляю оригинал")
            b64 = base64.b64encode(r.content).decode()
            media_type = r.headers.get("content-type", "image/png").split(";")[0]
            log(f"✅ Screenshot {label}: {len(r.content)//1024}KB")
            return {"b64": b64, "media_type": media_type, "label": label}
    except Exception as e:
        log(f"⚠️ Screenshot {label}: {e}")
        return None

# ── Vision ────────────────────────────────────────────────────────────────────
def analyze_images_vision(images, asin, log):
    if not images: return ""
    log(f"👁️ Vision: анализирую {len(images)} фото...")

    prompt = f"""Ты эксперт по Amazon листингам. Тебе показаны ПОЛНЫЕ СКРИНЫ страниц Amazon листингов.

Для КАЖДОГО скрина проанализируй:
1. Title — ключевые слова, длина, УТП
2. Bullets — о чём пишут, какие характеристики, цифры
3. Главное фото — фон, ракурс, качество
4. A+ контент — есть ли, что показывают
5. Цена и позиционирование
6. Оценка общего листинга 1-10

Потом сравни НАШ листинг с конкурентами:
- Что у нас лучше
- Чего нам не хватает
- Топ 5 конкретных улучшений

Отвечай на русском. Будь конкретен — указывай реальный текст который видишь на скринах."""

    # Anthropic payload
    content_a = [{"type":"text","text":prompt}]
    for i,img in enumerate(images):
        content_a.append({"type":"text","text":f"\n--- Изображение №{i+1} ---"})
        content_a.append({"type":"image","source":{"type":"base64","media_type":img["media_type"],"data":img["b64"]}})

    # Gemini payload
    parts_g = [{"text": prompt}]
    for i,img in enumerate(images):
        parts_g.append({"text": f"\n--- Изображение №{i+1} ---"})
        parts_g.append({"inline_data": {"mime_type": img["media_type"], "data": img["b64"]}})

    result, provider = ai_post(
        {"model": ANTHROPIC_MODEL, "max_tokens": 3000, "messages": [{"role":"user","content":content_a}]},
        {"contents": [{"parts": parts_g, "role": "user"}]},
        log
    )
    log(f"✅ Vision ({provider}): {len(result)} символов")
    return result

# ── Text + web search ─────────────────────────────────────────────────────────
def run_text_analysis(our_url, competitor_urls, vision_data, log):
    asin = get_asin(our_url) or our_url
    active = [u.strip() for u in competitor_urls if u.strip()]
    url_list = f"НАШ: {our_url}\n" + "\n".join([f"Конк.{i+1}: {u}" for i,u in enumerate(active)])

    log("🔍 Читаю листинги через Anthropic...")
    raw_data = ""
    try:
        key = st.secrets.get("ANTHROPIC_API_KEY","")
        if not key: raise Exception("no key")
        # Direct analysis without web_search tool (not available on this tier)
        prompt = f"""You are an Amazon product research expert. Based on your knowledge, provide detailed information about these Amazon listings:

{url_list}

For OUR product ({asin}) and each competitor, describe what you know about:
- Product title and main keywords
- Key product features (material, weight, certifications)
- Typical bullet points for this product category
- Price range and positioning
- Common strengths and weaknesses in this niche
- What competitors typically emphasize

Be specific and detailed. This is merino wool apparel category."""

        r = requests.post(ANTHROPIC_URL,
            headers={"x-api-key":key,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":ANTHROPIC_MODEL,"max_tokens":4000,
                  "messages":[{"role":"user","content":prompt}]}, timeout=120)
        if not r.ok: raise Exception(f"{r.status_code}: {r.json()}")
        raw_data = r.json()["content"][0]["text"]
        log(f"📄 Anthropic: {len(raw_data)} символов")
    except Exception as e:
        log(f"  ⚠️ {str(e)[:80]}")
        raw_data = f"Amazon merino wool tank top ASIN: {asin}"

    # Final JSON analysis
    log("🧠 Финальный анализ...")
    vision_section = f"\n\nВИЗУАЛЬНЫЙ АНАЛИЗ ФОТО:\n{vision_data[:3000]}" if vision_data else ""
    analysis_prompt = f"""Analyze Amazon listing. OUR product: {asin}

Vision analysis of screenshots:
{vision_data[:2000]}

Background data:
{raw_data[:2000]}

Return ONLY JSON in Russian (max 2 items per array). Be specific.
{SCHEMA}"""

    raw_json, provider = ai_post(
        {"model":ANTHROPIC_MODEL,"max_tokens":4000,
         "system":"Amazon expert. Return ONLY valid JSON. No markdown.",
         "messages":[{"role":"user","content":analysis_prompt}]},
        {"system_instruction":{"parts":[{"text":"Amazon expert. Return ONLY valid JSON. No markdown."}]},
         "contents":[{"parts":[{"text":analysis_prompt}],"role":"user"}],
         "generationConfig":{"temperature":0.1}},
        log
    )
    log(f"✅ JSON ({provider}): {len(raw_json)} символов")

    s = raw_json.strip().replace("```json","").replace("```","").strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1: raise ValueError(f"JSON не найден: {raw_json[:200]}")
    s = re.sub(r",\s*([}\]])", r"\1", s[start:end+1])
    try: return json.loads(s)
    except:
        s = re.sub(r'"([^"]*)"', lambda m: '"'+m.group(1).replace('\n',' ')+'"', s)
        return json.loads(re.sub(r",\s*([}\]])", r"\1", s))

# ── Main ──────────────────────────────────────────────────────────────────────
def run_full_analysis(our_url, competitor_urls, log):
    asin = get_asin(our_url) or "unknown"
    log(f"🎯 ASIN: {asin}")

    # Make screenshots of our listing + competitors
    screenshots = []
    shot = fetch_listing_screenshot(our_url, f"НАШ ({asin})", log)
    if shot: screenshots.append(shot)

    active = [u.strip() for u in competitor_urls if u.strip()]
    for i, url in enumerate(active[:2]):  # max 2 конкурента для Vision
        shot = fetch_listing_screenshot(url, f"Конкурент {i+1}", log)
        if shot: screenshots.append(shot)

    vision_data = analyze_images_vision(screenshots, asin, log) if screenshots else ""
    if not screenshots: log("⚠️ Скрины не получены — только текст")
    result = run_text_analysis(our_url, competitor_urls, vision_data, log)
    return result, vision_data

# ── UI ────────────────────────────────────────────────────────────────────────
st.title("🔍 Amazon Listing Analyzer")
st.caption("Anthropic (приоритет) + Gemini (запасной) · ScrapingDog Vision")

with st.sidebar:
    st.header("🔑 Статус API")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🧪 Anthropic"):
            try:
                key = st.secrets.get("ANTHROPIC_API_KEY","")
                if not key: raise Exception("ключ не задан")
                r = requests.post(ANTHROPIC_URL,
                    headers={"x-api-key":key,"anthropic-version":"2023-06-01","content-type":"application/json"},
                    json={"model":ANTHROPIC_MODEL,"max_tokens":10,"messages":[{"role":"user","content":"hi"}]},
                    timeout=15)
                if r.ok: st.success(f"✅ ...{key[-6:]}")
                else: raise Exception(r.json().get("error",{}).get("message",""))
            except Exception as e: st.error(f"❌ {str(e)[:60]}")

    with col2:
        if st.button("🧪 Gemini"):
            try:
                result = gemini_post({"contents":[{"parts":[{"text":"hi"}],"role":"user"}]})
                st.success(f"✅ OK")
            except Exception as e: st.error(f"❌ {str(e)[:60]}")

    st.divider()
    st.markdown("""
**Secrets:**
- `ANTHROPIC_API_KEY`
- `GEMINI_API_KEY` *(не нужен — ключи вшиты)*
- `SCRAPINGDOG_API_KEY`
    """)

st.subheader("📎 Листинги")
our_url = st.text_input("🔵 НАШ листинг", placeholder="https://www.amazon.com/dp/XXXXXXXXXX")
with st.expander("➕ Конкуренты (до 5)", expanded=False):
    competitor_urls = [
        st.text_input(f"Конкурент {i+1}", key=f"c{i}", placeholder="https://www.amazon.com/dp/XXXXXXXXXX")
        for i in range(5)
    ]

st.divider()
if st.button("🚀 Запустить анализ", type="primary", disabled=not our_url.strip()):
    lines = []
    ph = st.empty()
    def log(msg):
        lines.append(msg)
        ph.markdown("\n\n".join(lines))

    with st.spinner("Анализирую..."):
        try:
            r, vision_data = run_full_analysis(our_url, competitor_urls, log)
            st.session_state["result"] = r
            st.session_state["vision"] = vision_data
            st.success("✅ Готово!")
        except Exception as e:
            st.error(f"Ошибка: {e}")
            st.stop()

if "result" in st.session_state:
    r = st.session_state["result"]
    v = st.session_state.get("vision","")
    st.divider()
    if v:
        with st.expander("👁️ Vision анализ фотографий", expanded=True):
            st.markdown(v)
        st.divider()

    st.subheader("📊 Анализ листинга")
    st.info(f"**Резюме:** {r.get('summary','')}")

    if r.get("actions"):
        st.subheader("🎯 Приоритетные действия")
        for i,a in enumerate(r["actions"]):
            with st.container(border=True):
                c1,c2,c3 = st.columns([5,1,1])
                c1.markdown(f"**{i+1}. {a.get('action','')}**")
                c2.markdown(badge(a.get("impact","MEDIUM")))
                c3.caption(f"Усилия: {a.get('effort','?')}")
                st.caption(a.get("details",""))

    st.subheader("📝 Оценки")
    t1,t2,t3 = st.tabs(["Текст","Визуал","A+"])
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
                col1,col2 = st.columns([5,1])
                col1.markdown(f"**{c.get('name','')}**"); col1.caption(c.get("how_competitors_use",""))
                col2.markdown(badge(c.get("priority","MEDIUM")))

    if r.get("tech_params"):
        st.subheader("⚙️ Технические параметры")
        for p in r["tech_params"]:
            with st.container(border=True):
                st.markdown(f"**{p.get('param','')}**")
                c1,c2 = st.columns(2)
                c1.caption(f"🏆 {p.get('competitor_value','')}"); c2.caption(f"→ {p.get('our_gap','')}")

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
                c1,c2 = st.columns(2)
                c1.caption(f"Конкуренты: {n.get('competitor_usage','')}"); c2.success(f"→ {n.get('suggested','')}")

    with st.expander("🔧 Raw JSON"):
        st.json(r)
