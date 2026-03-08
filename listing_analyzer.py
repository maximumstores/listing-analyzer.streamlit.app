"""
listing_analyzer.py — Amazon Listing Analyzer с Claude Vision
pip install streamlit anthropic requests
Secrets:
  ANTHROPIC_API_KEY = "sk-ant-..."
  SCRAPINGDOG_API_KEY = "..."  # опционально — для авто-получения фото
"""
import json, re, base64, requests, streamlit as st
import anthropic

st.set_page_config(page_title="Listing Analyzer", page_icon="🔍", layout="wide")

SCHEMA = '{"summary":"...","title_score":7,"title_gaps":["x"],"title_advantages":["x"],"title_rec":"x","bullets_score":7,"bullets_gaps":["x"],"bullets_advantages":["x"],"bullets_rec":"x","desc_score":7,"desc_gaps":["x"],"desc_advantages":["x"],"desc_rec":"x","photos_score":7,"photos_gaps":["x"],"photos_advantages":["x"],"photos_rec":"x","video_score":7,"video_gaps":["x"],"video_advantages":["x"],"video_rec":"x","aplus_score":7,"aplus_gaps":["x"],"aplus_advantages":["x"],"aplus_rec":"x","missing_chars":[{"name":"x","how_competitors_use":"x","priority":"HIGH"}],"tech_params":[{"param":"x","competitor_value":"x","our_gap":"x"}],"scenarios":[{"scenario":"x","competitors":[],"how_to_add":"x"}],"numbers":[{"metric":"x","competitor_usage":"x","suggested":"x"}],"actions":[{"action":"x","impact":"HIGH","effort":"LOW","details":"x"}]}'

# ── Helpers ───────────────────────────────────────────────────────────────────
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

# ── Step 1: Fetch images via ScrapingDog ──────────────────────────────────────
def fetch_images_scrapingdog(asin, log):
    sd_key = st.secrets.get("SCRAPINGDOG_API_KEY", "")
    if not sd_key:
        log("⚠️ SCRAPINGDOG_API_KEY не задан — пропускаю авто-загрузку фото")
        return []

    log(f"🌐 ScrapingDog: загружаю страницу {asin}...")
    try:
        r = requests.get(
            "https://api.scrapingdog.com/amazon/product",
            params={"api_key": sd_key, "asin": asin, "domain": "com"},
            timeout=60
        )
        if not r.ok:
            log(f"⚠️ ScrapingDog error {r.status_code}")
            return []
        data = r.json()

        # Extract image URLs
        urls = []
        for field in ["images_of_specified_asin", "images"]:
            raw = data.get(field, [])
            if isinstance(raw, list):
                for u in raw:
                    if isinstance(u, str) and u.startswith("http"):
                        urls.append(u)
            elif isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        urls.extend([u for u in parsed if isinstance(u, str)])
                except:
                    pass

        # Filter thumbnails
        urls = [u for u in urls if not re.search(r'_SR\d{2}|_SX3[0-9]|_SS4|_SL75|sprite|grey', u)]
        urls = list(dict.fromkeys(urls))  # deduplicate
        log(f"✅ ScrapingDog: {len(urls)} изображений")
        return urls[:8]
    except Exception as e:
        log(f"⚠️ ScrapingDog exception: {e}")
        return []

# ── Step 2: Download images ───────────────────────────────────────────────────
def download_images(urls, log):
    images = []
    for i, url in enumerate(urls):
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if r.ok and len(r.content) > 1000:
                b64 = base64.b64encode(r.content).decode()
                media_type = r.headers.get("content-type", "image/jpeg").split(";")[0]
                images.append({"b64": b64, "media_type": media_type, "url": url})
                log(f"  📥 Фото {i+1}: {len(r.content)//1024}KB ✅")
            else:
                log(f"  ⚠️ Фото {i+1}: пропущено ({r.status_code})")
        except Exception as e:
            log(f"  ⚠️ Фото {i+1}: {e}")
    return images

# ── Step 3: Vision analysis ───────────────────────────────────────────────────
def analyze_images_vision(images, asin, log):
    if not images:
        return ""

    client = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])
    log(f"👁️ Vision: анализирую {len(images)} фото...")

    content = [{
        "type": "text",
        "text": f"""Ты эксперт по Amazon листингам. Проанализируй ВСЕ фотографии продукта ASIN {asin}.

Для КАЖДОГО изображения укажи:
1. Тип (главное фото / lifestyle / инфографика / A+ баннер / детали материала / размерная сетка)
2. Что видно — фон, модель, композиция, текст и цифры на фото
3. Оценка конверсии 1-10
4. Сильная сторона
5. Слабость или чего не хватает

В конце — топ 3 приоритетных улучшения для увеличения конверсии.
Отвечай на русском. Будь конкретен и actionable."""
    }]

    for i, img in enumerate(images):
        content.append({"type": "text", "text": f"\n--- Изображение №{i+1} ---"})
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": img["media_type"], "data": img["b64"]}
        })

    resp = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=4000,
        messages=[{"role": "user", "content": content}]
    )
    result = resp.content[0].text
    log(f"✅ Vision анализ: {len(result)} символов")
    return result

# ── Step 4: Text analysis via web_search ─────────────────────────────────────
def run_text_analysis(our_url, competitor_urls, vision_data, log):
    client = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])
    active = [u.strip() for u in competitor_urls if u.strip()]
    asin = get_asin(our_url) or our_url

    log("🔍 Шаг: web_search — читаю текст листингов...")
    url_list = f"НАШ: {our_url}\n" + "\n".join([f"Конк.{i+1}: {u}" for i,u in enumerate(active)])

    messages = [{
        "role": "user",
        "content": f"""Search Amazon for these product listings and extract their content.
For EACH product find: full title, all bullet points, description, A+ content, BSR rank, reviews, price.

{url_list}

Search each by ASIN and summarize what you find."""
    }]

    search_count = 0
    for _ in range(12):
        resp = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=6000,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=messages,
        )
        messages.append({"role": "assistant", "content": resp.content})

        if resp.stop_reason == "end_turn":
            raw_data = "".join(b.text for b in resp.content if hasattr(b, "text"))
            log(f"📄 Текст: {len(raw_data)} символов, {search_count} поисков")
            break

        if resp.stop_reason == "tool_use":
            tool_results = []
            for block in resp.content:
                if block.type == "tool_use":
                    search_count += 1
                    q = getattr(block.input, "query", "") or str(block.input)
                    log(f"  🔎 #{search_count}: {str(q)[:60]}")
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": "Search completed"
                    })
            messages.append({"role": "user", "content": tool_results})
    else:
        raw_data = ""

    # ── Final analysis → JSON ─────────────────────────────────────────────────
    log("🧠 Финальный анализ...")

    vision_section = ""
    if vision_data:
        vision_section = f"\n\nВИЗУАЛЬНЫЙ АНАЛИЗ ФОТОГРАФИЙ (Claude Vision):\n{vision_data[:3000]}"

    resp2 = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=8000,
        system="Amazon listing optimization expert. Respond ONLY with valid JSON. No markdown. No explanation.",
        messages=[{
            "role": "user",
            "content": f"""Analyze this Amazon listing. OUR product: {asin}

Text data from web:
{raw_data[:6000]}
{vision_section}

Return ONLY JSON in Russian (max 3 items per array).
For photos_score and photos_gaps use the Vision analysis above — be specific about real photos.
{SCHEMA}"""
        }]
    )

    raw_json = "".join(b.text for b in resp2.content if hasattr(b, "text"))
    log(f"✅ JSON: {len(raw_json)} символов")

    s = raw_json.strip().replace("```json","").replace("```","").strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1: raise ValueError(f"JSON не найден: {raw_json[:200]}")
    s = re.sub(r",\s*([}\]])", r"\1", s[start:end+1])
    try:
        return json.loads(s)
    except:
        s = re.sub(r'"([^"]*)"', lambda m: '"' + m.group(1).replace('\n',' ') + '"', s)
        return json.loads(re.sub(r",\s*([}\]])", r"\1", s))


# ── Main flow ─────────────────────────────────────────────────────────────────
def run_full_analysis(our_url, competitor_urls, uploaded_files, log):
    asin = get_asin(our_url) or "unknown"
    log(f"🎯 ASIN: {asin}")

    # 1. Get images
    images = []

    # Priority: uploaded files
    if uploaded_files:
        log(f"📸 Загружено вручную: {len(uploaded_files)} фото")
        for f in uploaded_files:
            b64 = base64.b64encode(f.read()).decode()
            images.append({"b64": b64, "media_type": f.type or "image/jpeg", "url": f.name})
        log(f"✅ Готово: {len(images)} фото")

    # Fallback: ScrapingDog
    elif asin != "unknown":
        img_urls = fetch_images_scrapingdog(asin, log)
        if img_urls:
            log(f"⬇️ Скачиваю {len(img_urls)} фото...")
            images = download_images(img_urls, log)

    # 2. Vision analysis
    vision_data = ""
    if images:
        vision_data = analyze_images_vision(images, asin, log)
    else:
        log("⚠️ Фото не загружены — анализ только по тексту")

    # 3. Text + final analysis
    result = run_text_analysis(our_url, competitor_urls, vision_data, log)
    return result, vision_data


# ── UI ────────────────────────────────────────────────────────────────────────
st.title("🔍 Amazon Listing Analyzer")
st.caption("Вставь ссылки → Claude читает листинг + анализирует фото через Vision")

with st.sidebar:
    st.header("ℹ️ Как пользоваться")
    
# Quick API test button
if st.sidebar.button("🧪 Тест API ключа"):
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])
        r = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=10,
            messages=[{"role":"user","content":"hi"}]
        )
        st.sidebar.success(f"✅ API работает! Key: ...{st.secrets['ANTHROPIC_API_KEY'][-8:]}")
    except Exception as e:
        st.sidebar.error(f"❌ {e}")
        st.sidebar.code(f"Key used: ...{st.secrets.get('ANTHROPIC_API_KEY','NOT SET')[-20:]}")

    st.markdown("""
**Авто (со ScrapingDog):**
1. Вставь ссылку нашего листинга
2. Нажми Запустить

**С загрузкой фото:**
1. Сохрани фото с Amazon (ПКМ → Сохранить)
2. Загрузи в поле ниже
3. Нажми Запустить

**Secrets нужны:**
- `ANTHROPIC_API_KEY`
- `SCRAPINGDOG_API_KEY` (опционально)
    """)

st.subheader("📎 Листинги")
our_url = st.text_input("🔵 НАШ листинг", placeholder="https://www.amazon.com/dp/XXXXXXXXXX")

with st.expander("➕ Конкуренты (до 5)", expanded=False):
    competitor_urls = [
        st.text_input(f"Конкурент {i+1}", key=f"c{i}", placeholder="https://www.amazon.com/dp/XXXXXXXXXX (опционально)")
        for i in range(5)
    ]

uploaded_files = []

st.divider()
if st.button("🚀 Запустить анализ", type="primary", disabled=not our_url.strip()):
    lines = []
    ph = st.empty()
    def log(msg):
        lines.append(msg)
        ph.markdown("\n\n".join(lines))

    with st.spinner("Анализирую..."):
        try:
            r, vision_data = run_full_analysis(our_url, competitor_urls, uploaded_files, log)
            st.session_state["result"] = r
            st.session_state["vision"] = vision_data
            st.success("✅ Готово!")
        except Exception as e:
            st.error(f"Ошибка: {e}")
            st.stop()

if "result" in st.session_state:
    r = st.session_state["result"]
    v = st.session_state.get("vision", "")
    st.divider()

    # Vision block
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
                col1.markdown(f"**{c.get('name','')}**")
                col1.caption(c.get("how_competitors_use",""))
                col2.markdown(badge(c.get("priority","MEDIUM")))

    if r.get("tech_params"):
        st.subheader("⚙️ Технические параметры")
        for p in r["tech_params"]:
            with st.container(border=True):
                st.markdown(f"**{p.get('param','')}**")
                c1,c2 = st.columns(2)
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
                c1,c2 = st.columns(2)
                c1.caption(f"Конкуренты: {n.get('competitor_usage','')}")
                c2.success(f"→ {n.get('suggested','')}")

    with st.expander("🔧 Raw JSON"):
        st.json(r)
