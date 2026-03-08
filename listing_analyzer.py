"""
listing_analyzer.py — Amazon Listing Analyzer
ScrapingDog Product API → фото → Claude Vision + текстовый анализ
Secrets: ANTHROPIC_API_KEY, SCRAPINGDOG_API_KEY
"""
import json, re, base64, requests, streamlit as st
from PIL import Image
import io

st.set_page_config(page_title="Listing Analyzer", page_icon="🔍", layout="wide")

ANTHROPIC_URL   = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-3-haiku-20240307"

SCHEMA = '{"summary":"...","title_score":7,"title_gaps":["gap1","gap2"],"title_rec":"rec","bullets_score":7,"bullets_gaps":["gap1"],"bullets_rec":"rec","desc_score":7,"desc_gaps":["gap1"],"desc_rec":"rec","photos_score":7,"photos_gaps":["gap1"],"photos_rec":"rec","aplus_score":7,"aplus_gaps":["gap1"],"aplus_rec":"rec","missing_chars":[{"name":"char","how_competitors_use":"use","priority":"HIGH"}],"tech_params":[{"param":"p","competitor_value":"v","our_gap":"g"}],"actions":[{"action":"act","impact":"HIGH","effort":"LOW","details":"det"}]}'

def get_asin(url):
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None

def sc(s): return "🟢" if s>=8 else ("🟡" if s>=6 else "🔴")
def badge(p): return {"HIGH":"🔴 HIGH","MEDIUM":"🟡 MEDIUM","LOW":"🟢 LOW"}.get(p,p)

def section(label, score, gaps, rec):
    c1,c2 = st.columns([4,1])
    c1.markdown(f"**{label}**"); c2.markdown(f"{sc(score)} **{score}/10**")
    st.progress(score/10)
    if gaps:
        with st.expander(f"⚠️ Пробелы ({len(gaps)})"):
            for g in gaps: st.markdown(f"- {g}")
    if rec: st.info(f"💡 {rec}")

def anthropic_call(system, user, max_tokens=3000):
    key = st.secrets.get("ANTHROPIC_API_KEY","")
    if not key: raise Exception("ANTHROPIC_API_KEY не задан")
    payload = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens, "messages": [{"role":"user","content":user}]}
    if system: payload["system"] = system
    r = requests.post(ANTHROPIC_URL,
        headers={"x-api-key":key,"anthropic-version":"2023-06-01","content-type":"application/json"},
        json=payload, timeout=120)
    if not r.ok: raise Exception(f"Anthropic {r.status_code}: {r.json().get('error',{}).get('message','')}")
    return r.json()["content"][0]["text"]

def anthropic_vision(content_blocks, max_tokens=3000):
    key = st.secrets.get("ANTHROPIC_API_KEY","")
    if not key: raise Exception("ANTHROPIC_API_KEY не задан")
    r = requests.post(ANTHROPIC_URL,
        headers={"x-api-key":key,"anthropic-version":"2023-06-01","content-type":"application/json"},
        json={"model": ANTHROPIC_MODEL, "max_tokens": max_tokens,
              "messages": [{"role":"user","content":content_blocks}]},
        timeout=120)
    if not r.ok: raise Exception(f"Anthropic {r.status_code}: {r.json().get('error',{}).get('message','')}")
    return r.json()["content"][0]["text"]

# ── ScrapingDog ───────────────────────────────────────────────────────────────
def scrapingdog_product(asin, log):
    sd_key = st.secrets.get("SCRAPINGDOG_API_KEY","")
    if not sd_key: log("⚠️ SCRAPINGDOG_API_KEY не задан"); return {}, []
    log(f"🌐 ScrapingDog: {asin}...")
    try:
        r = requests.get("https://api.scrapingdog.com/amazon/product",
            params={"api_key": sd_key, "asin": asin, "domain": "com"}, timeout=60)
        if not r.ok: log(f"⚠️ {r.status_code}: {r.text[:100]}"); return {}, []
        data = r.json()
        # Fetch A+ content separately if available
        if data.get("aplus"):
            try:
                ra = requests.get("https://api.scrapingdog.com/amazon/product",
                    params={"api_key": sd_key, "asin": asin, "domain": "com", "type": "aplus"}, timeout=60)
                if ra.ok:
                    adata = ra.json()
                    data["aplus_content"] = str(adata)[:2000]
                    log(f"  ✅ A+ контент получен ({len(str(adata))} chars)")
            except: pass
        # Extract image URLs
        urls = []
        for field in ["images_of_specified_asin","images"]:
            raw = data.get(field,[])
            if isinstance(raw, list): urls.extend([u for u in raw if isinstance(u,str) and u.startswith("http")])
            elif isinstance(raw, str):
                try: urls.extend([u for u in json.loads(raw) if isinstance(u,str)])
                except: pass
        urls = list(dict.fromkeys([u for u in urls if not re.search(r'_SR\d{2}|_SX3[0-9]|_SS4|_SL75|sprite|grey',u)]))
        log(f"✅ ScrapingDog: {len(urls)} фото")
        return data, urls[:5]
    except Exception as e:
        log(f"⚠️ ScrapingDog: {e}"); return {}, []

def compress_image(content, max_kb=800):
    try:
        img = Image.open(io.BytesIO(content)).convert("RGB")
        if img.width > 1200:
            ratio = 1200/img.width
            img = img.resize((1200, int(img.height*ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=75)
        return buf.getvalue(), "image/jpeg"
    except:
        return content, "image/jpeg"

def download_images(urls, log):
    images = []
    for i,url in enumerate(urls):
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent":"Mozilla/5.0"})
            if r.ok and len(r.content)>1000:
                data, mt = compress_image(r.content)
                images.append({"b64": base64.b64encode(data).decode(), "media_type": mt})
                log(f"  📥 Фото {i+1}: {len(data)//1024}KB ✅")
        except Exception as e:
            log(f"  ⚠️ Фото {i+1}: {e}")
    return images

# ── Vision ────────────────────────────────────────────────────────────────────
def analyze_vision(images, product_data, asin, log):
    if not images: return ""
    log(f"👁️ Vision: {len(images)} фото → Anthropic...")

    title = product_data.get("title","")
    price = product_data.get("price","")
    rating = product_data.get("average_rating","")
    reviews = product_data.get("reviews_count","")
    bsr = product_data.get("bestseller_rank","")

    blocks = [{
        "type":"text",
        "text": f"""Ты эксперт по Amazon листингам. Проанализируй фотографии продукта.

Информация о продукте:
- ASIN: {asin}
- Title: {title}
- Цена: {price}
- Рейтинг: {rating} ({reviews} отзывов)
- BSR: {bsr}

Для КАЖДОГО фото:
1. Тип (главное/lifestyle/инфографика/A+ баннер/детали/размерная сетка)
2. Что видно — фон, модель, текст и цифры на фото
3. Оценка конверсии 1-10
4. Сильная сторона и слабость

В конце — топ 3 конкретных улучшения фото для роста конверсии.
Отвечай на русском, будь конкретен."""
    }]

    for i,img in enumerate(images):
        blocks.append({"type":"text","text":f"\n--- Фото №{i+1} ---"})
        blocks.append({"type":"image","source":{"type":"base64","media_type":img["media_type"],"data":img["b64"]}})

    result = anthropic_vision(blocks, max_tokens=2000)
    log(f"✅ Vision: {len(result)} символов")
    return result

# ── Text analysis ─────────────────────────────────────────────────────────────
def analyze_text(our_data, competitor_data_list, vision_result, asin, log):
    log("🧠 Финальный анализ...")

    def fmt(data):
        if not data: return "нет данных"
        pi = data.get("product_information", {})
        bullets = data.get("feature_bullets", [])
        reviews = data.get("customer_reviews", [])
        review_texts = " | ".join([r.get("review_snippet","")[:100] for r in reviews[:5]])
        return "\n".join([
            f"Title: {data.get('title','')}",
            f"Price: {data.get('price','')} | Старая цена: {data.get('previous_price','')}",
            f"Rating: {data.get('average_rating','')} | Reviews: {pi.get('Customer Reviews',{}).get('ratings_count','')}",
            f"BSR: {pi.get('Best Sellers Rank','')}",
            f"Material: {pi.get('Material Type','')} | Fabric: {pi.get('Fabric Type','')}",
            f"A+: {data.get('aplus',False)} | Videos: {data.get('number_of_videos',0)}",
            f"Bullets: {chr(10).join(bullets[:5])}",
            f"Reviews snippets: {review_texts}",
            f"Description: {str(data.get('description',''))[:300]}",
            f"A+ Content: {str(data.get('aplus_content','нет данных'))[:500]}",
        ])

    our_text = fmt(our_data)
    comp_text = "\n\n".join([f"КОНКУРЕНТ {i+1}:\n{fmt(d)}" for i,d in enumerate(competitor_data_list) if d])
    vision_section = f"\nVISION АНАЛИЗ ФОТО:\n{vision_result[:1500]}" if vision_result else ""

    prompt = f"""Проанализируй Amazon листинг и дай рекомендации.

НАШ ЛИСТИНГ (ASIN {asin}):
{our_text}

{comp_text}
{vision_section}

Верни ТОЛЬКО JSON на русском языке. Заполни ВСЕ поля реальными данными из листинга, не "x".
{SCHEMA}"""

    raw = anthropic_call("Amazon listing expert. Return ONLY valid JSON. No markdown. No preamble.", prompt, max_tokens=3000)
    log(f"✅ JSON: {len(raw)} символов")

    s = raw.strip().replace("```json","").replace("```","").strip()
    start,end = s.find("{"),s.rfind("}")
    if start==-1: raise ValueError(f"JSON не найден: {raw[:200]}")
    s = re.sub(r",\s*([}\]])", r"\1", s[start:end+1])
    try: return json.loads(s)
    except:
        s2 = re.sub(r'"([^"]*)"', lambda m: '"'+m.group(1).replace('\n',' ')+'"', s)
        return json.loads(re.sub(r",\s*([}\]])", r"\1", s2))

# ── Main ──────────────────────────────────────────────────────────────────────
def run_analysis(our_url, competitor_urls, log):
    asin = get_asin(our_url) or "unknown"
    log(f"🎯 ASIN: {asin}")

    # Our product
    our_data, img_urls = scrapingdog_product(asin, log)

    # Download & Vision
    images = []
    if img_urls:
        log(f"⬇️ Скачиваю {len(img_urls)} фото...")
        images = download_images(img_urls, log)
    vision_result = analyze_vision(images, our_data, asin, log) if images else ""
    if not images: log("⚠️ Фото не загружены")

    # Competitors
    active = [u.strip() for u in competitor_urls if u.strip()]
    comp_data_list = []
    for i,url in enumerate(active[:3]):
        casin = get_asin(url)
        if casin:
            log(f"🔍 Конкурент {i+1}: {casin}...")
            cdata, _ = scrapingdog_product(casin, log)
            comp_data_list.append(cdata)

    result = analyze_text(our_data, comp_data_list, vision_result, asin, log)
    return result, vision_result

# ── UI ────────────────────────────────────────────────────────────────────────
st.title("🔍 Amazon Listing Analyzer")
st.caption("ScrapingDog → Claude Vision + анализ текста → рекомендации")

with st.sidebar:
    st.header("🔑 Тест API")
    if st.button("🧪 Anthropic"):
        try:
            r = anthropic_call(None, "Say: OK", max_tokens=5)
            st.success(f"✅ {r}")
        except Exception as e: st.error(f"❌ {e}")
    st.markdown("**Secrets:** `ANTHROPIC_API_KEY` · `SCRAPINGDOG_API_KEY`")

st.subheader("📎 Листинги")
our_url = st.text_input("🔵 НАШ листинг", value="https://www.amazon.com/dp/B0D6WBQ7G1")
with st.expander("➕ Конкуренты (до 3)"):
    competitor_urls = [
        st.text_input(f"Конкурент {i+1}", key=f"c{i}", placeholder="https://www.amazon.com/dp/...")
        for i in range(3)
    ]

st.divider()
if st.button("🚀 Запустить анализ", type="primary", disabled=not our_url.strip()):
    lines = []
    ph = st.empty()
    def log(msg):
        lines.append(msg); ph.markdown("\n\n".join(lines))

    with st.spinner("Анализирую..."):
        try:
            r, vision = run_analysis(our_url, competitor_urls, log)
            st.session_state.update({"result":r,"vision":vision})
            st.success("✅ Готово!")
        except Exception as e:
            st.error(f"Ошибка: {e}"); st.stop()

if "result" in st.session_state:
    r = st.session_state["result"]
    v = st.session_state.get("vision","")
    st.divider()

    if v:
        with st.expander("👁️ Vision анализ фотографий", expanded=True):
            st.markdown(v)
        st.divider()

    st.subheader("📊 Анализ")
    st.info(f"**Резюме:** {r.get('summary','')}")

    if r.get("actions"):
        st.subheader("🎯 Приоритетные действия")
        for i,a in enumerate(r["actions"]):
            with st.container(border=True):
                c1,c2,c3 = st.columns([5,1,1])
                c1.markdown(f"**{i+1}. {a.get('action','')}**")
                c2.markdown(badge(a.get("impact","MEDIUM")))
                c3.caption(f"Усилия: {a.get('effort','')}")
                if a.get("details"): st.caption(a["details"])

    st.subheader("📝 Оценки")
    t1,t2 = st.tabs(["Текст","Визуал"])
    with t1:
        section("Title",       r.get("title_score",0),   r.get("title_gaps",[]),   r.get("title_rec",""))
        st.divider()
        section("Bullets",     r.get("bullets_score",0), r.get("bullets_gaps",[]), r.get("bullets_rec",""))
        st.divider()
        section("Description", r.get("desc_score",0),    r.get("desc_gaps",[]),    r.get("desc_rec",""))
    with t2:
        section("Фото", r.get("photos_score",0), r.get("photos_gaps",[]), r.get("photos_rec",""))
        st.divider()
        section("A+",   r.get("aplus_score",0),  r.get("aplus_gaps",[]),  r.get("aplus_rec",""))

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
                c1.caption(f"🏆 {p.get('competitor_value','')}"); c2.caption(f"→ {p.get('our_gap','')}")

    with st.expander("🔧 Raw JSON"):
        st.json(r)
