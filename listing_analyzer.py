"""
listing_analyzer.py — Amazon Listing Analyzer
ScrapingDog Product API → фото → Claude Vision + текстовый анализ
Secrets: ANTHROPIC_API_KEY, SCRAPINGDOG_API_KEY
"""
import json, re, base64, requests, streamlit as st
from PIL import Image
import io

ANTHROPIC_URL   = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-3-haiku-20240307"

SCHEMA = '{"health_score":72,"health_breakdown":{"title":8,"bullets":8,"description":7,"photos":7,"aplus":6,"reviews":9,"bsr":8,"price":7,"variants":8,"prime":9},"summary":"...","title_score":7,"title_gaps":["gap1"],"title_rec":"rec","bullets_score":7,"bullets_gaps":["gap1"],"bullets_rec":"rec","desc_score":7,"desc_gaps":["gap1"],"desc_rec":"rec","photos_score":7,"photos_gaps":["gap1"],"photos_rec":"rec","aplus_score":7,"aplus_gaps":["gap1"],"aplus_rec":"rec","cosmo_score":65,"cosmo_semantic":[{"relationship":"Used For (Function)","status":"WELL-DEVELOPED","evidence":"...","opportunity":"..."},{"relationship":"Used For (Situation)","status":"GOOD","evidence":"...","opportunity":"..."},{"relationship":"Target Audience","status":"ADEQUATE","evidence":"...","opportunity":"..."},{"relationship":"Solves Problem","status":"GOOD","evidence":"...","opportunity":"..."},{"relationship":"Compared To (Alternative)","status":"PARTIAL","evidence":"...","opportunity":"..."},{"relationship":"Used In (Location)","status":"MINIMAL","evidence":"...","opportunity":"..."},{"relationship":"Used With (Complementary)","status":"MINIMAL","evidence":"...","opportunity":"..."}],"rufus_answered":[{"question":"q","answer":"..."}],"rufus_partial":[{"question":"q","gap":"..."}],"rufus_missing":[{"question":"q","missing":"..."}],"missing_chars":[{"name":"char","how_competitors_use":"use","priority":"HIGH"}],"tech_params":[{"param":"p","competitor_value":"v","our_gap":"g"}],"actions":[{"action":"act","impact":"HIGH","effort":"LOW","details":"det"}]}'

def get_asin(url):
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None

def sc(s): return "🟢" if s>=8 else ("🟡" if s>=6 else "🔴")
def badge(p): return {"HIGH":"🔴 HIGH","MEDIUM":"🟡 MEDIUM","LOW":"🟢 LOW"}.get(p,p)

def section(label, score, gaps, rec, raw_text="", char_limit=0):
    c1,c2 = st.columns([4,1])
    c1.markdown(f"**{label}**"); c2.markdown(f"{sc(score)} **{score}/10**")
    st.progress(score/10)
    if raw_text:
        char_count = len(raw_text)
        color = "red" if (char_limit and char_count > char_limit) else "gray"
        st.markdown(f"<small style='color:{color}'>📝 {char_count} симв{f' / {char_limit} лимит' if char_limit else ''}</small>", unsafe_allow_html=True)
        with st.expander("Показать текст"):
            st.markdown(f"> {raw_text}")
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
        blocks.append({"type":"text","text":f"\nPHOTO_BLOCK_{i+1}\nОтветь СТРОГО в формате (4 строки, не больше):\nТип: ...\nОценка: X/10\nСильная сторона: ...\nСлабость: ..."})
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
            f"A+ Content: {str(data.get('aplus_content','нет данных'))[:3000]}",
        ])

    our_text = fmt(our_data)
    comp_text = "\n\n".join([f"КОНКУРЕНТ {i+1}:\n{fmt(d)}" for i,d in enumerate(competitor_data_list) if d])
    vision_section = f"\nVISION АНАЛИЗ ФОТО:\n{vision_result[:1500]}" if vision_result else ""

    prompt = f"""Ты эксперт по Amazon листингам. Оцени листинг строго по рубрику ниже.

НАШ ЛИСТИНГ (ASIN {asin}):
{our_text}

{comp_text}
{vision_section}

═══ РУБРИК ОЦЕНКИ ═══

TITLE (0-10):
- Длина ≤125 символов: +1.5 балла
- Бренд + тип товара + материал + характеристики: +3.5 балла
- Чёткость назначения (для спорта / на каждый день): +3 балла
- Нет спецсимволов (! $ ? ¬): +1 балл
- Нет повторов слов ≥3 раз: +1 балл

BULLETS (0-10):
- ≤5 буллетов: +1.5 балла
- Формат "Свойство: Детали. Польза": +2.5 балла
- Покрытие: материал + комфорт + функциональность + уход: +4 балла
- Длина каждого ≤255 байт: +1 балл
- Нет эмодзи: +1 балл

DESCRIPTION (0-10):
- Отсутствует = 0 баллов автоматически
- Структура (заголовки, списки, абзацы): +3 балла
- Преимущества + характеристики + сценарии: +5 баллов
- Ответы на возражения из отзывов: +2 балла

PHOTOS (0-10):
- 6+ изображений + видео = 10; 6+ без видео = 8; <6 = штраф
- Чёткость и высокое разрешение: +4 балла
- Разнообразие: модель + крупный план + lifestyle + инфографика: +3 балла

APLUS (0-10):
- Отсутствует = 0 автоматически
- Структурированный текст (заголовки, логика): +2 балла
- Текст ≤80 символов в каждом модуле: +1.5 балла
- Brand Story присутствует: +1.5 балла
- Изображения с текстовыми описаниями: +1 балл

COSMO (0-100) — 11 семантических связей Amazon:
Used For Function, Used For Situation, Target Audience, Solves Problem,
Product Type, Capable Of, Compared To Alternative, Develops Skills,
Used In Location, Used On Season, Used With Complementary.
Каждая связь: WELL-DEVELOPED=9-10, GOOD=7-8, ADEQUATE=5-6, PARTIAL=3-4, MINIMAL=1-2.

RUFUS — типичные вопросы покупателей для этой категории.

Верни ТОЛЬКО JSON на русском. Все поля — реальные данные из листинга, не "x".
cosmo_score = среднее по всем связям × 10.
health_score (0-100) = взвешенное среднее:
  title×10% + bullets×10% + description×10% + photos×10% + aplus×10% + reviews×15% + bsr×15% + price×10% + variants×5% + prime×5%
  где каждый компонент нормализован к 0-10.
  reviews: ≥4.4 и ≥50 отз=10, ≥4.0=7, <4.0=4
  bsr: ≤1000=10, ≤5000=8, >5000=5, нет=5
  price: конкурентная=10, выше рынка=6
  variants: ≥5 цветов И размеры=10, только размеры=7, один вариант=4
  prime: prime exclusive=10, prime=8, нет=5
health_breakdown содержит все 10 компонентов со значениями 0-10.
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
    st.session_state["images"] = images
    # Store images for display
    import streamlit as _st
    _st.session_state["images"] = images

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
    st.session_state['our_data'] = our_data
    st.session_state['comp_data_list'] = comp_data_list
    return result, vision_result

# ── UI ────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Listing Analyzer", page_icon="🔍", layout="wide")

with st.sidebar:
    st.markdown("## 🔍 Listing Analyzer")
    st.divider()

    # Navigation
    if "result" in st.session_state:
        page = st.radio("📂 Навигация", [
            "🏠 Обзор",
            "📸 Фото",
            "📝 Контент",
            "🏆 Benchmark",
            "🧠 COSMO / Rufus",
        ], label_visibility="collapsed")
    else:
        page = "🏠 Обзор"
        st.caption("Запусти анализ чтобы открыть все страницы")

    st.divider()
    st.markdown("**🔑 API**")
    if st.button("🧪 Anthropic"):
        try:
            res = anthropic_call(None, "Say: OK", max_tokens=5)
            st.success(f"✅ {res}")
        except Exception as e:
            st.error(f"❌ {str(e)[:60]}")

# ── Input always visible at top ───────────────────────────────────────────────
with st.expander("📎 Листинги", expanded=("result" not in st.session_state)):
    our_url = st.text_input("🔵 НАШ листинг", value=st.session_state.get("our_url_saved","https://www.amazon.com/dp/B0D6WBQ7G1"))
    c1, c2, c3 = st.columns(3)
    comp1 = c1.text_input("Конкурент 1", key="c0", value=st.session_state.get("c0_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp2 = c2.text_input("Конкурент 2", key="c1", value=st.session_state.get("c1_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp3 = c3.text_input("Конкурент 3", key="c2", value=st.session_state.get("c2_saved",""), placeholder="https://www.amazon.com/dp/...")
    competitor_urls = [comp1, comp2, comp3]

    if st.button("🚀 Запустить анализ", type="primary", disabled=not our_url.strip()):
        st.session_state["our_url_saved"] = our_url
        st.session_state["c0_saved"] = comp1
        st.session_state["c1_saved"] = comp2
        st.session_state["c2_saved"] = comp3
        lines = []
        ph = st.empty()
        def log(msg):
            lines.append(msg); ph.markdown("\n\n".join(lines[-8:]))
        with st.spinner("Анализирую..."):
            try:
                result, vision = run_analysis(our_url, competitor_urls, log)
                st.session_state.update({"result": result, "vision": vision})
                st.success("✅ Готово! Открой нужный раздел в меню слева.")
                st.rerun()
            except Exception as e:
                st.error(f"Ошибка: {e}")

# ── Pages ─────────────────────────────────────────────────────────────────────
if "result" not in st.session_state:
    st.markdown("## 👈 Введи ссылки и нажми «Запустить анализ»")
    st.stop()

r  = st.session_state["result"]
v  = st.session_state.get("vision", "")
od = st.session_state.get("our_data", {})
pi = od.get("product_information", {})
cd = st.session_state.get("comp_data_list", [])
imgs = st.session_state.get("images", [])

# ── Helpers ───────────────────────────────────────────────────────────────────
def health_card():
    health = r.get("health_score", 0)
    hb     = r.get("health_breakdown", {})
    hc     = "#22c55e" if health>=75 else ("#f59e0b" if health>=50 else "#ef4444")
    hl     = "Отличный листинг" if health>=75 else ("Есть над чем работать" if health>=50 else "Требует срочных улучшений")
    title_h   = od.get("title","")
    tlen      = len(title_h)
    brand_h   = od.get("brand","")
    asin_h    = od.get("parent_asin","") or pi.get("ASIN","")
    price_h   = od.get("price","")
    rating_h  = od.get("average_rating","")
    reviews_h = pi.get("Customer Reviews",{}).get("ratings_count","")
    bsr_h     = str(pi.get("Best Sellers Rank",""))[:50]

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e293b,#334155);border-radius:16px;padding:24px;color:white;margin-bottom:16px">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px">
    <div>
      <a href="https://www.amazon.com/dp/{asin_h}" target="_blank" style="font-size:0.8rem;opacity:0.6;color:#93c5fd;text-decoration:none">{brand_h} · {asin_h} ↗</a>
      <div style="font-size:1rem;font-weight:600;max-width:520px;line-height:1.4;margin-top:4px">{title_h[:80]}{"..." if tlen>80 else ""}</div>
      <div style="display:flex;gap:14px;margin-top:8px;font-size:0.82rem;opacity:0.8;flex-wrap:wrap">
        <span>💰 {price_h}</span><span>⭐ {rating_h} ({reviews_h} отз.)</span>
        <span>📊 {bsr_h}</span>
        <span style="color:{'#fca5a5' if tlen>125 else '#86efac'}">📝 Title: {tlen} симв.</span>
      </div>
    </div>
    <div style="text-align:center">
      <div style="font-size:3.5rem;font-weight:800;color:{hc};line-height:1">{health}%</div>
      <div style="font-size:0.85rem;color:{hc};margin-top:2px">{hl}</div>
    </div>
  </div>
  <div style="background:rgba(255,255,255,0.12);border-radius:8px;height:10px;margin-top:14px">
    <div style="background:{hc};width:{health}%;height:10px;border-radius:8px"></div>
  </div>
</div>""", unsafe_allow_html=True)

    items = [("Title",hb.get("title",0)),("Bullets",hb.get("bullets",0)),
             ("Описание",hb.get("description",0)),("Фото",hb.get("photos",0)),
             ("A+",hb.get("aplus",0)),("Отзывы",hb.get("reviews",0)),
             ("BSR",hb.get("bsr",0)),("Цена",hb.get("price",0)),
             ("Варианты",hb.get("variants",0)),("Prime",hb.get("prime",0))]
    cols = st.columns(len(items))
    for col,(lbl,val) in zip(cols,items):
        pct = int(val/10*100)
        cc  = "#22c55e" if pct>=75 else ("#f59e0b" if pct>=50 else "#ef4444")
        col.markdown(f'<div style="background:#f1f5f9;border-radius:8px;padding:8px 4px;text-align:center;border-left:3px solid {cc}"><div style="font-size:1.2rem;font-weight:700;color:{cc}">{pct}%</div><div style="font-size:0.68rem;color:#64748b">{lbl}</div></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Обзор
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Обзор":
    st.title("🏠 Обзор листинга")
    health_card()
    st.divider()

    st.info(f"**📋 Резюме:** {r.get('summary','')}")

    if r.get("actions"):
        st.subheader("🎯 Приоритетные действия")
        for i, a in enumerate(r["actions"]):
            with st.container(border=True):
                c1,c2,c3 = st.columns([5,1,1])
                c1.markdown(f"**{i+1}. {a.get('action','')}**")
                c2.markdown(badge(a.get("impact","MEDIUM")))
                c3.caption(f"Усилия: {a.get('effort','')}")
                if a.get("details"): st.caption(a["details"])

    if r.get("missing_chars"):
        st.subheader("🔍 Отсутствующие характеристики")
        for ch in r["missing_chars"]:
            with st.container(border=True):
                col1,col2 = st.columns([5,1])
                col1.markdown(f"**{ch.get('name','')}**")
                col1.caption(ch.get("how_competitors_use",""))
                col2.markdown(badge(ch.get("priority","MEDIUM")))

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Фото
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📸 Фото":
    st.title("📸 Vision анализ фотографий")
    if not imgs:
        st.warning("Фото не загружены"); st.stop()

    blocks = re.split(r"PHOTO_BLOCK_\d+", v)
    blocks = [b.strip() for b in blocks if b.strip()]

    for i, img in enumerate(imgs):
        text = blocks[i] if i < len(blocks) else ""
        sm   = re.search(r"(\d+)/10", text)
        score = int(sm.group(1)) if sm else 0
        bc    = "#22c55e" if score>=8 else ("#f59e0b" if score>=6 else "#ef4444")
        slbl  = "Отлично" if score>=8 else ("Хорошо" if score>=6 else "Слабо")
        typ   = re.search(r"[Тт]ип:\s*(.+)", text)
        strg  = re.search(r"[Сс]ильная сторона:\s*(.+)", text)
        weak  = re.search(r"[Сс]лабость:\s*(.+)", text)
        ptype = typ.group(1).strip() if typ else f"Фото #{i+1}"
        stxt  = strg.group(1).strip() if strg else ""
        wtxt  = weak.group(1).strip() if weak else ""

        with st.container(border=True):
            c1,c2 = st.columns([1,2])
            with c1:
                st.image(__import__("base64").b64decode(img["b64"]), use_container_width=True)
            with c2:
                st.markdown(f"**Фото #{i+1} — {ptype}**")
                st.markdown(f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0"><div style="font-size:2rem;font-weight:800;color:{bc}">{score}/10</div><div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px"><div style="background:{bc};width:{score*10}%;height:10px;border-radius:6px"></div></div><div style="color:{bc};font-size:0.8rem;margin-top:2px">{slbl}</div></div></div>', unsafe_allow_html=True)
                if stxt: st.success(f"✅ {stxt}")
                if wtxt: st.warning(f"⚠️ {wtxt}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Контент
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📝 Контент":
    st.title("📝 Анализ контента")
    our_title   = od.get("title","")
    our_bullets = od.get("feature_bullets",[])
    our_desc    = od.get("description","")

    section("Title",       r.get("title_score",0),   r.get("title_gaps",[]),   r.get("title_rec",""),
            raw_text=our_title, char_limit=125)
    st.divider()
    bullets_text = "\n".join([f"• {b}" for b in our_bullets]) if our_bullets else ""
    section("Bullets",     r.get("bullets_score",0), r.get("bullets_gaps",[]), r.get("bullets_rec",""),
            raw_text=bullets_text)
    st.divider()
    section("Description", r.get("desc_score",0),    r.get("desc_gaps",[]),    r.get("desc_rec",""),
            raw_text=str(our_desc)[:400] if our_desc else "")
    st.divider()
    section("A+",          r.get("aplus_score",0),   r.get("aplus_gaps",[]),   r.get("aplus_rec",""))

    if r.get("tech_params"):
        st.divider()
        st.subheader("⚙️ Технические параметры")
        for p in r["tech_params"]:
            with st.container(border=True):
                st.markdown(f"**{p.get('param','')}**")
                x1,x2 = st.columns(2)
                x1.caption(f"🏆 Конкуренты: {p.get('competitor_value','')}"); x2.caption(f"→ Наш пробел: {p.get('our_gap','')}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Benchmark
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🏆 Benchmark":
    st.title("🏆 Benchmark — Сравнение с конкурентами")
    health_card()
    st.divider()

    if not cd:
        st.info("Добавь конкурентов в форму выше и запусти анализ повторно")
        st.stop()

    all_p  = [od] + cd
    labels = ["🔵 НАШ"] + [f"Конкурент {i+1}" for i in range(len(cd))]

    # Auto-score competitors from raw data
    def auto_score(d):
        pi2 = d.get("product_information", {})
        title2  = d.get("title","")
        imgs2   = d.get("images",[])
        bul2    = d.get("feature_bullets",[])
        desc2   = d.get("description","")
        rating2 = float(d.get("average_rating",0) or 0)
        rev_cnt = int(pi2.get("Customer Reviews",{}).get("ratings_count","0") or 0)
        has_vid = int(d.get("number_of_videos",0) or 0) > 0
        has_ap  = bool(d.get("aplus"))
        is_prime= bool(d.get("is_prime_exclusive"))
        bsr_raw = str(pi2.get("Best Sellers Rank",""))
        bsr_num = 99999
        bsr_m = re.search(r"#([\d,]+)", bsr_raw)
        if bsr_m:
            try: bsr_num = int(bsr_m.group(1).replace(",",""))
            except: pass
        colors  = len(d.get("customization_options",{}).get("color",[]))
        sizes   = len(d.get("customization_options",{}).get("size",[]))

        t_score = min(10, max(0,
            (1.5 if len(title2)<=125 else 0) +
            (3.5 if any(k in title2.lower() for k in ["merino","wool","shirt","base layer","tank"]) else 1.5) +
            (3.0 if any(k in title2.lower() for k in ["sport","hiking","travel","daily","gym","outdoor"]) else 1.0) +
            (1.0 if not re.search(r"[!$?{}^¬¦]", title2) else 0) +
            (1.0 if len(re.findall(r"(\w+)", title2.lower())) == len(set(re.findall(r"(\w+)", title2.lower()))) else 0)
        ))
        b_score = min(10, max(0,
            (1.5 if len(bul2) <= 5 else 0) +
            (2.5 if any(":" in b for b in bul2) else 1.0) +
            (4.0 if len(bul2) >= 4 else len(bul2)*1.0) +
            (1.0 if all(len(b.encode()) <= 255 for b in bul2) else 0) +
            (1.0 if not any(c in "".join(bul2) for c in ["🔥","✅","⭐","❌","💪"]) else 0)
        ))
        d_score = 0 if not desc2 else min(10, 4 + (3 if len(desc2)>200 else 1) + (2 if len(desc2)>500 else 0))
        p_score = min(10, max(0,
            (4.0 if len(imgs2)>=6 else len(imgs2)*0.6) +
            (2.0 if has_vid else 0) +
            (4.0 if len(imgs2)>=6 else 0)
        ))
        a_score = 0 if not has_ap else 7
        rev_score = 10 if (rating2>=4.4 and rev_cnt>=50) else (7 if rating2>=4.0 else 4)
        bsr_score = 10 if bsr_num<=1000 else (8 if bsr_num<=5000 else 5)
        pr_score  = 10 if is_prime else 5
        var_score = 10 if (colors>=5 and sizes>=3) else (7 if sizes>=3 else 4)

        health = int(
            t_score*0.10 + b_score*0.10 + d_score*0.10 + p_score*0.10 +
            a_score*0.10 + rev_score*0.15 + bsr_score*0.15 +
            7*0.10 + var_score*0.05 + pr_score*0.05
        ) * 10

        return {
            "title": round(t_score,1), "bullets": round(b_score,1),
            "description": round(d_score,1), "photos": round(p_score,1),
            "aplus": a_score, "reviews": rev_score, "bsr": bsr_score,
            "variants": var_score, "prime": pr_score, "health": health
        }

    # Score all products
    our_scores = {
        "title": r.get("title_score",0), "bullets": r.get("bullets_score",0),
        "description": r.get("desc_score",0), "photos": r.get("photos_score",0),
        "aplus": r.get("aplus_score",0),
        "reviews": r.get("health_breakdown",{}).get("reviews",0),
        "bsr": r.get("health_breakdown",{}).get("bsr",0),
        "variants": r.get("health_breakdown",{}).get("variants",0),
        "prime": r.get("health_breakdown",{}).get("prime",0),
        "health": r.get("health_score",0)
    }
    comp_scores = [auto_score(c) for c in cd]
    all_scores  = [our_scores] + comp_scores

    score_rows = [
        ("🏷️ Title",    "title",       10),
        ("📋 Bullets",  "bullets",     10),
        ("📄 Описание", "description", 10),
        ("📸 Фото",     "photos",      10),
        ("✨ A+",       "aplus",       10),
        ("⭐ Отзывы",   "reviews",     10),
        ("📊 BSR",      "bsr",         10),
        ("🎨 Варианты", "variants",    10),
        ("🚀 Prime",    "prime",       10),
        ("💯 Health",   "health",      100),
    ]

    st.subheader("📊 Оценки: мы vs конкуренты")
    hdr2 = st.columns([2] + [3]*(1+len(cd)))
    hdr2[0].markdown("**Метрика**")
    hdr2[1].markdown("**🔵 НАШ**")
    for j in range(len(cd)): hdr2[j+2].markdown(f"**Конк. {j+1}**")

    for lbl, key, mx in score_rows:
        vals = [s.get(key, 0) for s in all_scores]
        best_val = max(vals)
        row2 = st.columns([2] + [3]*(1+len(cd)))
        row2[0].caption(lbl)
        for j, val in enumerate(vals):
            pct = int(val/mx*100)
            is_best = (val == best_val)
            cc = "#22c55e" if is_best else ("#f59e0b" if pct>=50 else "#ef4444")
            star = " ★" if is_best else ""
            row2[j+1].markdown(
                f'<div style="background:#e5e7eb;border-radius:5px;height:22px;position:relative">'
                f'<div style="background:{cc};width:{pct}%;height:22px;border-radius:5px"></div>'
                f'<div style="position:absolute;top:2px;left:6px;font-size:0.75rem;font-weight:700;color:white">{val}{star}</div>'
                f'</div>', unsafe_allow_html=True
            )

    st.divider()
    st.subheader("📋 Метрики: мы vs конкуренты")
    metrics = [
        ("Название",   lambda d: (d.get("title","")[:55]+"...") if len(d.get("title",""))>55 else d.get("title","")),
        ("Цена",       lambda d: d.get("price","")),
        ("Рейтинг ⭐",  lambda d: d.get("average_rating","")),
        ("Отзывов",    lambda d: d.get("product_information",{}).get("Customer Reviews",{}).get("ratings_count","")),
        ("BSR",        lambda d: str(d.get("product_information",{}).get("Best Sellers Rank",""))[:45]),
        ("Материал",   lambda d: d.get("product_information",{}).get("Material Type","")),
        ("Фото шт.",   lambda d: str(len(d.get("images",[])))),
        ("Видео",      lambda d: str(d.get("number_of_videos","0"))),
        ("A+",         lambda d: "✅" if d.get("aplus") else "❌"),
        ("Prime",      lambda d: "✅" if d.get("is_prime_exclusive") else "—"),
    ]

    hdr = st.columns([2]+[3]*len(all_p))
    hdr[0].markdown("**Метрика**")
    for j,lbl in enumerate(labels):
        hdr[j+1].markdown(f"**{lbl}**")
    st.divider()
    for mname, gfn in metrics:
        row = st.columns([2]+[3]*len(all_p))
        row[0].caption(mname)
        for j,prod in enumerate(all_p):
            val = gfn(prod)
            if j==0: row[j+1].markdown(f"**{val}**")
            else:    row[j+1].caption(str(val))

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COSMO / Rufus
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🧠 COSMO / Rufus":
    st.title("🧠 COSMO / Rufus Анализ")

    cosmo = r.get("cosmo_score",0)
    cc    = "#22c55e" if cosmo>=80 else ("#f59e0b" if cosmo>=60 else "#ef4444")
    st.markdown(f'<div style="text-align:center;padding:20px 0"><div style="font-size:3rem;font-weight:800;color:{cc}">{cosmo}/100</div><div style="color:{cc}">COSMO Score</div><div style="background:#e5e7eb;border-radius:8px;height:12px;margin-top:10px"><div style="background:{cc};width:{cosmo}%;height:12px;border-radius:8px"></div></div></div>', unsafe_allow_html=True)
    st.divider()

    if r.get("cosmo_semantic"):
        st.subheader("📡 Семантические связи")
        status_icon = {"WELL-DEVELOPED":"✅","GOOD":"✅","ADEQUATE":"⚠️","PARTIAL":"⚠️","MINIMAL":"❌"}
        for rel in r["cosmo_semantic"]:
            icon = status_icon.get(rel.get("status",""),"❓")
            with st.container(border=True):
                st.markdown(f"{icon} **{rel.get('relationship','')}** — *{rel.get('status','')}*")
                if rel.get("evidence"):   st.caption(f"✓ {rel['evidence']}")
                if rel.get("opportunity"):st.info(f"💡 {rel['opportunity']}")

    st.divider()
    st.subheader("🤖 Rufus Q&A")
    c1,c2,c3 = st.columns(3)
    with c1:
        st.markdown("**✅ Отвечает**")
        for q in r.get("rufus_answered",[]):
            with st.container(border=True):
                st.caption(q.get("question",""))
                st.success(q.get("answer","")[:200])
    with c2:
        st.markdown("**⚠️ Частично**")
        for q in r.get("rufus_partial",[]):
            with st.container(border=True):
                st.caption(q.get("question",""))
                st.warning(q.get("gap","")[:200])
    with c3:
        st.markdown("**❌ Не отвечает**")
        for q in r.get("rufus_missing",[]):
            with st.container(border=True):
                st.caption(q.get("question",""))
                st.error(q.get("missing","")[:200])

    with st.expander("🔧 Raw JSON"):
        st.json(r)
# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Конкурент N
# ══════════════════════════════════════════════════════════════════════════════
elif page.startswith("🔴 Конкурент"):
    idx_match = re.search(r"Конкурент (\d+)", page)
    cidx = int(idx_match.group(1)) - 1 if idx_match else 0
    c = cd[cidx] if cidx < len(cd) else {}
    cpi = c.get("product_information", {})
    ctitle   = c.get("title","")
    cprice   = c.get("price","")
    crating  = c.get("average_rating","")
    creviews = cpi.get("Customer Reviews",{}).get("ratings_count","")
    cbsr     = str(cpi.get("Best Sellers Rank",""))[:50]
    cbrand   = c.get("brand","")
    casin    = c.get("parent_asin","") or cpi.get("ASIN","")
    cbullets = c.get("feature_bullets",[])
    cdesc    = c.get("description","")
    cimgs    = c.get("images",[])
    caplus   = c.get("aplus", False)
    cvideo   = int(c.get("number_of_videos",0) or 0)
    cprime   = c.get("is_prime_exclusive", False)
    ccolors  = c.get("customization_options",{}).get("color",[])
    csizes   = c.get("customization_options",{}).get("size",[])
    cmaterial= cpi.get("Material Type","")

    def _auto(d):
        dp = d.get("product_information",{})
        t2 = d.get("title",""); i2 = d.get("images",[]); b2 = d.get("feature_bullets",[])
        desc2 = d.get("description","")
        rat2 = float(d.get("average_rating",0) or 0)
        rev2 = int(dp.get("Customer Reviews",{}).get("ratings_count","0") or 0)
        vid2 = int(d.get("number_of_videos",0) or 0)>0; ap2 = bool(d.get("aplus"))
        pr2  = bool(d.get("is_prime_exclusive"))
        bsr2 = 99999
        bm = re.search(r"#(\d[\d,]*)", str(dp.get("Best Sellers Rank","")))
        if bm:
            try: bsr2 = int(bm.group(1).replace(",",""))
            except: pass
        col2 = len(d.get("customization_options",{}).get("color",[])); sz2 = len(d.get("customization_options",{}).get("size",[]))
        ts = min(10, (1.5 if len(t2)<=125 else 0) + (3.5 if any(k in t2.lower() for k in ["merino","wool","base layer","tank"]) else 1.5) + 3.0 + (1 if not re.search(r"[!$?{}]",t2) else 0) + 1)
        bs = min(10, (1.5 if len(b2)<=5 else 0) + (2.5 if any(":" in b for b in b2) else 1) + min(4, len(b2)) + 1 + 1)
        ds = 0 if not desc2 else min(10, 4+(3 if len(desc2)>200 else 1))
        ps = min(10, (4 if len(i2)>=6 else len(i2)*0.6) + (2 if vid2 else 0) + (4 if len(i2)>=6 else 0))
        as_ = 0 if not ap2 else 7
        rs = 10 if (rat2>=4.4 and rev2>=50) else (7 if rat2>=4.0 else 4)
        bsrs = 10 if bsr2<=1000 else (8 if bsr2<=5000 else 5)
        prs = 10 if pr2 else 5
        vs = 10 if (col2>=5 and sz2>=3) else (7 if sz2>=3 else 4)
        h = int((ts*0.10+bs*0.10+ds*0.10+ps*0.10+as_*0.10+rs*0.15+bsrs*0.15+7*0.10+vs*0.05+prs*0.05)*10)
        return {"title":round(ts,1),"bullets":round(bs,1),"description":round(ds,1),"photos":round(ps,1),"aplus":as_,"reviews":rs,"bsr":bsrs,"variants":vs,"prime":prs,"health":h}

    csc = _auto(c)
    ch = csc.get("health",0)
    chc = "#22c55e" if ch>=75 else ("#f59e0b" if ch>=50 else "#ef4444")
    tlen = len(ctitle)

    st.title(f"🔴 Конкурент {cidx+1}")
    st.caption(f"{cbrand} - {casin}")

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#3b1e1e,#5c2626);border-radius:16px;padding:20px;color:white;margin-bottom:16px">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
    <div>
      <div style="font-size:0.8rem;opacity:0.6">{cbrand} - {casin}</div>
      <div style="font-size:0.95rem;font-weight:600;max-width:500px;margin-top:4px">{ctitle[:80]}{"..." if tlen>80 else ""}</div>
      <div style="display:flex;gap:14px;margin-top:8px;font-size:0.82rem;opacity:0.8;flex-wrap:wrap">
        <span>Price: {cprice}</span>
        <span>Rating: {crating} ({creviews} rev.)</span>
        <span>BSR: {cbsr[:35]}</span>
        <span style="color:{'#fca5a5' if tlen>125 else '#86efac'}">Title: {tlen} chars</span>
      </div>
    </div>
    <div style="text-align:center">
      <div style="font-size:3rem;font-weight:800;color:{chc}">{ch}%</div>
      <div style="font-size:0.8rem;color:{chc}">Health Score</div>
    </div>
  </div>
  <div style="background:rgba(255,255,255,0.12);border-radius:8px;height:10px;margin-top:12px">
    <div style="background:{chc};width:{ch}%;height:10px;border-radius:8px"></div>
  </div>
</div>""", unsafe_allow_html=True)

    citems = [("Title",csc.get("title",0)),("Bullets",csc.get("bullets",0)),
              ("Описание",csc.get("description",0)),("Фото",csc.get("photos",0)),
              ("A+",csc.get("aplus",0)),("Отзывы",csc.get("reviews",0)),
              ("BSR",csc.get("bsr",0)),("Варианты",csc.get("variants",0)),("Prime",csc.get("prime",0))]
    ccols2 = st.columns(len(citems))
    for col2,(lbl2,val2) in zip(ccols2,citems):
        pct2 = int(val2/10*100)
        cc3 = "#22c55e" if pct2>=75 else ("#f59e0b" if pct2>=50 else "#ef4444")
        col2.markdown(f'<div style="background:#f8f0f0;border-radius:8px;padding:8px 4px;text-align:center;border-left:3px solid {cc3}"><div style="font-size:1.1rem;font-weight:700;color:{cc3}">{pct2}%</div><div style="font-size:0.68rem;color:#64748b">{lbl2}</div></div>', unsafe_allow_html=True)

    st.divider()
    ct1, ct2, ct3 = st.tabs(["Контент", "Фото", "Данные"])
    with ct1:
        tcc = "#ef4444" if tlen>125 else "#22c55e"
        st.markdown(f"**Title** — <span style='color:{tcc}'>{tlen} симв.</span>", unsafe_allow_html=True)
        st.markdown(f"> {ctitle}")
        st.divider()
        st.markdown(f"**Bullets** ({len(cbullets)})")
        for b2 in cbullets:
            blen2 = len(b2.encode())
            st.markdown(f"{'🔴' if blen2>255 else '✅'} {b2}")
            st.caption(f"{blen2} байт")
        st.divider()
        st.markdown("**Описание**")
        if cdesc: st.markdown(str(cdesc)[:600])
        else: st.warning("Описание отсутствует")
        st.divider()
        st.markdown(f"**A+:** {'✅ Есть' if caplus else '❌ Нет'}  |  **Видео:** {'✅ ' + str(cvideo) + ' шт.' if cvideo else '❌ Нет'}")
    with ct2:
        if cimgs:
            for row_s in range(0, min(len(cimgs),9), 3):
                rcols2 = st.columns(3)
                for ci3,img_url2 in enumerate(cimgs[row_s:row_s+3]):
                    try:
                        ri2 = requests.get(img_url2, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
                        if ri2.ok: rcols2[ci3].image(ri2.content, caption=f"#{row_s+ci3+1}", use_container_width=True)
                    except: rcols2[ci3].caption(f"#{row_s+ci3+1} ошибка")
            st.caption(f"Всего: {len(cimgs)} фото")
        else: st.warning("Нет фото")
    with ct3:
        ca1,ca2 = st.columns(2)
        ca1.metric("Цена", cprice); ca2.metric("Рейтинг", f"{crating}")
        ca1.metric("Отзывов", creviews); ca2.metric("BSR", cbsr[:25])
        ca1.metric("Материал", cmaterial or "—"); ca2.metric("Prime", "Да" if cprime else "Нет")
        ca1.metric("Цветов", len(ccolors)); ca2.metric("Размеров", len(csizes))
