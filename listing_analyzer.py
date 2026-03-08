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

    # ── Health Score Dashboard ──────────────────────────────────────────────
    health = r.get("health_score", 0)
    hb = r.get("health_breakdown", {})
    h_color = "#22c55e" if health >= 75 else ("#f59e0b" if health >= 50 else "#ef4444")
    h_label = "Отличный листинг" if health >= 75 else ("Есть над чем работать" if health >= 50 else "Требует срочных улучшений")
    h_emoji = "🟢" if health >= 75 else ("🟡" if health >= 50 else "🔴")

    our_data_h = st.session_state.get("our_data", {})
    pi_h = our_data_h.get("product_information", {})
    asin_h = our_data_h.get("parent_asin", "") or pi_h.get("ASIN","")
    brand_h = our_data_h.get("brand","")
    price_h = our_data_h.get("price","")
    rating_h = our_data_h.get("average_rating","")
    reviews_h = pi_h.get("Customer Reviews",{}).get("ratings_count","")
    bsr_h = str(pi_h.get("Best Sellers_rank", pi_h.get("Best Sellers Rank","")))[:60]
    title_h = our_data_h.get("title","")
    title_len = len(title_h)

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e293b,#334155);border-radius:16px;padding:24px;margin-bottom:20px;color:white">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px">
    <div>
      <div style="font-size:0.85rem;opacity:0.7;margin-bottom:4px">{brand_h} · {asin_h}</div>
      <div style="font-size:1rem;font-weight:600;max-width:500px;line-height:1.3">{title_h[:80]}{"..." if len(title_h)>80 else ""}</div>
      <div style="display:flex;gap:16px;margin-top:8px;font-size:0.85rem;opacity:0.8">
        <span>💰 {price_h}</span>
        <span>⭐ {rating_h} ({reviews_h} отз.)</span>
        <span>📊 {bsr_h[:40]}</span>
        <span style="color:{'#fca5a5' if title_len>125 else '#86efac'}">📝 Title: {title_len} симв.</span>
      </div>
    </div>
    <div style="text-align:center;min-width:120px">
      <div style="font-size:3.5rem;font-weight:800;color:{h_color};line-height:1">{health}%</div>
      <div style="font-size:0.9rem;color:{h_color};margin-top:4px">{h_emoji} {h_label}</div>
    </div>
  </div>
  <div style="margin-top:16px;background:rgba(255,255,255,0.1);border-radius:8px;height:12px">
    <div style="background:{h_color};width:{health}%;height:12px;border-radius:8px;transition:width 0.5s"></div>
  </div>
  <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:16px">
""", unsafe_allow_html=True)

    score_items = [
        ("Title", hb.get("title",0), 10),
        ("Bullets", hb.get("bullets",0), 10),
        ("Описание", hb.get("description",0), 10),
        ("Фото", hb.get("photos",0), 10),
        ("A+", hb.get("aplus",0), 10),
        ("Отзывы", hb.get("reviews",0), 10),
        ("BSR", hb.get("bsr",0), 10),
        ("Цена", hb.get("price",0), 10),
        ("Варианты", hb.get("variants",0), 10),
        ("Prime", hb.get("prime",0), 10),
    ]
    cols = st.columns(len(score_items))
    for col, (label, val, mx) in zip(cols, score_items):
        pct = int(val/mx*100) if mx else 0
        c2 = "#22c55e" if pct>=75 else ("#f59e0b" if pct>=50 else "#ef4444")
        col.markdown(f"""<div style="background:rgba(255,255,255,0.08);border-radius:8px;padding:8px;text-align:center">
<div style="font-size:1.3rem;font-weight:700;color:{c2}">{pct}%</div>
<div style="font-size:0.7rem;opacity:0.7;margin-top:2px">{label}</div>
</div>""", unsafe_allow_html=True)

    st.markdown("</div></div>", unsafe_allow_html=True)
    st.divider()
    # ── End Health Score ─────────────────────────────────────────────────────

    if v:
        st.subheader("📸 Vision анализ фотографий")
        images_stored = st.session_state.get("images", [])

        # Split by photo blocks
        blocks = re.split(r"PHOTO_BLOCK_\d+", v)
        blocks = [b.strip() for b in blocks if b.strip()]

        for i, img in enumerate(images_stored):
            text = blocks[i] if i < len(blocks) else ""
            score_match = re.search(r"(\d+)/10", text)
            score = int(score_match.group(1)) if score_match else 0
            bar_color = "#22c55e" if score >= 8 else ("#f59e0b" if score >= 6 else "#ef4444")
            score_label = "Отлично" if score >= 8 else ("Хорошо" if score >= 6 else "Слабо")

            # Parse fields
            typ = re.search(r"[Тт]ип:\s*(.+)", text)
            strong = re.search(r"[Сс]ильная сторона:\s*(.+)", text)
            weak = re.search(r"[Сс]лабость:\s*(.+)", text)
            photo_type = typ.group(1).strip() if typ else f"Фото #{i+1}"
            strong_text = strong.group(1).strip() if strong else ""
            weak_text = weak.group(1).strip() if weak else ""

            img_bytes = __import__("base64").b64decode(img["b64"])
            with st.container(border=True):
                c1, c2 = st.columns([1, 2])
                with c1:
                    st.image(img_bytes, use_container_width=True)
                with c2:
                    st.markdown(f"**Фото #{i+1} — {photo_type}**")
                    st.markdown(f"""
<div style="display:flex;align-items:center;gap:12px;margin:8px 0">
  <div style="font-size:2rem;font-weight:700;color:{bar_color}">{score}/10</div>
  <div style="flex:1">
    <div style="background:#e5e7eb;border-radius:6px;height:10px">
      <div style="background:{bar_color};width:{score*10}%;height:10px;border-radius:6px"></div>
    </div>
    <div style="color:{bar_color};font-size:0.8rem;margin-top:2px">{score_label}</div>
  </div>
</div>
""", unsafe_allow_html=True)
                    if strong_text:
                        st.success(f"✅ {strong_text}")
                    if weak_text:
                        st.warning(f"⚠️ {weak_text}")

        if not images_stored:
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

    our_data_stored = st.session_state.get("our_data", {})
    comp_data_stored = st.session_state.get("comp_data_list", [])
    our_title = our_data_stored.get("title", "")
    our_bullets = our_data_stored.get("feature_bullets", [])
    our_desc = our_data_stored.get("description", "")

    st.subheader("📝 Оценки")
    t1, t2, t3 = st.tabs(["Текст", "Визуал", "🏆 Сравнение с конкурентами"])
    with t1:
        section("Title", r.get("title_score",0), r.get("title_gaps",[]), r.get("title_rec",""),
                raw_text=our_title, char_limit=125)
        st.divider()
        bullets_text = "\n".join([f"• {b}" for b in our_bullets]) if our_bullets else ""
        section("Bullets", r.get("bullets_score",0), r.get("bullets_gaps",[]), r.get("bullets_rec",""),
                raw_text=bullets_text)
        st.divider()
        section("Description", r.get("desc_score",0), r.get("desc_gaps",[]), r.get("desc_rec",""),
                raw_text=str(our_desc)[:300] if our_desc else "")
    with t2:
        section("Фото", r.get("photos_score",0), r.get("photos_gaps",[]), r.get("photos_rec",""))
        st.divider()
        section("A+", r.get("aplus_score",0), r.get("aplus_gaps",[]), r.get("aplus_rec",""))
    with t3:
        if not comp_data_stored:
            st.info("Добавь конкурентов в поле выше и запусти анализ повторно")
        else:
            # Build comparison table
            metrics = [
                ("Название", lambda d: d.get("title","")[:60]+"..." if len(d.get("title",""))>60 else d.get("title","")),
                ("Цена", lambda d: d.get("price","")),
                ("Рейтинг ⭐", lambda d: d.get("average_rating","")),
                ("Отзывов", lambda d: d.get("product_information",{}).get("Customer Reviews",{}).get("ratings_count","")),
                ("BSR", lambda d: str(d.get("product_information",{}).get("Best Sellers Rank",""))[:50]),
                ("Материал", lambda d: d.get("product_information",{}).get("Material Type","")),
                ("A+", lambda d: "✅" if d.get("aplus") else "❌"),
                ("Видео", lambda d: str(d.get("number_of_videos","0"))),
                ("Фото", lambda d: str(len(d.get("images",[])))),
                ("Prime", lambda d: "✅" if d.get("is_prime_exclusive") else "—"),
            ]
            all_products = [our_data_stored] + comp_data_stored
            labels = ["🔵 НАШ"] + [f"Конкурент {i+1}" for i in range(len(comp_data_stored))]

            header_cols = st.columns(len(all_products)+1)
            header_cols[0].markdown("**Метрика**")
            for j, label in enumerate(labels):
                header_cols[j+1].markdown(f"**{label}**")
            st.divider()

            for metric_name, get_val in metrics:
                row_cols = st.columns(len(all_products)+1)
                row_cols[0].caption(metric_name)
                vals = [get_val(p) for p in all_products]
                for j, val in enumerate(vals):
                    if j == 0:
                        row_cols[j+1].markdown(f"**{val}**")
                    else:
                        row_cols[j+1].caption(str(val))
            
            # Score comparison
            st.divider()
            st.markdown("**📊 Оценки листинга**")
            score_metrics = [
                ("Title", "title_score"),
                ("Bullets", "bullets_score"),
                ("Description", "desc_score"),
                ("Фото", "photos_score"),
                ("A+", "aplus_score"),
                ("COSMO", "cosmo_score"),
            ]
            score_cols = st.columns(len(score_metrics))
            for col, (label, key) in zip(score_cols, score_metrics):
                val = r.get(key, 0)
                max_val = 100 if key == "cosmo_score" else 10
                col.metric(label, f"{val}/{max_val}", delta=None)

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

    # COSMO Section
    if r.get("cosmo_score") or r.get("cosmo_semantic"):
        st.divider()
        st.subheader("🧠 COSMO / Rufus Анализ")
        cosmo_score = r.get("cosmo_score", 0)
        color = "🟢" if cosmo_score >= 80 else ("🟡" if cosmo_score >= 60 else "🔴")
        st.metric("COSMO Score", f"{color} {cosmo_score}/100")
        st.progress(cosmo_score/100)

        if r.get("cosmo_semantic"):
            st.markdown("**📡 Семантические связи**")
            status_icon = {"WELL-DEVELOPED":"✅","GOOD":"✅","ADEQUATE":"⚠️","PARTIAL":"⚠️","MINIMAL":"❌"}
            for rel in r["cosmo_semantic"]:
                icon = status_icon.get(rel.get("status",""), "❓")
                with st.container(border=True):
                    st.markdown(f"{icon} **{rel.get('relationship','')}** — {rel.get('status','')}")
                    if rel.get("evidence"): st.caption(f"✓ {rel['evidence']}")
                    if rel.get("opportunity"): st.info(f"💡 {rel['opportunity']}")

        col1, col2, col3 = st.columns(3)
        with col1:
            if r.get("rufus_answered"):
                st.markdown("**✅ Rufus отвечает**")
                for q in r["rufus_answered"]:
                    with st.container(border=True):
                        st.caption(q.get("question",""))
                        st.success(q.get("answer","")[:150])
        with col2:
            if r.get("rufus_partial"):
                st.markdown("**⚠️ Частично**")
                for q in r["rufus_partial"]:
                    with st.container(border=True):
                        st.caption(q.get("question",""))
                        st.warning(q.get("gap","")[:150])
        with col3:
            if r.get("rufus_missing"):
                st.markdown("**❌ Не отвечает**")
                for q in r["rufus_missing"]:
                    with st.container(border=True):
                        st.caption(q.get("question",""))
                        st.error(q.get("missing","")[:150])

    with st.expander("🔧 Raw JSON"):
        st.json(r)
