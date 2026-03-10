# Amazon Listing Analyzer v2 — MR.EQUIPP
import json, re, base64, requests, streamlit as st
from PIL import Image
import io

ANTHROPIC_URL   = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-3-haiku-20240307"

SCHEMA = '{"overall_score":"XX%","title_score":"XX%","bullets_score":"XX%","description_score":"XX%","images_score":"XX%","qa_score":"XX%","reviews_score":"XX%","aplus_score":"XX%","price_score":"XX%","availability_score":"XX%","average_rating_score":"XX%","total_reviews_score":"XX%","bsr_score":"XX%","keywords_score":"XX%","prime_score":"XX%","returns_score":"XX%","customization_score":"XX%","first_available_score":"XX%","title_gaps":["specific title issue"],"title_rec":"specific title recommendation","bullets_gaps":["specific bullets issue"],"bullets_rec":"specific bullets recommendation","description_gaps":["specific description issue"],"description_rec":"specific description recommendation","aplus_gaps":["specific A+ issue"],"aplus_rec":"specific A+ recommendation","images_gaps":["specific images issue"],"images_rec":"specific images recommendation","images_breakdown":{"main_image":"XX% - reason","gallery":"XX% - reason","ocr_readability":"XX% - reason"},"cosmo_analysis":{"score":"XX%","signals_present":["signal with evidence"],"signals_missing":["missing signal"]},"rufus_analysis":{"score":"XX%","issues":["specific issue"]},"priority_improvements":["1. specific action","2. specific action","3. specific action"],"missing_chars":[{"name":"characteristic name","how_competitors_use":"how they use it","priority":"HIGH"}],"tech_params":[{"param":"parameter name","competitor_value":"their value","our_gap":"our gap"}],"actions":[{"action":"specific action","impact":"HIGH","effort":"LOW","details":"details"}]}'


def get_asin(url):
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None

def sc(s): return "🟢" if s>=8 else ("🟡" if s>=6 else "🔴")
def badge(p): return {"HIGH":"🔴 HIGH","MEDIUM":"🟡 MEDIUM","LOW":"🟢 LOW"}.get(p,p)

def get_asin_from_data(d):
    """Get ASIN from product data, using stored input ASIN as priority"""
    return d.get("_input_asin","") or d.get("parent_asin","") or d.get("product_information",{}).get("ASIN","")

def pct(val):
    """Parse score: int 0-100, int 0-10, or string 'XX%' -> int 0-100"""
    if isinstance(val, str):
        try: return int(val.replace("%","").strip())
        except: return 0
    if isinstance(val, (float, int)):
        v = int(val)
        return v if v > 10 else v * 10
    return 0

def sc_pct(val):
    v = pct(val)
    return "🟢" if v>=75 else ("🟡" if v>=50 else "🔴")

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
def analyze_vision(images, product_data, asin, log, lang=None):
    if not images: return ""
    if lang is None:
        lang = st.session_state.get("analysis_lang", "ru")
    log(f"👁️ Vision: {len(images)} фото → Anthropic...")

    title  = product_data.get("title","")
    price  = product_data.get("price","")
    rating = product_data.get("average_rating","")
    reviews= product_data.get("reviews_count","")
    bsr    = product_data.get("bestseller_rank","")

    if lang == "en":
        intro = f"""You are an Amazon photo conversion expert. Score each product photo using this RUBRIC.

Product: {title} | ASIN: {asin} | Price: {price} | Rating: {rating}

SCORING RUBRIC (each photo scored 1-10):
+2 pts — Subject clarity: product clearly visible, sharp focus, no blur
+2 pts — Background: main=pure white required; lifestyle=relevant setting; infographic=clean layout
+2 pts — Information value: shows features/benefits/use case relevant to buyer decision
+2 pts — Amazon compliance: no watermarks, no promotional text on main, correct aspect ratio
+1 pt  — Emotional/lifestyle appeal: buyer can visualize using the product
+1 pt  — Uniqueness vs generic stock photo

SCORE MEANINGS: 9-10=excellent, 7-8=good, 5-6=needs improvement, 1-4=poor/replace

PHOTO TYPES: main | lifestyle | infographic | size-chart | detail | A+-banner | comparison | packaging"""
        block_fmt = "\nPHOTO_BLOCK_{i}\nSTRICTLY 4 lines:\nType: [one of the types above]\nScore: X/10 [apply rubric]\nStrength: [1 specific strength of this photo]\nWeakness: [REQUIRED — 1 specific improvement: what exactly to add or change, even if photo is good. Never write 'None' or 'No weakness']"
    else:
        intro = f"""Ты эксперт по конверсии Amazon фотографий. Оценивай каждое фото по РУБРИКУ.

Товар: {title} | ASIN: {asin} | Цена: {price} | Рейтинг: {rating}

РУБРИК ОЦЕНКИ (каждое фото 1-10 баллов):
+2 балла — Чёткость объекта: товар хорошо виден, резкий фокус, нет размытия
+2 балла — Фон: главное фото=чисто белый; lifestyle=релевантная обстановка; инфографика=чистый макет
+2 балла — Информационная ценность: показывает характеристики/пользу/сценарий важный для покупателя
+2 балла — Соответствие Amazon: нет водяных знаков, нет промотекста на главном фото, правильное соотношение
+1 балл  — Эмоциональный/lifestyle appeal: покупатель представляет себя с товаром
+1 балл  — Уникальность: не выглядит как стоковое фото

ЗНАЧЕНИЯ: 9-10=отлично, 7-8=хорошо, 5-6=требует улучшения, 1-4=слабо/заменить

ТИПЫ ФОТ: главное | lifestyle | инфографика | размерная-сетка | детали | A+-баннер | сравнение | упаковка"""
        block_fmt = "\nPHOTO_BLOCK_{i}\nОТРОГО 4 строки:\nТип: [один из типов выше]\nОценка: X/10 [применяй рубрик]\nСильная сторона: [1 конкретная сильная сторона этого фото]\nСлабость: [ОБЯЗАТЕЛЬНО 1 конкретное улучшение — что именно добавить или изменить, даже если фото хорошее]"

    blocks = [{"type":"text","text": intro}]
    for i,img in enumerate(images):
        blocks.append({"type":"text","text": block_fmt.format(i=i+1)})
        blocks.append({"type":"image","source":{"type":"base64","media_type":img["media_type"],"data":img["b64"]}})

    result = anthropic_vision(blocks, max_tokens=2000)
    log(f"✅ Vision: {len(result)} символов")
    return result

# ── Text analysis ─────────────────────────────────────────────────────────────
def analyze_text(our_data, competitor_data_list, vision_result, asin, log, lang="ru"):
    log("🧠 Финальный анализ...")

    def fmt(data):
        if not data: return "нет данных"
        pi = data.get("product_information", {})
        bullets = data.get("feature_bullets", [])
        reviews = data.get("customer_reviews", [])
        review_texts = " | ".join([r.get("review_snippet","")[:100] for r in reviews[:5]])
        opts = data.get("customization_options", {})
        sizes  = [s.get("value","") for s in opts.get("size",[])]
        colors = [c.get("value","") for c in opts.get("color",[])]
        # Also check product_information for size field
        pi_size = pi.get("Size","") or pi.get("size","")
        prime = data.get("is_prime_exclusive", False) or data.get("is_prime", False)
        return "\n".join([
            f"Title: {data.get('title','')}",
            f"Price: {data.get('price','')} | Старая цена: {data.get('previous_price','')}",
            f"Rating: {data.get('average_rating','')} | Reviews: {pi.get('Customer Reviews',{}).get('ratings_count','')}",
            f"BSR: {pi.get('Best Sellers Rank','')}",
            f"Material: {pi.get('Material Type','')} | Fabric: {pi.get('Fabric Type','')}",
            f"Sizes (variants): {sizes if sizes else pi_size or 'не указаны'}",
            f"Colors (variants): {colors if colors else 'не указаны'}",
            f"Prime: {prime} | A+: {data.get('aplus',False)} | Videos: {data.get('number_of_videos',0)}",
            f"Images count: {len(data.get('images',[]))}",
            f"Bullets:\n{chr(10).join(bullets[:5])}",
            f"Reviews snippets: {review_texts}",
            f"Description: {str(data.get('description',''))[:300]}",
            f"A+ Content: {str(data.get('aplus_content','нет данных'))[:3000]}",
        ])

    our_text = fmt(our_data)
    comp_text = "\n\n".join([f"COMPETITOR {{i+1}}:\n{{fmt(d)}}" for i,d in enumerate(competitor_data_list) if d])
    vision_section = f"\nPHOTO VISION ANALYSIS:\n{{vision_result[:1500]}}" if vision_result else ""
    lang_name = "Russian" if lang == "ru" else "English"

    _ctx = st.session_state.get("ai_context_saved", st.session_state.get("ai_context","")).strip()
    context_section = f"\n\n## BRAND CONTEXT (provided by seller):\n{_ctx}" if _ctx else ""

    prompt = f"""You are an expert Amazon listing analyst specializing in the Listing 3.0 era where AI visibility (Cosmo + Rufus) determines 50% of success.

OUR LISTING (ASIN {asin}):
{our_text}

{comp_text}
{vision_section}
{context_section}

## YOUR TASK
Analyze the listing above and score each component. Use ONLY real data from the listing provided.

## SCORING CRITERIA

### TITLE — ≤125 chars, [Brand][Gender][Material][Type][Feature] format, top keywords, readable
- 90-100%: All criteria met
- 70-89%: Minor issues (slightly long, missing 1 element)
- 50-69%: Major issues (too long, poor structure, missing brand/material)
- 0-49%: Unreadable or broken

### BULLETS — 5 bullets, ≤250 chars each, "Feature: Details. Benefit." format
- 90-100%: All 5 bullets, perfect format, addresses customer concerns
- 70-89%: Good structure but missing benefits
- 50-69%: Walls of text, no benefits
- 0-49%: Missing bullets

### DESCRIPTION — HTML formatted, covers Benefits/Features/Care/Usage
- 90-100%: ≤2000 words, HTML, storytelling, all sections covered
- 70-89%: Decent but poor formatting
- 0-49%: Missing or duplicate of bullets

### IMAGES — Evaluate: main image (40%), gallery completeness (30%), OCR readability for Rufus (30%)
- Main 90-100%: Unique angle, shows product in action, clear at thumbnail
- Gallery 90-100%: 6+ images: lifestyle, infographics, size/scale, packaging, variants
- OCR 90-100%: Dark text on white background, sans-serif, horizontal layout
- Combined 90-100%: All three excellent

### Q&A — 10+ Q&As, brand responses, covers objections
### REVIEWS — 100+ reviews, 4.5+ stars, recent activity
### A+ CONTENT — Comparison charts, brand story, lifestyle imagery
### PRICE — Competitive + deal badge OR coupon OR Subscribe&Save
### AVAILABILITY — In stock, FBA, fast shipping
### BSR — Top 1%=100%, Top 5%=80%, Top 20%=60%, below=40%
### KEYWORDS — 5-10 phrases, ≤249 bytes, no duplicates from title
### PRIME — 100% if Prime, 0% if not
### RETURNS — Free returns=90%, standard=70%, restricted=50%
### CUSTOMIZATION — Multiple variants with clear images
### FIRST_AVAILABLE — 2+ years=90%, 1-2yr=70%, 6-12mo=60%, <6mo=40%
### AVERAGE_RATING — 4.7+=100%, 4.3-4.6=80%, 4.0-4.2=60%, <4.0=40%
### TOTAL_REVIEWS — 500+=100%, 100-499=80%, 20-99=60%, <20=40%

## COSMO AI (15 signals)
Score how well Amazon's Cosmo AI understands this product:
Use Cases, Audience, Functional Attributes, Material/Composition, Size/Dimensions,
Compatibility, Occasion/Setting, Season/Weather, Skill Level, Age Appropriateness,
Gender, Style/Aesthetic, Quality Tier, Problem Solved, Unique Value

## RUFUS RECOMMENDATION POTENTIAL
Evaluate: Relevance to Queries, Proof & Evidence, Visual Clarity (OCR), Completeness, Competitive Position

CRITICAL RULES:
- Return ONLY valid JSON, no markdown, no explanation
- All text fields in {lang_name}
- Use REAL data from the listing — no placeholder text
- overall_score = weighted average of all 17 scores
- images_score = 40% main + 30% gallery + 30% OCR combined
- title_gaps: list of 2-3 specific issues with the title
- bullets_gaps: list of 2-3 specific issues with the bullets
- description_gaps: list of 1-2 specific issues with the description
- aplus_gaps: list of 1-2 specific issues with the A+ content
- images_gaps: list of 1-2 specific issues with the images
- Each gap must be specific to THAT section, not generic

{SCHEMA}"""

    sys_prompt = f"Amazon listing expert. Return ONLY valid JSON. No markdown. No preamble. All text in {lang_name}."
    raw = anthropic_call(sys_prompt, prompt, max_tokens=4000)
    log(f"✅ JSON: {len(raw)} chars")

    s = raw.strip().replace("```json","").replace("```","").strip()
    start,end = s.find("{"),s.rfind("}")
    if start==-1: start,end = s.find("{"),s.rfind("}")
    if start==-1: raise ValueError(f"JSON not found: {{raw[:200]}}")
    s = re.sub(r",\s*([}\]])", r"\1", s[start:end+1])
    try: return json.loads(s)
    except:
        s2 = re.sub(r'"([^"]*)"', lambda m: '"'+m.group(1).replace('\n',' ')+'"', s)
        return json.loads(re.sub(r",\s*([}\]])", r"\1", s2))

# ── Main ──────────────────────────────────────────────────────────────────────
def run_analysis(our_url, competitor_urls, log, prog=None):
    def _prog(pct, text):
        if prog: prog.progress(pct, text=text)
        log(text)

    asin = get_asin(our_url) or "unknown"
    n_comps = len([u for u in competitor_urls if u.strip()])
    # Steps: scrape(10) + photos(20) + vision(35) + comps*N(50) + AI(90) + done(100)

    _prog(5,  f"🌐 Загружаю данные листинга {asin}...")
    our_data, img_urls = scrapingdog_product(asin, log)

    _prog(15, f"⬇️ Скачиваю фото ({len(img_urls)} шт.)...")
    images = download_images(img_urls, log) if img_urls else []
    st.session_state["images"] = images

    _lang = st.session_state.get("analysis_lang","ru")
    _prog(30, "👁️ Vision анализ фото...")
    vision_result = analyze_vision(images, our_data, asin, log, lang=_lang) if images else ""
    if not images: log("⚠️ Фото не загружены")

    # Competitors — full analysis (scrape + vision + AI) for each
    active = [u.strip() for u in competitor_urls if u.strip()]
    comp_data_list = []
    _lang = st.session_state.get("analysis_lang","ru")
    n_active = max(len(active), 1)

    for i, url in enumerate(active[:3]):
        casin = get_asin(url)
        if not casin: continue
        base_pct = 50 + i * (20 // n_active)

        _prog(base_pct, f"🌐 Конкурент {i+1}: загружаю {casin}...")
        cdata, cimg_urls = scrapingdog_product(casin, log)
        cdata["_input_asin"] = casin
        comp_data_list.append(cdata)

        _prog(base_pct + 3, f"⬇️ Конкурент {i+1}: скачиваю фото...")
        cimgs_dl = download_images(cimg_urls[:5], log) if cimg_urls else []

        _prog(base_pct + 5, f"👁️ Конкурент {i+1}: Vision анализ...")
        cvision = analyze_vision(cimgs_dl, cdata, casin, log, lang=_lang) if cimgs_dl else ""

        _prog(base_pct + 8, f"🧠 Конкурент {i+1}: AI анализ...")
        cai = analyze_text(cdata, [], cvision, casin, log, lang=_lang)

        # Store in session state — same keys as the manual button
        st.session_state[f"comp_ai_{i}"] = cai
        if cimgs_dl:
            st.session_state[f"comp_vision_{i}"] = (cimgs_dl, cvision)

    _prog(75, "🧠 AI финальный анализ нашего листинга...")
    result = analyze_text(our_data, comp_data_list, vision_result, asin, log, lang=_lang)
    st.session_state['our_data'] = our_data
    st.session_state['comp_data_list'] = comp_data_list
    return result, vision_result

# ── UI ────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Listing Analyzer", page_icon="🔍", layout="wide")

with st.sidebar:
    st.markdown("## 🔍 Listing Analyzer")
    st.divider()

    # Navigation via buttons
    if "page" not in st.session_state:
        st.session_state["page"] = "🏠 Обзор"

    NAV_ITEMS = [
        ("🏠", "Обзор"),
        ("📸", "Фото"),
        ("📝", "Контент"),
        ("🏆", "Benchmark"),
        ("🧠", "COSMO / Rufus"),
    ]

    if "result" in st.session_state:
        _our = st.session_state.get("our_data", {})
        _our_asin = _our.get("parent_asin","") or _our.get("product_information",{}).get("ASIN","")
        _our_title = _our.get("title","")
        _cd_nav = st.session_state.get("comp_data_list", [])

        st.markdown(f"""<div style="background:#f0f9ff;border-radius:8px;padding:8px 10px;margin-bottom:4px;border-left:3px solid #3b82f6">
<div style="font-size:0.75rem;font-weight:700;color:#1d4ed8">🔵 {_our_asin}</div>
<div style="font-size:0.7rem;color:#64748b;margin-top:1px">{_our_title[:30]}{"..." if len(_our_title)>30 else ""}</div>
</div>""", unsafe_allow_html=True)

        for _i, _c in enumerate(_cd_nav):
            _cpi = _c.get("product_information", {})
            _casin = get_asin_from_data(_c)
            _ct = _c.get("title","")
            st.markdown(f"""<div style="background:#fff5f5;border-radius:8px;padding:8px 10px;margin-bottom:4px;border-left:3px solid #ef4444">
<div style="font-size:0.75rem;font-weight:700;color:#b91c1c">🔴 {_casin}</div>
<div style="font-size:0.7rem;color:#64748b;margin-top:1px">{_ct[:30]}{"..." if len(_ct)>30 else ""}</div>
</div>""", unsafe_allow_html=True)

        st.divider()

        cur = st.session_state.get("page","🏠 Обзор")

        # ── МЫ ──────────────────────────────────────────────────────
        st.markdown('<div style="font-size:0.7rem;font-weight:700;color:#94a3b8;letter-spacing:0.08em;padding:4px 2px">МЫ</div>', unsafe_allow_html=True)
        for icon, label in NAV_ITEMS:
            full = f"{icon} {label}"
            is_active = (cur == full)
            if st.button(f"{icon}  {label}", key=f"nav_{label}", use_container_width=True,
                         type="primary" if is_active else "secondary"):
                st.session_state["page"] = full
                st.rerun()

        # ── КОНКУРЕНТЫ ───────────────────────────────────────────────
        if _cd_nav:
            st.markdown('<div style="font-size:0.7rem;font-weight:700;color:#94a3b8;letter-spacing:0.08em;padding:12px 2px 4px">КОНКУРЕНТЫ</div>', unsafe_allow_html=True)
            for _i2, _c2 in enumerate(_cd_nav):
                _cpi2 = _c2.get("product_information", {})
                _casin2 = get_asin_from_data(_c2)
                _ct2 = _c2.get("title","")
                _ct2_short = _ct2[:20]+"..." if len(_ct2)>20 else _ct2
                full = f"🔴 Конкурент {_i2+1}"
                is_active = cur.startswith(f"🔴 Конкурент {_i2+1}")
                if st.button(f"🔴  Конкурент {_i2+1}", key=f"nav_comp_{_i2}", use_container_width=True,
                             type="primary" if is_active else "secondary"):
                    st.session_state["page"] = full
                    st.rerun()
                st.caption(f"  {_casin2}  {_ct2_short}")
    else:
        st.caption("Запусти анализ чтобы открыть все страницы")
        for icon, label in NAV_ITEMS:
            st.markdown(f'<div style="padding:7px 10px;color:#94a3b8;font-size:0.9rem">{icon} {label}</div>', unsafe_allow_html=True)

    st.divider()
    st.markdown("**🔑 API**")
    if st.button("🧪 Anthropic", key="api_test"):
        try:
            res = anthropic_call(None, "Say: OK", max_tokens=5)
            st.success(f"✅ {res}")
        except Exception as e:
            st.error(f"❌ {str(e)[:60]}")

# ── Input always visible at top ───────────────────────────────────────────────
with st.expander("📎 Листинги", expanded=("result" not in st.session_state)):
    our_url = st.text_input("🔵 НАШ листинг", value=st.session_state.get("our_url_saved","https://www.amazon.com/dp/B0D6WBQ7G1"))
    c1, c2, c3, c4, c5 = st.columns(5)
    comp1 = c1.text_input("Конкурент 1", key="c0", value=st.session_state.get("c0_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp2 = c2.text_input("Конкурент 2", key="c1", value=st.session_state.get("c1_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp3 = c3.text_input("Конкурент 3", key="c2", value=st.session_state.get("c2_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp4 = c4.text_input("Конкурент 4", key="c3", value=st.session_state.get("c3_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp5 = c5.text_input("Конкурент 5", key="c4", value=st.session_state.get("c4_saved",""), placeholder="https://www.amazon.com/dp/...")
    competitor_urls = [comp1, comp2, comp3, comp4, comp5]

    # Detect what changed
    prev_url  = st.session_state.get("our_url_saved","")
    prev_comp = [st.session_state.get(f"c{i}_saved","") for i in range(5)]
    curr_comp = competitor_urls
    our_changed  = (our_url.strip() != prev_url.strip())
    new_comps    = [u for u,p in zip(curr_comp,prev_comp) if u.strip() and u.strip()!=p.strip()]
    already_done = "result" in st.session_state

    # Button label hint
    if already_done and not our_changed and new_comps:
        btn_label = f"➕ Добавить {len(new_comps)} конкурент(а)"
    elif already_done and not our_changed and not new_comps:
        btn_label = "🔄 Перезапустить анализ"
    else:
        btn_label = "🚀 Запустить анализ"

    lang = st.radio("🌐 Язык анализа", ["🇷🇺 Русский", "🇺🇸 English"], horizontal=True, key="lang_sel")
    st.session_state["analysis_lang"] = "ru" if "Русский" in lang else "en"

    with st.expander("🎯 Фокус анализа (необязательно)", expanded=False):
        st.caption("Помогает AI расставить приоритеты")
        _fa1, _fa2, _fa3 = st.columns(3)
        with _fa1:
            goal = st.radio("🎯 Цель", [
                "Полный аудит",
                "Поднять конверсию",
                "Выйти в топ поиска",
                "Победить конкурента",
            ], key="goal_sel")
        with _fa2:
            audience = st.radio("👥 Аудитория", [
                "Не указано",
                "Спортсмены",
                "Outdoor / туризм",
                "Everyday / офис",
                "Business casual",
            ], key="aud_sel")
        with _fa3:
            positioning = st.radio("💰 Позиционирование", [
                "Не указано",
                "Бюджет",
                "Средний сегмент",
                "Премиум",
            ], key="pos_sel")

        _ctx_parts = [f"Analysis goal: {goal}"]
        if audience != "Не указано":   _ctx_parts.append(f"Target audience: {audience}")
        if positioning != "Не указано": _ctx_parts.append(f"Brand positioning: {positioning}")
        st.session_state["ai_context"] = " | ".join(_ctx_parts)

    _bcol1, _bcol2 = st.columns([3, 1])
    with _bcol1:
        _run_btn = st.button(btn_label, type="primary", disabled=not our_url.strip(), use_container_width=True)
    with _bcol2:
        if st.button("🗑️ Сброс", type="secondary", use_container_width=True, help="Очистить всё и начать заново"):
            for _k in list(st.session_state.keys()):
                del st.session_state[_k]
            st.rerun()

    if _run_btn:
        lines = []; ph = st.empty()
        def log(msg):
            lines.append(msg); ph.markdown("\n\n".join(lines[-8:]))

        _main_prog = st.progress(0, text="🚀 Запускаю анализ...")
        try:
                # SMART: only re-run full analysis if our URL changed or first run
                if not already_done or our_changed:
                    result, vision = run_analysis(our_url, competitor_urls, log, prog=_main_prog)
                    st.session_state.update({"result": result, "vision": vision})
                    st.session_state["our_url_saved"] = our_url
                    st.session_state["c0_saved"] = comp1
                    st.session_state["c1_saved"] = comp2
                    st.session_state["c2_saved"] = comp3
                    st.session_state["c3_saved"] = comp4
                    st.session_state["c4_saved"] = comp5
                    st.session_state["ai_context_saved"] = st.session_state.get("ai_context","")
                else:
                    # Only fetch new competitors
                    existing = st.session_state.get("comp_data_list", [])
                    for i, url in enumerate(curr_comp):
                        if url.strip() and url.strip() != prev_comp[i].strip():
                            casin = get_asin(url)
                            if casin:
                                log(f"➕ Новый конкурент {i+1}: {casin}...")
                                cdata, _ = scrapingdog_product(casin, log)
                                # Replace or append
                                if i < len(existing):
                                    existing[i] = cdata
                                else:
                                    existing.append(cdata)
                                st.session_state[f"c{i}_saved"] = url.strip()
                    st.session_state["comp_data_list"] = existing
                    # Re-run only text analysis with updated competitors
                    log("🧠 Обновляю сравнительный анализ...")
                    od_s = st.session_state.get("our_data", {})
                    v_s  = st.session_state.get("vision", "")
                    asin_s = get_asin(our_url) or "unknown"
                    _lang = st.session_state.get("analysis_lang","ru")
                    result = analyze_text(od_s, existing, v_s, asin_s, log, lang=_lang)
                    st.session_state["result"] = result

                _main_prog.progress(100, text="✅ Анализ завершён!")
                st.rerun()
        except Exception as e:
                st.error(f"Ошибка: {e}")

# ── Pages ─────────────────────────────────────────────────────────────────────
page = st.session_state.get("page", "🏠 Обзор")
_is_competitor_page = page.startswith("🔴 Конкурент")

if "result" not in st.session_state:
    st.markdown("""
<div style="max-width:720px;margin:40px auto 0">
<h1 style="font-size:2rem;font-weight:800;margin-bottom:4px">🔍 Amazon Listing Analyzer</h1>
<p style="color:#64748b;font-size:1rem;margin-bottom:32px">Listing 3.0 — AI-анализ на основе COSMO + Rufus + Vision</p>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:32px">

<div style="background:#f8fafc;border-radius:12px;padding:16px;border-left:4px solid #3b82f6">
<div style="font-weight:700;margin-bottom:8px">⚡ Что делает инструмент</div>
<div style="font-size:0.88rem;color:#475569;line-height:1.6">
• Загружает данные листинга через ScrapingDog API<br>
• Анализирует <b>17 метрик</b> по рубрику Listing 3.0<br>
• Vision AI оценивает каждое фото по 6 критериям<br>
• Проверяет как Cosmo и Rufus понимают товар<br>
• Сравнивает до 5 конкурентов
</div>
</div>

<div style="background:#f8fafc;border-radius:12px;padding:16px;border-left:4px solid #10b981">
<div style="font-weight:700;margin-bottom:8px">🚀 Как запустить</div>
<div style="font-size:0.88rem;color:#475569;line-height:1.6">
1. Вставь ссылку на <b>наш листинг</b> (Amazon URL)<br>
2. Добавь ссылки на <b>конкурентов</b> (до 5 штук)<br>
3. Выбери язык и фокус анализа<br>
4. Нажми <b>🚀 Запустить анализ</b><br>
5. Подожди ~2-3 мин — анализ всех листингов
</div>
</div>

<div style="background:#f8fafc;border-radius:12px;padding:16px;border-left:4px solid #f59e0b">
<div style="font-weight:700;margin-bottom:8px">📊 Что получишь</div>
<div style="font-size:0.88rem;color:#475569;line-height:1.6">
• <b>Overall Score</b> — итоговый балл листинга<br>
• <b>Vision анализ</b> — оценка каждого фото 1-10<br>
• <b>Benchmark</b> — подиум vs конкуренты<br>
• <b>COSMO / Rufus</b> — AI-видимость товара<br>
• <b>Приоритетные действия</b> — что улучшить
</div>
</div>

<div style="background:#f8fafc;border-radius:12px;padding:16px;border-left:4px solid #8b5cf6">
<div style="font-weight:700;margin-bottom:8px">💡 Советы</div>
<div style="font-size:0.88rem;color:#475569;line-height:1.6">
• Используй <b>Русский</b> для внутреннего анализа<br>
• <b>English</b> — для листингов на .com рынке<br>
• Заполни <b>Фокус анализа</b> для точных рек.<br>
• Кнопка <b>🗑️ Сброс</b> — полная очистка данных<br>
• Анализ кэшируется — можно листать без перезапуска
</div>
</div>

</div>
<p style="text-align:center;color:#94a3b8;font-size:0.8rem">👈 Введи ссылку на листинг в форме выше и нажми «Запустить анализ»</p>
</div>
""", unsafe_allow_html=True)
    st.stop()

r  = st.session_state["result"]
v  = st.session_state.get("vision", "")
od = st.session_state.get("our_data", {})
pi = od.get("product_information", {})
cd = st.session_state.get("comp_data_list", [])
imgs = st.session_state.get("images", [])

# ── Helpers ───────────────────────────────────────────────────────────────────
def health_card():
    health = pct(r.get("overall_score", r.get("health_score", 0)))
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

    items = [
        ("Title",   r.get("title_score",0)),
        ("Bullets", r.get("bullets_score",0)),
        ("Описание",r.get("description_score",0)),
        ("Фото",    r.get("images_score",0)),
        ("A+",      r.get("aplus_score",0)),
        ("Отзывы",  r.get("reviews_score",0)),
        ("BSR",     r.get("bsr_score",0)),
        ("Цена",    r.get("price_score",0)),
        ("Варианты",r.get("customization_score",0)),
        ("Prime",   r.get("prime_score",0)),
    ]
    cols = st.columns(len(items))
    for col,(lbl,val) in zip(cols,items):
        p2 = pct(val)
        cc = "#22c55e" if p2>=75 else ("#f59e0b" if p2>=50 else "#ef4444")
        col.markdown(f'<div style="background:#f1f5f9;border-radius:8px;padding:8px 4px;text-align:center;border-left:3px solid {cc}"><div style="font-size:1.2rem;font-weight:700;color:{cc}">{p2}%</div><div style="font-size:0.68rem;color:#64748b">{lbl}</div></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Обзор
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Обзор":
    st.title("🏠 Обзор листинга")
    health_card()
    st.divider()

    _sum = r.get("summary", "")
    _cosmo = r.get("cosmo_analysis",{})
    _rufus = r.get("rufus_analysis",{})
    if _sum: st.info(f"**📋 Резюме:** {_sum}")

    # Cosmo + Rufus summary
    if _cosmo or _rufus:
        cc1, cc2 = st.columns(2)
        with cc1:
            cs = pct(_cosmo.get("score",0))
            cc = "#22c55e" if cs>=75 else ("#f59e0b" if cs>=50 else "#ef4444")
            st.markdown(f"**🧠 COSMO:** <span style='color:{cc};font-size:1.2rem;font-weight:700'>{cs}%</span>", unsafe_allow_html=True)
            for sig in _cosmo.get("signals_missing",[])[:3]: st.caption(f"❌ {sig}")
        with cc2:
            rs = pct(_rufus.get("score",0))
            rc = "#22c55e" if rs>=75 else ("#f59e0b" if rs>=50 else "#ef4444")
            st.markdown(f"**🤖 Rufus:** <span style='color:{rc};font-size:1.2rem;font-weight:700'>{rs}%</span>", unsafe_allow_html=True)
            for iss in _rufus.get("issues",[])[:3]: st.caption(f"⚠️ {iss}")

    actions = r.get("actions", [])
    priority_improvements = r.get("priority_improvements", [])
    if actions or priority_improvements:
        st.subheader("🎯 Приоритетные действия")
        # New format: priority_improvements is list of strings
        for item in priority_improvements:
            with st.container(border=True):
                st.markdown(f"**{item}**")
        # Old/mixed format: actions is list of dicts
        for i, a in enumerate(actions):
            if isinstance(a, dict):
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

    # Split by markers, skip any block without a score (intro text)
    _all_blocks = re.split(r"PHOTO_BLOCK_\d+", v)
    blocks = [b.strip() for b in _all_blocks if b.strip() and re.search(r"\d+/10", b)]
    # Fallback: if filtering removed all blocks, use raw split
    if not blocks:
        blocks = [b.strip() for b in _all_blocks if b.strip()]

    for i, img in enumerate(imgs):
        text = blocks[i] if i < len(blocks) else ""
        sm   = re.search(r"(\d+)/10", text)
        score = int(sm.group(1)) if sm else 0
        bc    = "#22c55e" if score>=8 else ("#f59e0b" if score>=6 else "#ef4444")
        slbl  = "Отлично" if score>=8 else ("Хорошо" if score>=6 else "Слабо")
        # Flexible parsing: RU and EN
        typ  = re.search(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)", text)
        # Broad regex: match Strength/Weakness labels + content on same line (3+ chars)
        strg = re.search(r"(?:[Сс]ильная\s+сторона|Strength|(?<!\w)✅)\s*[:\-]?\s*(.{3,})", text)
        weak = re.search(r"(?:[Сс]лабость|Weakness|(?<!\w)⚠️)\s*[:\-]?\s*(.{3,})", text)
        ptype = typ.group(1).strip().rstrip(".") if typ else ""
        stxt  = strg.group(1).strip() if strg else ""
        wtxt  = weak.group(1).strip() if weak else ""
        # Filter useless "no weakness" answers
        if wtxt and any(x in wtxt.lower() for x in ["нет.", "no.", "none", "n/a", "отсутствует", "полностью соответствует"]):
            wtxt = ""

        with st.container(border=True):
            c1,c2 = st.columns([1,2])
            with c1:
                st.image(__import__("base64").b64decode(img["b64"]), use_container_width=True)
            with c2:
                _head = f"Фото #{i+1}" + (f" — {ptype}" if ptype else "")
                st.markdown(f"**{_head}**")
                if score > 0:
                    bc = "#22c55e" if score>=8 else ("#f59e0b" if score>=6 else "#ef4444")
                    slbl = "Отлично" if score>=8 else ("Хорошо" if score>=6 else "Слабо")
                    st.markdown(f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0"><div style="font-size:2rem;font-weight:800;color:{bc}">{score}/10</div><div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px"><div style="background:{bc};width:{score*10}%;height:10px;border-radius:6px"></div></div><div style="color:{bc};font-size:0.8rem;margin-top:2px">{slbl}</div></div></div>', unsafe_allow_html=True)
                else:
                    st.warning("⚠️ Оценка не распознана")
                if stxt: st.success(f"✅ {stxt}")
                if wtxt: st.warning(f"⚠️ {wtxt}")
                # Debug: show raw if strength missing
                if not stxt and text:
                    with st.expander("🔧 Raw (Strength не распознан)"):
                        st.code(text[:400])

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Контент
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📝 Контент":
    st.title("📝 Анализ контента")
    our_title   = od.get("title","")
    our_bullets = od.get("feature_bullets",[])
    our_desc    = od.get("description","")

    def _sec(label, key, **kw):
        val = pct(r.get(key, 0))
        # Use section-specific gaps only — no fallback to priority_improvements
        gaps = r.get(key.replace("_score","_gaps"), [])
        rec  = r.get(key.replace("_score","_rec"), "")
        sc2  = sc_pct(val)
        c1,c2 = st.columns([4,1])
        c1.markdown(f"**{label}**"); c2.markdown(f"{sc2} **{val}%**")
        st.progress(val/100)
        if kw.get("raw_text"):
            cl = kw.get("char_limit",0); ct = len(kw["raw_text"])
            col = "red" if (cl and ct>cl) else "gray"
            st.markdown(f"<small style='color:{col}'>📝 {ct} симв{f' / {cl} лимит' if cl else ''}</small>", unsafe_allow_html=True)
            with st.expander("Показать текст"): st.markdown(f"> {kw['raw_text']}")
        if gaps and isinstance(gaps, list):
            with st.expander(f"⚠️ ({len(gaps)})"): 
                for g in gaps: st.markdown(f"- {g}")
        if rec: st.info(f"💡 {rec}")

    # ── Title ──────────────────────────────────────────────────────────────────
    _sec("Title", "title_score", raw_text=our_title, char_limit=125)
    _tlen = len(our_title)
    _twords = [w.lower() for w in our_title.split() if len(w)>3]
    _has_repeat = any(_twords.count(w)>=3 for w in _twords)
    _has_spec = any(c in our_title for c in "!$?{}^¬¦")
    _has_kw = any(w in our_title.lower() for w in ["merino","wool","tank","men","undershirt","shirt","layer"])
    _title_rubric = [
        ("Длина ≤125 симв.",     "15%", _tlen<=125,       f"{_tlen} симв."),
        ("Бренд+тип+материал",   "35%", _has_kw,          "ключевые слова есть" if _has_kw else "❌ нет ключевых слов"),
        ("Нет спецсимволов",     "10%", not _has_spec,    "есть ! $ ?" if _has_spec else ""),
        ("Нет повторов (≥3×)",   "10%", not _has_repeat,  "повтор найден" if _has_repeat else ""),
        ("Читаемость / цель",    "30%", True,             ""),
    ]
    with st.expander("📐 Рубрика оценки Title"):
        _th = st.columns([4,1,1,3])
        for _h,_lbl in zip(_th,["Критерий","Вес","Статус","Детали"]): _h.caption(f"**{_lbl}**")
        for _crit,_wt,_ok,_det in _title_rubric:
            _rc = st.columns([4,1,1,3])
            _rc[0].write(_crit); _rc[1].write(f"`{_wt}`"); _rc[2].write("✅" if _ok else "❌")
            if _det: _rc[3].caption(_det)
        st.divider()
        _ex1,_ex2 = st.columns(2)
        _ex1.success("✅ **Пример хорошего**\nMerino Wool Tank Top Men – Lightweight Base Layer for Hiking & Sport\n• <125 симв.  • бренд+матер+тип  • нет повторов")
        _ex2.error("⛔ **Пример плохого**\nBEST WOOL TANK! WOOL WOOL FOR MEN - SUPER!\n• спецсимволы !  • повтор WOOL  • кричащий стиль")

    st.divider()
    # ── Bullets ─────────────────────────────────────────────────────────────────
    bullets_text = "\n".join([f"• {b}" for b in our_bullets]) if our_bullets else ""
    _sec("Bullets", "bullets_score", raw_text=bullets_text)
    _bul_rubric = [
        ("5 буллетов",              "20%", len(our_bullets)==5,                      f"{len(our_bullets)} шт."),
        ("Формат «Фича: Выгода.»",  "30%", sum(1 for b in our_bullets if ":" in b)>=3, f"{sum(1 for b in our_bullets if ':' in b)}/5 с двоеточием"),
        ("≤250 байт каждый",        "25%", all(len(b.encode())<250 for b in our_bullets), ""),
        ("Нет ALL CAPS блоков",     "15%", not any(b[:15].isupper() for b in our_bullets), ""),
        ("Покрывает возражения",    "10%", True, "уход, размер, совместимость"),
    ]
    with st.expander("📐 Рубрика оценки Bullets"):
        _bh = st.columns([4,1,1,3])
        for _h,_lbl in zip(_bh,["Критерий","Вес","Статус","Детали"]): _h.caption(f"**{_lbl}**")
        for _crit,_wt,_ok,_det in _bul_rubric:
            _rc = st.columns([4,1,1,3])
            _rc[0].write(_crit); _rc[1].write(f"`{_wt}`"); _rc[2].write("✅" if _ok else "❌")
            if _det: _rc[3].caption(_det)

    st.divider()
    _sec("Description", "description_score", raw_text=str(our_desc)[:400] if our_desc else "")
    st.divider()
    _sec("A+",          "aplus_score")
    st.divider()
    _sec("Фото",        "images_score")

    # Images breakdown
    ib = r.get("images_breakdown", {})
    if ib:
        st.subheader("📸 Детализация фото")
        for k,v in ib.items():
            st.markdown(f"**{k}:** {v}")

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

    if not cd:
        st.info("Добавь конкурентов в форму выше и запусти анализ повторно")
        st.stop()

    # ── auto_score helper ────────────────────────────────────────────────────
    def auto_score(d):
        pi2 = d.get("product_information", {})
        title2  = d.get("title",""); imgs2 = d.get("images",[])
        bul2    = d.get("feature_bullets",[]); desc2 = d.get("description","")
        rating2 = float(d.get("average_rating",0) or 0)
        rev_cnt = int(pi2.get("Customer Reviews",{}).get("ratings_count","0") or 0)
        has_vid = int(d.get("number_of_videos",0) or 0) > 0
        has_ap  = bool(d.get("aplus")); is_prime = bool(d.get("is_prime_exclusive"))
        bsr_num = 99999
        bsr_m = re.search(r"#([\d,]+)", str(pi2.get("Best Sellers Rank","")))
        if bsr_m:
            try: bsr_num = int(bsr_m.group(1).replace(",",""))
            except: pass
        colors = len(d.get("customization_options",{}).get("color",[]))
        sizes  = len(d.get("customization_options",{}).get("size",[]))
        ts = min(10, max(0, (1.5 if len(title2)<=125 else 0) + (3.5 if any(k in title2.lower() for k in ["merino","wool","shirt","base layer","tank"]) else 1.5) + (3.0 if any(k in title2.lower() for k in ["sport","hiking","travel","daily","gym","outdoor"]) else 1.0) + (1.0 if not re.search(r"[!$?{}]",title2) else 0) + 1))
        bs = min(10, max(0, (1.5 if len(bul2)<=5 else 0) + (2.5 if any(":" in b for b in bul2) else 1.0) + (4.0 if len(bul2)>=4 else len(bul2)*1.0) + (1.0 if all(len(b.encode())<=255 for b in bul2) else 0) + 1))
        ds = 0 if not desc2 else min(10, 4+(3 if len(desc2)>200 else 1)+(2 if len(desc2)>500 else 0))
        ps = min(10, max(0, (4.0 if len(imgs2)>=6 else len(imgs2)*0.6)+(2.0 if has_vid else 0)+(4.0 if len(imgs2)>=6 else 0)))
        as_ = 0 if not has_ap else 7
        rs = 10 if (rating2>=4.4 and rev_cnt>=50) else (7 if rating2>=4.0 else 4)
        bsrs = 10 if bsr_num<=1000 else (8 if bsr_num<=5000 else 5)
        prs = 10 if is_prime else 5
        vs = 10 if (colors>=5 and sizes>=3) else (7 if sizes>=3 else 4)
        h = int((ts*0.10+bs*0.10+ds*0.10+ps*0.10+as_*0.10+rs*0.15+bsrs*0.15+7*0.10+vs*0.05+prs*0.05)*10)
        return {"title":round(ts,1),"bullets":round(bs,1),"description":round(ds,1),"photos":round(ps,1),"aplus":as_,"reviews":rs,"bsr":bsrs,"variants":vs,"prime":prs,"health":h}

    our_scores = {
        "title":       pct(r.get("title_score",0)),
        "bullets":     pct(r.get("bullets_score",0)),
        "description": pct(r.get("description_score", r.get("desc_score",0))),
        "photos":      pct(r.get("images_score", r.get("photos_score",0))),
        "aplus":       pct(r.get("aplus_score",0)),
        "reviews":     pct(r.get("reviews_score",0)),
        "bsr":         pct(r.get("bsr_score",0)),
        "variants":    pct(r.get("customization_score",0)),
        "prime":       pct(r.get("prime_score",0)),
        "health":      pct(r.get("overall_score", r.get("health_score",0))),
    }
    def get_comp_scores(c, i):
        """Use AI scores if available, else auto_score"""
        cai = st.session_state.get(f"comp_ai_{i}")
        if cai:
            return {
                "title":       pct(cai.get("title_score",0)),
                "bullets":     pct(cai.get("bullets_score",0)),
                "description": pct(cai.get("description_score",0)),
                "photos":      pct(cai.get("images_score",0)),
                "aplus":       pct(cai.get("aplus_score",0)),
                "reviews":     pct(cai.get("reviews_score",0)),
                "bsr":         pct(cai.get("bsr_score",0)),
                "variants":    pct(cai.get("customization_score",0)),
                "prime":       pct(cai.get("prime_score",0)),
                "health":      pct(cai.get("overall_score",0)),
            }
        return auto_score(c)

    comp_scores = [get_comp_scores(c, i) for i,c in enumerate(cd)]
    all_scores  = [our_scores] + comp_scores
    all_p       = [od] + cd

    # ── helper: render one product full card ─────────────────────────────────
    def render_product_card(d, sc, label, is_ours=False):
        dpi  = d.get("product_information", {})
        dasin   = get_asin_from_data(d)
        dtitle  = d.get("title","")
        dprice  = d.get("price","")
        drating = d.get("average_rating","")
        drev    = dpi.get("Customer Reviews",{}).get("ratings_count","")
        dbsr    = str(dpi.get("Best Sellers Rank",""))[:50]
        dbrand  = d.get("brand","")
        dbul    = d.get("feature_bullets",[])
        ddesc   = d.get("description","")
        dimgs   = d.get("images",[])
        daplus  = d.get("aplus",False)
        dvid    = int(d.get("number_of_videos",0) or 0)
        dprime  = d.get("is_prime_exclusive",False)
        dcolors = d.get("customization_options",{}).get("color",[])
        dsizes  = d.get("customization_options",{}).get("size",[])
        dmat    = dpi.get("Material Type","")
        tlen    = len(dtitle)
        h       = sc.get("health",0)
        hc      = "#22c55e" if h>=75 else ("#f59e0b" if h>=50 else "#ef4444")
        bg      = "linear-gradient(135deg,#1e293b,#334155)" if is_ours else "linear-gradient(135deg,#3b1e1e,#5c2626)"

        st.markdown(f"""
<div style="background:{bg};border-radius:14px;padding:18px;color:white;margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px">
    <div>
      <div style="font-size:0.78rem;opacity:0.6">{dbrand} - {dasin}</div>
      <div style="font-size:0.92rem;font-weight:600;max-width:480px;margin-top:3px">{dtitle[:75]}{"..." if tlen>75 else ""}</div>
      <div style="display:flex;gap:12px;margin-top:7px;font-size:0.8rem;opacity:0.8;flex-wrap:wrap">
        <span>💰 {dprice}</span><span>⭐ {drating} ({drev})</span>
        <span style="color:{'#fca5a5' if tlen>125 else '#86efac'}">{tlen} симв.</span>
      </div>
    </div>
    <div style="text-align:center">
      <div style="font-size:2.8rem;font-weight:800;color:{hc}">{h}%</div>
      <div style="font-size:0.75rem;color:{hc}">Health</div>
    </div>
  </div>
  <div style="background:rgba(255,255,255,0.12);border-radius:6px;height:8px;margin-top:10px">
    <div style="background:{hc};width:{h}%;height:8px;border-radius:6px"></div>
  </div>
</div>""", unsafe_allow_html=True)

        # Mini score cards
        sitems = [("Title",sc.get("title",0)),("Bullets",sc.get("bullets",0)),
                  ("Описание",sc.get("description",0)),("Фото",sc.get("photos",0)),
                  ("A+",sc.get("aplus",0)),("Отзывы",sc.get("reviews",0)),
                  ("BSR",sc.get("bsr",0)),("Варианты",sc.get("variants",0)),("Prime",sc.get("prime",0))]
        sc2 = st.columns(len(sitems))
        for col2,(lbl2,val2) in zip(sc2,sitems):
            p2 = int(val2/10*100); c2="#22c55e" if p2>=75 else ("#f59e0b" if p2>=50 else "#ef4444")
            col2.markdown(f'<div style="border-left:3px solid {c2};padding:5px 4px;text-align:center;background:#f8fafc;border-radius:4px"><div style="font-size:1.05rem;font-weight:700;color:{c2}">{p2}%</div><div style="font-size:0.62rem;color:#64748b">{lbl2}</div></div>', unsafe_allow_html=True)

        # Content tabs
        tab_cont, tab_photo, tab_data = st.tabs(["📝 Контент", "📸 Фото", "📊 Данные"])
        with tab_cont:
            tcc = "#ef4444" if tlen>125 else "#22c55e"
            st.markdown(f"**Title** — <span style='color:{tcc}'>{tlen} симв.</span>", unsafe_allow_html=True)
            st.markdown(f"> {dtitle}")
            st.divider()
            st.markdown(f"**Bullets** ({len(dbul)})")
            for b in dbul:
                blen = len(b.encode())
                st.markdown(f"{'🔴' if blen>255 else '✅'} {b}")
                st.caption(f"{blen} байт")
            if not dbul: st.caption("Нет буллетов")
            st.divider()
            st.markdown("**Описание**")
            if ddesc: st.markdown(str(ddesc)[:600])
            else: st.warning("Описание отсутствует")
            st.divider()
            st.markdown(f"**A+:** {'✅' if daplus else '❌'}  |  **Видео:** {'✅ '+str(dvid)+' шт.' if dvid else '❌'}")
        with tab_photo:
            if dimgs:
                for rs in range(0, min(len(dimgs),9), 3):
                    rc = st.columns(3)
                    for ci2,iu in enumerate(dimgs[rs:rs+3]):
                        try:
                            ri2 = requests.get(iu, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
                            if ri2.ok: rc[ci2].image(ri2.content, caption=f"#{rs+ci2+1}", use_container_width=True)
                        except: rc[ci2].caption(f"#{rs+ci2+1} ошибка")
                st.caption(f"Всего: {len(dimgs)} фото")
            else: st.warning("Нет фото")
        with tab_data:
            da1,da2 = st.columns(2)
            da1.metric("Цена", dprice); da2.metric("Рейтинг", drating)
            da1.metric("Отзывов", drev); da2.metric("BSR", dbsr[:30])
            da1.metric("Материал", dmat or "—"); da2.metric("Prime", "Да" if dprime else "Нет")
            da1.metric("Цветов", len(dcolors)); da2.metric("Размеров", len(dsizes))
            st.caption(f"Размеры: {[s.get('value','') for s in dsizes]}")
            st.caption(f"Цвета: {[c.get('value','') for c in dcolors]}")

    # ── Score comparison bars (always visible at top) ────────────────────────
    score_rows = [
        ("🏷️ Title","title",100),("📋 Bullets","bullets",100),
        ("📄 Описание","description",100),("📸 Фото","photos",100),
        ("✨ A+","aplus",100),("⭐ Отзывы","reviews",100),
        ("📊 BSR","bsr",100),("🎨 Варианты","variants",100),
        ("🚀 Prime","prime",100),("💯 Overall","health",100),
    ]
    asin_labels = ["🔵 НАШ"] + [f"🔴 {get_asin_from_data(c) or f'Конк.{i+1}'}" for i,c in enumerate(cd)]

    # ── Podium ───────────────────────────────────────────────────────────────
    total_scores = []
    for sc in all_scores:
        # Use pre-computed health/overall if available, else weighted average
        if sc.get("health", 0) > 0:
            total_scores.append(sc["health"])
        else:
            keys = ["title","bullets","description","photos","aplus","reviews","bsr","variants","prime"]
            w    = [0.10,0.10,0.10,0.10,0.10,0.15,0.15,0.05,0.05]
            total = sum(sc.get(k,0)*wi for k,wi in zip(keys,w))
            total_scores.append(round(total))
    ranked = sorted(enumerate(zip(asin_labels, total_scores)), key=lambda x: x[1][1], reverse=True)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]

    st.subheader("🏅 Итоговый рейтинг")
    pcols = st.columns(len(ranked))
    for rank,(orig_idx,(lbl,score)) in enumerate(ranked):
        medal = medals[rank] if rank < len(medals) else ""
        bg = "#fef9c3" if rank==0 else ("#f8fafc" if rank==1 else "#fff7ed")
        border = "#f59e0b" if rank==0 else ("#94a3b8" if rank==1 else "#fb923c")
        tag = "Лучший" if rank==0 else f"#{rank+1} место"
        pcols[rank].markdown(
            f'<div style="background:{bg};border:2px solid {border};border-radius:12px;padding:14px;text-align:center">'
            f'<div style="font-size:1.8rem">{medal}</div>'
            f'<div style="font-size:0.82rem;font-weight:700;margin-top:4px">{lbl}</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:{border};margin-top:4px">{score}%</div>'
            f'<div style="font-size:0.65rem;color:#64748b">{tag}</div>'
            f'</div>', unsafe_allow_html=True)

    st.divider()
    st.subheader("📊 Сравнение оценок")
    hdr2 = st.columns([2]+[3]*(1+len(cd)))
    hdr2[0].markdown("**Метрика**")
    for j,al in enumerate(asin_labels): hdr2[j+1].markdown(f"**{al}**")

    for lbl,key,mx in score_rows:
        vals = [s.get(key,0) for s in all_scores]
        best_val = max(vals)
        row2 = st.columns([2]+[3]*(1+len(cd)))
        row2[0].caption(lbl)
        for j,val in enumerate(vals):
            p3 = int(val); is_best = (val==best_val)
            cc = "#22c55e" if is_best else ("#f59e0b" if p3>=50 else "#ef4444")
            row2[j+1].markdown(
                f'<div style="background:#e5e7eb;border-radius:5px;height:22px;position:relative">'
                f'<div style="background:{cc};width:{p3}%;height:22px;border-radius:5px"></div>'
                f'<div style="position:absolute;top:2px;left:6px;font-size:0.75rem;font-weight:700;color:white">{p3}%{"★" if is_best else ""}</div>'
                f'</div>', unsafe_allow_html=True)




# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COSMO / Rufus
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🧠 COSMO / Rufus":
    st.title("🧠 COSMO / Rufus Анализ")

    _ca   = r.get("cosmo_analysis", {})
    cosmo = pct(_ca.get("score", r.get("cosmo_score",0)))
    _ra   = r.get("rufus_analysis", {})
    rufus_s = pct(_ra.get("score", 0))
    cc    = "#22c55e" if cosmo>=75 else ("#f59e0b" if cosmo>=50 else "#ef4444")
    rc2   = "#22c55e" if rufus_s>=75 else ("#f59e0b" if rufus_s>=50 else "#ef4444")
    ccc1,ccc2 = st.columns(2)
    with ccc1:
        st.markdown(f'<div style="text-align:center;padding:16px;background:#f8fafc;border-radius:12px"><div style="font-size:2.5rem;font-weight:800;color:{cc}">{cosmo}%</div><div style="color:{cc};font-weight:600">COSMO Score</div><div style="background:#e5e7eb;border-radius:6px;height:10px;margin-top:8px"><div style="background:{cc};width:{cosmo}%;height:10px;border-radius:6px"></div></div></div>', unsafe_allow_html=True)
    with ccc2:
        st.markdown(f'<div style="text-align:center;padding:16px;background:#f8fafc;border-radius:12px"><div style="font-size:2.5rem;font-weight:800;color:{rc2}">{rufus_s}%</div><div style="color:{rc2};font-weight:600">Rufus Score</div><div style="background:#e5e7eb;border-radius:6px;height:10px;margin-top:8px"><div style="background:{rc2};width:{rufus_s}%;height:10px;border-radius:6px"></div></div></div>', unsafe_allow_html=True)
    st.divider()

    # New format: cosmo_analysis.signals_present/missing
    if _ca:
        c_present = _ca.get("signals_present",[])
        c_missing = _ca.get("signals_missing",[])
        if c_present or c_missing:
            st.subheader("📡 COSMO сигналы")
            col_p, col_m = st.columns(2)
            with col_p:
                st.markdown("**✅ Присутствуют**")
                for s2 in c_present:
                    st.success(s2)
            with col_m:
                st.markdown("**❌ Отсутствуют**")
                for s2 in c_missing:
                    st.error(s2)
    # Old format fallback
    elif r.get("cosmo_semantic"):
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
# PAGE: Конкурент N (individual pages)
# ══════════════════════════════════════════════════════════════════════════════
elif _is_competitor_page:
    idx_m = re.search(r"Конкурент (\d+)", page)
    cidx = int(idx_m.group(1)) - 1 if idx_m else 0
    c = cd[cidx] if cidx < len(cd) else {}

    if not c:
        st.warning("Данные конкурента не найдены"); st.stop()

    # Use render_product_card defined in benchmark — redefine inline here
    cpi   = c.get("product_information", {})
    casin = get_asin_from_data(c)
    csc_page = {}
    # auto_score inline
    _t2 = c.get("title",""); _i2 = c.get("images",[]); _b2 = c.get("feature_bullets",[])
    _d2 = c.get("description",""); _rat2 = float(c.get("average_rating",0) or 0)
    _rev2 = int(cpi.get("Customer Reviews",{}).get("ratings_count","0") or 0)
    _vid2 = int(c.get("number_of_videos",0) or 0)>0; _ap2 = bool(c.get("aplus"))
    _pr2  = bool(c.get("is_prime_exclusive"))
    _bsr2 = 99999
    _bm = re.search(r"#([\d,]+)", str(cpi.get("Best Sellers Rank","")))
    if _bm:
        try: _bsr2 = int(_bm.group(1).replace(",",""))
        except: pass
    _col2 = len(c.get("customization_options",{}).get("color",[])); _sz2 = len(c.get("customization_options",{}).get("size",[]))
    _ts = min(10, max(0, (1.5 if len(_t2)<=125 else 0)+(3.5 if any(k in _t2.lower() for k in ["merino","wool","tank","shirt","base layer"]) else 1.5)+3+(1 if not re.search(r"[!$?{}]",_t2) else 0)+1))
    _bs = min(10, max(0, (1.5 if len(_b2)<=5 else 0)+(2.5 if any(":" in b for b in _b2) else 1)+min(4,len(_b2))+1+1))
    _ds = 0 if not _d2 else min(10, 4+(3 if len(_d2)>200 else 1))
    _ps = min(10, max(0, (4 if len(_i2)>=6 else len(_i2)*0.6)+(2 if _vid2 else 0)+(4 if len(_i2)>=6 else 0)))
    _as = 0 if not _ap2 else 7
    _rs = 10 if (_rat2>=4.4 and _rev2>=50) else (7 if _rat2>=4.0 else 4)
    _bsrs = 10 if _bsr2<=1000 else (8 if _bsr2<=5000 else 5)
    _prs = 10 if _pr2 else 5
    _vs = 10 if (_col2>=5 and _sz2>=3) else (7 if _sz2>=3 else 4)
    _h = int((_ts*0.10+_bs*0.10+_ds*0.10+_ps*0.10+_as*0.10+_rs*0.15+_bsrs*0.15+7*0.10+_vs*0.05+_prs*0.05)*10)
    csc_page = {"title":round(_ts,1),"bullets":round(_bs,1),"description":round(_ds,1),"photos":round(_ps,1),"aplus":_as,"reviews":_rs,"bsr":_bsrs,"variants":_vs,"prime":_prs,"health":_h}

    ch = csc_page["health"]; hc = "#22c55e" if ch>=75 else ("#f59e0b" if ch>=50 else "#ef4444")
    tlen = len(_t2); cprice = c.get("price",""); cbrand = c.get("brand","")
    crating = c.get("average_rating",""); crev = cpi.get("Customer Reviews",{}).get("ratings_count","")
    cbsr_s = str(cpi.get("Best Sellers Rank",""))[:50]

    st.title(f"🔴 Конкурент {cidx+1}")

    # Health card
    st.markdown(f"""
<div style="background:linear-gradient(135deg,#3b1e1e,#5c2626);border-radius:14px;padding:18px;color:white;margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
    <div>
      <div style="font-size:0.78rem;opacity:0.6">{cbrand} - {casin}</div>
      <div style="font-size:0.95rem;font-weight:600;max-width:500px;margin-top:3px">{_t2[:80]}{"..." if tlen>80 else ""}</div>
      <div style="display:flex;gap:12px;margin-top:7px;font-size:0.8rem;opacity:0.8;flex-wrap:wrap">
        <span>Price: {cprice}</span>
        <span>Rating: {crating} ({crev} reviews)</span>
        <span style="color:{'#fca5a5' if tlen>125 else '#86efac'}">{tlen} chars</span>
      </div>
    </div>
    <div style="text-align:center">
      <div style="font-size:2.8rem;font-weight:800;color:{hc}">{ch}%</div>
      <div style="font-size:0.75rem;color:{hc}">Health Score</div>
    </div>
  </div>
  <div style="background:rgba(255,255,255,0.12);border-radius:6px;height:8px;margin-top:10px">
    <div style="background:{hc};width:{ch}%;height:8px;border-radius:6px"></div>
  </div>
</div>""", unsafe_allow_html=True)

    # ── AI Analysis button for competitor ───────────────────────────────────
    _cai_key    = f"comp_ai_{cidx}"
    _vision_key = f"comp_vision_{cidx}"
    _cai_result = st.session_state.get(_cai_key)

    if not _cai_result:
        _cbtn1, _cbtn2 = st.columns([3,1])
        with _cbtn1:
            st.info("💡 Нажми Анализ — или перезапусти главный анализ с URL конкурента")
        with _cbtn2:
            if st.button("🧠 Анализ", key=f"ai_btn_{cidx}", type="primary"):
                _clang = st.session_state.get("analysis_lang","ru")
                _cimgs_urls = c.get("images",[])
                _prog = st.progress(0, text="⬇️ Загружаю фото...")
                # Step 1: download images
                _cimgs_dl = download_images(_cimgs_urls[:5], lambda m: None) if _cimgs_urls else []
                _prog.progress(33, text="👁️ Vision анализ фото...")
                # Step 2: vision
                _comp_vision = analyze_vision(_cimgs_dl, c, casin, lambda m: None, lang=_clang) if _cimgs_dl else ""
                _prog.progress(66, text="🧠 AI анализ текста...")
                # Step 3: text analysis
                _cai_result = analyze_text(c, [], _comp_vision, casin, lambda m: None, lang=_clang)
                # Save both
                st.session_state[_cai_key]    = _cai_result
                st.session_state[_vision_key] = (_cimgs_dl, _comp_vision) if _cimgs_dl else None
                _prog.progress(100, text="✅ Готово!")
                st.rerun()

    # Score mini-cards — use AI scores if available, else auto-score
    if _cai_result:
        _sitems = [
            ("Title",   pct(_cai_result.get("title_score",0))),
            ("Bullets", pct(_cai_result.get("bullets_score",0))),
            ("Описание",pct(_cai_result.get("description_score",0))),
            ("Фото",    pct(_cai_result.get("images_score",0))),
            ("A+",      pct(_cai_result.get("aplus_score",0))),
            ("Отзывы",  pct(_cai_result.get("reviews_score",0))),
            ("BSR",     pct(_cai_result.get("bsr_score",0))),
            ("Варианты",pct(_cai_result.get("customization_score",0))),
            ("Prime",   pct(_cai_result.get("prime_score",0))),
        ]
        _overall = pct(_cai_result.get("overall_score",0))
        _ohc = "#22c55e" if _overall>=75 else ("#f59e0b" if _overall>=50 else "#ef4444")
        st.markdown(f"**🧠 AI Overall: <span style='color:{_ohc};font-size:1.3rem'>{_overall}%</span>**", unsafe_allow_html=True)
    else:
        _sitems = [("Title",_ts*10),("Bullets",_bs*10),("Описание",_ds*10),("Фото",_ps*10),
                   ("A+",_as*10),("Отзывы",_rs*10),("BSR",_bsrs*10),("Варианты",_vs*10),("Prime",_prs*10)]
        st.caption("📊 Авто-оценка (формула) — нажми AI Анализ для точных данных")

    _sc2 = st.columns(len(_sitems))
    for _col3,(_lbl3,_p3) in zip(_sc2,_sitems):
        _c3 = "#22c55e" if _p3>=75 else ("#f59e0b" if _p3>=50 else "#ef4444")
        _col3.markdown(f'<div style="border-left:3px solid {_c3};padding:5px 4px;text-align:center;background:#f8fafc;border-radius:4px"><div style="font-size:1.05rem;font-weight:700;color:{_c3}">{_p3}%</div><div style="font-size:0.62rem;color:#64748b">{_lbl3}</div></div>', unsafe_allow_html=True)

    st.divider()

    # Content
    tab_cont, tab_photo, tab_data = st.tabs(["📝 Контент", "📸 Фото", "📊 Данные"])
    with tab_cont:
        _tcc = "#ef4444" if tlen>125 else "#22c55e"
        st.markdown(f"**Title** — <span style='color:{_tcc}'>{tlen} симв.</span>", unsafe_allow_html=True)
        st.markdown(f"> {_t2}")
        if _cai_result:
            _ts_ai = pct(_cai_result.get("title_score",0))
            _tc2 = "#22c55e" if _ts_ai>=75 else ("#f59e0b" if _ts_ai>=50 else "#ef4444")
            st.markdown(f"🧠 AI оценка: <span style='color:{_tc2};font-weight:700'>{_ts_ai}%</span>", unsafe_allow_html=True)
            _tgi = _cai_result.get("priority_improvements",[])[:2]
            for _tg in _tgi: st.caption(f"💡 {_tg}")
        st.divider()
        st.markdown(f"**Bullets** ({len(_b2)})")
        for _bul in _b2:
            _blen = len(_bul.encode())
            st.markdown(f"{'🔴' if _blen>255 else '✅'} {_bul}")
            st.caption(f"{_blen} байт")
        if not _b2: st.caption("Нет буллетов")
        if _cai_result:
            _bs_ai = pct(_cai_result.get("bullets_score",0))
            _bc2 = "#22c55e" if _bs_ai>=75 else ("#f59e0b" if _bs_ai>=50 else "#ef4444")
            st.markdown(f"🧠 Bullets AI: <span style='color:{_bc2};font-weight:700'>{_bs_ai}%</span>", unsafe_allow_html=True)
        st.divider()
        st.markdown("**Описание**")
        if _d2: st.markdown(str(_d2)[:600])
        else: st.warning("Описание отсутствует")
        if _cai_result:
            _ds_ai = pct(_cai_result.get("description_score",0))
            _dc2 = "#22c55e" if _ds_ai>=75 else ("#f59e0b" if _ds_ai>=50 else "#ef4444")
            st.markdown(f"🧠 Описание AI: <span style='color:{_dc2};font-weight:700'>{_ds_ai}%</span>", unsafe_allow_html=True)
        st.divider()
        st.markdown(f"**A+:** {'✅' if _ap2 else '❌'}  |  **Видео:** {'✅ '+str(int(c.get('number_of_videos',0) or 0))+' шт.' if _vid2 else '❌'}")
        if _cai_result:
            _as_ai = pct(_cai_result.get("aplus_score",0))
            _ac2 = "#22c55e" if _as_ai>=75 else ("#f59e0b" if _as_ai>=50 else "#ef4444")
            st.markdown(f"🧠 A+ AI: <span style='color:{_ac2};font-weight:700'>{_as_ai}%</span>", unsafe_allow_html=True)
    with tab_photo:
        _cimgs = c.get("images",[])
        if _cimgs:
            # Vision results come from the combined AI Анализ button above
            if _vision_key in st.session_state and st.session_state[_vision_key]:
                _cv_imgs, _cv_text = st.session_state[_vision_key]
                _cv_blocks = re.split(r"PHOTO_BLOCK_\d+", _cv_text)
                _cv_blocks = [b.strip() for b in _cv_blocks if b.strip()]
                for _pi3, _pimg in enumerate(_cv_imgs):
                    _ptext = _cv_blocks[_pi3] if _pi3 < len(_cv_blocks) else ""
                    _psm = re.search(r"(\d+)/10", _ptext)
                    _pscore = int(_psm.group(1)) if _psm else 0
                    _pbc = "#22c55e" if _pscore>=8 else ("#f59e0b" if _pscore>=6 else "#ef4444")
                    _pslbl = "Отлично" if _pscore>=8 else ("Хорошо" if _pscore>=6 else "Слабо")
                    _ptyp = re.search(r"(?:[Тт]ип|Type):\s*(.+)", _ptext)
                    _pstrg = re.search(r"(?:[Сс]ильная сторона|Strength):\s*(.+)", _ptext)
                    _pweak = re.search(r"(?:[Сс]лабость|Weakness):\s*(.+)", _ptext)
                    with st.container(border=True):
                        _pc1,_pc2 = st.columns([1,2])
                        with _pc1:
                            st.image(__import__("base64").b64decode(_pimg["b64"]), use_container_width=True)
                        with _pc2:
                            st.markdown(f"**Фото #{_pi3+1} — {_ptyp.group(1).strip() if _ptyp else ''}**")
                            st.markdown(
                                f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0">'
                                f'<div style="font-size:2rem;font-weight:800;color:{_pbc}">{_pscore}/10</div>'
                                f'<div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px">'
                                f'<div style="background:{_pbc};width:{_pscore*10}%;height:10px;border-radius:6px"></div>'
                                f'</div><div style="color:{_pbc};font-size:0.8rem;margin-top:2px">{_pslbl}</div></div></div>',
                                unsafe_allow_html=True)
                            if _pstrg: st.success(f"✅ {_pstrg.group(1).strip()}")
                            if _pweak: st.warning(f"⚠️ {_pweak.group(1).strip()}")
            else:
                # Show photos without analysis
                for _rs in range(0, min(len(_cimgs),9), 3):
                    _rc = st.columns(3)
                    for _ci2,_iu in enumerate(_cimgs[_rs:_rs+3]):
                        try:
                            _ri2 = requests.get(_iu, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
                            if _ri2.ok: _rc[_ci2].image(_ri2.content, caption=f"#{_rs+_ci2+1}", use_container_width=True)
                        except: _rc[_ci2].caption(f"#{_rs+_ci2+1} ошибка")
            st.caption(f"Всего: {len(_cimgs)} фото")
        else: st.warning("Нет фото")
    with tab_data:
        _da1,_da2 = st.columns(2)
        _da1.metric("Цена", cprice); _da2.metric("Рейтинг", crating)
        _da1.metric("Отзывов", crev); _da2.metric("BSR", cbsr_s[:30])
        _da1.metric("Материал", cpi.get("Material Type","") or "—"); _da2.metric("Prime", "Да" if _pr2 else "Нет")
        _ccolors = c.get("customization_options",{}).get("color",[])
        _csizes  = c.get("customization_options",{}).get("size",[])
        _da1.metric("Цветов", len(_ccolors)); _da2.metric("Размеров", len(_csizes))
        st.caption(f"Размеры: {[s.get('value','') for s in _csizes]}")
        st.caption(f"Цвета: {[s.get('value','') for s in _ccolors]}")
