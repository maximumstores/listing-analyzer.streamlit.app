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

SCHEMA = '{"health_score":72,"health_breakdown":{"title":8,"bullets":8,"description":7,"photos":7,"aplus":6,"reviews":9,"bsr":8,"price":7,"variants":8,"prime":9},"summary":"Листинг в целом хорошо оптимизирован, но требует доработки описания и A+","title_score":7,"title_gaps":["Не указан вес ткани (GSM)","Отсутствует слово outdoor"],"title_rec":"Добавить GSM ткани и сценарий использования в заголовок","bullets_score":7,"bullets_gaps":["Нет информации об уходе за изделием","Не указан процент шерсти"],"bullets_rec":"Добавить буллет с инструкцией по стирке и составом 100% merino","desc_score":0,"desc_gaps":["Описание полностью отсутствует"],"desc_rec":"Создать описание с заголовками, сценариями и ответами на возражения","photos_score":8,"photos_gaps":["Нет lifestyle фото на улице"],"photos_rec":"Добавить фото в природных условиях для подчёркивания outdoor применения","aplus_score":7,"aplus_gaps":["Brand Story отсутствует","Текст модулей превышает 80 символов"],"aplus_rec":"Добавить Brand Story и сократить текст в модулях","cosmo_score":65,"cosmo_semantic":[{"relationship":"Used For (Function)","status":"WELL-DEVELOPED","evidence":"Title и буллеты упоминают базовый слой и спорт","opportunity":"Добавить конкретные виды спорта: hiking, skiing"},{"relationship":"Used For (Situation)","status":"GOOD","evidence":"Упоминается travel и everyday wear","opportunity":"Расширить на деловую одежду"},{"relationship":"Target Audience","status":"ADEQUATE","evidence":"Мужчины, спортсмены","opportunity":"Уточнить возраст и активность"},{"relationship":"Solves Problem","status":"GOOD","evidence":"Терморегуляция и влагоотвод","opportunity":"Добавить проблему запаха"},{"relationship":"Compared To (Alternative)","status":"PARTIAL","evidence":"Упоминается шерсть vs синтетика","opportunity":"Прямое сравнение с хлопком"},{"relationship":"Used In (Location)","status":"MINIMAL","evidence":"Нет конкретных локаций","opportunity":"Горы, офис, путешествия"},{"relationship":"Used With (Complementary)","status":"MINIMAL","evidence":"Не упомянуто","opportunity":"Упомянуть hiking pants, fleece jacket"}],"rufus_answered":[{"question":"Из какого материала сделана майка?","answer":"100% мериносовая шерсть, указано в title и буллетах"}],"rufus_partial":[{"question":"Подходит ли для холодной погоды?","gap":"Упоминается терморегуляция, но не указана температура комфорта"}],"rufus_missing":[{"question":"Как стирать изделие?","missing":"Инструкция по уходу полностью отсутствует в контенте"}],"missing_chars":[{"name":"Инструкция по уходу","how_competitors_use":"Конкуренты указывают машинная стирка при 30°C","priority":"HIGH"},{"name":"Вес ткани (GSM)","how_competitors_use":"Конкуренты указывают 160-200 GSM в title","priority":"HIGH"}],"tech_params":[{"param":"Вес ткани","competitor_value":"160-200 GSM у топ-конкурентов","our_gap":"GSM не указан нигде в листинге"},{"param":"Состав","competitor_value":"Конкуренты пишут 100% Merino Wool в буллете #1","our_gap":"Состав упомянут только в title"}],"actions":[{"action":"Добавить описание товара","impact":"HIGH","effort":"LOW","details":"Написать 300-500 слов с заголовками, сценариями и FAQ"},{"action":"Добавить GSM в title","impact":"HIGH","effort":"LOW","details":"Изменить title: добавить 160GSM после Merino Wool"}]}'

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

КРИТИЧЕСКИ ВАЖНО: Верни ТОЛЬКО валидный JSON. 
- Все строковые поля ДОЛЖНЫ содержать реальный текст на русском — анализ реального листинга выше.
- ЗАПРЕЩЕНО копировать значения из примера схемы ("gap1", "rec", "p", "v", "char", "use", "act", "det", "q").
- Каждый gap — конкретная проблема данного листинга.
- Каждый rec — конкретная рекомендация с примером текста.
- tech_params.param — реальное название характеристики (например "Вес ткани GSM").
- tech_params.competitor_value — реальное значение у конкурента из данных выше.
- missing_chars.name — реальная отсутствующая характеристика.
- actions.action — конкретное действие, не абстрактное.
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
            _casin = _c.get("parent_asin","") or _cpi.get("ASIN","")
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
                _casin2 = _c2.get("parent_asin","") or _cpi2.get("ASIN","")
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
    c1, c2, c3 = st.columns(3)
    comp1 = c1.text_input("Конкурент 1", key="c0", value=st.session_state.get("c0_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp2 = c2.text_input("Конкурент 2", key="c1", value=st.session_state.get("c1_saved",""), placeholder="https://www.amazon.com/dp/...")
    comp3 = c3.text_input("Конкурент 3", key="c2", value=st.session_state.get("c2_saved",""), placeholder="https://www.amazon.com/dp/...")
    competitor_urls = [comp1, comp2, comp3]

    # Detect what changed
    prev_url  = st.session_state.get("our_url_saved","")
    prev_comp = [st.session_state.get(f"c{i}_saved","") for i in range(3)]
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

    if st.button(btn_label, type="primary", disabled=not our_url.strip()):
        lines = []; ph = st.empty()
        def log(msg):
            lines.append(msg); ph.markdown("\n\n".join(lines[-8:]))

        with st.spinner("Анализирую..."):
            try:
                # SMART: only re-run full analysis if our URL changed or first run
                if not already_done or our_changed:
                    result, vision = run_analysis(our_url, competitor_urls, log)
                    st.session_state.update({"result": result, "vision": vision})
                    st.session_state["our_url_saved"] = our_url
                    st.session_state["c0_saved"] = comp1
                    st.session_state["c1_saved"] = comp2
                    st.session_state["c2_saved"] = comp3
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
                    result = analyze_text(od_s, existing, v_s, asin_s, log)
                    st.session_state["result"] = result

                st.success("✅ Готово!")
                st.rerun()
            except Exception as e:
                st.error(f"Ошибка: {e}")

# ── Pages ─────────────────────────────────────────────────────────────────────
page = st.session_state.get("page", "🏠 Обзор")
_is_competitor_page = page.startswith("🔴 Конкурент")

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
    all_p       = [od] + cd

    # ── helper: render one product full card ─────────────────────────────────
    def render_product_card(d, sc, label, is_ours=False):
        dpi  = d.get("product_information", {})
        dasin   = d.get("parent_asin","") or dpi.get("ASIN","")
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
        ("🏷️ Title","title",10),("📋 Bullets","bullets",10),
        ("📄 Описание","description",10),("📸 Фото","photos",10),
        ("✨ A+","aplus",10),("⭐ Отзывы","reviews",10),
        ("📊 BSR","bsr",10),("🎨 Варианты","variants",10),
        ("🚀 Prime","prime",10),("💯 Health","health",100),
    ]
    asin_labels = ["🔵 НАШ"] + [f"🔴 {c.get('parent_asin','') or c.get('product_information',{}).get('ASIN',f'Конк.{i+1}')}" for i,c in enumerate(cd)]

    # ── Podium ───────────────────────────────────────────────────────────────
    total_scores = []
    for sc in all_scores:
        keys = ["title","bullets","description","photos","aplus","reviews","bsr","variants","prime"]
        w    = [0.10,0.10,0.10,0.10,0.10,0.15,0.15,0.05,0.05]
        total = sum(sc.get(k,0)*wi for k,wi in zip(keys,w)) * 10
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
            pct = int(val/mx*100); is_best = (val==best_val)
            cc = "#22c55e" if is_best else ("#f59e0b" if pct>=50 else "#ef4444")
            row2[j+1].markdown(
                f'<div style="background:#e5e7eb;border-radius:5px;height:22px;position:relative">'
                f'<div style="background:{cc};width:{pct}%;height:22px;border-radius:5px"></div>'
                f'<div style="position:absolute;top:2px;left:6px;font-size:0.75rem;font-weight:700;color:white">{val}{"★" if is_best else ""}</div>'
                f'</div>', unsafe_allow_html=True)




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
    casin = c.get("parent_asin","") or cpi.get("ASIN","")
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

    # Score mini-cards
    _sitems = [("Title",_ts),("Bullets",_bs),("Описание",_ds),("Фото",_ps),
               ("A+",_as),("Отзывы",_rs),("BSR",_bsrs),("Варианты",_vs),("Prime",_prs)]
    _sc2 = st.columns(len(_sitems))
    for _col3,(_lbl3,_val3) in zip(_sc2,_sitems):
        _p3 = int(_val3/10*100); _c3 = "#22c55e" if _p3>=75 else ("#f59e0b" if _p3>=50 else "#ef4444")
        _col3.markdown(f'<div style="border-left:3px solid {_c3};padding:5px 4px;text-align:center;background:#f8fafc;border-radius:4px"><div style="font-size:1.05rem;font-weight:700;color:{_c3}">{_p3}%</div><div style="font-size:0.62rem;color:#64748b">{_lbl3}</div></div>', unsafe_allow_html=True)

    st.divider()

    # Content
    tab_cont, tab_photo, tab_data = st.tabs(["📝 Контент", "📸 Фото", "📊 Данные"])
    with tab_cont:
        _tcc = "#ef4444" if tlen>125 else "#22c55e"
        st.markdown(f"**Title** — <span style='color:{_tcc}'>{tlen} симв.</span>", unsafe_allow_html=True)
        st.markdown(f"> {_t2}")
        st.divider()
        st.markdown(f"**Bullets** ({len(_b2)})")
        for _bul in _b2:
            _blen = len(_bul.encode())
            st.markdown(f"{'🔴' if _blen>255 else '✅'} {_bul}")
            st.caption(f"{_blen} байт")
        if not _b2: st.caption("Нет буллетов")
        st.divider()
        st.markdown("**Описание**")
        if _d2: st.markdown(str(_d2)[:600])
        else: st.warning("Описание отсутствует")
        st.divider()
        st.markdown(f"**A+:** {'✅' if _ap2 else '❌'}  |  **Видео:** {'✅ '+str(int(c.get('number_of_videos',0) or 0))+' шт.' if _vid2 else '❌'}")
    with tab_photo:
        _cimgs = c.get("images",[])
        if _cimgs:
            # Cache key for this competitor's vision
            _vision_key = f"comp_vision_{cidx}"
            if _vision_key not in st.session_state:
                if st.button("🔍 Запустить Vision анализ фото", key=f"vision_btn_{cidx}"):
                    with st.spinner("Анализирую фото конкурента..."):
                        _comp_imgs_dl = download_images(_cimgs[:5], lambda m: None)
                        if _comp_imgs_dl:
                            _comp_vision = analyze_vision(_comp_imgs_dl, c, casin, lambda m: None)
                            st.session_state[_vision_key] = (_comp_imgs_dl, _comp_vision)
                            st.rerun()
            if _vision_key in st.session_state:
                _cv_imgs, _cv_text = st.session_state[_vision_key]
                _cv_blocks = re.split(r"PHOTO_BLOCK_\d+", _cv_text)
                _cv_blocks = [b.strip() for b in _cv_blocks if b.strip()]
                for _pi3, _pimg in enumerate(_cv_imgs):
                    _ptext = _cv_blocks[_pi3] if _pi3 < len(_cv_blocks) else ""
                    _psm = re.search(r"(\d+)/10", _ptext)
                    _pscore = int(_psm.group(1)) if _psm else 0
                    _pbc = "#22c55e" if _pscore>=8 else ("#f59e0b" if _pscore>=6 else "#ef4444")
                    _pslbl = "Отлично" if _pscore>=8 else ("Хорошо" if _pscore>=6 else "Слабо")
                    _ptyp = re.search(r"[Тт]ип:\s*(.+)", _ptext)
                    _pstrg = re.search(r"[Сс]ильная сторона:\s*(.+)", _ptext)
                    _pweak = re.search(r"[Сс]лабость:\s*(.+)", _ptext)
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
