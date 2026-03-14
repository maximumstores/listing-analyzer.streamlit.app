# Amazon Listing Analyzer v2 — MR.EQUIPP
import json, re, base64, requests, streamlit as st
from PIL import Image
import io
from datetime import datetime

# ── PostgreSQL history ─────────────────────────────────────────────────────────
def get_db():
    db_url = st.secrets.get("DATABASE_URL","")
    if not db_url:
        st.session_state["_db_err"] = "DATABASE_URL не найден в secrets"
        return None
    try:
        import psycopg2
        conn = psycopg2.connect(db_url, sslmode="require")
        return conn
    except ImportError as e:
        st.session_state["_db_err"] = f"psycopg2 не установлен: {e}"
        return None
    except Exception as _e:
        st.session_state["_db_err"] = f"Ошибка подключения: {_e}"
        return None

def db_init():
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS listing_analysis (
                id SERIAL PRIMARY KEY,
                asin TEXT NOT NULL,
                listing_type TEXT DEFAULT 'наш',
                analyzed_at TIMESTAMP DEFAULT NOW(),
                overall_score INT,
                title_score INT,
                bullets_score INT,
                images_score INT,
                aplus_score INT,
                cosmo_score INT,
                rufus_score INT,
                result_json TEXT,
                vision_text TEXT,
                our_title TEXT,
                competitors_json TEXT,
                workflow_status TEXT DEFAULT 'new_audit',
                workflow_note TEXT,
                workflow_updated_at TIMESTAMP
            )
        """)
        conn.commit()
        for _col, _def in [
            ("listing_type", "TEXT DEFAULT 'наш'"),
            ("workflow_status", "TEXT DEFAULT 'new_audit'"),
            ("workflow_note", "TEXT"),
            ("workflow_updated_at", "TIMESTAMP"),
        ]:
            try:
                cur.execute(f"ALTER TABLE listing_analysis ADD COLUMN IF NOT EXISTS {_col} {_def}")
                conn.commit()
            except Exception:
                pass
        conn.close()
    except Exception:
        pass

def db_save(asin, result, vision_text, our_title):
    conn = get_db()
    if not conn: return False
    try:
        cosmo = pct(result.get("cosmo_analysis",{}).get("score",0)) if isinstance(result.get("cosmo_analysis"),dict) else 0
        rufus = pct(result.get("rufus_analysis",{}).get("score",0)) if isinstance(result.get("rufus_analysis"),dict) else 0
        comp_list = st.session_state.get("comp_data_list", [])
        comp_snap = []
        for _i, _cd in enumerate(comp_list):
            if not _cd: continue
            _cai = st.session_state.get(f"comp_ai_{_i}", {})
            comp_snap.append({
                "asin": get_asin_from_data(_cd),
                "title": _cd.get("title","")[:80],
                "overall": pct(_cai.get("overall_score",0)) if _cai else 0,
                "price": _cd.get("price",""),
                "rating": _cd.get("average_rating",""),
                "reviews": _cd.get("reviews_count",""),
            })
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO listing_analysis
              (asin, overall_score, title_score, bullets_score, images_score,
               aplus_score, cosmo_score, rufus_score, result_json, vision_text, our_title, competitors_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (asin,
              pct(result.get("overall_score",0)),
              pct(result.get("title_score",0)),
              pct(result.get("bullets_score",0)),
              pct(result.get("images_score",0)),
              pct(result.get("aplus_score",0)),
              cosmo, rufus,
              json.dumps(result, ensure_ascii=False),
              vision_text or "",
              our_title or "",
              json.dumps(comp_snap, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return True
    except Exception as _e:
        st.session_state["_db_save_err"] = str(_e)
        return False

WORKFLOW_STATUSES = [
    ("🆕", "new_audit",      "Новый аудит"),
    ("✏️", "needs_rewrite",  "Нужен рерайт"),
    ("🎨", "ready_designer", "К дизайнеру"),
    ("📋", "ready_update",   "К загрузке"),
    ("🔁", "recheck",        "Перепроверить"),
    ("✅", "done",           "Готово"),
]

def workflow_label(status):
    for icon, key, label in WORKFLOW_STATUSES:
        if key == status: return f"{icon} {label}"
    return "🆕 Новый аудит"

def workflow_icon(status):
    for icon, key, label in WORKFLOW_STATUSES:
        if key == status: return icon
    return "🆕"

def db_update_workflow(record_id, status, note=""):
    try:
        conn = get_db()
        if not conn: return False
        cur = conn.cursor()
        cur.execute("UPDATE listing_analysis SET workflow_status=%s, workflow_note=%s, workflow_updated_at=NOW() WHERE id=%s",
                    (status, note, record_id))
        conn.commit(); conn.close()
        return True
    except Exception:
        return False

def db_workflow_board():
    try:
        conn = get_db()
        if not conn: return []
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (asin) id, asin, our_title, overall_score,
                   workflow_status, workflow_note, workflow_updated_at, analyzed_at
            FROM listing_analysis WHERE listing_type='наш'
            ORDER BY asin, analyzed_at DESC
        """)
        rows = cur.fetchall(); conn.close()
        return [{"id":r[0],"asin":r[1],"title":r[2],"score":r[3],
                 "status":r[4] or "new_audit","note":r[5] or "","updated":r[6],"analyzed":r[7]}
                for r in rows]
    except Exception:
        return []

def db_history(asin, limit=10):
    conn = get_db()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT analyzed_at, overall_score, title_score, bullets_score,
                   images_score, aplus_score, cosmo_score, rufus_score, our_title
            FROM listing_analysis
            WHERE asin = %s
            ORDER BY analyzed_at DESC
            LIMIT %s
        """, (asin, limit))
        rows = cur.fetchall()
        conn.close()
        return [{"date": r[0], "overall": r[1], "title": r[2], "bullets": r[3],
                 "images": r[4], "aplus": r[5], "cosmo": r[6], "rufus": r[7], "our_title": r[8]}
                for r in rows]
    except Exception:
        return []

def db_all_asins():
    conn = get_db()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (asin) asin, our_title, overall_score, analyzed_at, listing_type
            FROM listing_analysis
            ORDER BY asin, analyzed_at DESC
        """)
        rows = cur.fetchall()
        conn.close()
        return [{"asin": r[0], "title": r[1], "score": r[2], "date": r[3], "type": r[4]} for r in rows]
    except Exception:
        return []

ANTHROPIC_URL          = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL        = "claude-sonnet-4-5-20250929"
ANTHROPIC_MODEL_VISION = "claude-sonnet-4-5-20250929"

SCHEMA = '{"overall_score":"XX%","title_score":"XX%","bullets_score":"XX%","description_score":"XX%","images_score":"XX%","qa_score":"XX%","reviews_score":"XX%","aplus_score":"XX%","price_score":"XX%","availability_score":"XX%","average_rating_score":"XX%","total_reviews_score":"XX%","bsr_score":"XX%","keywords_score":"XX%","prime_score":"XX%","returns_score":"XX%","customization_score":"XX%","first_available_score":"XX%","title_gaps":["specific title issue"],"title_rec":"specific title recommendation","bullets_gaps":["specific bullets issue"],"bullets_rec":"specific bullets recommendation","description_gaps":["specific description issue"],"description_rec":"specific description recommendation","aplus_gaps":["specific A+ issue"],"aplus_rec":"specific A+ recommendation","images_gaps":["specific images issue"],"images_rec":"specific images recommendation","images_breakdown":{"main_image":"XX% - reason","gallery":"XX% - reason","ocr_readability":"XX% - reason"},"cosmo_analysis":{"score":"XX%","signals_present":["signal with evidence"],"signals_missing":["missing signal"]},"rufus_analysis":{"score":"XX%","issues":["specific issue"]},"priority_improvements":["1. specific action","2. specific action","3. specific action"],"missing_chars":[{"name":"characteristic name","how_competitors_use":"how they use it","priority":"HIGH"}],"tech_params":[{"param":"parameter name","competitor_value":"their value","our_gap":"our gap"}],"actions":[{"action":"specific action","impact":"HIGH","effort":"LOW","details":"details"}]}'


def get_asin(url):
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None

def sc(s): return "🟢" if s>=8 else ("🟡" if s>=6 else "🔴")
def badge(p): return {"HIGH":"🔴 HIGH","MEDIUM":"🟡 MEDIUM","LOW":"🟢 LOW"}.get(p,p)

def get_asin_from_data(d):
    return d.get("_input_asin","") or d.get("parent_asin","") or d.get("product_information",{}).get("ASIN","")

def pct(val):
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

# ── Vision cost/time estimator ────────────────────────────────────────────────
def estimate_run(do_vision, do_aplus, do_comp_vision, n_competitors, use_gemini):
    """Returns (time_str, cost_str) estimate based on selected options"""
    # Base: scraping + text AI
    secs = 30
    cost = 0.008

    model_mult = 0.3 if use_gemini else 1.0  # Gemini much cheaper

    if do_vision:
        secs += 45
        cost += 0.015 * model_mult
    if do_aplus:
        secs += 20
        cost += 0.008 * model_mult
    if do_comp_vision and n_competitors > 0:
        secs += 40 * n_competitors
        cost += 0.02 * n_competitors * model_mult
    elif n_competitors > 0:
        # Still scrape + text AI for competitors
        secs += 20 * n_competitors
        cost += 0.008 * n_competitors * model_mult

    mins = secs // 60
    secs_rem = secs % 60
    time_str = f"~{mins}м {secs_rem}с" if mins > 0 else f"~{secs}с"
    cost_str = f"~${cost:.2f}"
    return time_str, cost_str

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

def _anthropic_post(payload, retries=3):
    import time
    key = st.secrets.get("ANTHROPIC_API_KEY","")
    if not key: raise Exception("ANTHROPIC_API_KEY не задан")
    headers = {"x-api-key":key,"anthropic-version":"2023-06-01","content-type":"application/json"}
    for attempt in range(retries):
        r = requests.post(ANTHROPIC_URL, headers=headers, json=payload, timeout=300)
        if r.ok:
            st.session_state.pop("_api_balance_error", None)
            return r.json()["content"][0]["text"]
        if r.status_code == 402 or "credit balance" in r.text.lower() or "insufficient" in r.text.lower():
            st.session_state["_api_balance_error"] = True
            raise Exception("❌ Баланс Claude API исчерпан — пополни на console.anthropic.com")
        if r.status_code == 529:
            wait = 20 * (attempt + 1)
            st.toast(f"⏳ Anthropic перегружен, жду {wait}с... ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        raise Exception(f"Anthropic {r.status_code}: {r.json().get('error',{}).get('message','')}")
    raise Exception("Anthropic перегружен — попробуй через минуту")

def anthropic_call(system, user, max_tokens=3000):
    payload = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens, "messages": [{"role":"user","content":user}]}
    if system: payload["system"] = system
    return _anthropic_post(payload)

def anthropic_vision(content_blocks, max_tokens=3000, system=None):
    payload = {"model": ANTHROPIC_MODEL_VISION, "max_tokens": max_tokens,
               "messages": [{"role":"user","content":content_blocks}]}
    if system: payload["system"] = system
    return _anthropic_post(payload)

def gemini_call(prompt, max_tokens=3000):
    import time
    key = st.secrets.get("GEMINI_API_KEY","")
    if not key: raise Exception("GEMINI_API_KEY не задан в Secrets")
    _gmodel = st.session_state.get("gemini_model","gemini-2.5-flash")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{_gmodel}:generateContent?key={key}"
    payload = {"contents":[{"parts":[{"text":prompt}]}],
               "generationConfig":{"maxOutputTokens":max_tokens}}
    for attempt in range(3):
        r = requests.post(url, json=payload, timeout=120)
        if r.ok:
            return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        if r.status_code in (429, 503, 500):
            wait = 60*(attempt+1)
            st.toast(f"⏳ Gemini лимит, жду {wait}с... ({attempt+1}/3)")
            import time; time.sleep(wait); continue
        raise Exception(f"Gemini {r.status_code}: {r.text[:200]}")
    raise Exception("Gemini перегружен — попробуй через 2 мин")

def gemini_vision_call(prompt, image_urls=None, image_b64_list=None, max_tokens=2000):
    import time
    key = st.secrets.get("GEMINI_API_KEY","")
    if not key: raise Exception("GEMINI_API_KEY не задан")
    _gmodel = st.session_state.get("gemini_model","gemini-2.5-flash")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{_gmodel}:generateContent?key={key}"
    parts = []
    if image_urls:
        for img_url in image_urls:
            try:
                r_img = requests.get(img_url, timeout=15)
                if r_img.ok:
                    import base64 as _b64
                    parts.append({"inline_data": {"mime_type": "image/jpeg",
                        "data": _b64.b64encode(r_img.content).decode()}})
            except: pass
    if image_b64_list:
        for b64, mime in image_b64_list:
            parts.append({"inline_data": {"mime_type": mime or "image/jpeg", "data": b64}})
    parts.append({"text": prompt})
    payload = {"contents": [{"parts": parts}],
               "generationConfig": {"maxOutputTokens": max_tokens}}
    _last_err = ""
    for attempt in range(3):
        r = requests.post(url, json=payload, timeout=120)
        _last_err = f"{r.status_code}: {r.text[:300]}"
        st.toast(f"🔍 Gemini Vision attempt {attempt+1}: {r.status_code}")
        if r.ok:
            return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        if r.status_code in (429, 503, 500):
            wait = 60*(attempt+1)
            st.toast(f"⏳ Gemini Vision {r.status_code}, жду {wait}с...")
            import time; time.sleep(wait); continue
        raise Exception(f"Gemini Vision {_last_err}")
    raise Exception(f"Gemini Vision исчерпан: {_last_err}")

def ai_vision_call(prompt, image_b64=None, image_url=None, media_type="image/jpeg", max_tokens=400, system=None):
    if st.session_state.get("use_gemini"):
        full = f"{system}\n\n{prompt}" if system else prompt
        if image_url:
            return gemini_vision_call(full, image_urls=[image_url], max_tokens=max_tokens)
        elif image_b64:
            return gemini_vision_call(full, image_b64_list=[(image_b64, media_type)], max_tokens=max_tokens)
    else:
        blocks = []
        if image_b64:
            blocks = [
                {"type":"text","text": prompt},
                {"type":"image","source":{"type":"base64","media_type":media_type,"data":image_b64}}
            ]
        return anthropic_vision(blocks, max_tokens=max_tokens, system=system)

def ai_call(system, user, max_tokens=3000):
    if st.session_state.get("use_gemini"):
        full = f"{system}\n\n{user}" if system else user
        return gemini_call(full, max_tokens)
    return anthropic_call(system, user, max_tokens)

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
        if data.get("aplus"):
            _api_aplus = data.get("aplus_images", [])
            _real_aplus = []
            for _u in _api_aplus:
                if not isinstance(_u, str): continue
                if "grey-pixel" in _u: continue
                if not _u.startswith("http"): continue
                import re as _re
                _u2 = _re.sub(r'\.__CR[^.]+_PT0_SX\d+_V\d+___', '', _u)
                _real_aplus.append(_u2)
            if _real_aplus:
                data["aplus_image_urls"] = _real_aplus[:8]
                log(f"  ✅ A+ из aplus_images: {len(_real_aplus)} баннеров")
            else:
                _prod_imgs = data.get("images_of_specified_asin", [])
                _all_imgs  = data.get("images", [])
                _split = len(_prod_imgs) if _prod_imgs else 6
                _aplus_candidates = [u for u in _all_imgs[_split:]
                                     if isinstance(u, str) and u.startswith("http")
                                     and "grey-pixel" not in u
                                     and "_SS4" not in u and "_SR3" not in u][:12]
                if _aplus_candidates:
                    data["aplus_image_urls"] = _aplus_candidates
                    log(f"  ✅ A+ из images[{_split}+]: {len(_aplus_candidates)} шт")
                else:
                    log("  ℹ️ A+ баннеры недоступны")
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
    _vision_model = st.session_state.get("gemini_model","gemini-2.5-flash") if st.session_state.get("use_gemini") else ANTHROPIC_MODEL_VISION
    log(f"👁️ Vision: {len(images)} фото → {'Gemini '+_vision_model if st.session_state.get('use_gemini') else 'Claude'} ...")

    title  = product_data.get("title","")
    price  = product_data.get("price","")
    rating = product_data.get("average_rating","")
    reviews= product_data.get("reviews_count","")
    bsr    = product_data.get("bestseller_rank","")

    if lang == "en":
        intro = f"""You are an Amazon photo conversion expert. Score each product photo using this RUBRIC.

Product: {title} | ASIN: {asin} | Price: {price} | Rating: {rating}

SCORING RUBRIC (each photo scored 1-10):
+2 pts — Product visibility: what % of frame is the ACTUAL sold product? If product is hidden under other clothing or <30% of frame — max 1 pt for this criterion
+2 pts — Background: main=pure white RGB(255,255,255); lifestyle=relevant setting; infographic=clean layout
+2 pts — Information value: shows features/benefits/use case relevant to buyer decision
+2 pts — Amazon compliance: no watermarks, no promo text on main, correct aspect ratio
+1 pt  — Lifestyle appeal: buyer can visualize using the product
+1 pt  — Uniqueness vs generic stock photo

CRITICAL PENALTIES (deduct immediately):
-3 pts — Sold product hidden / barely visible (e.g. tank top under a shirt)
-2 pts — Main photo has clothing/accessories not included in purchase
-2 pts — Main photo background is not pure white

BE STRICT: real problems must lower the score. 10/10 only if photo is perfect on ALL criteria.
SCORE MEANINGS: 9-10=excellent, 7-8=good, 5-6=needs improvement, 1-4=poor/replace

PHOTO TYPES: main | lifestyle | infographic | size-chart | detail | A+-banner | comparison | packaging

MAIN IMAGE AMAZON REQUIREMENTS (apply strictly for photo #1):
✅ Pure white background RGB(255,255,255) — no shadows, no grey
✅ Product fills ≥85% of frame — estimate visually in %
✅ No text, logos, watermarks, promo graphics, badges
✅ Minimum 1000px long side (ideally 2000-3000px) for zoom
✅ Real photo (not illustration)
✅ ONLY the sold product — FORBIDDEN: other clothing on model, accessories, props not included
✅ For apparel: the sold item must be the primary focus
IMPORTANT: Look carefully — are there any items in the photo that are NOT the sold product? If yes — deduct 2 pts and name exactly what violates the rule."""
        block_fmt = "\nPHOTO_BLOCK_{i}\nSTRICTLY 5 lines:\nType: [one of the types above]\nScore: X/10 [apply rubric]\nStrength: [1 specific strength]\nWeakness: [1 specific problem — ONLY what you actually see]\nAction: [1 fix starting with a verb: Reshoot / Remove / Crop / Replace / Reduce. Be specific about what to do.]"
    else:
        intro = f"""Ты эксперт по конверсии Amazon фотографий. Оценивай каждое фото по РУБРИКУ.

Товар: {title} | ASIN: {asin} | Цена: {price} | Рейтинг: {rating}

РУБРИК ОЦЕНКИ (каждое фото 1-10 баллов):
+2 балла — Видимость товара: сколько % кадра занимает продаваемый товар? Если <30% — максимум 1 балл
+2 балла — Фон: главное=чисто белый RGB(255,255,255); lifestyle=релевантная обстановка
+2 балла — Информационная ценность: показывает характеристики/пользу/сценарий важный для покупателя
+2 балла — Соответствие Amazon: нет водяных знаков, нет промотекста на главном
+1 балл  — Lifestyle appeal: покупатель представляет себя с товаром
+1 балл  — Уникальность: не выглядит как стоковое фото

КРИТИЧЕСКИЕ ШТРАФЫ (вычитай сразу):
-3 балла — Продаваемый товар скрыт / почти не виден
-2 балла — На главном фото одежда/аксессуары НЕ из комплекта
-2 балла — Фон главного фото не чисто белый

БУДЬ СТРОГ: реальные проблемы должны снижать оценку. 10/10 только если фото идеально по ВСЕМ критериям.
ЗНАЧЕНИЯ: 9-10=отлично, 7-8=хорошо, 5-6=требует улучшения, 1-4=слабо/заменить

ТИПЫ ФОТ: главное | lifestyle | инфографика | размерная-сетка | детали | A+-баннер | сравнение | упаковка

ТРЕБОВАНИЯ AMAZON К ГЛАВНОМУ ФОТО (применяй строго к фото #1):
✅ Фон исключительно белый RGB(255,255,255) — без теней, без серого
✅ Товар занимает ≥85% площади кадра
✅ Нет текста, логотипов, водяных знаков, промо-графики, бейджей
✅ Минимум 1000px по длинной стороне
✅ Реальная фотография (не иллюстрация)
✅ ТОЛЬКО продаваемый товар — ЗАПРЕЩЕНЫ: другая одежда на модели, аксессуары не из комплекта
ВАЖНО: Посмотри внимательно — есть ли на фото предметы которые НЕ являются продаваемым товаром? Если да — это нарушение, снять 2 балла и написать конкретно что нарушает правило."""
        block_fmt = "\nPHOTO_BLOCK_{i}\nОТРОГО 5 строк:\nТип: [один из типов выше]\nОценка: X/10 [применяй рубрик]\nСильная сторона: [1 конкретная сильная сторона]\nСлабость: [1 конкретная проблема — ТОЛЬКО то что видишь на фото]\nДействие: [1 конкретное исправление начиная с глагола: Переснять / Убрать / Обрезать / Заменить / Уменьшить. Конкретно что сделать.]"

    results = []
    if st.session_state.get("use_gemini"):
        import time; time.sleep(5)
        _fmt = "Тип: [тип]\nОценка: X/10\nСильная сторона: [текст]\nСлабость: [текст]\nДействие: [текст]"
        for _i, _img in enumerate(images):
            if _i > 0: time.sleep(8)
            log(f"👁️ Gemini фото {_i+1}/{len(images)}...")
            if _i == 0:
                _pp = intro
            else:
                _pp = f"Ты эксперт Amazon фотографий. Оцени это фото #{_i+1} по рубрику: +2 видимость товара, +2 фон, +2 инфоценность, +2 Amazon соответствие, +1 appeal, +1 уникальность. Штрафы: -3 товар не виден, -2 лишняя одежда на главном, -2 фон не белый. Товар: {title}"
            _pp += f"\n\nОтветь СТРОГО в формате:\nPHOTO_BLOCK_{_i+1}\n{_fmt}"
            _br = gemini_vision_call(_pp, image_b64_list=[(_img["b64"], _img.get("media_type","image/jpeg"))], max_tokens=600)
            _m = re.search(r"PHOTO_BLOCK_\d+\s*(.*)", _br, re.DOTALL)
            _blk = _m.group(1).strip() if _m else _br.strip()
            results.append(f"PHOTO_BLOCK_{_i+1}\n{_blk}")
    else:
        for i, img in enumerate(images):
            log(f"👁️ Фото {i+1}/{len(images)} {'🔵' * (i+1)}{'⚪' * (len(images)-i-1)}")
            if i == 0:
                photo_intro = intro
            else:
                if lang == "en":
                    photo_intro = f"You are an Amazon photo expert. Score photo #{i+1}: +2 clarity, +2 background, +2 info value, +2 Amazon compliance, +1 appeal, +1 uniqueness. Product: {title}"
                else:
                    photo_intro = f"Ты эксперт Amazon фотографий. Оцени фото #{i+1}: +2 чёткость, +2 фон, +2 инфоценность, +2 Amazon, +1 appeal, +1 уникальность. Товар: {title}"
            _full_prompt = photo_intro + "\n" + block_fmt.format(i=i+1)
            res = ai_vision_call(prompt=_full_prompt, image_b64=img["b64"],
                image_url=img.get("url"), media_type=img.get("media_type","image/jpeg"), max_tokens=400)
            results.append("PHOTO_BLOCK_" + str(i+1) + "\n" + res)

    result = "\n\n".join(results)
    log(f"✅ Vision готово: {len(images)} фото")
    return result

# ── A+ Vision ─────────────────────────────────────────────────────────────────
def analyze_aplus_vision(aplus_urls, product_data, log, lang=None):
    if not aplus_urls: return ""
    if lang is None:
        lang = st.session_state.get("analysis_lang", "ru")

    images = []
    for i, url in enumerate(aplus_urls[:8]):
        if "grey-pixel" in url: continue
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent":"Mozilla/5.0"})
            if r.ok and len(r.content) > 3000:
                data_img, mt = compress_image(r.content)
                images.append({"b64": base64.b64encode(data_img).decode(), "media_type": mt, "url": url})
                log(f"  📥 A+ баннер {len(images)}: {len(data_img)//1024}KB ✅")
        except Exception as e:
            log(f"  ⚠️ A+ баннер {i+1}: {e}")
    if not images: return ""

    title = product_data.get("title","")
    if lang == "en":
        sys_prompt = f"""You are an Amazon A+ Content expert. Analyze each A+ banner/module.
Product: {title}

For each image output EXACTLY:
APLUS_BLOCK_{{i}}
Module: [comparison-table | lifestyle | feature-highlight | brand-story | size-guide | product-range | other]
Summary: [what this module shows — 1-2 sentences]
Score: X/10
Strength: [1 specific strength]
Weakness: [1 specific problem you see]
Action: [1 concrete fix starting with a verb: Redesign / Remove / Add / Replace / Simplify]"""
    else:
        sys_prompt = f"""Ты эксперт по Amazon A+ Content. Анализируй каждый A+ баннер.
Товар: {title}

Для каждого изображения выводи СТРОГО:
APLUS_BLOCK_{{i}}
Модуль: [сравнительная-таблица | lifestyle | highlight-фич | brand-story | таблица-размеров | линейка-продуктов | другой]
Содержание: [что показывает — 1-2 предложения]
Оценка: X/10
Сильная сторона: [1 конкретная]
Слабость: [1 конкретная проблема которую видишь]
Действие: [1 конкретный фикс начиная с глагола: Переделать / Убрать / Добавить / Заменить / Упростить]"""

    msg_content = []
    for i, img in enumerate(images):
        msg_content.append({"type":"text","text":f"{'A+ banner' if lang=='en' else 'A+ баннер'} #{i+1}:"})
        msg_content.append({"type":"image","source":{"type":"base64","media_type":img["media_type"],"data":img["b64"]}})

    try:
        if st.session_state.get("use_gemini"):
            _ap_b64 = [(img["b64"], img["media_type"]) for img in images]
            _ap_prompt = sys_prompt + "\n\n" + "\n".join(
                [f"A+ баннер #{i+1}:" for i in range(len(images))])
            result = gemini_vision_call(_ap_prompt, image_b64_list=_ap_b64, max_tokens=2000)
        else:
            result = anthropic_vision(msg_content, max_tokens=2000, system=sys_prompt)
        log(f"✅ A+ Vision: {len(images)} баннеров проанализировано")
        return result
    except Exception as e:
        log(f"⚠️ A+ Vision: {e}"); return ""

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

    _title_text   = our_data.get("title", "")
    _bullets      = our_data.get("feature_bullets", [])
    _title_len    = len(_title_text)
    _bullet_bytes = [len(b.encode()) for b in _bullets]
    _facts = f"""
FACTUAL STATS (do NOT contradict these):
- Title length: {_title_len} characters (limit: 125)
- Title is {'OVER limit' if _title_len > 125 else 'within limit'}
- Number of bullets: {len(_bullets)}
- Bullet byte lengths: {_bullet_bytes}
- Any bullet over 250 bytes: {'YES - ' + str([i+1 for i,x in enumerate(_bullet_bytes) if x>250]) if any(x>250 for x in _bullet_bytes) else 'NO'}
- Description present: {'YES' if our_data.get('description') else 'NO - score must be 0%'}
- A+ present: {'YES' if our_data.get('aplus_content') else 'NO'}
"""
    our_text = _facts + "\n" + fmt(our_data)[:2500]
    def fmt_comp(d):
        pi = d.get("product_information", {})
        buls = d.get("feature_bullets", [])
        parts = [
            f"Title: {d.get('title','')}",
            f"Price: {d.get('price','')} | Rating: {d.get('average_rating','')} | Reviews: {pi.get('Customer Reviews',{}).get('ratings_count','')}",
            f"BSR: {str(pi.get('Best Sellers Rank',''))[:80]}",
            f"A+: {'Yes' if d.get('aplus_content') else 'No'} | Sizes: {len(d.get('customization_options',{}).get('size',[]))}",
        ]
        if buls: parts.append("Bullets:\n" + "\n".join(f"- {b[:120]}" for b in buls[:5]))
        if d.get("description"): parts.append(f"Description: {str(d['description'])[:200]}")
        return "\n".join(filter(None, parts))
    comp_text = "\n\n".join([f"COMPETITOR {i+1}:\n{fmt_comp(d)}" for i,d in enumerate(competitor_data_list) if d])
    vision_section = f"\nPHOTO VISION ANALYSIS:\n{vision_result[:1500]}" if vision_result else ""
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

### TITLE — ≤125 chars, [Material][Gender][Type][Feature][Use case] format, top keywords, readable
- NOTE: Brand name is shown separately above title by Amazon — do NOT penalize for missing brand in title
- 90-100%: ≤125 chars, has material+type+gender+key feature, no keyword stuffing, readable
- 70-89%: Minor issues (missing 1 element, slightly keyword-heavy)
- 50-69%: Too long (>125), poor structure, or unreadable
- 0-49%: Broken, all caps spam, or completely irrelevant

### BULLETS — 5 bullets, ≤250 chars each, "Feature: Details. Benefit." format
- 90-100%: All 5 bullets, perfect format, addresses customer concerns
- 70-89%: Good structure but missing benefits
- 50-69%: Walls of text, no benefits
- 0-49%: Missing bullets

### DESCRIPTION — Plain text or limited HTML
- IMPORTANT: If seller has A+ content → description is hidden from buyers
- 90-100%: 300-2000 chars, covers features/care/use cases, no duplicate of bullets
- 0%: Empty or missing — score MUST be "0%". No exceptions.
- description_gaps: if empty, write ONLY ["Описание отсутствует"].

### IMAGES — Evaluate: main image (40%), gallery completeness (30%), OCR readability (30%)
- IMPORTANT: If vision analysis shows main image has violations → images_score MAX 70%

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
- title_gaps: 1-3 REAL issues in the ACTUAL title text. Score >=85% = max 1 gap or [].
- bullets_gaps: 1-3 REAL issues in the ACTUAL bullets.
- description_gaps: if no description exists write ONLY ["Описание отсутствует"].
- aplus_gaps: if no A+ exists write ["A+ контент отсутствует — создать"], else real issues only.
- CRITICAL: Read the actual listing text before writing gaps. Never hallucinate missing elements that are already present.

{SCHEMA}"""

    sys_prompt = f"Amazon listing expert. Return ONLY valid JSON. No markdown. No preamble. All text in {lang_name}."
    raw = ai_call(sys_prompt, prompt, max_tokens=12000)
    log(f"✅ JSON: {len(raw)} chars")

    if not raw or not raw.strip():
        log("⚠️ AI пустой ответ, повтор...")
        raw = ai_call(sys_prompt, prompt, max_tokens=12000)
    if not raw or not raw.strip():
        raise ValueError("AI вернул пустой ответ")
    log(f"🔍 Raw preview: {raw[:60]}")
    s = raw.strip()
    s = re.sub(r"^```[a-z]*\s*", "", s, flags=re.MULTILINE)
    s = re.sub(r"```\s*$", "", s, flags=re.MULTILINE)
    s = s.strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1:
        raise ValueError(f"JSON не найден: {s[:200]}")
    s = s[start:end+1]
    s = re.sub(r",\s*([}\]])", r"\1", s)
    def _try_parse(txt):
        txt = re.sub(r",\s*([}\]])", r"\1", txt)
        return json.loads(txt)
    try:
        return _try_parse(s)
    except Exception:
        s2 = re.sub(r'"([^"]*)"', lambda m: '"'+m.group(1).replace("\n"," ")+'"', s)
        try:
            return _try_parse(s2)
        except Exception:
            for cut in range(len(s2)-1, 0, -1):
                if s2[cut] in ('"', '}', ']', '0123456789'):
                    candidate = s2[:cut+1]
                    candidate += "]" * max(0, s2[:cut+1].count("[") - s2[:cut+1].count("]"))
                    candidate += "}" * max(0, s2[:cut+1].count("{") - s2[:cut+1].count("}"))
                    try:
                        return _try_parse(candidate)
                    except:
                        continue
            raise ValueError("Не удалось исправить JSON")


# ── Main ──────────────────────────────────────────────────────────────────────
def run_analysis(our_url, competitor_urls, log, prog=None):
    def _prog(pct, text):
        if prog: prog.progress(pct, text=text)
        log(text)

    asin = get_asin(our_url) or "unknown"
    _lang = st.session_state.get("analysis_lang","ru")

    # Read vision toggles
    _do_vision      = st.session_state.get("do_vision", True)
    _do_aplus       = st.session_state.get("do_aplus_vision", True)
    _do_comp_vision = st.session_state.get("do_comp_vision", True)

    _prog(5,  f"🌐 Загружаю данные листинга {asin}...")
    our_data, img_urls = scrapingdog_product(asin, log)

    _prog(15, f"⬇️ Скачиваю фото ({len(img_urls)} шт.)...")
    images = download_images(img_urls, log) if img_urls else []
    st.session_state["images"] = images

    # ── Vision фото (основной листинг) ───────────────────────────────────────
    if images and _do_vision:
        _prog(30, "👁️ Vision анализ фото...")
        vision_result = analyze_vision(images, our_data, asin, log, lang=_lang)
    else:
        vision_result = ""
        if not images:
            log("⚠️ Фото не загружены")
        else:
            log("⏭️ Vision фото пропущен (отключён)")

    # ── A+ Vision ─────────────────────────────────────────────────────────────
    _aplus_urls = our_data.get("aplus_image_urls", [])
    if _aplus_urls and _do_aplus:
        _prog(35, f"🎨 A+ Vision: {len(_aplus_urls)} баннеров...")
        aplus_vision = analyze_aplus_vision(_aplus_urls, our_data, log, lang=_lang)
        st.session_state["aplus_vision"] = aplus_vision
        log(f"✅ A+ Vision: {len(_aplus_urls)} баннеров проанализировано")
    else:
        st.session_state["aplus_vision"] = ""
        if _aplus_urls and not _do_aplus:
            log("⏭️ A+ Vision пропущен (отключён)")
        else:
            log("ℹ️ A+ баннеры не найдены (нет aplus_image_urls)")
    # URLs сохраняем всегда — чтобы картинки показывались даже без анализа
    st.session_state["aplus_img_urls"] = _aplus_urls

    # ── Конкуренты ────────────────────────────────────────────────────────────
    active = [u.strip() for u in competitor_urls if u.strip()]
    comp_data_list = []
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

        # ── Vision конкурента — управляется чекбоксом ─────────────────────
        if cimgs_dl and _do_comp_vision:
            _prog(base_pct + 5, f"👁️ Конкурент {i+1}: Vision анализ...")
            cvision = analyze_vision(cimgs_dl, cdata, casin, log, lang=_lang)
        else:
            cvision = ""
            if cimgs_dl:
                log(f"⏭️ Vision конкурент {i+1} пропущен (отключён)")

        _prog(base_pct + 8, f"🧠 Конкурент {i+1}: AI анализ...")
        cai = analyze_text(cdata, [], cvision, casin, log, lang=_lang)

        st.session_state[f"comp_ai_{i}"] = cai
        if cimgs_dl:
            st.session_state[f"comp_vision_{i}"] = (cimgs_dl, cvision)

    _prog(75, "🧠 AI финальный анализ нашего листинга...")
    result = analyze_text(our_data, comp_data_list, vision_result, asin, log, lang=_lang)
    st.session_state['our_data'] = our_data
    st.session_state['comp_data_list'] = comp_data_list
    return result, vision_result

# ── UI ────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Listing Analyzer", page_icon="https://merino.tech/cdn/shop/files/MT_logo_1.png?v=1685099753&width=260", layout="wide")

with st.sidebar:
    st.image("https://merino.tech/cdn/shop/files/MT_logo_1.png?v=1685099753&width=260", width=120)
    st.markdown("## <span style='color:#c0392b'>Listing Analyzer</span>", unsafe_allow_html=True)
    st.divider()

    # ── API balance warning ───────────────────────────────────────────────────
    if st.session_state.get("_api_balance_error"):
        st.markdown("""
<div style="background:#fef2f2;border:1.5px solid #ef4444;border-radius:8px;padding:10px 12px;margin-bottom:8px">
<div style="font-size:0.85rem;font-weight:700;color:#dc2626">💳 Баланс Anthropic API исчерпан</div>
<div style="font-size:0.75rem;color:#64748b;margin-top:3px">console.anthropic.com → Billing → Buy credits</div>
<div style="font-size:0.75rem;color:#94a3b8;margin-top:2px">⚠️ Не путать с подпиской Claude.ai — это разные счета</div>
</div>""", unsafe_allow_html=True)
        st.link_button("🔗 Пополнить баланс", "https://console.anthropic.com/settings/billing", use_container_width=True)
        if st.button("✅ Уже пополнил", use_container_width=True, key="dismiss_balance_err"):
            st.session_state.pop("_api_balance_error", None)
            st.rerun()
        st.divider()

    if "page" not in st.session_state:
        st.session_state["page"] = "🏠 Обзор"

    NAV_ITEMS = [
        ("🏠", "Обзор"),
        ("📸", "Фото"),
        ("🎨", "A+ Контент"),
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
            _casin = get_asin_from_data(_c)
            _ct = _c.get("title","")
            st.markdown(f"""<div style="background:#fff5f5;border-radius:8px;padding:8px 10px;margin-bottom:4px;border-left:3px solid #ef4444">
<div style="font-size:0.75rem;font-weight:700;color:#b91c1c">🔴 {_casin}</div>
<div style="font-size:0.7rem;color:#64748b;margin-top:1px">{_ct[:30]}{"..." if len(_ct)>30 else ""}</div>
</div>""", unsafe_allow_html=True)

        st.divider()

        cur = st.session_state.get("page","🏠 Обзор")

        st.markdown('<div style="font-size:0.7rem;font-weight:700;color:#94a3b8;letter-spacing:0.08em;padding:4px 2px">МЫ</div>', unsafe_allow_html=True)
        for icon, label in NAV_ITEMS:
            full = f"{icon} {label}"
            is_active = (cur == full)
            if st.button(f"{icon}  {label}", key=f"nav_{label}", use_container_width=True,
                         type="primary" if is_active else "secondary"):
                st.session_state["page"] = full
                st.rerun()

        if _cd_nav:
            st.markdown('<div style="font-size:0.7rem;font-weight:700;color:#94a3b8;letter-spacing:0.08em;padding:12px 2px 4px">КОНКУРЕНТЫ</div>', unsafe_allow_html=True)
            for _i2, _c2 in enumerate(_cd_nav):
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
    _cur3 = st.session_state.get("page","")
    _h_col1, _h_col2 = st.columns([2,1])
    if _h_col1.button("📈  История", key="nav_history", use_container_width=True,
                 type="primary" if _cur3=="📈 История" else "secondary"):
        st.session_state["page"] = "📈 История"
        st.rerun()
    if st.button("📋  Workflow", key="nav_workflow", use_container_width=True,
                 type="primary" if _cur3=="📋 Workflow" else "secondary"):
        st.session_state["page"] = "📋 Workflow"
        st.rerun()
    if st.session_state.get("our_url_saved") and "result" in st.session_state:
        if st.button("🔄 Обновить анализ", use_container_width=True, key="sidebar_refresh"):
            st.session_state["_trigger_rerun"] = True
            st.rerun()

    if "result" in st.session_state and "our_data" in st.session_state:
        _asin_s = st.session_state["our_data"].get("parent_asin","")
        _sc_s = st.session_state["result"].get("overall_score","—")
        if isinstance(_sc_s,(int,float)): _sc_s = f"{int(_sc_s)}%"
        _url = "https://listing-analyze.streamlit.app"
        _msg = f"Amazon Listing Audit%0AASIN: {_asin_s}%0AScore: {_sc_s}%0A{_url}"
        _h_col2.markdown(
            f'<a href="https://t.me/share/url?url={_url}&text={_msg}" target="_blank">' +
            '<button style="width:100%;padding:6px 2px;background:#0088cc;color:white;' +
            'border:none;border-radius:6px;cursor:pointer;font-size:0.75rem">📤 TG</button></a>',
            unsafe_allow_html=True)

    if st.session_state.get("_hist_loaded"):
        if st.sidebar.button("↩️ Новый анализ", type="primary", use_container_width=True):
            for _k in ["_hist_loaded", "result", "vision", "images", "our_data",
                       "comp_data_list"] + [f"comp_ai_{i}" for i in range(5)]:
                st.session_state.pop(_k, None)
            st.session_state["page"] = "🏠 Обзор"
            st.rerun()

    st.divider()
    st.markdown("**🗄️ DB**")
    if st.button("🧪 Тест БД", key="db_test"):
        db_url = st.secrets.get("DATABASE_URL","")
        if not db_url:
            st.sidebar.error("❌ DATABASE_URL не найден")
        else:
            conn = get_db()
            if not conn:
                err = st.session_state.get("_db_err","неизвестная ошибка")
                st.sidebar.error(f"❌ {err}")
            else:
                try:
                    db_init()
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM listing_analysis")
                    cnt = cur.fetchone()[0]
                    conn.close()
                    st.sidebar.success(f"✅ БД OK | {cnt} записей")
                except Exception as e:
                    st.sidebar.error(f"❌ {e}")

    st.divider()
    st.markdown("**🤖 Модель AI**")
    _model_choice = st.radio(
        "Выбор модели",
        ["⚡ Claude (Anthropic)", "🟢 Gemini (Google)"],
        horizontal=True, key="model_choice", label_visibility="collapsed"
    )
    st.session_state["use_gemini"] = "Gemini" in _model_choice
    if st.session_state.get("use_gemini"):
        _gem_model = st.selectbox("Gemini модель", [
            "gemini-2.5-flash",
            "gemini-2.5-flash-lite",
            "gemini-2.0-flash",
            "gemini-2.0-flash-lite",
            "gemini-2.5-pro",
        ], key="gemini_model_sel", label_visibility="collapsed")
        st.session_state["gemini_model"] = _gem_model

    st.divider()
    st.markdown("**🔑 API**")
    _ac1, _ac2 = st.columns(2)
    if _ac1.button("🧪 Claude", key="api_test"):
        try:
            res = anthropic_call(None, "Say: OK", max_tokens=5)
            st.success(f"✅ Claude: {res}")
        except Exception as e:
            st.error(f"❌ {str(e)[:60]}")
    if _ac2.button("🧪 Gemini", key="api_test_gem"):
        _key = st.secrets.get("GEMINI_API_KEY","")
        for _ep in ["v1", "v1beta"]:
            try:
                _r = requests.get(f"https://generativelanguage.googleapis.com/{_ep}/models?key={_key}", timeout=10)
                if _r.ok:
                    _names = [m["name"] for m in _r.json().get("models",[]) if "generateContent" in m.get("supportedGenerationMethods",[])]
                    st.info(f"[{_ep}] Доступно {len(_names)} моделей:\n" + "\n".join(_names[:20]))
                    break
                else:
                    st.warning(f"[{_ep}] {_r.status_code}: {_r.text[:100]}")
            except Exception as _le:
                st.warning(f"[{_ep}] {_le}")
        try:
            res = gemini_call("Say: OK")
            st.success(f"✅ Gemini: {res[:40]}")
        except Exception as e:
            st.error(f"❌ {str(e)[:200]}")

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

    prev_url  = st.session_state.get("our_url_saved","")
    prev_comp = [st.session_state.get(f"c{i}_saved","") for i in range(5)]
    curr_comp = competitor_urls
    our_changed  = (our_url.strip() != prev_url.strip())
    new_comps    = [u for u,p in zip(curr_comp,prev_comp) if u.strip() and u.strip()!=p.strip()]
    already_done = "result" in st.session_state

    if already_done and not our_changed and new_comps:
        btn_label = f"➕ Добавить {len(new_comps)} конкурент(а)"
    elif already_done and not our_changed and not new_comps:
        btn_label = "🔄 Перезапустить анализ"
    else:
        btn_label = "🚀 Запустить анализ"

    lang = st.radio("🌐 Язык анализа", ["🇷🇺 Русский", "🇺🇸 English"], horizontal=True, key="lang_sel")
    st.session_state["analysis_lang"] = "ru" if "Русский" in lang else "en"

    # ── Оптимизация токенов — чекбоксы Vision ────────────────────────────────
    with st.expander("⚡ Оптимизация токенов", expanded=False):
        _n_comps = len([u for u in competitor_urls if u.strip()])
        _use_gem = st.session_state.get("use_gemini", False)

        _vc1, _vc2, _vc3 = st.columns(3)
        _do_vision      = _vc1.checkbox("🖼️ Vision фото (наш)",    value=st.session_state.get("do_vision", True),      key="cb_vision")
        _do_aplus       = _vc2.checkbox("🎨 Vision A+ баннеры",     value=st.session_state.get("do_aplus_vision", True), key="cb_aplus")
        _do_comp_vision = _vc3.checkbox("👁️ Vision конкурентов",    value=st.session_state.get("do_comp_vision", True),  key="cb_comp_v")

        st.session_state["do_vision"]       = _do_vision
        st.session_state["do_aplus_vision"] = _do_aplus
        st.session_state["do_comp_vision"]  = _do_comp_vision

        # Cost/time estimate
        _t_str, _c_str = estimate_run(_do_vision, _do_aplus, _do_comp_vision, _n_comps, _use_gem)

        _what_on = []
        if _do_vision:      _what_on.append("Vision фото")
        if _do_aplus:       _what_on.append("A+")
        if _do_comp_vision and _n_comps: _what_on.append(f"Vision {_n_comps} конк.")
        _mode = " + ".join(_what_on) if _what_on else "только текст"

        # Determine scenario label and hint
        _off = []
        if not _do_vision:      _off.append("Vision фото")
        if not _do_aplus:       _off.append("Vision A+")
        if not _do_comp_vision: _off.append("Vision конк.")

        if not (_do_vision or _do_aplus or _do_comp_vision):
            _est_color = "#22c55e"
            _scenario  = "⚡ Быстрый ретест"
            _hint      = "Только текстовый AI — идеально после правки title/bullets"
        elif not (_do_vision and _do_aplus and _do_comp_vision):
            _est_color = "#f59e0b"
            _off_str   = ", ".join(_off)
            _scenario  = f"🔶 Частичный анализ (без: {_off_str})"
            if not _do_comp_vision and _do_vision:
                _hint = "Глубокий аудит нашего листинга без трат на конкурентов"
            elif not _do_vision and _do_comp_vision:
                _hint = "Быстро проверить конкурентов без полного Vision нашего листинга"
            else:
                _hint = "Выборочный режим — включи нужные блоки выше"
        else:
            _est_color = "#ef4444"
            _scenario  = "🔴 Полный аудит"
            _hint      = "Vision всех фото + A+ + конкуренты — максимум данных"

        st.markdown(
            f'<div style="background:#f1f5f9;border-radius:8px;padding:10px 14px;margin-top:6px;'
            f'border-left:4px solid {_est_color}">'
            f'<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:4px">'
            f'<span style="font-size:0.95rem">⏱ <b>{_t_str}</b> &nbsp;|&nbsp; 💸 <b>{_c_str}</b></span>'
            f'<span style="color:{_est_color};font-size:0.82rem;font-weight:700">{_scenario}</span>'
            f'</div>'
            f'<div style="color:#64748b;font-size:0.78rem;margin-top:3px">{_hint}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

    with st.expander("🎯 Фокус анализа (необязательно)", expanded=False):
        st.caption("Помогает AI расставить приоритеты")
        _fa1, _fa2, _fa3 = st.columns(3)
        with _fa1:
            goal = st.radio("🎯 Цель", [
                "Полный аудит", "Поднять конверсию",
                "Выйти в топ поиска", "Победить конкурента",
            ], key="goal_sel")
        with _fa2:
            audience = st.radio("👥 Аудитория", [
                "Не указано", "Спортсмены", "Outdoor / туризм",
                "Everyday / офис", "Business casual",
            ], key="aud_sel")
        with _fa3:
            positioning = st.radio("💰 Позиционирование", [
                "Не указано", "Бюджет", "Средний сегмент", "Премиум",
            ], key="pos_sel")

        _ctx_parts = [f"Analysis goal: {goal}"]
        if audience != "Не указано":    _ctx_parts.append(f"Target audience: {audience}")
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
                # Save to DB
                try:
                    _od = st.session_state.get("our_data", {})
                    _saved = db_save(get_asin_from_data(_od), result,
                            st.session_state.get("vision",""), _od.get("title",""))
                    log("💾 Сохранено в историю" if _saved else f"⚠️ БД ошибка: {st.session_state.get('_db_save_err','?')}")
                except Exception as _dbe:
                    log(f"⚠️ БД исключение: {_dbe}")
            else:
                existing = st.session_state.get("comp_data_list", [])
                for i, url in enumerate(curr_comp):
                    if url.strip() and url.strip() != prev_comp[i].strip():
                        casin = get_asin(url)
                        if casin:
                            log(f"➕ Новый конкурент {i+1}: {casin}...")
                            cdata, _ = scrapingdog_product(casin, log)
                            if i < len(existing): existing[i] = cdata
                            else: existing.append(cdata)
                            st.session_state[f"c{i}_saved"] = url.strip()
                st.session_state["comp_data_list"] = existing
                log("🧠 Обновляю сравнительный анализ...")
                od_s = st.session_state.get("our_data", {})
                v_s  = st.session_state.get("vision", "")
                asin_s = get_asin(our_url) or "unknown"
                _lang2 = st.session_state.get("analysis_lang","ru")
                result = analyze_text(od_s, existing, v_s, asin_s, log, lang=_lang2)
                st.session_state["result"] = result
                try:
                    _od = st.session_state.get("our_data", {})
                    _saved = db_save(get_asin_from_data(_od), result,
                            st.session_state.get("vision",""), _od.get("title",""))
                    log("💾 Сохранено в историю" if _saved else f"⚠️ БД ошибка")
                except Exception as _dbe:
                    log(f"⚠️ БД исключение: {_dbe}")

            _main_prog.progress(100, text="✅ Анализ завершён!")
            st.rerun()
        except Exception as e:
            st.error(f"Ошибка: {e}")

def page_history():
    st.title("📈 История анализов")

    with st.expander("🔧 Диагностика БД"):
        db_url = st.secrets.get("DATABASE_URL","")
        if not db_url:
            st.error("❌ DATABASE_URL не найден в Secrets")
        else:
            st.success(f"✅ DATABASE_URL найден ({db_url[:30]}...)")
            try:
                _tc = get_db()
                if _tc:
                    _cur = _tc.cursor()
                    _cur.execute("SELECT COUNT(*) FROM listing_analysis")
                    cnt = _cur.fetchone()[0]
                    _tc.close()
                    st.success(f"✅ Подключение ОК | Записей в таблице: {cnt}")
                    _tc2 = get_db()
                    if _tc2:
                        _cur2 = _tc2.cursor()
                        _cur2.execute("SELECT asin, our_title, overall_score, analyzed_at FROM listing_analysis ORDER BY analyzed_at DESC LIMIT 5")
                        _rows2 = _cur2.fetchall()
                        _tc2.close()
                        for _r in _rows2:
                            st.code(f"asin={_r[0]} | title={str(_r[1])[:40]} | score={_r[2]} | date={_r[3]}")
                else:
                    st.error("❌ get_db() вернул None")
            except Exception as e:
                st.error(f"❌ Ошибка БД: {e}")

    all_asins = db_all_asins()
    if not all_asins:
        st.info("История пуста — запусти первый анализ")
        return

    asin_opts = [f"{"🔵" if a.get("type","наш")=="наш" else "🔴"} {a['asin']} — {(a['title'] or '')[:40]}" for a in all_asins]
    sel = st.selectbox("ASIN", asin_opts)
    sel_asin = sel.split(" — ")[0].strip().lstrip("🔵🔴 ")

    history = db_history(sel_asin, limit=20)
    if not history:
        st.warning("Нет данных для этого ASIN")
        return

    latest = history[0]
    st.subheader(f"Последний анализ: {latest['date'].strftime('%d.%m.%Y %H:%M')}")

    cols = st.columns(4)
    metrics = [("Overall", "overall"), ("Title", "title"), ("Bullets", "bullets"), ("Images", "images")]
    for col, (label, key) in zip(cols, metrics):
        val = latest[key] or 0
        delta = None
        if len(history) > 1:
            prev = history[1][key] or 0
            delta = f"{val-prev:+d}%" if val != prev else None
        col.metric(label, f"{val}%", delta=delta)

    cols2 = st.columns(3)
    for col, (label, key) in zip(cols2, [("A+","aplus"),("COSMO","cosmo"),("Rufus","rufus")]):
        val = latest[key] or 0
        delta = None
        if len(history) > 1:
            prev = history[1][key] or 0
            delta = f"{val-prev:+d}%" if val != prev else None
        col.metric(label, f"{val}%", delta=delta)

    if len(history) > 1:
        st.divider()
        st.subheader("📊 Динамика Overall Score")
        import pandas as pd
        _hist_rev = list(reversed(history))
        dates  = [h["date"].strftime("%d.%m %H:%M") for h in _hist_rev]
        scores = [h["overall"] or 0 for h in _hist_rev]
        df_chart = pd.DataFrame({"Overall %": scores}, index=dates)
        if len(scores) <= 3:
            st.bar_chart(df_chart, color="#3b82f6")
        else:
            st.line_chart(df_chart)

    st.divider()
    amz_url = f"https://www.amazon.com/dp/{sel_asin}"
    st.markdown(f"🔗 [Открыть листинг на Amazon ↗]({amz_url})")

    st.subheader("Все запуски")
    import pandas as pd
    df = pd.DataFrame([{
        "Дата": h["date"].strftime("%d.%m.%Y %H:%M"),
        "Overall": h["overall"], "Title": h["title"], "Bullets": h["bullets"],
        "Images": h["images"], "A+": h["aplus"], "COSMO": h["cosmo"], "Rufus": h["rufus"],
    } for h in history])
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🔍 Загрузить полный анализ из истории")
    hist_opts = [f"{h['date'].strftime('%d.%m.%Y %H:%M')} — Overall: {h['overall']}%" for h in history]
    sel_hist = st.selectbox("Выбери запуск", hist_opts, key="hist_sel")
    sel_hist_idx = hist_opts.index(sel_hist)

    if st.button("📂 Открыть этот анализ", type="primary", use_container_width=True):
        conn_h = get_db()
        if conn_h:
            try:
                cur_h = conn_h.cursor()
                try:
                    cur_h.execute("ALTER TABLE listing_analysis ADD COLUMN IF NOT EXISTS competitors_json TEXT")
                    conn_h.commit()
                except Exception: pass
                cur_h.execute("""
                    SELECT result_json, vision_text, competitors_json
                    FROM listing_analysis
                    WHERE asin = %s
                    ORDER BY analyzed_at DESC
                    LIMIT %s
                """, (sel_asin, len(history)))
                rows_h = cur_h.fetchall()
                conn_h.close()
                row_h = rows_h[sel_hist_idx]
                st.session_state["result"]  = json.loads(row_h[0]) if row_h[0] else {}
                st.session_state["vision"]  = row_h[1] or ""
                st.session_state["images"]  = []
                if row_h[2]:
                    comps_h = json.loads(row_h[2])
                    for _ci, _ch in enumerate(comps_h):
                        st.session_state[f"comp_ai_{_ci}"] = {"overall_score": f"{_ch.get('overall',0)}%"}
                st.session_state["_hist_loaded"] = sel_hist
                st.session_state["page"] = "🏠 Обзор"
                st.success(f"✅ Загружен анализ от {sel_hist}")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Ошибка: {e}")

    if st.session_state.get("_hist_loaded"):
        _hl = st.session_state["_hist_loaded"]
        st.info(f"📅 Просматриваешь: {_hl}")
        if st.button("↩️ Вернуться к текущему анализу", type="primary", use_container_width=True):
            st.session_state.pop("_hist_loaded", None)
            st.session_state["page"] = "🏠 Обзор"
            st.rerun()

    if history:
        conn2 = get_db()
        if conn2:
            try:
                cur2 = conn2.cursor()
                cur2.execute("SELECT competitors_json FROM listing_analysis WHERE asin=%s ORDER BY analyzed_at DESC LIMIT 1", (sel_asin,))
                row = cur2.fetchone()
                conn2.close()
                if row and row[0]:
                    comps = json.loads(row[0])
                    if comps:
                        st.divider()
                        st.subheader("🔴 Конкуренты на момент последнего анализа")
                        import pandas as pd
                        cdf = pd.DataFrame([{
                            "ASIN": c.get("asin",""), "Title": c.get("title",""),
                            "Overall": c.get("overall",0), "Цена": c.get("price",""),
                            "Рейтинг": c.get("rating",""), "Отзывы": c.get("reviews",""),
                        } for c in comps])
                        st.dataframe(cdf, use_container_width=True, hide_index=True)
            except Exception:
                pass

# Init DB on startup
db_init()

# ── Handle sidebar refresh trigger ────────────────────────────────────────────
if st.session_state.pop("_trigger_rerun", False):
    _saved_url = st.session_state.get("our_url_saved","")
    _saved_comps = [st.session_state.get(f"c{i}_saved","") for i in range(5)]
    if _saved_url:
        _ph_refresh = st.empty()
        _lines_r = []
        def _log_r(msg): _lines_r.append(msg); _ph_refresh.markdown("\n\n".join(_lines_r[-8:]))
        _prog_r = st.progress(0, text="🔄 Обновляю анализ...")
        try:
            _r2, _v2 = run_analysis(_saved_url, _saved_comps, _log_r, prog=_prog_r)
            st.session_state.update({"result": _r2, "vision": _v2})
            st.session_state["page"] = "🏠 Обзор"
            _prog_r.empty(); _ph_refresh.empty()
            st.rerun()
        except Exception as _e:
            st.error(f"Ошибка: {_e}")
            st.stop()

# ── Pages ─────────────────────────────────────────────────────────────────────
page = st.session_state.get("page", "🏠 Обзор")
if page == "📈 История": page_history(); st.stop()
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
• Используй <b>⚡ Оптимизацию токенов</b> для быстрых ретестов<br>
• Отключи Vision — анализ ускорится в 3-4х<br>
• <b>English</b> — для листингов на .com рынке<br>
• Заполни <b>Фокус анализа</b> для точных рек.<br>
• Кнопка <b>🗑️ Сброс</b> — полная очистка данных
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
# PDF REPORT GENERATOR
# ══════════════════════════════════════════════════════════════════════════════
def generate_pdf_report(result, our_data, vision_text, images, asin, comp_data=None):
    import io, base64, re as _re
    from datetime import datetime
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
        Table, TableStyle, HRFlowable, PageBreak, Image as RLImage)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from PIL import Image as PILImage

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=20*mm, bottomMargin=20*mm)

    W = A4[0] - 40*mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    import os, tempfile

    def _get_font(name, url):
        cache = os.path.join(tempfile.gettempdir(), name)
        if not os.path.exists(cache):
            try:
                r = requests.get(url, timeout=20)
                if r.ok: open(cache, "wb").write(r.content)
            except: return None
        return cache if os.path.exists(cache) else None

    _SOURCES = [
        ("NotoSans-Regular.ttf", "https://cdn.jsdelivr.net/gh/googlefonts/noto-fonts@main/hinted/ttf/NotoSans/NotoSans-Regular.ttf"),
        ("NotoSans-Regular.ttf", "https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf"),
    ]
    _SOURCES_BOLD = [
        ("NotoSans-Bold.ttf", "https://cdn.jsdelivr.net/gh/googlefonts/noto-fonts@main/hinted/ttf/NotoSans/NotoSans-Bold.ttf"),
        ("NotoSans-Bold.ttf", "https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Bold.ttf"),
    ]

    def _try_load(font_name, sources):
        _sys = "/usr/share/fonts/truetype/dejavu/DejaVuSans" + ("-Bold" if "Bold" in font_name else "") + ".ttf"
        if os.path.exists(_sys):
            try:
                pdfmetrics.registerFont(TTFont(font_name, _sys))
                return True
            except: pass
        for _fname, _url in sources:
            _p = _get_font(_fname, _url)
            if _p:
                try:
                    pdfmetrics.registerFont(TTFont(font_name, _p))
                    return True
                except: pass
        return False

    _ok_r = _try_load("DV",      _SOURCES)
    _ok_b = _try_load("DV-Bold", _SOURCES_BOLD)
    if _ok_r and _ok_b:
        _F, _FB = "DV", "DV-Bold"
    else:
        _F, _FB = "Helvetica", "Helvetica-Bold"

    S = {
        "title":  ParagraphStyle("t",  fontSize=24, fontName=_FB,  textColor=colors.HexColor("#0f172a"), spaceAfter=4),
        "h1":     ParagraphStyle("h1", fontSize=16, fontName=_FB,  textColor=colors.HexColor("#1e293b"), spaceBefore=12, spaceAfter=4),
        "h2":     ParagraphStyle("h2", fontSize=13, fontName=_FB,  textColor=colors.HexColor("#334155"), spaceBefore=8,  spaceAfter=3),
        "body":   ParagraphStyle("b",  fontSize=9,  fontName=_F,   textColor=colors.HexColor("#475569"), spaceAfter=3, leading=14),
        "small":  ParagraphStyle("s",  fontSize=8,  fontName=_F,   textColor=colors.HexColor("#64748b"), spaceAfter=2),
        "green":  ParagraphStyle("g",  fontSize=9,  fontName=_F,   textColor=colors.HexColor("#15803d"), spaceAfter=2),
        "orange": ParagraphStyle("o",  fontSize=9,  fontName=_F,   textColor=colors.HexColor("#d97706"), spaceAfter=2),
        "center": ParagraphStyle("c",  fontSize=9,  fontName=_F,   alignment=TA_CENTER, spaceAfter=2),
        "action": ParagraphStyle("a",  fontSize=9,  fontName=_FB,  textColor=colors.HexColor("#1d4ed8"), spaceAfter=2),
    }

    def score_color(s):
        if s >= 75: return colors.HexColor("#15803d")
        if s >= 50: return colors.HexColor("#d97706")
        return colors.HexColor("#dc2626")

    def hex_str(c):
        return '#{:02x}{:02x}{:02x}'.format(int(c.red*255), int(c.green*255), int(c.blue*255))

    def score_label(s):
        if s >= 75: return "Хорошо"
        if s >= 50: return "Требует улучшений"
        return "Критично"

    story = []

    title_val = our_data.get("title", asin)[:80]
    price     = our_data.get("price", "")
    rating    = our_data.get("average_rating", "")
    reviews   = our_data.get("total_reviews", "")
    date_str  = datetime.now().strftime("%d.%m.%Y %H:%M")
    ov_pct    = pct(result.get("overall_score", 0))
    ov_col    = score_color(ov_pct)
    _asin_val = asin or our_data.get("parent_asin","") or "—"

    story.append(Spacer(1, 10*mm))
    story.append(Paragraph("Amazon Listing Audit", S["title"]))
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width=W, thickness=2, color=colors.HexColor("#3b82f6")))
    story.append(Spacer(1, 5*mm))

    _asin_link_p = Paragraph(
        f'<link href="https://www.amazon.com/dp/{_asin_val}" color="#1d4ed8"><b>{_asin_val}</b></link>',
        S["body"])
    cover_data = [
        ["ASIN", _asin_link_p, "Дата", date_str],
        ["Цена", price, "Рейтинг", f"{rating} ({reviews})"],
        ["Заголовок", Paragraph(title_val, S["body"]), "", ""],
    ]
    cover_tbl = Table(cover_data, colWidths=[25*mm, 65*mm, 25*mm, 55*mm])
    cover_tbl.setStyle(TableStyle([
        ("FONTNAME", (0,0), (-1,-1), _F),
        ("FONTNAME", (0,0), (0,-1), _FB),
        ("FONTNAME", (2,0), (2,-1), _FB),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("BACKGROUND", (0,0), (-1,-1), colors.HexColor("#f8fafc")),
        ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
        ("PADDING", (0,0), (-1,-1), 6),
        ("SPAN", (1,2), (3,2)),
    ]))
    story.append(cover_tbl)
    story.append(Spacer(1, 6*mm))

    ov_tbl = Table([[
        Paragraph(f"<font size=32 color='{hex_str(ov_col)}'><b>{ov_pct}%</b></font>", S["center"]),
        Paragraph(f"<b>Overall Score</b><br/>{score_label(ov_pct)}", S["h2"])
    ]], colWidths=[55*mm, W-55*mm])
    ov_tbl.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BACKGROUND", (0,0), (-1,-1), colors.HexColor("#f1f5f9")),
        ("PADDING", (0,0), (-1,-1), 10),
    ]))
    story.append(ov_tbl)
    story.append(Spacer(1, 6*mm))

    def _get_score(r, key):
        v = r.get(key, 0)
        if isinstance(v, str) and "%" in v: return int(v.replace("%","").strip())
        if isinstance(v, (int,float)): return int(v)
        return 0

    _cosmo_sc = pct(_get_score(result.get("cosmo_analysis",{}), "score")) or _get_score(result, "cosmo_score")
    _rufus_sc = pct(_get_score(result.get("rufus_analysis",{}), "score")) or _get_score(result, "rufus_score")
    score_map = [
        ("Title",       _get_score(result, "title_score")),
        ("Bullets",     _get_score(result, "bullets_score")),
        ("Описание",    _get_score(result, "description_score")),
        ("Фото",        _get_score(result, "images_score")),
        ("A+",          _get_score(result, "aplus_score")),
        ("Отзывы",      _get_score(result, "reviews_score")),
        ("BSR",         _get_score(result, "bsr_score")),
        ("Цена",        _get_score(result, "price_score")),
        ("Варианты",    _get_score(result, "customization_score")),
        ("Prime",       _get_score(result, "prime_score")),
        ("COSMO",       _cosmo_sc),
        ("Rufus",       _rufus_sc),
    ]
    def _sc(raw):
        v = pct(raw)
        c = score_color(v)
        return Paragraph(f"<font color='{hex_str(c)}'><b>{v}%</b></font>", S["center"])

    score_rows = [["Метрика", "Оценка", "Метрика", "Оценка"]]
    pairs = [(score_map[i], score_map[i+1] if i+1 < len(score_map) else None)
             for i in range(0, len(score_map), 2)]
    for left, right in pairs:
        row = [left[0], _sc(left[1])]
        if right: row += [right[0], _sc(right[1])]
        else:     row += ["", ""]
        score_rows.append(row)

    sc_tbl = Table(score_rows, colWidths=[40*mm, 25*mm, 40*mm, 25*mm])
    sc_tbl.setStyle(TableStyle([
        ("FONTNAME",   (0,0), (-1,0),  _FB),
        ("FONTNAME",   (0,1), (-1,-1), _F),
        ("FONTSIZE",   (0,0), (-1,-1), 9),
        ("BACKGROUND", (0,0), (-1,0),  colors.HexColor("#1e293b")),
        ("TEXTCOLOR",  (0,0), (-1,0),  colors.white),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.HexColor("#f8fafc"), colors.white]),
        ("GRID",       (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
        ("PADDING",    (0,0), (-1,-1), 6),
        ("ALIGN",      (1,0), (1,-1), "CENTER"),
        ("ALIGN",      (3,0), (3,-1), "CENTER"),
    ]))
    story.append(sc_tbl)
    story.append(Spacer(1, 4*mm))

    actions = result.get("priority_improvements", []) or [
        a.get("action","") for a in result.get("actions",[]) if isinstance(a, dict)]
    if actions:
        story.append(Paragraph("Приоритетные действия", S["h2"]))
        for i, a in enumerate(actions[:6]):
            story.append(Paragraph(f"{i+1}. {a}", S["action"]))

    story.append(PageBreak())

    # Photos page
    story.append(Paragraph("Анализ фотографий", S["h1"]))
    story.append(HRFlowable(width=W, thickness=1, color=colors.HexColor("#e2e8f0")))
    story.append(Spacer(1, 3*mm))

    _clean = lambda s: _re.sub(r"\*+", "", s).strip()

    if vision_text and images:
        _all_blocks = {}
        for _m in _re.finditer(r"PHOTO_BLOCK_(\d+)\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", vision_text, _re.DOTALL):
            _all_blocks[int(_m.group(1))] = _m.group(2).strip()
        for i, img_d in enumerate(images[:5]):
            blk = _all_blocks.get(i+1, "")
            typ_m  = _re.search(r"(?:Тип|Type)\s*[:\-]\s*(.+)", blk)
            sc_m   = _re.search(r"(?:Оценка|Score)\s*[:\-]\s*(\d+)", blk)
            str_m  = _re.search(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.+)", blk)
            weak_m = _re.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.+)", blk)
            act_m  = _re.search(r"(?:Действие|Action)\s*[:\-]\s*(.+)", blk)
            sc_val  = int(sc_m.group(1)) if sc_m else 0
            sc_col  = colors.HexColor("#15803d") if sc_val>=8 else (colors.HexColor("#d97706") if sc_val>=6 else colors.HexColor("#dc2626"))
            sc_lbl  = "Отлично" if sc_val>=8 else ("Хорошо" if sc_val>=6 else "Слабо")
            try:
                _b64_data = img_d.get("b64","") if isinstance(img_d, dict) else img_d
                img_bytes = base64.b64decode(_b64_data)
                pil_img   = PILImage.open(io.BytesIO(img_bytes))
                pil_img.thumbnail((200, 200))
                thumb_buf = io.BytesIO()
                pil_img.save(thumb_buf, format="JPEG", quality=70)
                thumb_buf.seek(0)
                rl_img = RLImage(thumb_buf, width=35*mm, height=35*mm)
            except:
                rl_img = Paragraph(f"(фото {i+1})", S["small"])

            info_content = [
                Paragraph(f"<b>Фото #{i+1}</b> — {_clean(typ_m.group(1)) if typ_m else ''}", S["h2"]),
                Paragraph(f"<font color='{hex_str(sc_col)}'><b>{sc_val}/10</b></font>  {sc_lbl}", S["body"]),
            ]
            if str_m:  info_content.append(Paragraph(f"+ {_clean(str_m.group(1))}", S["green"]))
            if weak_m: info_content.append(Paragraph(f"! {_clean(weak_m.group(1))}", S["orange"]))
            if act_m:  info_content.append(Paragraph(f"> {_clean(act_m.group(1))}", S["action"]))
            if not str_m and blk:
                info_content.append(Paragraph(blk[:300], S["small"]))

            from reportlab.platypus import KeepTogether
            row_tbl = Table([[rl_img, info_content]], colWidths=[40*mm, W-40*mm])
            row_tbl.setStyle(TableStyle([
                ("VALIGN",     (0,0), (-1,-1), "TOP"),
                ("BACKGROUND", (0,0), (-1,-1), colors.HexColor("#f8fafc")),
                ("GRID",       (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
                ("PADDING",    (0,0), (-1,-1), 6),
            ]))
            story.append(KeepTogether([row_tbl, Spacer(1, 3*mm)]))

    story.append(PageBreak())

    # Content page
    story.append(Paragraph("Анализ контента", S["h1"]))
    story.append(HRFlowable(width=W, thickness=1, color=colors.HexColor("#e2e8f0")))
    story.append(Spacer(1, 3*mm))

    for sec_key, sec_score_key, sec_name in [
        ("title",       "title_score",       "Заголовок"),
        ("bullets",     "bullets_score",     "Bullets"),
        ("description", "description_score", "Description"),
        ("aplus",       "aplus_score",       "A+ Контент"),
    ]:
        sc_v = pct(result.get(sec_score_key, 0))
        sc_c = score_color(sc_v)
        story.append(Paragraph(
            f"{sec_name}  <font color='{hex_str(sc_c)}'><b>{sc_v}%</b></font>", S["h2"]))
        sec = result.get(sec_key, {})
        gaps = sec.get("gaps",[]) if isinstance(sec, dict) else result.get(f"{sec_key}_gaps",[])
        rec  = sec.get("recommendation","") if isinstance(sec, dict) else result.get(f"{sec_key}_rec","")
        for g in gaps[:3]:
            story.append(Paragraph(f"! {g}", S["orange"]))
        if rec:
            story.append(Paragraph(f"> {rec}", S["action"]))
        story.append(Spacer(1, 2*mm))

    story.append(Spacer(1, 6*mm))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#cbd5e1")))
    story.append(Paragraph(f"Сгенерировано: {date_str} | ASIN: {asin} | Amazon Listing Analyzer", S["small"]))

    doc.build(story)
    buf.seek(0)
    return buf.read()


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
        for item in priority_improvements:
            with st.container(border=True):
                st.markdown(f"**{item}**")
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

    st.divider()
    st.subheader("📥 Скачать PDF отчёт")
    _pdf_col1, _pdf_col2 = st.columns([2,4])
    with _pdf_col1:
        if st.button("📄 Сгенерировать PDF", type="primary", use_container_width=True):
            with st.spinner("Генерирую PDF отчёт..."):
                try:
                    _pdf_bytes = generate_pdf_report(
                        result=r, our_data=od,
                        vision_text=st.session_state.get("vision",""),
                        images=st.session_state.get("images",[]),
                        asin=od.get("parent_asin","") or od.get("asin",""),
                        comp_data=st.session_state.get("comp_data_list",[])
                    )
                    st.session_state["_pdf_bytes"] = _pdf_bytes
                    st.success("✅ PDF готов — нажми скачать")
                except Exception as _pe:
                    st.error(f"Ошибка PDF: {_pe}")
    with _pdf_col2:
        if st.session_state.get("_pdf_bytes"):
            _asin_dl = od.get("parent_asin","") or od.get("asin","listing")
            _date_dl = __import__("datetime").datetime.now().strftime("%Y%m%d")
            st.download_button(
                label="⬇️ Скачать PDF",
                data=st.session_state["_pdf_bytes"],
                file_name=f"amazon_audit_{_asin_dl}_{_date_dl}.pdf",
                mime="application/pdf",
                use_container_width=True
            )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Фото
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📸 Фото":
    st.title("📸 Vision анализ фотографий")

    _all_blocks = re.split(r"PHOTO_BLOCK_\d+", v) if v else []
    blocks = [b.strip() for b in _all_blocks if b.strip() and re.search(r"\d+/10", b)]
    if not blocks:
        blocks = [b.strip() for b in _all_blocks if b.strip()]

    if not imgs and blocks:
        st.info("📅 История: фото не сохраняются в БД — показан текстовый анализ Vision")
        for i, text in enumerate(blocks):
            sm = re.search(r"(\d+)/10", text)
            score = int(sm.group(1)) if sm else 0
            bc = "#22c55e" if score>=8 else ("#f59e0b" if score>=6 else "#ef4444")
            slbl = "Отлично" if score>=8 else ("Хорошо" if score>=6 else "Слабо")
            typ  = re.search(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)", text)
            strg = re.search(r"(?:[Сс]ильная\s+сторона|Strength|(?<!\w)✅)\s*[:\-]?\s*(.{3,})", text)
            weak = re.search(r"(?:[Сс]лабость|Weakness|(?<!\w)⚠️)\s*[:\-]?\s*(.{3,})", text)
            actn = re.search(r"(?:[Дд]ействие|Action)\s*[:\-]?\s*(.{3,})", text)
            _strip = lambda s: s.strip().strip("*").strip()
            ptype = _strip(typ.group(1)) if typ else ""
            stxt  = _strip(strg.group(1)) if strg else ""
            wtxt  = _strip(weak.group(1)) if weak else ""
            atxt  = _strip(actn.group(1)) if actn else ""
            if wtxt and any(x in wtxt.lower() for x in ["none","n/a","no weakness","нет слабостей"]):
                wtxt = ""
            with st.container(border=True):
                _head = f"Фото #{i+1}" + (f" — {ptype}" if ptype else "")
                st.markdown(f"**{_head}**")
                if score > 0:
                    st.markdown(f'<span style="font-size:2rem;font-weight:800;color:{bc}">{score}/10</span> <span style="color:{bc}">{slbl}</span>', unsafe_allow_html=True)
                    st.markdown(f'<div style="background:#e5e7eb;border-radius:4px;height:8px"><div style="background:{bc};width:{score*10}%;height:8px;border-radius:4px"></div></div>', unsafe_allow_html=True)
                if stxt: st.success(f"✅ {stxt}")
                if wtxt: st.warning(f"⚠️ {wtxt}")
                if score > 0 and score < 8 and (atxt or wtxt):
                    with st.expander("🛠 Что делать"):
                        st.markdown(f"→ {atxt or wtxt}")
        st.stop()

    if not imgs:
        if not st.session_state.get("do_vision", True):
            st.info("👁️ Vision фото был отключён при анализе — запусти повторно с включённым чекбоксом")
        else:
            st.warning("Фото не загружены")
        st.stop()

    for i, img in enumerate(imgs):
        text = blocks[i] if i < len(blocks) else ""
        sm   = re.search(r"(\d+)/10", text)
        score = int(sm.group(1)) if sm else 0
        bc    = "#22c55e" if score>=8 else ("#f59e0b" if score>=6 else "#ef4444")
        slbl  = "Отлично" if score>=8 else ("Хорошо" if score>=6 else "Слабо")
        typ  = re.search(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)", text)
        strg = re.search(r"(?:[Сс]ильная\s+сторона|Strength|(?<!\w)✅)\s*[:\-]?\s*(.{3,})", text)
        weak = re.search(r"(?:[Сс]лабость|Weakness|(?<!\w)⚠️)\s*[:\-]?\s*(.{3,})", text)
        actn = re.search(r"(?:[Дд]ействие|Action)\s*[:\-]?\s*(.{3,})", text)
        _strip = lambda s: s.strip().strip("*").strip()
        ptype = _strip(typ.group(1)) if typ else ""
        stxt  = _strip(strg.group(1)) if strg else ""
        wtxt  = _strip(weak.group(1)) if weak else ""
        atxt  = _strip(actn.group(1)) if actn else ""
        if wtxt and any(x in wtxt.lower() for x in ["none", "n/a", "no weakness", "нет слабостей"]):
            wtxt = ""

        with st.container(border=True):
            c1,c2 = st.columns([1,2])
            with c1:
                st.image(__import__("base64").b64decode(img["b64"]), use_container_width=True)
            with c2:
                _head = f"Фото #{i+1}" + (f" — {ptype}" if ptype else "")
                st.markdown(f"**{_head}**")
                if score > 0:
                    st.markdown(f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0"><div style="font-size:2rem;font-weight:800;color:{bc}">{score}/10</div><div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px"><div style="background:{bc};width:{score*10}%;height:10px;border-radius:6px"></div></div><div style="color:{bc};font-size:0.8rem;margin-top:2px">{slbl}</div></div></div>', unsafe_allow_html=True)
                else:
                    st.warning("⚠️ Оценка не распознана")
                if stxt: st.success(f"✅ {stxt}")
                if wtxt: st.warning(f"⚠️ {wtxt}")
                if score > 0 and score < 8 and (atxt or wtxt):
                    with st.expander("🛠 Что делать"):
                        st.markdown(f"→ {atxt or wtxt}")
                        if score <= 5 and i == 0:
                            st.error("🔴 Приоритет ВЫСОКИЙ — риск suppression листинга Amazon")
                if not stxt and text:
                    with st.expander("🔧 Raw (Strength не распознан)"):
                        st.code(text[:400])

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: A+ Контент
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🎨 A+ Контент":
    st.title("🎨 A+ Контент")
    _av = st.session_state.get("aplus_vision","")
    _av_urls = st.session_state.get("aplus_img_urls", od.get("aplus_image_urls",[]))

    if not _av and not _av_urls:
        if od.get("aplus"):
            st.warning("⚠️ A+ баннеры не проанализированы. Нажми **🔄 Обновить анализ** в меню слева.")
        else:
            st.info("ℹ️ У этого листинга нет A+ контента.")
    elif _av_urls and not _av:
        # Картинки есть, анализ отключён — показываем только картинки
        st.info("👁️ Vision A+ был отключён — показаны баннеры без AI-анализа. Включи чекбокс и перезапусти.")
        for _bi, _url in enumerate(_av_urls[:8]):
            st.image(_url, caption=f"A+ баннер #{_bi+1}", use_container_width=True)
    else:
        _av_total = pct(r.get("aplus_score", 0))
        if _av_total:
            st.metric("A+ Score", f"{_av_total}%")

        _av_blocks = []
        for _m in re.finditer(r"APLUS_BLOCK_\d+\s*(.*?)(?=APLUS_BLOCK_\d+|$)", _av, re.DOTALL):
            _blk = _m.group(1).strip()
            if _blk:
                _av_blocks.append(_blk)
        # Убираем мусор из начала каждого блока (заголовки AI типа "# АНАЛИЗ A+ CONTENT ---")
        _clean_blocks = []
        for _blk in _av_blocks:
            _lines = _blk.split("\n")
            _lines = [l for l in _lines if not re.match(r"^#+\s|^---", l.strip())]
            _clean_blocks.append("\n".join(_lines).strip())
        _av_blocks = [b for b in _clean_blocks if b]
        st.markdown(f"**{len(_av_blocks)} баннер(ов) проанализировано**")
        st.divider()

        for _bi, _block in enumerate(_av_blocks):
            _av_score_m = re.search(r"(?:Оценка|Score)\s*[:\-]?\s*(\d+)", _block)
            _av_score = int(_av_score_m.group(1)) if _av_score_m else 0
            _av_mod_m = re.search(r"(?:Модуль|Module)\s*[:\-]?\s*(.+)", _block)
            _av_sum_m = re.search(r"(?:Содержание|Summary|Content)\s*[:\-]?\s*(.+)", _block)
            _av_str_m = re.search(r"(?:Сильная сторона|Strength)\s*[:\-]?\s*(.{3,})", _block)
            _av_weak_m = re.search(r"(?:Слабость|Weakness)\s*[:\-]?\s*(.{3,})", _block)
            _av_act_m = re.search(r"(?:Действие|Action)\s*[:\-]?\s*(.{3,})", _block)
            _av_mod  = _av_mod_m.group(1).strip() if _av_mod_m else ""
            _av_sum  = _av_sum_m.group(1).strip() if _av_sum_m else ""
            _av_str  = _av_str_m.group(1).strip() if _av_str_m else ""
            _av_weak = _av_weak_m.group(1).strip() if _av_weak_m else ""
            _av_act  = _av_act_m.group(1).strip() if _av_act_m else ""
            _av_bc = "#22c55e" if _av_score>=8 else ("#f59e0b" if _av_score>=6 else "#ef4444")
            _av_sl = "Отлично" if _av_score>=8 else ("Хорошо" if _av_score>=6 else "Слабо")

            with st.container(border=True):
                if _av_urls and _bi < len(_av_urls):
                    st.image(_av_urls[_bi], use_container_width=True)
                _av_head = f"Баннер #{_bi+1}" + (f" — {_av_mod}" if _av_mod else "")
                st.markdown(f"**{_av_head}**")
                if _av_sum:
                    st.markdown(f"_{_av_sum}_")
                elif _block:
                    _raw_lines = [l.strip() for l in _block.split("\n") if l.strip() and not re.match(r"^#+|^---", l)][:2]
                    if _raw_lines: st.markdown(f"_{' '.join(_raw_lines)}_")
                if _av_score:
                    st.markdown(
                        f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0">' +
                        f'<div style="font-size:2.5rem;font-weight:800;color:{_av_bc}">{_av_score}/10</div>' +
                        f'<div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px">' +
                        f'<div style="background:{_av_bc};width:{_av_score*10}%;height:10px;border-radius:6px"></div></div>' +
                        f'<div style="color:{_av_bc};font-size:0.85rem;margin-top:3px">{_av_sl}</div></div></div>',
                        unsafe_allow_html=True)
                if _av_str:  st.success(f"✅ {_av_str}")
                if _av_weak: st.warning(f"⚠️ {_av_weak}")
                if _av_act:
                    with st.expander("🛠 Что делать"):
                        st.markdown(f"→ {_av_act}")

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
        _real_gaps = [g for g in gaps if g and str(g).strip()] if isinstance(gaps, list) else []
        if _real_gaps:
            with st.expander(f"⚠️ ({len(_real_gaps)})"):
                for g in _real_gaps: st.markdown(f"- {g}")
        if rec: st.info(f"💡 {rec}")

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
    _ai_title_score = pct(r.get("title_score", 0))
    _auto_title_score = sum(int(_ok) * int(_wt.strip("%")) for _,_wt,_ok,_ in _title_rubric)
    with st.expander("📐 Рубрика оценки Title"):
        _sc1, _sc2 = st.columns(2)
        _sc1.metric("🤖 AI оценка", f"{_ai_title_score}%")
        _sc2.metric("🔧 Авто-проверка", f"{_auto_title_score}%",
                    delta=f"{_ai_title_score - _auto_title_score:+d}%" if _ai_title_score != _auto_title_score else None,
                    delta_color="normal")

    st.divider()
    bullets_text = "\n".join([f"• {b}" for b in our_bullets]) if our_bullets else ""
    _sec("Bullets", "bullets_score", raw_text=bullets_text)
    st.divider()
    _sec("Description", "description_score", raw_text=str(our_desc)[:400] if our_desc else "")
    st.divider()
    _sec("A+", "aplus_score")

    _av_check = st.session_state.get("aplus_vision","")
    if _av_check:
        st.info("🎨 Визуальный анализ A+ баннеров → перейди в раздел **A+ Контент** в меню слева")
    elif od.get("aplus"):
        st.info("🎨 A+ есть, но баннеры не загружены. Перезапусти анализ.")

    st.divider()
    _sec("Фото", "images_score")

    ib = r.get("images_breakdown", {})
    if ib:
        st.subheader("📸 Детализация фото")
        for k,v2 in ib.items():
            st.markdown(f"**{k}:** {v2}")

    if r.get("tech_params"):
        st.divider()
        st.subheader("⚙️ Технические параметры")
        for p2 in r["tech_params"]:
            with st.container(border=True):
                st.markdown(f"**{p2.get('param','')}**")
                x1,x2 = st.columns(2)
                x1.caption(f"🏆 Конкуренты: {p2.get('competitor_value','')}"); x2.caption(f"→ Наш пробел: {p2.get('our_gap','')}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Benchmark
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🏆 Benchmark":
    st.title("🏆 Benchmark — Сравнение с конкурентами")

    if not cd:
        st.info("Добавь конкурентов в форму выше и запусти анализ повторно")
        st.stop()

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
        colors2 = len(d.get("customization_options",{}).get("color",[]))
        sizes2  = len(d.get("customization_options",{}).get("size",[]))
        ts = min(10, max(0, (1.5 if len(title2)<=125 else 0) + (3.5 if any(k in title2.lower() for k in ["merino","wool","shirt","base layer","tank"]) else 1.5) + 3.0 + (1.0 if not re.search(r"[!$?{}]",title2) else 0) + 1))
        bs = min(10, max(0, (1.5 if len(bul2)<=5 else 0) + (2.5 if any(":" in b for b in bul2) else 1.0) + min(4.0,len(bul2)) + 1.0 + 1))
        ds = 0 if not desc2 else min(10, 4+(3 if len(desc2)>200 else 1))
        ps = min(10, max(0, (4.0 if len(imgs2)>=6 else len(imgs2)*0.6)+(2.0 if has_vid else 0)+(4.0 if len(imgs2)>=6 else 0)))
        as_ = 0 if not has_ap else 7
        rs = 10 if (rating2>=4.4 and rev_cnt>=50) else (7 if rating2>=4.0 else 4)
        bsrs = 10 if bsr_num<=1000 else (8 if bsr_num<=5000 else 5)
        prs = 10 if is_prime else 5
        vs = 10 if (colors2>=5 and sizes2>=3) else (7 if sizes2>=3 else 4)
        h = int((ts*0.10+bs*0.10+ds*0.10+ps*0.10+as_*0.10+rs*0.15+bsrs*0.15+7*0.10+vs*0.05+prs*0.05)*10)
        return {"title":round(ts,1),"bullets":round(bs,1),"description":round(ds,1),"photos":round(ps,1),"aplus":as_,"reviews":rs,"bsr":bsrs,"variants":vs,"prime":prs,"health":h}

    our_scores = {
        "title":       pct(r.get("title_score",0)),
        "bullets":     pct(r.get("bullets_score",0)),
        "description": pct(r.get("description_score",0)),
        "photos":      pct(r.get("images_score",0)),
        "aplus":       pct(r.get("aplus_score",0)),
        "reviews":     pct(r.get("reviews_score",0)),
        "bsr":         pct(r.get("bsr_score",0)),
        "variants":    pct(r.get("customization_score",0)),
        "prime":       pct(r.get("prime_score",0)),
        "health":      pct(r.get("overall_score",0)),
    }
    def get_comp_scores(c, i):
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
    asin_labels = ["🔵 НАШ"] + [f"🔴 {get_asin_from_data(c) or f'Конк.{i+1}'}" for i,c in enumerate(cd)]

    total_scores = []
    for sc2 in all_scores:
        if sc2.get("health", 0) > 0:
            total_scores.append(sc2["health"])
        else:
            keys = ["title","bullets","description","photos","aplus","reviews","bsr","variants","prime"]
            w    = [0.10,0.10,0.10,0.10,0.10,0.15,0.15,0.05,0.05]
            total = sum(sc2.get(k,0)*wi for k,wi in zip(keys,w))
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
    score_rows = [
        ("🏷️ Title","title",100),("📋 Bullets","bullets",100),
        ("📄 Описание","description",100),("📸 Фото","photos",100),
        ("✨ A+","aplus",100),("⭐ Отзывы","reviews",100),
        ("📊 BSR","bsr",100),("🎨 Варианты","variants",100),
        ("🚀 Prime","prime",100),("💯 Overall","health",100),
    ]
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
        st.markdown(f'<div style="text-align:center;padding:16px;background:#f8fafc;border-radius:12px"><div style="font-size:2.5rem;font-weight:800;color:{cc}">{cosmo}%</div><div style="color:{cc};font-weight:600">COSMO Score</div></div>', unsafe_allow_html=True)
    with ccc2:
        st.markdown(f'<div style="text-align:center;padding:16px;background:#f8fafc;border-radius:12px"><div style="font-size:2.5rem;font-weight:800;color:{rc2}">{rufus_s}%</div><div style="color:{rc2};font-weight:600">Rufus Score</div></div>', unsafe_allow_html=True)
    st.divider()

    if _ca:
        c_present = _ca.get("signals_present",[])
        c_missing = _ca.get("signals_missing",[])
        if c_present or c_missing:
            st.subheader("📡 COSMO сигналы")
            col_p, col_m = st.columns(2)
            with col_p:
                st.markdown("**✅ Присутствуют**")
                for s2 in c_present: st.success(s2)
            with col_m:
                st.markdown("**❌ Отсутствуют**")
                for s2 in c_missing: st.error(s2)

    st.divider()
    st.subheader("🤖 Rufus Issues")
    if _ra.get("issues"):
        for iss in _ra["issues"]:
            st.warning(f"⚠️ {iss}")

    with st.expander("🔧 Raw JSON"):
        st.json(r)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Конкурент N
# ══════════════════════════════════════════════════════════════════════════════
elif _is_competitor_page:
    idx_m = re.search(r"Конкурент (\d+)", page)
    cidx = int(idx_m.group(1)) - 1 if idx_m else 0
    c = cd[cidx] if cidx < len(cd) else {}

    if not c:
        st.warning("Данные конкурента не найдены"); st.stop()

    cpi   = c.get("product_information", {})
    casin = get_asin_from_data(c)
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

    ch = _h; hc = "#22c55e" if ch>=75 else ("#f59e0b" if ch>=50 else "#ef4444")
    tlen = len(_t2); cprice = c.get("price",""); cbrand = c.get("brand","")
    crating = c.get("average_rating",""); crev = cpi.get("Customer Reviews",{}).get("ratings_count","")
    cbsr_s = str(cpi.get("Best Sellers Rank",""))[:50]

    st.title(f"🔴 Конкурент {cidx+1}")

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#3b1e1e,#5c2626);border-radius:14px;padding:18px;color:white;margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
    <div>
      <div style="font-size:0.78rem;opacity:0.6">{cbrand} - {casin}</div>
      <div style="font-size:0.95rem;font-weight:600;max-width:500px;margin-top:3px">{_t2[:80]}{"..." if tlen>80 else ""}</div>
      <div style="display:flex;gap:12px;margin-top:7px;font-size:0.8rem;opacity:0.8;flex-wrap:wrap">
        <span>Price: {cprice}</span><span>Rating: {crating} ({crev} reviews)</span>
        <span style="color:{'#fca5a5' if tlen>125 else '#86efac'}">{tlen} chars</span>
      </div>
    </div>
    <div style="text-align:center">
      <div style="font-size:2.8rem;font-weight:800;color:{hc}">{ch}%</div>
      <div style="font-size:0.75rem;color:{hc}">Health Score</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

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
                _cimgs_dl = download_images(_cimgs_urls[:5], lambda m: None) if _cimgs_urls else []
                _prog.progress(33, text="👁️ Vision анализ фото...")
                _comp_vision = analyze_vision(_cimgs_dl, c, casin, lambda m: None, lang=_clang) if _cimgs_dl else ""
                _prog.progress(66, text="🧠 AI анализ текста...")
                _cai_result = analyze_text(c, [], _comp_vision, casin, lambda m: None, lang=_clang)
                st.session_state[_cai_key]    = _cai_result
                st.session_state[_vision_key] = (_cimgs_dl, _comp_vision) if _cimgs_dl else None
                _prog.progress(100, text="✅ Готово!")
                st.rerun()

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
                    _cstrip = lambda s: s.strip().strip("*").strip() if s else ""
                    with st.container(border=True):
                        _pc1,_pc2 = st.columns([1,2])
                        with _pc1:
                            st.image(__import__("base64").b64decode(_pimg["b64"]), use_container_width=True)
                        with _pc2:
                            st.markdown(f"**Фото #{_pi3+1} — {_cstrip(_ptyp.group(1)) if _ptyp else ''}**")
                            st.markdown(
                                f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0">'
                                f'<div style="font-size:2rem;font-weight:800;color:{_pbc}">{_pscore}/10</div>'
                                f'<div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px">'
                                f'<div style="background:{_pbc};width:{_pscore*10}%;height:10px;border-radius:6px"></div>'
                                f'</div><div style="color:{_pbc};font-size:0.8rem;margin-top:2px">{_pslbl}</div></div></div>',
                                unsafe_allow_html=True)
                            if _pstrg: st.success(f"✅ {_cstrip(_pstrg.group(1))}")
                            if _pweak: st.warning(f"⚠️ {_cstrip(_pweak.group(1))}")
            else:
                if not st.session_state.get("do_comp_vision", True):
                    st.info("👁️ Vision конкурентов был отключён. Нажми 🧠 Анализ выше для анализа этого конкурента.")
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

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Workflow
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📋 Workflow":
    st.title("📋 Workflow — Pipeline листингов")

    _board = db_workflow_board()

    if not _board:
        st.info("Нет данных. Запусти анализ хотя бы одного листинга.")
    else:
        _status_cols = {key: [] for _, key, _ in WORKFLOW_STATUSES}
        for item in _board:
            _s = item.get("status", "new_audit")
            if _s not in _status_cols: _s = "new_audit"
            _status_cols[_s].append(item)

        _wf_cols = st.columns(len(WORKFLOW_STATUSES))
        for _ci, (icon, key, label) in enumerate(WORKFLOW_STATUSES):
            with _wf_cols[_ci]:
                _items = _status_cols[key]
                st.markdown(f"**{icon} {label}**")
                st.caption(f"{len(_items)} листинг(ов)")
                for _it in _items:
                    _score = _it.get("score") or 0
                    _sc_color = "#22c55e" if _score>=80 else ("#f59e0b" if _score>=60 else "#ef4444")
                    with st.container(border=True):
                        st.markdown(f"**{_it['asin']}**")
                        st.markdown(f'<span style="color:{_sc_color};font-weight:700">{_score}%</span>', unsafe_allow_html=True)
                        if _it.get("title"): st.caption(_it["title"][:40])
                        if _it.get("note"): st.caption(f"📝 {_it['note']}")

        st.divider()
        st.subheader("✏️ Изменить статус")
        if _board:
            _sel_asin = st.selectbox("ASIN", [i["asin"] for i in _board], key="wf_sel_asin")
            _sel_item = next((i for i in _board if i["asin"]==_sel_asin), None)
            if _sel_item:
                _cur_status = _sel_item.get("status","new_audit")
                _status_keys = [k for _,k,_ in WORKFLOW_STATUSES]
                _status_labels = [f"{ic} {lb}" for ic,_,lb in WORKFLOW_STATUSES]
                _cur_idx = _status_keys.index(_cur_status) if _cur_status in _status_keys else 0
                _new_status_label = st.radio("Статус", _status_labels, index=_cur_idx, horizontal=True, key="wf_status_radio")
                _new_status = _status_keys[_status_labels.index(_new_status_label)]
                _new_note = st.text_input("Заметка (необязательно)", value=_sel_item.get("note",""), key="wf_note")
                if st.button("💾 Сохранить", type="primary", key="wf_save"):
                    if db_update_workflow(_sel_item["id"], _new_status, _new_note):
                        st.success(f"✅ {_sel_asin} → {workflow_label(_new_status)}")
                        st.rerun()
                    else:
                        st.error("Ошибка сохранения")
