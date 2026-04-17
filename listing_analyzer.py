# Amazon Listing Analyzer v2 — MR.EQUIPP
import json, re, base64, requests, streamlit as st
from PIL import Image
import io
from datetime import datetime
from auth import (
    show_login, logout, show_admin_panel,
    ensure_tables, create_admin_if_not_exists
)
# После st.set_page_config
ensure_tables()
create_admin_if_not_exists()
if "user" not in st.session_state:
    show_login()
    st.stop()
# ── PostgreSQL history ─────────────────────────────────────────────────────────
def safe_float_rating(val):
    """Safely convert rating string like '4.5 out of 5 stars' to float"""
    try:
        return float(str(val or 0).split()[0].replace(",","."))
    except:
        return 0.0


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
    auth_db_init()    
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
                our_data_json TEXT,
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
            ("our_data_json", "TEXT"),
            ("images_json", "TEXT"),
            ("aplus_img_urls_json", "TEXT"),
            ("marketplace", "TEXT DEFAULT 'com'"),
            ("aplus_vision_text", "TEXT"),   # ← NEW: сохраняем A+ Vision
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
    if not result or not isinstance(result, dict): return False
    if not asin or asin == "unknown" or not our_title:
        return False  # no real our listing
    if pct(result.get("overall_score", 0)) == 0:
        return False
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
        _imgs_to_save = []
        for _img_d in st.session_state.get("images", [])[:5]:
            try:
                _ib = base64.b64decode(_img_d["b64"])
                _pil = Image.open(io.BytesIO(_ib)).convert("RGB")
                _pil.thumbnail((300, 300))
                _tb = io.BytesIO(); _pil.save(_tb, "JPEG", quality=55); _tb.seek(0)
                _imgs_to_save.append({"b64": base64.b64encode(_tb.read()).decode(), "media_type": "image/jpeg"})
            except: pass
        _aplus_urls_save = st.session_state.get("aplus_img_urls", [])
        _aplus_vision_save = st.session_state.get("aplus_vision", "")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO listing_analysis
              (asin, overall_score, title_score, bullets_score, images_score,
               aplus_score, cosmo_score, rufus_score, result_json, vision_text,
               our_title, competitors_json, our_data_json, marketplace,
               images_json, aplus_img_urls_json, aplus_vision_text, analyzed_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
              json.dumps(comp_snap, ensure_ascii=False),
              json.dumps(st.session_state.get("our_data",{}), ensure_ascii=False),
              st.session_state.get("_marketplace","com"),
              json.dumps(_imgs_to_save, ensure_ascii=False),
              json.dumps(_aplus_urls_save, ensure_ascii=False),
              _aplus_vision_save,
              auth_current_user_email()))   # ← NEW
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
            SELECT asin, our_title, overall_score, analyzed_at, listing_type,
                   COALESCE(marketplace,'com') as marketplace, our_data_json, competitors_json,
                   COALESCE(analyzed_by,'') as analyzed_by
            FROM listing_analysis
            ORDER BY analyzed_at DESC
        """)
        rows = cur.fetchall()
        conn.close()
        result = []
        for r in rows:
            _img = ""
            if r[6]:
                try:
                    _od = json.loads(r[6])
                    _imgs = _od.get("images_of_specified_asin", _od.get("images", []))
                    if _imgs and isinstance(_imgs, list):
                        _img = next((u for u in _imgs if isinstance(u,str) and u.startswith("http") and "_SR" not in u and "_SS4" not in u), "")
                except: pass
            _model_used = ""; _duration = 0; _comp_names = []
            if r[6]:
                try:
                    _od2 = json.loads(r[6])
                    _model_used = _od2.get("_model_used","")
                    _duration = _od2.get("_analysis_duration_sec", 0)
                except: pass
            if r[7]:
                try:
                    _comps = json.loads(r[7])
                    _comp_names = []
                    for c in _comps[:5]:
                        if isinstance(c, dict):
                            _ct = (c.get("title","") or "")[:35]
                            _ca2 = c.get("asin","")
                            _csc = c.get("overall_score", c.get("score", 0)) or 0
                            _cmp2 = c.get("marketplace","com") or "com"
                            _comp_names.append({"title": _ct, "asin": _ca2, "score": _csc, "marketplace": _cmp2})
                except: pass
            result.append({"asin": r[0], "title": r[1], "score": r[2], "date": r[3],
                           "type": r[4], "marketplace": r[5], "img": _img, "model_used": _model_used,
                           "duration": _duration, "competitors": _comp_names,
                           "analyst": r[8]})

        # ── Фильтр по юзеру ──────────────────────────────────────────────
        _user = st.session_state.get("user", {})
        if _user.get("role") != "admin":
            _my_email = _user.get("email", "")
            result = [a for a in result if not a.get("analyst") or a.get("analyst") == _my_email]

        return result
    except Exception:
        return []
        
ANTHROPIC_URL          = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL        = "claude-sonnet-4-6"
ANTHROPIC_MODEL_VISION = "claude-sonnet-4-6"

SCHEMA = '{"overall_score":"XX%","title_score":"XX%","bullets_score":"XX%","description_score":"XX%","images_score":"XX%","qa_score":"XX%","reviews_score":"XX%","aplus_score":"XX%","price_score":"XX%","availability_score":"XX%","average_rating_score":"XX%","total_reviews_score":"XX%","bsr_score":"XX%","keywords_score":"XX%","prime_score":"XX%","returns_score":"XX%","customization_score":"XX%","first_available_score":"XX%","title_gaps":["specific title issue"],"title_rec":"specific title recommendation","bullets_gaps":["specific bullets issue"],"bullets_rec":"specific bullets recommendation","description_gaps":["specific description issue"],"description_rec":"specific description recommendation","aplus_gaps":["specific A+ issue"],"aplus_rec":"specific A+ recommendation","images_gaps":["specific images issue"],"images_rec":"specific images recommendation","images_breakdown":{"main_image":"XX% - reason","gallery":"XX% - reason","ocr_readability":"XX% - reason"},"cosmo_analysis":{"score":"XX%","signals_present":["signal with evidence"],"signals_missing":["missing signal"]},"rufus_analysis":{"score":"XX%","issues":["specific issue"]},"jtbd_analysis":{"functional_job":"main functional job","emotional_job":"main emotional job","social_job":"main social job","job_story":"When [situation], I want to [motivation], so I can [outcome]","alignment_score":"XX%","listing_communicates_job":true,"jtbd_gaps":["gap 1"],"jtbd_recs":["rec 1"]},"vpc_analysis":{"fit_score":"XX%","customer_jobs":["job 1","job 2","job 3"],"customer_pains":["pain 1","pain 2","pain 3"],"customer_gains":["gain 1","gain 2","gain 3"],"pain_relievers_present":["what listing already addresses from pains"],"pain_relievers_missing":["what pains listing does NOT address"],"gain_creators_present":["what gains listing already communicates"],"gain_creators_missing":["what gains listing does NOT communicate"],"products_services":["feature/benefit listing has"],"vpc_verdict":"1-2 sentence McKinsey-style summary: product solves X but listing fails to communicate Y — leading to Z% conversion loss"},"priority_improvements":["1. specific action","2. specific action","3. specific action"],"missing_chars":[{"name":"characteristic name","how_competitors_use":"how they use it","priority":"HIGH"}],"tech_params":[{"param":"parameter name","competitor_value":"their value","our_gap":"our gap"}],"actions":[{"action":"specific action","impact":"HIGH","effort":"LOW","details":"details"}]}'


def get_asin(url):
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None

def sc(s): return "🟢" if s>=8 else ("🟡" if s>=6 else "🔴")
def badge(p): return {"HIGH":"🔴 HIGH","MEDIUM":"🟡 MEDIUM","LOW":"🟢 LOW"}.get(p,p)

# ── Apify Reviews + Return Analysis ──────────────────────────────────────────
def fetch_sp_returns(asin, days=30, log=None):
    import hashlib, hmac, urllib.parse
    from datetime import datetime, timedelta, timezone

    _log = log or (lambda m: None)

    client_id     = st.secrets.get("LWA_CLIENT_ID","")
    client_secret = st.secrets.get("LWA_CLIENT_SECRET","")
    refresh_token = st.secrets.get("LWA_REFRESH_TOKEN","")
    aws_key       = st.secrets.get("AWS_ACCESS_KEY_ID","")
    aws_secret    = st.secrets.get("AWS_SECRET_ACCESS_KEY","")
    marketplace   = st.secrets.get("MARKETPLACE_ID","ATVPDKIKX0DER")

    if not all([client_id, client_secret, refresh_token, aws_key, aws_secret]):
        _log("⚠️ SP-API credentials не заданы в Secrets")
        return []

    try:
        _log("🔑 SP-API: получаю access token...")
        tok_r = requests.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     client_id,
            "client_secret": client_secret,
        }, timeout=30)
        if not tok_r.ok:
            _log(f"⚠️ LWA ошибка: {tok_r.status_code} {tok_r.text[:100]}")
            return []
        access_token = tok_r.json()["access_token"]
    except Exception as e:
        _log(f"⚠️ LWA: {e}"); return []

    try:
        _log("📋 SP-API: запрашиваю отчёт возвратов...")
        end_dt   = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)

        endpoint = "https://sellingpartnerapi-na.amazon.com"
        path     = "/reports/2021-06-30/reports"
        body     = json.dumps({
            "reportType":        "GET_CUSTOMER_RETURNS_DATA",
            "marketplaceIds":    [marketplace],
            "dataStartTime":     start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "dataEndTime":       end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

        import hmac as _hmac, hashlib as _hs
        now      = datetime.now(timezone.utc)
        amzdate  = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp= now.strftime("%Y%m%d")
        region   = "us-east-1"
        service  = "execute-api"

        def _sign(key, msg):
            return _hmac.new(key, msg.encode("utf-8"), _hs.sha256).digest()
        def _get_sig_key(key, date, region, service):
            return _sign(_sign(_sign(_sign(("AWS4"+key).encode("utf-8"), date), region), service), "aws4_request")

        payload_hash = _hs.sha256(body.encode("utf-8")).hexdigest()
        canonical = "\n".join([
            "POST", path, "",
            f"content-type:application/json\nhost:sellingpartnerapi-na.amazon.com\nx-amz-access-token:{access_token}\nx-amz-date:{amzdate}\n",
            "content-type;host;x-amz-access-token;x-amz-date",
            payload_hash
        ])
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256", amzdate,
            f"{datestamp}/{region}/{service}/aws4_request",
            _hs.sha256(canonical.encode("utf-8")).hexdigest()
        ])
        sig_key   = _get_sig_key(aws_secret, datestamp, region, service)
        signature = _hmac.new(sig_key, string_to_sign.encode("utf-8"), _hs.sha256).hexdigest()
        auth      = (f"AWS4-HMAC-SHA256 Credential={aws_key}/{datestamp}/{region}/{service}/aws4_request, "
                     f"SignedHeaders=content-type;host;x-amz-access-token;x-amz-date, Signature={signature}")

        headers = {
            "Content-Type":       "application/json",
            "x-amz-access-token": access_token,
            "x-amz-date":         amzdate,
            "Authorization":      auth,
        }
        rep_r = requests.post(endpoint + path, headers=headers, data=body, timeout=60)
        if not rep_r.ok:
            _log(f"⚠️ Report request: {rep_r.status_code} {rep_r.text[:200]}")
            return []

        report_id = rep_r.json().get("reportId","")
        _log(f"✅ Report requested: {report_id}")

        import time
        for attempt in range(20):
            time.sleep(10)
            status_r = requests.get(
                f"{endpoint}/reports/2021-06-30/reports/{report_id}",
                headers=headers, timeout=30)
            if not status_r.ok: continue
            status = status_r.json().get("processingStatus","")
            _log(f"⏳ Report status: {status} ({attempt+1}/20)")
            if status == "DONE":
                doc_id = status_r.json().get("reportDocumentId","")
                break
            if status in ("CANCELLED","FATAL"):
                _log(f"❌ Report {status}")
                return []
        else:
            _log("⚠️ Report timeout"); return []

        doc_r = requests.get(
            f"{endpoint}/reports/2021-06-30/documents/{doc_id}",
            headers=headers, timeout=30)
        if not doc_r.ok: return []
        doc_url = doc_r.json().get("url","")
        csv_r   = requests.get(doc_url, timeout=60)
        if not csv_r.ok: return []

        import csv, io
        rows = []
        reader = csv.DictReader(io.StringIO(csv_r.text), delimiter="\t")
        for row in reader:
            if not asin or row.get("asin","") == asin or row.get("ASIN","") == asin:
                rows.append({
                    "order_id":    row.get("order-id", row.get("order_id","")),
                    "return_date": row.get("return-date", row.get("return_date","")),
                    "reason":      row.get("reason",""),
                    "comment":     row.get("customer-comments", row.get("customer_comments","")),
                    "asin":        row.get("asin", row.get("ASIN","")),
                })
        _log(f"✅ SP-API returns: {len(rows)} возвратов для {asin}")
        return rows

    except Exception as e:
        _log(f"⚠️ SP-API returns: {e}")
        return []

def analyze_sp_returns(returns, product_title, asin, lang="ru"):
    if not returns: return "Нет данных о возвратах"
    lang_name = "Russian" if lang == "ru" else "English"

    reasons = {}
    for r in returns:
        reason = r.get("reason","Unknown")
        reasons[reason] = reasons.get(reason, 0) + 1

    total = len(returns)
    reasons_text = "\n".join([f"- {k}: {v} ({v*100//total}%)" for k,v in sorted(reasons.items(), key=lambda x:-x[1])])
    comments = "\n".join([f"[{r.get('return_date','')}] {r.get('reason','')} — {r.get('comment','')[:150]}"
                          for r in returns[:15] if r.get("comment")])

    prompt = f"""Analyze these Amazon return data for: {product_title} (ASIN: {asin})

RETURN REASONS SUMMARY ({total} total returns, last 30 days):
{reasons_text}

CUSTOMER COMMENTS:
{comments}

Provide:
1. Top 3 root causes with % and whether it's LISTING issue or PRODUCT issue
2. Specific listing fixes (title/bullets/photos/size chart) to reduce returns
3. Risk assessment: is this heading toward "Frequently Returned" badge?
4. Priority actions ranked by impact

Be specific. Use actual data from the returns. Respond in {lang_name}."""

    return ai_call("Amazon returns expert. Actionable analysis only.", prompt, max_tokens=2000)

def fetch_1star_reviews(asin, domain="com", max_pages=1, log=None):
    api_token = st.secrets.get("APIFY_API_TOKEN","")
    if not api_token:
        if log: log("⚠️ APIFY_API_TOKEN не задан в Secrets")
        return []
    endpoint = f"https://api.apify.com/v2/acts/webdatalabs~amazon-reviews-scraper/run-sync-get-dataset-items?token={api_token}"
    all_reviews = []
    payload = {
        "productUrls": [{"url": f"https://www.amazon.{domain}/dp/{asin}"}],
    }
    try:
        if log: log(f"📥 Apify: загружаю отзывы {asin}...")
        r = requests.post(endpoint, json=payload, timeout=300)
        if log: log(f"  → HTTP {r.status_code}")
        if r.ok:
            data = r.json()
            if log: log(f"  → {len(data) if isinstance(data, list) else 0} отзывов получено")
            reviews = data if isinstance(data, list) else []
            try:
                low_reviews = [rv for rv in reviews if int(float(str(rv.get("rating", 5) or 5).split()[0])) <= 3]
            except:
                low_reviews = []
            all_reviews = low_reviews[:30] if low_reviews else reviews[:30]
            if log: log(f"  ✅ 1-3★: {len(low_reviews)}, передаём: {len(all_reviews)}")
        else:
            if log: log(f"  ❌ {r.status_code}: {r.text[:200]}")
    except Exception as e:
        if log: log(f"⚠️ Apify: {e}")
    if log: log(f"✅ Всего: {len(all_reviews)} отзывов (1★+2★)")
    return all_reviews

def analyze_return_reasons(reviews, product_title, asin, lang="ru"):
    if not reviews:
        return "Нет отзывов для анализа"
    lang_name = "Russian" if lang == "ru" else "English"
    reviews_text = "\n".join([
        f"[{r.get('rating','?')}★] {r.get('title', r.get('reviewTitle',''))} — {r.get('body', r.get('text', r.get('reviewText', r.get('content',''))))[:300]}"
        for r in reviews[:30]
    ])
    prompt = f"""Analyze these 1-star Amazon reviews for product: {product_title} (ASIN: {asin})

REVIEWS:
{reviews_text}

Identify the TOP return/complaint reasons. For each reason:
1. What % of reviews mention it (estimate)
2. Is it a LISTING problem (wrong description/photos) or PRODUCT problem (quality/design)?
3. Specific fix recommendation

Also provide:
- Is_frequently_returned_risk: HIGH/MEDIUM/LOW
- Quick wins: what can be fixed in the listing TODAY without changing the product

Respond in {lang_name}. Be specific and actionable. Format as clear sections."""

    return ai_call("Amazon return analysis expert. Be specific and data-driven.", prompt, max_tokens=2000)

# ── Amazon Stop Words ─────────────────────────────────────────────────────────
AMAZON_STOP_WORDS = {
    "do_not_use": [
        "ailment","cure","cured","cures","treat","treatment","treats","heal","healing","heals",
        "prevent","prevents","diagnose","remedy","remedies","medication","pharmaceutical",
        "detox","detoxify","detoxification","detoxifying","reparative","fast relief","relief",
        "clinically proven","doctor recommended","no side effects","pain free","proven to work",
        "performance enhancement","disease","diseases","illness","maladies","malady",
        "aids","add","adhd","als","alzheimer","autism","autistic","cancer","cancroid",
        "cataract","chlamydia","cmv","cytomegalovirus","concussion","coronavirus","covid",
        "crabs","cystic fibrosis","dementia","depression","diabetes","diabetic",
        "epilepsy","flu","glaucoma","gonorrhea","gout","hepatitis","herpes","hsv1","hsv2",
        "hiv","hodgkin","hpv","influenza","kidney disease","liver disease","lupus",
        "lymphoma","meningitis","mononucleosis","mono","multiple sclerosis",
        "muscular dystrophy","obesity","parkinson","pid","pelvic inflammatory",
        "scabies","seizure","seizures","stroke","syphilis","trichomoniasis","tumor",
        "ringworm","insomnia","anxiety","inflammation","infection",
        "antibacterial","anti-bacterial","antimicrobial","anti-microbial","antifungal",
        "anti-fungal","antiviral","antiseptic","bacteria","bacterial","contaminants",
        "contamination","disinfect","disinfectant","disinfects","fungal","fungus",
        "fungicide","fungicides","germ","germs","germ-free","insecticide","mildew",
        "mold","mould","mold resistant","mold spores","nano silver","parasitic",
        "pathogen","pest","pesticide","pesticides","pesticide-free","protozoa",
        "repel","repellent","repelling","sanitize","sanitizes","viral","virus","viruses",
        "mites","yeast","biological contaminants",
        "cbd","cannabinoid","thc","cannabidiol","cannabis","marijuana","kratom","hemp",
        "kanna","weed","dab","shatter","ketamine","psilocybin","ephedrine",
        "minoxidil","ketoconazole","hordenine","ayahuasca","picamilon","dmt",
        "knockoff","fake","weapon","weapons","stun guns","self defense","pepper spray",
        "swastika","poppy","iv therapy","intravenous therapy","fetal doppler",
        "heartbeat monitor","batons","drugged",
        "amazon approved","amazon certified","amazon recommended","amazon endorsed",
        "amazon authorized","amazon licensed","amazon verified",
    ],
    "try_to_avoid": [
        "best","best seller","best selling","best buy","best deal","best price",
        "best value","bestseller","#1","number one","top","top notch","top rated",
        "top selling","amazing","award winning","champion","elite","finest","flawless",
        "foremost","greatest","hallmark","highest rated","hot item","hottest item",
        "ideal","impeccable","incomparable","in-demand","infallible","invincible",
        "irresistible","leading","masterful","matchless","most popular","optimal",
        "outstanding","paramount","peerless","perfect","pinnacle","premier","prime",
        "pristine","professional quality","record-breaking","sought-after","superb",
        "supreme","ultimate","unbeatable","unblemished","unmatched","unparalleled",
        "unrivaled","magic solution","instant fix","the world's best","the world's strongest",
        "eco-friendly","eco friendly","ecofriendly","environmentally friendly",
        "earth-friendly","sustainable","biodegradable","compostable","home compostable",
        "marine degradable","decomposable","degradable","carbon-reducing",
        "all natural","all-natural","natural","recyclable","vegan","non-toxic",
        "bpa free","bisphenol a","hypoallergenic","organic","green",
        "allergy free","allergy safe","anti aging","healthy","healthier","proven",
        "recommended by","tested","validated","treatment","weight loss","hypoallergenic",
        "nano silver","safe","harmless","non-poisonous","non-injurious","non-toxic",
        "reduce anxiety","boost immunity","lower blood pressure","increase metabolism",
        "suppress appetite","slimming","fat burning","keto approved","appetite suppressant",
        "free","bonus","guarantee","money back","refund","warranty","price",
        "on sale","best deal","limited time","buy now","add to cart","get yours now",
        "shop now","don't miss","last chance","supplies won't last","available now",
        "save","discount","bargain","cheap","cheapest","clearance","closeout","overstock",
        "special offer","buy 1 get 1","wholesale","% off","affordable",
        "made in the usa","made in usa",
    ],
    "a_plus_restricted": [
        "approved","certified","drug","drugs","pearl","platinum","noncorrosive",
        "satisfaction guaranteed","100% satisfaction","buy now","add to cart",
        "get yours now","shop with us","free shipping","free gift","now","new","latest",
        "affordable","bonus","warranty","guarantee","money back","copyright",
        "trademark","patent pending","™","®","competitors","versus","vs.",
        "better than","unlike other brands","unlike competitors",
    ],
}

def check_stop_words(text):
    if not text: return {}
    text_lower = text.lower()
    found = {"do_not_use": [], "try_to_avoid": [], "a_plus_restricted": []}
    for cat, words in AMAZON_STOP_WORDS.items():
        for w in words:
            pattern = r'\b' + re.escape(w) + r'\b'
            if re.search(pattern, text_lower):
                found[cat].append(w)
    return {k: v for k, v in found.items() if v}

def check_listing_stop_words(our_data):
    fields = {
        "Title": our_data.get("title", ""),
        "Bullets": " ".join(our_data.get("feature_bullets", [])),
        "Description": str(our_data.get("description", "")),
        "A+ Content": str(our_data.get("aplus_content", "")),
    }
    results = {}
    for field, text in fields.items():
        found = check_stop_words(text)
        if found:
            results[field] = found
    return results

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

def estimate_run(do_vision, do_aplus, do_comp_vision, n_competitors, use_gemini):
    secs = 30
    cost = 0.008
    model_mult = 0.3 if use_gemini else 1.0
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

def _anthropic_post(payload, retries=4):
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
        if r.status_code == 500:
            wait = 15 * (attempt + 1)
            st.toast(f"⏳ Anthropic 500, жду {wait}с и повторяю... ({attempt+1}/{retries})")
            import time; time.sleep(wait)
            continue
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

# Актуальные модели Gemini (апрель 2026)
GEMINI_FLASH_MODELS = [
    "gemini-3.1-flash-preview",   # новейший flash
    "gemini-flash-latest",        # auto-latest alias
    "gemini-2.5-flash",           # стабильный
    "gemini-2.0-flash-001",
    "gemini-2.0-flash",
]
GEMINI_PRO_MODELS = [
    "gemini-3.1-pro-preview",     # новейший pro
    "gemini-pro-latest",          # auto-latest alias
    "gemini-2.5-pro",             # стабильный pro
    "gemini-2.5-flash",           # fallback
    "gemini-2.0-flash",
]
GEMINI_IMAGE_MODELS = [
    "gemini-3.1-flash-image-preview",  # Nano Banana 2 — генерация фото
    "gemini-2.5-flash-image",          # стабильный
    "gemini-2.0-flash-exp-image-generation",
]

@st.cache_data(ttl=300, show_spinner=False)  # 5 мин
def get_available_gemini_models(key):
    """Получает список реально доступных моделей через API"""
    try:
        r = __import__('requests').get(
            f"https://generativelanguage.googleapis.com/v1beta/models?key={key}",
            timeout=10
        )
        if r.ok:
            all_models = [m["name"].replace("models/","") for m in r.json().get("models",[])]
            # Только image/text модели (не embedding, не tts)
            return [m for m in all_models if any(x in m for x in ["flash","pro"]) 
                    and not any(x in m for x in ["embed","tts","aqa","vision-only"])]
    except: pass
    return []

@st.cache_data(ttl=300, show_spinner=False)  # 5 мин
def get_best_gemini_model(key, prefer_pro=False):
    """Автоматически находит лучшую доступную модель через реальный список API"""
    available = get_available_gemini_models(key)
    preferred = GEMINI_PRO_MODELS if prefer_pro else GEMINI_FLASH_MODELS
    # Сначала пробуем топовые модели напрямую (могут быть недоступны через list API но работают)
    _top_try = ["gemini-3.1-pro-preview","gemini-3.1-flash-preview","gemini-pro-latest","gemini-flash-latest"]
    for m in (_top_try if not prefer_pro else ["gemini-3.1-pro-preview","gemini-pro-latest"]):
        if m in available or True:  # пробуем всегда — list API не всегда полный
            try:
                _tr = __import__('requests').post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={key}",
                    json={"contents":[{"parts":[{"text":"hi"}]}],"generationConfig":{"maxOutputTokens":3}},
                    timeout=5
                )
                if _tr.ok: return m
            except: pass
    # Fallback — из доступных по list API
    for m in preferred:
        if m in available:
            return m
    # Если нет — берём лучшее из доступного
    if available:
        # Приоритет: 2.5 > 2.0, flash > lite
        for m in available:
            if "2.5" in m and ("pro" if prefer_pro else "flash") in m and "lite" not in m:
                return m
        for m in available:
            if ("pro" if prefer_pro else "flash") in m and "lite" not in m:
                return m
        return available[0]
    return preferred[-1]  # hardcoded fallback


def gemini_call(prompt, max_tokens=3000):
    import time
    key = st.secrets.get("GEMINI_API_KEY","")
    if not key: raise Exception("GEMINI_API_KEY не задан в Secrets")
    _prefer_pro = "pro" in st.session_state.get("gemini_model","")
    _gmodel = get_best_gemini_model(key, prefer_pro=_prefer_pro)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{_gmodel}:generateContent?key={key}"
    payload = {"contents":[{"parts":[{"text":prompt}]}],
               "generationConfig":{
                   "maxOutputTokens": max_tokens,
                   "temperature": 0.2,      # низкая = стабильные результаты
                   "topP": 0.8,
                   "topK": 40
               }}
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

def gemini_vision_call(prompt, image_urls=None, image_b64_list=None, max_tokens=8000):
    import time
    key = st.secrets.get("GEMINI_API_KEY","")
    if not key: raise Exception("GEMINI_API_KEY не задан")
    _prefer_pro = "про" in st.session_state.get("gemini_model","") or "pro" in st.session_state.get("gemini_model","")
    _gmodel = get_best_gemini_model(key, prefer_pro=_prefer_pro)
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
               "generationConfig": {
                   "maxOutputTokens": max_tokens,
                   "temperature": 0.2,
                   "topP": 0.8,
                   "topK": 40
               }}
    import time as _time
    _last_err = ""
    _waits = [15, 30, 60]  # короткие паузы для бесплатного tier
    for attempt in range(4):
        r = requests.post(url, json=payload, timeout=120)
        _last_err = f"{r.status_code}: {r.text[:200]}"
        if r.ok:
            return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        if r.status_code == 429:
            wait = _waits[min(attempt, len(_waits)-1)]
            st.toast(f"⏳ Gemini лимит, пауза {wait}с (попытка {attempt+1}/4)...")
            _time.sleep(wait)
            continue
        if r.status_code in (503, 500):
            _time.sleep(10)
            continue
        raise Exception(f"Gemini Vision ошибка: {_last_err}")
    raise Exception(f"Gemini Vision: превышен лимит запросов. Попробуй через минуту или переключись на Claude.")


def check_gemini_tier(api_key):
    """Check if Gemini API key is free or paid tier"""
    try:
        import requests as _rq
        # Simple test request - send minimal payload
        _r = _rq.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
            json={"contents": [{"parts": [{"text": "hi"}]}],
                  "generationConfig": {"maxOutputTokens": 5}},
            timeout=15
        )
        if _r.ok:
            # Check rate limit headers
            _rpm = _r.headers.get("x-ratelimit-limit-requests","")
            _remaining = _r.headers.get("x-ratelimit-remaining-requests","")
            return {"status": "ok", "rpm": _rpm, "remaining": _remaining,
                    "tier": "paid" if _rpm and int(_rpm or 0) > 15 else "likely_free"}
        elif _r.status_code == 429:
            return {"status": "429", "tier": "free", "msg": "Rate limit — бесплатный"}
        elif _r.status_code == 400:
            return {"status": "ok_paid", "tier": "paid", "msg": "Ключ рабочий"}
        else:
            return {"status": "error", "code": _r.status_code, "msg": _r.text[:100]}
    except Exception as e:
        return {"status": "exception", "msg": str(e)}

def ai_vision_call(prompt, image_b64=None, image_url=None, media_type="image/jpeg", max_tokens=500, system=None):
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
def scrapingdog_product(asin, log, domain="com"):
    sd_key = st.secrets.get("SCRAPINGDOG_API_KEY","")
    if not sd_key: log("⚠️ SCRAPINGDOG_API_KEY не задан"); return {}, []
    _sd_map = {"com":"com","co.uk":"co.uk","de":"de","fr":"fr","it":"it","es":"es","ca":"ca","nl":"nl","se":"se","pl":"pl","com.be":"com.be","com.mx":"com.mx","com.au":"com.au"}
    _sd_dom = _sd_map.get(domain, "com")
    log(f"🌐 ScrapingDog: {asin} [{_sd_dom}]...")
    try:
        r = requests.get("https://api.scrapingdog.com/amazon/product",
            params={"api_key": sd_key, "asin": asin, "domain": _sd_dom}, timeout=60)
        if not r.ok: log(f"⚠️ {r.status_code}: {r.text[:100]}"); return {}, []
        data = r.json()
        if data.get("aplus"):
            _api_aplus = data.get("aplus_images", [])
            _brand_imgs = [u for u in data.get("brand_images", [])
                          if isinstance(u, str) and "aplus-media-library-service-media" in u]
            if not _api_aplus and _brand_imgs:
                _api_aplus = _brand_imgs
                log(f"  ℹ️ A+ из brand_images (From the brand): {len(_brand_imgs)} шт.")
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
        # Deduplicate by ASIN part of URL (removes duplicate main image)
        _seen_asins = set()
        _deduped = []
        for _u in urls:
            _m = re.search(r'/I/([A-Z0-9]+)\.', _u)
            _key = _m.group(1) if _m else _u
            if _key not in _seen_asins:
                _seen_asins.add(_key)
                _deduped.append(_u)
        log(f"✅ ScrapingDog: {len(_deduped)} фото (из {len(urls)} до дедупликации)")
        return data, _deduped[:7]
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

    _target_aud = st.session_state.get("target_audience","")
    _aud_line = f" | Target buyer: {_target_aud}" if _target_aud else ""

    if lang == "en":
        intro = f"""You are an Amazon photo conversion expert. Score each product photo using this RUBRIC.

Product: {title} | ASIN: {asin} | Price: {price} | Rating: {rating}{_aud_line}

SCORING RUBRIC (each photo scored 1-10):
+2 pts — Product visibility: what % of frame is the ACTUAL sold product? If product is hidden under other clothing or <30% of frame — max 1 pt for this criterion
+2 pts — Background: main=pure white RGB(255,255,255); lifestyle=relevant setting; infographic=clean layout
+2 pts — Information value: shows features/benefits/use case relevant to buyer decision
+2 pts — Amazon compliance: no watermarks, correct aspect ratio. NOTE: text/badges on main photo = intentional CTR strategy — do NOT penalize, just note as Amazon policy risk
+1 pt  — Lifestyle appeal: buyer can visualize using the product
+1 pt  — Uniqueness vs generic stock photo

CRITICAL PENALTIES (deduct immediately):
-3 pts — Sold product hidden / barely visible (e.g. tank top under a shirt)
-2 pts — Main photo has clearly unrelated clothing/accessories NOT part of the product set (e.g. shoes/sneakers on a base layer listing, a jacket on a t-shirt listing)
-2 pts — Main photo background is not pure white

IMPORTANT EXCEPTIONS — do NOT penalize for:
- Socks/underwear on base layer/thermal underwear listings (they are part of the outfit context)
- Gloves on outerwear listings
- Small decorative props in corners (ribbons, flowers, small icons)
- Model's natural body parts (hands, face, feet without shoes)
- Second item shown when product IS sold as a set/bundle

BE STRICT: real problems must lower the score. 10/10 only if photo is perfect on ALL criteria.
SCORE MEANINGS: 9-10=excellent, 7-8=good, 5-6=needs improvement, 1-4=poor/replace

PHOTO TYPES: main | lifestyle | infographic | size-chart | detail | A+-banner | comparison | packaging

MAIN IMAGE AMAZON REQUIREMENTS (apply strictly for photo #1):
✅ Pure white background RGB(255,255,255) — no shadows, no grey
✅ Product fills ≥85% of frame — estimate visually in %
✅ Text/logos/badges on main = seller's intentional CTR strategy — do NOT deduct points, mention as "⚠️ Amazon policy risk but CTR tactic" in weakness if present
✅ Minimum 1000px long side (ideally 2000-3000px) for zoom
✅ Real photo (not illustration)
✅ ONLY the sold product — FORBIDDEN: other clothing on model, accessories, props not included
✅ For apparel: the sold item must be the primary focus
IMPORTANT: Look carefully — are there any items in the photo that are NOT the sold product? If yes — deduct 2 pts and name exactly what violates the rule."""
        block_fmt = "\nPHOTO_BLOCK_{i}\nSTRICTLY 7 lines:\nType: [one of the types above]\nScore: X/10 [apply rubric]\nStrength: [1 specific strength — what exactly drives conversion]\nWeakness: [1 specific problem — ONLY what you see, with number if possible]\nAction: [CONCRETE solution: WHAT to shoot/add/remove + HOW exactly + expected conversion impact in %]\nConversion: [1 insight — what fear or desire this photo triggers and how to amplify/resolve it]\nEmotion: REQUIRED — choose ONE: Trust / Desire / Doubt / Curiosity / Indifference — explain in 1 sentence WHY this emotion, what visual element triggers it"
    else:
        intro = f"""Ты эксперт по конверсии Amazon фотографий. Оценивай каждое фото по РУБРИКУ.

Товар: {title} | ASIN: {asin} | Цена: {price} | Рейтинг: {rating}{_aud_line}

РУБРИК ОЦЕНКИ (каждое фото 1-10 баллов):
+2 балла — Видимость товара: сколько % кадра занимает продаваемый товар? Если <30% — максимум 1 балл
+2 балла — Фон: главное=чисто белый RGB(255,255,255); lifestyle=релевантная обстановка
+2 балла — Информационная ценность: показывает характеристики/пользу/сценарий важный для покупателя
+2 балла — Соответствие Amazon: нет водяных знаков, нет промотекста на главном
+1 балл  — Lifestyle appeal: покупатель представляет себя с товаром
+1 балл  — Уникальность: не выглядит как стоковое фото

КРИТИЧЕСКИЕ ШТРАФЫ (вычитай сразу):
-3 балла — Продаваемый товар скрыт / почти не виден
-2 балла — На главном фото явно посторонняя одежда/обувь НЕ из комплекта (напр. кроссовки на листинге термобелья, куртка на листинге футболки)
-2 балла — Фон главного фото не чисто белый

ВАЖНЫЕ ИСКЛЮЧЕНИЯ — НЕ штрафовать за:
- Носки/нижнее бельё на листингах термобелья/базового слоя (контекст ношения)
- Перчатки на листингах верхней одежды
- Небольшие декоративные элементы в углу кадра (ленточки, иконки)
- Естественные части тела модели (руки, лицо, ступни без обуви)
- Второй предмет если товар продаётся комплектом/набором

БУДЬ СТРОГ: реальные проблемы должны снижать оценку. 10/10 только если фото идеально по ВСЕМ критериям.
ЗНАЧЕНИЯ: 9-10=отлично, 7-8=хорошо, 5-6=требует улучшения, 1-4=слабо/заменить

ТИПЫ ФОТ: главное | lifestyle | инфографика | размерная-сетка | детали | A+-баннер | сравнение | упаковка

ТРЕБОВАНИЯ AMAZON К ГЛАВНОМУ ФОТО (применяй строго к фото #1):
✅ Фон исключительно белый RGB(255,255,255) — без теней, без серого
✅ Товар занимает ≥85% площади кадра
✅ Текст/логотипы/бейджи на главном фото = намеренная CTR-стратегия продавца — НЕ снижай оценку, укажи как "⚠️ Риск политики Amazon, но CTR-тактика" в слабости если есть
✅ Минимум 1000px по длинной стороне
✅ Реальная фотография (не иллюстрация)
✅ ТОЛЬКО продаваемый товар — ЗАПРЕЩЕНЫ: другая одежда на модели, аксессуары не из комплекта
ВАЖНО: Посмотри внимательно — есть ли на фото предметы которые НЕ являются продаваемым товаром? Если да — это нарушение, снять 2 балла и написать конкретно что нарушает правило."""
        block_fmt = "\nPHOTO_BLOCK_{i}\nОТРОГО 7 строк:\nТип: [один из типов выше]\nОценка: X/10 [применяй рубрик]\nСильная сторона: [1 конкретная сильная сторона — что именно работает на конверсию]\nСлабость: [1 конкретная проблема — ТОЛЬКО то что видишь, с цифрой если возможно]\nДействие: [КОНКРЕТНОЕ решение: ЧТО снять/добавить/убрать + КАК именно + ожидаемый эффект на конверсию в %]\nКонверсия: [1 инсайт — какой страх или желание покупателя это фото вызывает и как его усилить/снять]\nЭмоция: ОБЯЗАТЕЛЬНО — выбери ОДНО: Доверие / Желание / Сомнение / Любопытство / Безразличие — объясни в 1 предложении ПОЧЕМУ эта эмоция и какой визуальный элемент её вызывает"

    results = []
    if st.session_state.get("use_gemini"):
        import time; time.sleep(5)
        _fmt = block_fmt  # используем полный формат как у Claude
        for _i, _img in enumerate(images):
            if _i > 0: 
                import time as _tg; _tg.sleep(8)  # пауза Gemini free tier
            if _i > 0: time.sleep(8)
            log(f"👁️ Gemini фото {_i+1}/{len(images)}...")
            # Используем тот же полный промпт что у Claude
            _fmt_i = _fmt.format(i=_i+1) if "{i}" in _fmt else _fmt
            _pp = intro + f"\n\nОтветь СТРОГО в формате (все 7 строк обязательны):\nPHOTO_BLOCK_{_i+1}\n{_fmt_i}"
            _br = gemini_vision_call(_pp, image_b64_list=[(_img["b64"], _img.get("media_type","image/jpeg"))], max_tokens=2500)
            _m = re.search(r"PHOTO_BLOCK_\d+\s*(.*)", _br, re.DOTALL)
            _blk = _m.group(1).strip() if _m else _br.strip()
            results.append(f"PHOTO_BLOCK_{_i+1}\n{_blk}")
    else:
        for i, img in enumerate(images):
            if i > 0 and st.session_state.get("use_gemini"):
                import time as _tg2; _tg2.sleep(8)
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
                image_url=img.get("url"), media_type=img.get("media_type","image/jpeg"), max_tokens=500)
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
Action: [1 concrete fix starting with a verb: Redesign / Remove / Add / Replace / Simplify]
Conversion: [1 insight from buyer psychology — what would make them click/buy based on this banner]"""
    else:
        sys_prompt = f"""Ты эксперт по Amazon A+ Content и конверсии. Анализируй каждый A+ баннер.
Товар: {title}

Для каждого изображения выводи СТРОГО:
APLUS_BLOCK_{{i}}
Модуль: [сравнительная-таблица | lifestyle | highlight-фич | brand-story | таблица-размеров | линейка-продуктов | другой]
Содержание: [что показывает — 1-2 предложения]
Оценка: X/10
Сильная сторона: [1 конкретная]
Слабость: [1 конкретная проблема которую видишь]
Действие: [1 конкретный фикс начиная с глагола: Переделать / Убрать / Добавить / Заменить / Упростить]
Конверсия: [1 инсайт из психологии покупателя — что конкретно сделать чтобы он кликнул или купил глядя на этот баннер]"""

    msg_content = []
    for i, img in enumerate(images):
        msg_content.append({"type":"text","text":f"{'A+ banner' if lang=='en' else 'A+ баннер'} #{i+1}:"})
        msg_content.append({"type":"image","source":{"type":"base64","media_type":img["media_type"],"data":img["b64"]}})

    try:
        if st.session_state.get("use_gemini"):
            # Gemini: по одному баннеру с паузой (как для фото)
            import time as _ta
            _ap_results = []
            for _ai, _aimg in enumerate(images):
                if _ai > 0: _ta.sleep(8)
                log(f"  👁️ A+ баннер {_ai+1}/{len(images)}...")
                _ap_fmt = sys_prompt + (
                    f"\n\nAnalyze ONLY banner #{_ai+1}. ALL 7 lines mandatory:\n"
                    f"APLUS_BLOCK_{_ai+1}\n"
                    f"Модуль: [тип баннера]\n"
                    f"Содержание: [1-2 предложения что показывает]\n"
                    f"Оценка: X/10\n"
                    f"Сильная сторона: [1 конкретная сильная сторона для конверсии]\n"
                    f"Слабость: [1 конкретная проблема которую видишь]\n"
                    f"Действие: [1 конкретный фикс начиная с глагола]\n"
                    f"Конверсия: [1 инсайт психологии покупателя что заставит купить]\n"
                    "\nDo NOT skip any line. Be critical and specific.")
                _apr = gemini_vision_call(_ap_fmt, image_b64_list=[(_aimg["b64"], _aimg["media_type"])], max_tokens=1500)
                _ap_results.append(_apr)
            result = "\n\n".join(_ap_results)
        else:
            result = anthropic_vision(msg_content, max_tokens=4000, system=sys_prompt)
        log(f"✅ A+ Vision: {len(images)} баннеров проанализировано")
        return result
    except Exception as e:
        log(f"⚠️ A+ Vision: {e}"); return ""

# ── Text analysis ─────────────────────────────────────────────────────────────
def analyze_text(our_data, competitor_data_list, vision_result, asin, log, lang="ru", is_competitor=False):
    log("🧠 Финальный анализ...")

    COMP_SCHEMA = '{"overall_score":"XX%","title_score":"XX%","bullets_score":"XX%","description_score":"XX%","images_score":"XX%","aplus_score":"XX%","reviews_score":"XX%","bsr_score":"XX%","price_score":"XX%","customization_score":"XX%","prime_score":"XX%","title_gaps":["issue"],"bullets_gaps":["issue"],"images_gaps":["issue"],"priority_improvements":["1. action","2. action","3. action"],"actions":[{"action":"action","impact":"HIGH","effort":"LOW","details":"details"}]}'

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
- Description present: {'YES' if our_data.get('description') else 'NO'}
- A+ present: {'YES' if our_data.get('aplus_content') or our_data.get('aplus') else 'NO'}
- IMPORTANT: If A+ is present AND description is empty → description_score = "0%" is acceptable (A+ replaces description for buyers, but description still matters for SEO indexing)
"""
    _qa_list = our_data.get("questions_and_answers", our_data.get("qa", our_data.get("customer_questions", [])))
    _qa_text = ""
    if _qa_list and isinstance(_qa_list, list):
        _qa_items = []
        for _qai in _qa_list[:8]:
            _q = _qai.get("question","") or _qai.get("q","") or ""
            _a = _qai.get("answer","") or _qai.get("a","") or "NO ANSWER"
            if _q: _qa_items.append(f"Q: {_q[:150]}\nA: {_a[:150]}")
        if _qa_items:
            _qa_text = "\n\nREAL CUSTOMER Q&A FROM AMAZON:\n" + "\n".join(_qa_items)
    our_text = _facts + "\n" + fmt(our_data)[:2500] + _qa_text
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

    if is_competitor:
        _d = our_data
        _pi = _d.get("product_information", {})
        _buls = _d.get("feature_bullets", [])
        _comp_text_slim = "\n".join(filter(None, [
            f"Title: {_d.get('title','')}",
            f"Price: {_d.get('price','')} | Rating: {_d.get('average_rating','')} | Reviews: {_pi.get('Customer Reviews',{}).get('ratings_count','')}",
            f"BSR: {str(_pi.get('Best Sellers Rank',''))[:80]}",
            f"A+: {'Yes' if _d.get('aplus') else 'No'} | Prime: {_d.get('is_prime_exclusive',False)} | Images: {len(_d.get('images',[]))}",
            f"Description: {'Yes' if _d.get('description') else 'No'}",
            "Bullets:\n" + "\n".join(f"- {b[:150]}" for b in _buls[:5]) if _buls else "",
        ]))
        _vis_slim = vision_result[:600] if vision_result else ""
        prompt = f"""Score this Amazon listing (ASIN {asin}). Return ONLY JSON. All text in {lang_name}.

{_comp_text_slim}
{_vis_slim}

{COMP_SCHEMA}"""
        sys_prompt = f"Amazon scorer. JSON only. {lang_name}."
        raw = ai_call(sys_prompt, prompt, max_tokens=1500)
        log(f"✅ Конкурент JSON: {len(raw) if raw else 0} chars")
        if not raw or not raw.strip():
            raise ValueError("AI вернул пустой ответ для конкурента")
        s = raw.strip()
        s = re.sub(r"^```[a-z]*\s*", "", s, flags=re.MULTILINE)
        s = re.sub(r"```\s*$", "", s, flags=re.MULTILINE)
        s = s.strip()
        start, end = s.find("{"), s.rfind("}")
        if start == -1: raise ValueError(f"JSON не найден: {s[:200]}")
        s = s[start:end+1]
        s = re.sub(r",\s*([}\]])", r"\1", s)
        s = re.sub(r'[\x00-\x1f\x7f]', ' ', s)
        try:
            return json.loads(s)
        except:
            s2 = re.sub(r'"([^"]*)"', lambda m: '"'+m.group(1).replace("\n"," ").replace("\r"," ").replace('"','\\"')+'"', s)
            try:
                return json.loads(s2)
            except:
                for cut in range(len(s2)-1, max(len(s2)-500, 0), -1):
                    if s2[cut] in ('"', '}', ']', '0123456789'):
                        candidate = s2[:cut+1]
                        candidate += "]" * max(0, candidate.count("[") - candidate.count("]"))
                        candidate += "}" * max(0, candidate.count("{") - candidate.count("}"))
                        try: return json.loads(candidate)
                        except: continue
                return {"overall_score": "50%", "title_score": "50%", "bullets_score": "50%",
                        "description_score": "50%", "images_score": "50%", "aplus_score": "0%",
                        "reviews_score": "50%", "bsr_score": "50%", "price_score": "50%",
                        "customization_score": "50%", "prime_score": "50%",
                        "priority_improvements": ["JSON repair failed — rerun analysis"]}

    # Inject previous analysis context
    _prev = db_get_prev_analysis(asin)
    _prev_context = ""
    if _prev and _prev.get("score",0) > 0:
        _prev_r = _prev.get("result",{})
        _prev_actions = _prev_r.get("priority_improvements",[])[:3] if _prev_r else []
        _prev_context = f"""
## PREVIOUS ANALYSIS ({_prev['date']}) — Overall: {_prev['score']}%
Previous top issues that needed fixing:
{chr(10).join(f"- {a}" for a in _prev_actions)}
Compare with current state: did these improve? Flag progress or regression.
"""

    prompt = f"""You are an expert Amazon listing analyst specializing in the Listing 3.0 era where AI visibility (Cosmo + Rufus) determines 50% of success.

OUR LISTING (ASIN {asin}):
{our_text}

{comp_text}
{vision_section}
{context_section}
{_prev_context}

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

## VALUE PROPOSITION CANVAS (VPC)
Map the gap between what the buyer needs and what the listing communicates.

**Customer Profile** (what buyer brings):
- Customer Jobs: functional/emotional/social jobs they hire this product for
- Pains: frustrations, risks, obstacles before/during/after the job
- Gains: outcomes and benefits they desire (required, expected, desired, unexpected)

**Value Map** (what listing offers):
- Products & Services: features/attributes listed
- Pain Relievers: how listing addresses customer pains (explicitly or implicitly)
- Gain Creators: how listing communicates desired outcomes

**Fit Score**: % of customer pains + gains that listing explicitly addresses
- 80%+ = strong fit, listing speaks buyer's language
- 50-79% = partial fit, feature-heavy but outcome-light  
- <50% = poor fit, listing talks about product, not buyer

**VPC Verdict**: AI CRO Consultant style 1-2 sentence conclusion — "The product solves X but the listing communicates Y, creating a Z% value communication gap that costs conversion"
Buyers don't buy products — they HIRE them to do a job. Analyze what job this product is hired for.

**3 types of jobs:**
1. **Functional job** — the core task: "stay fresh and odor-free during workouts without changing clothes"
2. **Emotional job** — desired feeling: "feel confident and professional even after the gym"
3. **Social job** — desired perception: "be seen as someone who has their life together"

**Job Story format:** "When [situation/context], I want to [motivation/job], so I can [outcome/benefit]"
Example: "When I go from gym to office, I want to not smell or sweat through my shirt, so I can feel confident in meetings without changing"

**Alignment score:** How well does the CURRENT listing communicate this job? Does the title/bullets/A+ speak to the job or just describe features?
- 90%+ = listing directly addresses the job scenario
- 70-89% = listing hints at the job but buries it in features
- <70% = listing is feature-focused, job is invisible

**JTBD gaps:** What job signals are MISSING from the listing? (e.g., no scenario/context described, no outcome stated, no "when X happens" trigger)
**JTBD recs:** Specific changes — rewrite title to include job context, add job scenario to bullet #1, etc.

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

AMAZON BANNED WORDS — NEVER recommend using these (instant listing suppression):
antimicrobial, antibacterial, antifungal, antiviral, kills bacteria, eliminates odor-causing bacteria,
pesticide, repels insects, UV protection (unless certified), SPF, sunscreen, medical claims,
treats, prevents, cures, heals, clinically proven, dermatologist tested (unless certified)
If the listing uses any banned word — flag it as HIGH priority risk, do NOT recommend adding it.

RECOMMENDATION PLACEMENT RULES — every rec must specify WHERE to implement:
- Title: "Add to Title: '...'"
- Bullet: "Rewrite Bullet #N: '...'"  
- A+ module: "Add A+ module: '...'"
- A+ carousel: "Add carousel with: '...'"
- Description: "Add to Description: '...'"
NEVER give a vague rec like "mention X somewhere" — always specify exact placement.

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


def db_save_competitor(casin, cdata, cai, cvision, cimgs, caplus_urls, caplus_vision, our_asin, marketplace="com"):
    """Save competitor analysis as a separate row in listing_analysis"""
    conn = get_db()
    if not conn: return False
    if not cai or not isinstance(cai, dict): return False
    if pct(cai.get("overall_score", 0)) == 0: return False
    try:
        # Compress images
        _imgs_to_save = []
        for _img_d in (cimgs or [])[:5]:
            try:
                _ib = base64.b64decode(_img_d["b64"])
                _pil = Image.open(io.BytesIO(_ib)).convert("RGB")
                _pil.thumbnail((300, 300))
                _tb = io.BytesIO(); _pil.save(_tb, "JPEG", quality=55); _tb.seek(0)
                _imgs_to_save.append({"b64": base64.b64encode(_tb.read()).decode(), "media_type": "image/jpeg"})
            except: pass

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO listing_analysis
              (asin, listing_type, overall_score, title_score, bullets_score, images_score,
               aplus_score, cosmo_score, rufus_score, result_json, vision_text,
               our_title, our_data_json, marketplace, images_json, aplus_img_urls_json, aplus_vision_text)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            casin, 'конкурент',
            pct(cai.get("overall_score",0)),
            pct(cai.get("title_score",0)),
            pct(cai.get("bullets_score",0)),
            pct(cai.get("images_score",0)),
            pct(cai.get("aplus_score",0)),
            0, 0,
            json.dumps(cai, ensure_ascii=False),
            cvision or "",
            cdata.get("title","")[:200],
            json.dumps(cdata, ensure_ascii=False),
            marketplace,
            json.dumps(_imgs_to_save, ensure_ascii=False),
            json.dumps(caplus_urls or [], ensure_ascii=False),
            caplus_vision or ""
        ))
        conn.commit(); conn.close()
        return True
    except Exception as _e:
        return False


def fetch_offers(asin, domain="com", log=None):
    """Fetch all sellers/offers for an ASIN via ScrapingDog Offers API"""
    sd_key = st.secrets.get("SCRAPINGDOG_API_KEY","")
    if not sd_key: return None
    _log = log or (lambda m: None)
    _country_map = {"com":"us","co.uk":"gb","ca":"ca","de":"de","es":"es",
                    "fr":"fr","it":"it","nl":"nl","se":"se","pl":"pl","com.au":"au"}
    try:
        _log(f"💰 Offers API: {asin} [{domain}]...")
        r = requests.get("https://api.scrapingdog.com/amazon/offers",
            params={"api_key": sd_key, "asin": asin, "domain": domain,
                    "country": _country_map.get(domain,"us")},
            timeout=30)
        if r.ok:
            data = r.json()
            _log(f"✅ Offers: {len(data.get('offers',[]))} продавцов")
            return data
        else:
            _log(f"⚠️ Offers API: {r.status_code}")
            return None
    except Exception as e:
        _log(f"⚠️ Offers: {e}")
        return None


def fetch_autocomplete(prefix, domain="com", language="en"):
    """Fetch Amazon search autocomplete suggestions via ScrapingDog"""
    sd_key = st.secrets.get("SCRAPINGDOG_API_KEY","")
    if not sd_key or not prefix or len(prefix) < 2: return []
    _lang_map = {"de":"de","fr":"fr","it":"it","es":"es","nl":"nl","co.uk":"en","ca":"en","com":"en"}
    try:
        r = requests.get("https://api.scrapingdog.com/amazon/autocomplete",
            params={"api_key": sd_key, "prefix": prefix, "domain": domain,
                    "language": _lang_map.get(domain, language)},
            timeout=10)
        if r.ok:
            data = r.json()
            if isinstance(data, list):
                return [item.get("keyword","") for item in data if item.get("keyword")]
        return []
    except: return []


def db_lookup_asin(asin):
    """Check if ASIN exists in our DB - as our listing or competitor"""
    conn = get_db()
    if not conn: return []
    try:
        cur = conn.cursor()
        # Search by main asin column AND inside our_data_json (handles parent/child ASIN differences)
        cur.execute("""
            SELECT asin, our_title, overall_score, analyzed_at, listing_type,
                   COALESCE(marketplace,'com') as marketplace, our_data_json
            FROM listing_analysis
            WHERE asin = %s
               OR our_data_json::text ILIKE %s
            ORDER BY analyzed_at DESC LIMIT 5
        """, (asin, f'%"_input_asin": "{asin}"%'))
        rows = cur.fetchall()
        if not rows:
            # Also try searching in our_title for ASIN
            cur.execute("""
                SELECT asin, our_title, overall_score, analyzed_at, listing_type,
                       COALESCE(marketplace,'com') as marketplace, our_data_json
                FROM listing_analysis
                WHERE our_data_json::text ILIKE %s
                ORDER BY analyzed_at DESC LIMIT 5
            """, (f'%{asin}%',))
            rows = cur.fetchall()
        conn.close()
        result_list = []
        for r in rows:
            _mu = ""
            try:
                import json as _jl
                _od = _jl.loads(r[6]) if r[6] else {}
                _mu = _od.get("_model_used","")
                _dur = _od.get("_analysis_duration_sec", 0)
            except: pass
            result_list.append({"asin":r[0],"title":r[1],"score":r[2],"date":r[3],"type":r[4],"marketplace":r[5],"model_used":_mu,"duration":_dur})
        return result_list
    except Exception as e:
        return []


def claid_generate_lifestyle(image_b64, scene="outdoor lifestyle", media_type="image/jpeg"):
    """Generate lifestyle photo via Gemini Nano Banana Pro (gemini-3-pro-image-preview).
    Step 1: Remove background (keep product/person)
    Step 2: Generate new scene with product placed in it
    """
    gemini_key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("GOOGLE_API_KEY","")
    if not gemini_key: return None, "GEMINI_API_KEY не найден в Secrets"
    try:
        import base64 as _b64g
        import io as _iog
        import json as _jsong

        _img_bytes = _b64g.b64decode(image_b64)
        _ext = "jpeg" if "jpeg" in media_type else "png"

        # Gemini API — image editing with scene description
        _prompt = f"""You are a professional Amazon product photographer.

Take this product image and place it in a new scene: {scene}

Requirements:
- Keep the product/person exactly as-is (same pose, clothing, proportions)
- Replace only the background with the new scene
- Maintain professional product photography quality
- The result should look like a real lifestyle photo
- Amazon-ready image quality
- Do NOT add text, logos or watermarks"""

        _payload = {
            "contents": [{
                "parts": [
                    {
                        "inline_data": {
                            "mime_type": media_type,
                            "data": image_b64
                        }
                    },
                    {"text": _prompt}
                ]
            }],
            "generationConfig": {
                "responseModalities": ["IMAGE", "TEXT"]
            }
        }

        # Try models in order: latest → fallback
        # Актуальные модели на апрель 2026:
        # gemini-3-pro-image-preview ОТКЛЮЧЁН 9 марта 2026
        # Берём из глобального списка если доступен
        _img_key = st.secrets.get("GEMINI_API_KEY","")
        _avail_img = get_available_gemini_models(_img_key) if _img_key else []
        _models_to_try = [m for m in GEMINI_IMAGE_MODELS if m in _avail_img] or GEMINI_IMAGE_MODELS
        _r = None
        _last_err = ""
        for _model_id in _models_to_try:
            _r = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{_model_id}:generateContent?key={gemini_key}",
                headers={"Content-Type": "application/json"},
                json=_payload,
                timeout=120
            )
            if _r.ok:
                break
            _last_err = f"{_model_id}: {_r.status_code}"

        if not _r or not _r.ok:
            return None, f"Gemini error: {_last_err} | {_r.text[:200] if _r else 'no response'}"

        _result = _r.json()
        # Extract base64 images from response
        _imgs_b64 = []
        for _cand in _result.get("candidates", []):
            for _part in _cand.get("content",{}).get("parts",[]):
                if _part.get("inlineData",{}).get("mimeType","").startswith("image"):
                    _imgs_b64.append({
                        "b64": _part["inlineData"]["data"],
                        "mt": _part["inlineData"]["mimeType"]
                    })
        if _imgs_b64:
            return _imgs_b64, None
        return None, f"No images in response: {str(_result)[:300]}"
    except Exception as e:
        return None, str(e)


def db_get_prev_analysis(asin):
    """Get previous analysis for context injection"""
    conn = get_db()
    if not conn or not asin: return None
    try:
        import json as _j2
        cur = conn.cursor()
        cur.execute("""
            SELECT overall_score, analyzed_at, result_json
            FROM listing_analysis
            WHERE (asin = %s OR our_data_json::text ILIKE %s)
            AND listing_type = 'наш'
            ORDER BY analyzed_at DESC LIMIT 1
        """, (asin, f'%{asin}%'))
        row = cur.fetchone(); conn.close()
        if not row: return None
        _result = {}
        try: _result = _j2.loads(row[2]) if row[2] else {}
        except: pass
        _date = row[1].strftime("%d.%m.%Y") if row[1] else "—"
        return {"score": row[0] or 0, "date": _date, "result": _result}
    except: return None


def run_analysis(our_url, competitor_urls, log, prog=None):
    import time as _time_mod
    _t_analysis_begin = _time_mod.time()
    _steps_done = []
    def _prog(pct, text):
        if prog:
            _steps_done.append(text)
            prog.progress(min(pct/100, 1.0), text=f"[{pct}%] {text}")
        log(text)

    asin = get_asin(our_url) or "unknown"
    _lang = st.session_state.get("analysis_lang","ru")
    _mp = "com"
    for _d in ["co.uk","de","fr","it","es","nl","se","pl","com.be","com.mx","com.au","ca","com"]:
        if f"amazon.{_d}" in our_url:
            _mp = _d; break
    st.session_state["_marketplace"] = _mp

    _do_vision      = st.session_state.get("do_vision", True)
    _do_aplus       = st.session_state.get("do_aplus_vision", True)
    _do_comp_vision = st.session_state.get("do_comp_vision", True)

    if not our_url.strip() or asin == "unknown":
        log("ℹ️ НАШ листинг не указан — анализируем только конкурентов")
        our_data = {}
        images = []
        vision_result = ""
        st.session_state["images"] = []
        st.session_state["aplus_vision"] = ""
        st.session_state["aplus_img_urls"] = []
        st.session_state["our_data"] = {}
    else:
        _prog(5,  f"🌐 Загружаю данные листинга {asin}...")
        our_data, img_urls = scrapingdog_product(asin, log, domain=_mp)
        _prog(12, f"✅ Данные получены — {len(our_data.get('feature_bullets',[]))} буллетов, {len(img_urls)} фото")

        _prog(15, f"⬇️ Скачиваю фото ({len(img_urls)} шт.)...")
        images = download_images(img_urls, log) if img_urls else []
        st.session_state["images"] = images
        _prog(22, f"✅ Фото скачаны: {len(images)} шт. готовы к анализу")

        if images and _do_vision:
            _prog(25, f"👁️ Vision AI: анализирую фото 1/{len(images)}...")
            vision_result = analyze_vision(images, our_data, asin, log, lang=_lang)
            _prog(33, f"✅ Vision готов: {len(images)} фото проанализировано")
        else:
            vision_result = ""
            if not images:
                log("⚠️ Фото не загружены")
            else:
                log("⏭️ Vision фото пропущен (отключён)")

        _aplus_urls = our_data.get("aplus_image_urls", our_data.get("aplus_images", []))
        _aplus_urls = [re.sub(r'\.__CR[^.]+_PT0_SX\d+_V\d+___', '', u) if isinstance(u,str) else u for u in _aplus_urls if isinstance(u,str) and u.startswith("http")]
        if _aplus_urls and _do_aplus:
            _prog(35, f"🎨 A+ Vision: анализирую {len(_aplus_urls)} баннеров...")
            aplus_vision = analyze_aplus_vision(_aplus_urls, our_data, log, lang=_lang)
            st.session_state["aplus_vision"] = aplus_vision
            _prog(42, f"✅ A+ Vision готов: {len(_aplus_urls)} баннеров")
        else:
            st.session_state["aplus_vision"] = ""
            if _aplus_urls and not _do_aplus:
                log("⏭️ A+ Vision пропущен (отключён)")
            else:
                log("ℹ️ A+ баннеры не найдены")
        st.session_state["aplus_img_urls"] = _aplus_urls

    active = [u.strip() for u in competitor_urls if u.strip()]
    comp_data_list = []
    n_active = max(len(active), 1)

    for i, url in enumerate(active[:3]):
        casin = get_asin(url)
        if not casin: continue
        base_pct = 45 + i * 10

        _prog(base_pct,     f"🌐 Конкурент {i+1}/{len(active)}: загружаю {casin}...")
        _comp_mp = "com"
        for _cd2 in ["co.uk","de","fr","it","es","nl","se","pl","com.be","ca","com"]:
            if f"amazon.{_cd2}" in url:
                _comp_mp = _cd2; break
        cdata, cimg_urls = scrapingdog_product(casin, log, domain=_comp_mp)
        cdata["_input_asin"] = casin
        comp_data_list.append(cdata)
        _prog(base_pct + 2, f"✅ Конкурент {i+1}: данные получены — {cdata.get('title','')[:30]}...")

        _prog(base_pct + 3, f"⬇️ Конкурент {i+1}: скачиваю фото...")
        cimgs_dl = download_images(cimg_urls[:5], log) if cimg_urls else []

        if cimgs_dl and _do_comp_vision:
            _prog(base_pct + 5, f"👁️ Конкурент {i+1}: Vision {len(cimgs_dl)} фото...")
            cvision = analyze_vision(cimgs_dl, cdata, casin, log, lang=_lang)
            _prog(base_pct + 6, f"✅ Конкурент {i+1}: Vision готов")
        else:
            cvision = ""
            if cimgs_dl:
                log(f"⏭️ Vision конкурент {i+1} пропущен (отключён)")

        _cap_urls = cdata.get("aplus_image_urls", [])
        if _cap_urls and _do_comp_vision:
            _prog(base_pct + 7, f"🎨 Конкурент {i+1}: A+ Vision ({len(_cap_urls)} баннеров)...")
            _caplus_vision = analyze_aplus_vision(_cap_urls, cdata, log, lang=_lang)
            st.session_state[f"comp_aplus_vision_{i}"] = _caplus_vision
            st.session_state[f"comp_aplus_urls_{i}"] = _cap_urls
        else:
            st.session_state[f"comp_aplus_vision_{i}"] = ""
            st.session_state[f"comp_aplus_urls_{i}"] = _cap_urls

        _prog(base_pct + 8, f"🧠 Конкурент {i+1}: AI скоринг листинга...")
        cai = analyze_text(cdata, [], cvision, casin, log, lang=_lang, is_competitor=True)
        _prog(base_pct + 9, f"✅ Конкурент {i+1}: готов — Overall {pct(cai.get('overall_score',0))}%")

        st.session_state[f"comp_ai_{i}"] = cai
        if cimgs_dl:
            st.session_state[f"comp_vision_{i}"] = (cimgs_dl, cvision)
        # Save competitor to DB
        _cap_urls_save = st.session_state.get(f"comp_aplus_urls_{i}", [])
        _cav_save = st.session_state.get(f"comp_aplus_vision_{i}", "")
        db_save_competitor(casin, cdata, cai, cvision, cimgs_dl, _cap_urls_save, _cav_save, asin, marketplace=_comp_mp)

    _prog(78, "🧠 AI финальный анализ — COSMO + Rufus + JTBD + VPC...")
    result = analyze_text(our_data, comp_data_list, vision_result, asin, log, lang=_lang)
    _prog(92, "💾 Сохраняю результаты в историю...")
    st.session_state['our_data'] = our_data
    st.session_state['comp_data_list'] = comp_data_list
    _total_sec = int(__import__("time").time() - _t_analysis_begin)
    our_data["_analysis_duration_sec"] = _total_sec
    _prog(98, f"✅ Анализ завершён за {_total_sec//60}м {_total_sec%60}с!")
    return result, vision_result

# ── UI ────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Listing Analyzer", page_icon="https://merino.tech/cdn/shop/files/MT_logo_1.png?v=1685099753&width=260", layout="wide")

with st.sidebar:
    _logo_col, _refresh_col = st.columns([4,1])
    with _logo_col:
        st.image("https://merino.tech/cdn/shop/files/MT_logo_1.png?v=1685099753&width=260", width=100)
        st.markdown("## <span style='color:#c0392b'>Listing Analyzer</span>", unsafe_allow_html=True)
    with _refresh_col:
        st.markdown("<br><br>", unsafe_allow_html=True)
        if st.button("🔄", key="refresh_sidebar", help="Обновить страницу"):
            st.rerun()

    # ── USER BADGE ──────────────────────────────────────────────────────────
    _u = st.session_state.get("user", {})
    if _u:
        _role_icon = "👑" if _u.get("role") == "admin" else "👤"
        st.markdown(
            f'<div style="background:#1e293b;border-radius:8px;padding:8px 12px;margin-bottom:4px">'
            f'<div style="font-size:0.78rem;font-weight:700;color:#e2e8f0">{_role_icon} {_u.get("name","")}</div>'
            f'<div style="font-size:0.68rem;color:#64748b">{_u.get("email","")}</div>'
            f'</div>', unsafe_allow_html=True)
        if st.button("🚪 Выйти", key="la_logout", use_container_width=True):
            logout()

    st.divider()
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
        ("🎯", "VPC / JTBD"),
        ("🔥", "Топ ниши"),
        ("📱", "Mobile Score"),
        ("ℹ️", "О инструменте"),
        ("📖", "Документация"),
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
        cur_no_result = st.session_state.get("page","")
        for icon, label in NAV_ITEMS:
            full = f"{icon} {label}"
            _always = label in ["Топ ниши", "Mobile Score", "О инструменте", "Документация"]
            if _always:
                is_active = (cur_no_result == full)
                if st.button(f"{icon}  {label}", key=f"nav_pre_{label}", use_container_width=True,
                             type="primary" if is_active else "secondary"):
                    st.session_state["page"] = full
                    st.rerun()
            else:
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
    # ── ADMIN ───────────────────────────────────────────────────────────────
    if st.session_state.get("user", {}).get("role") == "admin":
        if st.button("👑 Admin", key="nav_admin", use_container_width=True,
                     type="primary" if _cur3=="👑 Admin" else "secondary"):
            st.session_state["page"] = "👑 Admin"
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
        ["🥇 Лучшее качество (Claude)", "⚡ Быстро и дёшево (Gemini)"],
        horizontal=False, key="model_choice", label_visibility="collapsed",
        help="Claude — точнее анализирует фото и текст. Gemini — в 10x дешевле, подходит для частых тестов."
    )
    st.session_state["use_gemini"] = "Gemini" in _model_choice
    if st.session_state.get("use_gemini"):
        _key_for_check = st.secrets.get("GEMINI_API_KEY","")
        if _key_for_check:
            get_best_gemini_model.clear()
            _actual_model = get_best_gemini_model(_key_for_check, prefer_pro=True)
            st.session_state["gemini_model"] = _actual_model
            st.caption(f"✅ Авто: `{_actual_model}`")
        else:
            st.session_state["gemini_model"] = "gemini-3.1-pro-preview"
            st.caption("⚠️ Добавь GEMINI_API_KEY в Secrets")

    st.divider()
    st.markdown("**🔑 API**")
    _gkey_check = st.secrets.get("GEMINI_API_KEY","")
    if _gkey_check:
        if st.button("🔍 Проверить Gemini tier", key="btn_check_gemini", use_container_width=True):
            with st.spinner("Проверяю..."):
                _tier_result = check_gemini_tier(_gkey_check)
                _avail = get_available_gemini_models(_gkey_check)
            if _tier_result.get("tier") == "free" or _tier_result.get("status") == "429":
                st.error("🆓 Бесплатный — будут лимиты 429")
            elif _tier_result.get("status") in ("ok","ok_paid"):
                st.success(f"✅ Платный — лучшая модель: `{get_best_gemini_model(_gkey_check)}`")
            else:
                st.warning(f"⚠️ {_tier_result.get('msg','?')}")
    _ac1, _ac2 = st.columns(2)
    if _ac1.button("🧪 Claude", key="api_test"):
        try:
            res = anthropic_call(None, "Say: OK", max_tokens=5)
            st.success(f"✅ Claude: {res}")
        except Exception as e:
            st.error(f"❌ {str(e)[:60]}")
    if _ac2.button("🧪 Gemini", key="api_test_gem"):
        get_available_gemini_models.clear()
        get_best_gemini_model.clear()
        _key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("GOOGLE_API_KEY","")
        st.caption(f"Ключ: `...{_key[-8:] if _key else 'НЕТ'}`")
        for _ep in ["v1", "v1beta"]:
            try:
                _r = requests.get(f"https://generativelanguage.googleapis.com/{_ep}/models?key={_key}", timeout=10)
                if _r.ok:
                    _names = [m["name"] for m in _r.json().get("models",[]) if "generateContent" in m.get("supportedGenerationMethods",[])]
                    _best = get_best_gemini_model(_key)
                    st.success(f"✅ Gemini OK — {len(_names)} моделей доступно, используем: `{_best}`")
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

    our_url = st.text_input("🔵 НАШ листинг", value=st.session_state.get("our_url_saved",""), placeholder="https://www.amazon.com/dp/...")
    if our_url.strip() and len(our_url.strip()) > 10:
        _om = re.search(r'/dp/([A-Z0-9]{10})', our_url, re.IGNORECASE)
        if _om:
            _oasin = _om.group(1).upper()
            _ofound = db_lookup_asin(_oasin)
            if _ofound:
                _of = _ofound[0]
                _ofc = "#22c55e" if _of["score"]>=75 else ("#f59e0b" if _of["score"]>=50 else "#ef4444")
                _oft = "🔵 НАШ" if _of["type"]=="наш" else "🔴 Конкурент"
                _ofmp = {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸","nl":"🇳🇱"}.get(_of.get("marketplace","com"),"🌍")
                _ofdate = _of["date"].strftime("%d.%m.%Y") if _of.get("date") else "—"
                _oh_c1, _oh_c2 = st.columns([5,1])
                with _oh_c1:
                    st.markdown(
                        f'<div style="background:#0f172a;border-left:3px solid {_ofc};border-radius:6px;padding:7px 12px;margin-top:3px;display:flex;justify-content:space-between;align-items:center">' +
                        f'<div><span style="font-size:0.75rem;font-weight:700;color:{_ofc}">{_oft} {_ofmp}</span>' +
                        f'<span style="font-size:0.7rem;color:#64748b;margin-left:8px">{_ofdate}</span>' +
                        f'<div style="font-size:0.75rem;color:#94a3b8">{str(_of.get("title") or "")[:50]}</div></div>' +
                        f'<div style="font-size:1.2rem;font-weight:800;color:{_ofc}">{_of["score"]}%</div></div>',
                        unsafe_allow_html=True)
                with _oh_c2:
                    if st.button("📂", key="btn_open_our_hist", help="Открыть этот анализ", use_container_width=True):
                        _conn_oh = get_db()
                        if _conn_oh:
                            try:
                                _cur_oh = _conn_oh.cursor()
                                _cur_oh.execute("""SELECT result_json, our_data_json, images_json, aplus_img_urls_json, aplus_vision_text
                                    FROM listing_analysis WHERE (asin=%s OR our_data_json::text ILIKE %s)
                                    AND listing_type='наш' ORDER BY analyzed_at DESC LIMIT 1""",
                                    (_of["asin"], f'%{_of["asin"]}%'))
                                _row_oh = _cur_oh.fetchone(); _conn_oh.close()
                                if _row_oh:
                                    if _row_oh[0]:
                                        try: st.session_state["result"] = json.loads(_row_oh[0])
                                        except: pass
                                    if _row_oh[1]:
                                        try: st.session_state["our_data"] = json.loads(_row_oh[1])
                                        except: pass
                                    if _row_oh[2]:
                                        try: st.session_state["images"] = json.loads(_row_oh[2])
                                        except: pass
                                    if _row_oh[3]:
                                        try: st.session_state["aplus_img_urls"] = json.loads(_row_oh[3])
                                        except: pass
                                    if _row_oh[4]: st.session_state["aplus_vision"] = _row_oh[4]
                                    st.session_state["page"] = "🏠 Обзор"
                                    st.rerun()
                            except Exception as _oe: st.error(str(_oe)[:100])
            else:
                st.markdown('<div style="font-size:0.72rem;color:#64748b;margin-top:2px">🆕 Новый листинг — ещё не анализировался</div>', unsafe_allow_html=True)

    # Auto-check history when URL/ASIN entered
    if our_url and our_url.strip():
        _auto_m = re.search(r'/dp/([A-Z0-9]{10})', our_url, re.IGNORECASE)
        _auto_asin = _auto_m.group(1).upper() if _auto_m else (our_url.strip().upper() if len(our_url.strip()) == 10 else "")
        if _auto_asin and len(_auto_asin) == 10:
            _auto_found = db_lookup_asin(_auto_asin)
            if _auto_found:
                _mp_flags3 = {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸","nl":"🇳🇱"}
                for _af in _auto_found[:1]:
                    _afc = "#3b82f6" if _af["type"]=="наш" else "#ef4444"
                    _aft = "🔵 УЖЕ В ИСТОРИИ (НАШ)" if _af["type"]=="наш" else "🔴 УЖЕ В ИСТОРИИ (Конкурент)"
                    _afs = _af.get("score",0) or 0
                    _afsc = "#22c55e" if _afs>=75 else ("#f59e0b" if _afs>=50 else "#ef4444")
                    _afdate = _af["date"].strftime("%d.%m.%Y %H:%M") if _af.get("date") else "—"
                    st.markdown(
                        f'<div style="background:#f0f9ff;border-left:4px solid {_afc};border-radius:6px;'
                        f'padding:8px 12px;display:flex;justify-content:space-between;align-items:center">'
                        f'<div style="font-size:0.8rem">'
                        f'<b style="color:{_afc}">{_aft}</b> · {_mp_flags3.get(_af.get("marketplace","com"),"🌍")} {_auto_asin} · {_afdate}' +
                        (f' &nbsp;·&nbsp; <span style="color:#22c55e">⬤ Gemini</span>' if _af.get("model_used","").startswith("Gemini") else (f' &nbsp;·&nbsp; <span style="color:#a78bfa">⚡ Claude</span>' if _af.get("model_used","").startswith("Claude") else "")) +
                        f'<br><span style="color:#475569">{(_af.get("title") or "")[:60]}</span></div>'
                        f'<div style="font-size:1.2rem;font-weight:800;color:{_afsc}">{_afs}%</div>'
                        f'</div>',
                        unsafe_allow_html=True)
            else:
                st.caption(f"✨ {_auto_asin} — новый, ещё не анализировался")

    c1, c2, c3, c4, c5 = st.columns(5)
    _comp_vals = []
    _mp_flags_c = {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸","nl":"🇳🇱","se":"🇸🇪","pl":"🇵🇱"}
    for _ci2, (_cc2, _clbl) in enumerate(zip([c1,c2,c3,c4,c5], ["Конкурент 1","Конкурент 2","Конкурент 3","Конкурент 4","Конкурент 5"])):
        with _cc2:
            _cv2 = st.text_input(_clbl, key=f"c{_ci2}", value=st.session_state.get(f"c{_ci2}_saved",""), placeholder="https://www.amazon.com/dp/...")
            _comp_vals.append(_cv2)
            if _cv2.strip() and len(_cv2.strip()) > 10:
                _cm2 = re.search(r'/dp/([A-Z0-9]{10})', _cv2, re.IGNORECASE)
                if _cm2:
                    _casin2 = _cm2.group(1).upper()
                    _cfound2 = db_lookup_asin(_casin2)
                    if _cfound2:
                        _cf2 = _cfound2[0]
                        _cfc2 = "#22c55e" if _cf2["score"]>=75 else ("#f59e0b" if _cf2["score"]>=50 else "#ef4444")
                        _cft2 = "🔵 НАШ" if _cf2["type"]=="наш" else "🔴 Конкурент"
                        _cfmp2 = _mp_flags_c.get(_cf2.get("marketplace","com"),"🌍")
                        st.markdown(
                            (f'<div style="background:#0f172a;border-radius:6px;padding:5px 8px;margin-top:2px">' +
                            f'<div style="font-size:0.68rem"><span style="color:{_cfc2}">{_cft2} {_cfmp2}</span>' +
                            f'<b style="color:{_cfc2};margin-left:6px">{_cf2["score"]}%</b></div>' +
                            f'<div style="font-size:0.65rem;color:#94a3b8">{str(_cf2.get("title") or "")[:28]}...</div></div>'),
                            unsafe_allow_html=True)
                    else:
                        st.markdown('<div style="font-size:0.68rem;color:#64748b;margin-top:2px">🆕 Новый — не анализировался</div>', unsafe_allow_html=True)
    comp1,comp2,comp3,comp4,comp5 = _comp_vals
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

        _t_str, _c_str = estimate_run(_do_vision, _do_aplus, _do_comp_vision, _n_comps, _use_gem)

        _what_on = []
        if _do_vision:      _what_on.append("Vision фото")
        if _do_aplus:       _what_on.append("A+")
        if _do_comp_vision and _n_comps: _what_on.append(f"Vision {_n_comps} конк.")
        _mode = " + ".join(_what_on) if _what_on else "только текст"

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

    with st.expander("🎯 Фокус анализа (Дополнительно)", expanded=False):
        st.caption("Помогает AI расставить приоритеты и оценивать фото под нужную аудиторию")
        _fa1, _fa2, _fa3 = st.columns(3)
        with _fa1:
            goal = st.radio("🎯 Цель", [
                "Полный аудит", "Поднять конверсию",
                "Выйти в топ поиска", "Победить конкурента",
            ], key="goal_sel")
        with _fa2:
            positioning = st.radio("💰 Позиционирование", [
                "Не указано", "Бюджет", "Средний сегмент", "Премиум",
            ], key="pos_sel")
        with _fa3:
            st.markdown('<div style="font-size:0.85rem;font-weight:600;margin-bottom:4px">👤 Целевая аудитория</div>', unsafe_allow_html=True)
            audience_custom = st.text_input(
                "Аудитория", key="aud_custom",
                value=st.session_state.get("aud_custom_saved",""),
                placeholder="Женщина, 45 лет, активный образ жизни",
                label_visibility="collapsed"
            )
            st.caption("Влияет на Vision-анализ фото")

        _ctx_parts = [f"Analysis goal: {goal}"]
        if audience_custom.strip():
            _ctx_parts.append(f"Target audience: {audience_custom.strip()}")
            st.session_state["aud_custom_saved"] = audience_custom.strip()
        if positioning != "Не указано": _ctx_parts.append(f"Brand positioning: {positioning}")
        st.session_state["ai_context"] = " | ".join(_ctx_parts)
        st.session_state["target_audience"] = audience_custom.strip()

    _has_any_url = bool(our_url.strip()) or any(u.strip() for u in competitor_urls)
    _bcol1, _bcol2 = st.columns([3, 1])
    with _bcol1:
        _run_btn = st.button(btn_label, type="primary", disabled=not _has_any_url, use_container_width=True)
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
                # Auto-navigate: if no OUR listing but competitors exist → go to first competitor
                if not our_url.strip() and any(u.strip() for u in competitor_urls):
                    st.session_state["page"] = "🔴 Конкурент 1"
                try:
                    _od = st.session_state.get("our_data", {})
                    # Save input ASIN from URL for better lookup later
                    _input_asin_save = get_asin(our_url) or get_asin_from_data(_od)
                    if _input_asin_save: _od["_input_asin"] = _input_asin_save
                    # Сохраняем какой моделью сделан анализ
                    _model_used = "Gemini/" + st.session_state.get("gemini_model","gemini-2.5-flash") if st.session_state.get("use_gemini") else "Claude/" + ANTHROPIC_MODEL_VISION
                    _od["_model_used"] = _model_used
                    _saved = db_save(get_asin_from_data(_od) or _input_asin_save, result,
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
                            _add_mp = "com"
                            for _dm in ["co.uk","de","fr","it","es","nl","se","pl","com.be","ca","com"]:
                                if f"amazon.{_dm}" in url:
                                    _add_mp = _dm; break
                            cdata, cimg_urls = scrapingdog_product(casin, log, domain=_add_mp)
                            cimgs_add = download_images(cimg_urls[:5], log) if cimg_urls else []
                            if i < len(existing): existing[i] = cdata
                            else: existing.append(cdata)
                            st.session_state[f"c{i}_saved"] = url.strip()
                            # Save to DB
                            _cai_add = st.session_state.get(f"comp_ai_{i}", {})
                            db_save_competitor(casin, cdata, _cai_add, "", cimgs_add, [], "", "", marketplace=_add_mp)
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
                    # Save input ASIN from URL for better lookup later
                    _input_asin_save = get_asin(our_url) or get_asin_from_data(_od)
                    if _input_asin_save: _od["_input_asin"] = _input_asin_save
                    # Сохраняем какой моделью сделан анализ
                    _model_used = "Gemini/" + st.session_state.get("gemini_model","gemini-2.5-flash") if st.session_state.get("use_gemini") else "Claude/" + ANTHROPIC_MODEL_VISION
                    _od["_model_used"] = _model_used
                    _saved = db_save(get_asin_from_data(_od) or _input_asin_save, result,
                            st.session_state.get("vision",""), _od.get("title",""))
                    log("💾 Сохранено в историю" if _saved else f"⚠️ БД ошибка")
                except Exception as _dbe:
                    log(f"⚠️ БД исключение: {_dbe}")

            _main_prog.progress(100, text="✅ Анализ завершён!")
            st.rerun()
        except Exception as e:
            st.error(f"Ошибка: {e}")


def db_all_competitors():
    """Extract all unique competitors from competitors_json field"""
    conn = get_db()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT asin, competitors_json, analyzed_at, our_title
            FROM listing_analysis
            WHERE competitors_json IS NOT NULL AND competitors_json != '[]'
            ORDER BY analyzed_at DESC
            LIMIT 50
        """)
        rows = cur.fetchall()
        conn.close()
        seen = {}
        for asin, comp_json, date, our_title in rows:
            try:
                comps = json.loads(comp_json) if comp_json else []
                for c in comps:
                    casin = c.get("asin","")
                    if not casin or casin in seen: continue
                    seen[casin] = {
                        "asin": casin,
                        "title": c.get("title",""),
                        "score": c.get("overall",0),
                        "price": c.get("price",""),
                        "rating": c.get("rating",""),
                        "reviews": c.get("reviews",""),
                        "analyzed_with": asin,
                        "our_title": our_title,
                        "date": date,
                    }
            except: pass
        return list(seen.values())
    except Exception as e:
        return []

def db_all_competitors():
    conn = get_db()
    if not conn: return []
    try:
        cur = conn.cursor()
        # New-style: saved as listing_type='конкурент'
        cur.execute("""
            SELECT DISTINCT ON (asin) asin, our_title, overall_score, analyzed_at,
                   result_json, our_data_json, images_json, aplus_img_urls_json, aplus_vision_text, vision_text,
                   COALESCE(marketplace,'com') as marketplace
            FROM listing_analysis
            WHERE listing_type = 'конкурент'
            ORDER BY asin, analyzed_at DESC
        """)
        rows = cur.fetchall()
        result = []
        seen_asins = set()
        for r in rows:
            _price, _rating, _reviews = "", "", ""
            if r[5]:
                try:
                    _od = json.loads(r[5])
                    _price = _od.get("price","")
                    _rating = str(_od.get("average_rating",""))
                    _reviews = str(_od.get("product_information",{}).get("Customer Reviews",{}).get("ratings_count","") or _od.get("reviews_count",""))
                except: pass
            seen_asins.add(r[0])
            result.append({
                "asin": r[0], "title": r[1], "score": r[2], "date": r[3],
                "price": _price, "rating": _rating, "reviews": _reviews,
                "result_json": r[4], "our_data_json": r[5],
                "images_json": r[6], "aplus_urls_json": r[7],
                "aplus_vision": r[8], "vision_text": r[9],
                "marketplace": r[10] if len(r) > 10 else "com",
            })
        # Old-style: from competitors_json field
        cur.execute("""
            SELECT asin, competitors_json, analyzed_at, our_title
            FROM listing_analysis
            WHERE competitors_json IS NOT NULL AND competitors_json != '[]'
            ORDER BY analyzed_at DESC LIMIT 100
        """)
        old_rows = cur.fetchall()
        conn.close()
        for _asin, comp_json, date, our_title in old_rows:
            try:
                for c in (json.loads(comp_json) if comp_json else []):
                    casin = c.get("asin","")
                    if not casin or casin in seen_asins: continue
                    seen_asins.add(casin)
                    result.append({
                        "asin": casin, "title": c.get("title",""), "score": c.get("overall",0),
                        "date": date, "price": c.get("price",""), "rating": c.get("rating",""),
                        "reviews": c.get("reviews",""), "our_title": our_title,
                        "result_json": None, "our_data_json": None,
                        "images_json": None, "aplus_urls_json": None,
                        "aplus_vision": None, "vision_text": None, "marketplace": "com",
                    })
            except: pass
        return result
    except: return []

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
    all_comps = db_all_competitors()
    if not all_asins and not all_comps:
        st.info("История пуста — запусти первый анализ")
        return
    _unique_asins_count = len(set(a["asin"] for a in all_asins))
    _tab_our, _tab_comp, _tab_bench = st.tabs([f"🔵 Наши ({_unique_asins_count})", f"🔴 Конкуренты ({len(all_comps)})", "📊 AI Benchmark"])

    # ══════════════════════════════════════════════════════════════════════
    # TAB: Конкуренты
    # ══════════════════════════════════════════════════════════════════════
    with _tab_comp:
        if not all_comps:
            st.info("Конкуренты появятся после анализа с конкурентами")
        else:
            _csearch = st.text_input("🔍", placeholder="ASIN или название", key="comp_hist_search", label_visibility="collapsed")
            _csearch_asin = _csearch
            if _csearch and "/dp/" in _csearch:
                _csm = re.search(r'/dp/([A-Z0-9]{10})', _csearch, re.IGNORECASE)
                if _csm: _csearch_asin = _csm.group(1)
            _fcomps = [c for c in all_comps if not _csearch or
                _csearch_asin.upper() in c["asin"].upper() or
                _csearch.lower() in (c.get("title") or "").lower()]
            for _cidx2, _ca in enumerate(_fcomps):
                _csc = _ca.get("score",0) or 0
                _csc_c = "#22c55e" if _csc>=75 else ("#f59e0b" if _csc>=50 else ("#ef4444" if _csc>0 else "#94a3b8"))
                _csc_l = "Strong" if _csc>=75 else ("Needs Work" if _csc>=50 else ("Critical" if _csc>0 else "—"))
                _cc1, _cc2, _cc3, _cc4, _cc5 = st.columns([1, 6, 2, 1.5, 0.8])
                with _cc1:
                    _c_img_url = ""
                    if _ca.get("our_data_json"):
                        try:
                            _cod = json.loads(_ca["our_data_json"])
                            _c_imgs = _cod.get("images_of_specified_asin", _cod.get("images",[]))
                            if _c_imgs: _c_img_url = next((u for u in _c_imgs if isinstance(u,str) and u.startswith("http")), "")
                        except: pass
                    if _c_img_url:
                        st.markdown(f'<img src="{_c_img_url}" width="56" height="56" style="object-fit:cover;border-radius:8px;border:1px solid #e2e8f0">', unsafe_allow_html=True)
                    else:
                        _pl = (_ca.get("title","?")[0]).upper()
                        st.markdown(f'<div style="width:56px;height:56px;background:#fee2e2;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:1.4rem;font-weight:800;color:#dc2626">{_pl}</div>', unsafe_allow_html=True)
                with _cc2:
                    _cdate = _ca["date"].strftime("%d.%m.%Y %H:%M") if _ca.get("date") else "—"
                    st.markdown(
                        f'<div style="padding:6px 0">' +
                        f'<div style="font-size:0.9rem;font-weight:600;color:#0f172a">{(_ca.get("title") or "")[:60]}</div>' +
                        f'<div style="font-size:0.78rem;color:#64748b;margin-top:3px">' +
                        f'{ {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸","nl":"🇳🇱","se":"🇸🇪","pl":"🇵🇱","com.be":"🇧🇪","com.mx":"🇲🇽","com.au":"🇦🇺"}.get(_ca.get("marketplace","com"),"🌍") }' +
                        f' &nbsp;·&nbsp; <a href="https://www.amazon.{_ca.get("marketplace","com")}/dp/{_ca["asin"]}" target="_blank" style="color:#3b82f6">{_ca["asin"]} ↗</a>' +
                        (f' · 💰{_ca["price"]}' if _ca.get("price") else "") +
                        (f' · ⭐{_ca["rating"]}' if _ca.get("rating") else "") +
                        (f' · ({_ca["reviews"]} отз.)' if _ca.get("reviews") else "") +
                        f' · {_cdate}' +
                        ((lambda od2: f' &nbsp;·&nbsp; <span style="color:#22c55e">⬤ Gemini</span>' if od2 and "Gemini" in od2.get("_model_used","") else (f' &nbsp;·&nbsp; <span style="color:#a78bfa">⚡ Claude</span>' if od2 and od2.get("_model_used") else ""))(json.loads(_ca["our_data_json"]) if _ca.get("our_data_json") else {})) +
                        f'</div></div>',
                        unsafe_allow_html=True)
                with _cc3:
                    if _csc>0:
                        st.markdown(f'<div style="text-align:center;padding:8px 0"><div style="font-size:1.5rem;font-weight:800;color:{_csc_c}">{_csc}%</div><div style="font-size:0.7rem;color:{_csc_c}">{_csc_l}</div></div>', unsafe_allow_html=True)
                with _cc4:
                    if st.button("Open", key=f"comp_hist_open_{_cidx2}", use_container_width=True, type="primary"):
                        if _ca.get("result_json"):
                            try: st.session_state["comp_ai_0"] = json.loads(_ca["result_json"])
                            except: pass
                        if _ca.get("our_data_json"):
                            try: st.session_state["comp_data_list"] = [json.loads(_ca["our_data_json"])]
                            except: pass
                        if _ca.get("images_json"):
                            try: st.session_state["comp_vision_0"] = (json.loads(_ca["images_json"]), _ca.get("vision_text",""))
                            except: pass
                        if _ca.get("aplus_urls_json"):
                            try: st.session_state["comp_aplus_urls_0"] = json.loads(_ca["aplus_urls_json"])
                            except: pass
                        if _ca.get("aplus_vision"):
                            st.session_state["comp_aplus_vision_0"] = _ca["aplus_vision"]
                        if "result" not in st.session_state:
                            st.session_state["result"] = {"overall_score": 0}
                        st.session_state["_hist_loaded"] = _ca["asin"]
                        st.session_state["page"] = "🔴 Конкурент 1"
                        st.rerun()
                with _cc5:
                    if st.button("🗑️", key=f"comp_hist_del_{_cidx2}", use_container_width=True, help="Удалить"):
                        _conn_del = get_db()
                        if _conn_del:
                            try:
                                _cur_del = _conn_del.cursor()
                                _cur_del.execute("DELETE FROM listing_analysis WHERE asin=%s AND listing_type='конкурент'", (_ca["asin"],))
                                _conn_del.commit(); _conn_del.close()
                                st.rerun()
                            except Exception as _de: st.error(f"{_de}")
                st.markdown('<hr style="margin:4px 0;border-color:#f1f5f9">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    # TAB: Наши
    # ══════════════════════════════════════════════════════════════════════
    with _tab_our:
        st.subheader(f"📋 Все листинги в базе — {len(all_asins)} шт.")
        import pandas as pd

        _search = st.text_input("🔍 Поиск по ASIN или названию", placeholder="B08M3D... или merino gaiter", key="hist_search", label_visibility="collapsed")

        _search_asin = _search
        if _search and "/dp/" in _search:
            _sm2 = re.search(r'/dp/([A-Z0-9]{10})', _search, re.IGNORECASE)
            if _sm2: _search_asin = _sm2.group(1)

        # Deduplicate — latest per ASIN
        _seen_asins_d = set()
        _deduped_asins = []
        for _a in all_asins:
            if _a["asin"] not in _seen_asins_d:
                _seen_asins_d.add(_a["asin"])
                _deduped_asins.append(_a)
        _all_versions_map = {}
        for _a in all_asins:
            _all_versions_map.setdefault(_a["asin"], []).append(_a)

        _filtered_asins = [a for a in _deduped_asins if not _search or
            _search_asin.upper() in a["asin"].upper() or
            _search.lower() in (a.get("title") or "").lower()]

        if st.session_state.get("_hist_select_asin"):
            _pre_asin = st.session_state.pop("_hist_select_asin")
        else:
            _pre_asin = None

        # ── MAIN LOOP: render each ASIN card ──────────────────────────────
        for _idx, _a in enumerate(_filtered_asins):
            _sc = _a.get("score") or 0
            _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else ("#ef4444" if _sc>0 else "#94a3b8"))
            _sc_lbl = "Strong" if _sc>=75 else ("Needs Work" if _sc>=50 else ("Critical" if _sc>0 else "—"))
            _title = (_a.get("title") or "")[:60]
            _asin = _a["asin"]
            _date = _a["date"].strftime("%d.%m.%Y %H:%M") if _a.get("date") else "—"
            _mp = _a.get("marketplace","com")
            _mp_flag = {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸","nl":"🇳🇱","se":"🇸🇪","pl":"🇵🇱","com.be":"🇧🇪","com.mx":"🇲🇽","com.au":"🇦🇺"}.get(_mp,"🌍")
            _ph_c = "#dcfce7" if _sc>=75 else ("#fef9c3" if _sc>=50 else ("#fee2e2" if _sc>0 else "#f1f5f9"))
            _ph_tc = "#15803d" if _sc>=75 else ("#d97706" if _sc>=50 else ("#dc2626" if _sc>0 else "#94a3b8"))
            _ph_letter = (_title[0] if len(_title)>0 else (_asin[0] if len(_asin)>0 else "?")).upper()

            _ci1, _ci2, _ci3, _ci4, _ci5 = st.columns([1, 6, 2, 1.5, 0.8])

            with _ci1:
                _img_url = _a.get("img","")
                if _img_url:
                    st.markdown(
                        f'<img src="{_img_url}" width="56" height="56" '
                        f'style="object-fit:cover;border-radius:8px;border:1px solid #e2e8f0" '
                        f'onerror="this.parentNode.innerHTML=\'<div style=&quot;width:56px;height:56px;background:{_ph_c};border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:1.4rem;font-weight:800;color:{_ph_tc}&quot;>{_ph_letter}</div>\'">', 
                        unsafe_allow_html=True)
                else:
                    st.markdown(
                        f'<div style="width:56px;height:56px;background:{_ph_c};border-radius:8px;'
                        f'display:flex;align-items:center;justify-content:center;'
                        f'font-size:1.4rem;font-weight:800;color:{_ph_tc}">'
                        f'{_ph_letter}</div>', unsafe_allow_html=True)

            with _ci2:
                _display_title = _title if _title else f"ASIN: {_asin}"
                st.markdown(
                    f'<div style="padding:6px 0">'
                    f'<div style="font-size:0.9rem;font-weight:600;color:#0f172a;line-height:1.3">{_display_title}</div>'
                    f'<div style="font-size:0.78rem;color:#64748b;margin-top:3px">'
                    f'{_mp_flag} &nbsp;·&nbsp; '
                    f'<a href="https://www.amazon.com/dp/{_asin}" target="_blank" style="color:#3b82f6;text-decoration:none">{_asin} ↗</a>'
                    f' &nbsp;·&nbsp; {_date}' +
                    (f' &nbsp;·&nbsp; <span style="color:#22c55e">⬤ Gemini</span>' if (_a.get("model_used","")).startswith("Gemini") else
                     (f' &nbsp;·&nbsp; <span style="color:#a78bfa">⚡ Claude</span>' if (_a.get("model_used","")).startswith("Claude") else "")) +
                    (f' &nbsp;·&nbsp; <span style="color:#64748b">⏱ {_a["duration"]//60}м {_a["duration"]%60}с</span>' if _a.get("duration",0)>0 else "") +
                    (f' &nbsp;·&nbsp; <span style="background:#312e81;color:#a5b4fc;border-radius:4px;padding:1px 6px;font-size:0.68rem;font-weight:600">👤 {_a["analyst"].split("@")[0]}</span> <span style="color:#64748b;font-size:0.65rem">{_a["analyst"]}</span>' if _a.get("analyst") else "") +
                    f'</div>' +
                    (f'<div style="font-size:0.72rem;color:#64748b;margin-top:2px">🏆 {len(_a["competitors"])} конкурента</div>' if _a.get("competitors") else "") +
                    f'</div>', unsafe_allow_html=True)

            with _ci3:
                if _sc > 0:
                    st.markdown(
                        f'<div style="text-align:center;padding:8px 0">'
                        f'<div style="font-size:1.5rem;font-weight:800;color:{_sc_c}">{_sc}%</div>'
                        f'<div style="font-size:0.7rem;color:{_sc_c};font-weight:600">{_sc_lbl}</div>'
                        f'</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div style="text-align:center;padding:10px 0;color:#94a3b8">—</div>', unsafe_allow_html=True)

            with _ci4:
                if st.button("Open", key=f"hist_open_{_idx}", use_container_width=True, type="primary"):
                    _conn_o = get_db()
                    if _conn_o:
                        try:
                            _cur_o = _conn_o.cursor()
                            _cur_o.execute("""
                                SELECT result_json, vision_text, competitors_json, our_data_json,
                                       images_json, aplus_img_urls_json, aplus_vision_text
                                FROM listing_analysis WHERE asin=%s AND overall_score>0
                                ORDER BY overall_score DESC, analyzed_at DESC LIMIT 1
                            """, (_asin,))
                            _row_o = _cur_o.fetchone()
                            _conn_o.close()
                            if _row_o:
                                st.session_state["result"] = json.loads(_row_o[0]) if _row_o[0] else {}
                                st.session_state["vision"] = _row_o[1] or ""
                                st.session_state["images"] = []
                                if _row_o[2]:
                                    _comps_o = json.loads(_row_o[2])
                                    for _ci2o, _ch2 in enumerate(_comps_o):
                                        st.session_state[f"comp_ai_{_ci2o}"] = {"overall_score": f"{_ch2.get('overall',0)}%"}
                                if _row_o[3]:
                                    try: st.session_state["our_data"] = json.loads(_row_o[3])
                                    except: pass
                                if _row_o[4]:
                                    try: st.session_state["images"] = json.loads(_row_o[4])
                                    except: pass
                                if _row_o[5]:
                                    try: st.session_state["aplus_img_urls"] = json.loads(_row_o[5])
                                    except: pass
                                st.session_state["aplus_vision"] = _row_o[6] or "" if len(_row_o) > 6 else ""
                                st.session_state["_hist_loaded"] = _asin
                                st.session_state["page"] = "🏠 Обзор"
                                st.rerun()
                        except Exception as _oe:
                            st.error(f"Ошибка: {_oe}")

            with _ci5:
                if st.button("🗑️", key=f"hist_del_{_idx}", use_container_width=True, help="Удалить все записи этого ASIN"):
                    _conn_d = get_db()
                    if _conn_d:
                        try:
                            _cur_d = _conn_d.cursor()
                            _cur_d.execute("DELETE FROM listing_analysis WHERE asin=%s", (_asin,))
                            _conn_d.commit(); _conn_d.close()
                            st.rerun()
                        except Exception as _de:
                            st.error(f"{_de}")

            # Multiple versions expander
            _all_versions = _all_versions_map.get(_asin, [])
            if len(_all_versions) > 1:
                with st.expander(f"📅 {len(_all_versions)} анализа", expanded=False):
                    for _vi, _v in enumerate(_all_versions):
                        _vd = _v["date"].strftime("%d.%m.%Y %H:%M") if _v.get("date") else "—"
                        _vmu = _v.get("model_used","")
                        _vsc = _v.get("score",0) or 0
                        _vsc_c = "#22c55e" if _vsc>=75 else ("#f59e0b" if _vsc>=50 else "#ef4444")
                        _vm_badge = f'<span style="color:#22c55e">⬤ Gemini</span>' if "Gemini" in _vmu else f'<span style="color:#a78bfa">⚡ Claude</span>' if "Claude" in _vmu else ""
                        _vc1, _vc2, _vc3 = st.columns([5, 2, 1])
                        with _vc1:
                            st.markdown(
                                f'<div style="padding:4px 0">' +
                                f'<span style="font-size:0.75rem;color:#64748b">{_vd} &nbsp; {_vm_badge}</span>' +
                                f'</div>', unsafe_allow_html=True)
                        with _vc2:
                            st.markdown(f'<div style="text-align:right;padding:4px 0"><span style="font-size:0.8rem;font-weight:700;color:{_vsc_c}">{_vsc}%</span></div>', unsafe_allow_html=True)
                        with _vc3:
                            if st.button("🗑", key=f"del_ver_{_asin}_{_vi}", help="Удалить этот анализ"):
                                _conn_del = get_db()
                                if _conn_del:
                                    try:
                                        _cur_del = _conn_del.cursor()
                                        _cur_del.execute(
                                            "DELETE FROM listing_analysis WHERE asin=%s AND analyzed_at=%s",
                                            (_asin, _v["date"])
                                        )
                                        _conn_del.commit(); _conn_del.close()
                                        st.rerun()
                                    except Exception as _de: st.error(str(_de)[:80])

            # Competitors expander
            if _a.get("competitors"):
                with st.expander(f"🏆 {len(_a['competitors'])} конкурента в Benchmark", expanded=False):
                    for _ci in _a["competitors"]:
                        _cn  = _ci.get("title","") if isinstance(_ci,dict) else str(_ci)
                        _cas = _ci.get("asin","") if isinstance(_ci,dict) else ""
                        _csc2 = _ci.get("score",0) if isinstance(_ci,dict) else 0
                        _cmp2 = _ci.get("marketplace","com") if isinstance(_ci,dict) else "com"
                        _cfl = {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸","nl":"🇳🇱","se":"🇸🇪","pl":"🇵🇱"}.get(_cmp2,"🌍")
                        _csc_c2 = "#22c55e" if _csc2>=75 else ("#f59e0b" if _csc2>=50 else ("#ef4444" if _csc2>0 else "#94a3b8"))
                        _sc_str = f'<span style="font-weight:700;color:{_csc_c2}">{_csc2}%</span>' if _csc2 else ""
                        _asin_link = f'<a href="https://www.amazon.{_cmp2}/dp/{_cas}" target="_blank" style="color:#3b82f6;text-decoration:none;font-family:monospace;font-size:0.8rem">{_cas} ↗</a>' if _cas else ""
                        st.markdown(
                            f'{_cfl} • **{_cn}** {_asin_link}' + (f' — {_sc_str}' if _sc_str else ""),
                            unsafe_allow_html=True)

            st.markdown('<hr style="margin:4px 0;border-color:#f1f5f9">', unsafe_allow_html=True)
        # ── END OF MAIN LOOP ──────────────────────────────────────────────

        st.divider()

        if _pre_asin:
            st.markdown('<div id="asin-details"></div>', unsafe_allow_html=True)
        st.components.v1.html('<script>document.getElementById("asin-details")?.scrollIntoView({behavior:"smooth"})</script>', height=0)

        asin_opts = [f"{'🔵' if a.get('type','наш')=='наш' else '🔴'} {a['asin']} — {(a['title'] or '')[:40]}" for a in all_asins]
        _default_idx = 0
        if _pre_asin:
            _match = next((i for i,a in enumerate(all_asins) if a["asin"]==_pre_asin), 0)
            _default_idx = _match
        sel = st.selectbox("ASIN", asin_opts, index=_default_idx)
        sel_asin = sel.split(" — ")[0].strip().lstrip("🔵🔴 ")

        _sel_data = next((a for a in all_asins if a["asin"] == sel_asin), {})
        _full_title = _sel_data.get("title","")
        if _full_title:
            st.caption(f"📦 {_full_title}")
        st.markdown(f'<a href="https://www.amazon.com/dp/{sel_asin}" target="_blank" style="color:#93c5fd;font-size:0.85rem">🔗 amazon.com/dp/{sel_asin} ↗</a>', unsafe_allow_html=True)

        history = db_history(sel_asin, limit=20)
        if not history:
            st.warning("Нет данных для этого ASIN")
            return

        latest = history[0]
        history_valid = [h for h in history if (h.get("overall") or 0) > 0]
        if not history_valid:
            st.info("Все записи в истории имеют Overall: 0% — это старые упавшие анализы. Запусти новый анализ.")

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
            _hist_valid = [h for h in history if (h.get("overall") or 0) > 0]
            if len(_hist_valid) >= 2:
                st.subheader("📊 Динамика Overall Score")
                import pandas as pd
                _hist_rev = list(reversed(_hist_valid))
                dates  = [h["date"].strftime("%d.%m %H:%M") for h in _hist_rev]
                scores = [h["overall"] or 0 for h in _hist_rev]
                df_chart = pd.DataFrame({"Overall %": scores}, index=dates)
                st.area_chart(df_chart, color="#3b82f6")

        _zero_count = len([h for h in history if (h.get("overall") or 0) == 0])
        if _zero_count > 0:
            _cl1, _cl2 = st.columns([3,1])
            _cl1.caption(f"⚠️ В истории {_zero_count} записей с Overall 0% (упавшие анализы)")
            if _cl2.button("🗑️ Очистить 0%", key="btn_clean_history", use_container_width=True):
                _cconn = get_db()
                if _cconn:
                    try:
                        _ccur = _cconn.cursor()
                        _ccur.execute("DELETE FROM listing_analysis WHERE asin=%s AND overall_score=0", (sel_asin,))
                        _cconn.commit(); _cconn.close()
                        st.success(f"✅ Удалено {_zero_count} записей")
                        st.rerun()
                    except Exception as _ce:
                        st.error(f"Ошибка: {_ce}")

        st.divider()
        amz_url = f"https://www.amazon.com/dp/{sel_asin}"
        st.markdown(f"🔗 [Открыть листинг на Amazon ↗]({amz_url})")

        st.subheader("Все запуски")
        history_show = [h for h in history if (h.get("overall") or 0) > 0]
        if not history_show:
            st.info("Нет успешных анализов — все записи с Overall 0% скрыты. Запусти новый анализ.")
        else:
            def _fmt(v):
                if not v: return "—"
                return f"{v}%"
            df = pd.DataFrame([{
                "Дата": h["date"].strftime("%d.%m.%Y %H:%M"),
                "Overall": _fmt(h["overall"]),
                "Title":   _fmt(h["title"]),
                "Bullets": _fmt(h["bullets"]),
                "Images":  _fmt(h["images"]),
                "A+":      _fmt(h["aplus"]),
                "COSMO":   _fmt(h["cosmo"]),
                "Rufus":   _fmt(h["rufus"]),
            } for h in history_show])
            st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("🔍 Загрузить полный анализ из истории")
        history_ok   = [h for h in history if (h.get("overall") or 0) > 0]
        history_zero = [h for h in history if (h.get("overall") or 0) == 0]
        history_sorted = history_ok + history_zero

        if not history_ok:
            st.warning("⚠️ Все записи имеют Overall: 0% — это старые упавшие анализы. Запусти новый анализ.")

        hist_opts = []
        for h in history_sorted:
            _ov = h.get("overall") or 0
            _prefix = "✅" if _ov > 0 else "❌"
            hist_opts.append(f"{_prefix} {h['date'].strftime('%d.%m.%Y %H:%M')} — Overall: {_ov}%")

        sel_hist = st.selectbox("Выбери запуск", hist_opts, key="hist_sel")
        sel_hist_idx_sorted = hist_opts.index(sel_hist)
        sel_hist_date = history_sorted[sel_hist_idx_sorted]["date"]
        sel_hist_idx = next((i for i, h in enumerate(history) if h["date"] == sel_hist_date), 0)

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
                        SELECT result_json, vision_text, competitors_json, our_data_json,
                               images_json, aplus_img_urls_json, aplus_vision_text
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
                    if row_h[3]:
                        try: st.session_state["our_data"] = json.loads(row_h[3])
                        except: pass
                    if row_h[4]:
                        try: st.session_state["images"] = json.loads(row_h[4])
                        except: pass
                    if row_h[5]:
                        try: st.session_state["aplus_img_urls"] = json.loads(row_h[5])
                        except: pass
                    st.session_state["aplus_vision"] = row_h[6] or "" if len(row_h) > 6 else ""
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

    # ══════════════════════════════════════════════════════════════════════
    # TAB: AI Benchmark
    # ══════════════════════════════════════════════════════════════════════
    with _tab_bench:
        st.markdown("### 📊 Сравнение результатов: Claude vs Gemini")
        st.caption("Одни и те же ASINы — разные модели. Показывает реальную разницу в оценках.")

        _bench_data = {}
        for _a in all_asins:
            _asin = _a["asin"]
            _mu = _a.get("model_used","")
            _sc = _a.get("score", 0) or 0
            if _sc == 0: continue
            if _asin not in _bench_data:
                _bench_data[_asin] = {"title": (_a.get("title") or "")[:40], "claude": [], "gemini": []}
            if "Gemini" in _mu:
                _bench_data[_asin]["gemini"].append(_sc)
            elif "Claude" in _mu:
                _bench_data[_asin]["claude"].append(_sc)
            else:
                _bench_data[_asin]["claude"].append(_sc)

        _both = {k:v for k,v in _bench_data.items() if v["claude"] and v["gemini"]}
        _only_one = {k:v for k,v in _bench_data.items() if not (v["claude"] and v["gemini"])}

        if _both:
            st.markdown("#### 🆚 Анализировали обеими моделями:")
            _total_diff = []
            for _asin, _d in _both.items():
                _c_avg = sum(_d["claude"]) / len(_d["claude"])
                _g_avg = sum(_d["gemini"]) / len(_d["gemini"])
                _diff = _c_avg - _g_avg
                _total_diff.append(_diff)
                _diff_c = "#22c55e" if abs(_diff) < 5 else ("#f59e0b" if abs(_diff) < 15 else "#ef4444")
                _diff_str = f"+{_diff:.0f}%" if _diff > 0 else f"{_diff:.0f}%"
                st.markdown(
                    f'<div style="background:#0f172a;border-radius:8px;padding:10px 14px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center">' +
                    f'<div style="font-size:0.82rem;color:#e2e8f0">{_d["title"]}<br><span style="color:#64748b;font-size:0.72rem">{_asin}</span></div>' +
                    f'<div style="display:flex;gap:20px;align-items:center">' +
                    f'<div style="text-align:center"><div style="font-size:0.7rem;color:#64748b">⚡ Claude</div><div style="font-size:1.1rem;font-weight:700;color:#a78bfa">{_c_avg:.0f}%</div></div>' +
                    f'<div style="text-align:center"><div style="font-size:0.7rem;color:#64748b">🟢 Gemini</div><div style="font-size:1.1rem;font-weight:700;color:#34d399">{_g_avg:.0f}%</div></div>' +
                    f'<div style="text-align:center;min-width:60px"><div style="font-size:0.7rem;color:#64748b">Разница</div><div style="font-size:1.1rem;font-weight:700;color:{_diff_c}">{_diff_str}</div></div>' +
                    f'</div></div>',
                    unsafe_allow_html=True)
            if _total_diff:
                _avg_diff = sum(_total_diff) / len(_total_diff)
                st.markdown("---")
                if abs(_avg_diff) < 3:
                    st.success(f"✅ Модели дают похожие результаты — разница {_avg_diff:+.1f}% в среднем")
                elif _avg_diff > 0:
                    st.info(f"⚡ Claude даёт на **{_avg_diff:.1f}%** выше оценку чем Gemini в среднем")
                else:
                    st.info(f"🟢 Gemini даёт на **{abs(_avg_diff):.1f}%** выше оценку чем Claude в среднем")
        else:
            st.info("Нет ASINов проанализированных обеими моделями. Запусти один и тот же листинг через Claude и Gemini для сравнения.")

        if _only_one:
            st.markdown("#### 📋 Только одной моделью:")
            for _asin, _d in list(_only_one.items())[:10]:
                _mu_label = "⚡ Claude" if _d["claude"] else "🟢 Gemini"
                _sc_list = _d["claude"] or _d["gemini"]
                _sc_avg = sum(_sc_list) / len(_sc_list)
                st.markdown(f"- {_mu_label} &nbsp; **{_sc_avg:.0f}%** &nbsp; `{_asin}` {_d['title']}")

    st.markdown("<br>", unsafe_allow_html=True)

    # Tips row
    _t1, _t2, _t3, _t4 = st.columns(4)
    _t1.info("💡 Заполни **Целевую аудиторию** — Vision AI учтёт кто покупатель")
    _t2.info("💡 **EU:** Вставь amazon.de или .fr — маркетплейс определится авто")
    _t3.info("💡 **История:** все анализы сохраняются — следи за динамикой Score")
    _t4.info("💡 **Топ ниши** работает без URL — просто введи запрос покупателя")

    st.divider()

    st.markdown("#### 📊 Что получишь после анализа")
    _f1,_f2,_f3,_f4,_f5 = st.columns(5)
    _feat_names = ["Overall Score\n17 метрик","Vision AI\nФото 1-10","COSMO/Rufus\nAI-видимость","VPC/JTBD\nЯзык покупателя","Mobile Score\nМоб. конверсия"]
    for _fcol, _ficon, _fname in zip([_f1,_f2,_f3,_f4,_f5], ["🏆","📸","🧠","🎯","📱"], _feat_names):
        _fcol.markdown(f'''<div style="text-align:center;background:#f8fafc;border-radius:10px;padding:12px;font-size:0.78rem">
<div style="font-size:1.5rem">{_ficon}</div>
<div style="font-weight:600;color:#1e293b;margin-top:4px;white-space:pre-line">{_fname}</div>
</div>''', unsafe_allow_html=True)

# ── Pages ─────────────────────────────────────────────────────────────────────
page = st.session_state.get("page", "🏠 Обзор")
r  = st.session_state.get("result", {})
v  = st.session_state.get("vision", "")
od = st.session_state.get("our_data", {})
pi = od.get("product_information", {})
cd = st.session_state.get("comp_data_list", [])
imgs = st.session_state.get("images", [])

if page == "👑 Admin":
    show_admin_panel()
    st.stop()

if page == "📈 История": page_history(); st.stop()
_is_competitor_page = page.startswith("🔴 Конкурент")
if "result" not in st.session_state and page not in ["🔥 Топ ниши", "📱 Mobile Score", "ℹ️ О инструменте", "📖 Документация"]:
    # ── Onboarding для новых пользователей ──────────────────────────────────
    st.markdown("""
<div style="text-align:center;padding:20px 0 10px">
<h1 style="font-size:2.2rem;font-weight:800;color:#0f172a">🔍 Amazon Listing Analyzer</h1>
<p style="color:#64748b;font-size:1.05rem">Listing 3.0 — AI-анализ на основе COSMO + Rufus + Vision</p>
</div>""", unsafe_allow_html=True)

    # 3 шага
    _s1, _s2, _s3 = st.columns(3)
    with _s1:
        st.markdown('''<div style="background:#eff6ff;border-radius:14px;padding:20px;border-top:4px solid #3b82f6;min-height:170px">
<div style="font-size:2rem">1️⃣</div>
<div style="font-weight:700;color:#1e293b;font-size:1rem;margin:8px 0 6px">Вставь URL листинга</div>
<div style="font-size:0.82rem;color:#475569;line-height:1.5">Скопируй ссылку с Amazon.com / .de / .fr / .it — любой маркетплейс. Можно добавить до 5 конкурентов.</div>
</div>''', unsafe_allow_html=True)
    with _s2:
        st.markdown('''<div style="background:#f0fdf4;border-radius:14px;padding:20px;border-top:4px solid #22c55e;min-height:170px">
<div style="font-size:2rem">2️⃣</div>
<div style="font-weight:700;color:#1e293b;font-size:1rem;margin:8px 0 6px">Нажми Запустить анализ</div>
<div style="font-size:0.82rem;color:#475569;line-height:1.5">AI проанализирует фото, текст, BSR, A+ и конкурентов. Полный анализ — 2-3 минуты.</div>
</div>''', unsafe_allow_html=True)
    with _s3:
        st.markdown('''<div style="background:#fdf4ff;border-radius:14px;padding:20px;border-top:4px solid #a855f7;min-height:170px">
<div style="font-size:2rem">3️⃣</div>
<div style="font-weight:700;color:#1e293b;font-size:1rem;margin:8px 0 6px">Читай результаты</div>
<div style="font-size:0.82rem;color:#475569;line-height:1.5">Обзор → Health Score. Фото → Vision. COSMO/Rufus → AI-видимость. Топ ниши → рынок.</div>
</div>''', unsafe_allow_html=True)

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
        Table, TableStyle, PageBreak, Image as RLImage, KeepTogether)
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT
    from PIL import Image as PILImage
    import requests as _req, os

    C = {
        "navy": colors.HexColor("#0f172a"), "blue": colors.HexColor("#1d4ed8"),
        "blue2": colors.HexColor("#3b82f6"), "slate": colors.HexColor("#334155"),
        "muted": colors.HexColor("#64748b"), "light": colors.HexColor("#f1f5f9"),
        "border": colors.HexColor("#e2e8f0"), "green": colors.HexColor("#15803d"),
        "green2": colors.HexColor("#22c55e"), "yellow": colors.HexColor("#d97706"),
        "yellow2": colors.HexColor("#fbbf24"), "red": colors.HexColor("#dc2626"),
        "red2": colors.HexColor("#ef4444"), "white": colors.white,
        "accent": colors.HexColor("#7c3aed"),
    }
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=18*mm, rightMargin=18*mm, topMargin=16*mm, bottomMargin=16*mm)
    W = A4[0] - 36*mm

    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    _sys_r = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    _sys_b = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    if os.path.exists(_sys_r) and os.path.exists(_sys_b):
        try:
            pdfmetrics.registerFont(TTFont("F", _sys_r))
            pdfmetrics.registerFont(TTFont("FB", _sys_b))
            _F, _FB = "F", "FB"
        except: _F, _FB = "Helvetica", "Helvetica-Bold"
    else: _F, _FB = "Helvetica", "Helvetica-Bold"

    def ps(name, **kw):
        d = dict(fontName=_F, fontSize=9, textColor=C["slate"], spaceAfter=2, leading=13)
        d.update(kw); return ParagraphStyle(name, **d)

    S = {
        "h1": ps("h1", fontName=_FB, fontSize=13, textColor=C["navy"], spaceBefore=8, spaceAfter=3),
        "h2": ps("h2", fontName=_FB, fontSize=10, textColor=C["slate"], spaceBefore=5, spaceAfter=2),
        "body": ps("bd", fontSize=9, textColor=C["slate"], spaceAfter=2, leading=13),
        "small": ps("sm", fontSize=8, textColor=C["muted"], spaceAfter=1, leading=11),
        "green": ps("gr", fontSize=9, textColor=C["green"], fontName=_FB, spaceAfter=2),
        "orange": ps("or", fontSize=9, textColor=C["yellow"], spaceAfter=2),
        "red": ps("rd", fontSize=9, textColor=C["red"], fontName=_FB, spaceAfter=2),
        "action": ps("ac", fontSize=9, textColor=C["blue"], fontName=_FB, spaceAfter=2),
        "footer": ps("ft", fontSize=7, textColor=C["muted"], alignment=TA_CENTER),
    }

    story = []
    date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    ov_pct = pct(result.get("overall_score", 0))

    def score_color(v):
        return C["green"] if v >= 75 else (C["yellow"] if v >= 50 else C["red"])
    def score_bg(v):
        return colors.HexColor("#dcfce7") if v >= 75 else (colors.HexColor("#fef9c3") if v >= 50 else colors.HexColor("#fee2e2"))
    def hx(c):
        return '#{:02x}{:02x}{:02x}'.format(int(c.red*255),int(c.green*255),int(c.blue*255))
    def score_label(v):
        return "STRONG" if v >= 75 else ("NEEDS WORK" if v >= 50 else "CRITICAL")
    def _clean(s):
        return _re.sub(r"\*+","",str(s or "")).strip()

    title_val = our_data.get("title", asin)
    price = our_data.get("price",""); rating = our_data.get("average_rating",""); brand = our_data.get("brand","")

    # Cover
    cover_tbl = Table([[
        Paragraph("Amazon Listing Audit", ps("ct", fontName=_FB, fontSize=22, textColor=C["white"], leading=26)),
        Paragraph(f"<b>{ov_pct}%</b>", ps("ov", fontName=_FB, fontSize=44, textColor=colors.HexColor("#22c55e" if ov_pct>=75 else ("#fbbf24" if ov_pct>=50 else "#ef4444")), alignment=TA_RIGHT, leading=50)),
    ]], colWidths=[W*0.65, W*0.35])
    cover_tbl.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"MIDDLE"),("BACKGROUND",(0,0),(-1,-1),C["navy"]),("PADDING",(0,0),(-1,-1),14)]))
    story.append(cover_tbl); story.append(Spacer(1,3*mm))

    meta = [
        [Paragraph("<b>ASIN</b>",S["small"]), Paragraph(f"<b>{asin}</b>",S["body"]), Paragraph("<b>Дата</b>",S["small"]), Paragraph(date_str,S["body"]), Paragraph("<b>Рейтинг</b>",S["small"]), Paragraph(f"{rating}",S["body"])],
        [Paragraph("<b>Бренд</b>",S["small"]), Paragraph(brand[:30],S["body"]), Paragraph("<b>Цена</b>",S["small"]), Paragraph(price,S["body"]), Paragraph("<b>Overall</b>",S["small"]), Paragraph(f"<font color='{hx(score_color(ov_pct))}'><b>{ov_pct}% — {score_label(ov_pct)}</b></font>",S["body"])],
        [Paragraph("<b>Листинг</b>",S["small"]), Paragraph(_clean(title_val)[:80], ps("tt",fontSize=8,textColor=C["slate"])), "","","",""],
    ]
    mt = Table(meta, colWidths=[18*mm,W*0.27,14*mm,W*0.2,16*mm,W*0.2])
    mt.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),_F),("FONTSIZE",(0,0),(-1,-1),8),("BACKGROUND",(0,0),(-1,-1),C["light"]),("GRID",(0,0),(-1,-1),0.3,C["border"]),("PADDING",(0,0),(-1,-1),5),("SPAN",(1,2),(5,2)),("VALIGN",(0,0),(-1,-1),"MIDDLE")]))
    story.append(mt); story.append(Spacer(1,5*mm))

    # Score dashboard
    story.append(Paragraph("▌ SCORE DASHBOARD", ps("sdh",fontName=_FB,fontSize=11,textColor=C["navy"],spaceBefore=4,spaceAfter=3)))
    score_map = [
        ("Title",result.get("title_score",0)),("Bullets",result.get("bullets_score",0)),
        ("Описание",result.get("description_score",0)),("Фото",result.get("images_score",0)),
        ("A+",result.get("aplus_score",0)),("Отзывы",result.get("reviews_score",0)),
        ("BSR",result.get("bsr_score",0)),("Цена",result.get("price_score",0)),
        ("Варианты",result.get("customization_score",0)),("Prime",result.get("prime_score",0)),
        ("COSMO",result.get("cosmo_analysis",{}).get("score",0) if isinstance(result.get("cosmo_analysis"),dict) else 0),
        ("Rufus",result.get("rufus_analysis",{}).get("score",0) if isinstance(result.get("rufus_analysis"),dict) else 0),
    ]
    _has_aplus_pdf = bool(our_data.get("aplus") or our_data.get("aplus_content"))
    def score_card(label, raw):
        v = pct(raw); sc = score_color(v); bg = score_bg(v)
        if label == "Описание" and v == 0 and _has_aplus_pdf:
            return Table([[Paragraph(f"<b>{label}</b>",ps(f"sc_{label}",fontName=_FB,fontSize=8,textColor=C["navy"])),Paragraph("<b>A+</b>",ps(f"sv_{label}",fontName=_FB,fontSize=11,alignment=TA_RIGHT,textColor=C["muted"]))],[Paragraph("скрыто A+",ps(f"sb_{label}",fontSize=7,textColor=C["muted"])),""]],colWidths=[35*mm,18*mm],style=TableStyle([("BACKGROUND",(0,0),(-1,-1),C["light"]),("PADDING",(0,0),(-1,-1),4),("SPAN",(0,1),(1,1))]))
        return Table([[Paragraph(f"<b>{label}</b>",ps(f"sc_{label}",fontName=_FB,fontSize=8,textColor=C["navy"])),Paragraph(f"<font color='{hx(sc)}'><b>{v}%</b></font>",ps(f"sv_{label}",fontName=_FB,fontSize=11,alignment=TA_RIGHT))],[Table([[""]], colWidths=[max(2,int(v*0.5))*mm if max(2,int(v*0.5))*mm<40*mm else 40*mm],style=[("BACKGROUND",(0,0),(-1,-1),sc),("ROWHEIGHTS",(0,0),(-1,-1),3)]),""]],colWidths=[35*mm,18*mm],style=TableStyle([("BACKGROUND",(0,0),(-1,-1),bg),("PADDING",(0,0),(-1,-1),4),("SPAN",(0,1),(1,1)),("VALIGN",(0,0),(-1,-1),"MIDDLE")]))
    n=len(score_map); cols3=3; rows3=(n+cols3-1)//cols3; dash_rows=[]
    for ri in range(rows3):
        row=[]
        for ci in range(cols3):
            idx=ri*cols3+ci
            if idx<n: row.append(score_card(*score_map[idx]))
            else: row.append("")
        dash_rows.append(row)
    dash_tbl=Table(dash_rows,colWidths=[W/3-1*mm]*3)
    dash_tbl.setStyle(TableStyle([("PADDING",(0,0),(-1,-1),1.5),("VALIGN",(0,0),(-1,-1),"TOP")]))
    story.append(dash_tbl); story.append(Spacer(1,5*mm))

    # Priority actions
    _prio=result.get("priority_improvements",[]); _acts=result.get("actions",[])
    if _prio or _acts:
        story.append(Paragraph("▌ ПРИОРИТЕТНЫЕ ДЕЙСТВИЯ",ps("pah",fontName=_FB,fontSize=11,textColor=C["navy"],spaceBefore=2,spaceAfter=3)))
        _all=[{"action":_clean(item),"impact":"HIGH","effort":"","details":""} for item in _prio]
        _all+=[a for a in _acts if isinstance(a,dict)]
        _all.sort(key=lambda x:{"HIGH":0,"MEDIUM":1,"LOW":2}.get(x.get("impact","MEDIUM"),1))
        for a in _all[:8]:
            _imp=a.get("impact","MEDIUM"); _ic=C["red"] if _imp=="HIGH" else (C["yellow"] if _imp=="MEDIUM" else C["green"])
            _ibg=colors.HexColor("#fee2e2") if _imp=="HIGH" else (colors.HexColor("#fef9c3") if _imp=="MEDIUM" else colors.HexColor("#dcfce7"))
            _ar=Table([[Paragraph(f"<b>{_imp}</b>",ps(f"imp_{_imp}",fontName=_FB,fontSize=7,textColor=_ic,alignment=TA_CENTER)),Paragraph(_clean(a.get("action","")),ps("act_t",fontSize=9,textColor=C["navy"]))]],colWidths=[14*mm,W-14*mm])
            _ar.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),_ibg),("LINEBELOW",(0,0),(-1,-1),0.3,C["border"]),("PADDING",(0,0),(-1,-1),5),("VALIGN",(0,0),(-1,-1),"MIDDLE")]))
            story.append(_ar)

    story.append(PageBreak())

    # Photos
    story.append(Paragraph("▌ АНАЛИЗ ФОТОГРАФИЙ",ps("ph",fontName=_FB,fontSize=11,textColor=C["navy"],spaceBefore=0,spaceAfter=4)))
    _blocks={}
    if vision_text:
        for _m in _re.finditer(r"PHOTO_BLOCK_(\d+)\s*(.*?)(?=PHOTO_BLOCK_\d+|$)",vision_text,_re.DOTALL):
            _blocks[int(_m.group(1))]=_m.group(2).strip()
    for i,img_d in enumerate(images[:7]):
        blk=_blocks.get(i+1,"")
        sc_m=_re.search(r"(\d+)/10",blk); sc_v=int(sc_m.group(1)) if sc_m else 0
        sc_c=score_color(sc_v*10); sc_lbl="Отлично" if sc_v>=8 else ("Хорошо" if sc_v>=6 else "Слабо")
        typ_m=_re.search(r"(?:Тип|Type)\s*[:\-]\s*(.+)",blk)
        str_m=_re.search(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.{3,})",blk)
        wk_m=_re.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{3,})",blk)
        ac_m=_re.search(r"(?:Действие|Action)\s*[:\-]\s*(.{3,})",blk)
        em_m=_re.search(r"(?:Эмоция|Emotion)\s*[:\-]\s*(.{3,})",blk)
        ptype=_clean(typ_m.group(1)) if typ_m else ""; stxt=_clean(str_m.group(1)) if str_m else ""
        wtxt=_clean(wk_m.group(1)) if wk_m else ""; atxt=_clean(ac_m.group(1)) if ac_m else ""; etxt=_clean(em_m.group(1)) if em_m else ""
        try:
            _b64=img_d.get("b64","") if isinstance(img_d,dict) else img_d
            _bytes=base64.b64decode(_b64); _pil=PILImage.open(io.BytesIO(_bytes)).convert("RGB")
            _pil.thumbnail((200,200)); _tb=io.BytesIO(); _pil.save(_tb,"JPEG",quality=80); _tb.seek(0)
            _rl=RLImage(_tb,width=40*mm,height=40*mm)
        except: _rl=Paragraph(f"#{i+1}",S["small"])
        info=[Paragraph(f"<b>Фото #{i+1}" + (f" — {ptype}" if ptype else "") + f"</b>  <font color='{hx(sc_c)}'><b>{sc_v}/10 {sc_lbl}</b></font>",ps("phdr",fontName=_FB,fontSize=10,textColor=C["navy"]))]
        if stxt: info.append(Paragraph(f"✅ {stxt}",S["green"]))
        if wtxt: info.append(Paragraph(f"⚠ {wtxt}",S["orange"]))
        if atxt: info.append(Paragraph(f"→ {atxt}",S["action"]))
        if etxt: info.append(Paragraph(f"😶 {etxt}",S["small"]))
        photo_row=Table([[_rl,info]],colWidths=[43*mm,W-43*mm])
        photo_row.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),("LINEBELOW",(0,0),(-1,-1),0.5,C["border"]),("LINEBEFORE",(0,0),(0,-1),3,sc_c),("PADDING",(0,0),(0,0),5),("PADDING",(1,0),(1,0),6)]))
        story.append(KeepTogether([photo_row,Spacer(1,2*mm)]))

    # A+ Vision
    _av=st.session_state.get("aplus_vision",""); _aurls=st.session_state.get("aplus_img_urls",[])
    if _av or _aurls:
        story.append(PageBreak())
        story.append(Paragraph("▌ A+ КОНТЕНТ",ps("aph",fontName=_FB,fontSize=11,textColor=C["navy"],spaceBefore=0,spaceAfter=4)))
        _apblks={}
        if _av:
            for _m in _re.finditer(r"APLUS_BLOCK_(\d+)\s*(.*?)(?=APLUS_BLOCK_\d+|$)",_av,_re.DOTALL):
                _apblks[int(_m.group(1))]=_m.group(2).strip()
        for _bi in range(max(len(_aurls),len(_apblks))):
            _bblk=_apblks.get(_bi+1,""); _bmod=_re.search(r"(?:Модуль|Module)\s*[:\-]\s*(.+)",_bblk)
            _bsc=_re.search(r"(\d+)/10",_bblk); _bstr=_re.search(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.{3,})",_bblk)
            _bwk=_re.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{3,})",_bblk); _bact=_re.search(r"(?:Действие|Action)\s*[:\-]\s*(.{3,})",_bblk)
            _bscv=int(_bsc.group(1)) if _bsc else 0; _bscc=score_color(_bscv*10)
            _ap_info=[Paragraph(f"<b>Баннер #{_bi+1}" + (f" — {_clean(_bmod.group(1))}" if _bmod else "") + f"</b>  <font color='{hx(_bscc)}'>{_bscv}/10</font>",ps("aphi",fontName=_FB,fontSize=9,textColor=C["navy"]))]
            if _bstr: _ap_info.append(Paragraph(f"✅ {_clean(_bstr.group(1))}",S["green"]))
            if _bwk: _ap_info.append(Paragraph(f"⚠ {_clean(_bwk.group(1))}",S["orange"]))
            if _bact: _ap_info.append(Paragraph(f"→ {_clean(_bact.group(1))}",S["action"]))
            _ap_img=""
            if _bi<len(_aurls):
                try:
                    _apr=_req.get(_aurls[_bi],timeout=10,headers={"User-Agent":"Mozilla/5.0"})
                    if _apr.ok:
                        _apil=PILImage.open(io.BytesIO(_apr.content)).convert("RGB"); _apil.thumbnail((500,200))
                        _ab=io.BytesIO(); _apil.save(_ab,"JPEG",quality=70); _ab.seek(0)
                        _ap_img=RLImage(_ab,width=W,height=35*mm)
                except: pass
            _parts=([_ap_img] if _ap_img else [])+_ap_info+[Spacer(1,3*mm)]
            story.append(KeepTogether(_parts))

    story.append(PageBreak())
    story.append(Paragraph("▌ АНАЛИЗ КОНТЕНТА",ps("cah",fontName=_FB,fontSize=11,textColor=C["navy"],spaceBefore=0,spaceAfter=4)))
    for sec_key,sec_score_key,sec_name in [("title","title_score","Title"),("bullets","bullets_score","Bullets"),("description","description_score","Description"),("aplus","aplus_score","A+")]:
        sc_v=pct(result.get(sec_score_key,0)); sc_c=score_color(sc_v)
        gaps=result.get(f"{sec_key}_gaps",[]); rec=result.get(f"{sec_key}_rec","")
        hdr_row=Table([[Paragraph(f"<b>{sec_name}</b>",ps(f"ch_{sec_key}",fontName=_FB,fontSize=10,textColor=C["navy"])),Paragraph(f"<font color='{hx(sc_c)}'><b>{sc_v}%</b></font>",ps(f"cs_{sec_key}",fontName=_FB,fontSize=14,alignment=TA_RIGHT))]],colWidths=[W-25*mm,25*mm])
        hdr_row.setStyle(TableStyle([("LINEBELOW",(0,0),(-1,-1),1.5,sc_c),("PADDING",(0,0),(-1,-1),4),("VALIGN",(0,0),(-1,-1),"MIDDLE")]))
        story.append(hdr_row)
        if gaps:
            for g in gaps[:3]: story.append(Paragraph(f"⚠ {_clean(g)}",S["orange"]))
        if rec: story.append(Paragraph(f"→ {_clean(rec)}",S["action"]))
        story.append(Spacer(1,3*mm))
    _title_txt=our_data.get("title",""); _bullets=our_data.get("feature_bullets",[])
    if _title_txt: story.append(Paragraph("Title:",S["h2"])); story.append(Paragraph(_clean(_title_txt)[:200],S["body"]))
    if _bullets:
        story.append(Paragraph("Bullets:",S["h2"]))
        for b in _bullets[:5]:
            _bl=len(b.encode()); _bc=C["red"] if _bl>255 else C["slate"]
            story.append(Paragraph(f"<font color='{hx(_bc)}'>{"🔴" if _bl>255 else "✅"}</font> {_clean(b)[:200]} [{_bl}b]",S["body"]))

    # COSMO/RUFUS/JTBD/VPC
    story.append(PageBreak())
    story.append(Paragraph("▌ COSMO / RUFUS / JTBD",ps("aih",fontName=_FB,fontSize=11,textColor=C["navy"],spaceBefore=0,spaceAfter=4)))
    _ca=result.get("cosmo_analysis",{}); _ra=result.get("rufus_analysis",{})
    _cs=pct(_ca.get("score",0)); _rs=pct(_ra.get("score",0))
    ai_tbl=Table([[Table([[Paragraph(f"<b>{_cs}%</b>",ps("csp",fontName=_FB,fontSize=22,textColor=score_color(_cs),alignment=TA_CENTER)),Paragraph("COSMO",ps("csl",fontSize=8,alignment=TA_CENTER,textColor=C["muted"]))]],style=[("BACKGROUND",(0,0),(-1,-1),score_bg(_cs)),("PADDING",(0,0),(-1,-1),8)]),Table([[Paragraph(f"<b>{_rs}%</b>",ps("rsp",fontName=_FB,fontSize=22,textColor=score_color(_rs),alignment=TA_CENTER)),Paragraph("Rufus",ps("rsl",fontSize=8,alignment=TA_CENTER,textColor=C["muted"]))]],style=[("BACKGROUND",(0,0),(-1,-1),score_bg(_rs)),("PADDING",(0,0),(-1,-1),8)])]],colWidths=[W/2-2*mm,W/2-2*mm])
    ai_tbl.setStyle(TableStyle([("PADDING",(0,0),(-1,-1),2)])); story.append(ai_tbl); story.append(Spacer(1,4*mm))
    _sig_p=_ca.get("signals_present",[]); _sig_m=_ca.get("signals_missing",[])
    if _sig_p or _sig_m:
        sig_tbl=Table([[[Paragraph("<b>✅ Присутствуют</b>",S["h2"])]+[Paragraph(f"• {_clean(s)}",S["green"]) for s in _sig_p[:5]],[Paragraph("<b>❌ Отсутствуют</b>",S["h2"])]+[Paragraph(f"• {_clean(s)}",S["orange"]) for s in _sig_m[:5]]]],colWidths=[W/2-2*mm,W/2-2*mm])
        sig_tbl.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),("BACKGROUND",(0,0),(0,0),colors.HexColor("#f0fdf4")),("BACKGROUND",(1,0),(1,0),colors.HexColor("#fffbeb")),("GRID",(0,0),(-1,-1),0.3,C["border"]),("PADDING",(0,0),(-1,-1),6)]))
        story.append(sig_tbl)
    _jtbd=result.get("jtbd_analysis",{})
    if _jtbd and _jtbd.get("job_story"):
        story.append(Spacer(1,4*mm)); story.append(Paragraph("▌ JTBD Job Story",ps("jh",fontName=_FB,fontSize=10,textColor=C["navy"],spaceAfter=2)))
        js_box=Table([[Paragraph(_clean(_jtbd["job_story"]),ps("jbs",fontSize=9,textColor=C["navy"],fontName=_FB,leading=14))]],colWidths=[W])
        js_box.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#f0f9ff")),("PADDING",(0,0),(-1,-1),8)])); story.append(js_box)
    _vpc=result.get("vpc_analysis",{})
    if _vpc and _vpc.get("vpc_verdict"):
        story.append(Spacer(1,4*mm)); story.append(Paragraph("▌ VPC Verdict",ps("vh",fontName=_FB,fontSize=10,textColor=C["navy"],spaceAfter=2)))
        verd_box=Table([[Paragraph(_clean(_vpc["vpc_verdict"]),ps("vb",fontSize=9,textColor=C["navy"],leading=13))]],colWidths=[W])
        verd_box.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#faf5ff")),("LINEBEFORE",(0,0),(0,-1),3,C["accent"]),("PADDING",(0,0),(-1,-1),8)])); story.append(verd_box)

    # Competitors
    if comp_data:
        story.append(PageBreak())
        story.append(Paragraph("▌ КОНКУРЕНТЫ",ps("comph",fontName=_FB,fontSize=11,textColor=C["navy"],spaceAfter=4)))
        comp_hdr=[[Paragraph("<b>ASIN</b>",S["small"]),Paragraph("<b>Title</b>",S["small"]),Paragraph("<b>Цена</b>",S["small"]),Paragraph("<b>★</b>",S["small"]),Paragraph("<b>Overall</b>",S["small"])]]
        for i,comp in enumerate(comp_data[:5]):
            _cai=st.session_state.get(f"comp_ai_{i}",{}); _cov=pct(_cai.get("overall_score",0)) if _cai else 0; _coc=score_color(_cov)
            comp_hdr.append([Paragraph(get_asin_from_data(comp),S["small"]),Paragraph(_clean(comp.get("title",""))[:45],S["small"]),Paragraph(str(comp.get("price","")),S["small"]),Paragraph(str(comp.get("average_rating","")),S["small"]),Paragraph(f"<font color='{hx(_coc)}'><b>{_cov}%</b></font>",ps("cov",fontName=_FB,fontSize=9,alignment=TA_CENTER))])
        comp_tbl=Table(comp_hdr,colWidths=[26*mm,80*mm,18*mm,12*mm,18*mm])
        comp_tbl.setStyle(TableStyle([("FONTNAME",(0,0),(-1,0),_FB),("FONTSIZE",(0,0),(-1,-1),8),("BACKGROUND",(0,0),(-1,0),C["navy"]),("TEXTCOLOR",(0,0),(-1,0),C["white"]),("ROWBACKGROUNDS",(0,1),(-1,-1),[C["light"],C["white"]]),("GRID",(0,0),(-1,-1),0.3,C["border"]),("PADDING",(0,0),(-1,-1),5)]))
        story.append(comp_tbl)

    # Footer
    story.append(Spacer(1,6*mm))
    ft=Table([[Paragraph(f"Amazon Listing Analyzer  |  ASIN: {asin}  |  {date_str}",S["footer"])]],colWidths=[W])
    ft.setStyle(TableStyle([("LINEABOVE",(0,0),(-1,-1),0.5,C["border"]),("PADDING",(0,0),(-1,-1),4)])); story.append(ft)
    doc.build(story); buf.seek(0)
    return buf.read()


def health_card():
    health = pct(r.get("overall_score", r.get("health_score", 0)))
    hc     = "#22c55e" if health>=75 else ("#f59e0b" if health>=50 else "#ef4444")
    hl     = "Отличный листинг" if health>=75 else ("Есть над чем работать" if health>=50 else "Требует срочных улучшений")
    title_h   = od.get("title","") or st.session_state.get("_hist_title","")
    tlen      = len(title_h)
    brand_h   = od.get("brand","")
    _input_asin_h = od.get("_input_asin","")
    _parent_asin_h = od.get("parent_asin","") or pi.get("ASIN","")
    asin_h = _parent_asin_h or _input_asin_h or st.session_state.get("our_url_saved","")
    # Show both if they differ
    _asin_display = asin_h
    if _input_asin_h and _parent_asin_h and _input_asin_h != _parent_asin_h:
        _asin_display = f"{_input_asin_h} → {_parent_asin_h} (parent)"
    price_h   = od.get("price","")
    prev_price = od.get("previous_price","") or od.get("list_price","")
    rating_h  = od.get("average_rating","")
    reviews_h = pi.get("Customer Reviews",{}).get("ratings_count","")
    bsr_h     = str(pi.get("Best Sellers Rank",""))[:50]
    coupon    = od.get("coupon_text","") or ("🎟️ Купон" if od.get("is_coupon_exists") else "")
    promo     = od.get("promo_text","")
    is_prime  = od.get("is_prime_exclusive") or od.get("is_prime")
    bought    = od.get("number_of_people_bought","")
    _is_history = st.session_state.get("_hist_loaded") and not title_h
    _mp_hc = st.session_state.get("_marketplace","com")
    price_parts = []
    if price_h:
        price_str = f"💰 <b>{price_h}</b>"
        if prev_price and prev_price != price_h:
            price_str += f" <span style='text-decoration:line-through;opacity:0.5'>{prev_price}</span>"
        price_parts.append(price_str)
    if coupon: price_parts.append(f"<span style='background:#16a34a;color:white;border-radius:4px;padding:1px 6px;font-size:0.78rem'>🎟️ {coupon}</span>")
    if promo: price_parts.append(f"<span style='background:#1d4ed8;color:white;border-radius:4px;padding:1px 6px;font-size:0.78rem'>📦 {promo[:40]}</span>")
    if is_prime: price_parts.append(f"<span style='background:#f59e0b;color:#1c1917;border-radius:4px;padding:1px 6px;font-size:0.78rem'>👑 Prime</span>")
    if bought: price_parts.append(f"<span style='opacity:0.7;font-size:0.78rem'>🛒 {bought}</span>")
    price_line = "  ".join(price_parts)
    if _is_history:
        st.markdown(f"""<div style="background:linear-gradient(135deg,#1e293b,#334155);border-radius:16px;padding:24px;color:white;margin-bottom:16px">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px">
    <div><div style="font-size:0.8rem;opacity:0.6;color:#93c5fd">📅 Загружено из истории</div><div style="font-size:0.85rem;color:#94a3b8;margin-top:4px">Данные листинга недоступны — только оценки AI</div></div>
    <div style="text-align:center"><div style="font-size:3.5rem;font-weight:800;color:{hc};line-height:1">{health}%</div><div style="font-size:0.85rem;color:{hc};margin-top:2px">{hl}</div></div>
  </div>
  <div style="background:rgba(255,255,255,0.12);border-radius:8px;height:10px;margin-top:14px"><div style="background:{hc};width:{health}%;height:10px;border-radius:8px"></div></div>
</div>""", unsafe_allow_html=True)
    else:
        try:
            _rat_val = safe_float_rating(rating_h)
        except:
            _rat_val = 0.0
        _rat_c = "#22c55e" if _rat_val >= 4.4 else ("#f59e0b" if _rat_val >= 4.3 else "#ef4444")
        _title_c = "#fca5a5" if tlen > 125 else "#86efac"
        st.markdown(f"""<div style="background:linear-gradient(135deg,#1e293b,#334155);border-radius:16px;padding:24px;color:white;margin-bottom:16px">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px">
    <div>
      <a href="https://www.amazon.{_mp_hc}/dp/{asin_h}" target="_blank" style="font-size:0.8rem;opacity:0.6;color:#93c5fd;text-decoration:none">{brand_h} · {asin_h} ({_mp_hc}) ↗</a>
      <div style="font-size:1rem;font-weight:600;max-width:520px;line-height:1.4;margin-top:4px">{title_h[:80]}{"..." if tlen>80 else ""}</div>
      <div style="display:flex;gap:10px;margin-top:8px;font-size:0.82rem;flex-wrap:wrap;align-items:center">{price_line}</div>
      <div style="display:flex;gap:14px;margin-top:6px;font-size:0.82rem;flex-wrap:wrap">
        <span style="color:{_rat_c};font-weight:600">⭐ {rating_h} ({reviews_h} отз.)</span>
        <span style="opacity:0.8">📊 {bsr_h}</span>
        <span style="color:{_title_c}">📝 Title: {tlen} симв.</span>
      </div>
    </div>
    <div style="text-align:center"><div style="font-size:3.5rem;font-weight:800;color:{hc};line-height:1">{health}%</div><div style="font-size:0.85rem;color:{hc};margin-top:2px">{hl}</div></div>
  </div>
  <div style="background:rgba(255,255,255,0.12);border-radius:8px;height:10px;margin-top:14px"><div style="background:{hc};width:{health}%;height:10px;border-radius:8px"></div></div>
</div>""", unsafe_allow_html=True)

    items = [
        ("Title",r.get("title_score",0)),("Bullets",r.get("bullets_score",0)),
        ("Описание",r.get("description_score",0)),("Фото",r.get("images_score",0)),
        ("A+",r.get("aplus_score",0)),("Отзывы",r.get("reviews_score",0)),
        ("BSR",r.get("bsr_score",0)),("Цена",r.get("price_score",0)),
        ("Варианты",r.get("customization_score",0)),("Prime",r.get("prime_score",0)),
    ]
    _has_aplus_od = bool(od.get("aplus") or od.get("aplus_content"))
    cols = st.columns(len(items))
    for col,(lbl,val) in zip(cols,items):
        p2 = pct(val)
        if lbl == "Описание" and p2 == 0 and _has_aplus_od:
            col.markdown('<div style="background:#f1f5f9;border-radius:8px;padding:8px 4px;text-align:center;border-left:3px solid #64748b"><div style="font-size:1rem;font-weight:700;color:#64748b">A+</div><div style="font-size:0.68rem;color:#64748b">Описание</div></div>', unsafe_allow_html=True)
        else:
            cc = "#22c55e" if p2>=75 else ("#f59e0b" if p2>=50 else "#ef4444")
            col.markdown(f'<div style="background:#f1f5f9;border-radius:8px;padding:8px 4px;text-align:center;border-left:3px solid {cc}"><div style="font-size:1.2rem;font-weight:700;color:{cc}">{p2}%</div><div style="font-size:0.68rem;color:#64748b">{lbl}</div></div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# 🎯 LISTING OPPORTUNITY OPERATOR + RENDER
# ══════════════════════════════════════════════════════════════════════════════

def listing_opportunity_operator(r, od, vision_text=""):
    """Internal decision block: missed revenue + priority actions for our listing vs competitors."""
    if not od or not od.get("title"):
        return

    st.divider()

    st.markdown(
        '<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px">'
        '<div style="font-size:1.45rem">🎯</div>'
        '<div>'
        '<div style="font-size:1.15rem;font-weight:800;color:#e2e8f0">Listing Opportunity Operator</div>'
        '<div style="font-size:0.76rem;color:#64748b">Что мешает листингу добирать выручку и что делать первым</div>'
        '</div></div>',
        unsafe_allow_html=True
    )

    _c1, _c2, _c3 = st.columns(3)

    with _c1:
        _sessions = st.number_input(
            "📊 Sessions / мес",
            value=st.session_state.get("_lo_sessions", 3000),
            min_value=100,
            max_value=500000,
            step=500,
            key="lo_sessions",
            help="Detail Page Sessions / Sessions из Business Reports"
        )

    with _c2:
        _cvr = st.number_input(
            "📈 CVR %",
            value=st.session_state.get("_lo_cvr", 8.0),
            min_value=0.5,
            max_value=50.0,
            step=0.5,
            key="lo_cvr",
            help="Unit Session Percentage / Conversion Rate"
        )

    with _c3:
        _price_default = 29.99
        try:
            _price_default = float(
                str(od.get("price", ""))
                .replace("$", "")
                .replace("€", "")
                .replace("£", "")
                .replace(",", ".")
                .strip()
                .split()[0]
            )
        except Exception:
            pass

        _price = st.number_input(
            "💰 Price",
            value=st.session_state.get("_lo_price", _price_default),
            min_value=1.0,
            max_value=9999.0,
            step=1.0,
            key="lo_price"
        )

    st.session_state["_lo_sessions"] = _sessions
    st.session_state["_lo_cvr"] = _cvr
    st.session_state["_lo_price"] = _price

    _current_orders = _sessions * (_cvr / 100)
    _current_revenue = _current_orders * _price

    st.markdown(
        f'<div style="background:#0f172a;border:1px solid #1e293b;border-radius:10px;padding:10px 14px;margin:8px 0 14px 0">'
        f'<div style="font-size:0.68rem;color:#64748b;letter-spacing:0.08em;font-weight:700">CURRENT BASELINE</div>'
        f'<div style="display:flex;gap:18px;flex-wrap:wrap;margin-top:6px">'
        f'<div style="font-size:0.85rem;color:#e2e8f0">Orders/month: <b>{int(_current_orders)}</b></div>'
        f'<div style="font-size:0.85rem;color:#e2e8f0">Revenue/month: <b>${_current_revenue:,.0f}</b></div>'
        f'<div style="font-size:0.85rem;color:#e2e8f0">CVR base: <b>{_cvr:.1f}%</b></div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True
    )

    if st.button("🧠 Построить Opportunity Plan", type="primary", use_container_width=True, key="btn_listing_opportunity"):
        with st.spinner("AI анализирует недореализованный потенциал листинга..."):
            _title = od.get("title", "")
            _bullets = od.get("feature_bullets", [])
            _pi = od.get("product_information", {})
            _rating = od.get("average_rating", "")
            _reviews = _pi.get("Customer Reviews", {}).get("ratings_count", "")
            _has_aplus = bool(od.get("aplus") or od.get("aplus_content"))
            _has_video = int(od.get("number_of_videos", 0) or 0) > 0
            _n_images = len(od.get("images", []))
            _coupon = od.get("coupon_text", "") or od.get("is_coupon_exists", False)
            _colors = len(od.get("customization_options", {}).get("color", []))
            _sizes = len(od.get("customization_options", {}).get("size", []))
            _bsr = str(_pi.get("Best Sellers Rank", ""))[:120]
            _is_returned = od.get("is_frequently_returned", False)

            _scores = {
                k: pct(r.get(k, 0)) for k in [
                    "overall_score",
                    "title_score",
                    "bullets_score",
                    "images_score",
                    "aplus_score",
                    "description_score",
                    "reviews_score",
                    "bsr_score",
                    "price_score",
                    "customization_score",
                    "prime_score"
                ]
            }

            _cosmo = pct(r.get("cosmo_analysis", {}).get("score", 0)) if isinstance(r.get("cosmo_analysis"), dict) else 0
            _rufus = pct(r.get("rufus_analysis", {}).get("score", 0)) if isinstance(r.get("rufus_analysis"), dict) else 0
            _jtbd = pct(r.get("jtbd_analysis", {}).get("alignment_score", 0)) if isinstance(r.get("jtbd_analysis"), dict) else 0
            _vpc_verdict = r.get("vpc_analysis", {}).get("vpc_verdict", "") if isinstance(r.get("vpc_analysis"), dict) else ""
            _job_story = r.get("jtbd_analysis", {}).get("job_story", "") if isinstance(r.get("jtbd_analysis"), dict) else ""

            _vision_summary = ""
            if vision_text:
                import re as _rev
                _photo_scores = [int(_m.group(1)) for _m in _rev.finditer(r"(\d+)/10", vision_text)]
                _weaknesses = [
                    _m.group(1).strip()[:120]
                    for _m in _rev.finditer(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{10,140})", vision_text)
                ]
                _actions_v = [
                    _m.group(1).strip()[:120]
                    for _m in _rev.finditer(r"(?:Действие|Action)\s*[:\-]\s*(.{10,140})", vision_text)
                ]
                _vision_summary = (
                    f"Photo scores: {_photo_scores[:8]}. "
                    f"Weaknesses: {'; '.join(_weaknesses[:4])}. "
                    f"Suggested fixes: {'; '.join(_actions_v[:3])}"
                )

            _title_gaps = r.get("title_gaps", [])
            _bullets_gaps = r.get("bullets_gaps", [])
            _images_gaps = r.get("images_gaps", [])
            _aplus_gaps = r.get("aplus_gaps", [])
            _missing_chars = r.get("missing_chars", [])

            _comp_summary = ""
            _cd = st.session_state.get("comp_data_list", [])
            if _cd:
                _comp_lines = []
                for _ci, _c in enumerate(_cd[:3]):
                    _cai = st.session_state.get(f"comp_ai_{_ci}", {})
                    _cov = pct(_cai.get("overall_score", 0)) if _cai else 0
                    _comp_lines.append(
                        f"Comp{_ci+1}: {_c.get('title','')[:60]} | "
                        f"ASIN: {get_asin_from_data(_c)} | "
                        f"Price: {_c.get('price','')} | "
                        f"Rating: {_c.get('average_rating','')}★ | "
                        f"Score: {_cov}%"
                    )
                _comp_summary = "\n".join(_comp_lines)

            _lang = "Russian" if st.session_state.get("analysis_lang", "ru") == "ru" else "English"

            _prompt = f"""You are a senior Amazon internal growth operator.
This is NOT a public SaaS output. This is an INTERNAL decision-support tool for the team.

Your task:
1. Estimate where the listing is underperforming vs its current potential
2. Compare our listing to competitors
3. Convert gaps into priority actions
4. Show MISSED REVENUE / RECOVERY POTENTIAL, not "guaranteed future revenue"
5. Be conservative, specific, and operational

BUSINESS BASELINE:
Sessions: {_sessions}/month
CVR: {_cvr}%
Price: ${_price}
Current revenue: ${_current_revenue:,.0f}/month
Current orders: {int(_current_orders)}/month

OUR LISTING:
Title ({len(_title)} chars): {_title[:180]}
Bullets ({len(_bullets)}): {chr(10).join([f"- {b[:180]}" for b in _bullets[:5]])}
Images: {_n_images}
Video: {"YES" if _has_video else "NO"}
A+: {"YES" if _has_aplus else "NO"}
Coupon: {"YES" if _coupon else "NO"}
Variants: {_colors} colors / {_sizes} sizes
Rating: {_rating}
Reviews: {_reviews}
BSR: {_bsr}
Frequently returned: {"YES" if _is_returned else "NO"}

ANALYSIS SCORES:
Overall: {_scores['overall_score']}%
Title: {_scores['title_score']}%
Bullets: {_scores['bullets_score']}%
Images: {_scores['images_score']}%
A+: {_scores['aplus_score']}%
Description: {_scores['description_score']}%
Reviews: {_scores['reviews_score']}%
BSR: {_scores['bsr_score']}%
Price: {_scores['price_score']}%
Customization: {_scores['customization_score']}%
Prime: {_scores['prime_score']}%
COSMO: {_cosmo}%
Rufus: {_rufus}%
JTBD: {_jtbd}%

PHOTO VISION:
{_vision_summary}

KNOWN GAPS:
Title gaps: {_title_gaps[:4]}
Bullets gaps: {_bullets_gaps[:4]}
Images gaps: {_images_gaps[:4]}
A+ gaps: {_aplus_gaps[:4]}
Missing characteristics: {[c.get('name','') for c in _missing_chars[:5]] if _missing_chars else 'none'}
VPC verdict: {_vpc_verdict}
JTBD story: {_job_story}

COMPETITORS:
{_comp_summary if _comp_summary else "No competitor snapshots available"}

OUTPUT JSON ONLY.
All text in {_lang}.

JSON FORMAT:
{{
  "mode": "internal_decision_tool",
  "headline": "short headline",
  "goal_text": "one-sentence operational summary",
  "current_issue": "main reason why listing underperforms",
  "missed_revenue_low": 1200,
  "missed_revenue_high": 3500,
  "recovery_potential_pct_low": 8,
  "recovery_potential_pct_high": 22,
  "confidence": 76,
  "confidence_based_on": ["source 1", "source 2"],
  "actions": [
    {{
      "rank": 1,
      "tag": "CRITICAL",
      "title": "specific action title",
      "effort": "15 min",
      "effort_type": "quick",
      "problem": "what exactly is wrong now",
      "action_steps": ["step 1", "step 2"],
      "why_works": "why this should improve conversion",
      "cvr_low": 4,
      "cvr_high": 8,
      "revenue_low": 600,
      "revenue_high": 1200,
      "competitor_ref": "how competitor does it better, if relevant"
    }}
  ],
  "execution_order": [
    "first thing to do",
    "second thing to do",
    "third thing to do"
  ]
}}

STRICT RULES:
- 3-5 actions max
- Use only real evidence from the listing data, scores, photo analysis and competitor comparison
- Do NOT present estimates as guaranteed revenue
- Think in terms of missed revenue / recoverable value / underperformance
- Single action uplift should usually stay realistic
- Prioritize actions by impact / effort
- Use competitor references only when relevant
- Return ONLY valid JSON
"""

            _raw = ai_call(
                "You are an internal Amazon listing opportunity analyst. Return ONLY valid JSON. Be concise in text fields.",
                _prompt,
                max_tokens=6000
            )

            import json as _json
            import re as _re_g

            def _try_parse_plan(_raw_text):
                _rc = _raw_text.strip()
                _rc = _re_g.sub(r"^```[a-zA-Z]*\s*", "", _rc, flags=_re_g.MULTILINE)
                _rc = _re_g.sub(r"```\s*$", "", _rc, flags=_re_g.MULTILINE)
                _s = _rc.find("{")
                if _s < 0:
                    return None
                _rc = _rc[_s:]
                _e = _rc.rfind("}")
                if _e > 0:
                    _candidate = _re_g.sub(r",\s*([}\]])", r"\1", _rc[:_e+1])
                    try:
                        return _json.loads(_candidate)
                    except Exception:
                        pass

                _in_str = False
                _esc = False
                _stack = []
                _last_safe = -1
                for _idx, _ch in enumerate(_rc):
                    if _esc:
                        _esc = False
                        continue
                    if _ch == "\\":
                        _esc = True
                        continue
                    if _ch == '"':
                        _in_str = not _in_str
                        continue
                    if _in_str:
                        continue
                    if _ch in "{[":
                        _stack.append("}" if _ch == "{" else "]")
                    elif _ch in "}]":
                        if _stack and _stack[-1] == _ch:
                            _stack.pop()
                            if not _stack:
                                _last_safe = _idx

                if _in_str:
                    _rc_fix = _rc + '"'
                else:
                    _rc_fix = _rc
                _rc_fix = _re_g.sub(r",\s*$", "", _rc_fix.rstrip())
                _rc_fix = _re_g.sub(r":\s*$", ': ""', _rc_fix)
                _stack2 = list(_stack)
                while _stack2:
                    _rc_fix += _stack2.pop()
                _rc_fix = _re_g.sub(r",\s*([}\]])", r"\1", _rc_fix)
                try:
                    return _json.loads(_rc_fix)
                except Exception:
                    pass

                if _last_safe > 0:
                    try:
                        return _json.loads(_re_g.sub(r",\s*([}\]])", r"\1", _rc[:_last_safe+1]))
                    except Exception:
                        return None
                return None

            _plan_parsed = _try_parse_plan(_raw)
            if _plan_parsed and isinstance(_plan_parsed, dict) and _plan_parsed.get("actions"):
                st.session_state["_listing_opportunity_plan"] = _plan_parsed
                st.session_state.pop("_listing_opportunity_raw", None)
            else:
                st.session_state["_listing_opportunity_plan"] = None
                st.session_state["_listing_opportunity_raw"] = _raw
                st.warning("⚠️ AI вернул обрезанный ответ — показываю красивую текстовую версию.")

    _plan = st.session_state.get("_listing_opportunity_plan")
    _raw_fallback = st.session_state.get("_listing_opportunity_raw")

    if _plan:
        _render_listing_opportunity_plan(_plan, _current_revenue, _sessions, _cvr, _price)
    elif _raw_fallback:
        _render_listing_opportunity_text(_raw_fallback, _current_revenue)
        if st.button("🗑️ Очистить результат", key="clear_listing_opportunity_raw"):
            st.session_state.pop("_listing_opportunity_raw", None)
            st.rerun()


def _render_listing_opportunity_text(raw_text, current_revenue):
    """Pretty-render a (possibly truncated) Opportunity Plan output as styled cards."""
    import re as _ret
    import json as _jt

    _rc = raw_text.strip()
    _rc = _ret.sub(r"^```[a-zA-Z]*\s*", "", _rc, flags=_ret.MULTILINE)
    _rc = _ret.sub(r"```\s*$", "", _rc, flags=_ret.MULTILINE)

    def _grab_str(_key):
        _m = _ret.search(rf'"{_key}"\s*:\s*"((?:[^"\\]|\\.)*)"', _rc)
        if not _m:
            return ""
        return _m.group(1).encode("utf-8").decode("unicode_escape", errors="ignore")

    def _grab_num(_key):
        _m = _ret.search(rf'"{_key}"\s*:\s*(-?\d+(?:\.\d+)?)', _rc)
        if not _m:
            return 0
        try:
            _v = float(_m.group(1))
            return int(_v) if _v == int(_v) else _v
        except Exception:
            return 0

    _headline = _grab_str("headline")
    _goal = _grab_str("goal_text")
    _issue = _grab_str("current_issue")
    _ml = _grab_num("missed_revenue_low")
    _mh = _grab_num("missed_revenue_high")
    _rl = _grab_num("recovery_potential_pct_low")
    _rh = _grab_num("recovery_potential_pct_high")
    _conf = _grab_num("confidence") or 70

    _actions = []
    _am = _ret.search(r'"actions"\s*:\s*\[', _rc)
    if _am:
        _depth = 0
        _in_str = False
        _esc = False
        _start = _am.end()
        _i = _start
        _obj_start = -1
        while _i < len(_rc):
            _ch = _rc[_i]
            if _esc:
                _esc = False
            elif _ch == "\\":
                _esc = True
            elif _ch == '"':
                _in_str = not _in_str
            elif not _in_str:
                if _ch == "{":
                    if _depth == 0:
                        _obj_start = _i
                    _depth += 1
                elif _ch == "}":
                    _depth -= 1
                    if _depth == 0 and _obj_start >= 0:
                        _chunk = _rc[_obj_start:_i+1]
                        _chunk_clean = _ret.sub(r",\s*([}\]])", r"\1", _chunk)
                        try:
                            _actions.append(_jt.loads(_chunk_clean))
                        except Exception:
                            _a = {
                                "rank": len(_actions) + 1,
                                "tag": "MEDIUM",
                                "title": "",
                                "effort": "",
                                "problem": "",
                                "why_works": "",
                                "action_steps": [],
                                "revenue_low": 0,
                                "revenue_high": 0,
                                "cvr_low": 0,
                                "cvr_high": 0,
                                "competitor_ref": "",
                            }
                            for _k in ("rank","tag","title","effort","problem","why_works","competitor_ref"):
                                _mm = _ret.search(rf'"{_k}"\s*:\s*"?((?:[^"\\]|\\.)*?)"?\s*[,}}]', _chunk)
                                if _mm:
                                    _val = _mm.group(1)
                                    if _k == "rank":
                                        try: _a[_k] = int(_val)
                                        except: pass
                                    else:
                                        _a[_k] = _val.encode("utf-8").decode("unicode_escape", errors="ignore")
                            for _k in ("revenue_low","revenue_high","cvr_low","cvr_high"):
                                _mm = _ret.search(rf'"{_k}"\s*:\s*(-?\d+(?:\.\d+)?)', _chunk)
                                if _mm:
                                    try: _a[_k] = int(float(_mm.group(1)))
                                    except: pass
                            _sm = _ret.search(r'"action_steps"\s*:\s*\[(.*?)\]', _chunk, _ret.DOTALL)
                            if _sm:
                                _a["action_steps"] = [
                                    _s.encode("utf-8").decode("unicode_escape", errors="ignore")
                                    for _s in _ret.findall(r'"((?:[^"\\]|\\.)*)"', _sm.group(1))
                                ]
                            _actions.append(_a)
                        _obj_start = -1
                elif _ch == "]" and _depth == 0:
                    break
            _i += 1

    if _headline or _actions or _ml or _mh:
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#1a1208,#2b1d0d);border:2px solid #f59e0b;'
            f'border-radius:16px;padding:24px 28px;margin:8px 0 14px 0">'
            f'<div style="font-size:0.65rem;color:#f59e0b;font-weight:700;letter-spacing:0.2em">⚠️ MISSED REVENUE</div>'
            + (f'<div style="font-size:1.55rem;font-weight:800;color:#f8fafc;margin-top:6px">${_ml:,}–${_mh:,} / month</div>' if (_ml or _mh) else '')
            + (f'<div style="font-size:0.88rem;color:#cbd5e1;margin-top:6px">{_headline}</div>' if _headline else '')
            + (f'<div style="font-size:0.82rem;color:#94a3b8;margin-top:8px">Recovery potential: +{_rl}% to +{_rh}% vs current baseline</div>' if (_rl or _rh) else '')
            + (f'<div style="font-size:0.8rem;color:#fbbf24;margin-top:8px;border-top:1px solid #f59e0b33;padding-top:8px">Main issue: {_issue}</div>' if _issue else '')
            + (f'<div style="font-size:0.78rem;color:#94a3b8;margin-top:8px">{_goal}</div>' if _goal else '')
            + '</div>',
            unsafe_allow_html=True
        )

    _tag_styles = {
        "CRITICAL":   {"color": "#ef4444", "bg": "#1a0a0a", "icon": "🔥"},
        "HIGH":       {"color": "#ef4444", "bg": "#1a0a0a", "icon": "🔥"},
        "QUICK WIN":  {"color": "#22c55e", "bg": "#0a1a0a", "icon": "⚡"},
        "QUICK":      {"color": "#22c55e", "bg": "#0a1a0a", "icon": "⚡"},
        "STRUCTURAL": {"color": "#3b82f6", "bg": "#0a0f1a", "icon": "🏗"},
        "MEDIUM":     {"color": "#f59e0b", "bg": "#1a1a0a", "icon": "🔧"},
    }

    if _actions:
        st.markdown(
            '<div style="font-size:0.7rem;color:#64748b;font-weight:700;letter-spacing:0.12em;margin:14px 0 8px 0">PRIORITY ACTIONS</div>',
            unsafe_allow_html=True
        )

    for _a in _actions:
        _tag = str(_a.get("tag","MEDIUM")).upper()
        _style = _tag_styles.get(_tag, _tag_styles["MEDIUM"])
        _rank = _a.get("rank", 0)
        _title = _a.get("title","")
        _effort = _a.get("effort","")
        _problem = _a.get("problem","")
        _why = _a.get("why_works","")
        _comp_ref = _a.get("competitor_ref","")
        _rev_l = _a.get("revenue_low",0)
        _rev_h = _a.get("revenue_high",0)
        _cvr_l = _a.get("cvr_low",0)
        _cvr_h = _a.get("cvr_high",0)
        _steps = _a.get("action_steps",[]) or []

        _steps_html = "".join(
            f'<div style="font-size:0.84rem;color:#e2e8f0;padding:4px 0 4px 12px;border-left:2px solid {_style["color"]}40">✓ {_s}</div>'
            for _s in _steps
        )

        st.markdown(
            f'<div style="background:{_style["bg"]};border-left:5px solid {_style["color"]};'
            f'border-radius:12px;padding:18px 22px;margin:10px 0">'
            f'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">'
            f'<span style="font-size:1.45rem;font-weight:900;color:{_style["color"]}">#{_rank}</span>'
            f'<span style="background:{_style["color"]}22;border:1px solid {_style["color"]};'
            f'color:{_style["color"]};border-radius:4px;padding:2px 10px;font-size:0.72rem;font-weight:700">{_style["icon"]} {_tag}</span>'
            f'<span style="font-size:1.02rem;font-weight:700;color:#e2e8f0;flex:1">{_title}</span>'
            + (f'<span style="font-size:0.78rem;color:#64748b;background:#1e293b;border-radius:4px;padding:3px 10px">⏱ {_effort}</span>' if _effort else '')
            + '</div>'
            + (f'<div style="font-size:0.85rem;color:#fca5a5;margin-top:10px;padding:8px 12px;background:#ef444410;border-radius:6px">❌ {_problem}</div>' if _problem else '')
            + (f'<div style="margin-top:10px"><div style="font-size:0.7rem;color:#64748b;font-weight:700;margin-bottom:4px">→ WHAT TO DO:</div>{_steps_html}</div>' if _steps_html else '')
            + (f'<div style="font-size:0.8rem;color:#94a3b8;margin-top:8px;font-style:italic">💡 {_why}</div>' if _why else '')
            + (f'<div style="font-size:0.78rem;color:#f59e0b;margin-top:6px">🏆 {_comp_ref}</div>' if _comp_ref else '')
            + (f'<div style="margin-top:12px;background:#0f172a;border-radius:8px;padding:10px 14px">'
               f'<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px">'
               f'<span style="font-size:0.78rem;color:#64748b">RECOVERABLE VALUE</span>'
               f'<div style="display:flex;gap:16px;flex-wrap:wrap">'
               f'<span style="font-size:0.85rem;color:#f59e0b;font-weight:700">CVR +{_cvr_l}–{_cvr_h}%</span>'
               f'<span style="font-size:0.95rem;color:#22c55e;font-weight:800">${_rev_l:,}–${_rev_h:,}/month</span>'
               f'</div></div></div>' if (_rev_l or _rev_h or _cvr_l or _cvr_h) else '')
            + '</div>',
            unsafe_allow_html=True
        )

    if not (_headline or _actions):
        with st.expander("📄 Сырой вывод AI (fallback)"):
            st.code(raw_text, language="json")


def _render_listing_opportunity_plan(plan, current_revenue, sessions, cvr, price):
    """Render internal decision-support plan."""

    _headline = plan.get("headline", "Listing opportunity detected")
    _goal_text = plan.get("goal_text", "")
    _current_issue = plan.get("current_issue", "")
    _actions = plan.get("actions", [])

    _missed_low = plan.get("missed_revenue_low", 0)
    _missed_high = plan.get("missed_revenue_high", 0)
    _rp_low = plan.get("recovery_potential_pct_low", 0)
    _rp_high = plan.get("recovery_potential_pct_high", 0)

    _confidence = plan.get("confidence", 70)
    _conf_based = plan.get("confidence_based_on", [])
    _exec_order = plan.get("execution_order", [])

    st.markdown(
        f'<div style="background:linear-gradient(135deg,#1a1208,#2b1d0d);border:2px solid #f59e0b;'
        f'border-radius:16px;padding:24px 28px;margin:8px 0 14px 0">'
        f'<div style="font-size:0.65rem;color:#f59e0b;font-weight:700;letter-spacing:0.2em">⚠️ MISSED REVENUE</div>'
        f'<div style="font-size:1.55rem;font-weight:800;color:#f8fafc;margin-top:6px">${_missed_low:,}–${_missed_high:,} / month</div>'
        f'<div style="font-size:0.88rem;color:#cbd5e1;margin-top:6px">{_headline}</div>'
        f'<div style="font-size:0.82rem;color:#94a3b8;margin-top:8px">Recovery potential: +{_rp_low}% to +{_rp_high}% vs current baseline</div>'
        + (f'<div style="font-size:0.8rem;color:#fbbf24;margin-top:8px;border-top:1px solid #f59e0b33;padding-top:8px">Main issue: {_current_issue}</div>' if _current_issue else '')
        + (f'<div style="font-size:0.78rem;color:#94a3b8;margin-top:8px">{_goal_text}</div>' if _goal_text else '')
        + f'</div>',
        unsafe_allow_html=True
    )

    _tag_styles = {
        "CRITICAL":   {"color": "#ef4444", "bg": "#1a0a0a", "border": "#ef4444", "icon": "🔥"},
        "QUICK WIN":  {"color": "#22c55e", "bg": "#0a1a0a", "border": "#22c55e", "icon": "⚡"},
        "QUICK":      {"color": "#22c55e", "bg": "#0a1a0a", "border": "#22c55e", "icon": "⚡"},
        "STRUCTURAL": {"color": "#3b82f6", "bg": "#0a0f1a", "border": "#3b82f6", "icon": "🏗"},
        "MEDIUM":     {"color": "#f59e0b", "bg": "#1a1a0a", "border": "#f59e0b", "icon": "🔧"},
    }

    if _actions:
        st.markdown(
            '<div style="font-size:0.7rem;color:#64748b;font-weight:700;letter-spacing:0.12em;margin:14px 0 8px 0">PRIORITY ACTIONS</div>',
            unsafe_allow_html=True
        )

    for _a in _actions:
        _rank = _a.get("rank", 0)
        _tag = str(_a.get("tag", "MEDIUM")).upper()
        _style = _tag_styles.get(_tag, _tag_styles["MEDIUM"])
        _effort = _a.get("effort", "")
        _problem = _a.get("problem", "")
        _steps = _a.get("action_steps", [])
        _why = _a.get("why_works", "")
        _cvr_l = _a.get("cvr_low", 0)
        _cvr_h = _a.get("cvr_high", 0)
        _rev_l = _a.get("revenue_low", 0)
        _rev_h = _a.get("revenue_high", 0)
        _comp_ref = _a.get("competitor_ref", "")

        _steps_html = ""
        for _s in _steps:
            _steps_html += (
                f'<div style="font-size:0.84rem;color:#e2e8f0;padding:4px 0 4px 12px;'
                f'border-left:2px solid {_style["color"]}40">✓ {_s}</div>'
            )

        _max_rev = max((x.get("revenue_high", 0) for x in _actions), default=1)
        _bar_width = int((_rev_h / max(_max_rev, 1)) * 100)

        st.markdown(
            f'<div style="background:{_style["bg"]};border-left:5px solid {_style["border"]};'
            f'border-radius:12px;padding:18px 22px;margin:10px 0">'
            f'<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">'
            f'<span style="font-size:1.45rem;font-weight:900;color:{_style["color"]}">#{_rank}</span>'
            f'<span style="background:{_style["color"]}22;border:1px solid {_style["color"]};'
            f'color:{_style["color"]};border-radius:4px;padding:2px 10px;font-size:0.72rem;font-weight:700">{_style["icon"]} {_tag}</span>'
            f'<span style="font-size:1.02rem;font-weight:700;color:#e2e8f0;flex:1">{_a.get("title","")}</span>'
            f'<span style="font-size:0.78rem;color:#64748b;background:#1e293b;border-radius:4px;padding:3px 10px">⏱ {_effort}</span>'
            f'</div>'
            + (f'<div style="font-size:0.85rem;color:#fca5a5;margin-top:10px;padding:8px 12px;background:#ef444410;border-radius:6px">❌ {_problem}</div>' if _problem else '')
            + f'<div style="margin-top:10px">'
              f'<div style="font-size:0.7rem;color:#64748b;font-weight:700;margin-bottom:4px">→ WHAT TO DO:</div>'
              f'{_steps_html}'
              f'</div>'
            + (f'<div style="font-size:0.8rem;color:#94a3b8;margin-top:8px;font-style:italic">💡 {_why}</div>' if _why else '')
            + (f'<div style="font-size:0.78rem;color:#f59e0b;margin-top:6px">🏆 {_comp_ref}</div>' if _comp_ref else '')
            + f'<div style="margin-top:12px;background:#0f172a;border-radius:8px;padding:10px 14px">'
              f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">'
              f'<span style="font-size:0.78rem;color:#64748b">RECOVERABLE VALUE</span>'
              f'<div style="display:flex;gap:16px;flex-wrap:wrap">'
              f'<span style="font-size:0.85rem;color:#f59e0b;font-weight:700">CVR +{_cvr_l}–{_cvr_h}%</span>'
              f'<span style="font-size:0.95rem;color:#22c55e;font-weight:800">${_rev_l:,}–${_rev_h:,}/month</span>'
              f'</div></div>'
              f'<div style="background:#1e293b;border-radius:4px;height:8px">'
              f'<div style="background:linear-gradient(90deg,{_style["color"]},{_style["color"]}88);width:{_bar_width}%;height:8px;border-radius:4px"></div>'
              f'</div>'
              f'</div>'
            f'</div>',
            unsafe_allow_html=True
        )

    _recoverable_revenue_low = current_revenue + _missed_low
    _recoverable_revenue_high = current_revenue + _missed_high

    st.markdown(
        f'<div style="background:linear-gradient(135deg,#0a1320,#111c2d);border:2px solid #3b82f6;'
        f'border-radius:14px;padding:22px 26px;margin:14px 0">'
        f'<div style="font-size:0.65rem;color:#3b82f6;font-weight:700;letter-spacing:0.2em;margin-bottom:10px">📊 RECOVERY SCENARIO</div>'
        f'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px">'
        f'<div style="text-align:center">'
        f'<div style="font-size:0.68rem;color:#64748b">RECOVERY POTENTIAL</div>'
        f'<div style="font-size:1.6rem;font-weight:800;color:#3b82f6">+{_rp_low}%–+{_rp_high}%</div>'
        f'<div style="font-size:0.75rem;color:#94a3b8">vs current listing performance</div>'
        f'</div>'
        f'<div style="text-align:center">'
        f'<div style="font-size:0.68rem;color:#64748b">MISSED / MONTH</div>'
        f'<div style="font-size:1.6rem;font-weight:800;color:#f59e0b">${_missed_low:,}–${_missed_high:,}</div>'
        f'<div style="font-size:0.75rem;color:#94a3b8">recoverable estimate</div>'
        f'</div>'
        f'<div style="text-align:center">'
        f'<div style="font-size:0.68rem;color:#64748b">IF FIXED WELL</div>'
        f'<div style="font-size:1.6rem;font-weight:800;color:#22c55e">${_recoverable_revenue_low:,.0f}–${_recoverable_revenue_high:,.0f}</div>'
        f'<div style="font-size:0.75rem;color:#94a3b8">possible monthly run-rate</div>'
        f'</div>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True
    )

    _chart_data = [{"label": "Current", "value": current_revenue}]
    _cum = current_revenue
    for _a in _actions:
        _mid = (_a.get("revenue_low", 0) + _a.get("revenue_high", 0)) / 2
        _cum += _mid
        _chart_data.append({"label": f"+ #{_a.get('rank', '?')}", "value": _cum})

    _max_val = max((d["value"] for d in _chart_data), default=1)

    _bars_html = '<div style="display:flex;align-items:flex-end;gap:6px;height:140px;padding:0 10px">'
    for _i, _cd in enumerate(_chart_data):
        _h = int((_cd["value"] / max(_max_val, 1)) * 100)
        _c = "#ef4444" if _i == 0 else "#22c55e"
        _opacity = 1.0 if _i == 0 else 0.65 + (_i / max(len(_chart_data)-1, 1)) * 0.35
        _bars_html += (
            f'<div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end">'
            f'<div style="font-size:0.68rem;font-weight:700;color:{_c};margin-bottom:4px">${_cd["value"]:,.0f}</div>'
            f'<div style="width:100%;height:{max(_h,5)}px;background:{_c};opacity:{_opacity};border-radius:4px 4px 0 0"></div>'
            f'<div style="font-size:0.62rem;color:#94a3b8;margin-top:6px;text-align:center">{_cd["label"]}</div>'
            f'</div>'
        )
    _bars_html += '</div>'

    st.markdown(
        '<div style="background:#0f172a;border-radius:10px;padding:16px 20px;margin:10px 0">'
        '<div style="font-size:0.7rem;color:#94a3b8;font-weight:700;letter-spacing:0.12em;margin-bottom:12px">📈 VALUE RECOVERY PATH</div>'
        + _bars_html +
        '</div>',
        unsafe_allow_html=True
    )

    _conf_c = "#22c55e" if _confidence >= 75 else ("#f59e0b" if _confidence >= 50 else "#ef4444")
    _conf_label = "Высокая" if _confidence >= 75 else ("Средняя" if _confidence >= 50 else "Низкая")
    _conf_sources = " · ".join(_conf_based[:4]) if _conf_based else "listing data"

    _exec_html = ""
    for _ei, _step in enumerate(_exec_order):
        _step_c = "#22c55e" if _ei == 0 else ("#f59e0b" if _ei == 1 else "#3b82f6")
        _timeline = "TODAY" if _ei < 2 else ("THIS WEEK" if _ei < 4 else "NEXT")
        _exec_html += (
            f'<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid #1e293b">'
            f'<span style="font-size:1rem;font-weight:800;color:{_step_c};min-width:28px">{_ei+1}.</span>'
            f'<span style="font-size:0.85rem;color:#e2e8f0;flex:1;line-height:1.4">{_step}</span>'
            f'<span style="font-size:0.62rem;color:{_step_c};background:{_step_c}22;border:1px solid {_step_c}55;border-radius:4px;padding:2px 8px;font-weight:700;white-space:nowrap">{_timeline}</span>'
            f'</div>'
        )

    st.markdown(
        f'<div style="display:grid;grid-template-columns:1fr 1.6fr;gap:12px;margin-top:12px">'
        f'<div style="background:#0f172a;border-radius:10px;padding:16px 20px;border-top:3px solid {_conf_c}">'
        f'<div style="font-size:0.65rem;color:#94a3b8;font-weight:700;letter-spacing:0.12em;margin-bottom:6px">CONFIDENCE</div>'
        f'<div style="font-size:1.6rem;font-weight:800;color:{_conf_c}">{_confidence}%</div>'
        f'<div style="font-size:0.8rem;color:#e2e8f0;margin-top:4px;font-weight:600">{_conf_label}</div>'
        f'<div style="font-size:0.7rem;color:#94a3b8;margin-top:8px;line-height:1.5">Based on: {_conf_sources}</div>'
        f'</div>'
        f'<div style="background:#0f172a;border-radius:10px;padding:16px 20px;border-top:3px solid #3b82f6">'
        f'<div style="font-size:0.65rem;color:#94a3b8;font-weight:700;letter-spacing:0.12em;margin-bottom:10px">🎯 EXECUTION ORDER</div>'
        f'{_exec_html}'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True
    )

    if st.button("🗑️ Очистить Opportunity Plan", key="clear_listing_opportunity_plan"):
        st.session_state.pop("_listing_opportunity_plan", None)
        st.session_state.pop("_listing_opportunity_raw", None)
        st.rerun()


# ── Pages dispatch ────────────────────────────────────────────────────────────
if page == "🏠 Обзор":
    _rc1, _rc2 = st.columns([8,1])
    with _rc1: st.title("🏠 Обзор листинга")
    with _rc2:
        if st.button("🔄", key="refresh_overview", help="Обновить страницу"):
            st.rerun()
    with st.expander("ℹ️ Как читать эту страницу", expanded=False):
        st.markdown("""
**Health Score** — итоговая оценка листинга. 🟢 75%+ = сильный, 🟡 50-74% = есть проблемы, 🔴 <50% = критично.

**Что делать:**
1. Смотри **Приоритетные действия** → красные (HIGH) делаешь первыми
2. Нажми **✍️ Переписать листинг** если низкий Title/Bullets score
3. Нажми **🔍 Анализ возвратов** чтобы понять почему покупатели жалуются
4. Нажми **👥 BuyBox** чтобы проверить не перехватил ли конкурент твои продажи
""")

    if not od or not od.get("title"):
        st.info("ℹ️ НАШ листинг не анализировался — показаны только конкуренты.")
        _cd_ov = st.session_state.get("comp_data_list",[])
        for _i, _c in enumerate(_cd_ov):
            _cai = st.session_state.get(f"comp_ai_{_i}",{})
            _cov = pct(_cai.get("overall_score",0)) if _cai else 0
            _cc = "#22c55e" if _cov>=75 else ("#f59e0b" if _cov>=50 else "#ef4444")
            st.markdown(f"""<div style="background:#1e293b;border-radius:10px;padding:14px;margin-bottom:8px;border-left:4px solid {_cc}">
<div style="font-weight:700;color:#e2e8f0">{get_asin_from_data(_c)} — {_c.get('title','')[:60]}</div>
<div style="font-size:1.5rem;font-weight:800;color:{_cc}">{_cov}%</div>
</div>""", unsafe_allow_html=True)
            if st.button(f"→ Перейти к конкуренту {_i+1}", key=f"ov_comp_{_i}"):
                st.session_state["page"] = f"🔴 Конкурент {_i+1}"
                st.rerun()
        st.stop()
    health_card()
    listing_opportunity_operator(r, od, st.session_state.get("vision",""))

    if od.get("is_frequently_returned"):
        st.markdown('<div style="background:#7f1d1d;border:2px solid #ef4444;border-radius:10px;padding:14px 18px;margin:8px 0"><div style="font-size:1.1rem;font-weight:800;color:#fca5a5">🔴 ВНИМАНИЕ: Amazon пометил листинг как "Часто возвращают"</div><div style="color:#fca5a5;font-size:0.88rem;margin-top:6px;line-height:1.7">Исправь фото/описание, размерную сетку, добавь видео распаковки. Снизи % возвратов ниже ~10% за 30 дней.</div></div>', unsafe_allow_html=True)

    _our_asin_ret = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")
    if _our_asin_ret:
        _ret_col1, _ret_col2 = st.columns([2, 5])
        with _ret_col1:
            if st.button("🔍 Анализ возвратов (1★+2★+3★)", key="btn_return_analysis", use_container_width=True, help="Загружает 1-3★ отзывы → AI находит топ причины возвратов и что исправить в листинге."):
                _ret_lines = []
                with st.spinner("📥 Загружаю отзывы через Apify..."):
                    _ret_reviews = fetch_1star_reviews(_our_asin_ret, domain="com", max_pages=1, log=lambda m: _ret_lines.append(m))
                for _l in _ret_lines: st.caption(_l)
                if _ret_reviews:
                    with st.spinner("🧠 AI анализирует..."):
                        _ret_analysis = analyze_return_reasons(_ret_reviews, od.get("title",""), _our_asin_ret, lang=st.session_state.get("analysis_lang","ru"))
                    st.session_state["_return_analysis"] = _ret_analysis
                    st.session_state["_return_reviews_count"] = len(_ret_reviews)
                    st.session_state["_return_source"] = "Apify"
                else:
                    st.warning("Отзывы не загружены — проверь APIFY_API_TOKEN")
        with _ret_col2:
            st.caption("Загружает отзывы 1-3★ → AI находит топ причины возвратов и что исправить в листинге")
        if st.session_state.get("_return_analysis"):
            src = st.session_state.get("_return_source","")
            cnt = st.session_state.get("_return_reviews_count",0)
            with st.expander(f"📊 Анализ возвратов {src} — {cnt} записей", expanded=True):
                st.markdown(st.session_state["_return_analysis"])

    st.divider()

    # ── BuyBox / Sellers ─────────────────────────────────────────────────────
    _our_asin_bb = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")
    _mp_bb = st.session_state.get("_marketplace","com")
    _bb_col1, _bb_col2 = st.columns([2,5])
    with _bb_col1:
        if st.button("👥 BuyBox & Sellers", key="btn_buybox", use_container_width=True, help="Показывает кто выигрывает BuyBox на этом ASIN и всех продавцов с ценами. Стоит 5 кредитов ScrapingDog."):
            with st.spinner("💰 Загружаю данные продавцов..."):
                _offers_data = fetch_offers(_our_asin_bb, domain=_mp_bb)
                if _offers_data:
                    st.session_state["_offers_data"] = _offers_data
                else:
                    st.warning("Данные не получены")
    with _bb_col2:
        st.caption("Кто выигрывает BuyBox, все продавцы и их цены")

    if st.session_state.get("_offers_data"):
        _od_bb = st.session_state["_offers_data"]
        _offers = _od_bb.get("offers", [])
        if _offers:
            _bb_winner = next((o for o in _offers if o.get("buybox_winner")), None)
            _bb_seller = _bb_winner.get("seller",{}).get("name","") if _bb_winner else "—"
            _bb_price  = _bb_winner.get("price",{}).get("raw","—") if _bb_winner else "—"
            _seller_id = st.secrets.get("SELLER_ID","")
            _bb_is_us  = bool(_seller_id and _bb_winner and _seller_id in _bb_winner.get("seller",{}).get("link",""))
            _bb_color  = "#22c55e" if _bb_is_us else "#ef4444"
            _bb_label  = "✅ ВЫ" if _bb_is_us else f"❌ {_bb_seller}"

            # Min price safe
            _min_price = "—"
            try:
                _price_vals = []
                for _o in _offers:
                    _raw = _o.get("price",{}).get("raw","")
                    if _raw:
                        _v = _raw.replace("$","").replace("€","").replace("£","").replace(",","").strip()
                        try: _price_vals.append((float(_v), _raw))
                        except: pass
                if _price_vals:
                    _min_price = min(_price_vals, key=lambda x: x[0])[1]
            except: pass

            _fba_count = sum(1 for o in _offers if o.get("delivery",{}).get("fulfilled_by_amazon"))

            st.markdown(
                f'<div style="background:#0f172a;border-radius:10px;padding:14px 16px;margin-bottom:12px">'
                f'<div style="display:flex;gap:24px;flex-wrap:wrap;align-items:center">'
                f'<div><div style="font-size:0.68rem;color:#64748b;margin-bottom:3px">🏆 BUYBOX</div>'
                f'<div style="font-size:1.1rem;font-weight:800;color:{_bb_color}">{_bb_label}</div>'
                f'<div style="font-size:0.8rem;color:#64748b">{_bb_price}</div></div>'
                f'<div><div style="font-size:0.68rem;color:#64748b;margin-bottom:3px">👥 ПРОДАВЦОВ</div>'
                f'<div style="font-size:1.1rem;font-weight:800;color:#3b82f6">{len(_offers)}</div></div>'
                f'<div><div style="font-size:0.68rem;color:#64748b;margin-bottom:3px">💰 МИН. ЦЕНА</div>'
                f'<div style="font-size:1.1rem;font-weight:800;color:#f59e0b">{_min_price}</div></div>'
                f'<div><div style="font-size:0.68rem;color:#64748b;margin-bottom:3px">🚀 FBA</div>'
                f'<div style="font-size:1.1rem;font-weight:800;color:#8b5cf6">{_fba_count}</div></div>'
                f'</div></div>',
                unsafe_allow_html=True)

            with st.expander(f"📋 Все продавцы ({len(_offers)})", expanded=False):
                for _o in _offers[:10]:
                    _s = _o.get("seller",{}); _p = _o.get("price",{}); _d = _o.get("delivery",{})
                    _is_bb = _o.get("buybox_winner",False); _is_fba = _d.get("fulfilled_by_amazon",False)
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;align-items:center;'
                        f'padding:8px 12px;border-radius:6px;margin-bottom:4px;'
                        f'background:{"#1a2a0a" if _is_bb else "#0f172a"};'
                        f'border-left:3px solid {"#f59e0b" if _is_bb else "#334155"}">'
                        f'<div><span style="font-size:0.85rem;font-weight:600;color:#e2e8f0">{_s.get("name","—")}</span>'
                        f'{"  🏆 BuyBox" if _is_bb else ""}{"  🚀 FBA" if _is_fba else "  📦 FBM"}</div>'
                        f'<div style="text-align:right">'
                        f'<div style="font-size:0.9rem;font-weight:700;color:#22c55e">{_p.get("raw","—")}</div>'
                        f'<div style="font-size:0.68rem;color:#64748b">⭐{_s.get("ratings_percentage_positive","")}%</div>'
                        f'</div></div>',
                        unsafe_allow_html=True)

    st.divider()
    _cosmo = r.get("cosmo_analysis",{})
    _rufus = r.get("rufus_analysis",{})
    _sum = r.get("summary","")
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
            rc2 = "#22c55e" if rs>=75 else ("#f59e0b" if rs>=50 else "#ef4444")
            st.markdown(f"**🤖 Rufus:** <span style='color:{rc2};font-size:1.2rem;font-weight:700'>{rs}%</span>", unsafe_allow_html=True)
            for iss in _rufus.get("issues",[])[:3]: st.caption(f"⚠️ {iss}")

    actions = r.get("actions", [])
    priority_improvements = r.get("priority_improvements", [])
    if actions or priority_improvements:
        st.subheader("🎯 Приоритетные действия")
        _all_actions = []
        for item in priority_improvements:
            _all_actions.append({"action": item, "impact": "HIGH", "effort": "MEDIUM", "details": ""})
        for a in actions:
            if isinstance(a, dict): _all_actions.append(a)

        # ── Content-Visual Gap ─────────────────────────────────────────────
        _vision_txt = st.session_state.get("vision","")
        _title_txt = od.get("title","") if od else ""
        _bullets_txt = " ".join(od.get("feature_bullets",[]) if od else [])
        if _vision_txt and (_title_txt or _bullets_txt):
            _gap_prompt = f"""Ты эксперт Amazon. Найди РАЗРЫВ между тем что обещает текст листинга и тем что реально показывают фото.

ТЕКСТ (Title + Bullets):
{_title_txt}
{_bullets_txt[:500]}

ФОТО (Vision анализ):
{_vision_txt[:800]}

Найди максимум 3 конкретных разрыва. Формат — строго JSON массив:
[{{"gap": "короткое описание разрыва", "text_claims": "что говорит текст", "photo_shows": "что показывает фото", "fix": "что добавить/изменить в фото"}}]
Если разрывов нет — верни пустой массив [].
Отвечай ТОЛЬКО JSON, без markdown."""
            try:
                _gap_r = ai_call("Amazon listing expert", _gap_prompt, max_tokens=600)
                import json as _jg
                _gaps = _jg.loads(_gap_r.strip().replace("```json","").replace("```",""))
                if _gaps:
                    for _g in _gaps[:3]:
                        _all_actions.append({
                            "action": f"📸 ФОТО не подтверждает текст: «{_g.get('text_claims','')}» — {_g.get('fix','')}",
                            "impact": "MEDIUM",
                            "effort": "MEDIUM",
                            "details": f"Текст обещает: {_g.get('text_claims','')} | Фото показывает: {_g.get('photo_shows','')}"
                        })
            except: pass
        _order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        _all_actions.sort(key=lambda x: _order.get(x.get("impact","MEDIUM"), 1))
        _high = [a for a in _all_actions if a.get("impact","") == "HIGH"]
        _med  = [a for a in _all_actions if a.get("impact","") == "MEDIUM"]
        _low  = [a for a in _all_actions if a.get("impact","") == "LOW"]

        def _action_cards(items, color, label, icon):
            if not items: return
            st.markdown(f'<div style="font-size:0.75rem;font-weight:700;color:{color};letter-spacing:0.08em;margin:12px 0 6px">{icon} {label} — {len(items)} действий</div>', unsafe_allow_html=True)
            for a in items:
                _effort = a.get("effort","MEDIUM")
                _ec = {"LOW":"#22c55e","MEDIUM":"#f59e0b","HIGH":"#ef4444"}.get(_effort,"#94a3b8")
                _act = a.get("action",""); _det = a.get("details","")
                _det_html = f'<div style="font-size:0.8rem;color:#94a3b8;margin-top:6px">{_det}</div>' if _det else ""
                st.markdown(f'<div style="background:#0f172a;border-left:4px solid {color};border-radius:8px;padding:12px 16px;margin-bottom:8px"><div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px"><div style="font-size:0.9rem;font-weight:600;color:#e2e8f0;flex:1">{_act}</div><span style="background:{_ec}22;color:{_ec};border:1px solid {_ec};border-radius:4px;padding:2px 8px;font-size:0.72rem;font-weight:700;white-space:nowrap">⚡ {_effort}</span></div>{_det_html}</div>', unsafe_allow_html=True)

        _action_cards(_high, "#ef4444", "КРИТИЧНО", "🔴")
        _action_cards(_med,  "#f59e0b", "ВАЖНО",    "🟡")
        _action_cards(_low,  "#22c55e", "УЛУЧШЕНИЕ","🟢")

    if r.get("missing_chars"):
        st.subheader("🔍 Отсутствующие характеристики")
        for ch in r["missing_chars"]:
            _mc_color = "#ef4444" if ch.get("priority","") == "HIGH" else "#f59e0b"
            st.markdown(f'<div style="background:#0f172a;border-left:4px solid {_mc_color};border-radius:8px;padding:10px 14px;margin-bottom:6px"><div style="font-size:0.88rem;font-weight:600;color:#e2e8f0">{ch.get("name","")}</div><div style="font-size:0.78rem;color:#94a3b8;margin-top:3px">{ch.get("how_competitors_use","")}</div></div>', unsafe_allow_html=True)

    st.divider()
    st.subheader("🤖 AI Инструменты")
    _tool_cols = st.columns(5)

    with _tool_cols[0]:
        if st.button("✍️ Переписать листинг", use_container_width=True, key="btn_rewriter", help="AI перепишет Title + 5 Bullets с учётом COSMO, JTBD и VPC gaps. ~10 сек."):
            with st.spinner("✍️ AI пишет title + 5 буллетов..."):
                _rw_prompt = f"""Rewrite this Amazon listing. Product: {od.get('title','')}
VPC gaps: {r.get('vpc_analysis',{}).get('pain_relievers_missing',[])}
JTBD: {r.get('jtbd_analysis',{}).get('job_story','')}
Title gaps: {r.get('title_gaps',[])}
Bullets gaps: {r.get('bullets_gaps',[])}
Write: 1. TITLE (max 125 chars) 2. BULLET 1-5 (max 200 chars each, "Feature: Benefit.")
NO stop words. Respond in {'Russian' if st.session_state.get('analysis_lang','ru')=='ru' else 'English'}."""
                st.session_state["_ai_rewrite"] = ai_call("Amazon listing copywriter.", _rw_prompt, max_tokens=1500)

    with _tool_cols[1]:
        if st.button("🔑 Keyword Gap", use_container_width=True, key="btn_kwgap", help="Находит ключевые слова конкурентов которых нет в нашем листинге. Добавь в Title/Bullets."):
            with st.spinner("🔑 Анализирую keyword gaps..."):
                _comps = st.session_state.get("comp_data_list", [])
                _comp_all = " ".join([_cd.get("title","") + " " + " ".join(_cd.get("feature_bullets",[])) for _cd in _comps]).lower()
                _kw_prompt = f"""Keyword gaps OUR vs COMPETITORS.
OUR: {od.get('title','')}
{chr(10).join(od.get('feature_bullets',[]))}
COMPETITORS: {_comp_all[:3000]}
Find TOP 15 missing keywords. Format: KEYWORD | competitor usage | where to add (title/bullet/backend)
Respond in {'Russian' if st.session_state.get('analysis_lang','ru')=='ru' else 'English'}."""
                st.session_state["_ai_kwgap"] = ai_call("Amazon SEO expert.", _kw_prompt, max_tokens=1200)

    with _tool_cols[2]:
        if st.button("📈 График Health Score", use_container_width=True, key="btn_chart"):
            _hist_asin = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")
            if _hist_asin:
                _hconn = get_db()
                if _hconn:
                    try:
                        _hcur = _hconn.cursor()
                        _hcur.execute("SELECT analyzed_at, overall_score FROM listing_analysis WHERE asin=%s AND overall_score>0 ORDER BY analyzed_at ASC LIMIT 30", (_hist_asin,))
                        st.session_state["_health_chart"] = _hcur.fetchall()
                        _hconn.close()
                    except: pass

    with _tool_cols[3]:
        if st.button("💬 Mining отзывов", use_container_width=True, key="btn_review_mine", help="Извлекает язык покупателей из 4-5★ отзывов — используй эти слова в Bullets и A+."):
            _mine_asin = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")
            if _mine_asin:
                with st.spinner("📥 Загружаю отзывы..."):
                    _mine_reviews = fetch_1star_reviews(_mine_asin, domain="com", max_pages=1)
                if _mine_reviews:
                    _pos = [rv for rv in _mine_reviews if int(float(str(rv.get("rating",1) or 1).split()[0])) >= 4][:15]
                    with st.spinner("🧠 AI извлекает инсайты..."):
                        _mine_text = "\n".join([f"[{rv.get('rating')}★] {rv.get('title','')} — {rv.get('body',rv.get('text',rv.get('reviewText','')))[:200]}" for rv in _pos])
                        _mine_prompt = f"""Extract buyer insights from 4-5★ reviews for: {od.get('title','')}
REVIEWS:\n{_mine_text}
Extract: 1. TOP 5 phrases buyers use 2. TOP 3 use cases 3. TOP 3 overcome objections 4. Bullet rewrites
Respond in {'Russian' if st.session_state.get('analysis_lang','ru')=='ru' else 'English'}."""
                        st.session_state["_ai_mining"] = ai_call("Amazon VOC expert.", _mine_prompt, max_tokens=1200)

    with _tool_cols[4]:
        st.markdown('<div style="font-size:0.75rem;color:#94a3b8;text-align:center;margin-top:4px">💬 AI Chat</div>', unsafe_allow_html=True)
        _chat_q = st.text_input("Спроси про листинг", placeholder="Почему низкий BSR?", key="ai_chat_input", label_visibility="collapsed")
        if _chat_q and st.session_state.get("_chat_last") != _chat_q:
            st.session_state["_chat_last"] = _chat_q
            with st.spinner("🧠"):
                _chat_ctx = f"Listing: {od.get('title','')} | Overall: {pct(r.get('overall_score',0))}% | BSR: {od.get('product_information',{}).get('Best Sellers Rank','')} | Gaps: {r.get('title_gaps',[])} {r.get('bullets_gaps',[])}"
                st.session_state["_ai_chat_ans"] = ai_call("Amazon expert. Answer concisely.", f"Context: {_chat_ctx}\n\nQuestion: {_chat_q}", max_tokens=600)

    if st.session_state.get("_ai_rewrite"):
        with st.expander("✍️ Переписанный листинг", expanded=True):
            st.markdown(st.session_state["_ai_rewrite"])
    if st.session_state.get("_ai_kwgap"):
        with st.expander("🔑 Keyword Gap", expanded=True):
            st.markdown(st.session_state["_ai_kwgap"])
    if st.session_state.get("_health_chart"):
        with st.expander("📈 История Health Score", expanded=True):
            _rows = st.session_state["_health_chart"]
            if len(_rows) >= 2:
                import pandas as pd
                st.line_chart(pd.DataFrame(_rows, columns=["Дата","Score"]).set_index("Дата"))
            else:
                st.info(f"Мало данных ({len(_rows)}) — нужно минимум 2 анализа")
    if st.session_state.get("_ai_mining"):
        with st.expander("💬 Voice of Customer", expanded=True):
            st.markdown(st.session_state["_ai_mining"])
    if st.session_state.get("_ai_chat_ans"):
        with st.expander("💬 AI ответ", expanded=True):
            st.markdown(st.session_state["_ai_chat_ans"])

    st.divider()
    st.subheader("📥 Скачать PDF отчёт")
    _pdf_col1, _pdf_col2 = st.columns([2,4])
    with _pdf_col1:
        if st.button("📄 Сгенерировать PDF", type="primary", use_container_width=True, help="Профессиональный отчёт с фото, Vision анализом, COSMO/Rufus/VPC данными. Для клиентов и команды."):
            with st.spinner("Генерирую PDF отчёт..."):
                try:
                    _pdf_bytes = generate_pdf_report(result=r, our_data=od, vision_text=st.session_state.get("vision",""), images=st.session_state.get("images",[]), asin=od.get("parent_asin","") or od.get("asin",""), comp_data=st.session_state.get("comp_data_list",[]))
                    st.session_state["_pdf_bytes"] = _pdf_bytes
                    st.success("✅ PDF готов — нажми скачать")
                except Exception as _pe:
                    st.error(f"Ошибка PDF: {_pe}")
    with _pdf_col2:
        if st.session_state.get("_pdf_bytes"):
            _asin_dl = od.get("parent_asin","") or od.get("asin","listing")
            _date_dl = __import__("datetime").datetime.now().strftime("%Y%m%d")
            st.download_button(label="⬇️ Скачать PDF", data=st.session_state["_pdf_bytes"], file_name=f"amazon_audit_{_asin_dl}_{_date_dl}.pdf", mime="application/pdf", use_container_width=True)


# ══ Фото ══════════════════════════════════════════════════════════════════════
elif page == "📸 Фото":
    _rc1, _rc2 = st.columns([8,1])
    with _rc1: st.title("📸 Vision анализ фотографий")
    with _rc2:
        if st.button("🔄", key="refresh_vision", help="Обновить страницу"):
            st.rerun()
    with st.expander("ℹ️ Как читать Vision анализ", expanded=False):
        st.markdown("""
**Оценки фото (1-10):** 9-10 = отлично, 7-8 = хорошо, 5-6 = улучшить, 1-4 = заменить.

**Эмоции покупателя:** Доверие 💚 = хорошо | Сомнение 🔴 = плохо | Безразличие ⚪ = заменить фото.

**Что делать:**
- Фото с оценкой <6 → читай "Что делать" и передай дизайнеру
- Главное фото (#1) <7 → приоритет HIGH, влияет на CTR в поиске
- Нажми **🧠 AI-оценка галереи** для общего McKinsey-вывода
""")

    if not od or not od.get("title"):
        st.info("ℹ️ Эта страница доступна только при анализе **нашего листинга**. Добавь URL в поле 🔵 НАШ листинг и перезапусти.")
        st.stop()

    # ── Audience Score ────────────────────────────────────────────────────────
    with st.expander("👥 AI Audience Score — оценка фото для вашей ЦА", expanded=False):
        st.caption("AI оценивает каждое фото с точки зрения вашей целевой аудитории")
        _aud_col1, _aud_col2 = st.columns(2)
        with _aud_col1:
            _aud_age = st.text_input("👤 Возраст и пол", placeholder="Мужчина 30-45 лет", key="aud_age")
            _aud_lifestyle = st.text_input("🏃 Образ жизни", placeholder="Активный, outdoor, hiking, путешествия", key="aud_lifestyle")
        with _aud_col2:
            _aud_income = st.selectbox("💰 Доход", ["Средний", "Выше среднего", "Высокий", "Любой"], key="aud_income")
            _aud_geo = st.text_input("🌍 География", placeholder="Германия, EU", key="aud_geo")
        _aud_extra = st.text_area("💬 Доп. инфо о ЦА", placeholder="Ценит качество, экологичность, покупает онлайн, читает отзывы...", height=68, key="aud_extra")

        _imgs_for_aud = st.session_state.get("images", [])
        if not _imgs_for_aud:
            st.info("Запусти анализ чтобы загрузить фото листинга")
        elif st.button("👥 Оценить все фото для ЦА", type="primary", key="btn_audience_score"):
            if not (_aud_age or _aud_lifestyle):
                st.warning("Заполни хотя бы возраст/пол и образ жизни")
            else:
                _aud_profile = f"""
ЦЕЛЕВАЯ АУДИТОРИЯ:
- Возраст/пол: {_aud_age}
- Образ жизни: {_aud_lifestyle}
- Доход: {_aud_income}
- География: {_aud_geo}
- Дополнительно: {_aud_extra}
""".strip()
                _aud_results = []
                _vprog = st.progress(0, "👥 Оцениваю фото для ЦА...")
                for _ai_idx, _aimg in enumerate(_imgs_for_aud[:6]):
                    _vprog.progress(int((_ai_idx+1)/min(len(_imgs_for_aud),6)*100), f"Фото {_ai_idx+1}...")
                    try:
                        _ab64 = _aimg.get("b64","") if isinstance(_aimg, dict) else _aimg
                        _amt = _aimg.get("media_type","image/jpeg") if isinstance(_aimg, dict) else "image/jpeg"
                        # Photo #1 = main Amazon image (white bg required)
                        _is_main_photo = (_ai_idx == 0)
                        _photo_context = """ВАЖНО: Это ГЛАВНОЕ фото листинга (#1).
По правилам Amazon главное фото ОБЯЗАНО быть на чистом белом фоне (RGB 255,255,255).
НЕ рекомендуй менять белый фон на lifestyle — это нарушит правила Amazon и приведёт к suppression.
Оценивай только: позу модели, читаемость товара, посадку, выражение лица, динамику движения, 
соответствие модели ЦА. Рекомендуй улучшения в рамках белого фона (поза, движение, угол съёмки).""" if _is_main_photo else """Это дополнительное фото #{} листинга. Оценивай свободно — фон, контекст, lifestyle.""".format(_ai_idx+1)

                        _aprompt = f"""Ты маркетолог-эксперт по Amazon. Оцени это фото товара с точки зрения целевой аудитории.

{_aud_profile}

ПРОДУКТ: {od.get('title','')}

{_photo_context}

Оцени по шкале 0-100% насколько это фото убедительно для данной аудитории.

Ответь строго в формате:
SCORE: [0-100]%
ЭМОЦИЯ_ЦА: [что чувствует покупатель глядя на это фото]
СООТВЕТСТВУЕТ: [что на фото совпадает с интересами ЦА]
НЕ СООТВЕТСТВУЕТ: [что не совпадает или отталкивает ЦА]
РЕКОМЕНДАЦИЯ: [одно конкретное улучшение — поза/движение/угол для #1, сцена/контекст для остальных]"""
                        _aud_resp = anthropic_vision(
                            content_blocks=[
                                {"type":"image","source":{"type":"base64","media_type":_amt,"data":_ab64}},
                                {"type":"text","text":_aprompt}
                            ],
                            max_tokens=500,
                            system="Ты маркетолог-эксперт по Amazon. Отвечай строго по формату."
                        )
                        _aud_text = _aud_resp.get("content",[{}])[0].get("text","") if isinstance(_aud_resp, dict) else str(_aud_resp)
                        _aud_results.append({"idx": _ai_idx+1, "text": _aud_text, "b64": _ab64, "mt": _amt})
                    except Exception as _ae:
                        _aud_results.append({"idx": _ai_idx+1, "text": f"Ошибка: {_ae}", "b64": "", "mt": ""})
                _vprog.progress(100, "✅ Готово!")
                st.session_state["_aud_results"] = _aud_results

        # Show results
        if st.session_state.get("_aud_results"):
            st.markdown("---")
            for _ar in st.session_state["_aud_results"]:
                _arc1, _arc2 = st.columns([1, 3])
                with _arc1:
                    if _ar.get("b64"):
                        st.image(f"data:{_ar['mt']};base64,{_ar['b64']}", use_container_width=True)
                    st.caption(f"Фото #{_ar['idx']}")
                with _arc2:
                    _txt = _ar["text"]
                    # Extract score for color
                    import re as _re2
                    _sm = _re2.search(r'SCORE:\s*(\d+)%', _txt)
                    _sc = int(_sm.group(1)) if _sm else 0
                    _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else "#ef4444")
                    _sc_l = "✅ Отлично для ЦА" if _sc>=75 else ("🟡 Средне" if _sc>=50 else "🔴 Слабо для ЦА")
                    st.markdown(
                        f'<div style="background:#0f172a;border-left:4px solid {_sc_c};border-radius:8px;padding:12px 14px">' +
                        f'<div style="font-size:1.4rem;font-weight:800;color:{_sc_c}">{_sc}% <span style="font-size:0.8rem">{_sc_l}</span></div>' +
                        f'<div style="font-size:0.82rem;color:#e2e8f0;margin-top:8px;white-space:pre-line">{_txt.replace("SCORE:","").replace(f"{_sc}%","",1).strip()}</div>' +
                        f'</div>',
                        unsafe_allow_html=True)
            # ── Summary & Ranking ────────────────────────────────────────────
            import re as _re3
            _aud_scored = []
            for _ar2 in st.session_state["_aud_results"]:
                _sm2 = _re3.search(r'SCORE:\s*(\d+)%', _ar2["text"])
                _sc2 = int(_sm2.group(1)) if _sm2 else 0
                _aud_scored.append((_sc2, _ar2["idx"], _ar2["text"]))
            _aud_scored_sorted = sorted(_aud_scored, reverse=True)

            st.markdown("---")
            st.markdown("### 🏆 Итоговый рейтинг фото для вашей ЦА")

            # Detect which photo has white background (likely photo #1 from listing)
            # Main image (#1) MUST be white bg per Amazon rules — pin it to position #1
            _main_photo_idx = 1  # always keep original #1 as main (white bg rule)
            _rest_sorted = [(sc,idx,txt) for sc,idx,txt in _aud_scored_sorted if idx != _main_photo_idx]
            _best_sc, _best_idx, _best_txt = _aud_scored_sorted[0]

            st.markdown(
                '<div style="background:#1a1a0a;border:2px solid #f59e0b;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                '<div style="font-size:0.75rem;font-weight:700;color:#f59e0b">⚠️ ПРАВИЛО AMAZON</div>' +
                '<div style="font-size:0.82rem;color:#fde68a;margin-top:4px">Главное фото (#1) <b>всегда белый фон</b> — это обязательное требование Amazon. Оно не участвует в ранжировании по ЦА.</div>' +
                '</div>', unsafe_allow_html=True)

            # Best lifestyle photo
            if _rest_sorted:
                _bsc, _bidx, _btxt = _rest_sorted[0]
                _brm = _re3.search(r'РЕКОМЕНДАЦИЯ:(.+?)(?=\n[А-Я]|$)', _btxt, _re3.DOTALL)
                _brec = _brm.group(1).strip()[:200] if _brm else ""
                st.markdown(
                    f'<div style="background:#0f3a1a;border:2px solid #22c55e;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                    f'<div style="font-size:0.9rem;font-weight:800;color:#22c55e">🥇 Лучшее lifestyle фото для ЦА — Фото #{_bidx} ({_bsc}%)</div>' +
                    (f'<div style="font-size:0.78rem;color:#86efac;margin-top:4px">{_brec}</div>' if _brec else "") +
                    f'</div>', unsafe_allow_html=True)

            # Ordered ranking table
            _rank_html = '<div style="background:#0f172a;border-radius:10px;padding:12px 16px">' + '<div style="font-size:0.75rem;font-weight:700;color:#64748b;margin-bottom:8px">РЕКОМЕНДУЕМЫЙ ПОРЯДОК ФОТО</div>'
            # Position 1 = always original main photo (white bg)
            _rank_html += (
                '<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                '<span style="font-size:0.8rem;color:#94a3b8">📸 Главное (#1) — Amazon rule</span>' +
                f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #1</span>' +
                '<span style="font-size:0.75rem;color:#f59e0b">🤍 Белый фон</span>' +
                '</div>'
            )
            _position_labels2 = ["🥇 Второе (#2) — лучшее lifestyle", "🥈 Третье (#3)", "🥉 Четвёртое (#4)", "4️⃣ Пятое (#5)", "5️⃣ Шестое (#6)"]
            for _pi, (_psc, _pidx, _ptxt) in enumerate(_rest_sorted):
                _pc = "#22c55e" if _psc>=75 else ("#f59e0b" if _psc>=50 else "#ef4444")
                _plbl = _position_labels2[_pi] if _pi < len(_position_labels2) else f"#{_pi+2}"
                _rank_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                    f'<span style="font-size:0.8rem;color:#94a3b8">{_plbl}</span>' +
                    f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #{_pidx}</span>' +
                    f'<span style="font-size:0.85rem;font-weight:700;color:{_pc}">{_psc}%</span>' +
                    f'</div>'
                )
            _rank_html += '</div>'
            st.markdown(_rank_html, unsafe_allow_html=True)

            # AI summary prompt
            if st.button("🧠 AI — сводный план улучшений", key="btn_aud_summary"):
                with st.spinner("🧠 Генерирую план..."):
                    _all_reviews = "\n\n".join([
                        f"ФОТО #{_ar2['idx']} ({_ar2['text'][:300]})"
                        for _ar2 in st.session_state["_aud_results"]
                    ])
                    _sum_prompt = f"""На основе оценки {len(st.session_state["_aud_results"])} фото листинга для ЦА ({_aud_age}, {_aud_lifestyle}, {_aud_income}):

{_all_reviews}

Дай КРАТКИЙ план из 3-5 конкретных действий чтобы улучшить весь набор фото для этой ЦА.
Формат: пронумерованный список, каждый пункт = одно действие + ожидаемый эффект (+X% конверсии).
Язык: {'русский' if st.session_state.get('analysis_lang','ru')=='ru' else 'английский'}."""
                    _sum_r = ai_call("Amazon photo strategist", _sum_prompt, max_tokens=600)
                    st.session_state["_aud_summary"] = _sum_r

            if st.session_state.get("_aud_summary"):
                st.markdown(
                    '<div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-top:8px">' +
                    '<div style="font-size:0.72rem;font-weight:700;color:#64748b;margin-bottom:8px">📋 ПЛАН УЛУЧШЕНИЙ ФОТО ДЛЯ ЦА</div>' +
                    f'<div style="font-size:0.85rem;color:#e2e8f0;line-height:1.7">{st.session_state["_aud_summary"].replace(chr(10),"<br>")}</div></div>',
                    unsafe_allow_html=True)

            if st.button("🗑️ Очистить", key="clear_aud"):
                st.session_state.pop("_aud_results", None)
                st.session_state.pop("_aud_summary", None)
                st.rerun()

    # ── Claid AI Photo Generator ──────────────────────────────────────────────
    _claid_key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("CLAID_API_KEY","")
    if _claid_key:  # GEMINI_API_KEY
        with st.expander("🎨 AI-генерация lifestyle фото — Gemini Nano Banana Pro", expanded=False):
            st.caption("Выбери любое фото из анализа или загрузи своё → AI создаст lifestyle сцену")
            _claid_col1, _claid_col2 = st.columns([3,2])
            with _claid_col1:
                _scene_options = {
                "🤍 Белый фон (Amazon Main Image)": "pure white background, professional studio product photography, soft shadow, white backdrop, e-commerce",
                "🏔️ Hiking / Outdoor горы": "on a rugged mountain trail, scattered rocks, sunlight through pine trees, rich earthy colors, warm light, professional product photography",
                "🏠 Уютный дом / Interior": "cozy modern home interior, warm lighting, wooden furniture, lifestyle product photography",
                "🏋️ Gym / Фитнес": "modern gym interior, fitness lifestyle, dynamic lighting, professional product photography",
                "❄️ Зима / Snow": "snowy mountain landscape, cold winter day, soft blue light, outdoor winter lifestyle",
                "🌊 Лето / Summer beach": "sunny summer day, outdoor adventure, bright natural light, lifestyle product photography",
                "🌿 Nature / Природа": "lush green forest path, dappled sunlight, natural outdoor setting, professional photography",
                "💼 Офис / Professional": "modern office setting, professional lifestyle, clean minimalist background",
            }
            _scene_label = st.selectbox("Сцена", list(_scene_options.keys()), key="claid_scene_sel")
            _claid_scene = _scene_options[_scene_label]
            if "белый" in _scene_label.lower() or "Amazon" in _scene_label:
                st.caption("✅ Подходит для главного фото Amazon (#1) — чистый белый фон")
            else:
                st.caption("📸 Lifestyle сцена — для фото #2-7 в листинге")

            # Photos from analysis
            _analysis_imgs = st.session_state.get("images", [])
            _img_b64_claid = None
            _img_mt_claid = "image/jpeg"

            if _analysis_imgs:
                st.markdown("**📸 Выбери фото из анализа:**")
                _img_cols = st.columns(min(len(_analysis_imgs), 5))
                for _pi, (_pc, _pimg) in enumerate(zip(_img_cols, _analysis_imgs[:5])):
                    with _pc:
                        try:
                            _pb64 = _pimg.get("b64","") if isinstance(_pimg, dict) else _pimg
                            _pmt = _pimg.get("media_type","image/jpeg") if isinstance(_pimg, dict) else "image/jpeg"
                            st.image(f"data:{_pmt};base64,{_pb64}", use_container_width=True)
                            if st.button(f"✓ Фото {_pi+1}", key=f"claid_pick_{_pi}", use_container_width=True):
                                st.session_state["_claid_picked_b64"] = _pb64
                                st.session_state["_claid_picked_mt"] = _pmt
                                st.session_state["_claid_picked_idx"] = _pi+1
                                st.rerun()
                        except: pass

                if st.session_state.get("_claid_picked_b64"):
                    _img_b64_claid = st.session_state["_claid_picked_b64"]
                    _img_mt_claid = st.session_state.get("_claid_picked_mt","image/jpeg")
                    st.success(f"✅ Выбрано фото #{st.session_state.get('_claid_picked_idx',1)}")

            # Or upload new
            with _claid_col2:
                st.markdown("**или загрузи своё:**")
                _claid_upload = st.file_uploader("📤 PNG/JPG", type=["png","jpg","jpeg"], key="claid_upload", label_visibility="collapsed")
                if _claid_upload:
                    import base64 as _b64c
                    _img_bytes = _claid_upload.read()
                    _img_b64_claid = _b64c.b64encode(_img_bytes).decode()
                    _img_mt_claid = "image/jpeg" if _claid_upload.name.lower().endswith(("jpg","jpeg")) else "image/png"
                    st.image(_img_bytes, width=120)

            # Показываем рекомендацию из Vision анализа для выбранного фото
            _picked_idx = st.session_state.get("_claid_picked_idx", 0)
            if _picked_idx and st.session_state.get("vision"):
                import re as _re_v
                _v_text = st.session_state.get("vision","")
                _blk_m = _re_v.search(rf"PHOTO_BLOCK_{_picked_idx}\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", _v_text, _re_v.DOTALL)
                if _blk_m:
                    _blk = _blk_m.group(1)
                    _act_m = _re_v.search(r"(?:Действие|Action)\s*[:\-]\s*(.{10,})", _blk)
                    _wk_m  = _re_v.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{10,})", _blk)
                    if _act_m or _wk_m:
                        st.markdown("**🤖 AI рекомендует исправить:**")
                        if _wk_m:  st.warning(f"⚠️ {_wk_m.group(1).strip()[:150]}")
                        if _act_m:
                            _ai_action = _act_m.group(1).strip()[:200]
                            st.info(f"→ {_ai_action}")
                            # Авто-подставляем рекомендацию как сцену
                            if st.checkbox("🎯 Использовать рекомендацию AI как сцену", value=True, key="use_ai_scene"):
                                _claid_scene = _ai_action

            if _img_b64_claid:
                if st.button("🎨 Генерировать улучшенное фото", type="primary", key="btn_claid_gen"):
                    with st.spinner(f"🎨 Gemini генерирует..."):
                        _urls, _err = claid_generate_lifestyle(_img_b64_claid, scene=_claid_scene, media_type=_img_mt_claid)
                        if _err:
                            st.error(f"❌ {_err}")
                        elif _urls:
                            st.session_state["_claid_results"] = _urls
                            st.success(f"✅ {len(_urls)} фото готово!")
                        else:
                            st.warning("Фото не получены — попробуй ещё раз")

            if st.session_state.get("_claid_results"):
                _cr = st.session_state["_claid_results"]
                st.markdown(f"**🖼️ Результат ({len(_cr)} фото):**")
                _rc = st.columns(min(len(_cr), 4))
                for _i, (_rcol, _rimg) in enumerate(zip(_rc, _cr)):
                    with _rcol:
                        if isinstance(_rimg, dict) and _rimg.get("b64"):
                            import base64 as _b64dl
                            _rb64=_rimg["b64"]; _rmt=_rimg.get("mt","image/jpeg")
                            st.image(f"data:{_rmt};base64,{_rb64}", use_container_width=True)
                            st.download_button(f"⬇️ #{_i+1}", _b64dl.b64decode(_rb64), f"lifestyle_{_i+1}.jpg", "image/jpeg", key=f"dl_img_{_i}", use_container_width=True)
                        elif isinstance(_rimg, str):
                            st.image(_rimg, use_container_width=True)

                if st.button("🗑️ Очистить", key="claid_clear"):
                    st.session_state.pop("_claid_results", None)
                    st.session_state.pop("_claid_picked_b64", None)
                    st.rerun()

    _all_blocks = re.split(r"PHOTO_BLOCK_\d+", v) if v else []
    blocks = [b.strip() for b in _all_blocks if b.strip() and re.search(r"\d+/10", b)]
    if not blocks: blocks = [b.strip() for b in _all_blocks if b.strip()]

    # ── Сводка по всем фото ────────────────────────────────────────────────
    if blocks:
        _scores_sum = []
        for _bi, _bt in enumerate(blocks):
            _sm = re.search(r"(\d+)/10", _bt)
            _sv = int(_sm.group(1)) if _sm else 0
            _tm = re.search(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)", _bt)
            _tv = _tm.group(1).strip()[:18] if _tm else f"#{_bi+1}"
            _scores_sum.append((_bi+1, _sv, _tv))
        _avg = sum(s for _,s,_ in _scores_sum) / len(_scores_sum) if _scores_sum else 0
        _avg_c = "#22c55e" if _avg>=7 else ("#f59e0b" if _avg>=5 else "#ef4444")
        _cards_html = ""
        for _num, _sc, _tp in _scores_sum:
            _cc = "#22c55e" if _sc>=8 else ("#f59e0b" if _sc>=6 else "#ef4444")
            _cards_html += (
                f'<div style="display:flex;flex-direction:column;align-items:center;'
                f'background:#1e293b;border-radius:8px;padding:8px 10px;min-width:68px;'
                f'border-top:3px solid {_cc}">'
                f'<div style="font-size:0.68rem;color:#64748b;margin-bottom:2px">#{_num}</div>'
                f'<div style="font-size:1.5rem;font-weight:800;color:{_cc};line-height:1">{_sc}</div>'
                f'<div style="font-size:0.6rem;color:{_cc}">/10</div>'
                f'<div style="font-size:0.58rem;color:#64748b;margin-top:3px;text-align:center;'
                f'max-width:62px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{_tp}</div>'
                f'</div>'
            )
        st.markdown(
            f'<div style="background:#0f172a;border-radius:12px;padding:14px 16px;margin-bottom:16px">'
            f'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">'
            f'<div style="font-size:0.85rem;font-weight:700;color:#94a3b8">📊 Итог: {len(_scores_sum)} фото</div>'
            f'<div style="font-size:1.1rem;font-weight:800;color:{_avg_c}">Средняя: {_avg:.1f}/10</div>'
            f'</div>'
            f'<div style="display:flex;gap:8px;flex-wrap:wrap">{_cards_html}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
        st.divider()

        # ── AI McKinsey-style overall gallery assessment ──────────────────
        if st.button("🧠 AI-оценка галереи", key="btn_gallery_ai", use_container_width=False, help="McKinsey-вывод по всей галерее: что видит покупатель, главные проблемы и одно действие с максимальным ROI."):
            with st.spinner("🧠 Анализирую всю галерею..."):
                _aud_ctx = st.session_state.get("target_audience","")
                _aud_str = f"\nЦелевая аудитория: {_aud_ctx}" if _aud_ctx else ""
                _all_vision_text = "\n\n".join([
                    f"Фото #{_i+1}: {_bt[:400]}"
                    for _i,_bt in enumerate(blocks)
                ])
                _gal_prompt = f"""Ты McKinsey-консультант по Amazon конверсии. Проанализируй всю фотогалерею листинга как единое целое.

Товар: {od.get('title','')}
{_aud_str}

АНАЛИЗ ФОТО:
{_all_vision_text[:3000]}

Дай оценку в формате:
**Общее впечатление покупателя** (1-2 предл.): что чувствует покупатель просматривая галерею целиком
**Визуальная последовательность**: есть ли логика от главного фото к деталям, или хаос
**Топ-2 сильных стороны** галереи
**Топ-2 критических проблемы** которые убивают конверсию
**McKinsey-вывод** (1 предл.): "Галерея [делает X], но [не закрывает Y], что приводит к Z"
**Одно действие с максимальным ROI**: что изменить прямо сейчас

Ответь {'по-русски' if st.session_state.get('analysis_lang','ru')=='ru' else 'in English'}."""
                _gal_result = ai_call("Amazon photo gallery expert. Concise McKinsey-style.", _gal_prompt, max_tokens=800)
                st.session_state["_gallery_ai"] = _gal_result

        if st.session_state.get("_gallery_ai"):
            st.markdown(
                f'<div style="background:#0f172a;border:1px solid #334155;border-radius:10px;'
                f'padding:16px 20px;margin-bottom:12px">'
                f'<div style="font-size:0.75rem;font-weight:700;color:#64748b;letter-spacing:0.08em;'
                f'margin-bottom:10px">🧠 AI ОЦЕНКА ГАЛЕРЕИ</div>'
                f'<div style="color:#e2e8f0;font-size:0.9rem;line-height:1.6">{st.session_state["_gallery_ai"].replace(chr(10),"<br>")}</div>'
                f'</div>',
                unsafe_allow_html=True
            )
        st.divider()



    def _render_photo_block(img_data, text, idx):
        sm = re.search(r"(\d+)/10", text)
        score = int(sm.group(1)) if sm else 0
        bc = "#22c55e" if score>=8 else ("#f59e0b" if score>=6 else "#ef4444")
        slbl = "Отлично" if score>=8 else ("Хорошо" if score>=6 else "Слабо")
        _s = lambda pat: re.search(pat, text)
        _strip = lambda m: m.group(1).strip().strip("*").strip() if m else ""
        typ  = _strip(_s(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)"))
        stxt = _strip(_s(r"(?:[Сс]ильная\s+сторона|Strength)\s*[:\-]?\s*(.{3,})"))
        wtxt = _strip(_s(r"(?:[Сс]лабость|Weakness)\s*[:\-]?\s*(.{3,})"))
        atxt = _strip(_s(r"(?:[Дд]ействие|Action)\s*[:\-]?\s*(.{3,})"))
        ctxt = _strip(_s(r"(?:[Кк]онверсия|Conversion)\s*[:\-]?\s*(.{3,})"))
        etxt = _strip(_s(r"(?:[Ээ]моция|Emotion)\s*[:\-]?\s*(.{3,})"))
        if wtxt and any(x in wtxt.lower() for x in ["none","n/a","no weakness","нет слабостей"]): wtxt = ""
        with st.container(border=True):
            if img_data:
                c1,c2 = st.columns([1,2])
                with c1: st.image(__import__("base64").b64decode(img_data["b64"]), use_container_width=True)
                col = c2
            else:
                col = st.container()
            with col:
                st.markdown(f"**Фото #{idx+1}" + (f" — {typ}" if typ else "") + "**")
                if score > 0:
                    st.markdown(f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0"><div style="font-size:2rem;font-weight:800;color:{bc}">{score}/10</div><div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px"><div style="background:{bc};width:{score*10}%;height:10px;border-radius:6px"></div></div><div style="color:{bc};font-size:0.8rem;margin-top:2px">{slbl}</div></div></div>', unsafe_allow_html=True)
                if stxt: st.success(f"✅ {stxt}")
                if wtxt: st.warning(f"⚠️ {wtxt}")
                if atxt:
                    with st.expander("🛠 Что делать"): st.markdown(f"→ **{atxt}**")
                if ctxt:
                    with st.expander("💡 Конверсия"): st.markdown(f"🎯 {ctxt}")
                if etxt:
                    _ec = {"доверие":"#22c55e","trust":"#22c55e","желание":"#f59e0b","сомнение":"#ef4444","doubt":"#ef4444","любопытство":"#3b82f6","curiosity":"#3b82f6","безразличие":"#94a3b8","indifference":"#94a3b8"}.get(etxt.split()[0].lower().rstrip("/:"), "#8b5cf6")
                    st.markdown(f'<div style="background:{_ec}22;border-left:3px solid {_ec};border-radius:6px;padding:8px 12px;margin-top:4px"><span style="font-size:0.8rem;font-weight:700;color:{_ec}">😶 ЭМОЦИЯ: </span><span style="font-size:0.82rem;color:#1e293b">{etxt}</span></div>', unsafe_allow_html=True)

    if not imgs and blocks:
        st.info("📅 История: показан текстовый анализ Vision (фото сохраняются только в новых анализах)")
        for i, text in enumerate(blocks): _render_photo_block(None, text, i)
        st.stop()
    if not imgs:
        st.info("👁️ Vision фото был отключён — запусти повторно с включённым чекбоксом")
        st.stop()
    for i, img in enumerate(imgs):
        _render_photo_block(img, blocks[i] if i < len(blocks) else "", i)

# ══ A+ Контент ════════════════════════════════════════════════════════════════
elif page == "🎨 A+ Контент":
    st.title("🎨 A+ Контент")
    if not od or not od.get("title"):
        st.info("ℹ️ Эта страница доступна только при анализе **нашего листинга**. Добавь URL в поле 🔵 НАШ листинг и перезапусти.")
        st.stop()

    # ── Audience Score ────────────────────────────────────────────────────────
    with st.expander("👥 AI Audience Score — оценка фото для вашей ЦА", expanded=False):
        st.caption("AI оценивает каждое фото с точки зрения вашей целевой аудитории")
        _aud_col1, _aud_col2 = st.columns(2)
        with _aud_col1:
            _aud_age = st.text_input("👤 Возраст и пол", placeholder="Мужчина 30-45 лет", key="aud_age")
            _aud_lifestyle = st.text_input("🏃 Образ жизни", placeholder="Активный, outdoor, hiking, путешествия", key="aud_lifestyle")
        with _aud_col2:
            _aud_income = st.selectbox("💰 Доход", ["Средний", "Выше среднего", "Высокий", "Любой"], key="aud_income")
            _aud_geo = st.text_input("🌍 География", placeholder="Германия, EU", key="aud_geo")
        _aud_extra = st.text_area("💬 Доп. инфо о ЦА", placeholder="Ценит качество, экологичность, покупает онлайн, читает отзывы...", height=68, key="aud_extra")

        _imgs_for_aud = st.session_state.get("images", [])
        if not _imgs_for_aud:
            st.info("Запусти анализ чтобы загрузить фото листинга")
        elif st.button("👥 Оценить все фото для ЦА", type="primary", key="btn_audience_score"):
            if not (_aud_age or _aud_lifestyle):
                st.warning("Заполни хотя бы возраст/пол и образ жизни")
            else:
                _aud_profile = f"""
ЦЕЛЕВАЯ АУДИТОРИЯ:
- Возраст/пол: {_aud_age}
- Образ жизни: {_aud_lifestyle}
- Доход: {_aud_income}
- География: {_aud_geo}
- Дополнительно: {_aud_extra}
""".strip()
                _aud_results = []
                _vprog = st.progress(0, "👥 Оцениваю фото для ЦА...")
                for _ai_idx, _aimg in enumerate(_imgs_for_aud[:6]):
                    _vprog.progress(int((_ai_idx+1)/min(len(_imgs_for_aud),6)*100), f"Фото {_ai_idx+1}...")
                    try:
                        _ab64 = _aimg.get("b64","") if isinstance(_aimg, dict) else _aimg
                        _amt = _aimg.get("media_type","image/jpeg") if isinstance(_aimg, dict) else "image/jpeg"
                        # Photo #1 = main Amazon image (white bg required)
                        _is_main_photo = (_ai_idx == 0)
                        _photo_context = """ВАЖНО: Это ГЛАВНОЕ фото листинга (#1).
По правилам Amazon главное фото ОБЯЗАНО быть на чистом белом фоне (RGB 255,255,255).
НЕ рекомендуй менять белый фон на lifestyle — это нарушит правила Amazon и приведёт к suppression.
Оценивай только: позу модели, читаемость товара, посадку, выражение лица, динамику движения, 
соответствие модели ЦА. Рекомендуй улучшения в рамках белого фона (поза, движение, угол съёмки).""" if _is_main_photo else """Это дополнительное фото #{} листинга. Оценивай свободно — фон, контекст, lifestyle.""".format(_ai_idx+1)

                        _aprompt = f"""Ты маркетолог-эксперт по Amazon. Оцени это фото товара с точки зрения целевой аудитории.

{_aud_profile}

ПРОДУКТ: {od.get('title','')}

{_photo_context}

Оцени по шкале 0-100% насколько это фото убедительно для данной аудитории.

Ответь строго в формате:
SCORE: [0-100]%
ЭМОЦИЯ_ЦА: [что чувствует покупатель глядя на это фото]
СООТВЕТСТВУЕТ: [что на фото совпадает с интересами ЦА]
НЕ СООТВЕТСТВУЕТ: [что не совпадает или отталкивает ЦА]
РЕКОМЕНДАЦИЯ: [одно конкретное улучшение — поза/движение/угол для #1, сцена/контекст для остальных]"""
                        _aud_resp = anthropic_vision(
                            content_blocks=[
                                {"type":"image","source":{"type":"base64","media_type":_amt,"data":_ab64}},
                                {"type":"text","text":_aprompt}
                            ],
                            max_tokens=500,
                            system="Ты маркетолог-эксперт по Amazon. Отвечай строго по формату."
                        )
                        _aud_text = _aud_resp.get("content",[{}])[0].get("text","") if isinstance(_aud_resp, dict) else str(_aud_resp)
                        _aud_results.append({"idx": _ai_idx+1, "text": _aud_text, "b64": _ab64, "mt": _amt})
                    except Exception as _ae:
                        _aud_results.append({"idx": _ai_idx+1, "text": f"Ошибка: {_ae}", "b64": "", "mt": ""})
                _vprog.progress(100, "✅ Готово!")
                st.session_state["_aud_results"] = _aud_results

        # Show results
        if st.session_state.get("_aud_results"):
            st.markdown("---")
            for _ar in st.session_state["_aud_results"]:
                _arc1, _arc2 = st.columns([1, 3])
                with _arc1:
                    if _ar.get("b64"):
                        st.image(f"data:{_ar['mt']};base64,{_ar['b64']}", use_container_width=True)
                    st.caption(f"Фото #{_ar['idx']}")
                with _arc2:
                    _txt = _ar["text"]
                    # Extract score for color
                    import re as _re2
                    _sm = _re2.search(r'SCORE:\s*(\d+)%', _txt)
                    _sc = int(_sm.group(1)) if _sm else 0
                    _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else "#ef4444")
                    _sc_l = "✅ Отлично для ЦА" if _sc>=75 else ("🟡 Средне" if _sc>=50 else "🔴 Слабо для ЦА")
                    st.markdown(
                        f'<div style="background:#0f172a;border-left:4px solid {_sc_c};border-radius:8px;padding:12px 14px">' +
                        f'<div style="font-size:1.4rem;font-weight:800;color:{_sc_c}">{_sc}% <span style="font-size:0.8rem">{_sc_l}</span></div>' +
                        f'<div style="font-size:0.82rem;color:#e2e8f0;margin-top:8px;white-space:pre-line">{_txt.replace("SCORE:","").replace(f"{_sc}%","",1).strip()}</div>' +
                        f'</div>',
                        unsafe_allow_html=True)
            # ── Summary & Ranking ────────────────────────────────────────────
            import re as _re3
            _aud_scored = []
            for _ar2 in st.session_state["_aud_results"]:
                _sm2 = _re3.search(r'SCORE:\s*(\d+)%', _ar2["text"])
                _sc2 = int(_sm2.group(1)) if _sm2 else 0
                _aud_scored.append((_sc2, _ar2["idx"], _ar2["text"]))
            _aud_scored_sorted = sorted(_aud_scored, reverse=True)

            st.markdown("---")
            st.markdown("### 🏆 Итоговый рейтинг фото для вашей ЦА")

            # Detect which photo has white background (likely photo #1 from listing)
            # Main image (#1) MUST be white bg per Amazon rules — pin it to position #1
            _main_photo_idx = 1  # always keep original #1 as main (white bg rule)
            _rest_sorted = [(sc,idx,txt) for sc,idx,txt in _aud_scored_sorted if idx != _main_photo_idx]
            _best_sc, _best_idx, _best_txt = _aud_scored_sorted[0]

            st.markdown(
                '<div style="background:#1a1a0a;border:2px solid #f59e0b;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                '<div style="font-size:0.75rem;font-weight:700;color:#f59e0b">⚠️ ПРАВИЛО AMAZON</div>' +
                '<div style="font-size:0.82rem;color:#fde68a;margin-top:4px">Главное фото (#1) <b>всегда белый фон</b> — это обязательное требование Amazon. Оно не участвует в ранжировании по ЦА.</div>' +
                '</div>', unsafe_allow_html=True)

            # Best lifestyle photo
            if _rest_sorted:
                _bsc, _bidx, _btxt = _rest_sorted[0]
                _brm = _re3.search(r'РЕКОМЕНДАЦИЯ:(.+?)(?=\n[А-Я]|$)', _btxt, _re3.DOTALL)
                _brec = _brm.group(1).strip()[:200] if _brm else ""
                st.markdown(
                    f'<div style="background:#0f3a1a;border:2px solid #22c55e;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                    f'<div style="font-size:0.9rem;font-weight:800;color:#22c55e">🥇 Лучшее lifestyle фото для ЦА — Фото #{_bidx} ({_bsc}%)</div>' +
                    (f'<div style="font-size:0.78rem;color:#86efac;margin-top:4px">{_brec}</div>' if _brec else "") +
                    f'</div>', unsafe_allow_html=True)

            # Ordered ranking table
            _rank_html = '<div style="background:#0f172a;border-radius:10px;padding:12px 16px">' + '<div style="font-size:0.75rem;font-weight:700;color:#64748b;margin-bottom:8px">РЕКОМЕНДУЕМЫЙ ПОРЯДОК ФОТО</div>'
            # Position 1 = always original main photo (white bg)
            _rank_html += (
                '<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                '<span style="font-size:0.8rem;color:#94a3b8">📸 Главное (#1) — Amazon rule</span>' +
                f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #1</span>' +
                '<span style="font-size:0.75rem;color:#f59e0b">🤍 Белый фон</span>' +
                '</div>'
            )
            _position_labels2 = ["🥇 Второе (#2) — лучшее lifestyle", "🥈 Третье (#3)", "🥉 Четвёртое (#4)", "4️⃣ Пятое (#5)", "5️⃣ Шестое (#6)"]
            for _pi, (_psc, _pidx, _ptxt) in enumerate(_rest_sorted):
                _pc = "#22c55e" if _psc>=75 else ("#f59e0b" if _psc>=50 else "#ef4444")
                _plbl = _position_labels2[_pi] if _pi < len(_position_labels2) else f"#{_pi+2}"
                _rank_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                    f'<span style="font-size:0.8rem;color:#94a3b8">{_plbl}</span>' +
                    f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #{_pidx}</span>' +
                    f'<span style="font-size:0.85rem;font-weight:700;color:{_pc}">{_psc}%</span>' +
                    f'</div>'
                )
            _rank_html += '</div>'
            st.markdown(_rank_html, unsafe_allow_html=True)

            # AI summary prompt
            if st.button("🧠 AI — сводный план улучшений", key="btn_aud_summary"):
                with st.spinner("🧠 Генерирую план..."):
                    _all_reviews = "\n\n".join([
                        f"ФОТО #{_ar2['idx']} ({_ar2['text'][:300]})"
                        for _ar2 in st.session_state["_aud_results"]
                    ])
                    _sum_prompt = f"""На основе оценки {len(st.session_state["_aud_results"])} фото листинга для ЦА ({_aud_age}, {_aud_lifestyle}, {_aud_income}):

{_all_reviews}

Дай КРАТКИЙ план из 3-5 конкретных действий чтобы улучшить весь набор фото для этой ЦА.
Формат: пронумерованный список, каждый пункт = одно действие + ожидаемый эффект (+X% конверсии).
Язык: {'русский' if st.session_state.get('analysis_lang','ru')=='ru' else 'английский'}."""
                    _sum_r = ai_call("Amazon photo strategist", _sum_prompt, max_tokens=600)
                    st.session_state["_aud_summary"] = _sum_r

            if st.session_state.get("_aud_summary"):
                st.markdown(
                    '<div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-top:8px">' +
                    '<div style="font-size:0.72rem;font-weight:700;color:#64748b;margin-bottom:8px">📋 ПЛАН УЛУЧШЕНИЙ ФОТО ДЛЯ ЦА</div>' +
                    f'<div style="font-size:0.85rem;color:#e2e8f0;line-height:1.7">{st.session_state["_aud_summary"].replace(chr(10),"<br>")}</div></div>',
                    unsafe_allow_html=True)

            if st.button("🗑️ Очистить", key="clear_aud"):
                st.session_state.pop("_aud_results", None)
                st.session_state.pop("_aud_summary", None)
                st.rerun()

    # ── Claid AI Photo Generator ──────────────────────────────────────────────
    _claid_key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("CLAID_API_KEY","")
    if _claid_key:  # GEMINI_API_KEY
        with st.expander("🎨 AI-генерация lifestyle фото — Gemini Nano Banana Pro", expanded=False):
            st.caption("Выбери любое фото из анализа или загрузи своё → AI создаст lifestyle сцену")
            _claid_col1, _claid_col2 = st.columns([3,2])
            with _claid_col1:
                _scene_options = {
                "🤍 Белый фон (Amazon Main Image)": "pure white background, professional studio product photography, soft shadow, white backdrop, e-commerce",
                "🏔️ Hiking / Outdoor горы": "on a rugged mountain trail, scattered rocks, sunlight through pine trees, rich earthy colors, warm light, professional product photography",
                "🏠 Уютный дом / Interior": "cozy modern home interior, warm lighting, wooden furniture, lifestyle product photography",
                "🏋️ Gym / Фитнес": "modern gym interior, fitness lifestyle, dynamic lighting, professional product photography",
                "❄️ Зима / Snow": "snowy mountain landscape, cold winter day, soft blue light, outdoor winter lifestyle",
                "🌊 Лето / Summer beach": "sunny summer day, outdoor adventure, bright natural light, lifestyle product photography",
                "🌿 Nature / Природа": "lush green forest path, dappled sunlight, natural outdoor setting, professional photography",
                "💼 Офис / Professional": "modern office setting, professional lifestyle, clean minimalist background",
            }
            _scene_label = st.selectbox("Сцена", list(_scene_options.keys()), key="claid_scene_sel")
            _claid_scene = _scene_options[_scene_label]
            if "белый" in _scene_label.lower() or "Amazon" in _scene_label:
                st.caption("✅ Подходит для главного фото Amazon (#1) — чистый белый фон")
            else:
                st.caption("📸 Lifestyle сцена — для фото #2-7 в листинге")

            # Photos from analysis
            _analysis_imgs = st.session_state.get("images", [])
            _img_b64_claid = None
            _img_mt_claid = "image/jpeg"

            if _analysis_imgs:
                st.markdown("**📸 Выбери фото из анализа:**")
                _img_cols = st.columns(min(len(_analysis_imgs), 5))
                for _pi, (_pc, _pimg) in enumerate(zip(_img_cols, _analysis_imgs[:5])):
                    with _pc:
                        try:
                            _pb64 = _pimg.get("b64","") if isinstance(_pimg, dict) else _pimg
                            _pmt = _pimg.get("media_type","image/jpeg") if isinstance(_pimg, dict) else "image/jpeg"
                            st.image(f"data:{_pmt};base64,{_pb64}", use_container_width=True)
                            if st.button(f"✓ Фото {_pi+1}", key=f"claid_pick_{_pi}", use_container_width=True):
                                st.session_state["_claid_picked_b64"] = _pb64
                                st.session_state["_claid_picked_mt"] = _pmt
                                st.session_state["_claid_picked_idx"] = _pi+1
                                st.rerun()
                        except: pass

                if st.session_state.get("_claid_picked_b64"):
                    _img_b64_claid = st.session_state["_claid_picked_b64"]
                    _img_mt_claid = st.session_state.get("_claid_picked_mt","image/jpeg")
                    st.success(f"✅ Выбрано фото #{st.session_state.get('_claid_picked_idx',1)}")

            # Or upload new
            with _claid_col2:
                st.markdown("**или загрузи своё:**")
                _claid_upload = st.file_uploader("📤 PNG/JPG", type=["png","jpg","jpeg"], key="claid_upload", label_visibility="collapsed")
                if _claid_upload:
                    import base64 as _b64c
                    _img_bytes = _claid_upload.read()
                    _img_b64_claid = _b64c.b64encode(_img_bytes).decode()
                    _img_mt_claid = "image/jpeg" if _claid_upload.name.lower().endswith(("jpg","jpeg")) else "image/png"
                    st.image(_img_bytes, width=120)

            # Показываем рекомендацию из Vision анализа для выбранного фото
            _picked_idx = st.session_state.get("_claid_picked_idx", 0)
            if _picked_idx and st.session_state.get("vision"):
                import re as _re_v
                _v_text = st.session_state.get("vision","")
                _blk_m = _re_v.search(rf"PHOTO_BLOCK_{_picked_idx}\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", _v_text, _re_v.DOTALL)
                if _blk_m:
                    _blk = _blk_m.group(1)
                    _act_m = _re_v.search(r"(?:Действие|Action)\s*[:\-]\s*(.{10,})", _blk)
                    _wk_m  = _re_v.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{10,})", _blk)
                    if _act_m or _wk_m:
                        st.markdown("**🤖 AI рекомендует исправить:**")
                        if _wk_m:  st.warning(f"⚠️ {_wk_m.group(1).strip()[:150]}")
                        if _act_m:
                            _ai_action = _act_m.group(1).strip()[:200]
                            st.info(f"→ {_ai_action}")
                            # Авто-подставляем рекомендацию как сцену
                            if st.checkbox("🎯 Использовать рекомендацию AI как сцену", value=True, key="use_ai_scene"):
                                _claid_scene = _ai_action

            if _img_b64_claid:
                if st.button("🎨 Генерировать улучшенное фото", type="primary", key="btn_claid_gen"):
                    with st.spinner(f"🎨 Gemini генерирует..."):
                        _urls, _err = claid_generate_lifestyle(_img_b64_claid, scene=_claid_scene, media_type=_img_mt_claid)
                        if _err:
                            st.error(f"❌ {_err}")
                        elif _urls:
                            st.session_state["_claid_results"] = _urls
                            st.success(f"✅ {len(_urls)} фото готово!")
                        else:
                            st.warning("Фото не получены — попробуй ещё раз")

            if st.session_state.get("_claid_results"):
                _cr = st.session_state["_claid_results"]
                st.markdown(f"**🖼️ Результат ({len(_cr)} фото):**")
                _rc = st.columns(min(len(_cr), 4))
                for _i, (_rcol, _rimg) in enumerate(zip(_rc, _cr)):
                    with _rcol:
                        if isinstance(_rimg, dict) and _rimg.get("b64"):
                            import base64 as _b64dl
                            _rb64=_rimg["b64"]; _rmt=_rimg.get("mt","image/jpeg")
                            st.image(f"data:{_rmt};base64,{_rb64}", use_container_width=True)
                            st.download_button(f"⬇️ #{_i+1}", _b64dl.b64decode(_rb64), f"lifestyle_{_i+1}.jpg", "image/jpeg", key=f"dl_img_{_i}", use_container_width=True)
                        elif isinstance(_rimg, str):
                            st.image(_rimg, use_container_width=True)

                if st.button("🗑️ Очистить", key="claid_clear"):
                    st.session_state.pop("_claid_results", None)
                    st.session_state.pop("_claid_picked_b64", None)
                    st.rerun()

    _av = st.session_state.get("aplus_vision","")
    _av_urls = st.session_state.get("aplus_img_urls", [])
    if not _av_urls:
        _av_urls = od.get("aplus_image_urls", od.get("aplus_images", []))
        _av_urls = [re.sub(r'\.__CR[^.]+_PT0_SX\d+_V\d+___','',u) for u in _av_urls if isinstance(u,str) and u.startswith("http")]
    _aplus_text = od.get("aplus_content","") or ""
    _desc_text  = od.get("description","") or ""

    if _aplus_text or (od.get("aplus") and _desc_text):
        with st.expander("📄 A+ Текстовый контент", expanded=not _av_urls):
            if _aplus_text: st.markdown(str(_aplus_text)[:3000])
            if od.get("aplus") and _desc_text: st.markdown(str(_desc_text)[:2000])

    if not _av and not _av_urls and not _aplus_text:
        st.info("ℹ️ У этого листинга нет A+ контента." if not od.get("aplus") else "⚠️ A+ баннеры не проанализированы. Нажми 🔄 Обновить анализ.")
    elif _av_urls and not _av:
        st.info("👁️ Vision A+ был отключён — баннеры без анализа.")
        for _bi, _url in enumerate(_av_urls[:8]): st.image(_url, caption=f"A+ баннер #{_bi+1}", use_container_width=True)
    else:
        _av_total = pct(r.get("aplus_score", 0))
        if _av_total: st.metric("A+ Score", f"{_av_total}%")
        _av_blocks = []
        for _m in re.finditer(r"APLUS_BLOCK_\d+\s*(.*?)(?=APLUS_BLOCK_\d+|$)", _av, re.DOTALL):
            _blk = _m.group(1).strip()
            if _blk: _av_blocks.append("\n".join([l for l in _blk.split("\n") if not re.match(r"^#+\s|^---",l.strip())]).strip())
        _av_blocks = [b for b in _av_blocks if b]
        st.markdown(f"**{len(_av_blocks)} баннер(ов)**"); st.divider()
        for _bi, _block in enumerate(_av_blocks):
            _av_score = int(re.search(r"(?:Оценка|Score)\s*[:\-]?\s*(\d+)", _block).group(1)) if re.search(r"(?:Оценка|Score)\s*[:\-]?\s*(\d+)", _block) else 0
            _s = lambda pat: re.search(pat, _block)
            _strip = lambda m: m.group(1).strip() if m else ""
            _av_mod  = _strip(_s(r"(?:Модуль|Module)\s*[:\-]\s*(.+)"))
            _av_sum  = _strip(_s(r"(?:Содержание|Summary)\s*[:\-]\s*(.+)"))
            _av_str  = _strip(_s(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.{3,})"))
            _av_weak = _strip(_s(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{3,})"))
            _av_act  = _strip(_s(r"(?:Действие|Action)\s*[:\-]\s*(.{3,})"))
            _av_conv = _strip(_s(r"(?:Конверсия|Conversion)\s*[:\-]\s*(.{3,})"))
            _av_bc = "#22c55e" if _av_score>=8 else ("#f59e0b" if _av_score>=6 else "#ef4444")
            _av_sl = "Отлично" if _av_score>=8 else ("Хорошо" if _av_score>=6 else "Слабо")
            with st.container(border=True):
                if _av_urls and _bi < len(_av_urls): st.image(_av_urls[_bi], use_container_width=True)
                st.markdown(f"**Баннер #{_bi+1}" + (f" — {_av_mod}" if _av_mod else "") + "**")
                if _av_sum: st.markdown(f"_{_av_sum}_")
                if _av_score:
                    st.markdown(f'<div style="display:flex;align-items:center;gap:16px;margin:12px 0"><div style="font-size:3.5rem;font-weight:800;color:{_av_bc};line-height:1">{_av_score}/10</div><div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:12px"><div style="background:{_av_bc};width:{_av_score*10}%;height:12px;border-radius:6px"></div></div><div style="color:{_av_bc};font-size:0.9rem;margin-top:4px;font-weight:700">{_av_sl}</div></div></div>', unsafe_allow_html=True)
                if _av_str:  st.success(f"✅ {_av_str}")
                if _av_weak: st.warning(f"⚠️ {_av_weak}")
                if _av_act:
                    with st.expander("🛠 Что делать"): st.markdown(f"→ {_av_act}")
                if _av_conv:
                    with st.expander("💡 Конверсия"): st.info(f"🎯 {_av_conv}")

# ══ Контент ════════════════════════════════════════════════════════════════════
elif page == "📝 Контент":
    st.title("📝 Анализ контента")
    with st.expander("ℹ️ Как работать с контентом", expanded=False):
        st.markdown("""
**Stop Words** (вверху) — 🚫 красные = мгновенная suppression листинга Amazon. Убирай немедленно!

**Title:** ≤125 символов. Формат: [Материал][Гендер][Тип][Фича][Использование]

**Bullets:** 5 штук, ≤250 байт каждый. Формат: "Фича: Польза. Контекст."

**Keyword Ideas:** введи seed-слово → получи реальные поисковые запросы Amazon → ✅/❌ показывает есть ли они в листинге.

**Что делать:**
1. Убери все 🚫 Stop Words
2. Исправь Title если >125 симв.
3. Добавь ❌ ключевые слова из Keyword Ideas
""")

    if not od or not od.get("title"):
        st.info("ℹ️ Эта страница доступна только при анализе **нашего листинга**. Добавь URL в поле 🔵 НАШ листинг и перезапусти.")
        st.stop()

    # ── Audience Score ────────────────────────────────────────────────────────
    with st.expander("👥 AI Audience Score — оценка фото для вашей ЦА", expanded=False):
        st.caption("AI оценивает каждое фото с точки зрения вашей целевой аудитории")
        _aud_col1, _aud_col2 = st.columns(2)
        with _aud_col1:
            _aud_age = st.text_input("👤 Возраст и пол", placeholder="Мужчина 30-45 лет", key="aud_age")
            _aud_lifestyle = st.text_input("🏃 Образ жизни", placeholder="Активный, outdoor, hiking, путешествия", key="aud_lifestyle")
        with _aud_col2:
            _aud_income = st.selectbox("💰 Доход", ["Средний", "Выше среднего", "Высокий", "Любой"], key="aud_income")
            _aud_geo = st.text_input("🌍 География", placeholder="Германия, EU", key="aud_geo")
        _aud_extra = st.text_area("💬 Доп. инфо о ЦА", placeholder="Ценит качество, экологичность, покупает онлайн, читает отзывы...", height=68, key="aud_extra")

        _imgs_for_aud = st.session_state.get("images", [])
        if not _imgs_for_aud:
            st.info("Запусти анализ чтобы загрузить фото листинга")
        elif st.button("👥 Оценить все фото для ЦА", type="primary", key="btn_audience_score"):
            if not (_aud_age or _aud_lifestyle):
                st.warning("Заполни хотя бы возраст/пол и образ жизни")
            else:
                _aud_profile = f"""
ЦЕЛЕВАЯ АУДИТОРИЯ:
- Возраст/пол: {_aud_age}
- Образ жизни: {_aud_lifestyle}
- Доход: {_aud_income}
- География: {_aud_geo}
- Дополнительно: {_aud_extra}
""".strip()
                _aud_results = []
                _vprog = st.progress(0, "👥 Оцениваю фото для ЦА...")
                for _ai_idx, _aimg in enumerate(_imgs_for_aud[:6]):
                    _vprog.progress(int((_ai_idx+1)/min(len(_imgs_for_aud),6)*100), f"Фото {_ai_idx+1}...")
                    try:
                        _ab64 = _aimg.get("b64","") if isinstance(_aimg, dict) else _aimg
                        _amt = _aimg.get("media_type","image/jpeg") if isinstance(_aimg, dict) else "image/jpeg"
                        # Photo #1 = main Amazon image (white bg required)
                        _is_main_photo = (_ai_idx == 0)
                        _photo_context = """ВАЖНО: Это ГЛАВНОЕ фото листинга (#1).
По правилам Amazon главное фото ОБЯЗАНО быть на чистом белом фоне (RGB 255,255,255).
НЕ рекомендуй менять белый фон на lifestyle — это нарушит правила Amazon и приведёт к suppression.
Оценивай только: позу модели, читаемость товара, посадку, выражение лица, динамику движения, 
соответствие модели ЦА. Рекомендуй улучшения в рамках белого фона (поза, движение, угол съёмки).""" if _is_main_photo else """Это дополнительное фото #{} листинга. Оценивай свободно — фон, контекст, lifestyle.""".format(_ai_idx+1)

                        _aprompt = f"""Ты маркетолог-эксперт по Amazon. Оцени это фото товара с точки зрения целевой аудитории.

{_aud_profile}

ПРОДУКТ: {od.get('title','')}

{_photo_context}

Оцени по шкале 0-100% насколько это фото убедительно для данной аудитории.

Ответь строго в формате:
SCORE: [0-100]%
ЭМОЦИЯ_ЦА: [что чувствует покупатель глядя на это фото]
СООТВЕТСТВУЕТ: [что на фото совпадает с интересами ЦА]
НЕ СООТВЕТСТВУЕТ: [что не совпадает или отталкивает ЦА]
РЕКОМЕНДАЦИЯ: [одно конкретное улучшение — поза/движение/угол для #1, сцена/контекст для остальных]"""
                        _aud_resp = anthropic_vision(
                            content_blocks=[
                                {"type":"image","source":{"type":"base64","media_type":_amt,"data":_ab64}},
                                {"type":"text","text":_aprompt}
                            ],
                            max_tokens=500,
                            system="Ты маркетолог-эксперт по Amazon. Отвечай строго по формату."
                        )
                        _aud_text = _aud_resp.get("content",[{}])[0].get("text","") if isinstance(_aud_resp, dict) else str(_aud_resp)
                        _aud_results.append({"idx": _ai_idx+1, "text": _aud_text, "b64": _ab64, "mt": _amt})
                    except Exception as _ae:
                        _aud_results.append({"idx": _ai_idx+1, "text": f"Ошибка: {_ae}", "b64": "", "mt": ""})
                _vprog.progress(100, "✅ Готово!")
                st.session_state["_aud_results"] = _aud_results

        # Show results
        if st.session_state.get("_aud_results"):
            st.markdown("---")
            for _ar in st.session_state["_aud_results"]:
                _arc1, _arc2 = st.columns([1, 3])
                with _arc1:
                    if _ar.get("b64"):
                        st.image(f"data:{_ar['mt']};base64,{_ar['b64']}", use_container_width=True)
                    st.caption(f"Фото #{_ar['idx']}")
                with _arc2:
                    _txt = _ar["text"]
                    # Extract score for color
                    import re as _re2
                    _sm = _re2.search(r'SCORE:\s*(\d+)%', _txt)
                    _sc = int(_sm.group(1)) if _sm else 0
                    _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else "#ef4444")
                    _sc_l = "✅ Отлично для ЦА" if _sc>=75 else ("🟡 Средне" if _sc>=50 else "🔴 Слабо для ЦА")
                    st.markdown(
                        f'<div style="background:#0f172a;border-left:4px solid {_sc_c};border-radius:8px;padding:12px 14px">' +
                        f'<div style="font-size:1.4rem;font-weight:800;color:{_sc_c}">{_sc}% <span style="font-size:0.8rem">{_sc_l}</span></div>' +
                        f'<div style="font-size:0.82rem;color:#e2e8f0;margin-top:8px;white-space:pre-line">{_txt.replace("SCORE:","").replace(f"{_sc}%","",1).strip()}</div>' +
                        f'</div>',
                        unsafe_allow_html=True)
            # ── Summary & Ranking ────────────────────────────────────────────
            import re as _re3
            _aud_scored = []
            for _ar2 in st.session_state["_aud_results"]:
                _sm2 = _re3.search(r'SCORE:\s*(\d+)%', _ar2["text"])
                _sc2 = int(_sm2.group(1)) if _sm2 else 0
                _aud_scored.append((_sc2, _ar2["idx"], _ar2["text"]))
            _aud_scored_sorted = sorted(_aud_scored, reverse=True)

            st.markdown("---")
            st.markdown("### 🏆 Итоговый рейтинг фото для вашей ЦА")

            # Detect which photo has white background (likely photo #1 from listing)
            # Main image (#1) MUST be white bg per Amazon rules — pin it to position #1
            _main_photo_idx = 1  # always keep original #1 as main (white bg rule)
            _rest_sorted = [(sc,idx,txt) for sc,idx,txt in _aud_scored_sorted if idx != _main_photo_idx]
            _best_sc, _best_idx, _best_txt = _aud_scored_sorted[0]

            st.markdown(
                '<div style="background:#1a1a0a;border:2px solid #f59e0b;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                '<div style="font-size:0.75rem;font-weight:700;color:#f59e0b">⚠️ ПРАВИЛО AMAZON</div>' +
                '<div style="font-size:0.82rem;color:#fde68a;margin-top:4px">Главное фото (#1) <b>всегда белый фон</b> — это обязательное требование Amazon. Оно не участвует в ранжировании по ЦА.</div>' +
                '</div>', unsafe_allow_html=True)

            # Best lifestyle photo
            if _rest_sorted:
                _bsc, _bidx, _btxt = _rest_sorted[0]
                _brm = _re3.search(r'РЕКОМЕНДАЦИЯ:(.+?)(?=\n[А-Я]|$)', _btxt, _re3.DOTALL)
                _brec = _brm.group(1).strip()[:200] if _brm else ""
                st.markdown(
                    f'<div style="background:#0f3a1a;border:2px solid #22c55e;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                    f'<div style="font-size:0.9rem;font-weight:800;color:#22c55e">🥇 Лучшее lifestyle фото для ЦА — Фото #{_bidx} ({_bsc}%)</div>' +
                    (f'<div style="font-size:0.78rem;color:#86efac;margin-top:4px">{_brec}</div>' if _brec else "") +
                    f'</div>', unsafe_allow_html=True)

            # Ordered ranking table
            _rank_html = '<div style="background:#0f172a;border-radius:10px;padding:12px 16px">' + '<div style="font-size:0.75rem;font-weight:700;color:#64748b;margin-bottom:8px">РЕКОМЕНДУЕМЫЙ ПОРЯДОК ФОТО</div>'
            # Position 1 = always original main photo (white bg)
            _rank_html += (
                '<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                '<span style="font-size:0.8rem;color:#94a3b8">📸 Главное (#1) — Amazon rule</span>' +
                f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #1</span>' +
                '<span style="font-size:0.75rem;color:#f59e0b">🤍 Белый фон</span>' +
                '</div>'
            )
            _position_labels2 = ["🥇 Второе (#2) — лучшее lifestyle", "🥈 Третье (#3)", "🥉 Четвёртое (#4)", "4️⃣ Пятое (#5)", "5️⃣ Шестое (#6)"]
            for _pi, (_psc, _pidx, _ptxt) in enumerate(_rest_sorted):
                _pc = "#22c55e" if _psc>=75 else ("#f59e0b" if _psc>=50 else "#ef4444")
                _plbl = _position_labels2[_pi] if _pi < len(_position_labels2) else f"#{_pi+2}"
                _rank_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                    f'<span style="font-size:0.8rem;color:#94a3b8">{_plbl}</span>' +
                    f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #{_pidx}</span>' +
                    f'<span style="font-size:0.85rem;font-weight:700;color:{_pc}">{_psc}%</span>' +
                    f'</div>'
                )
            _rank_html += '</div>'
            st.markdown(_rank_html, unsafe_allow_html=True)

            # AI summary prompt
            if st.button("🧠 AI — сводный план улучшений", key="btn_aud_summary"):
                with st.spinner("🧠 Генерирую план..."):
                    _all_reviews = "\n\n".join([
                        f"ФОТО #{_ar2['idx']} ({_ar2['text'][:300]})"
                        for _ar2 in st.session_state["_aud_results"]
                    ])
                    _sum_prompt = f"""На основе оценки {len(st.session_state["_aud_results"])} фото листинга для ЦА ({_aud_age}, {_aud_lifestyle}, {_aud_income}):

{_all_reviews}

Дай КРАТКИЙ план из 3-5 конкретных действий чтобы улучшить весь набор фото для этой ЦА.
Формат: пронумерованный список, каждый пункт = одно действие + ожидаемый эффект (+X% конверсии).
Язык: {'русский' if st.session_state.get('analysis_lang','ru')=='ru' else 'английский'}."""
                    _sum_r = ai_call("Amazon photo strategist", _sum_prompt, max_tokens=600)
                    st.session_state["_aud_summary"] = _sum_r

            if st.session_state.get("_aud_summary"):
                st.markdown(
                    '<div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-top:8px">' +
                    '<div style="font-size:0.72rem;font-weight:700;color:#64748b;margin-bottom:8px">📋 ПЛАН УЛУЧШЕНИЙ ФОТО ДЛЯ ЦА</div>' +
                    f'<div style="font-size:0.85rem;color:#e2e8f0;line-height:1.7">{st.session_state["_aud_summary"].replace(chr(10),"<br>")}</div></div>',
                    unsafe_allow_html=True)

            if st.button("🗑️ Очистить", key="clear_aud"):
                st.session_state.pop("_aud_results", None)
                st.session_state.pop("_aud_summary", None)
                st.rerun()

    # ── Claid AI Photo Generator ──────────────────────────────────────────────
    _claid_key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("CLAID_API_KEY","")
    if _claid_key:  # GEMINI_API_KEY
        with st.expander("🎨 AI-генерация lifestyle фото — Gemini Nano Banana Pro", expanded=False):
            st.caption("Выбери любое фото из анализа или загрузи своё → AI создаст lifestyle сцену")
            _claid_col1, _claid_col2 = st.columns([3,2])
            with _claid_col1:
                _scene_options = {
                "🤍 Белый фон (Amazon Main Image)": "pure white background, professional studio product photography, soft shadow, white backdrop, e-commerce",
                "🏔️ Hiking / Outdoor горы": "on a rugged mountain trail, scattered rocks, sunlight through pine trees, rich earthy colors, warm light, professional product photography",
                "🏠 Уютный дом / Interior": "cozy modern home interior, warm lighting, wooden furniture, lifestyle product photography",
                "🏋️ Gym / Фитнес": "modern gym interior, fitness lifestyle, dynamic lighting, professional product photography",
                "❄️ Зима / Snow": "snowy mountain landscape, cold winter day, soft blue light, outdoor winter lifestyle",
                "🌊 Лето / Summer beach": "sunny summer day, outdoor adventure, bright natural light, lifestyle product photography",
                "🌿 Nature / Природа": "lush green forest path, dappled sunlight, natural outdoor setting, professional photography",
                "💼 Офис / Professional": "modern office setting, professional lifestyle, clean minimalist background",
            }
            _scene_label = st.selectbox("Сцена", list(_scene_options.keys()), key="claid_scene_sel")
            _claid_scene = _scene_options[_scene_label]
            if "белый" in _scene_label.lower() or "Amazon" in _scene_label:
                st.caption("✅ Подходит для главного фото Amazon (#1) — чистый белый фон")
            else:
                st.caption("📸 Lifestyle сцена — для фото #2-7 в листинге")

            # Photos from analysis
            _analysis_imgs = st.session_state.get("images", [])
            _img_b64_claid = None
            _img_mt_claid = "image/jpeg"

            if _analysis_imgs:
                st.markdown("**📸 Выбери фото из анализа:**")
                _img_cols = st.columns(min(len(_analysis_imgs), 5))
                for _pi, (_pc, _pimg) in enumerate(zip(_img_cols, _analysis_imgs[:5])):
                    with _pc:
                        try:
                            _pb64 = _pimg.get("b64","") if isinstance(_pimg, dict) else _pimg
                            _pmt = _pimg.get("media_type","image/jpeg") if isinstance(_pimg, dict) else "image/jpeg"
                            st.image(f"data:{_pmt};base64,{_pb64}", use_container_width=True)
                            if st.button(f"✓ Фото {_pi+1}", key=f"claid_pick_{_pi}", use_container_width=True):
                                st.session_state["_claid_picked_b64"] = _pb64
                                st.session_state["_claid_picked_mt"] = _pmt
                                st.session_state["_claid_picked_idx"] = _pi+1
                                st.rerun()
                        except: pass

                if st.session_state.get("_claid_picked_b64"):
                    _img_b64_claid = st.session_state["_claid_picked_b64"]
                    _img_mt_claid = st.session_state.get("_claid_picked_mt","image/jpeg")
                    st.success(f"✅ Выбрано фото #{st.session_state.get('_claid_picked_idx',1)}")

            # Or upload new
            with _claid_col2:
                st.markdown("**или загрузи своё:**")
                _claid_upload = st.file_uploader("📤 PNG/JPG", type=["png","jpg","jpeg"], key="claid_upload", label_visibility="collapsed")
                if _claid_upload:
                    import base64 as _b64c
                    _img_bytes = _claid_upload.read()
                    _img_b64_claid = _b64c.b64encode(_img_bytes).decode()
                    _img_mt_claid = "image/jpeg" if _claid_upload.name.lower().endswith(("jpg","jpeg")) else "image/png"
                    st.image(_img_bytes, width=120)

            # Показываем рекомендацию из Vision анализа для выбранного фото
            _picked_idx = st.session_state.get("_claid_picked_idx", 0)
            if _picked_idx and st.session_state.get("vision"):
                import re as _re_v
                _v_text = st.session_state.get("vision","")
                _blk_m = _re_v.search(rf"PHOTO_BLOCK_{_picked_idx}\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", _v_text, _re_v.DOTALL)
                if _blk_m:
                    _blk = _blk_m.group(1)
                    _act_m = _re_v.search(r"(?:Действие|Action)\s*[:\-]\s*(.{10,})", _blk)
                    _wk_m  = _re_v.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{10,})", _blk)
                    if _act_m or _wk_m:
                        st.markdown("**🤖 AI рекомендует исправить:**")
                        if _wk_m:  st.warning(f"⚠️ {_wk_m.group(1).strip()[:150]}")
                        if _act_m:
                            _ai_action = _act_m.group(1).strip()[:200]
                            st.info(f"→ {_ai_action}")
                            # Авто-подставляем рекомендацию как сцену
                            if st.checkbox("🎯 Использовать рекомендацию AI как сцену", value=True, key="use_ai_scene"):
                                _claid_scene = _ai_action

            if _img_b64_claid:
                if st.button("🎨 Генерировать улучшенное фото", type="primary", key="btn_claid_gen"):
                    with st.spinner(f"🎨 Gemini генерирует..."):
                        _urls, _err = claid_generate_lifestyle(_img_b64_claid, scene=_claid_scene, media_type=_img_mt_claid)
                        if _err:
                            st.error(f"❌ {_err}")
                        elif _urls:
                            st.session_state["_claid_results"] = _urls
                            st.success(f"✅ {len(_urls)} фото готово!")
                        else:
                            st.warning("Фото не получены — попробуй ещё раз")

            if st.session_state.get("_claid_results"):
                _cr = st.session_state["_claid_results"]
                st.markdown(f"**🖼️ Результат ({len(_cr)} фото):**")
                _rc = st.columns(min(len(_cr), 4))
                for _i, (_rcol, _rimg) in enumerate(zip(_rc, _cr)):
                    with _rcol:
                        if isinstance(_rimg, dict) and _rimg.get("b64"):
                            import base64 as _b64dl
                            _rb64=_rimg["b64"]; _rmt=_rimg.get("mt","image/jpeg")
                            st.image(f"data:{_rmt};base64,{_rb64}", use_container_width=True)
                            st.download_button(f"⬇️ #{_i+1}", _b64dl.b64decode(_rb64), f"lifestyle_{_i+1}.jpg", "image/jpeg", key=f"dl_img_{_i}", use_container_width=True)
                        elif isinstance(_rimg, str):
                            st.image(_rimg, use_container_width=True)

                if st.button("🗑️ Очистить", key="claid_clear"):
                    st.session_state.pop("_claid_results", None)
                    st.session_state.pop("_claid_picked_b64", None)
                    st.rerun()

    our_title   = od.get("title","")
    our_bullets = od.get("feature_bullets",[])
    our_desc    = od.get("description","")
    _sw_results = check_listing_stop_words(od)
    if _sw_results:
        _total_banned = sum(len(v.get("do_not_use",[])) for v in _sw_results.values())
        _total_warn   = sum(len(v.get("try_to_avoid",[])) for v in _sw_results.values())
        _sw_label = f"🚨 {_total_banned} запрещённых!" if _total_banned else f"⚠️ {_total_warn} нежелательных"
        with st.expander(f"🔴 Amazon Stop Words — {_sw_label}", expanded=_total_banned > 0):
            for field, found in _sw_results.items():
                st.markdown(f"**{field}:**")
                for w in found.get("do_not_use",[]): st.markdown(f'<span style="background:#ef444433;border:1px solid #ef4444;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.85rem;color:#ef4444">🚫 {w}</span>', unsafe_allow_html=True)
                for w in found.get("try_to_avoid",[]): st.markdown(f'<span style="background:#f59e0b33;border:1px solid #f59e0b;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.85rem;color:#f59e0b">⚠️ {w}</span>', unsafe_allow_html=True)
                for w in found.get("a_plus_restricted",[]): st.markdown(f'<span style="background:#3b82f633;border:1px solid #3b82f6;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.85rem;color:#3b82f6">📋 A+ {w}</span>', unsafe_allow_html=True)
    else: st.success("✅ Стоп-слова Amazon не найдены")
    st.divider()

    def _sec(label, key, **kw):
        val = pct(r.get(key, 0)); gaps = r.get(key.replace("_score","_gaps"), []); rec = r.get(key.replace("_score","_rec"), "")
        sc2 = sc_pct(val); c1,c2 = st.columns([4,1])
        c1.markdown(f"**{label}**"); c2.markdown(f"{sc2} **{val}%**")
        st.progress(val/100)
        if kw.get("raw_text"):
            cl = kw.get("char_limit",0); ct = len(kw["raw_text"])
            st.markdown(f"<small style='color:{'red' if (cl and ct>cl) else 'gray'}'>📝 {ct} симв{f' / {cl} лимит' if cl else ''}</small>", unsafe_allow_html=True)
            with st.expander("Показать текст"): st.markdown(f"> {kw['raw_text']}")
        _real_gaps = [g for g in gaps if g and str(g).strip()] if isinstance(gaps,list) else []
        if _real_gaps:
            with st.expander(f"⚠️ ({len(_real_gaps)})"):
                for g in _real_gaps: st.markdown(f"- {g}")
        if rec: st.info(f"💡 {rec}")

    # ── Keyword Ideas (Autocomplete) ─────────────────────────────────────────
    _ki_col1, _ki_col2 = st.columns([3,1])
    with _ki_col1:
        _ki_seed = st.text_input("💡 Keyword Ideas — введи seed:", 
            placeholder="merino wool, base layer, outdoor...",
            key="ki_seed_input", label_visibility="collapsed")
    with _ki_col2:
        _ki_run = st.button("🔍 Идеи", key="btn_ki", use_container_width=True)
    if _ki_run and _ki_seed.strip():
        _ki_mp = st.session_state.get("_marketplace","com")
        _ki_results = fetch_autocomplete(_ki_seed.strip(), domain=_ki_mp)
        if _ki_results:
            st.session_state["_ki_results"] = _ki_results
            st.session_state["_ki_seed"] = _ki_seed.strip()
    if st.session_state.get("_ki_results"):
        st.markdown(f'<div style="font-size:0.8rem;font-weight:700;color:#3b82f6;margin-bottom:6px">💡 Amazon подсказки для "{st.session_state.get("_ki_seed","")}":</div>', unsafe_allow_html=True)
        _ki_grid = ""
        for _kw in st.session_state["_ki_results"][:10]:
            _in_title = _kw.lower() in (our_title or "").lower()
            _in_bullets = any(_kw.lower() in b.lower() for b in (our_bullets or []))
            _status = "✅" if (_in_title or _in_bullets) else "❌"
            _bg = "#1a3a1a" if (_in_title or _in_bullets) else "#1e293b"
            _ki_grid += f'<div style="background:{_bg};border-radius:6px;padding:5px 10px;display:inline-block;margin:2px;font-size:0.8rem;color:#e2e8f0">{_status} {_kw}</div>'
        st.markdown(f'<div style="line-height:2">{_ki_grid}</div>', unsafe_allow_html=True)
        st.caption("✅ = уже есть в Title/Bullets | ❌ = отсутствует — добавить!")
    st.divider()

    _sec("Title", "title_score", raw_text=our_title, char_limit=125)
    st.divider()
    _sec("Bullets", "bullets_score", raw_text="\n".join([f"• {b}" for b in our_bullets]) if our_bullets else "")
    st.divider()
    _desc_score = pct(r.get("description_score", 0))
    _has_aplus  = bool(od.get("aplus") or od.get("aplus_content"))
    if _desc_score == 0 and _has_aplus:
        st.markdown("**Description**")
        st.markdown('<div style="background:#1e3a1e;border-left:4px solid #22c55e;border-radius:8px;padding:10px 14px;margin:4px 0"><span style="color:#22c55e;font-weight:700">✅ Скрыто A+ контентом — это нормально</span><br><span style="color:#94a3b8;font-size:0.82rem">Amazon показывает A+ вместо описания. Описание не видит покупатель, но индексируется поиском — заполни для SEO.</span></div>', unsafe_allow_html=True)
    else: _sec("Description", "description_score", raw_text=str(our_desc)[:400] if our_desc else "")
    st.divider(); _sec("A+", "aplus_score"); st.divider(); _sec("Фото", "images_score")
    ib = r.get("images_breakdown", {})
    if ib:
        st.subheader("📸 Детализация фото")
        for k,v2 in ib.items(): st.markdown(f"**{k}:** {v2}")
    if r.get("tech_params"):
        st.divider(); st.subheader("⚙️ Технические параметры")
        for p2 in r["tech_params"]:
            with st.container(border=True):
                st.markdown(f"**{p2.get('param','')}**"); x1,x2 = st.columns(2)
                x1.caption(f"🏆 Конкуренты: {p2.get('competitor_value','')}"); x2.caption(f"→ Наш пробел: {p2.get('our_gap','')}")

# ══ Benchmark ════════════════════════════════════════════════════════════════
elif page == "🏆 Benchmark":
    st.title("🏆 Benchmark")
    if not cd: st.info("Добавь конкурентов в форму выше"); st.stop()

    def auto_score(d):
        pi2=d.get("product_information",{}); title2=d.get("title",""); imgs2=d.get("images",[]); bul2=d.get("feature_bullets",[])
        desc2=d.get("description",""); rating2=safe_float_rating(d.get("average_rating",0))
        rev_cnt=int(str(pi2.get("Customer Reviews",{}).get("ratings_count","0") or 0).replace(",","").strip() or 0)
        has_vid=int(d.get("number_of_videos",0) or 0)>0; has_ap=bool(d.get("aplus"))
        is_prime=bool(d.get("is_prime_exclusive") or d.get("is_prime"))
        bsr_num=99999; bsr_m=re.search(r"#([\d,]+)",str(pi2.get("Best Sellers Rank","")))
        if bsr_m:
            try: bsr_num=int(bsr_m.group(1).replace(",",""))
            except: pass
        colors2=len([c for c in d.get("customization_options",{}).get("color",[]) if c.get("asin") and c.get("asin")!="undefined"])
        sizes2=len(d.get("customization_options",{}).get("size",[]))
        if "one size" in str(pi2.get("Size","")).lower(): sizes2=3
        ts=min(10,max(0,(1.5 if len(title2)<=125 else 0)+(3.5 if any(k in title2.lower() for k in ["merino","wool","shirt","base layer","tank"]) else 1.5)+3+(1 if not re.search(r"[!$?{}]",title2) else 0)+1))
        bs=min(10,max(0,(1.5 if len(bul2)<=5 else 0)+(2.5 if any(":"in b for b in bul2) else 1)+min(4,len(bul2))+1+1))
        ds=0 if not desc2 else min(10,4+(3 if len(desc2)>200 else 1))
        ps=min(10,max(0,(4 if len(imgs2)>=6 else len(imgs2)*0.6)+(2 if has_vid else 0)+(4 if len(imgs2)>=6 else 0)))
        as_=0 if not has_ap else 7; rs=10 if (rating2>=4.4 and rev_cnt>=50) else (7 if rating2>=4.0 else 4)
        bsrs=10 if bsr_num<=1000 else (8 if bsr_num<=5000 else 5); prs=10 if is_prime else 5
        vs=10 if (colors2>=5 and sizes2>=3) else (8 if colors2>=5 else (7 if sizes2>=3 else 4))
        h=int((ts*0.10+bs*0.10+ds*0.10+ps*0.10+as_*0.10+rs*0.15+bsrs*0.15+7*0.10+vs*0.05+prs*0.05)*10)
        return {"title":round(ts,1),"bullets":round(bs,1),"description":round(ds,1),"photos":round(ps,1),"aplus":as_,"reviews":rs,"bsr":bsrs,"variants":vs,"prime":prs,"health":h}

    our_scores={"title":pct(r.get("title_score",0)),"bullets":pct(r.get("bullets_score",0)),"description":pct(r.get("description_score",0)),"photos":pct(r.get("images_score",0)),"aplus":pct(r.get("aplus_score",0)),"reviews":pct(r.get("reviews_score",0)),"bsr":pct(r.get("bsr_score",0)),"variants":pct(r.get("customization_score",0)),"prime":pct(r.get("prime_score",0)),"health":pct(r.get("overall_score",0))}
    def get_comp_scores(c,i):
        cai=st.session_state.get(f"comp_ai_{i}")
        if cai: return {"title":pct(cai.get("title_score",0)),"bullets":pct(cai.get("bullets_score",0)),"description":pct(cai.get("description_score",0)),"photos":pct(cai.get("images_score",0)),"aplus":pct(cai.get("aplus_score",0)),"reviews":pct(cai.get("reviews_score",0)),"bsr":pct(cai.get("bsr_score",0)),"variants":pct(cai.get("customization_score",0)),"prime":pct(cai.get("prime_score",0)),"health":pct(cai.get("overall_score",0))}
        return auto_score(c)
    comp_scores=[get_comp_scores(c,i) for i,c in enumerate(cd)]
    all_scores=[our_scores]+comp_scores
    asin_labels=["🔵 НАШ"]+[f"🔴 {get_asin_from_data(c) or f'Конк.{i+1}'}" for i,c in enumerate(cd)]
    total_scores=[s["health"] if s.get("health",0)>0 else round(sum(s.get(k,0)*wi for k,wi in zip(["title","bullets","description","photos","aplus","reviews","bsr","variants","prime"],[0.10,0.10,0.10,0.10,0.10,0.15,0.15,0.05,0.05]))) for s in all_scores]
    ranked=sorted(enumerate(zip(asin_labels,total_scores)),key=lambda x:x[1][1],reverse=True)
    medals=["🥇","🥈","🥉","4️⃣","5️⃣"]

    st.subheader("🏅 Итоговый рейтинг")
    pcols=st.columns(len(ranked))
    for rank,(orig_idx,(lbl,score)) in enumerate(ranked):
        medal=medals[rank] if rank<len(medals) else ""
        bg="#fef9c3" if rank==0 else ("#f8fafc" if rank==1 else "#fff7ed")
        border="#f59e0b" if rank==0 else ("#94a3b8" if rank==1 else "#fb923c")
        pcols[rank].markdown(f'<div style="background:{bg};border:2px solid {border};border-radius:12px;padding:14px;text-align:center"><div style="font-size:1.8rem">{medal}</div><div style="font-size:0.82rem;font-weight:700;margin-top:4px">{lbl}</div><div style="font-size:1.6rem;font-weight:800;color:{border};margin-top:4px">{score}%</div><div style="font-size:0.65rem;color:#64748b">{"Лучший" if rank==0 else f"#{rank+1} место"}</div></div>', unsafe_allow_html=True)

    st.divider(); st.subheader("📊 Сравнение оценок")
    score_rows=[("🏷️ Title","title"),("📋 Bullets","bullets"),("📄 Описание","description"),("📸 Фото","photos"),("✨ A+","aplus"),("⭐ Отзывы","reviews"),("📊 BSR","bsr"),("🎨 Варианты","variants"),("🚀 Prime","prime"),("💯 Overall","health")]
    hdr2=st.columns([2]+[3]*(1+len(cd)))
    hdr2[0].markdown("**Метрика**")
    for j,al in enumerate(asin_labels): hdr2[j+1].markdown(f"**{al}**")
    for lbl,key in score_rows:
        vals=[s.get(key,0) for s in all_scores]; best_val=max(vals)
        row2=st.columns([2]+[3]*(1+len(cd))); row2[0].caption(lbl)
        for j,val in enumerate(vals):
            p3=int(val); is_best=(val==best_val); cc="#22c55e" if is_best else ("#f59e0b" if p3>=50 else "#ef4444")
            row2[j+1].markdown(f'<div style="background:#e5e7eb;border-radius:5px;height:22px;position:relative"><div style="background:{cc};width:{p3}%;height:22px;border-radius:5px"></div><div style="position:absolute;top:2px;left:6px;font-size:0.75rem;font-weight:700;color:white">{p3}%{"★" if is_best else ""}</div></div>', unsafe_allow_html=True)

# ══ COSMO / Rufus ════════════════════════════════════════════════════════════
elif page == "🧠 COSMO / Rufus":
    st.title("🧠 COSMO / Rufus Анализ")
    with st.expander("ℹ️ Что такое COSMO и Rufus", expanded=False):
        st.markdown("""
**COSMO** — алгоритм Amazon который решает что показывать покупателям. Если сигнал ❌ отсутствует → Amazon не понимает твой товар → не показывает нужной аудитории.

**Rufus** — AI-ассистент Amazon. Покупатель спрашивает Rufus → Rufus читает твой листинг → отвечает. Низкий score = Rufus не найдёт ответ = не порекомендует товар.

**Что делать:**
1. Смотри ❌ Отсутствующие сигналы COSMO → добавь эти слова в листинг
2. Используй **Rufus Симулятор** → задай 3-5 вопросов покупателя → нажми "Сохранить Q&A"
3. Нажми **🧠 Plan of Action** → получи конкретный план что добавить
""")

    if not od or not od.get("title"):
        st.info("ℹ️ Эта страница доступна только при анализе **нашего листинга**. Добавь URL в поле 🔵 НАШ листинг и перезапусти.")
        st.stop()

    # ── Audience Score ────────────────────────────────────────────────────────
    with st.expander("👥 AI Audience Score — оценка фото для вашей ЦА", expanded=False):
        st.caption("AI оценивает каждое фото с точки зрения вашей целевой аудитории")
        _aud_col1, _aud_col2 = st.columns(2)
        with _aud_col1:
            _aud_age = st.text_input("👤 Возраст и пол", placeholder="Мужчина 30-45 лет", key="aud_age")
            _aud_lifestyle = st.text_input("🏃 Образ жизни", placeholder="Активный, outdoor, hiking, путешествия", key="aud_lifestyle")
        with _aud_col2:
            _aud_income = st.selectbox("💰 Доход", ["Средний", "Выше среднего", "Высокий", "Любой"], key="aud_income")
            _aud_geo = st.text_input("🌍 География", placeholder="Германия, EU", key="aud_geo")
        _aud_extra = st.text_area("💬 Доп. инфо о ЦА", placeholder="Ценит качество, экологичность, покупает онлайн, читает отзывы...", height=68, key="aud_extra")

        _imgs_for_aud = st.session_state.get("images", [])
        if not _imgs_for_aud:
            st.info("Запусти анализ чтобы загрузить фото листинга")
        elif st.button("👥 Оценить все фото для ЦА", type="primary", key="btn_audience_score"):
            if not (_aud_age or _aud_lifestyle):
                st.warning("Заполни хотя бы возраст/пол и образ жизни")
            else:
                _aud_profile = f"""
ЦЕЛЕВАЯ АУДИТОРИЯ:
- Возраст/пол: {_aud_age}
- Образ жизни: {_aud_lifestyle}
- Доход: {_aud_income}
- География: {_aud_geo}
- Дополнительно: {_aud_extra}
""".strip()
                _aud_results = []
                _vprog = st.progress(0, "👥 Оцениваю фото для ЦА...")
                for _ai_idx, _aimg in enumerate(_imgs_for_aud[:6]):
                    _vprog.progress(int((_ai_idx+1)/min(len(_imgs_for_aud),6)*100), f"Фото {_ai_idx+1}...")
                    try:
                        _ab64 = _aimg.get("b64","") if isinstance(_aimg, dict) else _aimg
                        _amt = _aimg.get("media_type","image/jpeg") if isinstance(_aimg, dict) else "image/jpeg"
                        # Photo #1 = main Amazon image (white bg required)
                        _is_main_photo = (_ai_idx == 0)
                        _photo_context = """ВАЖНО: Это ГЛАВНОЕ фото листинга (#1).
По правилам Amazon главное фото ОБЯЗАНО быть на чистом белом фоне (RGB 255,255,255).
НЕ рекомендуй менять белый фон на lifestyle — это нарушит правила Amazon и приведёт к suppression.
Оценивай только: позу модели, читаемость товара, посадку, выражение лица, динамику движения, 
соответствие модели ЦА. Рекомендуй улучшения в рамках белого фона (поза, движение, угол съёмки).""" if _is_main_photo else """Это дополнительное фото #{} листинга. Оценивай свободно — фон, контекст, lifestyle.""".format(_ai_idx+1)

                        _aprompt = f"""Ты маркетолог-эксперт по Amazon. Оцени это фото товара с точки зрения целевой аудитории.

{_aud_profile}

ПРОДУКТ: {od.get('title','')}

{_photo_context}

Оцени по шкале 0-100% насколько это фото убедительно для данной аудитории.

Ответь строго в формате:
SCORE: [0-100]%
ЭМОЦИЯ_ЦА: [что чувствует покупатель глядя на это фото]
СООТВЕТСТВУЕТ: [что на фото совпадает с интересами ЦА]
НЕ СООТВЕТСТВУЕТ: [что не совпадает или отталкивает ЦА]
РЕКОМЕНДАЦИЯ: [одно конкретное улучшение — поза/движение/угол для #1, сцена/контекст для остальных]"""
                        _aud_resp = anthropic_vision(
                            content_blocks=[
                                {"type":"image","source":{"type":"base64","media_type":_amt,"data":_ab64}},
                                {"type":"text","text":_aprompt}
                            ],
                            max_tokens=500,
                            system="Ты маркетолог-эксперт по Amazon. Отвечай строго по формату."
                        )
                        _aud_text = _aud_resp.get("content",[{}])[0].get("text","") if isinstance(_aud_resp, dict) else str(_aud_resp)
                        _aud_results.append({"idx": _ai_idx+1, "text": _aud_text, "b64": _ab64, "mt": _amt})
                    except Exception as _ae:
                        _aud_results.append({"idx": _ai_idx+1, "text": f"Ошибка: {_ae}", "b64": "", "mt": ""})
                _vprog.progress(100, "✅ Готово!")
                st.session_state["_aud_results"] = _aud_results

        # Show results
        if st.session_state.get("_aud_results"):
            st.markdown("---")
            for _ar in st.session_state["_aud_results"]:
                _arc1, _arc2 = st.columns([1, 3])
                with _arc1:
                    if _ar.get("b64"):
                        st.image(f"data:{_ar['mt']};base64,{_ar['b64']}", use_container_width=True)
                    st.caption(f"Фото #{_ar['idx']}")
                with _arc2:
                    _txt = _ar["text"]
                    # Extract score for color
                    import re as _re2
                    _sm = _re2.search(r'SCORE:\s*(\d+)%', _txt)
                    _sc = int(_sm.group(1)) if _sm else 0
                    _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else "#ef4444")
                    _sc_l = "✅ Отлично для ЦА" if _sc>=75 else ("🟡 Средне" if _sc>=50 else "🔴 Слабо для ЦА")
                    st.markdown(
                        f'<div style="background:#0f172a;border-left:4px solid {_sc_c};border-radius:8px;padding:12px 14px">' +
                        f'<div style="font-size:1.4rem;font-weight:800;color:{_sc_c}">{_sc}% <span style="font-size:0.8rem">{_sc_l}</span></div>' +
                        f'<div style="font-size:0.82rem;color:#e2e8f0;margin-top:8px;white-space:pre-line">{_txt.replace("SCORE:","").replace(f"{_sc}%","",1).strip()}</div>' +
                        f'</div>',
                        unsafe_allow_html=True)
            # ── Summary & Ranking ────────────────────────────────────────────
            import re as _re3
            _aud_scored = []
            for _ar2 in st.session_state["_aud_results"]:
                _sm2 = _re3.search(r'SCORE:\s*(\d+)%', _ar2["text"])
                _sc2 = int(_sm2.group(1)) if _sm2 else 0
                _aud_scored.append((_sc2, _ar2["idx"], _ar2["text"]))
            _aud_scored_sorted = sorted(_aud_scored, reverse=True)

            st.markdown("---")
            st.markdown("### 🏆 Итоговый рейтинг фото для вашей ЦА")

            # Detect which photo has white background (likely photo #1 from listing)
            # Main image (#1) MUST be white bg per Amazon rules — pin it to position #1
            _main_photo_idx = 1  # always keep original #1 as main (white bg rule)
            _rest_sorted = [(sc,idx,txt) for sc,idx,txt in _aud_scored_sorted if idx != _main_photo_idx]
            _best_sc, _best_idx, _best_txt = _aud_scored_sorted[0]

            st.markdown(
                '<div style="background:#1a1a0a;border:2px solid #f59e0b;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                '<div style="font-size:0.75rem;font-weight:700;color:#f59e0b">⚠️ ПРАВИЛО AMAZON</div>' +
                '<div style="font-size:0.82rem;color:#fde68a;margin-top:4px">Главное фото (#1) <b>всегда белый фон</b> — это обязательное требование Amazon. Оно не участвует в ранжировании по ЦА.</div>' +
                '</div>', unsafe_allow_html=True)

            # Best lifestyle photo
            if _rest_sorted:
                _bsc, _bidx, _btxt = _rest_sorted[0]
                _brm = _re3.search(r'РЕКОМЕНДАЦИЯ:(.+?)(?=\n[А-Я]|$)', _btxt, _re3.DOTALL)
                _brec = _brm.group(1).strip()[:200] if _brm else ""
                st.markdown(
                    f'<div style="background:#0f3a1a;border:2px solid #22c55e;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                    f'<div style="font-size:0.9rem;font-weight:800;color:#22c55e">🥇 Лучшее lifestyle фото для ЦА — Фото #{_bidx} ({_bsc}%)</div>' +
                    (f'<div style="font-size:0.78rem;color:#86efac;margin-top:4px">{_brec}</div>' if _brec else "") +
                    f'</div>', unsafe_allow_html=True)

            # Ordered ranking table
            _rank_html = '<div style="background:#0f172a;border-radius:10px;padding:12px 16px">' + '<div style="font-size:0.75rem;font-weight:700;color:#64748b;margin-bottom:8px">РЕКОМЕНДУЕМЫЙ ПОРЯДОК ФОТО</div>'
            # Position 1 = always original main photo (white bg)
            _rank_html += (
                '<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                '<span style="font-size:0.8rem;color:#94a3b8">📸 Главное (#1) — Amazon rule</span>' +
                f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #1</span>' +
                '<span style="font-size:0.75rem;color:#f59e0b">🤍 Белый фон</span>' +
                '</div>'
            )
            _position_labels2 = ["🥇 Второе (#2) — лучшее lifestyle", "🥈 Третье (#3)", "🥉 Четвёртое (#4)", "4️⃣ Пятое (#5)", "5️⃣ Шестое (#6)"]
            for _pi, (_psc, _pidx, _ptxt) in enumerate(_rest_sorted):
                _pc = "#22c55e" if _psc>=75 else ("#f59e0b" if _psc>=50 else "#ef4444")
                _plbl = _position_labels2[_pi] if _pi < len(_position_labels2) else f"#{_pi+2}"
                _rank_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                    f'<span style="font-size:0.8rem;color:#94a3b8">{_plbl}</span>' +
                    f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #{_pidx}</span>' +
                    f'<span style="font-size:0.85rem;font-weight:700;color:{_pc}">{_psc}%</span>' +
                    f'</div>'
                )
            _rank_html += '</div>'
            st.markdown(_rank_html, unsafe_allow_html=True)

            # AI summary prompt
            if st.button("🧠 AI — сводный план улучшений", key="btn_aud_summary"):
                with st.spinner("🧠 Генерирую план..."):
                    _all_reviews = "\n\n".join([
                        f"ФОТО #{_ar2['idx']} ({_ar2['text'][:300]})"
                        for _ar2 in st.session_state["_aud_results"]
                    ])
                    _sum_prompt = f"""На основе оценки {len(st.session_state["_aud_results"])} фото листинга для ЦА ({_aud_age}, {_aud_lifestyle}, {_aud_income}):

{_all_reviews}

Дай КРАТКИЙ план из 3-5 конкретных действий чтобы улучшить весь набор фото для этой ЦА.
Формат: пронумерованный список, каждый пункт = одно действие + ожидаемый эффект (+X% конверсии).
Язык: {'русский' if st.session_state.get('analysis_lang','ru')=='ru' else 'английский'}."""
                    _sum_r = ai_call("Amazon photo strategist", _sum_prompt, max_tokens=600)
                    st.session_state["_aud_summary"] = _sum_r

            if st.session_state.get("_aud_summary"):
                st.markdown(
                    '<div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-top:8px">' +
                    '<div style="font-size:0.72rem;font-weight:700;color:#64748b;margin-bottom:8px">📋 ПЛАН УЛУЧШЕНИЙ ФОТО ДЛЯ ЦА</div>' +
                    f'<div style="font-size:0.85rem;color:#e2e8f0;line-height:1.7">{st.session_state["_aud_summary"].replace(chr(10),"<br>")}</div></div>',
                    unsafe_allow_html=True)

            if st.button("🗑️ Очистить", key="clear_aud"):
                st.session_state.pop("_aud_results", None)
                st.session_state.pop("_aud_summary", None)
                st.rerun()

    # ── Claid AI Photo Generator ──────────────────────────────────────────────
    _claid_key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("CLAID_API_KEY","")
    if _claid_key:  # GEMINI_API_KEY
        with st.expander("🎨 AI-генерация lifestyle фото — Gemini Nano Banana Pro", expanded=False):
            st.caption("Выбери любое фото из анализа или загрузи своё → AI создаст lifestyle сцену")
            _claid_col1, _claid_col2 = st.columns([3,2])
            with _claid_col1:
                _scene_options = {
                "🤍 Белый фон (Amazon Main Image)": "pure white background, professional studio product photography, soft shadow, white backdrop, e-commerce",
                "🏔️ Hiking / Outdoor горы": "on a rugged mountain trail, scattered rocks, sunlight through pine trees, rich earthy colors, warm light, professional product photography",
                "🏠 Уютный дом / Interior": "cozy modern home interior, warm lighting, wooden furniture, lifestyle product photography",
                "🏋️ Gym / Фитнес": "modern gym interior, fitness lifestyle, dynamic lighting, professional product photography",
                "❄️ Зима / Snow": "snowy mountain landscape, cold winter day, soft blue light, outdoor winter lifestyle",
                "🌊 Лето / Summer beach": "sunny summer day, outdoor adventure, bright natural light, lifestyle product photography",
                "🌿 Nature / Природа": "lush green forest path, dappled sunlight, natural outdoor setting, professional photography",
                "💼 Офис / Professional": "modern office setting, professional lifestyle, clean minimalist background",
            }
            _scene_label = st.selectbox("Сцена", list(_scene_options.keys()), key="claid_scene_sel")
            _claid_scene = _scene_options[_scene_label]
            if "белый" in _scene_label.lower() or "Amazon" in _scene_label:
                st.caption("✅ Подходит для главного фото Amazon (#1) — чистый белый фон")
            else:
                st.caption("📸 Lifestyle сцена — для фото #2-7 в листинге")

            # Photos from analysis
            _analysis_imgs = st.session_state.get("images", [])
            _img_b64_claid = None
            _img_mt_claid = "image/jpeg"

            if _analysis_imgs:
                st.markdown("**📸 Выбери фото из анализа:**")
                _img_cols = st.columns(min(len(_analysis_imgs), 5))
                for _pi, (_pc, _pimg) in enumerate(zip(_img_cols, _analysis_imgs[:5])):
                    with _pc:
                        try:
                            _pb64 = _pimg.get("b64","") if isinstance(_pimg, dict) else _pimg
                            _pmt = _pimg.get("media_type","image/jpeg") if isinstance(_pimg, dict) else "image/jpeg"
                            st.image(f"data:{_pmt};base64,{_pb64}", use_container_width=True)
                            if st.button(f"✓ Фото {_pi+1}", key=f"claid_pick_{_pi}", use_container_width=True):
                                st.session_state["_claid_picked_b64"] = _pb64
                                st.session_state["_claid_picked_mt"] = _pmt
                                st.session_state["_claid_picked_idx"] = _pi+1
                                st.rerun()
                        except: pass

                if st.session_state.get("_claid_picked_b64"):
                    _img_b64_claid = st.session_state["_claid_picked_b64"]
                    _img_mt_claid = st.session_state.get("_claid_picked_mt","image/jpeg")
                    st.success(f"✅ Выбрано фото #{st.session_state.get('_claid_picked_idx',1)}")

            # Or upload new
            with _claid_col2:
                st.markdown("**или загрузи своё:**")
                _claid_upload = st.file_uploader("📤 PNG/JPG", type=["png","jpg","jpeg"], key="claid_upload", label_visibility="collapsed")
                if _claid_upload:
                    import base64 as _b64c
                    _img_bytes = _claid_upload.read()
                    _img_b64_claid = _b64c.b64encode(_img_bytes).decode()
                    _img_mt_claid = "image/jpeg" if _claid_upload.name.lower().endswith(("jpg","jpeg")) else "image/png"
                    st.image(_img_bytes, width=120)

            # Показываем рекомендацию из Vision анализа для выбранного фото
            _picked_idx = st.session_state.get("_claid_picked_idx", 0)
            if _picked_idx and st.session_state.get("vision"):
                import re as _re_v
                _v_text = st.session_state.get("vision","")
                _blk_m = _re_v.search(rf"PHOTO_BLOCK_{_picked_idx}\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", _v_text, _re_v.DOTALL)
                if _blk_m:
                    _blk = _blk_m.group(1)
                    _act_m = _re_v.search(r"(?:Действие|Action)\s*[:\-]\s*(.{10,})", _blk)
                    _wk_m  = _re_v.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{10,})", _blk)
                    if _act_m or _wk_m:
                        st.markdown("**🤖 AI рекомендует исправить:**")
                        if _wk_m:  st.warning(f"⚠️ {_wk_m.group(1).strip()[:150]}")
                        if _act_m:
                            _ai_action = _act_m.group(1).strip()[:200]
                            st.info(f"→ {_ai_action}")
                            # Авто-подставляем рекомендацию как сцену
                            if st.checkbox("🎯 Использовать рекомендацию AI как сцену", value=True, key="use_ai_scene"):
                                _claid_scene = _ai_action

            if _img_b64_claid:
                if st.button("🎨 Генерировать улучшенное фото", type="primary", key="btn_claid_gen"):
                    with st.spinner(f"🎨 Gemini генерирует..."):
                        _urls, _err = claid_generate_lifestyle(_img_b64_claid, scene=_claid_scene, media_type=_img_mt_claid)
                        if _err:
                            st.error(f"❌ {_err}")
                        elif _urls:
                            st.session_state["_claid_results"] = _urls
                            st.success(f"✅ {len(_urls)} фото готово!")
                        else:
                            st.warning("Фото не получены — попробуй ещё раз")

            if st.session_state.get("_claid_results"):
                _cr = st.session_state["_claid_results"]
                st.markdown(f"**🖼️ Результат ({len(_cr)} фото):**")
                _rc = st.columns(min(len(_cr), 4))
                for _i, (_rcol, _rimg) in enumerate(zip(_rc, _cr)):
                    with _rcol:
                        if isinstance(_rimg, dict) and _rimg.get("b64"):
                            import base64 as _b64dl
                            _rb64=_rimg["b64"]; _rmt=_rimg.get("mt","image/jpeg")
                            st.image(f"data:{_rmt};base64,{_rb64}", use_container_width=True)
                            st.download_button(f"⬇️ #{_i+1}", _b64dl.b64decode(_rb64), f"lifestyle_{_i+1}.jpg", "image/jpeg", key=f"dl_img_{_i}", use_container_width=True)
                        elif isinstance(_rimg, str):
                            st.image(_rimg, use_container_width=True)

                if st.button("🗑️ Очистить", key="claid_clear"):
                    st.session_state.pop("_claid_results", None)
                    st.session_state.pop("_claid_picked_b64", None)
                    st.rerun()

    _ca=r.get("cosmo_analysis",{}); cosmo=pct(_ca.get("score",r.get("cosmo_score",0)))
    _ra=r.get("rufus_analysis",{}); rufus_s=pct(_ra.get("score",0))
    cc="#22c55e" if cosmo>=75 else ("#f59e0b" if cosmo>=50 else "#ef4444")
    rc2="#22c55e" if rufus_s>=75 else ("#f59e0b" if rufus_s>=50 else "#ef4444")
    ccc1,ccc2=st.columns(2)
    ccc1.markdown(f'<div style="text-align:center;padding:16px;background:#f8fafc;border-radius:12px"><div style="font-size:2.5rem;font-weight:800;color:{cc}">{cosmo}%</div><div style="color:{cc};font-weight:600">COSMO Score</div></div>', unsafe_allow_html=True)
    ccc2.markdown(f'<div style="text-align:center;padding:16px;background:#f8fafc;border-radius:12px"><div style="font-size:2.5rem;font-weight:800;color:{rc2}">{rufus_s}%</div><div style="color:{rc2};font-weight:600">Rufus Score</div></div>', unsafe_allow_html=True)
    st.divider()
    if _ca:
        c_present=_ca.get("signals_present",[]); c_missing=_ca.get("signals_missing",[])
        if c_present or c_missing:
            st.subheader("📡 COSMO сигналы"); col_p,col_m=st.columns(2)
            with col_p:
                st.markdown("**✅ Присутствуют**")
                for s2 in c_present: st.success(s2)
            with col_m:
                st.markdown("**❌ Отсутствуют**")
                for s2 in c_missing: st.error(s2)
    st.divider(); st.subheader("🤖 Rufus Issues")
    if _ra.get("issues"):
        for iss in _ra["issues"]: st.warning(f"⚠️ {iss}")
    _jtbd=r.get("jtbd_analysis",{})
    if _jtbd:
        st.divider(); st.subheader("🎯 JTBD — Jobs To Be Done")
        _jtbd_score=pct(_jtbd.get("alignment_score",0)); _jc="#22c55e" if _jtbd_score>=75 else ("#f59e0b" if _jtbd_score>=50 else "#ef4444")
        st.markdown(f'<div style="background:#1e293b;border-radius:12px;padding:16px;margin-bottom:12px"><div style="font-size:2rem;font-weight:800;color:{_jc}">{_jtbd_score}%</div></div>', unsafe_allow_html=True)
        if _jtbd.get("job_story"): st.info(f"**📖 Job Story:**\n\n_{_jtbd['job_story']}_")
        _j1,_j2,_j3=st.columns(3)
        if _jtbd.get("functional_job"): _j1.markdown(f"**⚙️ Функциональная**\n\n{_jtbd['functional_job']}")
        if _jtbd.get("emotional_job"):  _j2.markdown(f"**❤️ Эмоциональная**\n\n{_jtbd['emotional_job']}")
        if _jtbd.get("social_job"):     _j3.markdown(f"**👥 Социальная**\n\n{_jtbd['social_job']}")
        if _jtbd.get("jtbd_gaps"):
            st.subheader("❌ Gaps")
            for g in _jtbd["jtbd_gaps"]: st.error(f"✗ {g}")
        if _jtbd.get("jtbd_recs"):
            st.subheader("✅ Рекомендации")
            for rec in _jtbd["jtbd_recs"]: st.success(f"→ {rec}")
    # ══ AI READINESS SCORE ════════════════════════════════════════════════════
    st.divider()
    st.subheader("🤖 AI Readiness Score")
    st.caption("Насколько листинг готов к эпохе AI-рекомендаций (Rufus, Cosmo, агенты)")

    _jtbd_score = pct(r.get("jtbd_analysis",{}).get("alignment_score",0)) if r.get("jtbd_analysis") else 0
    _vpc_score  = pct(r.get("vpc_analysis",{}).get("fit_score",0)) if r.get("vpc_analysis") else 0
    _cosmo_s    = cosmo
    _rufus_s2   = rufus_s
    _title_s    = pct(r.get("title_score",0))
    _bullets_s  = pct(r.get("bullets_score",0))

    # AI Readiness = weighted combo of AI-relevant signals
    _ai_ready = int(
        _cosmo_s   * 0.30 +   # Cosmo понимает товар
        _rufus_s2  * 0.25 +   # Rufus может ответить на вопросы
        _jtbd_score* 0.20 +   # Листинг говорит языком покупателя
        _vpc_score * 0.15 +   # Ценность коммуницирована
        _title_s   * 0.05 +   # Title индексируется
        _bullets_s * 0.05     # Bullets индексируются
    )
    _ar_c = "#22c55e" if _ai_ready>=75 else ("#f59e0b" if _ai_ready>=50 else "#ef4444")
    _ar_label = (
        "🟢 Готов к AI-эпохе" if _ai_ready>=75 else
        "🟡 Частично готов — нужны правки" if _ai_ready>=50 else
        "🔴 Не готов — AI-агент не выберет этот товар"
    )
    _ar_desc = (
        "AI-агент Amazon сможет точно рекомендовать этот товар нужной аудитории" if _ai_ready>=75 else
        "Rufus найдёт товар, но не всегда выберет его первым — листинг не говорит языком покупателя" if _ai_ready>=50 else
        "Cosmo/Rufus не понимают товар достаточно — листинг будет проигрывать конкурентам в AI-рекомендациях"
    )

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#0f172a,#1e293b);border-radius:14px;padding:20px 24px;margin-bottom:16px">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px">
    <div>
      <div style="font-size:1.1rem;font-weight:700;color:{_ar_c}">{_ar_label}</div>
      <div style="font-size:0.85rem;color:#94a3b8;margin-top:4px;max-width:480px">{_ar_desc}</div>
    </div>
    <div style="text-align:center">
      <div style="font-size:3rem;font-weight:800;color:{_ar_c};line-height:1">{_ai_ready}%</div>
      <div style="font-size:0.75rem;color:#64748b">AI Readiness</div>
    </div>
  </div>
  <div style="background:rgba(255,255,255,0.08);border-radius:6px;height:8px;margin-top:14px">
    <div style="background:{_ar_c};width:{_ai_ready}%;height:8px;border-radius:6px"></div>
  </div>
</div>""", unsafe_allow_html=True)

    # Component breakdown
    _ai_comps = [
        ("🧠 COSMO", _cosmo_s, 30, "Алгоритм понимает товар"),
        ("🤖 Rufus", _rufus_s2, 25, "Отвечает на вопросы покупателей"),
        ("🎯 JTBD", _jtbd_score, 20, "Язык покупателя"),
        ("📦 VPC", _vpc_score, 15, "Ценность коммуницирована"),
        ("🏷️ Title", _title_s, 5, "SEO индексация"),
        ("📋 Bullets", _bullets_s, 5, "Структура контента"),
    ]
    _ac_cols = st.columns(len(_ai_comps))
    for _col, (_lbl, _val, _wt, _hint) in zip(_ac_cols, _ai_comps):
        _cc2 = "#22c55e" if _val>=75 else ("#f59e0b" if _val>=50 else "#ef4444")
        _col.markdown(
            f'<div style="background:#f1f5f9;border-radius:8px;padding:8px 4px;text-align:center;border-top:3px solid {_cc2}">'
            f'<div style="font-size:0.65rem;color:#64748b">{_lbl}</div>'
            f'<div style="font-size:1.1rem;font-weight:800;color:{_cc2}">{_val}%</div>'
            f'<div style="font-size:0.58rem;color:#94a3b8">вес {_wt}%</div>'
            f'</div>',
            unsafe_allow_html=True
        )
        _col.caption(_hint)

    # ══ RUFUS SIMULATOR ═══════════════════════════════════════════════════════
    st.divider()
    st.subheader("🤖 Rufus Симулятор")
    st.caption("Задай вопрос как покупатель — AI ответит как Amazon Rufus, используя данные твоего листинга")

    _rfcol1, _rfcol2 = st.columns([3,1])
    with _rfcol1:
        _rufus_q = st.text_input(
            "Вопрос покупателя",
            placeholder="Is this good for hiking in cold weather? / Подойдёт ли для горного туризма?",
            key="rufus_sim_input",
            label_visibility="collapsed"
        )
    # Show autocomplete for Rufus input
    if _rufus_q and len(_rufus_q) >= 3:
        _rmp = st.session_state.get("_marketplace","com")
        _rac = fetch_autocomplete(_rufus_q, domain=_rmp)
        if _rac:
            _rac_cols = st.columns(min(4, len(_rac)))
            for _ri, (_rc2, _rq) in enumerate(zip(_rac_cols, _rac[:4])):
                if _rc2.button(_rq[:25]+"…" if len(_rq)>25 else _rq, key=f"rufus_ac_{_ri}", use_container_width=True):
                    st.session_state["_rufus_quick_q"] = _rq
                    st.rerun()
    with _rfcol2:
        _run_rufus = st.button("▶ Спросить Rufus", key="btn_rufus_sim", type="primary", use_container_width=True, help="AI отвечает как Amazon Rufus используя данные твоего листинга → показывает ⚠️ Gap что не нашёл.")

    # Suggested questions
    _title_for_rufus = od.get("title","")
    _cat_hint = "base layer" if any(w in _title_for_rufus.lower() for w in ["merino","wool","thermal","base"]) else                 "jacket" if any(w in _title_for_rufus.lower() for w in ["jacket","coat","parka"]) else                 "outdoor apparel"
    _suggested = [
        f"What temperature is this {_cat_hint} good for?",
        f"Is this suitable for hiking?",
        f"How does this fit — true to size?",
        f"Can I wear this for everyday use?",
        f"How does this compare to competitors?",
    ]
    st.markdown('<div style="font-size:0.75rem;color:#94a3b8;margin:4px 0 8px">💡 Быстрые вопросы:</div>', unsafe_allow_html=True)
    _q_cols = st.columns(len(_suggested))
    for _qi, (_qc, _qs) in enumerate(zip(_q_cols, _suggested)):
        if _qc.button(_qs[:35]+"…" if len(_qs)>35 else _qs, key=f"rufus_q_{_qi}", use_container_width=True):
            st.session_state["_rufus_quick_q"] = _qs
            st.rerun()

    # Handle quick question selection
    if st.session_state.get("_rufus_quick_q"):
        _rufus_q = st.session_state.pop("_rufus_quick_q")
        _run_rufus = True

    if _run_rufus and _rufus_q:
        with st.spinner("🤖 Rufus анализирует листинг..."):
            _listing_ctx = f"""Title: {od.get("title","")}
Price: {od.get("price","")} | Rating: {od.get("average_rating","")} | Reviews: {od.get("product_information",{}).get("Customer Reviews",{}).get("ratings_count","")}
Material: {od.get("product_information",{}).get("Material Type","")}
Bullets:\n{chr(10).join(od.get("feature_bullets",[])[:5])}
Description: {str(od.get("description",""))[:500]}
A+: {str(od.get("aplus_content",""))[:500]}"""

            _rufus_prompt = f"""You are Amazon Rufus — Amazon's AI shopping assistant. A customer asked you about this product.

PRODUCT LISTING:
{_listing_ctx}

CUSTOMER QUESTION: {_rufus_q}

Answer EXACTLY as Amazon Rufus would:
1. Give a direct, helpful answer based ONLY on what is in the listing
2. If the listing doesn't contain the answer — say "Based on the listing, I couldn't find specific information about [X]" and suggest what to look for
3. Quote specific bullets or specs when relevant
4. Keep it conversational, 2-4 sentences max
5. End with: "⚠️ LISTING GAP:" — one sentence on what info is MISSING from listing that would help answer this better

Respond in {'Russian' if st.session_state.get("analysis_lang","ru")=="ru" else "English"}."""

            _rufus_answer = ai_call("You are Amazon Rufus AI shopping assistant.", _rufus_prompt, max_tokens=400)
            if "rufus_history" not in st.session_state:
                st.session_state["rufus_history"] = []
            st.session_state["rufus_history"].insert(0, {"q": _rufus_q, "a": _rufus_answer})

    # Show Rufus conversation history
    if st.session_state.get("rufus_history"):
        for _rh in st.session_state["rufus_history"][:5]:
            _ans = _rh["a"]
            _gap_part = ""
            _main_part = _ans
            if "⚠️ LISTING GAP:" in _ans:
                _parts = _ans.split("⚠️ LISTING GAP:")
                _main_part = _parts[0].strip()
                _gap_part = _parts[1].strip() if len(_parts)>1 else ""
            st.markdown(f"""
<div style="background:#0f172a;border-radius:10px;padding:14px 16px;margin-bottom:10px">
  <div style="font-size:0.75rem;color:#3b82f6;font-weight:700;margin-bottom:6px">❓ {_rh["q"]}</div>
  <div style="font-size:0.9rem;color:#e2e8f0;line-height:1.6">{_main_part}</div>
  {f'<div style="background:#7f1d1d22;border-left:3px solid #ef4444;border-radius:4px;padding:6px 10px;margin-top:8px;font-size:0.8rem;color:#fca5a5"><b>⚠️ Gap в листинге:</b> {_gap_part}</div>' if _gap_part else ""}
</div>""", unsafe_allow_html=True)
        if st.button("🗑️ Очистить историю", key="clear_rufus"):
            st.session_state.pop("rufus_history", None)
            st.rerun()

    # ══ RUFUS PLAN OF ACTION ══════════════════════════════════════════════════
    st.divider()
    st.subheader("📋 Rufus — Plan of Action")
    st.caption("Реальные вопросы покупателей с Amazon + симулятор → AI генерирует план улучшений")

    # ── Загрузить реальные Q&A из ScrapingDog ────────────────────────────────
    _real_qa = od.get("questions_and_answers", od.get("qa", od.get("customer_questions", [])))
    if _real_qa and isinstance(_real_qa, list):
        with st.expander(f"📥 Реальные вопросы покупателей с Amazon ({len(_real_qa)} шт.)", expanded=False):
            st.caption("Это реальные вопросы которые покупатели задавали на странице товара")
            for _rq in _real_qa[:10]:
                _q_text = _rq.get("question","") or _rq.get("q","") or str(_rq)
                _a_text = _rq.get("answer","") or _rq.get("a","") or ""
                if not _q_text: continue
                _has_answer = bool(_a_text)
                st.markdown(
                    f'<div style="background:#0f172a;border-left:3px solid {"#22c55e" if _has_answer else "#ef4444"};border-radius:6px;padding:8px 12px;margin-bottom:4px">' +
                    f'<div style="font-size:0.8rem;font-weight:700;color:#3b82f6">❓ {_q_text[:200]}</div>' +
                    (f'<div style="font-size:0.78rem;color:#94a3b8;margin-top:3px">💬 {_a_text[:200]}</div>' if _has_answer else
                     '<div style="font-size:0.72rem;color:#ef4444;margin-top:2px">⚠️ Нет ответа от продавца — Gap для Rufus</div>') +
                    '</div>',
                    unsafe_allow_html=True
                )
            if st.button("📥 Загрузить все в Plan of Action", key="load_real_qa"):
                if "rufus_qa_saved" not in st.session_state:
                    st.session_state["rufus_qa_saved"] = []
                _loaded = 0
                for _rq in _real_qa[:10]:
                    _q_text = _rq.get("question","") or _rq.get("q","") or ""
                    _a_text = _rq.get("answer","") or _rq.get("a","") or "⚠️ Gap: нет ответа от продавца в листинге"
                    if _q_text:
                        st.session_state["rufus_qa_saved"].append({"q": _q_text, "a": _a_text, "saved": True, "source": "amazon_real"})
                        _loaded += 1
                st.success(f"✅ Загружено {_loaded} реальных вопросов")
                st.rerun()
    elif od.get("title"):
        st.caption("ℹ️ ScrapingDog не вернул Q&A для этого листинга — используй симулятор выше")

    # Save Q&A button on each history item
    if st.session_state.get("rufus_history"):
        _unsaved = [h for h in st.session_state["rufus_history"] if not h.get("saved")]
        if _unsaved:
            if st.button("💾 Сохранить все Q&A в план", key="save_rufus_qa", type="secondary"):
                if "rufus_qa_saved" not in st.session_state:
                    st.session_state["rufus_qa_saved"] = []
                for _h in _unsaved:
                    _h["saved"] = True
                    st.session_state["rufus_qa_saved"].append(_h)
                st.success(f"✅ Сохранено {len(_unsaved)} Q&A")
                st.rerun()

    # Manual text input for Q&A
    st.markdown("**✏️ Добавить вопрос/ответ вручную:**")
    _qa_col1, _qa_col2 = st.columns([3,1])
    with _qa_col1:
        _manual_q = st.text_input("Вопрос покупателя", key="manual_rufus_q", placeholder="Is this suitable for hiking?", label_visibility="collapsed")
        _manual_a = st.text_area("Ответ / Gap", key="manual_rufus_a", placeholder="Rufus ответил: ... ⚠️ Gap: в листинге нет информации о...", height=80, label_visibility="collapsed")
    with _qa_col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("➕ Добавить", key="add_manual_qa", use_container_width=True):
            if _manual_q.strip():
                if "rufus_qa_saved" not in st.session_state:
                    st.session_state["rufus_qa_saved"] = []
                st.session_state["rufus_qa_saved"].append({
                    "q": _manual_q.strip(),
                    "a": _manual_a.strip(),
                    "saved": True
                })
                st.rerun()

    # Show saved Q&A
    _saved_qa = st.session_state.get("rufus_qa_saved", [])
    if _saved_qa:
        st.markdown(f"**📝 Сохранено Q&A: {len(_saved_qa)}**")
        for _qi, _qh in enumerate(_saved_qa):
            _gap_in_a = "⚠️" in _qh.get("a","") or "Gap" in _qh.get("a","")
            _q_color = "#ef4444" if _gap_in_a else "#3b82f6"
            _del_col, _text_col = st.columns([0.5, 9.5])
            with _del_col:
                if st.button("×", key=f"del_qa_{_qi}", help="Удалить"):
                    st.session_state["rufus_qa_saved"].pop(_qi)
                    st.rerun()
            with _text_col:
                st.markdown(
                    f'<div style="background:#0f172a;border-left:3px solid {_q_color};border-radius:6px;padding:8px 12px;margin-bottom:4px">' +
                    f'<div style="font-size:0.78rem;font-weight:700;color:{_q_color}">❓ {_qh["q"]}</div>' +
                    (f'<div style="font-size:0.8rem;color:#94a3b8;margin-top:4px">{_qh["a"][:200]}</div>' if _qh.get("a") else "") +
                    f'</div>',
                    unsafe_allow_html=True
                )

        st.divider()
        # Generate Plan of Action from Q&A
        if st.button("🧠 Сгенерировать Plan of Action", type="primary", key="btn_rufus_plan", help="AI читает все Q&A и генерирует конкретный план: что добавить в Title/Bullets/A+ для улучшения Rufus Score."):
            with st.spinner("🧠 AI генерирует план на основе Rufus Q&A..."):
                _qa_text = "\n".join([
                    f"Q: {_h['q']}\nA: {_h.get('a','')}"
                    for _h in _saved_qa
                ])
                _plan_prompt = f"""Ты Amazon листинг-оптимизатор. На основе Q&A из симулятора Rufus создай конкретный план улучшений листинга.

ТОВАР: {od.get("title","")}
ASIN: {od.get("parent_asin","")}

RUFUS Q&A (реальные вопросы покупателей и что Rufus нашёл/не нашёл в листинге):
{_qa_text}

Создай PLAN OF ACTION:

**Что добавить в Title** (если Rufus не находит ключевые характеристики)
**Что добавить в Bullet #1-5** (конкретно какой bullet и что написать)
**Что добавить в A+ / Description** (сценарии использования которых нет)
**Backend keywords** (что индексировать чтобы Rufus находил)
**Приоритет** каждого действия: HIGH / MEDIUM / LOW

Формат: конкретные действия, не общие советы. Каждое действие = одна строка начинающаяся с ✅ HIGH / 🟡 MEDIUM / ⚪ LOW

Ответь {'по-русски' if st.session_state.get('analysis_lang','ru')=='ru' else 'in English'}."""

                _plan = ai_call("Amazon listing optimizer. Actionable plan only.", _plan_prompt, max_tokens=1200)
                st.session_state["rufus_plan"] = _plan

        if st.session_state.get("rufus_plan"):
            st.markdown(
                '<div style="background:#0f172a;border:1px solid #334155;border-radius:12px;padding:18px 20px">' +
                '<div style="font-size:0.75rem;font-weight:700;color:#64748b;letter-spacing:0.08em;margin-bottom:12px">🎯 RUFUS PLAN OF ACTION</div>' +
                '<div style="color:#e2e8f0;font-size:0.9rem;line-height:1.8">' +
                st.session_state["rufus_plan"].replace("\n","<br>") +
                '</div></div>',
                unsafe_allow_html=True
            )
            if st.button("🗑️ Очистить план", key="clear_plan"):
                st.session_state.pop("rufus_plan", None)
                st.session_state.pop("rufus_qa_saved", None)
                st.rerun()
    else:
        st.info("💡 Задай вопросы в симуляторе выше → нажми 'Сохранить все Q&A' → получи план улучшений")

    with st.expander("🔧 Raw JSON"): st.json(r)

# ══ VPC / JTBD ════════════════════════════════════════════════════════════════
elif page == "🎯 VPC / JTBD":
    st.title("🎯 Value Proposition Canvas + JTBD")
    with st.expander("ℹ️ Что такое VPC и JTBD", expanded=False):
        st.markdown("""
**Главная идея:** Покупатель не покупает продукт — он **нанимает** его для работы.

**JTBD Alignment Score** — насколько листинг говорит языком покупателя:
- 90%+ = листинг обращается к ситуации покупателя
- <70% = листинг описывает продукт, не работу

**VPC Fit Score** — % болей и выгод покупателя которые листинг закрывает.

**Что делать:**
1. Читай **Job Story** — это и есть ключевое сообщение листинга
2. Смотри ❌ Pain Relievers Missing → это Gap в коммуникации
3. Перепиши Bullet #1 чтобы начинался со сценария из Job Story
""")

    if not od or not od.get("title"):
        st.info("ℹ️ Эта страница доступна только при анализе **нашего листинга**. Добавь URL в поле 🔵 НАШ листинг и перезапусти.")
        st.stop()

    # ── Audience Score ────────────────────────────────────────────────────────
    with st.expander("👥 AI Audience Score — оценка фото для вашей ЦА", expanded=False):
        st.caption("AI оценивает каждое фото с точки зрения вашей целевой аудитории")
        _aud_col1, _aud_col2 = st.columns(2)
        with _aud_col1:
            _aud_age = st.text_input("👤 Возраст и пол", placeholder="Мужчина 30-45 лет", key="aud_age")
            _aud_lifestyle = st.text_input("🏃 Образ жизни", placeholder="Активный, outdoor, hiking, путешествия", key="aud_lifestyle")
        with _aud_col2:
            _aud_income = st.selectbox("💰 Доход", ["Средний", "Выше среднего", "Высокий", "Любой"], key="aud_income")
            _aud_geo = st.text_input("🌍 География", placeholder="Германия, EU", key="aud_geo")
        _aud_extra = st.text_area("💬 Доп. инфо о ЦА", placeholder="Ценит качество, экологичность, покупает онлайн, читает отзывы...", height=68, key="aud_extra")

        _imgs_for_aud = st.session_state.get("images", [])
        if not _imgs_for_aud:
            st.info("Запусти анализ чтобы загрузить фото листинга")
        elif st.button("👥 Оценить все фото для ЦА", type="primary", key="btn_audience_score"):
            if not (_aud_age or _aud_lifestyle):
                st.warning("Заполни хотя бы возраст/пол и образ жизни")
            else:
                _aud_profile = f"""
ЦЕЛЕВАЯ АУДИТОРИЯ:
- Возраст/пол: {_aud_age}
- Образ жизни: {_aud_lifestyle}
- Доход: {_aud_income}
- География: {_aud_geo}
- Дополнительно: {_aud_extra}
""".strip()
                _aud_results = []
                _vprog = st.progress(0, "👥 Оцениваю фото для ЦА...")
                for _ai_idx, _aimg in enumerate(_imgs_for_aud[:6]):
                    _vprog.progress(int((_ai_idx+1)/min(len(_imgs_for_aud),6)*100), f"Фото {_ai_idx+1}...")
                    try:
                        _ab64 = _aimg.get("b64","") if isinstance(_aimg, dict) else _aimg
                        _amt = _aimg.get("media_type","image/jpeg") if isinstance(_aimg, dict) else "image/jpeg"
                        # Photo #1 = main Amazon image (white bg required)
                        _is_main_photo = (_ai_idx == 0)
                        _photo_context = """ВАЖНО: Это ГЛАВНОЕ фото листинга (#1).
По правилам Amazon главное фото ОБЯЗАНО быть на чистом белом фоне (RGB 255,255,255).
НЕ рекомендуй менять белый фон на lifestyle — это нарушит правила Amazon и приведёт к suppression.
Оценивай только: позу модели, читаемость товара, посадку, выражение лица, динамику движения, 
соответствие модели ЦА. Рекомендуй улучшения в рамках белого фона (поза, движение, угол съёмки).""" if _is_main_photo else """Это дополнительное фото #{} листинга. Оценивай свободно — фон, контекст, lifestyle.""".format(_ai_idx+1)

                        _aprompt = f"""Ты маркетолог-эксперт по Amazon. Оцени это фото товара с точки зрения целевой аудитории.

{_aud_profile}

ПРОДУКТ: {od.get('title','')}

{_photo_context}

Оцени по шкале 0-100% насколько это фото убедительно для данной аудитории.

Ответь строго в формате:
SCORE: [0-100]%
ЭМОЦИЯ_ЦА: [что чувствует покупатель глядя на это фото]
СООТВЕТСТВУЕТ: [что на фото совпадает с интересами ЦА]
НЕ СООТВЕТСТВУЕТ: [что не совпадает или отталкивает ЦА]
РЕКОМЕНДАЦИЯ: [одно конкретное улучшение — поза/движение/угол для #1, сцена/контекст для остальных]"""
                        _aud_resp = anthropic_vision(
                            content_blocks=[
                                {"type":"image","source":{"type":"base64","media_type":_amt,"data":_ab64}},
                                {"type":"text","text":_aprompt}
                            ],
                            max_tokens=500,
                            system="Ты маркетолог-эксперт по Amazon. Отвечай строго по формату."
                        )
                        _aud_text = _aud_resp.get("content",[{}])[0].get("text","") if isinstance(_aud_resp, dict) else str(_aud_resp)
                        _aud_results.append({"idx": _ai_idx+1, "text": _aud_text, "b64": _ab64, "mt": _amt})
                    except Exception as _ae:
                        _aud_results.append({"idx": _ai_idx+1, "text": f"Ошибка: {_ae}", "b64": "", "mt": ""})
                _vprog.progress(100, "✅ Готово!")
                st.session_state["_aud_results"] = _aud_results

        # Show results
        if st.session_state.get("_aud_results"):
            st.markdown("---")
            for _ar in st.session_state["_aud_results"]:
                _arc1, _arc2 = st.columns([1, 3])
                with _arc1:
                    if _ar.get("b64"):
                        st.image(f"data:{_ar['mt']};base64,{_ar['b64']}", use_container_width=True)
                    st.caption(f"Фото #{_ar['idx']}")
                with _arc2:
                    _txt = _ar["text"]
                    # Extract score for color
                    import re as _re2
                    _sm = _re2.search(r'SCORE:\s*(\d+)%', _txt)
                    _sc = int(_sm.group(1)) if _sm else 0
                    _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else "#ef4444")
                    _sc_l = "✅ Отлично для ЦА" if _sc>=75 else ("🟡 Средне" if _sc>=50 else "🔴 Слабо для ЦА")
                    st.markdown(
                        f'<div style="background:#0f172a;border-left:4px solid {_sc_c};border-radius:8px;padding:12px 14px">' +
                        f'<div style="font-size:1.4rem;font-weight:800;color:{_sc_c}">{_sc}% <span style="font-size:0.8rem">{_sc_l}</span></div>' +
                        f'<div style="font-size:0.82rem;color:#e2e8f0;margin-top:8px;white-space:pre-line">{_txt.replace("SCORE:","").replace(f"{_sc}%","",1).strip()}</div>' +
                        f'</div>',
                        unsafe_allow_html=True)
            # ── Summary & Ranking ────────────────────────────────────────────
            import re as _re3
            _aud_scored = []
            for _ar2 in st.session_state["_aud_results"]:
                _sm2 = _re3.search(r'SCORE:\s*(\d+)%', _ar2["text"])
                _sc2 = int(_sm2.group(1)) if _sm2 else 0
                _aud_scored.append((_sc2, _ar2["idx"], _ar2["text"]))
            _aud_scored_sorted = sorted(_aud_scored, reverse=True)

            st.markdown("---")
            st.markdown("### 🏆 Итоговый рейтинг фото для вашей ЦА")

            # Detect which photo has white background (likely photo #1 from listing)
            # Main image (#1) MUST be white bg per Amazon rules — pin it to position #1
            _main_photo_idx = 1  # always keep original #1 as main (white bg rule)
            _rest_sorted = [(sc,idx,txt) for sc,idx,txt in _aud_scored_sorted if idx != _main_photo_idx]
            _best_sc, _best_idx, _best_txt = _aud_scored_sorted[0]

            st.markdown(
                '<div style="background:#1a1a0a;border:2px solid #f59e0b;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                '<div style="font-size:0.75rem;font-weight:700;color:#f59e0b">⚠️ ПРАВИЛО AMAZON</div>' +
                '<div style="font-size:0.82rem;color:#fde68a;margin-top:4px">Главное фото (#1) <b>всегда белый фон</b> — это обязательное требование Amazon. Оно не участвует в ранжировании по ЦА.</div>' +
                '</div>', unsafe_allow_html=True)

            # Best lifestyle photo
            if _rest_sorted:
                _bsc, _bidx, _btxt = _rest_sorted[0]
                _brm = _re3.search(r'РЕКОМЕНДАЦИЯ:(.+?)(?=\n[А-Я]|$)', _btxt, _re3.DOTALL)
                _brec = _brm.group(1).strip()[:200] if _brm else ""
                st.markdown(
                    f'<div style="background:#0f3a1a;border:2px solid #22c55e;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                    f'<div style="font-size:0.9rem;font-weight:800;color:#22c55e">🥇 Лучшее lifestyle фото для ЦА — Фото #{_bidx} ({_bsc}%)</div>' +
                    (f'<div style="font-size:0.78rem;color:#86efac;margin-top:4px">{_brec}</div>' if _brec else "") +
                    f'</div>', unsafe_allow_html=True)

            # Ordered ranking table
            _rank_html = '<div style="background:#0f172a;border-radius:10px;padding:12px 16px">' + '<div style="font-size:0.75rem;font-weight:700;color:#64748b;margin-bottom:8px">РЕКОМЕНДУЕМЫЙ ПОРЯДОК ФОТО</div>'
            # Position 1 = always original main photo (white bg)
            _rank_html += (
                '<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                '<span style="font-size:0.8rem;color:#94a3b8">📸 Главное (#1) — Amazon rule</span>' +
                f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #1</span>' +
                '<span style="font-size:0.75rem;color:#f59e0b">🤍 Белый фон</span>' +
                '</div>'
            )
            _position_labels2 = ["🥇 Второе (#2) — лучшее lifestyle", "🥈 Третье (#3)", "🥉 Четвёртое (#4)", "4️⃣ Пятое (#5)", "5️⃣ Шестое (#6)"]
            for _pi, (_psc, _pidx, _ptxt) in enumerate(_rest_sorted):
                _pc = "#22c55e" if _psc>=75 else ("#f59e0b" if _psc>=50 else "#ef4444")
                _plbl = _position_labels2[_pi] if _pi < len(_position_labels2) else f"#{_pi+2}"
                _rank_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                    f'<span style="font-size:0.8rem;color:#94a3b8">{_plbl}</span>' +
                    f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #{_pidx}</span>' +
                    f'<span style="font-size:0.85rem;font-weight:700;color:{_pc}">{_psc}%</span>' +
                    f'</div>'
                )
            _rank_html += '</div>'
            st.markdown(_rank_html, unsafe_allow_html=True)

            # AI summary prompt
            if st.button("🧠 AI — сводный план улучшений", key="btn_aud_summary"):
                with st.spinner("🧠 Генерирую план..."):
                    _all_reviews = "\n\n".join([
                        f"ФОТО #{_ar2['idx']} ({_ar2['text'][:300]})"
                        for _ar2 in st.session_state["_aud_results"]
                    ])
                    _sum_prompt = f"""На основе оценки {len(st.session_state["_aud_results"])} фото листинга для ЦА ({_aud_age}, {_aud_lifestyle}, {_aud_income}):

{_all_reviews}

Дай КРАТКИЙ план из 3-5 конкретных действий чтобы улучшить весь набор фото для этой ЦА.
Формат: пронумерованный список, каждый пункт = одно действие + ожидаемый эффект (+X% конверсии).
Язык: {'русский' if st.session_state.get('analysis_lang','ru')=='ru' else 'английский'}."""
                    _sum_r = ai_call("Amazon photo strategist", _sum_prompt, max_tokens=600)
                    st.session_state["_aud_summary"] = _sum_r

            if st.session_state.get("_aud_summary"):
                st.markdown(
                    '<div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-top:8px">' +
                    '<div style="font-size:0.72rem;font-weight:700;color:#64748b;margin-bottom:8px">📋 ПЛАН УЛУЧШЕНИЙ ФОТО ДЛЯ ЦА</div>' +
                    f'<div style="font-size:0.85rem;color:#e2e8f0;line-height:1.7">{st.session_state["_aud_summary"].replace(chr(10),"<br>")}</div></div>',
                    unsafe_allow_html=True)

            if st.button("🗑️ Очистить", key="clear_aud"):
                st.session_state.pop("_aud_results", None)
                st.session_state.pop("_aud_summary", None)
                st.rerun()

    # ── Claid AI Photo Generator ──────────────────────────────────────────────
    _claid_key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("CLAID_API_KEY","")
    if _claid_key:  # GEMINI_API_KEY
        with st.expander("🎨 AI-генерация lifestyle фото — Gemini Nano Banana Pro", expanded=False):
            st.caption("Выбери любое фото из анализа или загрузи своё → AI создаст lifestyle сцену")
            _claid_col1, _claid_col2 = st.columns([3,2])
            with _claid_col1:
                _scene_options = {
                "🤍 Белый фон (Amazon Main Image)": "pure white background, professional studio product photography, soft shadow, white backdrop, e-commerce",
                "🏔️ Hiking / Outdoor горы": "on a rugged mountain trail, scattered rocks, sunlight through pine trees, rich earthy colors, warm light, professional product photography",
                "🏠 Уютный дом / Interior": "cozy modern home interior, warm lighting, wooden furniture, lifestyle product photography",
                "🏋️ Gym / Фитнес": "modern gym interior, fitness lifestyle, dynamic lighting, professional product photography",
                "❄️ Зима / Snow": "snowy mountain landscape, cold winter day, soft blue light, outdoor winter lifestyle",
                "🌊 Лето / Summer beach": "sunny summer day, outdoor adventure, bright natural light, lifestyle product photography",
                "🌿 Nature / Природа": "lush green forest path, dappled sunlight, natural outdoor setting, professional photography",
                "💼 Офис / Professional": "modern office setting, professional lifestyle, clean minimalist background",
            }
            _scene_label = st.selectbox("Сцена", list(_scene_options.keys()), key="claid_scene_sel")
            _claid_scene = _scene_options[_scene_label]
            if "белый" in _scene_label.lower() or "Amazon" in _scene_label:
                st.caption("✅ Подходит для главного фото Amazon (#1) — чистый белый фон")
            else:
                st.caption("📸 Lifestyle сцена — для фото #2-7 в листинге")

            # Photos from analysis
            _analysis_imgs = st.session_state.get("images", [])
            _img_b64_claid = None
            _img_mt_claid = "image/jpeg"

            if _analysis_imgs:
                st.markdown("**📸 Выбери фото из анализа:**")
                _img_cols = st.columns(min(len(_analysis_imgs), 5))
                for _pi, (_pc, _pimg) in enumerate(zip(_img_cols, _analysis_imgs[:5])):
                    with _pc:
                        try:
                            _pb64 = _pimg.get("b64","") if isinstance(_pimg, dict) else _pimg
                            _pmt = _pimg.get("media_type","image/jpeg") if isinstance(_pimg, dict) else "image/jpeg"
                            st.image(f"data:{_pmt};base64,{_pb64}", use_container_width=True)
                            if st.button(f"✓ Фото {_pi+1}", key=f"claid_pick_{_pi}", use_container_width=True):
                                st.session_state["_claid_picked_b64"] = _pb64
                                st.session_state["_claid_picked_mt"] = _pmt
                                st.session_state["_claid_picked_idx"] = _pi+1
                                st.rerun()
                        except: pass

                if st.session_state.get("_claid_picked_b64"):
                    _img_b64_claid = st.session_state["_claid_picked_b64"]
                    _img_mt_claid = st.session_state.get("_claid_picked_mt","image/jpeg")
                    st.success(f"✅ Выбрано фото #{st.session_state.get('_claid_picked_idx',1)}")

            # Or upload new
            with _claid_col2:
                st.markdown("**или загрузи своё:**")
                _claid_upload = st.file_uploader("📤 PNG/JPG", type=["png","jpg","jpeg"], key="claid_upload", label_visibility="collapsed")
                if _claid_upload:
                    import base64 as _b64c
                    _img_bytes = _claid_upload.read()
                    _img_b64_claid = _b64c.b64encode(_img_bytes).decode()
                    _img_mt_claid = "image/jpeg" if _claid_upload.name.lower().endswith(("jpg","jpeg")) else "image/png"
                    st.image(_img_bytes, width=120)

            # Показываем рекомендацию из Vision анализа для выбранного фото
            _picked_idx = st.session_state.get("_claid_picked_idx", 0)
            if _picked_idx and st.session_state.get("vision"):
                import re as _re_v
                _v_text = st.session_state.get("vision","")
                _blk_m = _re_v.search(rf"PHOTO_BLOCK_{_picked_idx}\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", _v_text, _re_v.DOTALL)
                if _blk_m:
                    _blk = _blk_m.group(1)
                    _act_m = _re_v.search(r"(?:Действие|Action)\s*[:\-]\s*(.{10,})", _blk)
                    _wk_m  = _re_v.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{10,})", _blk)
                    if _act_m or _wk_m:
                        st.markdown("**🤖 AI рекомендует исправить:**")
                        if _wk_m:  st.warning(f"⚠️ {_wk_m.group(1).strip()[:150]}")
                        if _act_m:
                            _ai_action = _act_m.group(1).strip()[:200]
                            st.info(f"→ {_ai_action}")
                            # Авто-подставляем рекомендацию как сцену
                            if st.checkbox("🎯 Использовать рекомендацию AI как сцену", value=True, key="use_ai_scene"):
                                _claid_scene = _ai_action

            if _img_b64_claid:
                if st.button("🎨 Генерировать улучшенное фото", type="primary", key="btn_claid_gen"):
                    with st.spinner(f"🎨 Gemini генерирует..."):
                        _urls, _err = claid_generate_lifestyle(_img_b64_claid, scene=_claid_scene, media_type=_img_mt_claid)
                        if _err:
                            st.error(f"❌ {_err}")
                        elif _urls:
                            st.session_state["_claid_results"] = _urls
                            st.success(f"✅ {len(_urls)} фото готово!")
                        else:
                            st.warning("Фото не получены — попробуй ещё раз")

            if st.session_state.get("_claid_results"):
                _cr = st.session_state["_claid_results"]
                st.markdown(f"**🖼️ Результат ({len(_cr)} фото):**")
                _rc = st.columns(min(len(_cr), 4))
                for _i, (_rcol, _rimg) in enumerate(zip(_rc, _cr)):
                    with _rcol:
                        if isinstance(_rimg, dict) and _rimg.get("b64"):
                            import base64 as _b64dl
                            _rb64=_rimg["b64"]; _rmt=_rimg.get("mt","image/jpeg")
                            st.image(f"data:{_rmt};base64,{_rb64}", use_container_width=True)
                            st.download_button(f"⬇️ #{_i+1}", _b64dl.b64decode(_rb64), f"lifestyle_{_i+1}.jpg", "image/jpeg", key=f"dl_img_{_i}", use_container_width=True)
                        elif isinstance(_rimg, str):
                            st.image(_rimg, use_container_width=True)

                if st.button("🗑️ Очистить", key="claid_clear"):
                    st.session_state.pop("_claid_results", None)
                    st.session_state.pop("_claid_picked_b64", None)
                    st.rerun()

    _vpc=r.get("vpc_analysis",{}); _jtbd=r.get("jtbd_analysis",{})
    if not _vpc and not _jtbd: st.info("Данные VPC/JTBD появятся после следующего анализа"); st.stop()
    _fit=pct(_vpc.get("fit_score",_jtbd.get("alignment_score",0))); _jfit=pct(_jtbd.get("alignment_score",0))
    _fc="#22c55e" if _fit>=75 else ("#f59e0b" if _fit>=50 else "#ef4444")
    _jfc="#22c55e" if _jfit>=75 else ("#f59e0b" if _jfit>=50 else "#ef4444")
    _gap=100-max(_fit,_jfit); _gc3="#ef4444" if _gap>50 else ("#f59e0b" if _gap>25 else "#22c55e")
    _hc1,_hc2,_hc3=st.columns(3)
    _hc1.markdown(f'<div style="background:#1e293b;border-radius:12px;padding:16px;text-align:center"><div style="font-size:2.5rem;font-weight:800;color:{_fc}">{_fit}%</div><div style="color:{_fc};font-size:0.85rem;font-weight:600">VPC Fit Score</div></div>', unsafe_allow_html=True)
    _hc2.markdown(f'<div style="background:#1e293b;border-radius:12px;padding:16px;text-align:center"><div style="font-size:2.5rem;font-weight:800;color:{_jfc}">{_jfit}%</div><div style="color:{_jfc};font-size:0.85rem;font-weight:600">JTBD Alignment</div></div>', unsafe_allow_html=True)
    _hc3.markdown(f'<div style="background:#1e293b;border-radius:12px;padding:16px;text-align:center"><div style="font-size:2.5rem;font-weight:800;color:{_gc3}">{_gap}%</div><div style="color:{_gc3};font-size:0.85rem;font-weight:600">Value Gap</div></div>', unsafe_allow_html=True)
    if _vpc.get("vpc_verdict"): st.markdown(f'<div style="background:#0f172a;border-left:4px solid {_fc};border-radius:8px;padding:14px 18px;margin:16px 0;color:#e2e8f0;line-height:1.6"><b style="color:{_fc}">🤖 AI CRO Консультант:</b> {_vpc["vpc_verdict"]}</div>', unsafe_allow_html=True)
    if _jtbd.get("job_story"): st.markdown(f'<div style="background:#1e293b;border-radius:10px;padding:14px 18px;margin-bottom:16px"><div style="font-size:0.75rem;font-weight:700;color:#64748b;letter-spacing:0.08em;margin-bottom:6px">JOB STORY</div><div style="font-size:0.95rem;color:#e2e8f0;font-style:italic;line-height:1.6">{_jtbd["job_story"]}</div></div>', unsafe_allow_html=True)
    st.divider(); st.subheader("📊 Value Proposition Canvas")
    _lc,_rc=st.columns(2)
    with _lc:
        st.markdown("**👤 Профиль покупателя**")
        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#3b82f6;margin:8px 0 4px">ЗАДАЧИ</div>', unsafe_allow_html=True)
        for j in [x for x in _vpc.get("customer_jobs",[_jtbd.get("functional_job",""),_jtbd.get("emotional_job",""),_jtbd.get("social_job","")]) if x]:
            st.markdown(f'<div style="background:#1e3a5f22;border-left:3px solid #3b82f6;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">{j}</div>', unsafe_allow_html=True)
        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#ef4444;margin:10px 0 4px">БОЛИ</div>', unsafe_allow_html=True)
        for p in _vpc.get("customer_pains",[]): st.markdown(f'<div style="background:#ef444422;border-left:3px solid #ef4444;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">{p}</div>', unsafe_allow_html=True)
        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#22c55e;margin:10px 0 4px">ВЫГОДЫ</div>', unsafe_allow_html=True)
        for g in _vpc.get("customer_gains",[]): st.markdown(f'<div style="background:#22c55e22;border-left:3px solid #22c55e;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">{g}</div>', unsafe_allow_html=True)
    with _rc:
        st.markdown("**📦 Карта ценности**")
        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#94a3b8;margin:8px 0 4px">ФИЧИ</div>', unsafe_allow_html=True)
        for ps in _vpc.get("products_services",[]): st.markdown(f'<div style="background:#33333322;border-left:3px solid #94a3b8;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">{ps}</div>', unsafe_allow_html=True)
        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#ef4444;margin:10px 0 4px">PAIN RELIEVERS</div>', unsafe_allow_html=True)
        for pr in _vpc.get("pain_relievers_present",[]): st.markdown(f'<div style="background:#22c55e22;border-left:3px solid #22c55e;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">✅ {pr}</div>', unsafe_allow_html=True)
        for pr in _vpc.get("pain_relievers_missing",[]): st.markdown(f'<div style="background:#ef444422;border-left:3px solid #ef4444;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">❌ {pr}</div>', unsafe_allow_html=True)
        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#22c55e;margin:10px 0 4px">GAIN CREATORS</div>', unsafe_allow_html=True)
        for gc in _vpc.get("gain_creators_present",[]): st.markdown(f'<div style="background:#22c55e22;border-left:3px solid #22c55e;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">✅ {gc}</div>', unsafe_allow_html=True)
        for gc in _vpc.get("gain_creators_missing",[]): st.markdown(f'<div style="background:#ef444422;border-left:3px solid #ef4444;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">❌ {gc}</div>', unsafe_allow_html=True)
    st.divider(); _gc1,_gc2=st.columns(2)
    with _gc1:
        st.subheader("❌ Что не коммуницирует")
        for g in _jtbd.get("jtbd_gaps",[]): st.error(f"✗ {g}")
        for g in _vpc.get("pain_relievers_missing",[]):
            if g not in str(_jtbd.get("jtbd_gaps",[])): st.error(f"✗ {g}")
    with _gc2:
        st.subheader("✅ Как переписать")
        for rec in _jtbd.get("jtbd_recs",[]): st.success(f"→ {rec}")

# ══ Конкурент N ════════════════════════════════════════════════════════════════
elif _is_competitor_page:
    idx_m=re.search(r"Конкурент (\d+)",page)
    cidx=int(idx_m.group(1))-1 if idx_m else 0
    c=cd[cidx] if cidx<len(cd) else {}
    if not c: st.warning("Данные конкурента не найдены"); st.stop()
    cpi=c.get("product_information",{}); casin=get_asin_from_data(c); _t2=c.get("title","")
    _i2=c.get("images",[]); _b2=c.get("feature_bullets",[]); _d2=c.get("description","")
    _rat2=safe_float_rating(c.get("average_rating",0))
    _rev2=int(str(cpi.get("Customer Reviews",{}).get("ratings_count","0") or 0).replace(",","").strip() or 0)
    _vid2=int(c.get("number_of_videos",0) or 0)>0; _ap2=bool(c.get("aplus"))
    _pr2=bool(c.get("is_prime_exclusive") or c.get("is_prime") or "amazon" in str(c.get("ships_from","")).lower())
    _bsr2=99999; _bm=re.search(r"#([\d,]+)",str(cpi.get("Best Sellers Rank","")))
    if _bm:
        try: _bsr2=int(_bm.group(1).replace(",",""))
        except: pass
    _col2=len([_cv for _cv in c.get("customization_options",{}).get("color",[]) if isinstance(_cv,dict) and _cv.get("asin","") not in ("","undefined")])
    _sz2=len(c.get("customization_options",{}).get("size",[]))
    if "one size" in str(cpi.get("Size","")).lower(): _sz2=3
    tlen=len(_t2); cprice=c.get("price",""); cbrand=c.get("brand","")
    crating=c.get("average_rating","")
    # reviews_count can be in multiple places
    crev = (cpi.get("Customer Reviews",{}).get("ratings_count","") or
            c.get("reviews_count","") or c.get("ratings_total","") or
            c.get("number_of_reviews","") or "")
    if crev: crev = str(crev).replace(",","").strip()
    # BSR can be string or dict
    _bsr_raw = cpi.get("Best Sellers Rank","") or c.get("bestseller_rank","") or c.get("bsr","")
    cbsr_s = str(_bsr_raw)[:60] if _bsr_raw else ""
    _ts=min(10,max(0,(1.5 if tlen<=125 else 0)+(3.5 if any(k in _t2.lower() for k in ["merino","wool","tank","shirt","base layer"]) else 1.5)+3+(1 if not re.search(r"[!$?{}]",_t2) else 0)+1))
    _bs=min(10,max(0,(1.5 if len(_b2)<=5 else 0)+(2.5 if any(":"in b for b in _b2) else 1)+min(4,len(_b2))+1+1))
    _ds=0 if not _d2 else min(10,4+(3 if len(_d2)>200 else 1))
    _ps=min(10,max(0,(4 if len(_i2)>=6 else len(_i2)*0.6)+(2 if _vid2 else 0)+(4 if len(_i2)>=6 else 0)))
    _as=0 if not _ap2 else 7; _rs=10 if (_rat2>=4.4 and _rev2>=50) else (7 if _rat2>=4.0 else 4)
    _bsrs=10 if _bsr2<=1000 else (8 if _bsr2<=5000 else 5); _prs=10 if _pr2 else 5
    _vs=10 if (_col2>=5 and _sz2>=3) else (8 if _col2>=5 else (7 if _sz2>=3 else 4))
    _h=int((_ts*0.10+_bs*0.10+_ds*0.10+_ps*0.10+_as*0.10+_rs*0.15+_bsrs*0.15+7*0.10+_vs*0.05+_prs*0.05)*10)
    ch=_h; hc="#22c55e" if ch>=75 else ("#f59e0b" if ch>=50 else "#ef4444")

    # Variables needed for price line
    _cprev   = c.get("previous_price","") or c.get("list_price","")
    _ccoupon = c.get("coupon_text","") or ("🎟️ Купон" if c.get("is_coupon_exists") else "")
    _cpromo  = c.get("promo_text","")
    _cbought = c.get("number_of_people_bought","")

    st.title(f"🔴 Конкурент {cidx+1}")

    # Build price line — same as health_card
    _cprice_parts = []
    if cprice:
        _cps = f"💰 <b>{cprice}</b>"
        if _cprev and _cprev != cprice:
            _cps += f" <span style='text-decoration:line-through;opacity:0.5'>{_cprev}</span>"
        _cprice_parts.append(_cps)
    if _ccoupon:
        _cprice_parts.append(f"<span style='background:#16a34a;color:white;border-radius:4px;padding:1px 6px;font-size:0.75rem'>🎟️ {_ccoupon}</span>")
    if _cpromo:
        _cprice_parts.append(f"<span style='background:#1d4ed8;color:white;border-radius:4px;padding:1px 6px;font-size:0.75rem'>📦 {_cpromo[:35]}</span>")
    if _pr2:
        _cprice_parts.append("<span style='background:#f59e0b;color:#1c1917;border-radius:4px;padding:1px 6px;font-size:0.75rem'>👑 Prime</span>")
    if _cbought:
        _cprice_parts.append(f"<span style='opacity:0.7;font-size:0.75rem'>🛒 {_cbought}</span>")
    _cprice_line = "  ".join(_cprice_parts)

    _cmp_saved = st.session_state.get("_marketplace","com")
    _crat_c = "#22c55e" if _rat2>=4.4 else ("#f59e0b" if _rat2>=4.3 else "#ef4444")
    _ctlen_c = "#fca5a5" if tlen>125 else "#86efac"
    _cbsr_s2 = str(cpi.get("Best Sellers Rank","") or c.get("bestseller_rank","") or "")[:50]
    _cmp_saved = st.session_state.get("_marketplace","com")

    # Header card — split into small chunks to avoid HTML rendering issues
    st.markdown(
        f'<div style="background:linear-gradient(135deg,#3b1e1e,#5c2626);border-radius:16px;padding:20px 24px;color:white;margin-bottom:16px">' +
        f'<a href="https://www.amazon.{_cmp_saved}/dp/{casin}" target="_blank" style="font-size:0.78rem;opacity:0.6;color:#93c5fd;text-decoration:none">{cbrand} · {casin} ↗</a>' +
        f'<div style="font-size:1rem;font-weight:600;max-width:520px;line-height:1.4;margin-top:4px">{_t2[:80]}{"..." if tlen>80 else ""}</div>' +
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-top:12px;flex-wrap:wrap;gap:12px">' +
        f'<div>' +
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:6px">{_cprice_line}</div>' +
        f'<div style="display:flex;gap:14px;font-size:0.82rem;flex-wrap:wrap">' +
        f'<span style="color:{_crat_c};font-weight:600">⭐ {crating} ({crev} отз.)</span>' +
        (f'<span style="opacity:0.8">📊 {_cbsr_s2}</span>' if _cbsr_s2 else '') +
        f'<span style="color:{_ctlen_c}">📝 {tlen} симв.</span>' +
        f'</div></div>' +
        f'<div style="text-align:center">' +
        f'<div style="font-size:3rem;font-weight:800;color:{hc};line-height:1">{ch}%</div>' +
        f'<div style="font-size:0.8rem;color:{hc};margin-top:2px">{"Отличный" if ch>=75 else ("Средний" if ch>=50 else "Слабый")}</div>' +
        f'</div></div>' +
        f'<div style="background:rgba(255,255,255,0.12);border-radius:6px;height:8px;margin-top:12px">' +
        f'<div style="background:{hc};width:{ch}%;height:8px;border-radius:6px"></div></div></div>',
        unsafe_allow_html=True
    )

    _cai_key=f"comp_ai_{cidx}"; _vision_key=f"comp_vision_{cidx}"; _cai_result=st.session_state.get(_cai_key)

    _comp_ret_col1,_comp_ret_col2=st.columns([2,4])
    with _comp_ret_col1:
        if st.button("🔍 Анализ возвратов", key=f"btn_comp_ret_{cidx}", use_container_width=True):
            with st.spinner("📥 Загружаю 1★ отзывы..."):
                _cret_reviews=fetch_1star_reviews(casin,domain="com",max_pages=1)
            if _cret_reviews:
                with st.spinner("🧠 AI анализирует..."):
                    st.session_state[f"_comp_ret_{cidx}"]=analyze_return_reasons(_cret_reviews,_t2,casin,lang=st.session_state.get("analysis_lang","ru"))
                    st.session_state[f"_comp_ret_cnt_{cidx}"]=len(_cret_reviews)
            else: st.warning("Отзывы не загружены")
    if st.session_state.get(f"_comp_ret_{cidx}"):
        with st.expander(f"📊 Анализ возвратов ({st.session_state.get(f'_comp_ret_cnt_{cidx}',0)} отзывов)", expanded=True):
            st.markdown(st.session_state[f"_comp_ret_{cidx}"])

    if not _cai_result:
        _cbtn1,_cbtn2=st.columns([3,1])
        with _cbtn1: st.info("💡 Нажми Анализ")
        with _cbtn2:
            if st.button("🧠 Анализ",key=f"ai_btn_{cidx}",type="primary"):
                _clang=st.session_state.get("analysis_lang","ru")
                _prog=st.progress(0,text="⬇️ Загружаю фото...")
                _cimgs_dl=download_images(c.get("images",[])[:5],lambda m:None) if c.get("images") else []
                _prog.progress(33,text="👁️ Vision...")
                _comp_vision=analyze_vision(_cimgs_dl,c,casin,lambda m:None,lang=_clang) if _cimgs_dl else ""
                _prog.progress(66,text="🧠 AI анализ...")
                _cai_result=analyze_text(c,[],_comp_vision,casin,lambda m:None,lang=_clang,is_competitor=True)
                st.session_state[_cai_key]=_cai_result
                if _cimgs_dl: st.session_state[_vision_key]=(_cimgs_dl,_comp_vision)
                _prog.progress(100,text="✅ Готово!")
                st.rerun()

    if _cai_result:
        _sitems=[("Title",pct(_cai_result.get("title_score",0))),("Bullets",pct(_cai_result.get("bullets_score",0))),("Описание",pct(_cai_result.get("description_score",0))),("Фото",pct(_cai_result.get("images_score",0))),("A+",pct(_cai_result.get("aplus_score",0))),("Отзывы",pct(_cai_result.get("reviews_score",0))),("BSR",pct(_cai_result.get("bsr_score",0))),("Варианты",pct(_cai_result.get("customization_score",0))),("Prime",pct(_cai_result.get("prime_score",0)))]
        _overall=pct(_cai_result.get("overall_score",0)); _ohc="#22c55e" if _overall>=75 else ("#f59e0b" if _overall>=50 else "#ef4444")
        st.markdown(f"**🧠 AI Overall: <span style='color:{_ohc};font-size:1.3rem'>{_overall}%</span>**", unsafe_allow_html=True)
    else:
        _sitems=[("Title",_ts*10),("Bullets",_bs*10),("Описание",_ds*10),("Фото",_ps*10),("A+",_as*10),("Отзывы",_rs*10),("BSR",_bsrs*10),("Варианты",_vs*10),("Prime",_prs*10)]
        st.caption("📊 Авто-оценка — нажми AI Анализ")

    _sc2=st.columns(len(_sitems))
    for _col3,(_lbl3,_p3) in zip(_sc2,_sitems):
        if _lbl3=="Описание" and _p3==0 and _ap2:
            _col3.markdown('<div style="border-left:3px solid #64748b;padding:5px 4px;text-align:center;background:#f8fafc;border-radius:4px"><div style="font-size:0.85rem;font-weight:700;color:#64748b">A+</div><div style="font-size:0.62rem;color:#64748b">Описание</div></div>', unsafe_allow_html=True)
        else:
            _c3="#22c55e" if _p3>=75 else ("#f59e0b" if _p3>=50 else "#ef4444")
            _col3.markdown(f'<div style="border-left:3px solid {_c3};padding:5px 4px;text-align:center;background:#f8fafc;border-radius:4px"><div style="font-size:1.05rem;font-weight:700;color:{_c3}">{_p3}%</div><div style="font-size:0.62rem;color:#64748b">{_lbl3}</div></div>', unsafe_allow_html=True)

    st.divider()
    tab_cont,tab_photo,tab_aplus,tab_data=st.tabs(["📝 Контент","📸 Фото","🎨 A+","📊 Данные"])
    with tab_cont:
        st.markdown(f"**Title** ({tlen} симв.)"); st.progress(min(pct(_cai_result.get("title_score",0)) if _cai_result else int(_ts*10),100)/100); st.markdown(f"> {_t2}")
        if _cai_result and _cai_result.get("title_gaps"):
            with st.expander(f"⚠️ ({len(_cai_result['title_gaps'])})"):
                for g in _cai_result["title_gaps"]: st.markdown(f"- {g}")
        st.divider(); st.markdown(f"**Bullets** ({len(_b2)})")
        for _bul in _b2:
            _blen=len(_bul.encode()); st.markdown(f"{'🔴' if _blen>255 else '✅'} {_bul}"); st.caption(f"{_blen} байт")
        st.divider(); st.markdown("**Описание**")
        if _d2: st.markdown(str(_d2)[:600])
        elif _ap2: st.markdown('<div style="background:#1e3a1e;border-left:4px solid #22c55e;border-radius:8px;padding:10px 14px"><span style="color:#22c55e;font-weight:700">✅ Скрыто A+</span></div>', unsafe_allow_html=True)
        else: st.warning("Описание отсутствует")
        st.divider(); st.markdown(f"**A+:** {'✅' if _ap2 else '❌'}  |  **Видео:** {'✅' if _vid2 else '❌'}")
    with tab_photo:
        _cimgs=c.get("images",[])
        if _cimgs:
            if _vision_key in st.session_state and st.session_state[_vision_key]:
                _cv_imgs,_cv_text=st.session_state[_vision_key]
                _cv_blocks={}
                for _m in re.finditer(r"PHOTO_BLOCK_(\d+)\s*(.*?)(?=PHOTO_BLOCK_\d+|$)",_cv_text,re.DOTALL):
                    _cv_blocks[int(_m.group(1))]=_m.group(2).strip()
                # ── Сводка конкурента ─────────────────────────────────
                _cscores_sum = []
                for _bi2, _bt2 in enumerate(_cv_blocks.values()):
                    _sm2 = re.search(r"(\d+)/10", _bt2)
                    _sv2 = int(_sm2.group(1)) if _sm2 else 0
                    _tm2 = re.search(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)", _bt2)
                    _tv2 = _tm2.group(1).strip()[:16] if _tm2 else f"#{_bi2+1}"
                    _cscores_sum.append((_bi2+1, _sv2, _tv2))
                if _cscores_sum:
                    _cavg = sum(s for _,s,_ in _cscores_sum) / len(_cscores_sum)
                    _cavg_c = "#22c55e" if _cavg>=7 else ("#f59e0b" if _cavg>=5 else "#ef4444")
                    _cc_html = "".join([
                        f'<div style="display:flex;flex-direction:column;align-items:center;background:#1e293b;border-radius:8px;padding:6px 8px;min-width:60px;border-top:3px solid {"#22c55e" if s>=8 else ("#f59e0b" if s>=6 else "#ef4444")}">' +
                        f'<div style="font-size:0.65rem;color:#64748b">#{n}</div>' +
                        f'<div style="font-size:1.3rem;font-weight:800;color:{"#22c55e" if s>=8 else ("#f59e0b" if s>=6 else "#ef4444")}">{s}</div>' +
                        f'<div style="font-size:0.55rem;color:#64748b;text-align:center;max-width:55px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis">{t}</div></div>'
                        for n,s,t in _cscores_sum
                    ])
                    st.markdown(
                        f'<div style="background:#0f172a;border-radius:10px;padding:12px 14px;margin-bottom:12px">' +
                        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">' +
                        f'<div style="font-size:0.8rem;color:#94a3b8">📊 {len(_cscores_sum)} фото</div>' +
                        f'<div style="font-size:1rem;font-weight:800;color:{_cavg_c}">Ср: {_cavg:.1f}/10</div></div>' +
                        f'<div style="display:flex;gap:6px;flex-wrap:wrap">{_cc_html}</div></div>',
                        unsafe_allow_html=True
                    )
                st.divider()

                for _pi3,_pimg in enumerate(_cv_imgs):
                    _ptext=_cv_blocks.get(_pi3+1,""); _psm=re.search(r"(\d+)/10",_ptext)
                    _pscore=int(_psm.group(1)) if _psm else 0
                    _pbc="#22c55e" if _pscore>=8 else ("#f59e0b" if _pscore>=6 else "#ef4444")
                    _pslbl="Отлично" if _pscore>=8 else ("Хорошо" if _pscore>=6 else "Слабо")
                    _s2=lambda pat,t=_ptext: re.search(pat,t)
                    _st=lambda m: m.group(1).strip().strip("*").strip() if m else ""
                    _ptyp  = _st(_s2(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)"))
                    _pstrg = _st(_s2(r"(?:[Сс]ильная\s+сторона|Strength)\s*[:\-]\s*(.{3,})"))
                    _pweak = _st(_s2(r"(?:[Сс]лабость|Weakness)\s*[:\-]\s*(.{3,})"))
                    _pact  = _st(_s2(r"(?:[Дд]ействие|Action)\s*[:\-]\s*(.{3,})"))
                    _pconv = _st(_s2(r"(?:[Кк]онверсия|Conversion)\s*[:\-]\s*(.{3,})"))
                    _pemot = _st(_s2(r"(?:[Ээ]моция|Emotion)\s*[:\-]\s*(.{3,})"))
                    if _pweak and any(x in _pweak.lower() for x in ["none","n/a","нет слабостей"]): _pweak=""
                    with st.container(border=True):
                        _pc1,_pc2=st.columns([1,2])
                        with _pc1: st.image(__import__("base64").b64decode(_pimg["b64"]),use_container_width=True)
                        with _pc2:
                            _phead = f"Фото #{_pi3+1}" + (f" — {_ptyp}" if _ptyp else "")
                            st.markdown(f"**{_phead}**")
                            if _pscore>0:
                                st.markdown(
                                    f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0">' +
                                    f'<div style="font-size:2rem;font-weight:800;color:{_pbc}">{_pscore}/10</div>' +
                                    f'<div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px">' +
                                    f'<div style="background:{_pbc};width:{_pscore*10}%;height:10px;border-radius:6px"></div></div>' +
                                    f'<div style="color:{_pbc};font-size:0.8rem;margin-top:2px">{_pslbl}</div></div></div>',
                                    unsafe_allow_html=True)
                            if _pstrg: st.success(f"✅ {_pstrg}")
                            if _pweak: st.warning(f"⚠️ {_pweak}")
                            if _pact:
                                with st.expander("🛠 Что делать"): st.markdown(f"→ {_pact}")
                            if _pconv:
                                with st.expander("💡 Конверсия"): st.info(f"🎯 {_pconv}")
                            if _pemot:
                                _ec2={"доверие":"#22c55e","trust":"#22c55e","желание":"#f59e0b","сомнение":"#ef4444","doubt":"#ef4444","любопытство":"#3b82f6","curiosity":"#3b82f6","безразличие":"#94a3b8"}.get(_pemot.split()[0].lower().rstrip("/:"), "#8b5cf6")
                                st.markdown(
                                    f'<div style="background:{_ec2}22;border-left:3px solid {_ec2};border-radius:6px;padding:8px 12px;margin-top:4px">' +
                                    f'<span style="font-size:0.8rem;font-weight:700;color:{_ec2}">😶 ЭМОЦИЯ: </span>' +
                                    f'<span style="font-size:0.82rem;color:#1e293b">{_pemot}</span></div>',
                                    unsafe_allow_html=True)
            else:
                st.info("👁️ Vision отключён — нажми 🧠 Анализ")
                for _rs in range(0,min(len(_cimgs),9),3):
                    _rc=st.columns(3)
                    for _ci2,_iu in enumerate(_cimgs[_rs:_rs+3]):
                        try:
                            _ri2=requests.get(_iu,timeout=10,headers={"User-Agent":"Mozilla/5.0"})
                            if _ri2.ok: _rc[_ci2].image(_ri2.content,caption=f"#{_rs+_ci2+1}",use_container_width=True)
                        except: pass
        else: st.warning("Нет фото")
    with tab_aplus:
        _cap2_urls=st.session_state.get(f"comp_aplus_urls_{cidx}",c.get("aplus_image_urls",[]))
        _cav_text=st.session_state.get(f"comp_aplus_vision_{cidx}","")
        _ac1,_ac2,_ac3=st.columns(3)
        _ac1.metric("A+","✅" if _ap2 else "❌"); _ac2.metric("Видео","✅" if _vid2 else "❌"); _ac3.metric("Баннеры",f"{len(_cap2_urls)}")
        st.divider()
        if not _cap2_urls: st.warning("❌ A+ баннеры не загружены")
        else:
            for _bi,_burl in enumerate(_cap2_urls[:8]):
                with st.container(border=True):
                    try: st.image(_burl,use_container_width=True)
                    except: st.caption(f"❌ {_burl[:50]}")
                    if _cav_text:
                        _cav_m=re.search(rf"APLUS_BLOCK_{_bi+1}\s*(.*?)(?=APLUS_BLOCK_\d+|$)",_cav_text,re.DOTALL)
                        if _cav_m:
                            _cblk=_cav_m.group(1).strip()
                            _bsc2=re.search(r"(?:Оценка|Score)\s*[:\-]\s*(\d+)",_cblk)
                            _bstr2=re.search(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.{3,})",_cblk)
                            _bwk2=re.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{3,})",_cblk)
                            if _bsc2: st.markdown(f"**{_bsc2.group(1)}/10**")
                            if _bstr2: st.success(f"✅ {_bstr2.group(1).strip()}")
                            if _bwk2: st.warning(f"⚠️ {_bwk2.group(1).strip()}")
    with tab_data:
        _da1,_da2=st.columns(2)
        _da1.metric("Цена",cprice); _da2.metric("Рейтинг",crating)
        _da1.metric("Отзывов",crev); _da2.metric("BSR",cbsr_s[:30])
        _da1.metric("Prime","Да" if _pr2 else "Нет"); _da2.metric("A+","Да" if _ap2 else "Нет")
        _da1.metric("Цветов",len(c.get("customization_options",{}).get("color",[])))
        _da2.metric("Размеров",len(c.get("customization_options",{}).get("size",[])))

# ══ Топ ниши ══════════════════════════════════════════════════════════════════
elif page == "🔥 Топ ниши":
    st.title("🔥 Топ ниши — AI-анализ лидеров")
    st.caption("Найди топ-продавцов в своей нише и узнай что делает их листинги лучшими")

    _our_title = od.get("title","") if od else ""
    _our_asin  = od.get("parent_asin","") if od else ""
    _mp_niche  = st.session_state.get("_marketplace","com")

    # Search query input
    _nc1, _nc2, _nc3 = st.columns([4,1,0.7])
    with _nc1:
        _niche_q = st.text_input(
            "🔍 Ниша / поисковый запрос",
            value=st.session_state.get("_niche_query_saved", " ".join(_our_title.split()[:4]) if _our_title else ""),
            placeholder="merino wool base layer men, hiking socks, outdoor jacket...",
            key="niche_query_input",
            label_visibility="collapsed"
        )
    # Autocomplete suggestions
    _cur_mp_ac = st.session_state.get("niche_mp_sel", st.session_state.get("_niche_mp","com"))
    if _niche_q and len(_niche_q) >= 3:
        _ac_suggestions = fetch_autocomplete(_niche_q, domain=_cur_mp_ac)
        if _ac_suggestions:
            st.markdown('<div style="font-size:0.72rem;color:#64748b;margin-bottom:3px">💡 Amazon подсказывает:</div>', unsafe_allow_html=True)
            _ac_cols = st.columns(min(5, len(_ac_suggestions)))
            for _aci, (_acc, _acq) in enumerate(zip(_ac_cols, _ac_suggestions[:5])):
                if _acc.button(_acq[:28]+"…" if len(_acq)>28 else _acq, key=f"ac_{_aci}", use_container_width=True):
                    st.session_state["_niche_query_saved"] = _acq
                    st.session_state["_niche_run_now"] = True
                    st.session_state.pop("_niche_results", None)
                    st.rerun()
    with _nc2:
        _run_niche = st.button("🔍 Найти топ", type="primary", use_container_width=True, key="btn_niche_search")
    with _nc3:
        if st.button("🗑️", use_container_width=True, key="btn_niche_clear", help="Сбросить результаты"):
            for _k in ["_niche_results","_niche_mp","_niche_query_saved","_niche_ai_report","_niche_mp_display"]:
                st.session_state.pop(_k, None)
            st.rerun()

    # Quick search buttons
    _niche_quick_by_mp = {
        "com": ["merino wool base layer men","outdoor hiking socks","merino wool t-shirt women","thermal underwear set","wool beanie hat outdoor","base layer women hiking"],
        "de":  ["merino wolle funktionsunterwäsche","merino unterhemd herren","outdoor socken wolle","thermounterwäsche herren set","wollmütze outdoor","merino baselayer damen"],
        "fr":  ["sous-vêtement merino homme","chaussettes randonnée laine","t-shirt merino femme","sous-couche thermique","bonnet laine outdoor","collants thermiques homme"],
        "it":  ["intimo merino uomo","calzini trekking lana","maglietta merino donna","intimo termico set","berretto lana outdoor","base layer donna"],
        "es":  ["ropa interior merino hombre","calcetines senderismo lana","camiseta merino mujer","ropa interior termica","gorro lana outdoor","base layer mujer"],
        "co.uk":["merino wool base layer men","hiking socks merino wool","merino t-shirt women","thermal underwear set","wool hat outdoor","base layer women"],
        "ca":  ["merino wool base layer men","hiking socks wool","merino t-shirt women","thermal underwear set","wool beanie outdoor","base layer women hiking"],
        "nl":  ["merino wol basislaag heren","wandelsokken wol","merino shirt dames","thermisch ondergoed set","wollen muts outdoor","basislaag dames"],
    }
    _niche_quick_queries = _niche_quick_by_mp.get(st.session_state.get("niche_mp_sel", st.session_state.get("_niche_mp","com")), _niche_quick_by_mp["com"])
    _current_query = st.session_state.get("_niche_query_saved","")
    st.markdown('<div style="font-size:0.75rem;color:#64748b;margin-bottom:6px">⚡ Быстрый поиск:</div>', unsafe_allow_html=True)
    _qbtn_cols = st.columns(len(_niche_quick_queries))
    for _qbi, (_qbc, _qbq) in enumerate(zip(_qbtn_cols, _niche_quick_queries)):
        _is_active = (_current_query == _qbq)
        _btn_label = ("✓ " if _is_active else "") + _qbq
        if _qbc.button(_btn_label, key=f"niche_quick_{_qbi}", use_container_width=True,
                       type="primary" if _is_active else "secondary"):
            st.session_state["_niche_query_saved"] = _qbq
            st.session_state["_niche_run_now"] = True
            st.session_state.pop("_niche_results", None)
            st.session_state.pop("_niche_ai_report", None)
            st.rerun()
    # Show active query badge
    if _current_query:
        st.markdown(
            f'<div style="font-size:0.75rem;margin-top:4px;color:#64748b">'
            f'🔍 Текущий запрос: <b style="color:#0f172a">{_current_query}</b></div>',
            unsafe_allow_html=True)

    _niche_mp_col1, _niche_mp_col2 = st.columns([2,4])
    with _niche_mp_col1:
        _mp_list = ["com","de","fr","it","es","co.uk","ca","nl"]
        _mp_default = st.session_state.get("_niche_mp", _mp_niche)
        _niche_mp = st.selectbox("Маркетплейс", _mp_list,
                                  index=_mp_list.index(_mp_default) if _mp_default in _mp_list else 0,
                                  key="niche_mp_sel")
    # Auto-clear results if marketplace changed since last search
    _last_searched_mp = st.session_state.get("_niche_mp", "com")
    if st.session_state.get("_niche_results") and _last_searched_mp != _niche_mp:
        for _k in ["_niche_results","_niche_ai_report"]:
            st.session_state.pop(_k, None)
        st.info(f"🔄 Маркетплейс изменён на {_niche_mp} — нажми 🔍 Найти топ")

    if st.session_state.pop("_niche_run_now", False):
        _run_niche = True
        _niche_q = st.session_state.get("_niche_query_saved", _niche_q)

    if _run_niche and _niche_q.strip():
        st.session_state["_niche_query_saved"] = _niche_q.strip()
        sd_key = st.secrets.get("SCRAPINGDOG_API_KEY","")
        if not sd_key:
            st.error("❌ SCRAPINGDOG_API_KEY не задан в Secrets")
        else:
            with st.spinner(f"🔍 Ищу топ листинги: '{_niche_q}'..."):
                try:
                    # Country codes per ScrapingDog docs
                    _country_map = {
                        "com":"us","co.uk":"gb","ca":"ca","de":"de","es":"es",
                        "fr":"fr","it":"it","co.jp":"jp","in":"in","cn":"cn",
                        "com.sg":"sg","com.mx":"mx","com.br":"br","nl":"nl",
                        "com.au":"au","com.tr":"tr","se":"se","pl":"pl",
                    }
                    _lang_map = {
                        "de":"de","fr":"fr","it":"it","es":"es","nl":"nl",
                        "se":"sv","pl":"pl","co.jp":"ja","in":"en","co.uk":"en",
                    }
                    _search_params = {
                        "api_key": sd_key,
                        "query": _niche_q,
                        "domain": _niche_mp,
                        "page": "1",
                        "country": _country_map.get(_niche_mp, "us"),
                    }
                    if _niche_mp in _lang_map:
                        _search_params["language"] = _lang_map[_niche_mp]
                    _search_r = requests.get(
                        "https://api.scrapingdog.com/amazon/search",
                        params=_search_params,
                        timeout=60
                    )
                    if _search_r.ok:
                        _search_data = _search_r.json()
                        _products = _search_data if isinstance(_search_data, list) else _search_data.get("products", _search_data.get("results", []))
                        if _products:
                            st.session_state["_niche_results"] = _products[:12]
                            st.session_state["_niche_mp"] = _niche_mp
                            st.session_state["_niche_mp_display"] = _niche_mp
                            st.success(f"✅ Найдено {len(_products[:12])} товаров")
                        else:
                            st.warning("Результаты не найдены — попробуй другой запрос")
                    else:
                        st.error(f"❌ ScrapingDog: {_search_r.status_code} — {_search_r.text[:200]}")
                except Exception as _ne:
                    st.error(f"❌ Ошибка: {_ne}")

    if st.session_state.get("_niche_results"):
        _niche_products = st.session_state["_niche_results"]
        # Always use current selectbox value for display
        _niche_mp_saved = _niche_mp  # use current selectbox value directly
        _mp_flags = {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸","nl":"🇳🇱"}

        # ── Canvas-style metrics ──────────────────────────────────────────────
        _total_products = len(_niche_products)
        _sponsored_count = sum(1 for p in _niche_products if p.get("sponsored"))
        _organic_count = _total_products - _sponsored_count
        # Price stats
        _prices = []
        for _p in _niche_products:
            try:
                _pv = str(_p.get("price","") or _p.get("current_price","") or "")
                _pv = _pv.replace("$","").replace("€","").replace("£","").replace(",",".").strip().split()[0]
                _prices.append(float(_pv))
            except: pass
        # Ratings stats
        _ratings = []
        for _p in _niche_products:
            try: _ratings.append(float(str(_p.get("rating","") or _p.get("stars","") or 0).split()[0]))
            except: pass
        # Reviews stats
        _revs = []
        for _p in _niche_products:
            try:
                _rv = str(_p.get("total_reviews","") or _p.get("reviews","") or _p.get("reviews_count","") or 0)
                _rv = _rv.replace(",","").replace("K","000").replace("k","000").strip().split()[0]
                _revs.append(int(_rv))
            except: pass

        _avg_price = f"${sum(_prices)/len(_prices):.2f}" if _prices else "—"
        _price_range = f"${min(_prices):.0f}–${max(_prices):.0f}" if len(_prices)>=2 else _avg_price
        _avg_rat = f"{sum(_ratings)/len(_ratings):.1f}★" if _ratings else "—"
        _avg_rev = f"{int(sum(_revs)/len(_revs)):,}" if _revs else "—"
        _competition = "🔴 Высокая" if _organic_count > 8 else ("🟡 Средняя" if _organic_count > 4 else "🟢 Низкая")

        st.subheader(f"📋 Топ листинги {_mp_flags.get(_niche_mp_saved,'')} amazon.{_niche_mp_saved}")

        # Canvas-style metric cards
        _m1, _m2, _m3, _m4, _m5 = st.columns(5)
        _m1.markdown(
            f'<div style="background:#1e293b;border-radius:10px;padding:12px;text-align:center;border-top:3px solid #3b82f6">'
            f'<div style="font-size:0.65rem;color:#64748b;margin-bottom:4px">📦 КОНКУРЕНТОВ</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:#3b82f6">{_organic_count}</div>'
            f'<div style="font-size:0.62rem;color:#64748b">{_competition}</div></div>',
            unsafe_allow_html=True)
        _m2.markdown(
            f'<div style="background:#1e293b;border-radius:10px;padding:12px;text-align:center;border-top:3px solid #f59e0b">'
            f'<div style="font-size:0.65rem;color:#64748b;margin-bottom:4px">💰 СРЕДНЯЯ ЦЕНА</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:#f59e0b">{_avg_price}</div>'
            f'<div style="font-size:0.62rem;color:#64748b">диапазон {_price_range}</div></div>',
            unsafe_allow_html=True)
        _m3.markdown(
            f'<div style="background:#1e293b;border-radius:10px;padding:12px;text-align:center;border-top:3px solid #22c55e">'
            f'<div style="font-size:0.65rem;color:#64748b;margin-bottom:4px">⭐ СР. РЕЙТИНГ</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:#22c55e">{_avg_rat}</div>'
            f'<div style="font-size:0.62rem;color:#64748b">топ листингов</div></div>',
            unsafe_allow_html=True)
        _m4.markdown(
            f'<div style="background:#1e293b;border-radius:10px;padding:12px;text-align:center;border-top:3px solid #8b5cf6">'
            f'<div style="font-size:0.65rem;color:#64748b;margin-bottom:4px">💬 СР. ОТЗЫВОВ</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:#8b5cf6">{_avg_rev}</div>'
            f'<div style="font-size:0.62rem;color:#64748b">соц. доказательство</div></div>',
            unsafe_allow_html=True)
        _m5.markdown(
            f'<div style="background:#1e293b;border-radius:10px;padding:12px;text-align:center;border-top:3px solid #ef4444">'
            f'<div style="font-size:0.65rem;color:#64748b;margin-bottom:4px">🎯 SPONSORED</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:#ef4444">{_sponsored_count}</div>'
            f'<div style="font-size:0.62rem;color:#64748b">из {_total_products} мест</div></div>',
            unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        # Display grid of results
        for _ni in range(0, len(_niche_products), 3):
            _row_cols = st.columns(3)
            for _nj, (_nc, _np) in enumerate(zip(_row_cols, _niche_products[_ni:_ni+3])):
                if not _np: continue
                _np_asin  = _np.get("asin","")
                _np_title = _np.get("title","")[:60]
                _np_price = _np.get("price","") or _np.get("price_string","") or _np.get("current_price","")
                _np_rat   = str(_np.get("stars","") or _np.get("rating",""))
                _np_rev   = str(_np.get("total_reviews","") or _np.get("reviews","") or _np.get("reviews_count","") or _np.get("number_of_reviews",""))
                _np_img   = _np.get("image","") or _np.get("thumbnail","") or _np.get("img","")
                _np_spon  = _np.get("sponsored", False)
                _np_prime = _np.get("has_prime", False)
                _np_bs    = _np.get("is_best_seller", False)
                _np_ac    = _np.get("is_amazon_choice", False)
                _np_bought= _np.get("number_of_people_bought","")
                _np_coupon= _np.get("coupon_text","")
                _np_colors= len(_np.get("colors",[])) 
                _is_ours  = (_np_asin == _our_asin)
                _np_pos   = _np.get("absolute_position", _np.get("organic_position", _ni + _nj + 1))
                # Extract brand - first 1-2 words of title usually
                _np_brand = _np.get("brand","")
                if not _np_brand and _np_title:
                    _np_brand = _np_title.split()[0] if _np_title else ""

                # Estimate opportunity: lower reviews = easier to enter
                try: _np_rev_int = int(str(_np_rev).replace(",","").replace("K","000").replace("k","000").split()[0])
                except: _np_rev_int = 0
                _opp = "🟢 Вход лёгкий" if _np_rev_int < 200 else ("🟡 Средний" if _np_rev_int < 1000 else "🔴 Высокий порог")

                with _nc:
                    with st.container(border=True):
                        # Position badge
                        _pos_color = "#f59e0b" if _np_pos <= 3 else "#64748b"
                        st.markdown(
                            f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">'
                            f'<span style="background:{_pos_color};color:white;border-radius:4px;padding:1px 7px;font-size:0.68rem;font-weight:700">#{_np_pos}</span>'
                            f'{"<span style=\'font-size:0.65rem;color:#94a3b8;background:#1e293b;padding:1px 5px;border-radius:3px\'>Sponsored</span>" if _np_spon else ""}'
                            f'{"<span style=\'font-size:0.65rem;color:#3b82f6;background:#1e3a5f;padding:1px 5px;border-radius:3px\'>🔵 НАШ</span>" if _is_ours else ""}'
                            f'</div>',
                            unsafe_allow_html=True)
                        if _np_img:
                            st.markdown(
                                f'<img src="{_np_img}" style="width:100%;height:130px;object-fit:contain;background:#f8fafc;border-radius:6px">',
                                unsafe_allow_html=True)
                        st.markdown(
                            (f'<div style="font-size:0.68rem;font-weight:700;color:#6366f1;letter-spacing:0.06em;margin-top:6px;margin-bottom:2px">{_np_brand.upper()}</div>' if _np_brand else "") +
                            f'<div style="font-size:0.82rem;font-weight:700;color:{"#3b82f6" if _is_ours else "#0f172a"};line-height:1.4">{_np_title}</div>',
                            unsafe_allow_html=True)
                        # Badges row
                        _badge_parts = []
                        if _np_bs:  _badge_parts.append("🏆 Best Seller")
                        if _np_ac:  _badge_parts.append("✅ Amazon's Choice")
                        if _np_prime: _badge_parts.append("👑 Prime")
                        if _badge_parts:
                            st.markdown(
                                " ".join([f'<span style="background:#1e3a5f;color:#93c5fd;border-radius:3px;padding:1px 5px;font-size:0.62rem">{b}</span>' for b in _badge_parts]),
                                unsafe_allow_html=True)
                        # Metrics row
                        _info_parts = []
                        if _np_price: _info_parts.append(f"💰 {_np_price}")
                        if _np_coupon: _info_parts.append(f"🎟️ {_np_coupon}")
                        if _np_rat:   _info_parts.append(f"⭐ {_np_rat}")
                        if _np_rev:   _info_parts.append(f"({_np_rev})")
                        if _np_bought: _info_parts.append(f"🛒 {_np_bought}")
                        if _np_colors > 1: _info_parts.append(f"🎨 {_np_colors} цв.")
                        if _info_parts:
                            st.markdown(
                                f'<div style="font-size:0.78rem;color:#1e293b;font-weight:500;margin-top:4px;line-height:1.6">' +
                                " · ".join(_info_parts) +
                                '</div>',
                                unsafe_allow_html=True)
                        # Opportunity badge
                        st.markdown(
                            f'<div style="font-size:0.65rem;margin-top:3px">{_opp}</div>',
                            unsafe_allow_html=True)
                        if _np_asin:
                            _btn_col1, _btn_col2 = st.columns(2)
                            _btn_col1.markdown(
                                f'<a href="https://www.amazon.{_niche_mp_saved}/dp/{_np_asin}" target="_blank">'
                                f'<button style="width:100%;padding:4px;background:#334155;color:white;border:none;border-radius:4px;cursor:pointer;font-size:0.7rem">↗ Amazon</button></a>',
                                unsafe_allow_html=True)
                            if _btn_col2.button("🔍 Анализ", key=f"niche_analyze_{_ni}_{_nj}", use_container_width=True):
                                st.session_state["_niche_analyze_asin"] = _np_asin
                                st.session_state["_niche_analyze_mp"]   = _niche_mp_saved
                                st.rerun()

        # ── Canvas-style Charts ───────────────────────────────────────────
        st.divider()
        st.markdown("#### 📊 Карта ниши")
        _ch1, _ch2 = st.columns(2)

        with _ch1:
            # Price distribution bar chart
            import pandas as pd
            _chart_data = []
            for _idx_c, _p in enumerate(_niche_products[:10]):
                _t = (_p.get("title","") or "")[:25] + "..."
                _pr_str = str(_p.get("price","") or _p.get("price_string","") or _p.get("current_price","") or "0")
                try:
                    _pr_v = float(_pr_str.replace("$","").replace("€","").replace("£","").replace(",",".").strip().split()[0])
                except: _pr_v = 0
                _rv_str = str(_p.get("total_reviews","") or _p.get("reviews","") or _p.get("reviews_count","") or "0").replace("K","000").replace("k","000")
                try:
                    _rv_v = int(_rv_str.replace(",","").strip().split()[0])
                except: _rv_v = 0
                _chart_data.append({"Товар": f"#{_idx_c+1} {_t}", "Цена": _pr_v, "Отзывы": _rv_v,
                    "Рейтинг": float(str(_p.get("rating","") or _p.get("stars","") or 0).split()[0]) if str(_p.get("rating","") or 0) else 0})

            if _chart_data:
                _df = pd.DataFrame(_chart_data)
                _df_prices = _df[_df["Цена"] > 0][["Товар","Цена"]].set_index("Товар")
                if not _df_prices.empty:
                    st.caption("💰 Ценовое распределение")
                    st.bar_chart(_df_prices, color="#f59e0b", height=200)

        with _ch2:
            if _chart_data:
                _df_revs = _df[_df["Отзывы"] > 0][["Товар","Отзывы"]].set_index("Товар")
                if not _df_revs.empty:
                    st.caption("💬 Отзывы — барьер входа")
                    st.bar_chart(_df_revs, color="#8b5cf6", height=200)

        # Opportunity scatter insight
        if _chart_data:
            _low_rev = [d for d in _chart_data if 0 < d["Отзывы"] < 300 and d["Цена"] > 0]
            _high_price_low_comp = []  # simplified
            if _low_rev:
                _opp_titles = ", ".join([d["Товар"][:20] for d in _low_rev[:3]])
                st.success(f"💡 **Opportunity:** {len(_low_rev)} товаров с <300 отзывами — лёгкий вход: {_opp_titles}")

        # ── AI Niche Intelligence ──────────────────────────────────────────
        st.divider()
        st.subheader("🧠 AI-анализ ниши")

        if st.button("🧠 Что делают лидеры — AI-отчёт", type="primary", key="btn_niche_ai", help="AI анализирует топ-6 листингов ниши: паттерны лидеров, топ keywords, что менять чтобы попасть в топ-3."):
            with st.spinner("🧠 AI анализирует топ-листинги..."):
                _niche_summary = []
                for _np in _niche_products[:6]:
                    _np_t = _np.get("title","")
                    _np_r = str(_np.get("rating","") or _np.get("stars",""))
                    _np_rv = str(_np.get("reviews","") or _np.get("number_of_reviews",""))
                    _np_p  = _np.get("price","")
                    if _np_t:
                        _niche_summary.append(f"• {_np_t[:80]} | {_np_r}★ {_np_rv} отз. | {_np_p}")
                _niche_ctx = "\n".join(_niche_summary)
                _aud_ctx2 = st.session_state.get("target_audience","")
                _aud_line2 = f"\nЦелевая аудитория: {_aud_ctx2}" if _aud_ctx2 else ""

                _niche_ai_prompt = f"""Ты эксперт по Amazon нишевому анализу. Проанализируй топ-листинги ниши.

ЗАПРОС: {st.session_state.get("_niche_query_saved","")}
МАРКЕТПЛЕЙС: amazon.{_niche_mp_saved}{_aud_line2}

ТОП ЛИСТИНГИ:
{_niche_ctx}

НАШ ТОВАР: {_our_title}

Дай McKinsey-анализ:

**Что объединяет топ-листинги** (3 общих паттерна в title/позиционировании)
**Ценовой диапазон** ниши и где мы находимся
**Топ-3 ключевых слова** которые встречаются у лидеров
**Главное отличие лидеров** от среднего листинга
**Что нам нужно изменить** чтобы попасть в топ-3 этой ниши (конкретно)
**AI-вывод**: "В этой нише побеждает тот, кто [X]"

Ответь {'по-русски' if st.session_state.get('analysis_lang','ru')=='ru' else 'in English'}. Будь конкретен."""

                _niche_ai = ai_call("Amazon niche analyst. McKinsey-style.", _niche_ai_prompt, max_tokens=1000)
                st.session_state["_niche_ai_report"] = _niche_ai

        if st.session_state.get("_niche_ai_report"):
            st.markdown(
                f'<div style="background:#0f172a;border:1px solid #334155;border-radius:12px;'
                f'padding:18px 20px;line-height:1.7">'
                f'<div style="font-size:0.75rem;font-weight:700;color:#64748b;letter-spacing:0.08em;margin-bottom:10px">🧠 AI ОТЧЁТ ПО НИШЕ</div>'
                f'<div style="color:#e2e8f0;font-size:0.9rem">{st.session_state["_niche_ai_report"].replace(chr(10),"<br>").replace("**","<b>").replace("**","</b>")}</div>'
                f'</div>',
                unsafe_allow_html=True
            )

        # ── Quick analyze specific product ─────────────────────────────────
        if st.session_state.get("_niche_analyze_asin"):
            _an_asin = st.session_state.pop("_niche_analyze_asin")
            _an_mp   = st.session_state.pop("_niche_analyze_mp","com")
            with st.spinner(f"🔍 Анализирую {_an_asin}..."):
                _an_data, _an_urls = scrapingdog_product(_an_asin, lambda m: None, domain=_an_mp)
                if _an_data:
                    _an_ai = analyze_text(_an_data, [], "", _an_asin, lambda m: None,
                                          lang=st.session_state.get("analysis_lang","ru"), is_competitor=True)
                    st.session_state[f"_niche_quick_{_an_asin}"] = {
                        "data": _an_data, "ai": _an_ai, "asin": _an_asin
                    }

        # Show quick analysis results
        for _k in [k for k in st.session_state if k.startswith("_niche_quick_")]:
            _qa = st.session_state[_k]
            _qa_overall = pct(_qa["ai"].get("overall_score",0))
            _qa_c = "#22c55e" if _qa_overall>=75 else ("#f59e0b" if _qa_overall>=50 else "#ef4444")
            with st.expander(f"🔍 {_qa['asin']} — Overall: {_qa_overall}%", expanded=True):
                _qa_cols = st.columns(5)
                for _col, (_lbl, _key) in zip(_qa_cols, [("Title","title_score"),("Bullets","bullets_score"),("Фото","images_score"),("A+","aplus_score"),("Overall","overall_score")]):
                    _v = pct(_qa["ai"].get(_key,0))
                    _vc = "#22c55e" if _v>=75 else ("#f59e0b" if _v>=50 else "#ef4444")
                    _col.markdown(f'<div style="text-align:center"><div style="font-size:1.2rem;font-weight:800;color:{_vc}">{_v}%</div><div style="font-size:0.7rem;color:#64748b">{_lbl}</div></div>', unsafe_allow_html=True)
                st.markdown(f"**Title:** {_qa['data'].get('title','')[:100]}")
                _prio = _qa["ai"].get("priority_improvements",[])
                if _prio:
                    st.markdown("**Топ проблемы:**")
                    for _p in _prio[:3]: st.caption(f"• {_p}")

# ══ Mobile Score ══════════════════════════════════════════════════════════════
elif page == "📱 Mobile Score":
    st.title("📱 Mobile Score")
    if not od or not od.get("title"):
        st.info("ℹ️ Эта страница доступна только при анализе **нашего листинга**. Добавь URL в поле 🔵 НАШ листинг и перезапусти.")
        st.stop()

    # ── Audience Score ────────────────────────────────────────────────────────
    with st.expander("👥 AI Audience Score — оценка фото для вашей ЦА", expanded=False):
        st.caption("AI оценивает каждое фото с точки зрения вашей целевой аудитории")
        _aud_col1, _aud_col2 = st.columns(2)
        with _aud_col1:
            _aud_age = st.text_input("👤 Возраст и пол", placeholder="Мужчина 30-45 лет", key="aud_age")
            _aud_lifestyle = st.text_input("🏃 Образ жизни", placeholder="Активный, outdoor, hiking, путешествия", key="aud_lifestyle")
        with _aud_col2:
            _aud_income = st.selectbox("💰 Доход", ["Средний", "Выше среднего", "Высокий", "Любой"], key="aud_income")
            _aud_geo = st.text_input("🌍 География", placeholder="Германия, EU", key="aud_geo")
        _aud_extra = st.text_area("💬 Доп. инфо о ЦА", placeholder="Ценит качество, экологичность, покупает онлайн, читает отзывы...", height=68, key="aud_extra")

        _imgs_for_aud = st.session_state.get("images", [])
        if not _imgs_for_aud:
            st.info("Запусти анализ чтобы загрузить фото листинга")
        elif st.button("👥 Оценить все фото для ЦА", type="primary", key="btn_audience_score"):
            if not (_aud_age or _aud_lifestyle):
                st.warning("Заполни хотя бы возраст/пол и образ жизни")
            else:
                _aud_profile = f"""
ЦЕЛЕВАЯ АУДИТОРИЯ:
- Возраст/пол: {_aud_age}
- Образ жизни: {_aud_lifestyle}
- Доход: {_aud_income}
- География: {_aud_geo}
- Дополнительно: {_aud_extra}
""".strip()
                _aud_results = []
                _vprog = st.progress(0, "👥 Оцениваю фото для ЦА...")
                for _ai_idx, _aimg in enumerate(_imgs_for_aud[:6]):
                    _vprog.progress(int((_ai_idx+1)/min(len(_imgs_for_aud),6)*100), f"Фото {_ai_idx+1}...")
                    try:
                        _ab64 = _aimg.get("b64","") if isinstance(_aimg, dict) else _aimg
                        _amt = _aimg.get("media_type","image/jpeg") if isinstance(_aimg, dict) else "image/jpeg"
                        # Photo #1 = main Amazon image (white bg required)
                        _is_main_photo = (_ai_idx == 0)
                        _photo_context = """ВАЖНО: Это ГЛАВНОЕ фото листинга (#1).
По правилам Amazon главное фото ОБЯЗАНО быть на чистом белом фоне (RGB 255,255,255).
НЕ рекомендуй менять белый фон на lifestyle — это нарушит правила Amazon и приведёт к suppression.
Оценивай только: позу модели, читаемость товара, посадку, выражение лица, динамику движения, 
соответствие модели ЦА. Рекомендуй улучшения в рамках белого фона (поза, движение, угол съёмки).""" if _is_main_photo else """Это дополнительное фото #{} листинга. Оценивай свободно — фон, контекст, lifestyle.""".format(_ai_idx+1)

                        _aprompt = f"""Ты маркетолог-эксперт по Amazon. Оцени это фото товара с точки зрения целевой аудитории.

{_aud_profile}

ПРОДУКТ: {od.get('title','')}

{_photo_context}

Оцени по шкале 0-100% насколько это фото убедительно для данной аудитории.

Ответь строго в формате:
SCORE: [0-100]%
ЭМОЦИЯ_ЦА: [что чувствует покупатель глядя на это фото]
СООТВЕТСТВУЕТ: [что на фото совпадает с интересами ЦА]
НЕ СООТВЕТСТВУЕТ: [что не совпадает или отталкивает ЦА]
РЕКОМЕНДАЦИЯ: [одно конкретное улучшение — поза/движение/угол для #1, сцена/контекст для остальных]"""
                        _aud_resp = anthropic_vision(
                            content_blocks=[
                                {"type":"image","source":{"type":"base64","media_type":_amt,"data":_ab64}},
                                {"type":"text","text":_aprompt}
                            ],
                            max_tokens=500,
                            system="Ты маркетолог-эксперт по Amazon. Отвечай строго по формату."
                        )
                        _aud_text = _aud_resp.get("content",[{}])[0].get("text","") if isinstance(_aud_resp, dict) else str(_aud_resp)
                        _aud_results.append({"idx": _ai_idx+1, "text": _aud_text, "b64": _ab64, "mt": _amt})
                    except Exception as _ae:
                        _aud_results.append({"idx": _ai_idx+1, "text": f"Ошибка: {_ae}", "b64": "", "mt": ""})
                _vprog.progress(100, "✅ Готово!")
                st.session_state["_aud_results"] = _aud_results

        # Show results
        if st.session_state.get("_aud_results"):
            st.markdown("---")
            for _ar in st.session_state["_aud_results"]:
                _arc1, _arc2 = st.columns([1, 3])
                with _arc1:
                    if _ar.get("b64"):
                        st.image(f"data:{_ar['mt']};base64,{_ar['b64']}", use_container_width=True)
                    st.caption(f"Фото #{_ar['idx']}")
                with _arc2:
                    _txt = _ar["text"]
                    # Extract score for color
                    import re as _re2
                    _sm = _re2.search(r'SCORE:\s*(\d+)%', _txt)
                    _sc = int(_sm.group(1)) if _sm else 0
                    _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else "#ef4444")
                    _sc_l = "✅ Отлично для ЦА" if _sc>=75 else ("🟡 Средне" if _sc>=50 else "🔴 Слабо для ЦА")
                    st.markdown(
                        f'<div style="background:#0f172a;border-left:4px solid {_sc_c};border-radius:8px;padding:12px 14px">' +
                        f'<div style="font-size:1.4rem;font-weight:800;color:{_sc_c}">{_sc}% <span style="font-size:0.8rem">{_sc_l}</span></div>' +
                        f'<div style="font-size:0.82rem;color:#e2e8f0;margin-top:8px;white-space:pre-line">{_txt.replace("SCORE:","").replace(f"{_sc}%","",1).strip()}</div>' +
                        f'</div>',
                        unsafe_allow_html=True)
            # ── Summary & Ranking ────────────────────────────────────────────
            import re as _re3
            _aud_scored = []
            for _ar2 in st.session_state["_aud_results"]:
                _sm2 = _re3.search(r'SCORE:\s*(\d+)%', _ar2["text"])
                _sc2 = int(_sm2.group(1)) if _sm2 else 0
                _aud_scored.append((_sc2, _ar2["idx"], _ar2["text"]))
            _aud_scored_sorted = sorted(_aud_scored, reverse=True)

            st.markdown("---")
            st.markdown("### 🏆 Итоговый рейтинг фото для вашей ЦА")

            # Detect which photo has white background (likely photo #1 from listing)
            # Main image (#1) MUST be white bg per Amazon rules — pin it to position #1
            _main_photo_idx = 1  # always keep original #1 as main (white bg rule)
            _rest_sorted = [(sc,idx,txt) for sc,idx,txt in _aud_scored_sorted if idx != _main_photo_idx]
            _best_sc, _best_idx, _best_txt = _aud_scored_sorted[0]

            st.markdown(
                '<div style="background:#1a1a0a;border:2px solid #f59e0b;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                '<div style="font-size:0.75rem;font-weight:700;color:#f59e0b">⚠️ ПРАВИЛО AMAZON</div>' +
                '<div style="font-size:0.82rem;color:#fde68a;margin-top:4px">Главное фото (#1) <b>всегда белый фон</b> — это обязательное требование Amazon. Оно не участвует в ранжировании по ЦА.</div>' +
                '</div>', unsafe_allow_html=True)

            # Best lifestyle photo
            if _rest_sorted:
                _bsc, _bidx, _btxt = _rest_sorted[0]
                _brm = _re3.search(r'РЕКОМЕНДАЦИЯ:(.+?)(?=\n[А-Я]|$)', _btxt, _re3.DOTALL)
                _brec = _brm.group(1).strip()[:200] if _brm else ""
                st.markdown(
                    f'<div style="background:#0f3a1a;border:2px solid #22c55e;border-radius:12px;padding:12px 16px;margin-bottom:10px">' +
                    f'<div style="font-size:0.9rem;font-weight:800;color:#22c55e">🥇 Лучшее lifestyle фото для ЦА — Фото #{_bidx} ({_bsc}%)</div>' +
                    (f'<div style="font-size:0.78rem;color:#86efac;margin-top:4px">{_brec}</div>' if _brec else "") +
                    f'</div>', unsafe_allow_html=True)

            # Ordered ranking table
            _rank_html = '<div style="background:#0f172a;border-radius:10px;padding:12px 16px">' + '<div style="font-size:0.75rem;font-weight:700;color:#64748b;margin-bottom:8px">РЕКОМЕНДУЕМЫЙ ПОРЯДОК ФОТО</div>'
            # Position 1 = always original main photo (white bg)
            _rank_html += (
                '<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                '<span style="font-size:0.8rem;color:#94a3b8">📸 Главное (#1) — Amazon rule</span>' +
                f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #1</span>' +
                '<span style="font-size:0.75rem;color:#f59e0b">🤍 Белый фон</span>' +
                '</div>'
            )
            _position_labels2 = ["🥇 Второе (#2) — лучшее lifestyle", "🥈 Третье (#3)", "🥉 Четвёртое (#4)", "4️⃣ Пятое (#5)", "5️⃣ Шестое (#6)"]
            for _pi, (_psc, _pidx, _ptxt) in enumerate(_rest_sorted):
                _pc = "#22c55e" if _psc>=75 else ("#f59e0b" if _psc>=50 else "#ef4444")
                _plbl = _position_labels2[_pi] if _pi < len(_position_labels2) else f"#{_pi+2}"
                _rank_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1e293b">' +
                    f'<span style="font-size:0.8rem;color:#94a3b8">{_plbl}</span>' +
                    f'<span style="font-size:0.82rem;color:#e2e8f0">Фото #{_pidx}</span>' +
                    f'<span style="font-size:0.85rem;font-weight:700;color:{_pc}">{_psc}%</span>' +
                    f'</div>'
                )
            _rank_html += '</div>'
            st.markdown(_rank_html, unsafe_allow_html=True)

            # AI summary prompt
            if st.button("🧠 AI — сводный план улучшений", key="btn_aud_summary"):
                with st.spinner("🧠 Генерирую план..."):
                    _all_reviews = "\n\n".join([
                        f"ФОТО #{_ar2['idx']} ({_ar2['text'][:300]})"
                        for _ar2 in st.session_state["_aud_results"]
                    ])
                    _sum_prompt = f"""На основе оценки {len(st.session_state["_aud_results"])} фото листинга для ЦА ({_aud_age}, {_aud_lifestyle}, {_aud_income}):

{_all_reviews}

Дай КРАТКИЙ план из 3-5 конкретных действий чтобы улучшить весь набор фото для этой ЦА.
Формат: пронумерованный список, каждый пункт = одно действие + ожидаемый эффект (+X% конверсии).
Язык: {'русский' if st.session_state.get('analysis_lang','ru')=='ru' else 'английский'}."""
                    _sum_r = ai_call("Amazon photo strategist", _sum_prompt, max_tokens=600)
                    st.session_state["_aud_summary"] = _sum_r

            if st.session_state.get("_aud_summary"):
                st.markdown(
                    '<div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-top:8px">' +
                    '<div style="font-size:0.72rem;font-weight:700;color:#64748b;margin-bottom:8px">📋 ПЛАН УЛУЧШЕНИЙ ФОТО ДЛЯ ЦА</div>' +
                    f'<div style="font-size:0.85rem;color:#e2e8f0;line-height:1.7">{st.session_state["_aud_summary"].replace(chr(10),"<br>")}</div></div>',
                    unsafe_allow_html=True)

            if st.button("🗑️ Очистить", key="clear_aud"):
                st.session_state.pop("_aud_results", None)
                st.session_state.pop("_aud_summary", None)
                st.rerun()

    # ── Claid AI Photo Generator ──────────────────────────────────────────────
    _claid_key = st.secrets.get("GEMINI_API_KEY","") or st.secrets.get("CLAID_API_KEY","")
    if _claid_key:  # GEMINI_API_KEY
        with st.expander("🎨 AI-генерация lifestyle фото — Gemini Nano Banana Pro", expanded=False):
            st.caption("Выбери любое фото из анализа или загрузи своё → AI создаст lifestyle сцену")
            _claid_col1, _claid_col2 = st.columns([3,2])
            with _claid_col1:
                _scene_options = {
                "🤍 Белый фон (Amazon Main Image)": "pure white background, professional studio product photography, soft shadow, white backdrop, e-commerce",
                "🏔️ Hiking / Outdoor горы": "on a rugged mountain trail, scattered rocks, sunlight through pine trees, rich earthy colors, warm light, professional product photography",
                "🏠 Уютный дом / Interior": "cozy modern home interior, warm lighting, wooden furniture, lifestyle product photography",
                "🏋️ Gym / Фитнес": "modern gym interior, fitness lifestyle, dynamic lighting, professional product photography",
                "❄️ Зима / Snow": "snowy mountain landscape, cold winter day, soft blue light, outdoor winter lifestyle",
                "🌊 Лето / Summer beach": "sunny summer day, outdoor adventure, bright natural light, lifestyle product photography",
                "🌿 Nature / Природа": "lush green forest path, dappled sunlight, natural outdoor setting, professional photography",
                "💼 Офис / Professional": "modern office setting, professional lifestyle, clean minimalist background",
            }
            _scene_label = st.selectbox("Сцена", list(_scene_options.keys()), key="claid_scene_sel")
            _claid_scene = _scene_options[_scene_label]
            if "белый" in _scene_label.lower() or "Amazon" in _scene_label:
                st.caption("✅ Подходит для главного фото Amazon (#1) — чистый белый фон")
            else:
                st.caption("📸 Lifestyle сцена — для фото #2-7 в листинге")

            # Photos from analysis
            _analysis_imgs = st.session_state.get("images", [])
            _img_b64_claid = None
            _img_mt_claid = "image/jpeg"

            if _analysis_imgs:
                st.markdown("**📸 Выбери фото из анализа:**")
                _img_cols = st.columns(min(len(_analysis_imgs), 5))
                for _pi, (_pc, _pimg) in enumerate(zip(_img_cols, _analysis_imgs[:5])):
                    with _pc:
                        try:
                            _pb64 = _pimg.get("b64","") if isinstance(_pimg, dict) else _pimg
                            _pmt = _pimg.get("media_type","image/jpeg") if isinstance(_pimg, dict) else "image/jpeg"
                            st.image(f"data:{_pmt};base64,{_pb64}", use_container_width=True)
                            if st.button(f"✓ Фото {_pi+1}", key=f"claid_pick_{_pi}", use_container_width=True):
                                st.session_state["_claid_picked_b64"] = _pb64
                                st.session_state["_claid_picked_mt"] = _pmt
                                st.session_state["_claid_picked_idx"] = _pi+1
                                st.rerun()
                        except: pass

                if st.session_state.get("_claid_picked_b64"):
                    _img_b64_claid = st.session_state["_claid_picked_b64"]
                    _img_mt_claid = st.session_state.get("_claid_picked_mt","image/jpeg")
                    st.success(f"✅ Выбрано фото #{st.session_state.get('_claid_picked_idx',1)}")

            # Or upload new
            with _claid_col2:
                st.markdown("**или загрузи своё:**")
                _claid_upload = st.file_uploader("📤 PNG/JPG", type=["png","jpg","jpeg"], key="claid_upload", label_visibility="collapsed")
                if _claid_upload:
                    import base64 as _b64c
                    _img_bytes = _claid_upload.read()
                    _img_b64_claid = _b64c.b64encode(_img_bytes).decode()
                    _img_mt_claid = "image/jpeg" if _claid_upload.name.lower().endswith(("jpg","jpeg")) else "image/png"
                    st.image(_img_bytes, width=120)

            # Показываем рекомендацию из Vision анализа для выбранного фото
            _picked_idx = st.session_state.get("_claid_picked_idx", 0)
            if _picked_idx and st.session_state.get("vision"):
                import re as _re_v
                _v_text = st.session_state.get("vision","")
                _blk_m = _re_v.search(rf"PHOTO_BLOCK_{_picked_idx}\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", _v_text, _re_v.DOTALL)
                if _blk_m:
                    _blk = _blk_m.group(1)
                    _act_m = _re_v.search(r"(?:Действие|Action)\s*[:\-]\s*(.{10,})", _blk)
                    _wk_m  = _re_v.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{10,})", _blk)
                    if _act_m or _wk_m:
                        st.markdown("**🤖 AI рекомендует исправить:**")
                        if _wk_m:  st.warning(f"⚠️ {_wk_m.group(1).strip()[:150]}")
                        if _act_m:
                            _ai_action = _act_m.group(1).strip()[:200]
                            st.info(f"→ {_ai_action}")
                            # Авто-подставляем рекомендацию как сцену
                            if st.checkbox("🎯 Использовать рекомендацию AI как сцену", value=True, key="use_ai_scene"):
                                _claid_scene = _ai_action

            if _img_b64_claid:
                if st.button("🎨 Генерировать улучшенное фото", type="primary", key="btn_claid_gen"):
                    with st.spinner(f"🎨 Gemini генерирует..."):
                        _urls, _err = claid_generate_lifestyle(_img_b64_claid, scene=_claid_scene, media_type=_img_mt_claid)
                        if _err:
                            st.error(f"❌ {_err}")
                        elif _urls:
                            st.session_state["_claid_results"] = _urls
                            st.success(f"✅ {len(_urls)} фото готово!")
                        else:
                            st.warning("Фото не получены — попробуй ещё раз")

            if st.session_state.get("_claid_results"):
                _cr = st.session_state["_claid_results"]
                st.markdown(f"**🖼️ Результат ({len(_cr)} фото):**")
                _rc = st.columns(min(len(_cr), 4))
                for _i, (_rcol, _rimg) in enumerate(zip(_rc, _cr)):
                    with _rcol:
                        if isinstance(_rimg, dict) and _rimg.get("b64"):
                            import base64 as _b64dl
                            _rb64=_rimg["b64"]; _rmt=_rimg.get("mt","image/jpeg")
                            st.image(f"data:{_rmt};base64,{_rb64}", use_container_width=True)
                            st.download_button(f"⬇️ #{_i+1}", _b64dl.b64decode(_rb64), f"lifestyle_{_i+1}.jpg", "image/jpeg", key=f"dl_img_{_i}", use_container_width=True)
                        elif isinstance(_rimg, str):
                            st.image(_rimg, use_container_width=True)

                if st.button("🗑️ Очистить", key="claid_clear"):
                    st.session_state.pop("_claid_results", None)
                    st.session_state.pop("_claid_picked_b64", None)
                    st.rerun()

    st.caption("70% покупок Amazon — с мобильного. Как выглядит твой листинг на смартфоне?")

    _title   = od.get("title","") if od else ""
    _bullets = od.get("feature_bullets",[]) if od else []
    _price   = od.get("price","") if od else ""
    _rating  = od.get("average_rating","") if od else ""
    _reviews = od.get("product_information",{}).get("Customer Reviews",{}).get("ratings_count","") if od else ""
    _has_ap  = bool(od.get("aplus") if od else False)
    _has_vid = int(od.get("number_of_videos",0) or 0) > 0 if od else False
    _imgs    = st.session_state.get("images",[])
    _tlen    = len(_title)

    # ── Mobile scoring ────────────────────────────────────────────────────────
    # Title: on mobile only ~80 chars visible in search, ~120 on PDP
    _mob_title_search = min(100, int((_tlen / 80) * 100)) if _tlen <= 80 else max(0, 100 - (_tlen-80)*2)
    _mob_title_pdp    = 100 if _tlen <= 120 else max(50, 100 - (_tlen-120)*3)
    _mob_title_score  = int(_mob_title_search * 0.4 + _mob_title_pdp * 0.6)

    # Bullets: mobile shows only first 3, collapsed
    _mob_bullets_score = 100 if len(_bullets)>=1 else 0
    # Are first 3 bullets strong? (have ":" format and benefit)
    _first3 = _bullets[:3]
    _has_format = sum(1 for b in _first3 if ":" in b)
    _mob_bullets_score = min(100, 40 + _has_format * 20)

    # Main image: most important on mobile - full screen
    _mob_img_score = 0
    if _imgs:
        # Check first image from vision analysis
        import re as _re2
        _v_text = st.session_state.get("vision","")
        _first_block = ""
        if _v_text:
            _m = _re2.search(r"PHOTO_BLOCK_1\s*(.*?)(?=PHOTO_BLOCK_2|$)", _v_text, _re2.DOTALL)
            if _m: _first_block = _m.group(1)
        _first_score = 0
        if _first_block:
            _sm = _re2.search(r"(\d+)/10", _first_block)
            if _sm: _first_score = int(_sm.group(1)) * 10
        _mob_img_score = _first_score if _first_score else 60

    # Price visibility on mobile
    _mob_price_score = 80 if _price else 20

    # A+ on mobile: renders but below fold, less impact
    _mob_aplus_score = 70 if _has_ap else 30

    # Video: autoplays on mobile = huge conversion boost
    _mob_video_score = 95 if _has_vid else 40

    # Overall mobile score
    _mob_overall = int(
        _mob_title_score  * 0.25 +
        _mob_bullets_score* 0.20 +
        _mob_img_score    * 0.30 +
        _mob_price_score  * 0.05 +
        _mob_aplus_score  * 0.10 +
        _mob_video_score  * 0.10
    )
    _mob_c = "#22c55e" if _mob_overall>=75 else ("#f59e0b" if _mob_overall>=50 else "#ef4444")
    _mob_label = (
        "🟢 Отличная мобильная конверсия" if _mob_overall>=75 else
        "🟡 Средняя — есть потери на мобиле" if _mob_overall>=50 else
        "🔴 Высокие потери мобильных покупателей"
    )

    # ── Header ────────────────────────────────────────────────────────────────
    _h1col, _h2col = st.columns([3,1])
    with _h1col:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#0f172a,#1e293b);border-radius:14px;padding:20px 24px">
  <div style="font-size:1rem;font-weight:700;color:{_mob_c};margin-bottom:6px">{_mob_label}</div>
  <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">
    {"".join([
        f'<div style="background:#1e293b;border-radius:6px;padding:6px 12px;text-align:center;min-width:80px;border-top:2px solid {"#22c55e" if v>=75 else ("#f59e0b" if v>=50 else "#ef4444")}">' +
        f'<div style="font-size:1rem;font-weight:800;color:{"#22c55e" if v>=75 else ("#f59e0b" if v>=50 else "#ef4444")}">{v}%</div>' +
        f'<div style="font-size:0.62rem;color:#64748b">{l}</div></div>'
        for l,v in [("Title",_mob_title_score),("Bullets",_mob_bullets_score),("Фото",_mob_img_score),("A+",_mob_aplus_score),("Видео",_mob_video_score)]
    ])}
  </div>
</div>""", unsafe_allow_html=True)
    with _h2col:
        st.markdown(f"""
<div style="background:#0f172a;border-radius:14px;padding:20px;text-align:center;height:100%">
  <div style="font-size:3rem;font-weight:800;color:{_mob_c};line-height:1">{_mob_overall}%</div>
  <div style="font-size:0.75rem;color:#64748b;margin-top:4px">Mobile Score</div>
  <div style="background:rgba(255,255,255,0.08);border-radius:6px;height:6px;margin-top:10px">
    <div style="background:{_mob_c};width:{_mob_overall}%;height:6px;border-radius:6px"></div>
  </div>
</div>""", unsafe_allow_html=True)

    st.divider()

    # ── Mobile Mockup ─────────────────────────────────────────────────────────
    _m1col, _m2col = st.columns([1, 1])

    with _m1col:
        st.markdown("#### 🔍 Поисковая выдача (мобиль)")
        st.caption("Так покупатель видит твой товар в поиске Amazon")

        _title_search = _title[:80] + ("…" if _tlen > 80 else "")
        _title_search_c = "#ef4444" if _tlen > 80 else "#22c55e"
        _main_img_html = ""
        if _imgs:
            try:
                import base64 as _b64
                _img_data = _b64.b64decode(_imgs[0]["b64"])
                _img_b64_str = _b64.b64encode(_img_data).decode()
                _main_img_html = f'<img src="data:image/jpeg;base64,{_img_b64_str}" style="width:100%;height:160px;object-fit:contain;background:#fff;border-radius:6px">'
            except: _main_img_html = '<div style="width:100%;height:160px;background:#f1f5f9;border-radius:6px;display:flex;align-items:center;justify-content:center;color:#94a3b8">нет фото</div>'

        st.markdown(f"""
<div style="background:#fff;border-radius:12px;padding:12px;border:1px solid #e2e8f0;max-width:320px">
  {_main_img_html}
  <div style="margin-top:8px">
    <div style="font-size:0.78rem;color:#0f1111;line-height:1.3;font-weight:500">{_title_search}</div>
    <div style="margin-top:4px">
      <span style="color:{_title_search_c};font-size:0.65rem">{"⚠️ " + str(_tlen) + " симв. — обрезается" if _tlen>80 else "✅ " + str(_tlen) + " симв. — OK"}</span>
    </div>
    <div style="display:flex;align-items:center;gap:6px;margin-top:6px">
      <span style="color:#f59e0b;font-size:0.8rem">{("★" * min(5,int(float(str(_rating or "0").split()[0]) + 0.5))) if str(_rating or "0").split()[0].replace(".","").isdigit() else "★★★★"}</span>
      <span style="font-size:0.72rem;color:#007185">{_reviews}</span>
    </div>
    <div style="font-size:1rem;font-weight:700;color:#0f1111;margin-top:4px">{_price}</div>
    <div style="background:#ffd814;border-radius:4px;padding:4px 8px;font-size:0.72rem;font-weight:700;color:#0f1111;margin-top:6px;display:inline-block">Add to Cart</div>
  </div>
</div>""", unsafe_allow_html=True)

    with _m2col:
        st.markdown("#### 📄 Страница товара (мобиль)")
        st.caption("Первый экран без скролла — решает 80% покупок")

        _title_pdp = _title[:120] + ("…" if _tlen > 120 else "")
        _title_pdp_c = "#ef4444" if _tlen > 120 else "#22c55e"
        _b1 = _bullets[0][:90] + "…" if _bullets and len(_bullets[0])>90 else (_bullets[0] if _bullets else "")
        _b2 = _bullets[1][:90] + "…" if len(_bullets)>1 and len(_bullets[1])>90 else (_bullets[1] if len(_bullets)>1 else "")
        _b3 = _bullets[2][:90] + "…" if len(_bullets)>2 and len(_bullets[2])>90 else (_bullets[2] if len(_bullets)>2 else "")

        st.markdown(f"""
<div style="background:#fff;border-radius:12px;padding:12px;border:1px solid #e2e8f0;max-width:320px">
  {_main_img_html if _imgs else ""}
  <div style="margin-top:8px">
    <div style="font-size:0.75rem;color:#0f1111;line-height:1.3;font-weight:500">{_title_pdp}</div>
    <div style="font-size:0.62rem;color:{_title_pdp_c};margin-top:2px">{"⚠️ обрезается" if _tlen>120 else "✅ полный title"}</div>
    <div style="display:flex;align-items:center;gap:4px;margin-top:4px">
      <span style="color:#f59e0b;font-size:0.75rem">★ {_rating}</span>
      <span style="font-size:0.68rem;color:#007185">{_reviews} отзывов</span>
    </div>
    <div style="font-size:1.1rem;font-weight:700;color:#0f1111;margin-top:4px">{_price}</div>
    <div style="background:#ffd814;border-radius:6px;padding:8px;font-size:0.78rem;font-weight:700;color:#0f1111;text-align:center;margin-top:8px">Add to Cart</div>
    <div style="margin-top:8px;font-size:0.7rem;color:#0f1111">
      {"".join([f"<div style='margin-bottom:3px'>• {b}</div>" for b in [_b1,_b2,_b3] if b])}
      {"<div style='color:#007185;font-size:0.68rem'>▼ See more</div>" if len(_bullets)>3 else ""}
    </div>
  </div>
</div>""", unsafe_allow_html=True)

    st.divider()

    # ── Issues & Fixes ────────────────────────────────────────────────────────
    st.subheader("⚠️ Мобильные проблемы и фиксы")

    _mob_issues = []

    if _tlen > 80:
        _mob_issues.append({
            "severity": "HIGH",
            "icon": "🔴",
            "title": f"Title обрезается в поиске ({_tlen} симв., лимит ~80)",
            "impact": "Покупатель не видит ключевые слова → теряет интерес до клика",
            "fix": f"Перенеси самые важные слова в первые 80 символов. Сейчас виден только: '{_title[:80]}...'"
        })
    if _tlen > 120:
        _mob_issues.append({
            "severity": "HIGH",
            "icon": "🔴",
            "title": f"Title обрезается на странице товара ({_tlen} симв., лимит ~120)",
            "impact": "На первом экране покупатель видит неполное название",
            "fix": "Сократи title до 120 символов — всё важное должно быть в первых 120"
        })
    if not _has_vid:
        _mob_issues.append({
            "severity": "HIGH",
            "icon": "🔴",
            "title": "Нет видео — упускаешь главный мобильный конверсионный элемент",
            "impact": "Видео автоплеится на мобиле при скролле → +15-30% к конверсии",
            "fix": "Добавь короткое видео 30-60 сек: товар в действии + ключевые features"
        })
    if len(_bullets) > 0:
        _long_bullets = [i+1 for i,b in enumerate(_bullets[:3]) if len(b)>150]
        if _long_bullets:
            _mob_issues.append({
                "severity": "MEDIUM",
                "icon": "🟡",
                "title": f"Bullets {_long_bullets} слишком длинные для мобиля",
                "impact": "На мобиле первые 3 bullet — единственное что читают до 'See more'",
                "fix": "Укороти первые 3 bullet до 120-150 символов. Самое важное — в начало"
            })
    if not _has_ap:
        _mob_issues.append({
            "severity": "MEDIUM",
            "icon": "🟡",
            "title": "Нет A+ контента — конкуренты выглядят богаче на мобиле",
            "impact": "A+ рендерится ниже на мобиле, но создаёт brand trust",
            "fix": "Добавь базовый A+ с comparison chart и lifestyle фото"
        })
    if not _bullets:
        _mob_issues.append({
            "severity": "HIGH",
            "icon": "🔴",
            "title": "Нет bullets — покупатель не получает инфо без скролла",
            "impact": "На мобиле bullets — первое что читают после title и цены",
            "fix": "Добавь 5 bullets с форматом 'Feature: Benefit'"
        })

    if not _mob_issues:
        st.success("✅ Мобильных проблем не найдено — листинг хорошо адаптирован")
    else:
        _high = [i for i in _mob_issues if i["severity"]=="HIGH"]
        _med  = [i for i in _mob_issues if i["severity"]=="MEDIUM"]
        for _issues_group, _color, _label in [(_high,"#ef4444","🔴 Критичные"), (_med,"#f59e0b","🟡 Важные")]:
            if not _issues_group: continue
            st.markdown(f'<div style="font-size:0.8rem;font-weight:700;color:{_color};margin:12px 0 6px">{_label} — {len(_issues_group)} проблем</div>', unsafe_allow_html=True)
            for _iss in _issues_group:
                with st.container(border=True):
                    st.markdown(f"**{_iss['icon']} {_iss['title']}**")
                    st.caption(f"📉 Влияние: {_iss['impact']}")
                    st.info(f"🛠 Фикс: {_iss['fix']}")

    # ── AI Mobile Consultant ──────────────────────────────────────────────────
    st.divider()
    if st.button("🧠 AI-анализ мобильной конверсии", type="primary", key="btn_mobile_ai", help="Анализирует первый экран мобиля → главная проблема → одно изменение с максимальным приростом конверсии."):
        with st.spinner("🧠 AI анализирует мобильный опыт..."):
            _aud_mob = st.session_state.get("target_audience","")
            _mob_prompt = f"""Ты эксперт по мобильной конверсии Amazon. Проанализируй листинг.

Товар: {_title}
Title длина: {_tlen} символов
Bullets ({len(_bullets)}): {chr(10).join(_bullets[:3])}
Цена: {_price} | Рейтинг: {_rating} ({_reviews} отз.)
Видео: {"есть" if _has_vid else "нет"}
A+: {"есть" if _has_ap else "нет"}
{f"Аудитория: {_aud_mob}" if _aud_mob else ""}

Mobile Score: {_mob_overall}%

Дай конкретный анализ:
**Первый экран мобиля** (выше фолда): что видит покупатель и что теряет
**Главная мобильная проблема** которая убивает конверсию прямо сейчас
**Одно изменение** которое даст максимальный прирост мобильной конверсии
**Benchmark**: у топ-листингов этой ниши обычно есть X — у нас нет

Ответь {'по-русски' if st.session_state.get('analysis_lang','ru')=='ru' else 'in English'}. Коротко и конкретно."""

            _mob_ai = ai_call("Mobile conversion expert.", _mob_prompt, max_tokens=600)
            st.session_state["_mob_ai"] = _mob_ai

    if st.session_state.get("_mob_ai"):
        st.markdown(
            f'<div style="background:#0f172a;border:1px solid #334155;border-radius:12px;padding:18px 20px;line-height:1.7">'
            f'<div style="font-size:0.75rem;font-weight:700;color:#64748b;letter-spacing:0.08em;margin-bottom:10px">🧠 AI МОБИЛЬНЫЙ КОНСУЛЬТАНТ</div>'
            f'<div style="color:#e2e8f0;font-size:0.9rem">{st.session_state["_mob_ai"].replace(chr(10),"<br>")}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

# ══ О инструменте ═════════════════════════════════════════════════════════════
elif page == "ℹ️ О инструменте":
    st.markdown("## 🔍 Amazon Listing Analyzer")
    st.markdown("**Listing 3.0 — AI-инструмент нового поколения для Amazon продавцов**")
    st.divider()
    st.markdown("#### ЧТО ДЕЛАЕТ ИНСТРУМЕНТ")

    _features = [
        ("📸", "Vision AI", "Анализ фотографий", "#3b82f6",
         "Оценивает каждое фото по 6 критериям — видимость товара, фон, Amazon compliance. Определяет эмоцию покупателя: Доверие / Желание / Сомнение. McKinsey-вывод по всей галерее."),
        ("🧠", "COSMO / Rufus", "AI-видимость товара", "#8b5cf6",
         "Проверяет как Amazon AI понимает твой товар. Rufus Симулятор — задаёшь вопрос покупателя, AI отвечает как Amazon Rufus и показывает что не хватает в листинге."),
        ("🤖", "AI Readiness Score", "Метрика будущего", "#22c55e",
         "Насколько листинг готов к эпохе AI-агентов Amazon. Взвешенная оценка: COSMO × 30% + Rufus × 25% + JTBD × 20% + VPC × 15% + Title + Bullets."),
        ("🎯", "VPC / JTBD", "Язык покупателя", "#f59e0b",
         "Value Proposition Canvas — разрыв между тем что хочет покупатель и что говорит листинг. Jobs To Be Done — покупатель не покупает продукт, он нанимает его для работы."),
        ("📱", "Mobile Score", "Превью как на телефоне", "#06b6d4",
         "70% покупок Amazon — с мобильного. Показывает как title обрезается в поиске, как выглядит первый экран страницы товара. Конкретные фиксы с указанием влияния на конверсию."),
        ("🔥", "Топ ниши", "Разведка конкурентов", "#ef4444",
         "Ищет топ-продавцов в нише по любому запросу. AI-отчёт: что объединяет лидеров, какие ключевые слова используют, что нужно изменить чтобы попасть в топ-3."),
        ("🏆", "Benchmark", "Подиум vs конкуренты", "#94a3b8",
         "Сравнивает до 5 конкурентов по 10 метрикам. Показывает кто на первом месте и по каким параметрам ты проигрываешь. Vision анализ фото конкурентов — те же оценки что у нас."),
        ("📄", "PDF Отчёт", "Для клиентов и команды", "#f97316",
         "Профессиональный PDF с обложкой, Score Dashboard, анализом фото с миниатюрами, A+ баннерами, COSMO/Rufus/JTBD/VPC данными и сравнением конкурентов."),
        ("📋", "Workflow", "Pipeline листингов", "#10b981",
         "Kanban-доска для управления статусами: Новый аудит → Нужен рерайт → К дизайнеру → К загрузке → Готово. История всех анализов с динамикой Score."),
        ("🗄️", "История", "Трекинг изменений", "#e879f9",
         "Сохраняет каждый анализ в PostgreSQL. Показывает динамику Score. Фото и A+ Vision сохраняются для просмотра без повторного анализа."),
    ]

    for i in range(0, len(_features), 2):
        _fc1, _fc2 = st.columns(2)
        for _col, _f in zip([_fc1, _fc2], _features[i:i+2]):
            icon, name, subtitle, color, desc = _f
            _col.markdown(
                f'<div style="background:#1e293b;border-radius:10px;padding:16px;border-left:4px solid {color};margin-bottom:8px">' +
                f'<div style="font-size:1.1rem;font-weight:700;color:#e2e8f0;margin-bottom:4px">{icon} {name}</div>' +
                f'<div style="font-size:0.8rem;font-weight:600;color:{color};margin-bottom:6px">{subtitle}</div>' +
                f'<div style="font-size:0.8rem;color:#94a3b8;line-height:1.5">{desc}</div></div>',
                unsafe_allow_html=True
            )

    st.divider()
    _s1, _s2, _s3 = st.columns(3)
    _s1.markdown('<div style="text-align:center;padding:16px;background:#1e293b;border-radius:10px"><div style="font-size:2.5rem;font-weight:900;color:#3b82f6">17</div><div style="color:#64748b;font-size:0.8rem">метрик анализа</div></div>', unsafe_allow_html=True)
    _s2.markdown('<div style="text-align:center;padding:16px;background:#1e293b;border-radius:10px"><div style="font-size:1.8rem;font-weight:900;color:#8b5cf6">Vision AI</div><div style="color:#64748b;font-size:0.8rem">оценка каждого фото</div></div>', unsafe_allow_html=True)
    _s3.markdown('<div style="text-align:center;padding:16px;background:#1e293b;border-radius:10px"><div style="font-size:2rem;font-weight:900;color:#22c55e">Rufus</div><div style="color:#64748b;font-size:0.8rem">симулятор Amazon AI</div></div>', unsafe_allow_html=True)
    st.markdown('<div style="text-align:center;color:#64748b;font-size:0.85rem;margin-top:16px">Helium10 и Jungle Scout дают <b>данные</b>. Мы даём <b style="color:#22c55e">смысл и рекомендации</b>.</div>', unsafe_allow_html=True)
    st.divider()
    st.caption("Built by Vitaly Terershin / Merino.tech · Powered by Claude AI + Gemini")

# ══ Документация ══════════════════════════════════════════════════════════════
elif page == "📖 Документация":
    st.title("📖 Документация")
    st.caption("Руководство по всем разделам инструмента для команды")

    _doc_tab1, _doc_tab2, _doc_tab3, _doc_tab4, _doc_tab5, _doc_tab6, _doc_tab7, _doc_tab8, _doc_tab9 = st.tabs([
        "🏠 Обзор", "📸 Vision AI", "🧠 COSMO/Rufus", "🎯 VPC/JTBD",
        "🏆 Benchmark", "🔥 Топ ниши", "📱 Mobile", "📋 Workflow", "💡 Практики"
    ])

    with _doc_tab1:
        st.subheader("🏠 Обзор листинга")
        st.markdown("""
**Что это:** Главная страница после анализа. Показывает общий Health Score листинга и все ключевые метрики одним взглядом.

**Как читать:**
- **Health Score (%)** — итоговая оценка листинга. 75%+ = сильный, 50-74% = нужна работа, <50% = критично
- **Шкала метрик** — 10 компонентов: Title, Bullets, Описание, Фото, A+, Отзывы, BSR, Цена, Варианты, Prime
- **Приоритетные действия** — красные (HIGH) делаем первыми, жёлтые (MEDIUM) вторыми

**Инструменты:**
- ✍️ **Переписать листинг** — AI пишет новый Title + 5 Bullets на основе анализа
- 🔑 **Keyword Gap** — находит слова которые используют конкуренты но нет у нас
- 💬 **Mining отзывов** — извлекает язык покупателей из 4-5★ отзывов
- 🔍 **Анализ возвратов** — AI читает 1-3★ отзывы и находит причины возвратов
- 📄 **PDF отчёт** — профессиональный отчёт для клиента/команды

**Когда использовать:** После каждого анализа — смотришь Health Score в динамике, следишь за ростом метрик.
""")

    with _doc_tab2:
        st.subheader("📸 Vision AI — Анализ фотографий")
        st.markdown("""
**Что это:** AI анализирует каждое фото по 6 критериям и определяет какую эмоцию оно вызывает у покупателя.

**Как читать оценку фото (1-10):**
- **9-10** — Отлично, не трогать
- **7-8** — Хорошо, небольшие улучшения
- **5-6** — Требует улучшения
- **1-4** — Слабое фото, заменить

**6 критериев оценки:**
| Критерий | Вес | Что проверяет |
|----------|-----|---------------|
| Видимость товара | +2 | Товар занимает ≥85% кадра? |
| Фон | +2 | Белый для главного, релевантный для lifestyle |
| Информационная ценность | +2 | Показывает характеристики важные для покупателя |
| Amazon compliance | +2 | Нет водяных знаков, правильные пропорции |
| Lifestyle appeal | +1 | Покупатель видит себя с товаром |
| Уникальность | +1 | Не стоковое фото |

**Эмоции покупателя:**
- 💚 **Доверие** — белый фон, чёткий товар, сертификаты
- 🟡 **Желание** — lifestyle, человек в действии
- 🔴 **Сомнение** — мутное фото, товар не виден
- 💙 **Любопытство** — необычный ракурс, деталь
- ⚪ **Безразличие** — стоковое скучное фото

**Сводка галереи:** Карточки с оценками всех фото + средняя. Кнопка "🧠 AI-оценка галереи" — McKinsey-вывод по всей галерее.

**Аудитория:** Заполни поле "👤 Целевая аудитория" в форме (напр. "Женщина, 45 лет") — AI оценивает фото через призму этой аудитории.
""")

    with _doc_tab3:
        st.subheader("🧠 COSMO / Rufus — AI-видимость")
        st.markdown("""
**Что это:** Анализ насколько Amazon AI понимает твой товар и может его правильно рекомендовать покупателям.

**COSMO Score** — как алгоритм Amazon классифицирует твой товар по 15 сигналам:
Use Cases, Аудитория, Материал, Размеры, Совместимость, Occasion, Сезон, Возраст, Гендер, Стиль, Качество, Проблема которую решает, Уникальная ценность.

- ✅ **Присутствуют** — сигналы которые Amazon нашёл в листинге
- ❌ **Отсутствуют** — что нужно добавить чтобы AI правильно рекомендовал товар

**Rufus Score** — как Amazon AI-ассистент ответит на вопросы покупателей о товаре. Низкий score = Rufus не найдёт ответ в листинге = не порекомендует.

**AI Readiness Score** — итоговая готовность к AI-эпохе:
```
COSMO × 30% + Rufus × 25% + JTBD × 20% + VPC × 15% + Title × 5% + Bullets × 5%
```

**Rufus Симулятор:** Задаёшь вопрос как покупатель → AI отвечает как Amazon Rufus → показывает ⚠️ Gap: что не нашёл в листинге.

**Plan of Action:** Копишь Q&A из симулятора → AI генерирует конкретный план: что добавить в Title/Bullets/A+.

**Реальные Q&A:** Если ScrapingDog вернул вопросы покупателей — они показываются в Plan of Action автоматически.
""")

    with _doc_tab4:
        st.subheader("🎯 VPC / JTBD — Язык покупателя")
        st.markdown("""
**Что это:** Два фреймворка которые показывают разрыв между тем что хочет покупатель и тем что говорит листинг.

**JTBD — Jobs To Be Done:**
Покупатель не покупает продукт — он **нанимает его для работы**.

3 типа работ:
- ⚙️ **Функциональная** — "остаться сухим и тёплым на маршруте"
- ❤️ **Эмоциональная** — "чувствовать себя уверенно после тренировки"
- 👥 **Социальная** — "выглядеть как человек который серьёзно относится к outdoor"

**Job Story:** "Когда [ситуация], я хочу [мотивация], чтобы [результат]"

**Alignment Score:** Насколько листинг говорит языком этих работ:
- 90%+ = листинг напрямую обращается к ситуации покупателя
- 70-89% = работа видна, но спрятана за фичами
- <70% = листинг описывает продукт, а не работу

**VPC — Value Proposition Canvas:**
- **Профиль покупателя** — Jobs (задачи), Pains (боли), Gains (выгоды)
- **Карта ценности** — Pain Relievers (что закрывает боли), Gain Creators (что создаёт выгоды)
- **VPC Fit Score** — % болей и выгод которые листинг явно адресует

**Вердикт AI CRO консультанта:** 1-2 предложения о разрыве между продуктом и коммуникацией.
""")

    with _doc_tab5:
        st.subheader("🏆 Benchmark — Сравнение с конкурентами")
        st.markdown("""
**Что это:** Подиум — кто первый, кто второй, кто третий по Overall Score среди нас и конкурентов.

**Как добавить конкурентов:**
Вставь URL Amazon листинга конкурента в поле "Конкурент 1-5" и запусти анализ. До 5 конкурентов одновременно.

**Таблица сравнения — 10 метрик:**
Title · Bullets · Описание · Фото · A+ · Отзывы · BSR · Варианты · Prime · Overall

Зелёная звёздочка ★ = лидер по этой метрике.

**Страница конкурента (🔴 Конкурент N):**
Та же глубина анализа что у нашего листинга — Vision фото, A+ баннеры, Stop Words, возврат отзывов. Кнопка "🧠 Анализ" если не анализировался при запуске.

**История конкурентов:**
Все проанализированные конкуренты сохраняются в базу. Видны в История → вкладка 🔴 Конкуренты.
""")

    with _doc_tab6:
        st.subheader("🔥 Топ ниши — Разведка рынка")
        st.markdown("""
**Что это:** Живой срез Amazon поиска — кто сейчас топ в нише, по каким ценам, с какими отзывами.

**Как использовать:**

1. Введи запрос как покупатель: `merino wool base layer men`
2. Выбери маркетплейс (com / de / fr / it / es)
3. Нажми "🔍 Найти топ"

**5 метрик ниши:**
| Метрика | Что значит |
|---------|-----------|
| 📦 Конкурентов | Органических позиций в топ-12 |
| 💰 Средняя цена | Куда позиционироваться |
| ⭐ Средний рейтинг | Планка качества |
| 💬 Средние отзывы | Барьер входа |
| 🎯 Sponsored | Рекламных мест из 12 |

**Карточки товаров:**
- Позиция #1-12 (🥇 золото = топ-3)
- 🏆 Best Seller / ✅ Amazon's Choice / 👑 Prime
- 🟢 **Вход лёгкий** (<300 отз.) / 🟡 Средний / 🔴 Высокий порог
- Купон, "100+ bought in past month", количество цветов

**Графики:**
- Ценовое распределение — кто демпингует, кто в премиуме
- Отзывы — визуально где слабые места конкурентов

**Зелёный инсайт:** Автоматически находит товары с <300 отзывами = лёгкий вход.

**Кнопки действий:**
- ↗ Amazon — открыть листинг
- 🔍 Анализ — быстрый AI-скоринг без полного анализа
- 🧠 AI-отчёт — McKinsey-анализ всей ниши: паттерны лидеров, топ keywords, что менять

**Когда использовать:**
- Выводим новый товар → смотришь барьер входа и позиционирование
- Ищем нишу → ищешь 🟢 Вход лёгкий + маленькое число Sponsored
- Проверяем EU рынок → меняешь маркетплейс de/fr/it
""")

    with _doc_tab7:
        st.subheader("📱 Mobile Score — Мобильная конверсия")
        st.markdown("""
**Что это:** 70% покупок Amazon — с мобильного. Инструмент показывает как листинг выглядит на телефоне и что теряем.

**Mobile Score (0-100%):**
| Компонент | Вес | Критерий |
|-----------|-----|----------|
| Title в поиске | 25% | ≤80 символов видно в выдаче |
| Title на странице | — | ≤120 символов на PDP |
| Главное фото | 30% | Оценка из Vision AI |
| Bullets первые 3 | 20% | Единственное что читают до "See more" |
| A+ контент | 10% | Brand trust ниже fold |
| Видео | 10% | Автоплей на мобиле = +15-30% конверсии |

**Два превью:**
- 🔍 **Поисковая выдача** — как выглядит карточка в поиске
- 📄 **Страница товара** — первый экран без скролла

**Проблемы HIGH/MEDIUM:** Конкретные фиксы с указанием влияния на конверсию.

**AI мобильный консультант:** Одна кнопка → конкретный ответ что изменить прямо сейчас.

**Важно:** Страница работает без предварительного анализа — можно открыть сразу.
""")

    with _doc_tab8:
        st.subheader("📋 Workflow — Pipeline листингов")
        st.markdown("""
**Что это:** Kanban-доска для управления статусами всех листингов. Видно кто на какой стадии оптимизации.

**Статусы:**
| Иконка | Статус | Значит |
|--------|--------|--------|
| 🆕 | Новый аудит | Только что проанализирован |
| ✏️ | Нужен рерайт | Title/Bullets требуют переработки |
| 🎨 | К дизайнеру | Нужны новые фото/A+ баннеры |
| 📋 | К загрузке | Готово к публикации в Seller Central |
| 🔁 | Перепроверить | Проверить после изменений |
| ✅ | Готово | Оптимизация завершена |

**Как использовать:**
1. После анализа листинг автоматически попадает в "🆕 Новый аудит"
2. Смотришь Health Score и проблемы → устанавливаешь статус
3. Добавляешь заметку (что именно нужно сделать)
4. После правок → меняешь статус

**История:** Все анализы сохраняются → можно открыть любой предыдущий и посмотреть динамику Score через время.
""")

    with _doc_tab9:
        st.subheader("💡 Лучшие практики")
        st.markdown("""
**Как получить максимум от инструмента:**

**Аудит нового листинга (полный):**
1. Вставь URL нашего листинга + 3-5 конкурентов
2. Заполни "👤 Целевая аудитория" (напр. "Мужчина, 35 лет, outdoor hiking")
3. Выбери "🔴 Полный аудит" в Оптимизации токенов
4. Запусти → жди 3-5 минут
5. Смотри Health Score → Приоритетные действия → Vision фото → COSMO gaps

**Быстрый ретест после правок:**
1. В Оптимизации токенов → отключи Vision (⚡ Быстрый ретест)
2. Запусти → готово за 30 секунд
3. Сравни Health Score с предыдущим

**Разведка перед запуском нового товара:**
1. Открой 🔥 Топ ниши
2. Введи ключевой запрос
3. Смотри барьер входа и ценовой диапазон
4. Жми "🧠 AI-отчёт" → получи паттерны лидеров

**Работа с командой:**
- После анализа → скачай PDF отчёт → отправь дизайнеру/копирайтеру
- В Workflow → ставь статус и заметку для каждого листинга
- История → отслеживай динамику Score по месяцам

**Stop Words — обязательно:**
На странице 📝 Контент → блок Stop Words. Запрещённые слова (красные 🚫) = мгновенная suppression листинга. Убирай сразу.

**EU маркетплейсы:**
Просто вставь URL amazon.de или amazon.fr — инструмент автоматически определит маркетплейс и загрузит данные с правильного домена.

**Экономия API:**
- Claude дороже Gemini в ~3-4x но качественнее
- Для ежедневных ретестов → используй Gemini
- Для клиентских отчётов → используй Claude
""")
# ══ Workflow ══════════════════════════════════════════════════════════════════
elif page == "📋 Workflow":
    st.title("📋 Workflow — Pipeline листингов")
    _board=db_workflow_board()
    if not _board: st.info("Нет данных. Запусти анализ хотя бы одного листинга.")
    else:
        _status_cols={key:[] for _,key,_ in WORKFLOW_STATUSES}
        for item in _board:
            _s=item.get("status","new_audit")
            if _s not in _status_cols: _s="new_audit"
            _status_cols[_s].append(item)
        _wf_cols=st.columns(len(WORKFLOW_STATUSES))
        for _ci,(icon,key,label) in enumerate(WORKFLOW_STATUSES):
            with _wf_cols[_ci]:
                _items=_status_cols[key]; st.markdown(f"**{icon} {label}**"); st.caption(f"{len(_items)} листинг(ов)")
                for _it in _items:
                    _score=_it.get("score") or 0; _sc_color="#22c55e" if _score>=80 else ("#f59e0b" if _score>=60 else "#ef4444")
                    with st.container(border=True):
                        st.markdown(f"**{_it['asin']}**")
                        st.markdown(f'<span style="color:{_sc_color};font-weight:700">{_score}%</span>', unsafe_allow_html=True)
                        if _it.get("title"): st.caption(_it["title"][:40])
                        if _it.get("note"): st.caption(f"📝 {_it['note']}")
        st.divider(); st.subheader("✏️ Изменить статус")
        _sel_asin=st.selectbox("ASIN",[i["asin"] for i in _board],key="wf_sel_asin")
        _sel_item=next((i for i in _board if i["asin"]==_sel_asin),None)
        if _sel_item:
            _cur_status=_sel_item.get("status","new_audit"); _status_keys=[k for _,k,_ in WORKFLOW_STATUSES]
            _status_labels=[f"{ic} {lb}" for ic,_,lb in WORKFLOW_STATUSES]
            _cur_idx=_status_keys.index(_cur_status) if _cur_status in _status_keys else 0
            _new_status_label=st.radio("Статус",_status_labels,index=_cur_idx,horizontal=True,key="wf_status_radio")
            _new_status=_status_keys[_status_labels.index(_new_status_label)]
            _new_note=st.text_input("Заметка",value=_sel_item.get("note",""),key="wf_note")
            if st.button("💾 Сохранить",type="primary",key="wf_save"):
                if db_update_workflow(_sel_item["id"],_new_status,_new_note):
                    st.success(f"✅ {_sel_asin} → {workflow_label(_new_status)}")
                    st.rerun()
                else:
                    st.error("Ошибка сохранения")
    st.stop()

r  = st.session_state.get("result", {})
v  = st.session_state.get("vision", "")
od = st.session_state.get("our_data", {})
pi = od.get("product_information", {})
cd = st.session_state.get("comp_data_list", [])
imgs = st.session_state.get("images", [])
