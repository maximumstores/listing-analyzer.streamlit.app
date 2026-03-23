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
            ("marketplace", "TEXT DEFAULT 'com'"),
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
    # Не сохраняем если анализ упал (overall = 0)
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
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO listing_analysis
              (asin, overall_score, title_score, bullets_score, images_score,
               aplus_score, cosmo_score, rufus_score, result_json, vision_text,
               our_title, competitors_json, our_data_json, marketplace)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
              st.session_state.get("_marketplace","com")))
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
            SELECT DISTINCT ON (asin) asin, our_title, overall_score, analyzed_at, listing_type,
                   COALESCE(marketplace,'com') as marketplace
            FROM listing_analysis
            ORDER BY asin, analyzed_at DESC
        """)
        rows = cur.fetchall()
        conn.close()
        return [{"asin": r[0], "title": r[1], "score": r[2], "date": r[3],
                 "type": r[4], "marketplace": r[5]} for r in rows]
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
    """Fetch return reasons from SP-API for our ASINs."""
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

    # Step 1: Get LWA access token
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

    # Step 2: Request Returns report
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

        # AWS SigV4
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

        # Step 3: Poll for completion
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

        # Step 4: Download report
        doc_r = requests.get(
            f"{endpoint}/reports/2021-06-30/documents/{doc_id}",
            headers=headers, timeout=30)
        if not doc_r.ok: return []
        doc_url = doc_r.json().get("url","")
        csv_r   = requests.get(doc_url, timeout=60)
        if not csv_r.ok: return []

        # Step 5: Parse CSV → filter by ASIN
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
    """AI analysis of SP-API return data."""
    if not returns: return "Нет данных о возвратах"
    lang_name = "Russian" if lang == "ru" else "English"

    # Aggregate reasons
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
    """Fetch 1, 2, 3-star reviews via Apify — 10 each = 30 total."""
    api_token = st.secrets.get("APIFY_API_TOKEN","")
    if not api_token:
        if log: log("⚠️ APIFY_API_TOKEN не задан в Secrets")
        return []
    endpoint = f"https://api.apify.com/v2/acts/webdatalabs~amazon-reviews-scraper/run-sync-get-dataset-items?token={api_token}"
    all_reviews = []
    # Один запрос без фильтров — получаем все отзывы и фильтруем локально
    payload = {
        "productUrls": [{"url": f"https://www.amazon.com/dp/{asin}"}],
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
    """AI analysis of return reasons from 1-star reviews."""
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
        # Medical/Treatment
        "ailment","cure","cured","cures","treat","treatment","treats","heal","healing","heals",
        "prevent","prevents","diagnose","remedy","remedies","medication","pharmaceutical",
        "detox","detoxify","detoxification","detoxifying","reparative","fast relief","relief",
        "clinically proven","doctor recommended","no side effects","pain free","proven to work",
        "performance enhancement","disease","diseases","illness","maladies","malady",
        # Diseases
        "aids","add","adhd","als","alzheimer","autism","autistic","cancer","cancroid",
        "cataract","chlamydia","cmv","cytomegalovirus","concussion","coronavirus","covid",
        "crabs","cystic fibrosis","dementia","depression","diabetes","diabetic",
        "epilepsy","flu","glaucoma","gonorrhea","gout","hepatitis","herpes","hsv1","hsv2",
        "hiv","hodgkin","hpv","influenza","kidney disease","liver disease","lupus",
        "lymphoma","meningitis","mononucleosis","mono","multiple sclerosis",
        "muscular dystrophy","obesity","parkinson","pid","pelvic inflammatory",
        "scabies","seizure","seizures","stroke","syphilis","trichomoniasis","tumor",
        "ringworm","insomnia","anxiety","inflammation","infection",
        # Pesticide triggers
        "antibacterial","anti-bacterial","antimicrobial","anti-microbial","antifungal",
        "anti-fungal","antiviral","antiseptic","bacteria","bacterial","contaminants",
        "contamination","disinfect","disinfectant","disinfects","fungal","fungus",
        "fungicide","fungicides","germ","germs","germ-free","insecticide","mildew",
        "mold","mould","mold resistant","mold spores","nano silver","parasitic",
        "pathogen","pest","pesticide","pesticides","pesticide-free","protozoa",
        "repel","repellent","repelling","sanitize","sanitizes","viral","virus","viruses",
        "mites","yeast","biological contaminants",
        # Drugs/Substances
        "cbd","cannabinoid","thc","cannabidiol","cannabis","marijuana","kratom","hemp",
        "kanna","weed","dab","shatter","ketamine","psilocybin","ephedrine",
        "minoxidil","ketoconazole","hordenine","ayahuasca","picamilon","dmt",
        # Other prohibited
        "knockoff","fake","weapon","weapons","stun guns","self defense","pepper spray",
        "swastika","poppy","iv therapy","intravenous therapy","fetal doppler",
        "heartbeat monitor","batons","drugged",
        # Competitor/Amazon endorsement
        "amazon approved","amazon certified","amazon recommended","amazon endorsed",
        "amazon authorized","amazon licensed","amazon verified",
    ],
    "try_to_avoid": [
        # Superlatives
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
        # Environmental (banned Oct 2024)
        "eco-friendly","eco friendly","ecofriendly","environmentally friendly",
        "earth-friendly","sustainable","biodegradable","compostable","home compostable",
        "marine degradable","decomposable","degradable","carbon-reducing",
        "all natural","all-natural","natural","recyclable","vegan","non-toxic",
        "bpa free","bisphenol a","hypoallergenic","organic","green",
        # Health claims (avoid)
        "allergy free","allergy safe","anti aging","healthy","healthier","proven",
        "recommended by","tested","validated","treatment","weight loss","hypoallergenic",
        "nano silver","safe","harmless","non-poisonous","non-injurious","non-toxic",
        "reduce anxiety","boost immunity","lower blood pressure","increase metabolism",
        "suppress appetite","slimming","fat burning","keto approved","appetite suppressant",
        # Pricing/Promo
        "free","bonus","guarantee","money back","refund","warranty","price",
        "on sale","best deal","limited time","buy now","add to cart","get yours now",
        "shop now","don't miss","last chance","supplies won't last","available now",
        "save","discount","bargain","cheap","cheapest","clearance","closeout","overstock",
        "special offer","buy 1 get 1","wholesale","% off","affordable",
        # Made in USA (requires FTC compliance)
        "made in the usa","made in usa",
    ],
    "a_plus_restricted": [
        # Strictly prohibited in A+ Content
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
            # brand_images contains "From the brand" A+ banners — filter only aplus-media-library URLs
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
        block_fmt = "\nPHOTO_BLOCK_{i}\nSTRICTLY 7 lines:\nType: [one of the types above]\nScore: X/10 [apply rubric]\nStrength: [1 specific strength — what exactly drives conversion]\nWeakness: [1 specific problem — ONLY what you see, with number if possible]\nAction: [CONCRETE solution: WHAT to shoot/add/remove + HOW exactly + expected conversion impact in %]\nConversion: [1 insight — what fear or desire this photo triggers and how to amplify/resolve it]\nEmotion: [primary emotion: Trust / Desire / Doubt / Curiosity / Indifference — explain why in 1 sentence]"
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
        block_fmt = "\nPHOTO_BLOCK_{i}\nОТРОГО 7 строк:\nТип: [один из типов выше]\nОценка: X/10 [применяй рубрик]\nСильная сторона: [1 конкретная сильная сторона — что именно работает на конверсию]\nСлабость: [1 конкретная проблема — ТОЛЬКО то что видишь, с цифрой если возможно]\nДействие: [КОНКРЕТНОЕ решение: ЧТО снять/добавить/убрать + КАК именно + ожидаемый эффект на конверсию в %]\nКонверсия: [1 инсайт — какой страх или желание покупателя это фото вызывает и как его усилить/снять]\nЭмоция: [основная эмоция: Доверие / Желание / Сомнение / Любопытство / Безразличие — объясни почему в 1 предложении]"

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
def analyze_text(our_data, competitor_data_list, vision_result, asin, log, lang="ru", is_competitor=False):
    log("🧠 Финальный анализ...")

    # Лёгкая схема для конкурентов — без VPC/JTBD/COSMO/Rufus
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

    # ── Упрощённый промпт для конкурентов ────────────────────────────────────
    if is_competitor:
        # Урезанные данные для конкурента — только нужное
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
        # Aggressive JSON repair
        s = re.sub(r",\s*([}\]])", r"\1", s)
        s = re.sub(r'[\x00-\x1f\x7f]', ' ', s)  # remove control chars
        try:
            return json.loads(s)
        except:
            # Fix unescaped quotes inside strings
            s2 = re.sub(r'"([^"]*)"', lambda m: '"'+m.group(1).replace("\n"," ").replace("\r"," ").replace('"','\\"')+'"', s)
            try:
                return json.loads(s2)
            except:
                # Last resort: truncate and close brackets
                for cut in range(len(s2)-1, max(len(s2)-500, 0), -1):
                    if s2[cut] in ('"', '}', ']', '0123456789'):
                        candidate = s2[:cut+1]
                        candidate += "]" * max(0, candidate.count("[") - candidate.count("]"))
                        candidate += "}" * max(0, candidate.count("{") - candidate.count("}"))
                        try: return json.loads(candidate)
                        except: continue
                # Return minimal fallback
                return {"overall_score": "50%", "title_score": "50%", "bullets_score": "50%",
                        "description_score": "50%", "images_score": "50%", "aplus_score": "0%",
                        "reviews_score": "50%", "bsr_score": "50%", "price_score": "50%",
                        "customization_score": "50%", "prime_score": "50%",
                        "priority_improvements": ["JSON repair failed — rerun analysis"]}

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


# ── Main ──────────────────────────────────────────────────────────────────────
def run_analysis(our_url, competitor_urls, log, prog=None):
    _steps_done = []
    def _prog(pct, text):
        if prog:
            _steps_done.append(text)
            prog.progress(min(pct/100, 1.0), text=f"[{pct}%] {text}")
        log(text)

    asin = get_asin(our_url) or "unknown"
    _lang = st.session_state.get("analysis_lang","ru")
    # Detect marketplace from URL
    _mp = "com"
    for _d in ["co.uk","de","ca","fr","it","es","com"]:
        if f"amazon.{_d}" in our_url:
            _mp = _d; break
    st.session_state["_marketplace"] = _mp

    # Read vision toggles
    _do_vision      = st.session_state.get("do_vision", True)
    _do_aplus       = st.session_state.get("do_aplus_vision", True)
    _do_comp_vision = st.session_state.get("do_comp_vision", True)

    # ── Если нашего URL нет — пропускаем наш листинг ────────────────────────
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
        our_data, img_urls = scrapingdog_product(asin, log)
        _prog(12, f"✅ Данные получены — {len(our_data.get('feature_bullets',[]))} буллетов, {len(img_urls)} фото")

        _prog(15, f"⬇️ Скачиваю фото ({len(img_urls)} шт.)...")
        images = download_images(img_urls, log) if img_urls else []
        st.session_state["images"] = images
        _prog(22, f"✅ Фото скачаны: {len(images)} шт. готовы к анализу")

        # ── Vision фото (основной листинг) ───────────────────────────────────────
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

        # ── A+ Vision ─────────────────────────────────────────────────────────────
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

    # ── Конкуренты ────────────────────────────────────────────────────────────
    active = [u.strip() for u in competitor_urls if u.strip()]
    comp_data_list = []
    n_active = max(len(active), 1)

    for i, url in enumerate(active[:3]):
        casin = get_asin(url)
        if not casin: continue
        base_pct = 45 + i * 10

        _prog(base_pct,     f"🌐 Конкурент {i+1}/{len(active)}: загружаю {casin}...")
        cdata, cimg_urls = scrapingdog_product(casin, log)
        cdata["_input_asin"] = casin
        comp_data_list.append(cdata)
        _prog(base_pct + 2, f"✅ Конкурент {i+1}: данные получены — {cdata.get('title','')[:30]}...")

        _prog(base_pct + 3, f"⬇️ Конкурент {i+1}: скачиваю фото...")
        cimgs_dl = download_images(cimg_urls[:5], log) if cimg_urls else []

        # ── Vision конкурента ─────────────────────────────────────────────
        if cimgs_dl and _do_comp_vision:
            _prog(base_pct + 5, f"👁️ Конкурент {i+1}: Vision {len(cimgs_dl)} фото...")
            cvision = analyze_vision(cimgs_dl, cdata, casin, log, lang=_lang)
            _prog(base_pct + 6, f"✅ Конкурент {i+1}: Vision готов")
        else:
            cvision = ""
            if cimgs_dl:
                log(f"⏭️ Vision конкурент {i+1} пропущен (отключён)")

        # ── A+ Vision конкурента ───────────────────────────────────────────
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

    _prog(78, "🧠 AI финальный анализ — COSMO + Rufus + JTBD + VPC...")
    result = analyze_text(our_data, comp_data_list, vision_result, asin, log, lang=_lang)
    _prog(92, "💾 Сохраняю результаты в историю...")
    st.session_state['our_data'] = our_data
    st.session_state['comp_data_list'] = comp_data_list
    _prog(98, "✅ Анализ завершён!")
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
        ("🎯", "VPC / JTBD"),
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
    our_url = st.text_input("🔵 НАШ листинг", value=st.session_state.get("our_url_saved",""), placeholder="https://www.amazon.com/dp/...")
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

    # ── Общая сводка по всем ASIN ─────────────────────────────────────────────
    st.subheader(f"📋 Все листинги в базе — {len(all_asins)} шт.")
    import pandas as pd

    _search = st.text_input("🔍 Поиск по ASIN или названию", placeholder="B08M3D... или merino gaiter", key="hist_search", label_visibility="collapsed")

    def _amz_thumb(asin):
        return f"https://ws-na.amazon-adsystem.com/widgets/q?_encoding=UTF8&ASIN={asin}&ServiceVersion=20070822&ID=AsinImage&WS=1&Format=SL110"

    _filtered_asins = [a for a in all_asins if not _search or
        _search.lower() in a["asin"].lower() or
        _search.lower() in (a.get("title") or "").lower()]

    _clicked_asin = None
    for _idx, _a in enumerate(_filtered_asins):
        _sc = _a.get("score") or 0
        _sc_c = "#22c55e" if _sc>=75 else ("#f59e0b" if _sc>=50 else ("#ef4444" if _sc>0 else "#94a3b8"))
        _sc_lbl = "Strong" if _sc>=75 else ("Needs Work" if _sc>=50 else ("Critical" if _sc>0 else "—"))
        _title = (_a.get("title") or "")[:60]
        _asin = _a["asin"]
        _date = _a["date"].strftime("%d.%m.%Y %H:%M") if _a.get("date") else "—"
        _mp = _a.get("marketplace","com")
        _mp_flag = {"com":"🇺🇸","de":"🇩🇪","co.uk":"🇬🇧","ca":"🇨🇦","fr":"🇫🇷","it":"🇮🇹","es":"🇪🇸"}.get(_mp,"🇺🇸")

        _ci1, _ci2, _ci3, _ci4 = st.columns([1, 6, 2, 1.5])
        with _ci1:
            try:
                st.image(_amz_thumb(_asin), width=56)
            except:
                st.markdown(f'<div style="width:56px;height:56px;background:#f1f5f9;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:0.65rem;color:#94a3b8">{_asin[:4]}</div>', unsafe_allow_html=True)
        with _ci2:
            st.markdown(
                f'<div style="padding:6px 0">'
                f'<div style="font-size:0.9rem;font-weight:600;color:#0f172a;line-height:1.3">{_title}</div>'
                f'<div style="font-size:0.78rem;color:#64748b;margin-top:3px">'
                f'{_mp_flag} &nbsp;·&nbsp; '
                f'<a href="https://www.amazon.com/dp/{_asin}" target="_blank" style="color:#3b82f6;text-decoration:none">{_asin} ↗</a>'
                f' &nbsp;·&nbsp; {_date}</div>'
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
            if st.button("Open", key=f"hist_open_{_idx}", use_container_width=True):
                _clicked_asin = _asin

        st.markdown('<hr style="margin:4px 0;border-color:#f1f5f9">', unsafe_allow_html=True)

    st.divider()

    asin_opts = [f"{"🔵" if a.get("type","наш")=="наш" else "🔴"} {a['asin']} — {(a['title'] or '')[:40]}" for a in all_asins]
    # Pre-select from Open button click
    _default_idx = 0
    if _clicked_asin:
        _match = next((i for i,a in enumerate(all_asins) if a["asin"]==_clicked_asin), 0)
        _default_idx = _match
    sel = st.selectbox("ASIN", asin_opts, index=_default_idx)
    sel_asin = sel.split(" — ")[0].strip().lstrip("🔵🔴 ")

    # Full title + Amazon link
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
    # Фильтруем 0% записи из отображения таблицы
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

        # Cleanup button
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
    import pandas as pd
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
    # Показываем все записи — но 0% помечаем серым и перемещаем вниз
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
    # Map back to original history index for DB query
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
                    SELECT result_json, vision_text, competitors_json, our_data_json
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
                # Restore our_data if saved
                if row_h[3]:
                    try:
                        _od_hist = json.loads(row_h[3])
                        st.session_state["our_data"] = _od_hist
                    except: pass
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
    title_h   = od.get("title","") or st.session_state.get("_hist_title","")
    tlen      = len(title_h)
    brand_h   = od.get("brand","")
    asin_h    = od.get("parent_asin","") or pi.get("ASIN","") or st.session_state.get("our_url_saved","")
    price_h   = od.get("price","")
    prev_price = od.get("previous_price","") or od.get("list_price","")
    rating_h  = od.get("average_rating","")
    reviews_h = pi.get("Customer Reviews",{}).get("ratings_count","")
    bsr_h     = str(pi.get("Best Sellers Rank",""))[:50]
    coupon    = od.get("coupon_text","") or ("🎟️ Купон" if od.get("is_coupon_exists") else "")
    promo     = od.get("promo_text","")
    is_prime  = od.get("is_prime_exclusive") or od.get("is_prime")
    bought    = od.get("number_of_people_bought","")

    # If loaded from history — show notice instead of empty fields
    _is_history = st.session_state.get("_hist_loaded") and not title_h

    # Build price line
    price_parts = []
    if price_h:
        price_str = f"💰 <b>{price_h}</b>"
        if prev_price and prev_price != price_h:
            price_str += f" <span style='text-decoration:line-through;opacity:0.5'>{prev_price}</span>"
        price_parts.append(price_str)
    if coupon:
        price_parts.append(f"<span style='background:#16a34a;color:white;border-radius:4px;padding:1px 6px;font-size:0.78rem'>🎟️ {coupon}</span>")
    if promo:
        _promo_clean = promo.replace("Save","Save ").replace("Savings","").strip()[:40]
        price_parts.append(f"<span style='background:#1d4ed8;color:white;border-radius:4px;padding:1px 6px;font-size:0.78rem'>📦 {_promo_clean}</span>")
    if is_prime:
        price_parts.append(f"<span style='background:#f59e0b;color:#1c1917;border-radius:4px;padding:1px 6px;font-size:0.78rem'>👑 Prime</span>")
    if bought:
        price_parts.append(f"<span style='opacity:0.7;font-size:0.78rem'>🛒 {bought}</span>")
    price_line = "  ".join(price_parts)

    if _is_history:
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e293b,#334155);border-radius:16px;padding:24px;color:white;margin-bottom:16px">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px">
    <div>
      <div style="font-size:0.8rem;opacity:0.6;color:#93c5fd">📅 Загружено из истории</div>
      <div style="font-size:0.85rem;color:#94a3b8;margin-top:4px">Данные листинга недоступны — только оценки AI</div>
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
    else:
        _rat_val = float(rating_h or 0)
        _rat_c = "#22c55e" if _rat_val >= 4.4 else ("#f59e0b" if _rat_val >= 4.3 else "#ef4444")
        _title_c = "#fca5a5" if tlen > 125 else "#86efac"
        st.markdown(f"""
<div style="background:linear-gradient(135deg,#1e293b,#334155);border-radius:16px;padding:24px;color:white;margin-bottom:16px">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px">
    <div>
      <a href="https://www.amazon.com/dp/{asin_h}" target="_blank" style="font-size:0.8rem;opacity:0.6;color:#93c5fd;text-decoration:none">{brand_h} · {asin_h} ↗</a>
      <div style="font-size:1rem;font-weight:600;max-width:520px;line-height:1.4;margin-top:4px">{title_h[:80]}{"..." if tlen>80 else ""}</div>
      <div style="display:flex;gap:10px;margin-top:8px;font-size:0.82rem;flex-wrap:wrap;align-items:center">
        {price_line}
      </div>
      <div style="display:flex;gap:14px;margin-top:6px;font-size:0.82rem;flex-wrap:wrap">
        <span style="color:{_rat_c};font-weight:600">&#11088; {rating_h} ({reviews_h} отз.)</span>
        <span style="opacity:0.8">&#128202; {bsr_h}</span>
        <span style="color:{_title_c}">&#128221; Title: {tlen} симв.</span>
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
    _has_aplus_od = bool(od.get("aplus") or od.get("aplus_content"))
    cols = st.columns(len(items))
    for col,(lbl,val) in zip(cols,items):
        p2 = pct(val)
        if lbl == "Описание" and p2 == 0 and _has_aplus_od:
            col.markdown(
                '<div style="background:#f1f5f9;border-radius:8px;padding:8px 4px;text-align:center;border-left:3px solid #64748b">'
                '<div style="font-size:1rem;font-weight:700;color:#64748b">A+</div>'
                '<div style="font-size:0.68rem;color:#64748b">Описание</div></div>',
                unsafe_allow_html=True)
        else:
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
        Table, TableStyle, HRFlowable, PageBreak, Image as RLImage, KeepTogether)
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.graphics.shapes import Drawing, Rect, String, Line
    from PIL import Image as PILImage

    # ── Palette ───────────────────────────────────────────────────────────────
    C = {
        "navy":    colors.HexColor("#0f172a"),
        "blue":    colors.HexColor("#1d4ed8"),
        "blue2":   colors.HexColor("#3b82f6"),
        "slate":   colors.HexColor("#334155"),
        "muted":   colors.HexColor("#64748b"),
        "light":   colors.HexColor("#f1f5f9"),
        "border":  colors.HexColor("#e2e8f0"),
        "green":   colors.HexColor("#15803d"),
        "green2":  colors.HexColor("#22c55e"),
        "yellow":  colors.HexColor("#d97706"),
        "yellow2": colors.HexColor("#fbbf24"),
        "red":     colors.HexColor("#dc2626"),
        "red2":    colors.HexColor("#ef4444"),
        "white":   colors.white,
        "accent":  colors.HexColor("#7c3aed"),
    }

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm,
        topMargin=16*mm, bottomMargin=16*mm)
    W = A4[0] - 36*mm

    # ── Fonts ─────────────────────────────────────────────────────────────────
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    import os, tempfile, requests as _req

    def _try_font(name, url):
        cache = os.path.join(tempfile.gettempdir(), name)
        if not os.path.exists(cache):
            try:
                r = _req.get(url, timeout=20)
                if r.ok: open(cache,"wb").write(r.content)
            except: return None
        return cache if os.path.exists(cache) else None

    _sys_r = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    _sys_b = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    if os.path.exists(_sys_r) and os.path.exists(_sys_b):
        try:
            pdfmetrics.registerFont(TTFont("F", _sys_r))
            pdfmetrics.registerFont(TTFont("FB", _sys_b))
            _F, _FB = "F", "FB"
        except: _F, _FB = "Helvetica", "Helvetica-Bold"
    else:
        _F, _FB = "Helvetica", "Helvetica-Bold"

    # ── Styles ────────────────────────────────────────────────────────────────
    def ps(name, **kw):
        defaults = dict(fontName=_F, fontSize=9, textColor=C["slate"],
                       spaceAfter=2, leading=13)
        defaults.update(kw)
        return ParagraphStyle(name, **defaults)

    S = {
        "cover_title": ps("ct", fontName=_FB, fontSize=28, textColor=C["white"], spaceAfter=4, leading=34),
        "cover_sub":   ps("cs", fontName=_F,  fontSize=11, textColor=colors.HexColor("#93c5fd"), spaceAfter=3),
        "cover_meta":  ps("cm", fontName=_F,  fontSize=9,  textColor=colors.HexColor("#cbd5e1"), spaceAfter=2),
        "h1":    ps("h1", fontName=_FB, fontSize=13, textColor=C["navy"],  spaceBefore=8, spaceAfter=3, leading=16),
        "h2":    ps("h2", fontName=_FB, fontSize=10, textColor=C["slate"], spaceBefore=5, spaceAfter=2),
        "body":  ps("bd", fontSize=9,  textColor=C["slate"], spaceAfter=2, leading=13),
        "small": ps("sm", fontSize=8,  textColor=C["muted"], spaceAfter=1, leading=11),
        "green": ps("gr", fontSize=9,  textColor=C["green"], fontName=_FB, spaceAfter=2),
        "orange":ps("or", fontSize=9,  textColor=C["yellow"],spaceAfter=2),
        "red":   ps("rd", fontSize=9,  textColor=C["red"],   fontName=_FB, spaceAfter=2),
        "center":ps("cn", fontSize=9,  textColor=C["slate"], alignment=TA_CENTER),
        "action":ps("ac", fontSize=9,  textColor=C["blue"],  fontName=_FB, spaceAfter=2),
        "footer":ps("ft", fontSize=7,  textColor=C["muted"], alignment=TA_CENTER),
    }

    story = []
    date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    ov_pct = pct(result.get("overall_score", 0))

    def score_color(v):
        if v >= 75: return C["green"]
        if v >= 50: return C["yellow"]
        return C["red"]

    def score_bg(v):
        if v >= 75: return colors.HexColor("#dcfce7")
        if v >= 50: return colors.HexColor("#fef9c3")
        return colors.HexColor("#fee2e2")

    def hex_str(c):
        return '#{:02x}{:02x}{:02x}'.format(int(c.red*255),int(c.green*255),int(c.blue*255))

    def score_label(v):
        if v >= 75: return "STRONG"
        if v >= 50: return "NEEDS WORK"
        return "CRITICAL"

    def _clean(s):
        return _re.sub(r"\*+","",str(s or "")).strip()

    # ── COVER PAGE ────────────────────────────────────────────────────────────
    title_val = our_data.get("title", asin)
    price = our_data.get("price","")
    rating = our_data.get("average_rating","")
    brand = our_data.get("brand","")

    # Dark cover block
    cover_tbl = Table([[
        Paragraph(f"Amazon Listing Audit", S["cover_title"]),
        Paragraph(f"<b>{ov_pct}%</b>", ps("ov", fontName=_FB, fontSize=52,
            textColor=score_color(ov_pct) if False else colors.HexColor(
                "#22c55e" if ov_pct>=75 else ("#fbbf24" if ov_pct>=50 else "#ef4444")),
            alignment=TA_RIGHT, leading=56)),
    ]], colWidths=[W*0.65, W*0.35])
    cover_tbl.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("BACKGROUND",(0,0),(-1,-1),C["navy"]),
        ("PADDING",(0,0),(-1,-1),14),
        ("ROUNDEDCORNERS",(0,0),(-1,-1),4),
    ]))
    story.append(cover_tbl)
    story.append(Spacer(1,3*mm))

    # Meta strip
    meta_data = [
        [Paragraph(f"<b>ASIN</b>", S["small"]),
         Paragraph(f'<link href="https://www.amazon.com/dp/{asin}" color="#1d4ed8"><b>{asin}</b></link>', S["body"]),
         Paragraph(f"<b>Дата</b>", S["small"]),
         Paragraph(date_str, S["body"]),
         Paragraph(f"<b>Рейтинг</b>", S["small"]),
         Paragraph(f"{rating}", S["body"])],
        [Paragraph(f"<b>Бренд</b>", S["small"]),
         Paragraph(brand[:30], S["body"]),
         Paragraph(f"<b>Цена</b>", S["small"]),
         Paragraph(price, S["body"]),
         Paragraph(f"<b>Overall</b>", S["small"]),
         Paragraph(f"<font color='{hex_str(score_color(ov_pct))}'><b>{ov_pct}% — {score_label(ov_pct)}</b></font>", S["body"])],
        [Paragraph(f"<b>Листинг</b>", S["small"]),
         Paragraph(_clean(title_val)[:80], ps("tt", fontSize=8, textColor=C["slate"])),
         "", "", "", ""],
    ]
    mt = Table(meta_data, colWidths=[18*mm, W*0.27, 14*mm, W*0.2, 16*mm, W*0.2])
    mt.setStyle(TableStyle([
        ("FONTNAME",(0,0),(-1,-1),_F), ("FONTSIZE",(0,0),(-1,-1),8),
        ("BACKGROUND",(0,0),(-1,-1),C["light"]),
        ("GRID",(0,0),(-1,-1),0.3,C["border"]),
        ("PADDING",(0,0),(-1,-1),5),
        ("SPAN",(1,2),(5,2)),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(mt)
    story.append(Spacer(1,5*mm))

    # ── SCORE DASHBOARD ───────────────────────────────────────────────────────
    story.append(Paragraph("▌ SCORE DASHBOARD", ps("sdh", fontName=_FB, fontSize=11,
        textColor=C["navy"], spaceBefore=4, spaceAfter=3)))

    score_map = [
        ("Title",    result.get("title_score",0)),
        ("Bullets",  result.get("bullets_score",0)),
        ("Описание", result.get("description_score",0)),
        ("Фото",     result.get("images_score",0)),
        ("A+",       result.get("aplus_score",0)),
        ("Отзывы",   result.get("reviews_score",0)),
        ("BSR",      result.get("bsr_score",0)),
        ("Цена",     result.get("price_score",0)),
        ("Варианты", result.get("customization_score",0)),
        ("Prime",    result.get("prime_score",0)),
        ("COSMO",    result.get("cosmo_analysis",{}).get("score",0) if isinstance(result.get("cosmo_analysis"),dict) else 0),
        ("Rufus",    result.get("rufus_analysis",{}).get("score",0) if isinstance(result.get("rufus_analysis"),dict) else 0),
    ]

    # Build 2-column score cards
    _has_aplus_pdf = bool(our_data.get("aplus") or our_data.get("aplus_content"))

    def score_card(label, raw):
        v = pct(raw)
        # Description 0% + A+ present → show as grey "A+"
        if label == "Описание" and v == 0 and _has_aplus_pdf:
            return Table([[
                Paragraph(f"<b>{label}</b>", ps(f"sc_{label}", fontName=_FB, fontSize=8, textColor=C["navy"])),
                Paragraph("<b>A+</b>", ps(f"sv_{label}", fontName=_FB, fontSize=11,
                    alignment=TA_RIGHT, textColor=C["muted"])),
            ],[
                Paragraph("скрыто A+", ps(f"sb_{label}", fontSize=7, textColor=C["muted"])), "",
            ]], colWidths=[35*mm, 18*mm], style=TableStyle([
                ("BACKGROUND",(0,0),(-1,-1),C["light"]),
                ("PADDING",(0,0),(-1,-1),4),
                ("SPAN",(0,1),(1,1)),
                ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ]))
        sc = score_color(v)
        bg = score_bg(v)
        return Table([[
            Paragraph(f"<b>{label}</b>", ps(f"sc_{label}", fontName=_FB, fontSize=8, textColor=C["navy"])),
            Paragraph(f"<font color='{hex_str(sc)}'><b>{v}%</b></font>",
                ps(f"sv_{label}", fontName=_FB, fontSize=11, alignment=TA_RIGHT)),
        ],[
            Table([[""]], colWidths=[max(2, int(v * 0.5))*mm if max(2, int(v * 0.5))*mm < 40*mm else 40*mm],
                style=[("BACKGROUND",(0,0),(-1,-1),sc),("ROWHEIGHTS",(0,0),(-1,-1),3)]),
            "",
        ]], colWidths=[35*mm, 18*mm], style=TableStyle([
            ("BACKGROUND",(0,0),(-1,-1),bg),
            ("PADDING",(0,0),(-1,-1),4),
            ("SPAN",(0,1),(1,1)),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ]))

    # 3 columns × 4 rows
    n = len(score_map)
    cols3 = 3
    rows3 = (n + cols3 - 1) // cols3
    dash_rows = []
    for ri in range(rows3):
        row = []
        for ci in range(cols3):
            idx = ri * cols3 + ci
            if idx < n:
                lbl, val = score_map[idx]
                row.append(score_card(lbl, val))
            else:
                row.append("")
        dash_rows.append(row)

    dash_tbl = Table(dash_rows, colWidths=[W/3-1*mm]*3)
    dash_tbl.setStyle(TableStyle([
        ("PADDING",(0,0),(-1,-1),1.5),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
    ]))
    story.append(dash_tbl)
    story.append(Spacer(1,5*mm))

    # ── PRIORITY ACTIONS ──────────────────────────────────────────────────────
    _prio = result.get("priority_improvements",[])
    _acts = result.get("actions",[])
    if _prio or _acts:
        story.append(Paragraph("▌ ПРИОРИТЕТНЫЕ ДЕЙСТВИЯ", ps("pah", fontName=_FB, fontSize=11,
            textColor=C["navy"], spaceBefore=2, spaceAfter=3)))
        _all = []
        for item in _prio:
            _all.append({"action": _clean(item), "impact":"HIGH","effort":"","details":""})
        for a in _acts:
            if isinstance(a,dict):
                _all.append(a)
        _all.sort(key=lambda x:{"HIGH":0,"MEDIUM":1,"LOW":2}.get(x.get("impact","MEDIUM"),1))

        for a in _all[:8]:
            _imp = a.get("impact","MEDIUM")
            _ic = C["red"] if _imp=="HIGH" else (C["yellow"] if _imp=="MEDIUM" else C["green"])
            _ibg= colors.HexColor("#fee2e2") if _imp=="HIGH" else (colors.HexColor("#fef9c3") if _imp=="MEDIUM" else colors.HexColor("#dcfce7"))
            _act_row = Table([[
                Paragraph(f"<b>{_imp}</b>", ps(f"imp_{_imp}", fontName=_FB, fontSize=7,
                    textColor=_ic, alignment=TA_CENTER)),
                Paragraph(_clean(a.get("action","")), ps("act_t", fontSize=9, textColor=C["navy"])),
            ]], colWidths=[14*mm, W-14*mm])
            _act_row.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(0,0),_ibg),
                ("BACKGROUND",(1,0),(1,0),C["white"]),
                ("LINEBELOW",(0,0),(-1,-1),0.3,C["border"]),
                ("PADDING",(0,0),(-1,-1),5),
                ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ]))
            story.append(_act_row)
            if a.get("details"):
                story.append(Paragraph(f"  → {_clean(a['details'])[:180]}", S["small"]))

    story.append(PageBreak())

    # ── PHOTO ANALYSIS ────────────────────────────────────────────────────────
    story.append(Paragraph("▌ АНАЛИЗ ФОТОГРАФИЙ", ps("ph", fontName=_FB, fontSize=11,
        textColor=C["navy"], spaceBefore=0, spaceAfter=4)))

    _blocks = {}
    if vision_text:
        for _m in _re.finditer(r"PHOTO_BLOCK_(\d+)\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", vision_text, _re.DOTALL):
            _blocks[int(_m.group(1))] = _m.group(2).strip()

    for i, img_d in enumerate(images[:5]):
        blk = _blocks.get(i+1,"")
        sc_m  = _re.search(r"(\d+)/10", blk)
        typ_m = _re.search(r"(?:Тип|Type)\s*[:\-]\s*(.+)", blk)
        str_m = _re.search(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.{3,})", blk)
        wk_m  = _re.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{3,})", blk)
        ac_m  = _re.search(r"(?:Действие|Action)\s*[:\-]\s*(.{3,})", blk)
        cv_m  = _re.search(r"(?:Конверсия|Conversion)\s*[:\-]\s*(.{3,})", blk)
        em_m  = _re.search(r"(?:Эмоция|Emotion)\s*[:\-]\s*(.{3,})", blk)
        sc_v  = int(sc_m.group(1)) if sc_m else 0
        sc_c  = score_color(sc_v*10)
        ptype = _clean(typ_m.group(1)) if typ_m else ""
        stxt  = _clean(str_m.group(1)) if str_m else ""
        wtxt  = _clean(wk_m.group(1))  if wk_m  else ""
        atxt  = _clean(ac_m.group(1))  if ac_m  else ""
        ctxt  = _clean(cv_m.group(1))  if cv_m  else ""
        etxt  = _clean(em_m.group(1))  if em_m  else ""
        sc_lbl = "Отлично" if sc_v>=8 else ("Хорошо" if sc_v>=6 else "Слабо")

        try:
            _b64 = img_d.get("b64","") if isinstance(img_d,dict) else img_d
            _bytes = base64.b64decode(_b64)
            _pil = PILImage.open(io.BytesIO(_bytes)).convert("RGB")
            _pil.thumbnail((200,200))
            _tb = io.BytesIO(); _pil.save(_tb,"JPEG",quality=80); _tb.seek(0)
            _rl = RLImage(_tb, width=40*mm, height=40*mm)
        except: _rl = Paragraph(f"#{i+1}", S["small"])

        # Info content
        info = []
        hdr_txt = f"Фото #{i+1}" + (f" — {ptype}" if ptype else "")

        # Header row with score
        info.append(Table([[
            Paragraph(f"<b>{hdr_txt}</b>",
                ps("phdr", fontName=_FB, fontSize=10, textColor=C["navy"])),
            Paragraph(
                f"<font color='{hex_str(sc_c)}'><b>{sc_v}/10</b></font>  "
                f"<font color='{hex_str(sc_c)}'>{sc_lbl}</font>",
                ps("psc", fontName=_FB, fontSize=11, alignment=TA_RIGHT)),
        ]], colWidths=[W*0.45, 28*mm], style=[
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ("PADDING",(0,0),(-1,-1),0),
            ("LINEBELOW",(0,0),(-1,-1),1.5,sc_c),
        ]))
        info.append(Spacer(1,1.5*mm))

        # Score bar — colored segments
        bar_cells = []
        for _b in range(10):
            _bc = sc_c if _b < sc_v else C["border"]
            bar_cells.append(Table([[""]], colWidths=[4.8*mm],
                style=[("BACKGROUND",(0,0),(-1,-1),_bc),
                       ("ROWHEIGHTS",(0,0),(-1,-1),6)]))
        info.append(Table([bar_cells], colWidths=[4.8*mm]*10,
            style=[("PADDING",(0,0),(-1,-1),0.5)]))
        info.append(Spacer(1,2.5*mm))

        if stxt:
            st_box = Table([[Paragraph(f"✅  {stxt}", ps("pst", fontSize=9, textColor=C["green"]))]],
                colWidths=[W-40*mm-4*mm])
            st_box.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#f0fdf4")),
                ("LINEBEFORE",(0,0),(0,-1),2,C["green2"]),
                ("PADDING",(0,0),(-1,-1),5),
            ]))
            info.append(st_box)
            info.append(Spacer(1,1.5*mm))
        if wtxt:
            wt_box = Table([[Paragraph(f"⚠  {wtxt}", ps("pwt", fontSize=9, textColor=C["yellow"]))]],
                colWidths=[W-40*mm-4*mm])
            wt_box.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#fffbeb")),
                ("LINEBEFORE",(0,0),(0,-1),2,C["yellow2"]),
                ("PADDING",(0,0),(-1,-1),5),
            ]))
            info.append(wt_box)
            info.append(Spacer(1,1.5*mm))
        if atxt:
            info.append(Paragraph(f"<b>→</b> {atxt}",
                ps("pat", fontSize=9, textColor=C["blue"], fontName=_FB)))
        if ctxt:
            info.append(Paragraph(f"💡 {ctxt}", ps("pct", fontSize=8, textColor=C["muted"])))
        if etxt:
            em_box = Table([[Paragraph(
                f"<font color='{hex_str(C['accent'])}'><b>😶 ЭМОЦИЯ:</b></font>  {etxt}",
                ps("pet", fontSize=8, textColor=C["slate"]))]],
                colWidths=[W-40*mm-4*mm])
            em_box.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#faf5ff")),
                ("LINEBEFORE",(0,0),(0,-1),2,C["accent"]),
                ("PADDING",(0,0),(-1,-1),5),
            ]))
            info.append(Spacer(1,1.5*mm))
            info.append(em_box)

        photo_row = Table([[_rl, info]], colWidths=[43*mm, W-43*mm])
        photo_row.setStyle(TableStyle([
            ("VALIGN",(0,0),(-1,-1),"TOP"),
            ("BACKGROUND",(0,0),(-1,-1),C["white"]),
            ("LINEBELOW",(0,0),(-1,-1),0.5,C["border"]),
            ("LINEBEFORE",(0,0),(0,-1),3,sc_c),
            ("PADDING",(0,0),(0,0),5),
            ("PADDING",(1,0),(1,0),6),
        ]))
        story.append(KeepTogether([photo_row, Spacer(1,2*mm)]))

    # ── A+ ────────────────────────────────────────────────────────────────────
    _av   = st.session_state.get("aplus_vision","")
    _aurls= st.session_state.get("aplus_img_urls",[])
    if _av or _aurls:
        story.append(PageBreak())
        story.append(Paragraph("▌ A+ КОНТЕНТ", ps("aph", fontName=_FB, fontSize=11,
            textColor=C["navy"], spaceBefore=0, spaceAfter=4)))
        _ap_s = pct(result.get("aplus_score",0))
        story.append(Paragraph(
            f"A+ Score: <font color='{hex_str(score_color(_ap_s))}'><b>{_ap_s}%</b></font>",
            S["h2"]))
        story.append(Spacer(1,2*mm))

        _apblks = {}
        if _av:
            for _m in _re.finditer(r"APLUS_BLOCK_(\d+)\s*(.*?)(?=APLUS_BLOCK_\d+|$)",_av,_re.DOTALL):
                _apblks[int(_m.group(1))] = _m.group(2).strip()

        for _bi in range(max(len(_aurls),len(_apblks))):
            _bblk = _apblks.get(_bi+1,"")
            _bmod = _re.search(r"(?:Модуль|Module)\s*[:\-]\s*(.+)", _bblk)
            _bsc  = _re.search(r"(\d+)/10", _bblk)
            _bstr = _re.search(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.{3,})", _bblk)
            _bwk  = _re.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{3,})", _bblk)
            _bact = _re.search(r"(?:Действие|Action)\s*[:\-]\s*(.{3,})", _bblk)
            _bcv  = _re.search(r"(?:Конверсия|Conversion)\s*[:\-]\s*(.{3,})", _bblk)
            _bscv = int(_bsc.group(1)) if _bsc else 0
            _bscc = score_color(_bscv*10)

            _ap_info = [Paragraph(
                f"<b>Баннер #{_bi+1}" + (f" — {_clean(_bmod.group(1))}" if _bmod else "") +
                f"</b>  <font color='{hex_str(_bscc)}'>{_bscv}/10</font>",
                ps("aphi", fontName=_FB, fontSize=9, textColor=C["navy"]))]
            if _bstr: _ap_info.append(Paragraph(f"✅ {_clean(_bstr.group(1))}", S["green"]))
            if _bwk:  _ap_info.append(Paragraph(f"⚠ {_clean(_bwk.group(1))}", S["orange"]))
            if _bact: _ap_info.append(Paragraph(f"→ {_clean(_bact.group(1))}", S["action"]))
            if _bcv:  _ap_info.append(Paragraph(f"💡 {_clean(_bcv.group(1))}", S["body"]))

            _ap_img = ""
            if _bi < len(_aurls):
                try:
                    _apr = _req.get(_aurls[_bi], timeout=10, headers={"User-Agent":"Mozilla/5.0"})
                    if _apr.ok:
                        _apil = PILImage.open(io.BytesIO(_apr.content)).convert("RGB")
                        _apil.thumbnail((500,200))
                        _ab = io.BytesIO(); _apil.save(_ab,"JPEG",quality=70); _ab.seek(0)
                        _ap_img = RLImage(_ab, width=W, height=35*mm)
                except: pass

            _ap_blk_parts = []
            if _ap_img: _ap_blk_parts.append(_ap_img)
            _ap_blk_parts += _ap_info + [Spacer(1,3*mm)]
            story.append(KeepTogether(_ap_blk_parts))

    # ── CONTENT ANALYSIS ─────────────────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("▌ АНАЛИЗ КОНТЕНТА", ps("cah", fontName=_FB, fontSize=11,
        textColor=C["navy"], spaceBefore=0, spaceAfter=4)))

    for sec_key, sec_score_key, sec_name in [
        ("title","title_score","Title"),
        ("bullets","bullets_score","Bullets"),
        ("description","description_score","Description"),
        ("aplus","aplus_score","A+ Контент"),
    ]:
        sc_v = pct(result.get(sec_score_key,0))
        sc_c = score_color(sc_v)
        gaps = result.get(f"{sec_key}_gaps",[])
        rec  = result.get(f"{sec_key}_rec","")
        hdr_row = Table([[
            Paragraph(f"<b>{sec_name}</b>",
                ps(f"ch_{sec_key}", fontName=_FB, fontSize=10, textColor=C["navy"])),
            Paragraph(f"<font color='{hex_str(sc_c)}'><b>{sc_v}%</b></font>",
                ps(f"cs_{sec_key}", fontName=_FB, fontSize=14, alignment=TA_RIGHT)),
        ]], colWidths=[W-25*mm, 25*mm])
        hdr_row.setStyle(TableStyle([
            ("LINEBELOW",(0,0),(-1,-1),1.5,sc_c),
            ("PADDING",(0,0),(-1,-1),4),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ]))
        story.append(hdr_row)
        if gaps:
            for g in gaps[:3]: story.append(Paragraph(f"⚠ {_clean(g)}", S["orange"]))
        if rec: story.append(Paragraph(f"→ {_clean(rec)}", S["action"]))
        story.append(Spacer(1,3*mm))

    # Raw content
    _title_txt = our_data.get("title","")
    _bullets = our_data.get("feature_bullets",[])
    if _title_txt:
        story.append(Paragraph("Title:", S["h2"]))
        story.append(Paragraph(_clean(_title_txt)[:200], S["body"]))
        story.append(Spacer(1,2*mm))
    if _bullets:
        story.append(Paragraph("Bullets:", S["h2"]))
        for b in _bullets[:5]:
            _bl = len(b.encode())
            _bc = C["red"] if _bl>255 else C["slate"]
            story.append(Paragraph(
                f"<font color='{hex_str(_bc)}'>{'🔴' if _bl>255 else '✅'}</font> {_clean(b)[:200]} <font color='{hex_str(C['muted'])}'>[{_bl}b]</font>",
                S["body"]))

    # ── COSMO / RUFUS / JTBD / VPC ───────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("▌ AI ВИДИМОСТЬ — COSMO / RUFUS", ps("aih", fontName=_FB, fontSize=11,
        textColor=C["navy"], spaceBefore=0, spaceAfter=4)))

    _ca = result.get("cosmo_analysis",{})
    _ra = result.get("rufus_analysis",{})
    _cs = pct(_ca.get("score",0))
    _rs = pct(_ra.get("score",0))

    ai_score_tbl = Table([[
        Table([[
            Paragraph(f"<b>{_cs}%</b>", ps("csp", fontName=_FB, fontSize=26,
                textColor=score_color(_cs), alignment=TA_CENTER)),
            Paragraph("COSMO Score", ps("csl", fontSize=8, alignment=TA_CENTER, textColor=C["muted"])),
        ]], style=[("BACKGROUND",(0,0),(-1,-1),score_bg(_cs)),
                   ("PADDING",(0,0),(-1,-1),8),("ROUNDEDCORNERS",(0,0),(-1,-1),3)]),
        Table([[
            Paragraph(f"<b>{_rs}%</b>", ps("rsp", fontName=_FB, fontSize=26,
                textColor=score_color(_rs), alignment=TA_CENTER)),
            Paragraph("Rufus Score", ps("rsl", fontSize=8, alignment=TA_CENTER, textColor=C["muted"])),
        ]], style=[("BACKGROUND",(0,0),(-1,-1),score_bg(_rs)),
                   ("PADDING",(0,0),(-1,-1),8),("ROUNDEDCORNERS",(0,0),(-1,-1),3)]),
    ]], colWidths=[W/2-2*mm, W/2-2*mm])
    ai_score_tbl.setStyle(TableStyle([("PADDING",(0,0),(-1,-1),2)]))
    story.append(ai_score_tbl)
    story.append(Spacer(1,4*mm))

    _sig_p = _ca.get("signals_present",[])
    _sig_m = _ca.get("signals_missing",[])
    if _sig_p or _sig_m:
        sig_tbl = Table([[
            [Paragraph("<b>✅ Присутствуют</b>", S["h2"])] +
            [Paragraph(f"• {_clean(s)}", S["green"]) for s in _sig_p[:6]],
            [Paragraph("<b>❌ Отсутствуют</b>", S["h2"])] +
            [Paragraph(f"• {_clean(s)}", S["orange"]) for s in _sig_m[:6]],
        ]], colWidths=[W/2-2*mm, W/2-2*mm])
        sig_tbl.setStyle(TableStyle([
            ("VALIGN",(0,0),(-1,-1),"TOP"),
            ("BACKGROUND",(0,0),(0,0),colors.HexColor("#f0fdf4")),
            ("BACKGROUND",(1,0),(1,0),colors.HexColor("#fffbeb")),
            ("GRID",(0,0),(-1,-1),0.3,C["border"]),
            ("PADDING",(0,0),(-1,-1),6),
        ]))
        story.append(sig_tbl)

    if _ra.get("issues"):
        story.append(Spacer(1,3*mm))
        story.append(Paragraph("<b>Rufus Issues:</b>", S["h2"]))
        for iss in _ra["issues"][:4]:
            story.append(Paragraph(f"⚠ {_clean(iss)}", S["orange"]))

    # JTBD
    _jtbd = result.get("jtbd_analysis",{})
    if _jtbd:
        story.append(Spacer(1,5*mm))
        story.append(Paragraph("▌ JTBD — JOBS TO BE DONE", ps("jh", fontName=_FB, fontSize=11,
            textColor=C["navy"], spaceAfter=3)))
        _js = pct(_jtbd.get("alignment_score",0))
        story.append(Paragraph(
            f"Alignment Score: <font color='{hex_str(score_color(_js))}'><b>{_js}%</b></font>",
            S["body"]))
        if _jtbd.get("job_story"):
            js_box = Table([[Paragraph(_clean(_jtbd["job_story"]),
                ps("jbs", fontSize=9, textColor=C["navy"], fontName=_FB, leading=14))]],
                colWidths=[W])
            js_box.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#f0f9ff")),
                ("LINEBELOW",(0,0),(-1,-1),2,C["blue2"]),
                ("PADDING",(0,0),(-1,-1),8),
            ]))
            story.append(Spacer(1,2*mm))
            story.append(js_box)
            story.append(Spacer(1,3*mm))

        j_rows = []
        for jkey, jlbl, jclr in [
            ("functional_job","⚙️ Функциональная",C["blue"]),
            ("emotional_job","❤️ Эмоциональная",C["red"]),
            ("social_job","👥 Социальная",C["accent"]),
        ]:
            if _jtbd.get(jkey):
                j_rows.append(Table([[
                    Paragraph(f"<b>{jlbl}</b>", ps(f"jl_{jkey}", fontName=_FB, fontSize=8, textColor=jclr)),
                    Paragraph(_clean(_jtbd[jkey]), ps(f"jv_{jkey}", fontSize=8, textColor=C["slate"])),
                ]], colWidths=[30*mm, W-30*mm], style=[
                    ("LINEBELOW",(0,0),(-1,-1),0.3,C["border"]),
                    ("PADDING",(0,0),(-1,-1),4),
                ]))
        for jr in j_rows: story.append(jr)

        if _jtbd.get("jtbd_gaps"):
            story.append(Spacer(1,2*mm))
            story.append(Paragraph("<b>Gaps:</b>", S["h2"]))
            for g in _jtbd["jtbd_gaps"][:4]:
                story.append(Paragraph(f"✗ {_clean(g)}", S["red"]))
        if _jtbd.get("jtbd_recs"):
            story.append(Paragraph("<b>Рекомендации:</b>", S["h2"]))
            for rec in _jtbd["jtbd_recs"][:4]:
                story.append(Paragraph(f"→ {_clean(rec)}", S["action"]))

    # VPC
    _vpc = result.get("vpc_analysis",{})
    if _vpc:
        story.append(PageBreak())
        story.append(Paragraph("▌ VALUE PROPOSITION CANVAS", ps("vh", fontName=_FB, fontSize=11,
            textColor=C["navy"], spaceAfter=3)))
        _vfit = pct(_vpc.get("fit_score",0))
        story.append(Paragraph(
            f"VPC Fit Score: <font color='{hex_str(score_color(_vfit))}'><b>{_vfit}%</b></font>  |  "
            f"Value Gap: <b>{100-_vfit}%</b>",
            S["body"]))
        if _vpc.get("vpc_verdict"):
            verd_box = Table([[Paragraph(
                f"<b>AI CRO Консультант:</b> {_clean(_vpc['vpc_verdict'])}",
                ps("vb", fontSize=9, textColor=C["navy"], leading=13))]],
                colWidths=[W])
            verd_box.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#faf5ff")),
                ("LINEBEFORE",(0,0),(0,-1),3,C["accent"]),
                ("PADDING",(0,0),(-1,-1),8),
            ]))
            story.append(Spacer(1,2*mm))
            story.append(verd_box)
            story.append(Spacer(1,3*mm))

        def _bullets_list(items, icon, clr):
            return [Paragraph(f"<font color='{hex_str(clr)}'>{icon}</font> {_clean(it)}",
                ps(f"vp_{icon}", fontSize=8, textColor=C["slate"])) for it in (items or [])[:4]]

        vpc_tbl = Table([[
            [Paragraph("<b>👤 Профиль покупателя</b>", ps("vclh", fontName=_FB, fontSize=9, textColor=C["navy"])),
             Paragraph("<b>Jobs</b>", ps("vjh", fontName=_FB, fontSize=8, textColor=C["blue"]))] +
             _bullets_list(_vpc.get("customer_jobs",[]),"◆",C["blue"]) +
            [Paragraph("<b>Pains</b>", ps("vph2", fontName=_FB, fontSize=8, textColor=C["red"]))] +
             _bullets_list(_vpc.get("customer_pains",[]),"◆",C["red"]),
            [Paragraph("<b>📦 Карта ценности</b>", ps("vvlh", fontName=_FB, fontSize=9, textColor=C["navy"])),
             Paragraph("<b>✅ Закрывает</b>", ps("vprh", fontName=_FB, fontSize=8, textColor=C["green"]))] +
             _bullets_list(_vpc.get("pain_relievers_present",[]),"✓",C["green"]) +
            [Paragraph("<b>❌ Не закрывает</b>", ps("vmh", fontName=_FB, fontSize=8, textColor=C["red"]))] +
             _bullets_list(_vpc.get("pain_relievers_missing",[]),"✗",C["red"]),
        ]], colWidths=[W/2-2*mm, W/2-2*mm])
        vpc_tbl.setStyle(TableStyle([
            ("VALIGN",(0,0),(-1,-1),"TOP"),
            ("BACKGROUND",(0,0),(0,0),colors.HexColor("#f0f9ff")),
            ("BACKGROUND",(1,0),(1,0),colors.HexColor("#f0fdf4")),
            ("GRID",(0,0),(-1,-1),0.3,C["border"]),
            ("PADDING",(0,0),(-1,-1),7),
        ]))
        story.append(vpc_tbl)

    # ── FULL ACTIONS + MISSING CHARS + STOP WORDS ─────────────────────────────
    _fa = result.get("actions",[])
    _mc = result.get("missing_chars",[])
    _sw = check_listing_stop_words(our_data)

    if _fa or _mc or _sw:
        story.append(PageBreak())

    if _fa:
        story.append(Paragraph("▌ ПОЛНЫЙ ПЛАН ДЕЙСТВИЙ", ps("fah", fontName=_FB, fontSize=11,
            textColor=C["navy"], spaceAfter=3)))
        _fa_sorted = sorted(_fa, key=lambda x: {"HIGH":0,"MEDIUM":1,"LOW":2}.get(x.get("impact","MEDIUM"),1))
        for a in _fa_sorted:
            if not isinstance(a,dict): continue
            _imp = a.get("impact","MEDIUM")
            _ic = C["red"] if _imp=="HIGH" else (C["yellow"] if _imp=="MEDIUM" else C["green"])
            _ibg= colors.HexColor("#fee2e2") if _imp=="HIGH" else (colors.HexColor("#fef9c3") if _imp=="MEDIUM" else colors.HexColor("#dcfce7"))
            story.append(Table([[
                Paragraph(f"<b>{_imp}</b>", ps(f"fa_{_imp}", fontName=_FB, fontSize=7,
                    textColor=_ic, alignment=TA_CENTER)),
                [Paragraph(_clean(a.get("action","")),
                    ps("fat", fontName=_FB, fontSize=9, textColor=C["navy"]))] +
                ([Paragraph(_clean(a["details"])[:200], S["small"])] if a.get("details") else []),
            ]], colWidths=[14*mm, W-14*mm], style=[
                ("BACKGROUND",(0,0),(0,0),_ibg),
                ("LINEBELOW",(0,0),(-1,-1),0.3,C["border"]),
                ("PADDING",(0,0),(-1,-1),5),
                ("VALIGN",(0,0),(-1,-1),"TOP"),
            ]))

    if _mc:
        story.append(Spacer(1,5*mm))
        story.append(Paragraph("▌ ОТСУТСТВУЮЩИЕ ХАРАКТЕРИСТИКИ", ps("mch", fontName=_FB,
            fontSize=11, textColor=C["navy"], spaceAfter=3)))
        _mc_h = [c for c in _mc if c.get("priority")=="HIGH"]
        _mc_m = [c for c in _mc if c.get("priority")!="HIGH"]
        for ch in (_mc_h+_mc_m)[:10]:
            _cp = ch.get("priority","")
            _cc = C["red"] if _cp=="HIGH" else C["yellow"]
            story.append(Table([[
                Paragraph(f"<b>[{_cp}]</b>", ps(f"mc_{_cp}", fontName=_FB, fontSize=7,
                    textColor=_cc, alignment=TA_CENTER)),
                [Paragraph(f"<b>{_clean(ch.get('name',''))}</b>",
                    ps("mcn", fontName=_FB, fontSize=9, textColor=C["navy"])),
                 Paragraph(_clean(ch.get("how_competitors_use",""))[:150], S["small"])],
            ]], colWidths=[14*mm, W-14*mm], style=[
                ("LINEBELOW",(0,0),(-1,-1),0.3,C["border"]),
                ("PADDING",(0,0),(-1,-1),4),
                ("VALIGN",(0,0),(-1,-1),"TOP"),
            ]))

    if _sw:
        story.append(Spacer(1,4*mm))
        story.append(Paragraph("▌ AMAZON STOP WORDS", ps("swh", fontName=_FB, fontSize=11,
            textColor=C["navy"], spaceAfter=3)))
        for field, found in _sw.items():
            if found.get("do_not_use"):
                story.append(Paragraph(f"<b>{field}</b> — 🚫 Запрещено: " +
                    ", ".join(found["do_not_use"]), S["red"]))
            if found.get("try_to_avoid"):
                story.append(Paragraph(f"<b>{field}</b> — ⚠ Нежелательно: " +
                    ", ".join(found["try_to_avoid"]), S["orange"]))

    # ── COMPETITORS ───────────────────────────────────────────────────────────
    if comp_data:
        story.append(PageBreak())
        story.append(Paragraph("▌ АНАЛИЗ КОНКУРЕНТОВ", ps("comph", fontName=_FB, fontSize=11,
            textColor=C["navy"], spaceAfter=4)))

        comp_hdr = [[
            Paragraph("<b>ASIN</b>", S["small"]),
            Paragraph("<b>Title</b>", S["small"]),
            Paragraph("<b>Цена</b>", S["small"]),
            Paragraph("<b>★</b>", S["small"]),
            Paragraph("<b>Overall</b>", S["small"]),
        ]]
        for i, comp in enumerate(comp_data[:5]):
            _cai = st.session_state.get(f"comp_ai_{i}",{})
            _cov = pct(_cai.get("overall_score",0)) if _cai else 0
            _coc = score_color(_cov)
            comp_hdr.append([
                Paragraph(get_asin_from_data(comp), S["small"]),
                Paragraph(_clean(comp.get("title",""))[:45], S["small"]),
                Paragraph(str(comp.get("price","")), S["small"]),
                Paragraph(str(comp.get("average_rating","")), S["small"]),
                Paragraph(f"<font color='{hex_str(_coc)}'><b>{_cov}%</b></font>",
                    ps("cov", fontName=_FB, fontSize=9, alignment=TA_CENTER)),
            ])

        comp_tbl = Table(comp_hdr, colWidths=[26*mm, 80*mm, 18*mm, 12*mm, 18*mm])
        comp_tbl.setStyle(TableStyle([
            ("FONTNAME",(0,0),(-1,0),_FB), ("FONTSIZE",(0,0),(-1,-1),8),
            ("BACKGROUND",(0,0),(-1,0),C["navy"]),
            ("TEXTCOLOR",(0,0),(-1,0),C["white"]),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[C["light"], C["white"]]),
            ("GRID",(0,0),(-1,-1),0.3,C["border"]),
            ("PADDING",(0,0),(-1,-1),5),
        ]))
        story.append(comp_tbl)

        for i, comp in enumerate(comp_data[:3]):
            _cb = comp.get("feature_bullets",[])
            if not _cb: continue
            story.append(Spacer(1,4*mm))
            story.append(Paragraph(
                f"<b>Конкурент {i+1}</b> — {get_asin_from_data(comp)}",
                ps(f"cbh_{i}", fontName=_FB, fontSize=9, textColor=C["slate"])))
            for b in _cb[:5]:
                story.append(Paragraph(f"• {_clean(b)[:200]}", S["small"]))

    # ── FOOTER ────────────────────────────────────────────────────────────────
    story.append(Spacer(1,6*mm))
    footer_tbl = Table([[
        Paragraph(f"Amazon Listing Analyzer  |  ASIN: {asin}  |  {date_str}", S["footer"]),
    ]], colWidths=[W])
    footer_tbl.setStyle(TableStyle([
        ("LINEABOVE",(0,0),(-1,-1),0.5,C["border"]),
        ("PADDING",(0,0),(-1,-1),4),
    ]))
    story.append(footer_tbl)

    doc.build(story)
    buf.seek(0)
    return buf.read()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Обзор
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Обзор":
    st.title("🏠 Обзор листинга")
    health_card()

    # ── Frequently Returned warning ───────────────────────────────────────────
    if od.get("is_frequently_returned"):
        st.markdown("""
<div style="background:#7f1d1d;border:2px solid #ef4444;border-radius:10px;padding:14px 18px;margin:8px 0">
<div style="font-size:1.1rem;font-weight:800;color:#fca5a5">🔴 ВНИМАНИЕ: Amazon пометил листинг как "Часто возвращают"</div>
<div style="color:#fca5a5;font-size:0.88rem;margin-top:6px;line-height:1.7">
Amazon показывает покупателям предупреждение прямо на странице товара — это <b>критично убивает конверсию</b>.<br><br>
<b>Как убрать метку:</b><br>
1. Проверь соответствие фото и описания реальному товару — самая частая причина<br>
2. Исправь размерную сетку — добавь точные замеры, фото на модели с указанием роста<br>
3. Добавь видео распаковки — снижает ожидание vs реальность<br>
4. Улучши упаковку — товар должен доходить без повреждений<br>
5. Снизи % возвратов ниже ~10% за 30 дней — только тогда Amazon снимет метку
</div>
</div>""", unsafe_allow_html=True)

    # Return analysis button — available always (not only for frequently_returned)
    _our_asin_ret = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")
    if _our_asin_ret:
        _ret_col1, _ret_col2 = st.columns([2, 5])
        with _ret_col1:
            if st.button("🔍 Анализ возвратов (1★+2★+3★)", key="btn_return_analysis", use_container_width=True):
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

        # Merge priority_improvements + actions into unified card list
        _all_actions = []
        for item in priority_improvements:
            _all_actions.append({"action": item, "impact": "HIGH", "effort": "MEDIUM", "details": ""})
        for a in actions:
            if isinstance(a, dict):
                _all_actions.append(a)

        # Sort: HIGH first
        _order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        _all_actions.sort(key=lambda x: _order.get(x.get("impact","MEDIUM"), 1))

        # Group by impact
        _high = [a for a in _all_actions if a.get("impact","") == "HIGH"]
        _med  = [a for a in _all_actions if a.get("impact","") == "MEDIUM"]
        _low  = [a for a in _all_actions if a.get("impact","") == "LOW"]

        def _action_cards(items, color, label, icon):
            if not items: return
            st.markdown(f'<div style="font-size:0.75rem;font-weight:700;color:{color};letter-spacing:0.08em;margin:12px 0 6px">{icon} {label} — {len(items)} действий</div>', unsafe_allow_html=True)
            for i, a in enumerate(items):
                _effort = a.get("effort","MEDIUM")
                _effort_c = {"LOW":"#22c55e","MEDIUM":"#f59e0b","HIGH":"#ef4444"}.get(_effort,"#94a3b8")
                _act_text = a.get("action","")
                _det_text = a.get("details","")
                st.markdown(f"""<div style="background:#0f172a;border-left:4px solid {color};border-radius:8px;padding:12px 16px;margin-bottom:8px">
<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
  <div style="font-size:0.9rem;font-weight:600;color:#e2e8f0;flex:1">{_act_text}</div>
  <span style="background:{_effort_c}22;color:{_effort_c};border:1px solid {_effort_c};border-radius:4px;padding:2px 8px;font-size:0.72rem;font-weight:700;white-space:nowrap">⚡ {_effort}</span>
</div>
{f'<div style="font-size:0.8rem;color:#94a3b8;margin-top:6px;line-height:1.5">{_det_text}</div>' if _det_text else ''}
</div>""", unsafe_allow_html=True)

        _action_cards(_high, "#ef4444", "КРИТИЧНО", "🔴")
        _action_cards(_med,  "#f59e0b", "ВАЖНО",    "🟡")
        _action_cards(_low,  "#22c55e", "УЛУЧШЕНИЕ","🟢")

    if r.get("missing_chars"):
        st.subheader("🔍 Отсутствующие характеристики")
        _mc_high = [c for c in r["missing_chars"] if c.get("priority","") == "HIGH"]
        _mc_med  = [c for c in r["missing_chars"] if c.get("priority","") != "HIGH"]
        for _mc_group, _mc_color in [(_mc_high,"#ef4444"), (_mc_med,"#f59e0b")]:
            for ch in _mc_group:
                st.markdown(f"""<div style="background:#0f172a;border-left:4px solid {_mc_color};border-radius:8px;padding:10px 14px;margin-bottom:6px">
<div style="font-size:0.88rem;font-weight:600;color:#e2e8f0">{ch.get('name','')}</div>
<div style="font-size:0.78rem;color:#94a3b8;margin-top:3px">{ch.get('how_competitors_use','')}</div>
</div>""", unsafe_allow_html=True)

    st.divider()
    st.subheader("🤖 AI Инструменты")

    _tool_cols = st.columns(5)

    # 1. AI Listing Rewriter
    with _tool_cols[0]:
        if st.button("✍️ Переписать листинг", use_container_width=True, key="btn_rewriter"):
            with st.spinner("✍️ AI пишет title + 5 буллетов..."):
                _rw_prompt = f"""You are an expert Amazon listing copywriter. Using the full analysis below, write an optimized listing.

PRODUCT: {od.get('title','')}
ASIN: {od.get('parent_asin','')}

ANALYSIS CONTEXT:
- VPC gaps: {r.get('vpc_analysis',{}).get('pain_relievers_missing',[])}
- JTBD Job Story: {r.get('jtbd_analysis',{}).get('job_story','')}
- Title gaps: {r.get('title_gaps',[])}
- Bullets gaps: {r.get('bullets_gaps',[])}
- Stop words to avoid: check all output

Write:
1. TITLE (max 125 chars, include material+type+gender+use case)
2. BULLET 1-5 (max 200 chars each, format: "Feature: Benefit. Context.")

Rules: NO stop words (free/best/guarantee/organic/natural/safe), include job context, temperature ranges if relevant, quantify benefits.
Respond in {('Russian' if st.session_state.get('analysis_lang','ru')=='ru' else 'English')}."""
                _rw = ai_call("Amazon listing copywriter. Write compelling, compliant copy.", _rw_prompt, max_tokens=1500)
                st.session_state["_ai_rewrite"] = _rw

    # 2. Keyword Gap
    with _tool_cols[1]:
        if st.button("🔑 Keyword Gap", use_container_width=True, key="btn_kwgap"):
            with st.spinner("🔑 Анализирую keyword gaps..."):
                _comps = st.session_state.get("comp_data_list", [])
                _our_words = set((od.get("title","") + " " + " ".join(od.get("feature_bullets",[]))).lower().split())
                _comp_texts = []
                for _cd in _comps:
                    _comp_texts.append(_cd.get("title","") + " " + " ".join(_cd.get("feature_bullets",[])))
                _comp_all = " ".join(_comp_texts).lower()
                _kw_prompt = f"""Analyze keyword gaps between OUR listing and COMPETITORS.

OUR TITLE+BULLETS:
{od.get('title','')}
{chr(10).join(od.get('feature_bullets',[]))}

COMPETITORS COMBINED:
{_comp_all[:3000]}

Find TOP 15 keywords/phrases that:
1. Appear in competitor listings but NOT in ours
2. Are high-value search terms (not stop words)
3. Would improve ranking if added

Format each as:
- KEYWORD | where competitors use it | where to add in our listing (title/bullet/backend)

Respond in {('Russian' if st.session_state.get('analysis_lang','ru')=='ru' else 'English')}."""
                _kw = ai_call("Amazon SEO expert.", _kw_prompt, max_tokens=1200)
                st.session_state["_ai_kwgap"] = _kw

    # 3. Health Score Chart
    with _tool_cols[2]:
        if st.button("📈 График Health Score", use_container_width=True, key="btn_chart"):
            _hist_asin = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")
            if _hist_asin:
                _hconn = get_db()
                if _hconn:
                    try:
                        _hcur = _hconn.cursor()
                        _hcur.execute("SELECT created_at, overall_score FROM listing_analysis WHERE asin=%s AND overall_score>0 ORDER BY created_at ASC LIMIT 30", (_hist_asin,))
                        _hrows = _hcur.fetchall()
                        _hconn.close()
                        st.session_state["_health_chart"] = _hrows
                    except: pass

    # 4. Review Mining (позитив)
    with _tool_cols[3]:
        if st.button("💬 Mining отзывов", use_container_width=True, key="btn_review_mine"):
            _mine_asin = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")
            if _mine_asin:
                with st.spinner("📥 Загружаю 4-5★ отзывы..."):
                    _mine_reviews = fetch_1star_reviews(_mine_asin, domain="com", max_pages=1)
                if _mine_reviews:
                    _pos = [rv for rv in _mine_reviews if int(float(str(rv.get("rating",1) or 1).split()[0])) >= 4][:15]
                    with st.spinner("🧠 AI извлекает инсайты..."):
                        _mine_text = "\n".join([f"[{rv.get('rating')}★] {rv.get('title','')} — {rv.get('body',rv.get('text',rv.get('reviewText','')))[:200]}" for rv in _pos])
                        _mine_prompt = f"""Extract buyer insights from these 4-5★ reviews for: {od.get('title','')}

REVIEWS:
{_mine_text}

Extract:
1. TOP 5 phrases buyers use to describe what they love (exact language to use in bullets)
2. TOP 3 use cases/scenarios mentioned
3. TOP 3 objections buyers say were WRONG (e.g. "I was worried about X but...")
4. Suggested bullet rewrites using actual buyer language

Respond in {('Russian' if st.session_state.get('analysis_lang','ru')=='ru' else 'English')}."""
                        _mine_result = ai_call("Amazon VOC expert.", _mine_prompt, max_tokens=1200)
                        st.session_state["_ai_mining"] = _mine_result

    # 5. AI Chat
    with _tool_cols[4]:
        st.markdown('<div style="font-size:0.75rem;color:#94a3b8;text-align:center;margin-top:4px">💬 AI Chat</div>', unsafe_allow_html=True)
        _chat_q = st.text_input("Спроси про листинг", placeholder="Почему низкий BSR?", key="ai_chat_input", label_visibility="collapsed")
        if _chat_q and st.session_state.get("_chat_last") != _chat_q:
            st.session_state["_chat_last"] = _chat_q
            with st.spinner("🧠"):
                _chat_ctx = f"Listing: {od.get('title','')} | Overall: {pct(r.get('overall_score',0))}% | BSR: {od.get('product_information',{}).get('Best Sellers Rank','')} | Gaps: {r.get('title_gaps',[])} {r.get('bullets_gaps',[])}"
                _chat_ans = ai_call("Amazon expert. Answer concisely about this listing.", f"Context: {_chat_ctx}\n\nQuestion: {_chat_q}", max_tokens=600)
                st.session_state["_ai_chat_ans"] = _chat_ans

    # Show results
    if st.session_state.get("_ai_rewrite"):
        with st.expander("✍️ Переписанный листинг", expanded=True):
            st.markdown(st.session_state["_ai_rewrite"])
            if st.button("📋 Скопировать", key="btn_copy_rw"):
                st.code(st.session_state["_ai_rewrite"])

    if st.session_state.get("_ai_kwgap"):
        with st.expander("🔑 Keyword Gap — что добавить", expanded=True):
            st.markdown(st.session_state["_ai_kwgap"])

    if st.session_state.get("_health_chart"):
        with st.expander("📈 История Health Score", expanded=True):
            _rows = st.session_state["_health_chart"]
            if len(_rows) >= 2:
                import pandas as pd
                _df = pd.DataFrame(_rows, columns=["Дата","Score"])
                st.line_chart(_df.set_index("Дата"))
            else:
                st.info(f"Данных пока мало ({len(_rows)} запись) — нужно минимум 2 анализа")

    if st.session_state.get("_ai_mining"):
        with st.expander("💬 Voice of Customer — язык покупателей", expanded=True):
            st.markdown(st.session_state["_ai_mining"])

    if st.session_state.get("_ai_chat_ans"):
        with st.expander("💬 AI ответ", expanded=True):
            st.markdown(st.session_state["_ai_chat_ans"])

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
            conv = re.search(r"(?:[Кк]онверсия|Conversion)\s*[:\-]?\s*(.{3,})", text)
            emot = re.search(r"(?:[Ээ]моция|Emotion)\s*[:\-]?\s*(.{3,})", text)
            _strip = lambda s: s.strip().strip("*").strip()
            ptype = _strip(typ.group(1)) if typ else ""
            stxt  = _strip(strg.group(1)) if strg else ""
            wtxt  = _strip(weak.group(1)) if weak else ""
            atxt  = _strip(actn.group(1)) if actn else ""
            ctxt  = _strip(conv.group(1)) if conv else ""
            etxt  = _strip(emot.group(1)) if emot else ""
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
                if ctxt:
                    with st.expander("💡 Конверсия"):
                        st.info(f"🎯 {ctxt}")
                if etxt:
                    _ec = {"доверие":"#22c55e","trust":"#22c55e","desire":"#22c55e",
                           "желание":"#f59e0b","сомнение":"#ef4444","doubt":"#ef4444",
                           "любопытство":"#3b82f6","curiosity":"#3b82f6",
                           "безразличие":"#94a3b8","indifference":"#94a3b8"}.get(
                           etxt.split()[0].lower().rstrip("/:"), "#8b5cf6")
                    st.markdown(
                        f'<div style="background:{_ec}22;border-left:3px solid {_ec};border-radius:6px;padding:8px 12px;margin-top:4px">'
                        f'<span style="font-size:0.8rem;font-weight:700;color:{_ec}">😶 ЭМОЦИЯ: </span>'
                        f'<span style="font-size:0.82rem;color:#1e293b">{etxt}</span></div>',
                        unsafe_allow_html=True)
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
        conv = re.search(r"(?:[Кк]онверсия|Conversion)\s*[:\-]?\s*(.{3,})", text)
        emot = re.search(r"(?:[Ээ]моция|Emotion)\s*[:\-]?\s*(.{3,})", text)
        _strip = lambda s: s.strip().strip("*").strip()
        ptype = _strip(typ.group(1)) if typ else ""
        stxt  = _strip(strg.group(1)) if strg else ""
        wtxt  = _strip(weak.group(1)) if weak else ""
        atxt  = _strip(actn.group(1)) if actn else ""
        ctxt  = _strip(conv.group(1)) if conv else ""
        etxt  = _strip(emot.group(1)) if emot else ""
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
                if ctxt:
                    with st.expander("💡 Конверсия"):
                        st.info(f"🎯 {ctxt}")
                if etxt:
                    _ec = {"доверие":"#22c55e","trust":"#22c55e","desire":"#22c55e",
                           "желание":"#f59e0b","сомнение":"#ef4444","doubt":"#ef4444",
                           "любопытство":"#3b82f6","curiosity":"#3b82f6",
                           "безразличие":"#94a3b8","indifference":"#94a3b8"}.get(
                           etxt.split()[0].lower().rstrip("/:"), "#8b5cf6")
                    st.markdown(
                        f'<div style="background:{_ec}22;border-left:3px solid {_ec};border-radius:6px;padding:8px 12px;margin-top:4px">'
                        f'<span style="font-size:0.8rem;font-weight:700;color:{_ec}">😶 ЭМОЦИЯ: </span>'
                        f'<span style="font-size:0.82rem;color:#1e293b">{etxt}</span></div>',
                        unsafe_allow_html=True)
                if not stxt and text:
                    with st.expander("🔧 Raw (Strength не распознан)"):
                        st.code(text[:400])

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: A+ Контент
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🎨 A+ Контент":
    st.title("🎨 A+ Контент")
    _av = st.session_state.get("aplus_vision","")
    _av_urls = st.session_state.get("aplus_img_urls", [])
    if not _av_urls:
        _av_urls = od.get("aplus_image_urls", od.get("aplus_images", []))
        # Clean URLs just in case
        _av_urls = [re.sub(r'\.__CR[^.]+_PT0_SX\d+_V\d+___', '', u) if isinstance(u,str) else u for u in _av_urls if isinstance(u,str) and u.startswith("http")]
    _aplus_text = od.get("aplus_content","") or ""
    _desc_text  = od.get("description","") or ""

    # A+ Text content (From the brand / Product description blocks)
    if _aplus_text or (od.get("aplus") and _desc_text):
        with st.expander("📄 A+ Текстовый контент (From the brand / Product description)", expanded=not _av_urls):
            if _aplus_text:
                st.markdown("**A+ Content:**")
                st.markdown(str(_aplus_text)[:3000])
            if od.get("aplus") and _desc_text:
                st.markdown("**Product Description (часть A+):**")
                st.markdown(str(_desc_text)[:2000])
        if not _av_urls:
            st.info("ℹ️ ScrapingDog вернул A+ как текст — баннеры недоступны через API. Это нормально для 'From the brand' модулей.")

    if not _av and not _av_urls and not _aplus_text:
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
            _av_conv_m = re.search(r"(?:Конверсия|Conversion)\s*[:\-]?\s*(.{3,})", _block)
            _av_mod  = _av_mod_m.group(1).strip() if _av_mod_m else ""
            _av_sum  = _av_sum_m.group(1).strip() if _av_sum_m else ""
            _av_str  = _av_str_m.group(1).strip() if _av_str_m else ""
            _av_weak = _av_weak_m.group(1).strip() if _av_weak_m else ""
            _av_act  = _av_act_m.group(1).strip() if _av_act_m else ""
            _av_conv = _av_conv_m.group(1).strip() if _av_conv_m else ""
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
                        f'<div style="display:flex;align-items:center;gap:16px;margin:12px 0">' +
                        f'<div style="font-size:3.5rem;font-weight:800;color:{_av_bc};line-height:1">{_av_score}/10</div>' +
                        f'<div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:12px">' +
                        f'<div style="background:{_av_bc};width:{_av_score*10}%;height:12px;border-radius:6px"></div></div>' +
                        f'<div style="color:{_av_bc};font-size:0.9rem;margin-top:4px;font-weight:700">{_av_sl}</div></div></div>',
                        unsafe_allow_html=True)
                if _av_str:  st.success(f"✅ {_av_str}")
                if _av_weak: st.warning(f"⚠️ {_av_weak}")
                if _av_act:
                    with st.expander("🛠 Что делать"):
                        st.markdown(f"→ {_av_act}")
                if _av_conv:
                    with st.expander("💡 Конверсия"):
                        st.info(f"🎯 {_av_conv}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Контент
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📝 Контент":
    st.title("📝 Анализ контента")
    our_title   = od.get("title","")
    our_bullets = od.get("feature_bullets",[])
    our_desc    = od.get("description","")

    # ── Stop Words Check ──────────────────────────────────────────────────────
    _sw_results = check_listing_stop_words(od)
    if _sw_results:
        _total_banned  = sum(len(v.get("do_not_use",[])) for v in _sw_results.values())
        _total_warn    = sum(len(v.get("try_to_avoid",[])) for v in _sw_results.values())
        _total_aplus   = sum(len(v.get("a_plus_restricted",[])) for v in _sw_results.values())
        _sw_color = "#ef4444" if _total_banned > 0 else ("#f59e0b" if _total_warn > 0 else "#22c55e")
        _sw_label = f"🚨 {_total_banned} запрещённых слов!" if _total_banned else (
            f"⚠️ {_total_warn} нежелательных слов" if _total_warn else "✅ Стоп-слов нет")
        with st.expander(f"🔴 Amazon Stop Words — {_sw_label}", expanded=_total_banned > 0):
            for field, found in _sw_results.items():
                st.markdown(f"**{field}:**")
                if found.get("do_not_use"):
                    for w in found["do_not_use"]:
                        st.markdown(f'<span style="background:#ef444433;border:1px solid #ef4444;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.85rem;color:#ef4444">🚫 {w}</span>', unsafe_allow_html=True)
                if found.get("try_to_avoid"):
                    for w in found["try_to_avoid"]:
                        st.markdown(f'<span style="background:#f59e0b33;border:1px solid #f59e0b;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.85rem;color:#f59e0b">⚠️ {w}</span>', unsafe_allow_html=True)
                if found.get("a_plus_restricted"):
                    for w in found["a_plus_restricted"]:
                        st.markdown(f'<span style="background:#3b82f633;border:1px solid #3b82f6;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.85rem;color:#3b82f6">📋 A+ {w}</span>', unsafe_allow_html=True)
            st.caption("🚫 Запрещено Amazon | ⚠️ Нежелательно | 📋 Запрещено в A+")
    else:
        st.success("✅ Стоп-слова Amazon не найдены")
    st.divider()

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
    _desc_score = pct(r.get("description_score", 0))
    _has_aplus  = bool(od.get("aplus") or od.get("aplus_content"))
    if _desc_score == 0 and _has_aplus:
        # Description hidden by A+ — this is normal
        st.markdown("**Description**")
        st.markdown(
            '<div style="background:#1e3a1e;border-left:4px solid #22c55e;border-radius:8px;'
            'padding:10px 14px;margin:4px 0">'
            '<span style="color:#22c55e;font-weight:700">✅ Скрыто A+ контентом — это нормально</span><br>'
            '<span style="color:#94a3b8;font-size:0.82rem">Amazon показывает A+ вместо описания. '
            'Описание не видит покупатель, но индексируется поиском — заполни для SEO.</span>'
            '</div>', unsafe_allow_html=True)
    else:
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
        rev_cnt = int(str(pi2.get("Customer Reviews",{}).get("ratings_count","0") or 0).replace(",","").replace(".","").strip() or 0)
        has_vid = int(d.get("number_of_videos",0) or 0) > 0
        has_ap  = bool(d.get("aplus")); is_prime = bool(d.get("is_prime_exclusive"))
        bsr_num = 99999
        bsr_m = re.search(r"#([\d,]+)", str(pi2.get("Best Sellers Rank","")))
        if bsr_m:
            try: bsr_num = int(bsr_m.group(1).replace(",",""))
            except: pass
        colors2 = len([c for c in d.get("customization_options",{}).get("color",[]) if c.get("asin") and c.get("asin") != "undefined"])
        sizes2  = len(d.get("customization_options",{}).get("size",[]))
        # One Size products score full on size dimension
        _pi2 = d.get("product_information", {})
        _is_one_size = "one size" in str(_pi2.get("Size","")).lower() or "one size" in str(_pi2.get("size","")).lower()
        if _is_one_size: sizes2 = 3  # treat One Size as having size variants

        ts = min(10, max(0, (1.5 if len(title2)<=125 else 0) + (3.5 if any(k in title2.lower() for k in ["merino","wool","shirt","base layer","tank"]) else 1.5) + 3.0 + (1.0 if not re.search(r"[!$?{}]",title2) else 0) + 1))
        bs = min(10, max(0, (1.5 if len(bul2)<=5 else 0) + (2.5 if any(":" in b for b in bul2) else 1.0) + min(4.0,len(bul2)) + 1.0 + 1))
        ds = 0 if not desc2 else min(10, 4+(3 if len(desc2)>200 else 1))
        ps = min(10, max(0, (4.0 if len(imgs2)>=6 else len(imgs2)*0.6)+(2.0 if has_vid else 0)+(4.0 if len(imgs2)>=6 else 0)))
        as_ = 0 if not has_ap else 7
        rs = 10 if (rating2>=4.4 and rev_cnt>=50) else (7 if rating2>=4.0 else 4)
        bsrs = 10 if bsr_num<=1000 else (8 if bsr_num<=5000 else 5)
        has_vid = int(d.get("number_of_videos",0) or 0) > 0
        has_ap  = bool(d.get("aplus")); is_prime = bool(d.get("is_prime_exclusive") or d.get("is_prime") or "amazon" in str(d.get("ships_from","")).lower())
        prs = 10 if is_prime else 5
        vs = 10 if (colors2>=5 and sizes2>=3) else (8 if colors2>=5 else (7 if sizes2>=3 else 4))
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

    # ── JTBD ─────────────────────────────────────────────────────────────────
    _jtbd = r.get("jtbd_analysis", {})
    if _jtbd:
        st.divider()
        st.subheader("🎯 JTBD — Jobs To Be Done")
        st.caption("Покупатель не покупает продукт — он нанимает его для работы")

        _jtbd_score = pct(_jtbd.get("alignment_score", 0))
        _jc = "#22c55e" if _jtbd_score>=75 else ("#f59e0b" if _jtbd_score>=50 else "#ef4444")
        _jlbl = "Листинг говорит на языке покупателя" if _jtbd_score>=75 else ("Работа частично видна" if _jtbd_score>=50 else "Листинг говорит о фичах, не о работе")

        st.markdown(
            f'<div style="background:#1e293b;border-radius:12px;padding:16px;margin-bottom:12px">'
            f'<div style="font-size:2rem;font-weight:800;color:{_jc}">{_jtbd_score}%</div>'
            f'<div style="color:{_jc};font-size:0.85rem;font-weight:600">{_jlbl}</div>'
            f'</div>', unsafe_allow_html=True)

        _js = _jtbd.get("job_story","")
        if _js:
            st.info(f"**📖 Job Story:**\n\n_{_js}_")

        _j1, _j2, _j3 = st.columns(3)
        if _jtbd.get("functional_job"):
            _j1.markdown(f"**⚙️ Функциональная работа**\n\n{_jtbd['functional_job']}")
        if _jtbd.get("emotional_job"):
            _j2.markdown(f"**❤️ Эмоциональная работа**\n\n{_jtbd['emotional_job']}")
        if _jtbd.get("social_job"):
            _j3.markdown(f"**👥 Социальная работа**\n\n{_jtbd['social_job']}")

        if _jtbd.get("jtbd_gaps"):
            st.subheader("❌ Что листинг не коммуницирует")
            for g in _jtbd["jtbd_gaps"]:
                st.error(f"✗ {g}")

        if _jtbd.get("jtbd_recs"):
            st.subheader("✅ Как переписать под JTBD")
            for rec in _jtbd["jtbd_recs"]:
                st.success(f"→ {rec}")

    with st.expander("🔧 Raw JSON"):
        st.json(r)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: VPC / JTBD
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🎯 VPC / JTBD":
    st.title("🎯 Value Proposition Canvas + JTBD")

    _vpc  = r.get("vpc_analysis", {})
    _jtbd = r.get("jtbd_analysis", {})
    _asin_vpc = od.get("parent_asin","") or od.get("product_information",{}).get("ASIN","")

    if not _vpc and not _jtbd:
        st.info("Данные VPC/JTBD появятся после следующего анализа — перезапусти с новым ключом API")
        st.stop()

    # ── Fit Score header ──────────────────────────────────────────────────────
    _fit  = pct(_vpc.get("fit_score", _jtbd.get("alignment_score", 0)))
    _jfit = pct(_jtbd.get("alignment_score", 0))
    _fc   = "#22c55e" if _fit>=75 else ("#f59e0b" if _fit>=50 else "#ef4444")
    _jfc  = "#22c55e" if _jfit>=75 else ("#f59e0b" if _jfit>=50 else "#ef4444")

    _hc1, _hc2, _hc3 = st.columns(3)
    _hc1.markdown(f"""<div style="background:#1e293b;border-radius:12px;padding:16px;text-align:center">
<div style="font-size:2.5rem;font-weight:800;color:{_fc}">{_fit}%</div>
<div style="color:{_fc};font-size:0.85rem;font-weight:600">VPC Fit Score</div>
<div style="color:#64748b;font-size:0.75rem;margin-top:2px">Product–Market fit</div>
</div>""", unsafe_allow_html=True)
    _hc2.markdown(f"""<div style="background:#1e293b;border-radius:12px;padding:16px;text-align:center">
<div style="font-size:2.5rem;font-weight:800;color:{_jfc}">{_jfit}%</div>
<div style="color:{_jfc};font-size:0.85rem;font-weight:600">JTBD Alignment</div>
<div style="color:#64748b;font-size:0.75rem;margin-top:2px">Листинг говорит о работе</div>
</div>""", unsafe_allow_html=True)
    _gap = 100 - max(_fit, _jfit)
    _gc3 = "#ef4444" if _gap>50 else ("#f59e0b" if _gap>25 else "#22c55e")
    _hc3.markdown(f"""<div style="background:#1e293b;border-radius:12px;padding:16px;text-align:center">
<div style="font-size:2.5rem;font-weight:800;color:{_gc3}">{_gap}%</div>
<div style="color:{_gc3};font-size:0.85rem;font-weight:600">Value Gap</div>
<div style="color:#64748b;font-size:0.75rem;margin-top:2px">Ценность не коммуницирована</div>
</div>""", unsafe_allow_html=True)

    # ── VPC Verdict ────────────────────────────────────────────────────────────
    if _vpc.get("vpc_verdict"):
        st.markdown(f"""<div style="background:#0f172a;border-left:4px solid {_fc};border-radius:8px;
padding:14px 18px;margin:16px 0;font-size:0.95rem;color:#e2e8f0;line-height:1.6">
<b style="color:{_fc}">🤖 AI CRO Консультант:</b> {_vpc['vpc_verdict']}</div>""", unsafe_allow_html=True)

    # ── JTBD Job Story ─────────────────────────────────────────────────────────
    if _jtbd.get("job_story"):
        st.markdown(f"""<div style="background:#1e293b;border-radius:10px;padding:14px 18px;margin-bottom:16px">
<div style="font-size:0.75rem;font-weight:700;color:#64748b;letter-spacing:0.08em;margin-bottom:6px">JOB STORY</div>
<div style="font-size:0.95rem;color:#e2e8f0;font-style:italic;line-height:1.6">{_jtbd['job_story']}</div>
</div>""", unsafe_allow_html=True)

    st.divider()

    # ── VPC Canvas ─────────────────────────────────────────────────────────────
    st.subheader("📊 Value Proposition Canvas")
    _lc, _rc = st.columns(2)

    with _lc:
        st.markdown("**👤 Профиль покупателя**")

        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#3b82f6;letter-spacing:0.06em;margin:8px 0 4px">ЗАДАЧИ (JOBS)</div>', unsafe_allow_html=True)
        _jobs = _vpc.get("customer_jobs", [
            _jtbd.get("functional_job",""),
            _jtbd.get("emotional_job",""),
            _jtbd.get("social_job","")
        ])
        for j in [x for x in _jobs if x]:
            st.markdown(f'<div style="background:#1e3a5f22;border-left:3px solid #3b82f6;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem;color:var(--text)">{j}</div>', unsafe_allow_html=True)

        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#ef4444;letter-spacing:0.06em;margin:10px 0 4px">БОЛИ (PAINS)</div>', unsafe_allow_html=True)
        for p in _vpc.get("customer_pains", []):
            st.markdown(f'<div style="background:#ef444422;border-left:3px solid #ef4444;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">{p}</div>', unsafe_allow_html=True)

        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#22c55e;letter-spacing:0.06em;margin:10px 0 4px">ВЫГОДЫ (GAINS)</div>', unsafe_allow_html=True)
        for g in _vpc.get("customer_gains", []):
            st.markdown(f'<div style="background:#22c55e22;border-left:3px solid #22c55e;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">{g}</div>', unsafe_allow_html=True)

    with _rc:
        st.markdown("**📦 Карта ценности (листинг)**")

        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#94a3b8;letter-spacing:0.06em;margin:8px 0 4px">ПРОДУКТ / ФИЧИ</div>', unsafe_allow_html=True)
        for ps in _vpc.get("products_services", []):
            st.markdown(f'<div style="background:#33333322;border-left:3px solid #94a3b8;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">{ps}</div>', unsafe_allow_html=True)

        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#ef4444;letter-spacing:0.06em;margin:10px 0 4px">ОБЕЗБОЛИВАЮЩИЕ (PAIN RELIEVERS)</div>', unsafe_allow_html=True)
        for pr in _vpc.get("pain_relievers_present", []):
            st.markdown(f'<div style="background:#22c55e22;border-left:3px solid #22c55e;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">✅ {pr}</div>', unsafe_allow_html=True)
        for pr in _vpc.get("pain_relievers_missing", []):
            st.markdown(f'<div style="background:#ef444422;border-left:3px solid #ef4444;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">❌ {pr}</div>', unsafe_allow_html=True)

        st.markdown('<div style="font-size:0.75rem;font-weight:700;color:#22c55e;letter-spacing:0.06em;margin:10px 0 4px">ГЕНЕРАТОРЫ ВЫГОД (GAIN CREATORS)</div>', unsafe_allow_html=True)
        for gc in _vpc.get("gain_creators_present", []):
            st.markdown(f'<div style="background:#22c55e22;border-left:3px solid #22c55e;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">✅ {gc}</div>', unsafe_allow_html=True)
        for gc in _vpc.get("gain_creators_missing", []):
            st.markdown(f'<div style="background:#ef444422;border-left:3px solid #ef4444;border-radius:4px;padding:6px 10px;margin-bottom:4px;font-size:0.85rem">❌ {gc}</div>', unsafe_allow_html=True)

    st.divider()

    # ── JTBD 3 Jobs ────────────────────────────────────────────────────────────
    st.subheader("🎯 JTBD — 3 типа работ")
    _j1, _j2, _j3 = st.columns(3)
    with _j1:
        st.markdown("**⚙️ Функциональная**")
        if _jtbd.get("functional_job"):
            st.info(_jtbd["functional_job"])
    with _j2:
        st.markdown("**❤️ Эмоциональная**")
        if _jtbd.get("emotional_job"):
            st.info(_jtbd["emotional_job"])
    with _j3:
        st.markdown("**👥 Социальная**")
        if _jtbd.get("social_job"):
            st.info(_jtbd["social_job"])

    # ── Gaps & Recs ────────────────────────────────────────────────────────────
    st.divider()
    _gc1, _gc2 = st.columns(2)
    with _gc1:
        st.subheader("❌ Что не коммуницирует")
        for g in _jtbd.get("jtbd_gaps", []):
            st.error(f"✗ {g}")
        for g in _vpc.get("pain_relievers_missing", []):
            if g not in str(_jtbd.get("jtbd_gaps",[])):
                st.error(f"✗ {g}")
    with _gc2:
        st.subheader("✅ Как переписать")
        for rec in _jtbd.get("jtbd_recs", []):
            st.success(f"→ {rec}")

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
    _rev2 = int(str(cpi.get("Customer Reviews",{}).get("ratings_count","0") or 0).replace(",","").replace(".","").strip() or 0)
    _vid2 = int(c.get("number_of_videos",0) or 0)>0; _ap2 = bool(c.get("aplus"))
    _pr2  = bool(c.get("is_prime_exclusive") or c.get("is_prime") or
                 "amazon" in str(c.get("ships_from","")).lower())
    _bsr2 = 99999
    _bm = re.search(r"#([\d,]+)", str(cpi.get("Best Sellers Rank","")))
    if _bm:
        try: _bsr2 = int(_bm.group(1).replace(",",""))
        except: pass
    _col2 = len([_cv for _cv in c.get("customization_options",{}).get("color",[]) if isinstance(_cv, dict) and _cv.get("asin","") not in ("","undefined")])
    _sz2  = len(c.get("customization_options",{}).get("size",[]))
    _cis_one_size = "one size" in str(cpi.get("Size","")).lower()
    if _cis_one_size: _sz2 = 3
    _ts = min(10, max(0, (1.5 if len(_t2)<=125 else 0)+(3.5 if any(k in _t2.lower() for k in ["merino","wool","tank","shirt","base layer"]) else 1.5)+3+(1 if not re.search(r"[!$?{}]",_t2) else 0)+1))
    _bs = min(10, max(0, (1.5 if len(_b2)<=5 else 0)+(2.5 if any(":" in b for b in _b2) else 1)+min(4,len(_b2))+1+1))
    _ds = 0 if not _d2 else min(10, 4+(3 if len(_d2)>200 else 1))
    _ps = min(10, max(0, (4 if len(_i2)>=6 else len(_i2)*0.6)+(2 if _vid2 else 0)+(4 if len(_i2)>=6 else 0)))
    _as = 0 if not _ap2 else 7
    _rs = 10 if (_rat2>=4.4 and _rev2>=50) else (7 if _rat2>=4.0 else 4)
    _bsrs = 10 if _bsr2<=1000 else (8 if _bsr2<=5000 else 5)
    _prs = 10 if _pr2 else 5
    _vs = 10 if (_col2>=5 and _sz2>=3) else (8 if _col2>=5 else (7 if _sz2>=3 or _cis_one_size else 4))
    _h = int((_ts*0.10+_bs*0.10+_ds*0.10+_ps*0.10+_as*0.10+_rs*0.15+_bsrs*0.15+7*0.10+_vs*0.05+_prs*0.05)*10)

    ch = _h; hc = "#22c55e" if ch>=75 else ("#f59e0b" if ch>=50 else "#ef4444")
    tlen = len(_t2); cprice = c.get("price",""); cbrand = c.get("brand","")
    crating = c.get("average_rating",""); crev = cpi.get("Customer Reviews",{}).get("ratings_count","")
    cbsr_s = str(cpi.get("Best Sellers Rank",""))[:50]
    _cprev   = c.get("previous_price","") or c.get("list_price","")
    _ccoupon = c.get("coupon_text","") or ("🎟️ Купон" if c.get("is_coupon_exists") else "")
    _cpromo  = c.get("promo_text","")
    _cbought = c.get("number_of_people_bought","")

    # Competitor price line
    _cprice_parts = []
    if cprice:
        _ps = f"💰 <b>{cprice}</b>"
        if _cprev and _cprev != cprice:
            _ps += f" <span style='text-decoration:line-through;opacity:0.5'>{_cprev}</span>"
        _cprice_parts.append(_ps)
    if _ccoupon:
        _cprice_parts.append(f"<span style='background:#16a34a;color:white;border-radius:4px;padding:1px 6px;font-size:0.75rem'>🎟️ {_ccoupon}</span>")
    if _cpromo:
        _cprice_parts.append(f"<span style='background:#1d4ed8;color:white;border-radius:4px;padding:1px 6px;font-size:0.75rem'>📦 {_cpromo[:35]}</span>")
    if _pr2:
        _cprice_parts.append(f"<span style='background:#f59e0b;color:#1c1917;border-radius:4px;padding:1px 6px;font-size:0.75rem'>👑 Prime</span>")
    if _cbought:
        _cprice_parts.append(f"<span style='opacity:0.7;font-size:0.75rem'>🛒 {_cbought}</span>")
    _cprice_line = "  ".join(_cprice_parts)

    st.title(f"🔴 Конкурент {cidx+1}")

    st.markdown(f"""
<div style="background:linear-gradient(135deg,#3b1e1e,#5c2626);border-radius:14px;padding:18px;color:white;margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
    <div>
      <div style="font-size:0.78rem;opacity:0.6">{cbrand} - <a href="https://www.amazon.com/dp/{casin}" target="_blank" style="color:#93c5fd;text-decoration:none">{casin} ↗</a></div>
      <div style="font-size:0.95rem;font-weight:600;max-width:500px;margin-top:3px">{_t2[:80]}{"..." if tlen>80 else ""}</div>
      <div style="display:flex;gap:8px;margin-top:7px;font-size:0.8rem;flex-wrap:wrap;align-items:center">
        {_cprice_line}
      </div>
      <div style="display:flex;gap:12px;margin-top:5px;font-size:0.78rem;opacity:0.8;flex-wrap:wrap">
        <span style="color:{'#22c55e' if float(crating or 0)>=4.4 else ('#f59e0b' if float(crating or 0)>=4.3 else '#ef4444')};font-weight:600">⭐ {crating} ({crev} отз.)</span>
        <span style="color:{'#fca5a5' if tlen>125 else '#86efac'}">{tlen} симв.</span>
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

    # Frequently returned warning + analysis button
    if c.get("is_frequently_returned"):
        st.markdown("""
<div style="background:#7f1d1d;border:2px solid #ef4444;border-radius:8px;padding:12px 16px;margin-bottom:8px">
<div style="font-size:0.95rem;font-weight:800;color:#fca5a5">🔴 Amazon: "Часто возвращают" — конкурент имеет проблемы с возвратами!</div>
<div style="color:#fca5a5;font-size:0.8rem;margin-top:6px;line-height:1.6">
Это их слабое место — изучи причины и сделай наш листинг лучше по этим пунктам.
</div>
</div>""", unsafe_allow_html=True)

    # Return analysis button for competitor (always available)
    _comp_ret_col1, _comp_ret_col2 = st.columns([2,4])
    with _comp_ret_col1:
        if st.button("🔍 Анализ возвратов (1★+2★)", key=f"btn_comp_ret_{cidx}", use_container_width=True):
            with st.spinner("📥 Загружаю 1★ отзывы..."):
                _cret_reviews = fetch_1star_reviews(casin, domain="com", max_pages=1)
            if _cret_reviews:
                with st.spinner("🧠 AI анализирует..."):
                    _cret_analysis = analyze_return_reasons(
                        _cret_reviews, _t2, casin,
                        lang=st.session_state.get("analysis_lang","ru"))
                st.session_state[f"_comp_ret_{cidx}"] = _cret_analysis
                st.session_state[f"_comp_ret_cnt_{cidx}"] = len(_cret_reviews)
            else:
                st.warning("Отзывы не загружены — проверь APIFY_API_TOKEN")
    with _comp_ret_col2:
        st.caption("10 однозвёздочных отзывов → AI находит слабые места конкурента")

    if st.session_state.get(f"_comp_ret_{cidx}"):
        with st.expander(f"📊 Анализ возвратов конкурента ({st.session_state.get(f'_comp_ret_cnt_{cidx}',0)} отзывов)", expanded=True):
            st.markdown(st.session_state[f"_comp_ret_{cidx}"])
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
                _cai_result = analyze_text(c, [], _comp_vision, casin, lambda m: None, lang=_clang, is_competitor=True)
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
        # Special case: description 0% but A+ present → show as grey "A+"
        if _lbl3 == "Описание" and _p3 == 0 and _ap2:
            _col3.markdown(
                '<div style="border-left:3px solid #64748b;padding:5px 4px;text-align:center;'
                'background:#f8fafc;border-radius:4px">'
                '<div style="font-size:0.85rem;font-weight:700;color:#64748b">A+</div>'
                '<div style="font-size:0.62rem;color:#64748b">Описание</div></div>',
                unsafe_allow_html=True)
        else:
            _c3 = "#22c55e" if _p3>=75 else ("#f59e0b" if _p3>=50 else "#ef4444")
            _col3.markdown(f'<div style="border-left:3px solid {_c3};padding:5px 4px;text-align:center;background:#f8fafc;border-radius:4px"><div style="font-size:1.05rem;font-weight:700;color:{_c3}">{_p3}%</div><div style="font-size:0.62rem;color:#64748b">{_lbl3}</div></div>', unsafe_allow_html=True)

    st.divider()

    tab_cont, tab_photo, tab_aplus, tab_data = st.tabs(["📝 Контент", "📸 Фото", "🎨 A+", "📊 Данные"])
    with tab_cont:
        # Stop words check for competitor
        _csw = check_listing_stop_words(c)
        if _csw:
            _csw_banned = sum(len(v.get("do_not_use",[])) for v in _csw.values())
            _csw_warn   = sum(len(v.get("try_to_avoid",[])) for v in _csw.values())
            _csw_color  = "#ef4444" if _csw_banned > 0 else ("#f59e0b" if _csw_warn > 0 else "#22c55e")
            _csw_label  = f"🚨 {_csw_banned} запрещённых!" if _csw_banned else f"⚠️ {_csw_warn} нежелательных"
            with st.expander(f"🔴 Stop Words — {_csw_label}", expanded=_csw_banned > 0):
                for _cf, _cfw in _csw.items():
                    st.markdown(f"**{_cf}:**")
                    if _cfw.get("do_not_use"):
                        for _cw in _cfw["do_not_use"]:
                            st.markdown(f'<span style="background:#ef444433;border:1px solid #ef4444;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.82rem;color:#ef4444">🚫 {_cw}</span>', unsafe_allow_html=True)
                    if _cfw.get("try_to_avoid"):
                        for _cw in _cfw["try_to_avoid"]:
                            st.markdown(f'<span style="background:#f59e0b33;border:1px solid #f59e0b;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.82rem;color:#f59e0b">⚠️ {_cw}</span>', unsafe_allow_html=True)
                    if _cfw.get("a_plus_restricted"):
                        for _cw in _cfw["a_plus_restricted"]:
                            st.markdown(f'<span style="background:#3b82f633;border:1px solid #3b82f6;border-radius:4px;padding:2px 8px;margin:2px;display:inline-block;font-size:0.82rem;color:#3b82f6">📋 {_cw}</span>', unsafe_allow_html=True)
            st.divider()
        _tcc = "#ef4444" if tlen>125 else "#22c55e"
        _cai_title_score = pct(_cai_result.get("title_score",0)) if _cai_result else int(_ts*10)
        st.markdown(f"**Title** — <span style='color:{_tcc}'>{tlen} симв.</span>", unsafe_allow_html=True)
        st.progress(_cai_title_score/100)
        st.markdown(f"> {_t2}")

        # Авто-проверка title
        _ctwords = [w.lower() for w in _t2.split() if len(w)>3]
        _chas_repeat = any(_ctwords.count(w)>=3 for w in _ctwords)
        _chas_spec = any(c in _t2 for c in "!$?{}^¬¦")
        _chas_kw = any(w in _t2.lower() for w in ["merino","wool","tank","men","women","shirt","beanie","hat","layer","jacket"])
        _cauto_title = min(100, (15 if tlen<=125 else 0) + (35 if _chas_kw else 15) + 30 + (10 if not _chas_spec else 0) + 10)
        with st.expander("📐 Рубрика оценки Title"):
            _ct1, _ct2 = st.columns(2)
            _ct1.metric("🤖 AI оценка", f"{_cai_title_score}%")
            _ct2.metric("🔧 Авто-проверка", f"{_cauto_title}%",
                delta=f"{_cai_title_score - _cauto_title:+d}%" if _cai_title_score != _cauto_title else None,
                delta_color="normal")

        if _cai_result:
            _ctgaps = _cai_result.get("title_gaps", [])
            _ctrec  = _cai_result.get("title_rec", "")
            if _ctgaps:
                with st.expander(f"⚠️ Пробелы ({len(_ctgaps)})"):
                    for g in _ctgaps: st.markdown(f"- {g}")
            if _ctrec: st.info(f"💡 {_ctrec}")
        st.divider()
        st.markdown(f"**Bullets** ({len(_b2)})")
        for _bul in _b2:
            _blen = len(_bul.encode())
            st.markdown(f"{'🔴' if _blen>255 else '✅'} {_bul}")
            st.caption(f"{_blen} байт")
        if not _b2: st.caption("Нет буллетов")
        st.divider()
        st.markdown("**Описание**")
        if _d2:
            st.markdown(str(_d2)[:600])
        elif _ap2:
            st.markdown(
                '<div style="background:#1e3a1e;border-left:4px solid #22c55e;border-radius:8px;'
                'padding:10px 14px">'
                '<span style="color:#22c55e;font-weight:700">✅ Скрыто A+ контентом — это нормально</span><br>'
                '<span style="color:#94a3b8;font-size:0.82rem">Amazon показывает A+ вместо описания покупателю.</span>'
                '</div>', unsafe_allow_html=True)
        else:
            st.warning("Описание отсутствует")
        st.divider()
        st.markdown(f"**A+:** {'✅' if _ap2 else '❌'}  |  **Видео:** {'✅ '+str(int(c.get('number_of_videos',0) or 0))+' шт.' if _vid2 else '❌'}")
    with tab_photo:
        _cimgs = c.get("images",[])
        if _cimgs:
            if _vision_key in st.session_state and st.session_state[_vision_key]:
                _cv_imgs, _cv_text = st.session_state[_vision_key]
                # Use finditer for correct block mapping
                _cv_blocks = {}
                for _m in re.finditer(r"PHOTO_BLOCK_(\d+)\s*(.*?)(?=PHOTO_BLOCK_\d+|$)", _cv_text, re.DOTALL):
                    _cv_blocks[int(_m.group(1))] = _m.group(2).strip()

                _cstrip2 = lambda s: s.strip().strip("*").strip() if s else ""

                for _pi3, _pimg in enumerate(_cv_imgs):
                    _ptext = _cv_blocks.get(_pi3+1, "")
                    _psm   = re.search(r"(\d+)/10", _ptext)
                    _pscore = int(_psm.group(1)) if _psm else 0
                    _pbc   = "#22c55e" if _pscore>=8 else ("#f59e0b" if _pscore>=6 else "#ef4444")
                    _pslbl = "Отлично" if _pscore>=8 else ("Хорошо" if _pscore>=6 else "Слабо")
                    _ptyp  = re.search(r"(?:[Тт]ип|Type)\s*[:\-]\s*(.+)", _ptext)
                    _pstrg = re.search(r"(?:[Сс]ильная\s+сторона|Strength)\s*[:\-]\s*(.{3,})", _ptext)
                    _pweak = re.search(r"(?:[Сс]лабость|Weakness)\s*[:\-]\s*(.{3,})", _ptext)
                    _pact  = re.search(r"(?:[Дд]ействие|Action)\s*[:\-]\s*(.{3,})", _ptext)
                    _pconv = re.search(r"(?:[Кк]онверсия|Conversion)\s*[:\-]\s*(.{3,})", _ptext)
                    _pemot = re.search(r"(?:[Ээ]моция|Emotion)\s*[:\-]\s*(.{3,})", _ptext)

                    with st.container(border=True):
                        _pc1,_pc2 = st.columns([1,2])
                        with _pc1:
                            st.image(__import__("base64").b64decode(_pimg["b64"]), use_container_width=True)
                        with _pc2:
                            _phead = f"Фото #{_pi3+1}" + (f" — {_cstrip2(_ptyp.group(1))}" if _ptyp else "")
                            st.markdown(f"**{_phead}**")
                            if _pscore > 0:
                                st.markdown(
                                    f'<div style="display:flex;align-items:center;gap:12px;margin:8px 0">'
                                    f'<div style="font-size:2rem;font-weight:800;color:{_pbc}">{_pscore}/10</div>'
                                    f'<div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px">'
                                    f'<div style="background:{_pbc};width:{_pscore*10}%;height:10px;border-radius:6px"></div>'
                                    f'</div><div style="color:{_pbc};font-size:0.8rem;margin-top:2px">{_pslbl}</div></div></div>',
                                    unsafe_allow_html=True)
                            else:
                                st.warning("⚠️ Оценка не распознана")
                            if _pstrg: st.success(f"✅ {_cstrip2(_pstrg.group(1))}")
                            if _pweak: st.warning(f"⚠️ {_cstrip2(_pweak.group(1))}")
                            if _pact:
                                with st.expander("🛠 Что делать"):
                                    st.markdown(f"→ {_cstrip2(_pact.group(1))}")
                            if _pconv:
                                with st.expander("💡 Конверсия"):
                                    st.info(f"🎯 {_cstrip2(_pconv.group(1))}")
                            if _pemot:
                                _etxt2 = _cstrip2(_pemot.group(1))
                                _ec2 = {"доверие":"#22c55e","trust":"#22c55e","желание":"#f59e0b",
                                        "сомнение":"#ef4444","doubt":"#ef4444","любопытство":"#3b82f6",
                                        "curiosity":"#3b82f6","безразличие":"#94a3b8"}.get(
                                        _etxt2.split()[0].lower().rstrip("/:"), "#8b5cf6")
                                st.markdown(
                                    f'<div style="background:{_ec2}22;border-left:3px solid {_ec2};border-radius:6px;padding:8px 12px;margin-top:4px">'
                                    f'<span style="font-size:0.8rem;font-weight:700;color:{_ec2}">😶 ЭМОЦИЯ: </span>'
                                    f'<span style="font-size:0.82rem;color:#1e293b">{_etxt2}</span></div>',
                                    unsafe_allow_html=True)
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
    with tab_aplus:
        _cap2 = c.get("aplus_content","")
        _cap2_urls = st.session_state.get(f"comp_aplus_urls_{cidx}", c.get("aplus_image_urls", []))
        _cav_text  = st.session_state.get(f"comp_aplus_vision_{cidx}", "")
        _cvid2 = int(c.get("number_of_videos", 0) or 0)
        # Stats
        _ac1, _ac2, _ac3 = st.columns(3)
        _ac1.metric("A+ Контент", "✅ Есть" if _ap2 else "❌ Нет")
        _ac2.metric("Видео", f"✅ {_cvid2} шт." if _cvid2 > 0 else "❌ Нет")
        _ac3.metric("A+ баннеры", f"{len(_cap2_urls)} шт." if _cap2_urls else "—")
        st.divider()

        if not _cap2_urls:
            st.warning("❌ A+ баннеры не загружены")
        elif _cav_text:
            # Show Vision analysis like our A+ page
            _cav_blocks = {}
            for _m in re.finditer(r"APLUS_BLOCK_(\d+)\s*(.*?)(?=APLUS_BLOCK_\d+|$)", _cav_text, re.DOTALL):
                _cav_blocks[int(_m.group(1))] = _m.group(2).strip()

            st.markdown(f"**{len(_cap2_urls)} баннер(ов) проанализировано**")
            for _bi, _burl in enumerate(_cap2_urls[:8]):
                _bblk  = _cav_blocks.get(_bi+1, "")
                _bmod  = re.search(r"(?:Модуль|Module)\s*[:\-]\s*(.+)", _bblk)
                _bsum  = re.search(r"(?:Содержание|Summary)\s*[:\-]\s*(.+)", _bblk)
                _bsc   = re.search(r"(?:Оценка|Score)\s*[:\-]\s*(\d+)", _bblk)
                _bstr  = re.search(r"(?:Сильная сторона|Strength)\s*[:\-]\s*(.{3,})", _bblk)
                _bweak = re.search(r"(?:Слабость|Weakness)\s*[:\-]\s*(.{3,})", _bblk)
                _bact  = re.search(r"(?:Действие|Action)\s*[:\-]\s*(.{3,})", _bblk)
                _bconv = re.search(r"(?:Конверсия|Conversion)\s*[:\-]\s*(.{3,})", _bblk)
                _bscv  = int(_bsc.group(1)) if _bsc else 0
                _bbc   = "#22c55e" if _bscv>=8 else ("#f59e0b" if _bscv>=6 else "#ef4444")
                _bsl   = "Отлично" if _bscv>=8 else ("Хорошо" if _bscv>=6 else "Слабо")
                _bstrip = lambda s: s.strip().strip("*").strip() if s else ""

                with st.container(border=True):
                    _bc1, _bc2 = st.columns([3, 2])
                    with _bc1:
                        try: st.image(_burl, use_container_width=True)
                        except: st.caption(f"❌ {_burl[:50]}")
                    with _bc2:
                        _bhead = f"Баннер #{_bi+1}" + (f" — {_bstrip(_bmod.group(1))}" if _bmod else "")
                        st.markdown(f"**{_bhead}**")
                        if _bsum: st.caption(_bstrip(_bsum.group(1)))
                        if _bscv:
                            st.markdown(
                                f'<div style="display:flex;align-items:center;gap:12px;margin:10px 0">'
                                f'<div style="font-size:2.8rem;font-weight:800;color:{_bbc};line-height:1">{_bscv}/10</div>'
                                f'<div style="flex:1"><div style="background:#e5e7eb;border-radius:6px;height:10px">'
                                f'<div style="background:{_bbc};width:{_bscv*10}%;height:10px;border-radius:6px"></div></div>'
                                f'<div style="color:{_bbc};font-size:0.85rem;margin-top:3px;font-weight:600">{_bsl}</div>'
                                f'</div></div>', unsafe_allow_html=True)
                        if _bstr:  st.success(f"✅ {_bstrip(_bstr.group(1))}")
                        if _bweak: st.warning(f"⚠️ {_bstrip(_bweak.group(1))}")
                        if _bact:
                            with st.expander("🛠 Что делать"):
                                st.markdown(f"→ {_bstrip(_bact.group(1))}")
                        if _bconv:
                            with st.expander("💡 Конверсия"):
                                st.info(f"🎯 {_bstrip(_bconv.group(1))}")
        else:
            # No Vision analysis — just show images
            if not st.session_state.get("do_comp_vision", True):
                st.info("👁️ Vision конкурентов отключён — баннеры без анализа")
            st.markdown("**A+ баннеры:**")
            for _apu in _cap2_urls[:8]:
                try: st.image(_apu, use_container_width=True)
                except: st.caption(f"❌ {_apu[:60]}")

        if _cap2:
            with st.expander("📄 A+ текст"):
                st.markdown(str(_cap2)[:2000])
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
