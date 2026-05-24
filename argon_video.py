"""
argon_video.py
==============
Argon Video Intelligence module for Amazon Listing Analyzer v2.

UX FLOW:
  1. On page load → ScrapingDog fetches video list (~3 sec, ~$0.0007)
  2. Show thumbnails + titles immediately
  3. User clicks "AI анализ" on a specific video → run Gemini (~17 sec, ~$0.005)
  4. Both layers cached 7 days per ASIN

USAGE in listing_analyzer.py:
    from argon_video import render_video_intelligence
    render_video_intelligence(asin=current_asin)

SECRETS (.streamlit/secrets.toml or Streamlit Cloud Settings → Secrets):

    SCRAPINGDOG_API_KEY = "..."
    VERTEX_LOCATION = "us-central1"

    [vertex_sa_json]
    type = "service_account"
    project_id = "..."
    private_key_id = "..."
    private_key = '''-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n'''
    client_email = "..."
    client_id = "..."
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "..."

DEPENDENCIES (requirements.txt):
    google-genai
    google-cloud-aiplatform
    requests

SYSTEM (packages.txt for Streamlit Cloud):
    ffmpeg
"""

import os
import re
import json
import time
import shutil
import logging
import tempfile
import subprocess
from typing import Optional

import requests
import streamlit as st

from google import genai
from google.genai import types

# ============================================================
# LOGGING
# ============================================================

log = logging.getLogger("argon.video")
if not log.handlers:
    log.setLevel(logging.INFO)

# ============================================================
# CONFIG
# ============================================================

DEFAULT_MODEL = "gemini-2.5-flash"
PREMIUM_MODEL = "gemini-2.5-pro"

INLINE_SIZE_THRESHOLD = 20 * 1024 * 1024
DOWNLOAD_TIMEOUT = 60
SCRAPINGDOG_TIMEOUT = 90
FFMPEG_TIMEOUT = 180

COUNTRY_TO_DOMAIN = {
    "us": "com", "uk": "co.uk", "gb": "co.uk", "ca": "ca", "de": "de",
    "fr": "fr", "es": "es", "it": "it", "in": "in", "jp": "co.jp",
    "mx": "com.mx", "br": "com.br", "au": "com.au",
}

# ============================================================
# CREDENTIALS
# ============================================================

def _get_scrapingdog_key() -> str:
    return st.secrets.get("SCRAPINGDOG_API_KEY", os.getenv("SCRAPINGDOG_API_KEY", ""))


def _get_vertex_location() -> str:
    return st.secrets.get("VERTEX_LOCATION", os.getenv("VERTEX_LOCATION", "us-central1"))


@st.cache_resource(show_spinner=False)
def _get_gemini_sa_path() -> str:
    """
    Build SA JSON file on disk from st.secrets, cached per session.
    
    Supports TWO secret formats (in priority order):
      1. VERTEX_SA_JSON_B64 — base64-encoded full JSON (RECOMMENDED — no TOML escaping issues)
      2. [vertex_sa_json] section — TOML structured (legacy, fragile with PEM keys)
    """
    # ─── Format 1: base64 (preferred) ───
    if "VERTEX_SA_JSON_B64" in st.secrets:
        import base64
        try:
            raw = base64.b64decode(st.secrets["VERTEX_SA_JSON_B64"])
            sa_dict = json.loads(raw.decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"Failed to decode VERTEX_SA_JSON_B64: {e}")
    # ─── Format 2: TOML section (legacy) ───
    elif "vertex_sa_json" in st.secrets:
        sa_dict = dict(st.secrets["vertex_sa_json"])
        if "private_key" in sa_dict:
            sa_dict["private_key"] = sa_dict["private_key"].replace("\\n", "\n")
    else:
        raise RuntimeError(
            "Missing Vertex AI credentials. Add to Streamlit Secrets:\n"
            "  Option A (preferred): VERTEX_SA_JSON_B64 = \"<base64 of full SA JSON>\"\n"
            "  Option B: [vertex_sa_json] section with full service account fields"
        )
    
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(sa_dict, tmp)
    tmp.close()
    return tmp.name


@st.cache_resource(show_spinner=False)
def _get_gemini_client() -> genai.Client:
    """Init Vertex AI client, cached per session."""
    sa_path = _get_gemini_sa_path()
    with open(sa_path) as f:
        sa = json.load(f)
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    return genai.Client(
        vertexai=True,
        project=sa["project_id"],
        location=_get_vertex_location(),
    )


# ============================================================
# SCRAPINGDOG
# ============================================================

def fetch_asin_videos(asin: str, country: str = "us") -> list[dict]:
    """Return list of {link, thumbnail, title} from ScrapingDog."""
    api_key = _get_scrapingdog_key()
    if not api_key:
        raise RuntimeError("SCRAPINGDOG_API_KEY not set in st.secrets")
    
    domain = COUNTRY_TO_DOMAIN.get(country.lower(), "com")
    r = requests.get(
        "https://api.scrapingdog.com/amazon/product",
        params={
            "api_key": api_key,
            "domain": domain,
            "asin": asin,
            "country": country,
            "postal_code": "85001",
        },
        timeout=SCRAPINGDOG_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return data.get("videos", []) or []


# ============================================================
# HLS → MP4
# ============================================================

def hls_to_mp4_url(m3u8_url: str) -> str:
    if ".hls.m3u8" in m3u8_url:
        return m3u8_url.replace(".hls.m3u8", ".mp4")
    if m3u8_url.endswith(".m3u8"):
        return m3u8_url[:-5] + ".mp4"
    return m3u8_url


def extract_video_id(url: str) -> Optional[str]:
    m = re.search(r"/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})/", url)
    return m.group(1) if m else None


def _check_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


def download_via_ffmpeg(m3u8_url: str, dest_path: str) -> int:
    """Download HLS → MP4 via ffmpeg. Bulletproof for Amazon."""
    if not _check_ffmpeg():
        raise RuntimeError(
            "ffmpeg not found. Add 'ffmpeg' to packages.txt (Streamlit Cloud) "
            "or run: brew install ffmpeg (local)"
        )
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-user_agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "-i", m3u8_url,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        dest_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=FFMPEG_TIMEOUT)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr[:300]}")
    return os.path.getsize(dest_path)


def download_video(m3u8_url: str, dest_path: str) -> int:
    """Try direct MP4 first, fall back to ffmpeg HLS conversion."""
    mp4_url = hls_to_mp4_url(m3u8_url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "video/mp4,video/*;q=0.9,*/*;q=0.8",
    }
    try:
        r = requests.get(mp4_url, headers=headers, stream=True,
                         timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
        if r.status_code == 200:
            size = 0
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    f.write(chunk)
                    size += len(chunk)
            return size
    except Exception:
        pass
    return download_via_ffmpeg(m3u8_url, dest_path)


# ============================================================
# GEMINI VIDEO ANALYSIS
# ============================================================

VIDEO_ANALYSIS_PROMPT = """
You are an Amazon listing intelligence analyst. Analyze this product video carefully.

Return ONLY valid JSON (no markdown, no preamble) matching this schema:

{
  "duration_seconds": <int>,
  "production_quality": "professional" | "semi_pro" | "amateur",
  "has_voiceover": <bool>,
  "voiceover_language": <string or null>,
  "voiceover_transcript": <string or null>,
  "on_screen_text": [<all text shown on screen>],
  "key_claims": [<benefits/features stated or shown>],
  "use_cases_shown": [<e.g. "skiing", "hiking">],
  "lifestyle_vs_studio": "lifestyle" | "studio" | "mixed",
  "people_count": <int>,
  "people_description": <string>,
  "setting": <string>,
  "branded_intro": <bool>,
  "branded_outro": <bool>,
  "shows_product_demo": <bool>,
  "shows_product_closeup": <bool>,
  "music_present": <bool>,
  "competitive_signals": {
    "shows_comparison": <bool>,
    "mentions_competitors_by_name": [<strings>]
  },
  "target_audience_inferred": <string>,
  "summary": <2-3 sentences>,
  "listing_quality_score": <int 1-10>,
  "improvement_suggestions": [<strings>]
}

Be precise. Use null/false/empty array if absent. Do NOT hallucinate.
""".strip()


def analyze_video_with_gemini(mp4_path: str, model: str = DEFAULT_MODEL) -> dict:
    """Send local MP4 to Gemini Vision, return structured JSON."""
    client = _get_gemini_client()
    file_size = os.path.getsize(mp4_path)
    started = time.time()
    
    if file_size < INLINE_SIZE_THRESHOLD:
        with open(mp4_path, "rb") as f:
            video_bytes = f.read()
        response = client.models.generate_content(
            model=model,
            contents=[
                types.Part(inline_data=types.Blob(mime_type="video/mp4", data=video_bytes)),
                VIDEO_ANALYSIS_PROMPT,
            ],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.1,
            ),
        )
    else:
        video_file = client.files.upload(file=mp4_path)
        while video_file.state.name == "PROCESSING":
            time.sleep(2)
            video_file = client.files.get(name=video_file.name)
        if video_file.state.name == "FAILED":
            raise RuntimeError("Gemini File API processing failed")
        try:
            response = client.models.generate_content(
                model=model,
                contents=[video_file, VIDEO_ANALYSIS_PROMPT],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.1,
                ),
            )
        finally:
            client.files.delete(name=video_file.name)
    
    elapsed = time.time() - started
    result = json.loads(response.text)
    
    usage = getattr(response, "usage_metadata", None)
    result["_meta"] = {
        "model": model,
        "elapsed_seconds": round(elapsed, 2),
        "file_size_mb": round(file_size / 1024 / 1024, 2),
    }
    if usage:
        result["_meta"]["tokens"] = {
            "input": usage.prompt_token_count,
            "output": usage.candidates_token_count,
        }
        if model == "gemini-2.5-flash":
            cost = (usage.prompt_token_count * 0.30 + usage.candidates_token_count * 2.50) / 1_000_000
            result["_meta"]["cost_usd"] = round(cost, 6)
    
    return result


# ============================================================
# CACHED LAYERS (Streamlit-level)
# ============================================================

@st.cache_data(ttl=60 * 60 * 24 * 7, show_spinner=False)  # 7 days
def _cached_fetch_videos(asin: str, country: str = "us") -> list[dict]:
    """
    Layer 1: cheap & fast — get video list metadata.
    Cost: ~$0.0007 per call (1 ScrapingDog credit).
    """
    return fetch_asin_videos(asin, country=country)


@st.cache_data(ttl=60 * 60 * 24 * 7, show_spinner=False)  # 7 days
def _cached_analyze_video(asin: str, video_url: str, model: str = DEFAULT_MODEL) -> dict:
    """
    Layer 2: expensive — full AI analysis.
    Cost: ~$0.005 per call (download + Gemini).
    Cached by (asin, video_url) tuple — won't re-run for same input.
    """
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tf:
        tmp_path = tf.name
    try:
        download_video(video_url, tmp_path)
        return analyze_video_with_gemini(tmp_path, model=model)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ============================================================
# UI HELPERS
# ============================================================

def _render_ai_analysis(ai: dict, video_idx: int) -> None:
    """Render the rich AI analysis card (metrics + tabs)."""
    # Quality signals row
    sig = st.columns(4)
    sig[0].metric("Quality", f"{ai.get('listing_quality_score', 0)}/10")
    sig[1].metric("Style", ai.get('lifestyle_vs_studio', '—').title())
    sig[2].metric("Voiceover", "✅ Yes" if ai.get('has_voiceover') else "❌ No")
    sig[3].metric("Music", "🎵 Yes" if ai.get('music_present') else "🔇 No")
    
    # Summary
    st.markdown(f"**Summary:** {ai.get('summary', '—')}")
    st.markdown(f"**Setting:** {ai.get('setting', '—')}")
    st.markdown(f"**People:** {ai.get('people_count', 0)} — {ai.get('people_description', '—')}")
    
    # Tabs
    tabs = st.tabs(["📝 Claims", "🎯 Use Cases", "📺 On-Screen Text", "💡 Improvements", "🔧 Raw JSON"])
    
    with tabs[0]:
        claims = ai.get("key_claims", [])
        if claims:
            for c in claims:
                st.markdown(f"• {c}")
        else:
            st.caption("No claims detected.")
        
        transcript = ai.get("voiceover_transcript")
        if transcript:
            st.markdown("**Voiceover transcript:**")
            st.info(transcript)
    
    with tabs[1]:
        uses = ai.get("use_cases_shown", [])
        if uses:
            st.markdown(" ".join(f"`{u}`" for u in uses))
        else:
            st.caption("No specific use cases shown.")
        st.markdown(f"**Target audience:** {ai.get('target_audience_inferred', '—')}")
    
    with tabs[2]:
        text = ai.get("on_screen_text", [])
        if text:
            for t in text:
                st.markdown(f"• `{t}`")
        else:
            st.caption("No on-screen text detected.")
    
    with tabs[3]:
        tips = ai.get("improvement_suggestions", [])
        if tips:
            for t in tips:
                st.markdown(f"💡 {t}")
        else:
            st.caption("No improvement suggestions.")
    
    with tabs[4]:
        st.json(ai)
        meta = ai.get("_meta", {})
        if "cost_usd" in meta:
            st.caption(
                f"⚡ {meta.get('elapsed_seconds')}s • "
                f"💰 ${meta.get('cost_usd')} • "
                f"📦 {meta.get('file_size_mb')}MB • "
                f"🔤 {meta.get('tokens', {}).get('input', 0)}+"
                f"{meta.get('tokens', {}).get('output', 0)} tokens"
            )


# ============================================================
# MAIN ENTRY POINT
# ============================================================

def render_video_intelligence(asin: str, country: str = "us", **kwargs) -> None:
    """
    Render Video Intelligence section for the given ASIN.
    
    Flow:
      1. Auto-fetch video list (fast, cached 7d)
      2. Show thumbnails + metadata immediately
      3. Per-video "AI анализ" button → run Gemini on click
    
    Cost: ~$0.0007 per page load (list), +$0.005 per AI analysis click.
    """
    if not asin:
        return
    
    st.markdown("### 🎬 Video Intelligence")
    st.caption(f"AI-powered analysis of product videos for `{asin}` (Gemini 2.5 Flash via Vertex AI)")
    
    # Refresh button (top right)
    col_title, col_refresh = st.columns([5, 1])
    with col_refresh:
        if st.button("🔄 Refresh", key=f"argon_refresh_{asin}", help="Clear cache and refetch"):
            _cached_fetch_videos.clear()
            _cached_analyze_video.clear()
            # Clear all "AI started" flags for this ASIN
            for k in list(st.session_state.keys()):
                if k.startswith(f"argon_ai_started_{asin}_"):
                    del st.session_state[k]
            st.rerun()
    
    # ─── Step 1: Fetch list (always, cached) ─────────────────────
    try:
        with st.spinner("🐕 Loading video list..."):
            videos = _cached_fetch_videos(asin, country)
    except Exception as e:
        st.error(f"Failed to fetch videos: {e}")
        return
    
    if not videos:
        st.info(f"🎬 У этого ASIN ({asin}) нет видео на Amazon.")
        return
    
    st.success(f"✅ Найдено {len(videos)} видео")
    
    # ─── Step 2: Render each video card ──────────────────────────
    for idx, v in enumerate(videos):
        m3u8 = v.get("link", "")
        if not m3u8:
            continue
        
        video_id = extract_video_id(m3u8) or "unknown"
        title = v.get("title", "Untitled")
        thumbnail = v.get("thumbnail", "")
        mp4_url = hls_to_mp4_url(m3u8)
        
        ai_started_key = f"argon_ai_started_{asin}_{idx}"
        
        with st.container(border=True):
            col_thumb, col_info, col_btn = st.columns([1, 3, 1.2])
            
            with col_thumb:
                if thumbnail:
                    st.image(thumbnail, use_container_width=True)
                else:
                    st.caption("(no thumb)")
            
            with col_info:
                st.markdown(f"**🎥 Видео {idx+1}:** {title}")
                st.caption(f"🆔 `{video_id[:13]}...`")
                st.markdown(
                    f"🔗 [HLS stream]({m3u8}) • "
                    f"📥 [Direct MP4]({mp4_url})"
                )
            
            with col_btn:
                if st.button(
                    "🤖 AI анализ",
                    key=f"btn_{ai_started_key}",
                    help="Запустить Gemini Vision (~17 сек, ~$0.005)",
                    use_container_width=True,
                ):
                    st.session_state[ai_started_key] = True
            
            # ─── Step 3: Run AI analysis if button clicked ───────
            if st.session_state.get(ai_started_key):
                with st.spinner(f"🎬 ffmpeg → 🤖 Gemini analyzing video {idx+1}..."):
                    try:
                        ai = _cached_analyze_video(asin, m3u8)
                    except Exception as e:
                        st.error(f"AI analysis failed: {e}")
                        st.session_state[ai_started_key] = False
                        continue
                
                st.markdown("---")
                _render_ai_analysis(ai, idx)
