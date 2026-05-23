"""
argon_video.py
==============
Argon Video Intelligence module for Amazon Listing Analyzer v2.

USAGE in listing_analyzer.py (one-line integration):

    from argon_video import render_video_intelligence

    # ...somewhere after ASIN is obtained and analyzed (e.g. after photos analysis):
    render_video_intelligence(asin=current_asin)

That's it. The module handles:
  - Reading credentials from st.secrets
  - Calling ScrapingDog to get video URLs
  - Downloading MP4 via ffmpeg (HLS fallback)
  - Sending to Gemini 2.5 Flash via Vertex AI
  - Rendering a beautiful card with AI analysis
  - Caching results in st.session_state (no re-analysis on rerun)

SECRETS (.streamlit/secrets.toml or Streamlit Cloud Settings → Secrets):

    SCRAPINGDOG_API_KEY = "6952393076cf50f3a0d69d81"
    VERTEX_LOCATION = "us-central1"

    [vertex_sa_json]
    type = "service_account"
    project_id = "topsale-vertex"
    private_key_id = "..."
    private_key = '''-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n'''
    client_email = "vertex-ai-runner@topsale-vertex.iam.gserviceaccount.com"
    client_id = "..."
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "..."

DEPENDENCIES (add to requirements.txt):
    google-genai>=1.0.0
    google-cloud-aiplatform>=1.70.0
    requests

SYSTEM (add to packages.txt for Streamlit Cloud):
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
from pathlib import Path
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
# CONFIG (from Streamlit secrets, with safe fallbacks)
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
# CREDENTIALS HELPERS
# ============================================================

def _get_scrapingdog_key() -> str:
    return st.secrets.get("SCRAPINGDOG_API_KEY", os.getenv("SCRAPINGDOG_API_KEY", ""))


def _get_vertex_location() -> str:
    return st.secrets.get("VERTEX_LOCATION", os.getenv("VERTEX_LOCATION", "us-central1"))


@st.cache_resource(show_spinner=False)
def _get_gemini_sa_path() -> str:
    """
    Build a service account JSON file on disk from st.secrets["vertex_sa_json"].
    Cached across reruns so we only write it once per Streamlit session.
    """
    if "vertex_sa_json" not in st.secrets:
        raise RuntimeError(
            "Missing [vertex_sa_json] section in secrets.toml. "
            "Add the service account JSON content there."
        )
    sa_dict = dict(st.secrets["vertex_sa_json"])
    # Normalize keys & escaped newlines in private_key
    if "private_key" in sa_dict:
        sa_dict["private_key"] = sa_dict["private_key"].replace("\\n", "\n")
    
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(sa_dict, tmp)
    tmp.close()
    return tmp.name


@st.cache_resource(show_spinner=False)
def _get_gemini_client() -> genai.Client:
    """Init Vertex AI Gemini client, cached for the whole Streamlit session."""
    sa_path = _get_gemini_sa_path()
    with open(sa_path) as f:
        sa = json.load(f)
    project_id = sa["project_id"]
    location = _get_vertex_location()
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
    return genai.Client(vertexai=True, project=project_id, location=location)


# ============================================================
# STEP 1: SCRAPINGDOG
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
# STEP 2: HLS → MP4
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
    """Download HLS stream → MP4 using ffmpeg. Bulletproof Amazon path."""
    if not _check_ffmpeg():
        raise RuntimeError(
            "ffmpeg not found. Add 'ffmpeg' to packages.txt for Streamlit Cloud, "
            "or install with: brew install ffmpeg (local)"
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
# STEP 3: GEMINI ANALYSIS
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
# STEP 4: HIGH-LEVEL API (for use in listing_analyzer.py)
# ============================================================

@st.cache_data(ttl=60 * 60 * 24 * 7, show_spinner=False)  # cache 7 days
def analyze_asin_videos(asin: str, country: str = "us", model: str = DEFAULT_MODEL) -> dict:
    """
    Full pipeline cached for 7 days per ASIN.
    Returns: {"asin": str, "video_count": int, "videos": [...]}
    """
    videos = fetch_asin_videos(asin, country=country)
    if not videos:
        return {"asin": asin, "video_count": 0, "videos": []}
    
    results = []
    for idx, v in enumerate(videos):
        m3u8 = v.get("link", "")
        if not m3u8:
            continue
        
        mp4_url = hls_to_mp4_url(m3u8)
        video_id = extract_video_id(m3u8)
        
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tf:
            tmp_path = tf.name
        try:
            download_video(m3u8, tmp_path)
            analysis = analyze_video_with_gemini(tmp_path, model=model)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        results.append({
            "asin": asin,
            "video_id": video_id,
            "title": v.get("title", ""),
            "thumbnail_url": v.get("thumbnail", ""),
            "m3u8_url": m3u8,
            "mp4_url": mp4_url,
            "ai_analysis": analysis,
            "analyzed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    return {"asin": asin, "video_count": len(results), "videos": results}


# ============================================================
# STEP 5: STREAMLIT UI RENDERER
# ============================================================

def render_video_intelligence(asin: str, country: str = "us", expanded: bool = False) -> None:
    """
    Renders a Video Intelligence card for the given ASIN.
    
    Call this anywhere in your Streamlit app after you have the ASIN:
        from argon_video import render_video_intelligence
        render_video_intelligence(asin="B08D113DXR")
    """
    if not asin:
        return
    
    st.markdown("### 🎬 Video Intelligence")
    st.caption(f"AI-powered analysis of product videos for `{asin}` (Gemini 2.5 Flash via Vertex AI)")
    
    # Action button — analyze only on user click (saves API costs)
    cols = st.columns([1, 1, 4])
    do_analyze = cols[0].button("🔍 Analyze videos", key=f"argon_analyze_{asin}")
    force_refresh = cols[1].button("🔄 Refresh", key=f"argon_refresh_{asin}")
    
    if force_refresh:
        analyze_asin_videos.clear()
        st.rerun()
    
    if not do_analyze and f"argon_videos_{asin}" not in st.session_state:
        st.info("Click 'Analyze videos' to run AI analysis on this ASIN's videos.")
        return
    
    # Run pipeline
    try:
        with st.spinner("🐕 ScrapingDog → 🎬 ffmpeg → 🤖 Gemini..."):
            result = analyze_asin_videos(asin, country=country)
        st.session_state[f"argon_videos_{asin}"] = result
    except Exception as e:
        st.error(f"Analysis failed: {e}")
        return
    
    if result["video_count"] == 0:
        st.warning(f"No videos found for {asin} on amazon.{COUNTRY_TO_DOMAIN.get(country, 'com')}")
        return
    
    st.success(f"✅ Analyzed {result['video_count']} video(s)")
    
    for idx, v in enumerate(result["videos"]):
        ai = v["ai_analysis"]
        with st.expander(
            f"🎥 Video {idx+1}: {v['title'] or 'Untitled'}  •  Score {ai.get('listing_quality_score', '?')}/10",
            expanded=expanded or idx == 0,
        ):
            # Top row: thumbnail + summary
            col_thumb, col_summary = st.columns([1, 2])
            with col_thumb:
                if v.get("thumbnail_url"):
                    st.image(v["thumbnail_url"], use_container_width=True)
                st.caption(f"`{v.get('video_id', '')[:8]}...`")
                st.caption(f"⏱ {ai.get('duration_seconds', '?')}s • 🎬 {ai.get('production_quality', '?')}")
            with col_summary:
                st.markdown(f"**Summary:** {ai.get('summary', '—')}")
                st.markdown(f"**Setting:** {ai.get('setting', '—')}")
                st.markdown(f"**People:** {ai.get('people_count', 0)} — {ai.get('people_description', '—')}")
            
            # Quality signals row
            st.markdown("---")
            sig = st.columns(4)
            sig[0].metric("Quality", f"{ai.get('listing_quality_score', 0)}/10")
            sig[1].metric("Style", ai.get('lifestyle_vs_studio', '—').title())
            sig[2].metric("Voiceover", "✅ Yes" if ai.get('has_voiceover') else "❌ No")
            sig[3].metric("Music", "🎵 Yes" if ai.get('music_present') else "🔇 No")
            
            # Tabs for deep dive
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
                        f"🔤 {meta.get('tokens', {}).get('input', 0)}+{meta.get('tokens', {}).get('output', 0)} tokens"
                    )
            
            # Direct video link (for download)
            st.markdown(f"🔗 [Direct video file (HLS)]({v['m3u8_url']})")
