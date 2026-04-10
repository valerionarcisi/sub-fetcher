#!/usr/bin/env python3
"""
sub_fetcher.py — Telegram-interactive Italian subtitle downloader
Scans media folders, finds videos missing Italian subs, asks via Telegram
whether to download them, and saves them with the correct filename.

Features:
  - OpenSubtitles hash + name search
  - Placeholder/VIP-bait detection (rejects fake subs)
  - Fallback: download English subs and translate to Italian via Claude API
  - Telegram bot interaction

Runs as a long-running service (in Docker).
"""

import os
import sys
import struct
import json
import gzip
import base64
import logging
import time
import re
import urllib.request
import urllib.error
import urllib.parse
import zipfile
import io
from xmlrpc.client import ServerProxy
from pathlib import Path
from datetime import datetime, timedelta
from threading import Thread
from queue import Queue, Empty

# =============================================================================
# CONFIGURATION
# =============================================================================

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# OpenSubtitles.com REST API v1 (new, replaces legacy XML-RPC)
# Get an API key at https://www.opensubtitles.com/en/consumers
OPENSUBTITLES_API_KEY = os.environ.get("OPENSUBTITLES_API_KEY", "")
OPENSUBTITLES_API_URL = "https://api.opensubtitles.com/api/v1"
OPENSUBTITLES_USER_AGENT = "NarcisiSubs v1.0.0"
# Legacy XML-RPC credentials kept for backwards compatibility only — unused now.
OS_USERNAME = os.environ.get("OS_USERNAME", "")
OS_PASSWORD = os.environ.get("OS_PASSWORD", "")
OS_LANGUAGE = "it"  # REST v1 uses ISO 639-1 (two-letter) codes

# Subdl.com API (primary subtitle provider)
SUBDL_API_KEY = os.environ.get("SUBDL_API_KEY", "")
SUBDL_API_URL = "https://api.subdl.com/api/v1/subtitles"
SUBDL_DOWNLOAD_URL = "https://dl.subdl.com/subtitle"

# TMDb (used to resolve title+year -> IMDb ID when no .nfo is present)
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_API_URL = "https://api.themoviedb.org/3"

# SubSource (third subtitle provider)
SUBSOURCE_API_URL = "https://api.subsource.net/v1"

# Claude API for translation (EN -> IT fallback)
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")
CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"

# Media paths (inside the container, mapped via volumes)
SERIES_PATH = "/media/series"
FILMS_PATH = "/media/films"

# Excluded folders (Italian audio — no need for Italian subs)
EXCLUDE_FOLDERS_FILE = "/config/exclude_folders.txt"
DEFAULT_EXCLUDES = ["Boris"]

# State
STATE_FILE = "/config/state.json"
BATCHES_FILE = "/config/batches.json"
LOG_FILE = "/config/sub_fetcher.log"

# Timing
SCAN_INTERVAL = 300  # 5 minutes
DELAY_BETWEEN_API_CALLS = 2  # seconds
RETRY_AFTER_HOURS = 72  # don't re-ask for 3 days after "no"
FAILED_RETRY_HOURS = 24  # retry failed downloads after 24h

# Subtitle sync validation
SYNC_MIN_SCORE = 800  # ffsubsync score threshold — reject subs that don't match the video

# Video extensions
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".ts"}

# =============================================================================
# LOGGING
# =============================================================================

os.makedirs("/config", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("sub_fetcher")

# =============================================================================
# STATE
# =============================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "asked": {},       # path -> {"time": iso, "status": "pending"|"yes"|"no"|"failed"}
        "downloaded": {},  # path -> {"sub": filename, "time": iso}
        "last_offset": 0,  # Telegram update offset
    }


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        log.error(f"Failed to save state: {e}")


def load_batches():
    """Load pending download/translate batches from separate file (thread-safe)."""
    if os.path.exists(BATCHES_FILE):
        try:
            with open(BATCHES_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_batches(batches):
    """Persist batches independently of main state to avoid race conditions."""
    try:
        with open(BATCHES_FILE, "w") as f:
            json.dump(batches, f, indent=2, default=str)
    except Exception as e:
        log.error(f"Failed to save batches: {e}")


# =============================================================================
# EXCLUDE LIST
# =============================================================================

def load_excludes():
    excludes = set(DEFAULT_EXCLUDES)
    if os.path.exists(EXCLUDE_FOLDERS_FILE):
        try:
            with open(EXCLUDE_FOLDERS_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        excludes.add(line)
        except Exception:
            pass
    return excludes


def save_excludes(excludes):
    try:
        with open(EXCLUDE_FOLDERS_FILE, "w") as f:
            f.write("# Folders to exclude (Italian audio, one per line)\n")
            for e in sorted(excludes):
                f.write(f"{e}\n")
    except Exception as e:
        log.error(f"Failed to save excludes: {e}")


# =============================================================================
# TELEGRAM API
# =============================================================================

def tg_request(method, data=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        if data:
            payload = json.dumps(data).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        else:
            req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.error(f"Telegram API error ({method}): {e}")
        return None


def tg_send(text, reply_markup=None):
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    if reply_markup:
        data["reply_markup"] = reply_markup
    return tg_request("sendMessage", data)


def tg_answer_callback(callback_id, text=""):
    return tg_request("answerCallbackQuery", {
        "callback_query_id": callback_id,
        "text": text,
    })


def tg_edit_message(message_id, text, reply_markup=None):
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_request("editMessageText", payload)


def tg_get_updates(offset=0):
    result = tg_request("getUpdates", {
        "offset": offset,
        "timeout": 1,
    })
    if result and result.get("ok"):
        return result.get("result", [])
    return []


# =============================================================================
# OPENSUBTITLES HASH
# =============================================================================

def compute_hash(filepath):
    try:
        longlongformat = "<q"
        bytesize = struct.calcsize(longlongformat)
        filesize = os.path.getsize(filepath)
        if filesize < 65536 * 2:
            return None, filesize
        filehash = filesize
        with open(filepath, "rb") as f:
            for _ in range(65536 // bytesize):
                buf = f.read(bytesize)
                (val,) = struct.unpack(longlongformat, buf)
                filehash += val
                filehash &= 0xFFFFFFFFFFFFFFFF
            f.seek(max(0, filesize - 65536), 0)
            for _ in range(65536 // bytesize):
                buf = f.read(bytesize)
                (val,) = struct.unpack(longlongformat, buf)
                filehash += val
                filehash &= 0xFFFFFFFFFFFFFFFF
        return "%016x" % filehash, filesize
    except Exception as e:
        log.error(f"Hash failed for {filepath}: {e}")
        return None, 0


# =============================================================================
# FILENAME PARSING
# =============================================================================

_SCRAPER_PREFIX_RE = re.compile(
    r"^\s*(?:www[.\s_-]*)?[\w-]+\.(?:com|org|net|it|to|me|info|io)\s*[-_.\s]+",
    re.IGNORECASE,
)
_BRACKET_PREFIX_RE = re.compile(r"^\s*[\[\(][^\]\)]+[\]\)]\s*[-_.\s]*")


def _strip_scraper_noise(s):
    prev = None
    while prev != s:
        prev = s
        s = _SCRAPER_PREFIX_RE.sub("", s)
        s = _BRACKET_PREFIX_RE.sub("", s)
    return s


def _clean_title(s):
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" -")


def parse_video(filepath):
    fname = os.path.basename(filepath)
    name_no_ext = os.path.splitext(fname)[0]
    name_no_ext = _strip_scraper_noise(name_no_ext)

    # Series: S01E01
    m = re.search(r"(.+?)[.\s_-]+[Ss](\d{1,2})[Ee](\d{1,2})", name_no_ext)
    if m:
        return {"type": "episode", "name": _clean_title(m.group(1)), "season": int(m.group(2)), "episode": int(m.group(3))}

    # Movie: Name Year  or  Name (Year)
    m = re.search(r"(.+?)[.\s_\-]*\(?(\d{4})\)?(?:[.\s_\-)\]]|$)", name_no_ext)
    if m:
        return {"type": "movie", "name": _clean_title(m.group(1)), "year": int(m.group(2))}

    # Fallback: use filename without extension, cleaned up
    # Avoid using media root folders like "films" or "series" as name
    clean_name = name_no_ext.replace(".", " ").replace("_", " ").replace("-", " ").strip()
    # Remove common junk tags from filename
    clean_name = re.sub(r"\b(720p|1080p|2160p|4k|bluray|webrip|web dl|brrip|hdtv|x264|x265|hevc|aac|yts|yify|mx)\b", "", clean_name, flags=re.IGNORECASE).strip()
    clean_name = re.sub(r"\s{2,}", " ", clean_name).strip(" -[]().")
    if clean_name:
        return {"type": "unknown", "name": clean_name}

    parent = os.path.basename(os.path.dirname(filepath))
    return {"type": "unknown", "name": parent}


def _find_imdb_id_from_nfo(video_path):
    """Search for IMDB ID in .nfo files (created by Sonarr/Radarr)."""
    video_dir = os.path.dirname(video_path)
    parent_dir = os.path.dirname(video_dir)
    search_dirs = [video_dir, parent_dir]

    for d in search_dirs:
        if not os.path.isdir(d):
            continue
        try:
            for fname in os.listdir(d):
                if fname.lower().endswith(".nfo"):
                    nfo_path = os.path.join(d, fname)
                    try:
                        with open(nfo_path, "r", encoding="utf-8", errors="ignore") as f:
                            content = f.read()
                        m = re.search(r"(tt\d{7,})", content)
                        if m:
                            log.info(f"  Found IMDB ID {m.group(1)} from {fname}")
                            return m.group(1)
                    except Exception:
                        continue
        except Exception:
            continue
    return None


def tmdb_find_imdb_id(title, year=None, is_tv=False):
    """Look up IMDb ID from TMDb using title (+ year). Returns 'ttXXXXXXX' or None."""
    if not TMDB_API_KEY or not title:
        return None
    endpoint = "/search/tv" if is_tv else "/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": title, "include_adult": "false"}
    if year and not is_tv:
        params["year"] = str(year)
    elif year and is_tv:
        params["first_air_date_year"] = str(year)
    url = f"{TMDB_API_URL}{endpoint}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning(f"  TMDb search error: {e}")
        return None
    results = data.get("results") or []
    if not results:
        log.info(f"  TMDb: no match for '{title}' ({year})")
        return None
    tmdb_id = results[0].get("id")
    if not tmdb_id:
        return None
    # Fetch external_ids to get the IMDb ID
    ext_endpoint = f"/tv/{tmdb_id}/external_ids" if is_tv else f"/movie/{tmdb_id}/external_ids"
    ext_url = f"{TMDB_API_URL}{ext_endpoint}?api_key={TMDB_API_KEY}"
    try:
        with urllib.request.urlopen(ext_url, timeout=10) as resp:
            ext = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        log.warning(f"  TMDb external_ids error: {e}")
        return None
    imdb_id = ext.get("imdb_id")
    if imdb_id:
        log.info(f"  TMDb: resolved '{title}' ({year}) -> {imdb_id}")
        return imdb_id
    return None


def find_imdb_id(video_path):
    """Find IMDb ID from .nfo files first, fall back to TMDb lookup."""
    imdb = _find_imdb_id_from_nfo(video_path)
    if imdb:
        return imdb
    parsed = parse_video(video_path)
    is_tv = parsed.get("type") == "episode"
    return tmdb_find_imdb_id(parsed.get("name"), parsed.get("year"), is_tv=is_tv)


def detect_language_from_srt(srt_path, sample_size=2000):
    """Detect if an SRT file is Italian or English by inspecting common words."""
    try:
        with open(srt_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read(sample_size).lower()
    except Exception:
        return "unknown"

    # Remove timecodes and numbers
    text = re.sub(r"\d{2}:\d{2}:\d{2},\d+\s*-->.*", "", text)
    text = re.sub(r"^\d+\s*$", "", text, flags=re.MULTILINE)

    it_words = ["che", "non", "sono", "una", "per", "con", "questo", "della", "anche", "cosa", "come", "perché", "quello", "ancora", "dove", "quando", "tutti", "stato", "fatto", "sempre"]
    en_words = ["the", "and", "you", "that", "was", "for", "are", "with", "his", "they", "this", "have", "from", "not", "been", "what", "would", "there", "their", "will"]

    it_count = sum(1 for w in it_words if re.search(r"\b" + w + r"\b", text))
    en_count = sum(1 for w in en_words if re.search(r"\b" + w + r"\b", text))

    if it_count > en_count and it_count >= 3:
        return "it"
    if en_count > it_count and en_count >= 3:
        return "en"
    return "unknown"


EN_SUB_SUFFIXES = [".en.srt", ".eng.srt", ".english.srt"]


def find_english_sub(video_path):
    """Return the path of an existing English subtitle for this video, or None.
    Checks common suffixes in order: .en.srt, .eng.srt, .english.srt."""
    base = os.path.splitext(video_path)[0]
    for suffix in EN_SUB_SUFFIXES:
        path = base + suffix
        if os.path.exists(path):
            return path
    return None


def find_existing_srt(video_path):
    """Check for existing subtitle files in the video's directory."""
    base = os.path.splitext(video_path)[0]
    video_dir = os.path.dirname(video_path)
    video_base_name = os.path.splitext(os.path.basename(video_path))[0].lower()

    # Check for English subtitles
    en_path = find_english_sub(video_path)
    if en_path:
        return {"lang": "en", "path": en_path}

    # Check for generic .srt matching the video name
    generic_srt = base + ".srt"
    if os.path.exists(generic_srt):
        lang = detect_language_from_srt(generic_srt)
        return {"lang": lang, "path": generic_srt}

    # Check for any .srt in the directory that roughly matches
    try:
        for fname in os.listdir(video_dir):
            if not fname.lower().endswith(".srt"):
                continue
            fbase = fname.lower()
            # Skip files already tagged as Italian
            if any(fbase.endswith(s) for s in [".it.srt", ".ita.srt", ".italian.srt"]):
                continue
            # Check if filename partially matches the video
            if video_base_name[:10] in fbase:
                srt_path = os.path.join(video_dir, fname)
                lang = detect_language_from_srt(srt_path)
                return {"lang": lang, "path": srt_path}
    except Exception:
        pass

    return None


def get_search_queries(video_path):
    """Return a list of search query strings to try, from most to least specific."""
    queries = []
    parsed = parse_video(video_path)
    filename_name = parsed.get("name", "")
    if filename_name:
        queries.append(filename_name)

    series_folder = get_series_folder(video_path)
    if series_folder:
        folder_clean = series_folder.replace(".", " ").replace("_", " ").replace("-", " ").strip()
        if folder_clean.lower() not in [q.lower() for q in queries]:
            queries.append(folder_clean)
        # Also try removing digits/symbols for names like PLUR1BUS -> PLURIBUS
        alpha_only = re.sub(r"[^a-zA-Z\s]", "", folder_clean).strip()
        if alpha_only and alpha_only.lower() not in [q.lower() for q in queries]:
            queries.append(alpha_only)

    return queries


def friendly_name(filepath):
    """Human-readable name for Telegram messages."""
    parsed = parse_video(filepath)
    if parsed["type"] == "episode":
        return f"{parsed['name']} S{parsed['season']:02d}E{parsed['episode']:02d}"
    elif parsed["type"] == "movie":
        return f"{parsed['name']} ({parsed['year']})"
    else:
        return os.path.basename(filepath)


def get_series_folder(filepath):
    """Get the immediate series/movie folder name."""
    # e.g. /media/series/The Chosen/Season 1/file.mkv -> "The Chosen"
    rel = None
    for base in [SERIES_PATH, FILMS_PATH]:
        if filepath.startswith(base):
            rel = os.path.relpath(filepath, base)
            break
    if rel:
        return rel.split(os.sep)[0]
    return os.path.basename(os.path.dirname(filepath))


# =============================================================================
# SCAN
# =============================================================================

def has_italian_sub(video_path):
    base = os.path.splitext(video_path)[0]
    for suffix in [".it.srt", ".ita.srt", ".italian.srt", ".it.hi.srt", ".it.ass", ".ita.ass"]:
        if os.path.exists(base + suffix):
            return True
    return False


def has_italian_audio(video_path):
    """Check if the video file has an Italian audio track using ffprobe."""
    import subprocess
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_streams", "-select_streams", "a", video_path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return False
        data = json.loads(result.stdout)
        for stream in data.get("streams", []):
            tags = stream.get("tags", {})
            lang = tags.get("language", "").lower()
            title = tags.get("title", "").lower()
            if lang in ("ita", "it", "italian"):
                return True
            if "italian" in title or "italiano" in title:
                return True
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
        log.debug(f"ffprobe check skipped for {os.path.basename(video_path)}: {e}")
    return False


def is_excluded(filepath, excludes):
    folder = get_series_folder(filepath)
    for exc in excludes:
        if folder.lower() == exc.lower():
            return True
    return False


def scan_missing(state, excludes):
    missing = []
    now = datetime.now()

    for media_path in [SERIES_PATH, FILMS_PATH]:
        if not os.path.exists(media_path):
            continue
        for root, dirs, files in os.walk(media_path):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in VIDEO_EXTENSIONS:
                    continue

                full_path = os.path.join(root, fname)

                if has_italian_sub(full_path):
                    continue

                if is_excluded(full_path, excludes):
                    continue

                if has_italian_audio(full_path):
                    log.info(f"  Skipping (Italian audio): {fname}")
                    continue

                # Check state
                info = state["asked"].get(full_path)
                if info:
                    t = datetime.fromisoformat(info["time"])
                    status = info["status"]
                    if status == "pending":
                        continue  # Already asked, waiting for response
                    if status == "no" and now - t < timedelta(hours=RETRY_AFTER_HOURS):
                        continue  # User said no, wait before re-asking
                    if status == "failed" and now - t < timedelta(hours=FAILED_RETRY_HOURS):
                        continue  # Download failed, wait before retrying

                if full_path in state["downloaded"]:
                    continue  # Already downloaded

                missing.append(full_path)

    return missing


# =============================================================================
# OPENSUBTITLES CLIENT
# =============================================================================

_LANG_2LETTER = {"ita": "it", "eng": "en", "it": "it", "en": "en"}


def _to_iso2(lang):
    return _LANG_2LETTER.get((lang or "").lower(), (lang or "en").lower()[:2])


class OSClient:
    """Client for OpenSubtitles.com REST API v1.

    Returns results in a legacy-compatible shape (SubFileName, MovieReleaseName,
    IDSubtitleFile, SubFormat, SubDownloadsCnt, SubRating, MatchedBy) so that
    the existing pick_best / _download_first_valid code keeps working.
    """

    def __init__(self):
        self.api_key = OPENSUBTITLES_API_KEY
        self.available = bool(self.api_key)
        self.token = None  # kept for backwards compat with callers
        self.downloads_remaining = None  # populated after POST /download

    def _headers(self, json_body=False):
        h = {
            "Api-Key": self.api_key,
            "User-Agent": OPENSUBTITLES_USER_AGENT,
            "Accept": "application/json",
        }
        if json_body:
            h["Content-Type"] = "application/json"
        return h

    def _request(self, method, path, params=None, body=None, timeout=15):
        if not self.available:
            return None
        url = f"{OPENSUBTITLES_API_URL}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=self._headers(json_body=bool(body)))
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body_text = ""
            try:
                body_text = e.read().decode("utf-8", errors="ignore")[:200]
            except Exception:
                pass
            log.warning(f"  OpenSubtitles {method} {path} HTTP {e.code}: {body_text}")
        except Exception as e:
            log.warning(f"  OpenSubtitles {method} {path} error: {e}")
        return None

    def login(self):
        """No-op: REST v1 uses API key, no session token required."""
        if self.available:
            log.info("OpenSubtitles.com REST v1: ready")
            return True
        log.info("OpenSubtitles: OPENSUBTITLES_API_KEY not set, skipping")
        return False

    def logout(self):
        pass

    @staticmethod
    def _to_legacy(item):
        """Map a REST v1 subtitle item to the legacy XML-RPC field names."""
        attrs = item.get("attributes", {}) or {}
        files = attrs.get("files") or []
        first = files[0] if files else {}
        file_name = first.get("file_name") or attrs.get("release") or ""
        fmt = os.path.splitext(file_name)[1].lstrip(".").lower() or "srt"
        return {
            "SubFileName": file_name,
            "MovieReleaseName": attrs.get("release", ""),
            "IDSubtitleFile": str(first.get("file_id", "")),
            "SubFormat": fmt,
            "SubDownloadsCnt": attrs.get("download_count", 0) or 0,
            "SubRating": attrs.get("ratings", 0) or 0,
            "MatchedBy": "moviehash" if attrs.get("moviehash_match") else "",
            "_osd_attrs": attrs,
        }

    def _search(self, params):
        resp = self._request("GET", "/subtitles", params=params)
        if not resp:
            return []
        return [self._to_legacy(it) for it in (resp.get("data") or [])]

    def search_hash(self, file_hash, file_size):
        return self._search({"moviehash": file_hash, "languages": _to_iso2(OS_LANGUAGE)})

    def search_imdb(self, imdb_id, season=None, episode=None, language=None):
        lang = _to_iso2(language or OS_LANGUAGE)
        imdb_num = str(imdb_id).lower().lstrip("t")
        params = {"imdb_id": imdb_num, "languages": lang}
        if season is not None:
            params["season_number"] = int(season)
        if episode is not None:
            params["episode_number"] = int(episode)
        return self._search(params)

    def search_name(self, query, season=None, episode=None, language=None):
        lang = _to_iso2(language or OS_LANGUAGE)
        params = {"query": query, "languages": lang}
        if season is not None:
            params["season_number"] = int(season)
        if episode is not None:
            params["episode_number"] = int(episode)
        return self._search(params)

    def download(self, sub_id):
        """Request a download link, then fetch the SRT bytes."""
        if not sub_id:
            return None
        try:
            file_id = int(sub_id)
        except (TypeError, ValueError):
            log.warning(f"  OpenSubtitles download: invalid file_id '{sub_id}'")
            return None
        resp = self._request("POST", "/download", body={"file_id": file_id})
        if not resp:
            return None
        # Track download quota from response
        remaining = resp.get("remaining")
        if remaining is not None:
            self.downloads_remaining = int(remaining)
            log.info(f"  OpenSubtitles downloads remaining today: {self.downloads_remaining}")
        link = resp.get("link")
        if not link:
            log.warning(f"  OpenSubtitles download: no link in response ({resp})")
            return None
        try:
            with urllib.request.urlopen(link, timeout=30) as r:
                return r.read()
        except Exception as e:
            log.warning(f"  OpenSubtitles download fetch failed: {e}")
            return None


# =============================================================================
# SUBDL.COM CLIENT
# =============================================================================

class SubdlClient:
    """Client for Subdl.com subtitle API — no VIP placeholders."""

    LANG_MAP = {"ita": "it", "eng": "en", "it": "it", "en": "en"}

    def search(self, query, language="it", imdb_id=None, season=None, episode=None):
        if not SUBDL_API_KEY:
            return []
        try:
            lang = self.LANG_MAP.get(language, language)
            params = {
                "api_key": SUBDL_API_KEY,
                "languages": lang,
                "subs_per_page": "30",
            }
            if imdb_id:
                params["imdb_id"] = imdb_id
            else:
                params["film_name"] = query
            if season is not None:
                params["season_number"] = str(season)
            if episode is not None:
                params["episode_number"] = str(episode)

            url = SUBDL_API_URL + "?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(url, headers={"User-Agent": "SubFetcher/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            if not data.get("status"):
                log.warning(f"  Subdl API error: {data}")
                return []

            subs = data.get("subtitles", [])
            log.info(f"  Subdl search ({imdb_id or query}, {lang}): {len(subs)} results")
            if subs:
                log.debug(f"  Subdl first result keys: {list(subs[0].keys())}")
                log.info(f"  Subdl first result url: {subs[0].get('url', 'N/A')}")
            return subs

        except Exception as e:
            log.error(f"  Subdl search error: {e}")
            return []

    def download(self, sub_info):
        """Download subtitle from Subdl. Returns SRT content bytes or None."""
        try:
            url_path = sub_info.get("url", "")
            if not url_path:
                return None

            # URL from API may be relative path or full — normalize it
            if url_path.startswith("http"):
                url = url_path
            elif url_path.startswith("/"):
                url = f"https://dl.subdl.com{url_path}"
            else:
                url = f"https://dl.subdl.com/subtitle/{url_path}"
            log.info(f"  Subdl download URL: {url}")
            req = urllib.request.Request(url, headers={"User-Agent": "SubFetcher/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                zip_data = resp.read()

            # Extract best .srt from ZIP (prefer non-forced)
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                srt_files = [n for n in zf.namelist() if n.lower().endswith(".srt")]
                forced_tags = ["forced", "signs", "songs"]
                non_forced = [n for n in srt_files if not any(t in n.lower() for t in forced_tags)]
                best = non_forced[0] if non_forced else (srt_files[0] if srt_files else None)
                if best:
                    content = zf.read(best)
                    if non_forced and len(srt_files) > len(non_forced):
                        log.info(f"  Subdl: skipped forced subs, using: {best}")
                    log.info(f"  Subdl downloaded: {best} from {sub_info.get('release_name', '?')}")
                    return content

            log.warning(f"  Subdl ZIP contains no .srt files")
            return None

        except Exception as e:
            log.error(f"  Subdl download error: {e}")
            return None

    def search_and_download(self, video_path, language="it", trace=None):
        """Search and download best subtitle from Subdl. Returns content bytes or None.
        If trace is a list, appends per-attempt entries describing what was tried."""
        parsed = parse_video(video_path)
        season = parsed.get("season") if parsed["type"] == "episode" else None
        episode = parsed.get("episode") if parsed["type"] == "episode" else None
        lang_label = language.upper()

        # 1. Search by IMDB ID
        imdb_id = find_imdb_id(video_path)
        if imdb_id:
            time.sleep(DELAY_BETWEEN_API_CALLS)
            results = self.search("", language=language, imdb_id=imdb_id, season=season, episode=episode)
            if trace is not None:
                trace.append({"provider": "Subdl", "lang": lang_label,
                              "method": "imdb", "query": imdb_id, "results": len(results)})
            if results:
                content, reject = self._try_download(results, video_path)
                if content:
                    return content
                if trace is not None and reject:
                    trace[-1]["rejected"] = reject
        elif trace is not None:
            trace.append({"provider": "Subdl", "lang": lang_label,
                          "method": "imdb", "query": "(non risolto)", "results": 0})

        # 2. Search by name cascade
        queries = get_search_queries(video_path)
        for query in queries:
            time.sleep(DELAY_BETWEEN_API_CALLS)
            results = self.search(query, language=language, season=season, episode=episode)
            if trace is not None:
                trace.append({"provider": "Subdl", "lang": lang_label,
                              "method": "nome", "query": query, "results": len(results)})
            if results:
                content, reject = self._try_download(results, video_path)
                if content:
                    return content
                if trace is not None and reject:
                    trace[-1]["rejected"] = reject

        return None

    def _try_download(self, results, video_path, max_tries=3):
        """Try downloading from results, picking best match first.
        Returns (content_bytes_or_None, rejection_reason_or_None)."""
        video_base = os.path.splitext(os.path.basename(video_path))[0].lower()
        parsed = parse_video(video_path)
        video_season = parsed.get("season")
        video_episode = parsed.get("episode")

        # Extract release group (last part after dash, e.g. "FENiX" from "...x264-FENiX")
        release_group = ""
        m = re.search(r"-([a-zA-Z0-9]+)$", os.path.splitext(os.path.basename(video_path))[0])
        if m:
            release_group = m.group(1).lower()

        # Extract all meaningful tokens from video filename for matching
        video_tokens = set(re.findall(r"[a-zA-Z0-9]+", video_base))

        # Score and sort results
        scored = []
        for sub in results:
            score = 0
            release = (sub.get("release_name", "") or "").lower()
            sub_name = (sub.get("name", "") or "").lower()
            sub_tokens = set(re.findall(r"[a-zA-Z0-9]+", release))

            # CRITICAL: episode match — reject subs for wrong episode
            if video_season is not None and video_episode is not None:
                ep_match = re.search(r"s(\d+)e(\d+)", release)
                if ep_match:
                    sub_season = int(ep_match.group(1))
                    sub_episode = int(ep_match.group(2))
                    if sub_season == video_season and sub_episode == video_episode:
                        score += 500
                    else:
                        score -= 1000

            # Penalize forced/signs-only subs heavily
            if any(tag in release or tag in sub_name for tag in ["forced", "signs", "songs", "sdh", "hi-only"]):
                score -= 200

            # Exact filename match
            if video_base in release:
                score += 100

            # Release group match (most important for sync)
            if release_group and release_group in release:
                score += 80

            # Quality/source tag matches
            for tag in ["720p", "1080p", "2160p", "bluray", "webrip", "web-dl", "hdtv", "amzn", "x264", "x265", "hevc"]:
                if tag in video_base and tag in release:
                    score += 10

            # Token overlap (how many words in common)
            common = video_tokens & sub_tokens
            score += len(common) * 2

            scored.append((score, sub))

        scored.sort(key=lambda x: x[0], reverse=True)
        if scored:
            log.info(f"  Subdl best match: score={scored[0][0]}, release={scored[0][1].get('release_name', '?')}")

        last_reject = None
        for i, (score, sub) in enumerate(scored[:max_tries]):
            time.sleep(DELAY_BETWEEN_API_CALLS)
            content = self.download(sub)
            if not content:
                last_reject = "download vuoto"
                continue
            if is_placeholder_sub(content):
                log.warning(f"  Subdl result {i+1} looks like placeholder, trying next")
                last_reject = "placeholder"
                continue
            # Reject forced/incomplete subs (fewer than 10 dialogue blocks)
            block_count = len(re.findall(rb"\d+\r?\n\d{2}:\d{2}:\d{2}", content))
            if block_count < 10:
                log.warning(f"  Subdl result {i+1} has only {block_count} blocks (likely forced/signs-only), trying next")
                last_reject = f"forced/incompleti ({block_count} blocchi)"
                continue
            return content, None

        if scored and not last_reject:
            last_reject = "nessun candidato valido"
        return None, last_reject


def pick_best(results, video_path):
    if not results:
        return None
    srt = [r for r in results if (r.get("SubFormat", "") or "").lower() == "srt"] or results
    video_base = os.path.splitext(os.path.basename(video_path))[0].lower()

    scored = []
    for sub in srt:
        score = 0
        sf = (sub.get("SubFileName", "") or "").lower()
        mr = (sub.get("MovieReleaseName", "") or "").lower()
        if sub.get("MatchedBy") == "moviehash":
            score += 100
        if video_base in sf or video_base in mr:
            score += 50
        for tag in ["galaxytv", "webrip", "720p", "1080p", "amzn", "bluray", "web-dl"]:
            if tag in video_base and tag in (sf + mr):
                score += 10
        score += min(int(sub.get("SubDownloadsCnt", 0) or 0) // 100, 20)
        score += int(float(sub.get("SubRating", 0) or 0))
        scored.append((score, sub))

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1] if scored else None


# =============================================================================
# SUBTITLE SYNC (ffsubsync)
# =============================================================================

def sync_subtitle(video_path, srt_path, min_score=0):
    """Sync subtitle to video audio using ffsubsync. Overwrites srt_path in place.
    Returns dict with 'ok', 'score', 'offset', 'fps_scale' or False on failure.
    If min_score > 0, rejects syncs with score below threshold."""
    import subprocess, re as _re, tempfile, shutil
    try:
        tmp_out = tempfile.mktemp(suffix=".srt", prefix="ffsync_")
        tmp_log = tempfile.mktemp(suffix=".log", prefix="ffsync_")
        cmd = f'ffsubsync "{video_path}" -i "{srt_path}" -o "{tmp_out}" > "{tmp_log}" 2>&1'
        exit_code = os.system(cmd)
        log_content = ""
        if os.path.exists(tmp_log):
            with open(tmp_log) as f:
                log_content = f.read()
            os.remove(tmp_log)
        score_match = _re.search(r"score:\s*([\d.-]+)", log_content)
        offset_match = _re.search(r"offset seconds:\s*([\d.-]+)", log_content)
        framerate_match = _re.search(r"framerate scale factor:\s*([\d.]+)", log_content)
        score = float(score_match.group(1)) if score_match else 0.0
        offset = offset_match.group(1) if offset_match else "?"
        fps = framerate_match.group(1) if framerate_match else "1.000"
        if os.path.exists(tmp_out) and os.path.getsize(tmp_out) > 0:
            if min_score > 0 and score < min_score:
                log.warning(f"  ffsubsync score too low ({score:.1f} < {min_score}), rejecting sync")
                os.remove(tmp_out)
                return {"ok": False, "score": score, "offset": offset, "fps_scale": fps}
            shutil.move(tmp_out, srt_path)
            log.info(f"  🔄 Synced: {os.path.basename(srt_path)} (score: {score:.0f}, offset: {offset}s, fps_scale: {fps})")
            return {"ok": True, "score": score, "offset": offset, "fps_scale": fps}
        else:
            log.warning(f"  ffsubsync no output (exit {exit_code}): {log_content[:500]}")
            if os.path.exists(tmp_out):
                os.remove(tmp_out)
    except Exception as e:
        log.warning(f"  ffsubsync error: {e}")
        for p in [tmp_out, tmp_log]:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except:
                pass
    return False


def validate_sync(video_path, content_bytes, dest_path):
    """Write subtitle content to dest_path, sync it to the video audio,
    and validate the sync score against SYNC_MIN_SCORE.

    Returns:
      {"ok": True, "score": float}  — sync passed, dest_path contains synced sub
      {"ok": False, "score": float} — sync ran but score too low, dest_path removed
      False                         — sync completely failed, dest_path removed
    """
    try:
        with open(dest_path, "wb") as f:
            f.write(content_bytes if isinstance(content_bytes, bytes)
                    else content_bytes.encode("utf-8"))
    except Exception as e:
        log.error(f"  validate_sync: failed to write {dest_path}: {e}")
        return False

    result = sync_subtitle(video_path, dest_path, min_score=SYNC_MIN_SCORE)

    if result and result.get("ok"):
        return result

    # Sync failed or score too low — clean up the file
    try:
        if os.path.exists(dest_path):
            os.remove(dest_path)
    except Exception:
        pass

    if result and not result.get("ok"):
        log.warning(f"  validate_sync: score {result.get('score', '?')} < {SYNC_MIN_SCORE}, discarding")
        return result

    return False


# =============================================================================
# SUBTITLE VALIDATION (detect VIP placeholders)
# =============================================================================

# Patterns that appear in VIP placeholder ads but (almost) never in real subs.
# "opensubtitles" alone is too broad — real subs often credit opensubtitles.org
# in their footer. We require a stronger signal: phrases specific to the VIP
# upsell ads, OR a high density of placeholder keywords in a short sub.
PLACEHOLDER_STRONG_PATTERNS = [
    "osdb.link",
    "become a vip",
    "vip member",
    "advertise your product",
    "api.opensubtitles.org",
    "support us and become vip",
]
PLACEHOLDER_WEAK_PATTERNS = [
    "opensubtitles",
    "advertis",
    "get subtitles",
]


def is_placeholder_sub(content):
    """Check if downloaded subtitle content is a VIP placeholder/ad."""
    try:
        text = content.decode("utf-8", errors="ignore").lower()
    except Exception:
        text = str(content).lower()

    # Count actual SRT blocks first — real subs have 300-2000 blocks.
    blocks = re.findall(r"\d+\s*\n\d{2}:\d{2}:\d{2}", text)
    n_blocks = len(blocks)

    if n_blocks < 3:
        return True  # Too short to be real

    # Strong patterns = almost certainly a placeholder, reject immediately.
    for pattern in PLACEHOLDER_STRONG_PATTERNS:
        if pattern in text:
            return True

    # Weak patterns only count if the sub is also suspiciously short.
    # A real 90-min movie sub has >=200 blocks; anything shorter with
    # placeholder-ish keywords is almost always a fake.
    if n_blocks < 50:
        for pattern in PLACEHOLDER_WEAK_PATTERNS:
            if pattern in text:
                return True

    # Check if a single block spans an absurd time range (e.g., 00:00:00 --> 04:00:00)
    long_spans = re.findall(
        r"(\d{2}):(\d{2}):(\d{2}),\d+ --> (\d{2}):(\d{2}):(\d{2}),\d+", text
    )
    for h1, m1, s1, h2, m2, s2 in long_spans:
        start_sec = int(h1) * 3600 + int(m1) * 60 + int(s1)
        end_sec = int(h2) * 3600 + int(m2) * 60 + int(s2)
        if end_sec - start_sec > 600:  # Single block > 10 minutes = fake
            return True

    return False


# =============================================================================
# CLAUDE TRANSLATION (EN -> IT)
# =============================================================================

def parse_srt(content):
    """Parse SRT content into list of (index, timecode, text) tuples."""
    blocks = []
    # Normalize line endings
    text = content.replace("\r\n", "\n").replace("\r", "\n")
    # Split by double newlines (block separator)
    raw_blocks = re.split(r"\n\n+", text.strip())

    for block in raw_blocks:
        lines = block.strip().split("\n")
        if len(lines) >= 3:
            idx = lines[0].strip()
            timecode = lines[1].strip()
            subtitle_text = "\n".join(lines[2:])
            blocks.append((idx, timecode, subtitle_text))
        elif len(lines) == 2:
            # Sometimes index + timecode with empty text
            idx = lines[0].strip()
            timecode = lines[1].strip()
            blocks.append((idx, timecode, ""))

    return blocks


def blocks_to_srt(blocks):
    """Convert list of (index, timecode, text) back to SRT string."""
    parts = []
    for idx, timecode, text in blocks:
        parts.append(f"{idx}\n{timecode}\n{text}")
    return "\n\n".join(parts) + "\n"


# Claude API pricing (USD per million tokens) — Sonnet
CLAUDE_INPUT_PRICE = 3.0   # $3/M input tokens
CLAUDE_OUTPUT_PRICE = 15.0  # $15/M output tokens


def estimate_translation_cost(srt_content):
    """Estimate the Claude API cost for translating an SRT file.
    Returns (estimated_cost_usd, num_blocks).

    Calibrated on real usage data: output tokens ~ input tokens * 1.09,
    but output costs 5x more than input so the output dominates the total.
    A 15% safety margin is added to avoid systematic underestimation."""
    blocks = parse_srt(srt_content)
    if not blocks:
        return 0.0, 0
    total_text = " ".join(text for _, _, text in blocks if text.strip())
    word_count = len(total_text.split())
    num_batches = len(blocks) // 100 + 1
    input_tokens = int(word_count * 1.3) + num_batches * 150
    output_tokens = int(input_tokens * 1.15)  # Italian tends to be slightly longer
    raw_cost = (input_tokens * CLAUDE_INPUT_PRICE + output_tokens * CLAUDE_OUTPUT_PRICE) / 1_000_000
    cost = raw_cost * 1.15  # 15% safety margin based on observed real vs estimated ratio
    return cost, len(blocks)


def translate_srt_with_claude(srt_content, video_name):
    """Translate SRT content from English to Italian using Claude API.
    Sends text in batches to stay within limits. Returns (translated_srt, usage_stats) tuple."""

    if not CLAUDE_API_KEY:
        log.error("CLAUDE_API_KEY not set, cannot translate")
        return None

    blocks = parse_srt(srt_content)
    if not blocks:
        log.error("No SRT blocks found to translate")
        return None

    log.info(f"  Translating {len(blocks)} subtitle blocks EN->IT via Claude...")

    # Process in batches of ~100 blocks to avoid token limits
    BATCH_SIZE = 100
    translated_blocks = []
    total_input_tokens = 0
    total_output_tokens = 0

    for batch_start in range(0, len(blocks), BATCH_SIZE):
        batch = blocks[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(blocks) + BATCH_SIZE - 1) // BATCH_SIZE

        # Build numbered text for translation
        lines_to_translate = []
        for i, (idx, timecode, text) in enumerate(batch):
            if text.strip():
                lines_to_translate.append(f"[{i}] {text}")

        if not lines_to_translate:
            translated_blocks.extend(batch)
            continue

        text_block = "\n".join(lines_to_translate)

        prompt = (
            f"Translate these movie subtitles from English to Italian. "
            f"Movie: {video_name}. "
            f"Keep the [N] numbering prefix on each line. "
            f"Translate naturally — use colloquial Italian as it would appear in a real Italian dub. "
            f"Keep it concise as subtitles should be. "
            f"Do NOT add any explanation, just output the translated lines.\n\n"
            f"{text_block}"
        )

        try:
            payload = json.dumps({
                "model": CLAUDE_MODEL,
                "max_tokens": 4096,
                "messages": [{"role": "user", "content": prompt}],
            }).encode("utf-8")

            req = urllib.request.Request(
                CLAUDE_API_URL,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                },
            )

            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.loads(resp.read().decode("utf-8"))

            # Track token usage
            usage = result.get("usage", {})
            total_input_tokens += usage.get("input_tokens", 0)
            total_output_tokens += usage.get("output_tokens", 0)

            # Extract translated text
            translated_text = ""
            for block_resp in result.get("content", []):
                if block_resp.get("type") == "text":
                    translated_text += block_resp["text"]

            # Parse translated lines back by [N] prefix
            translations = {}
            for line in translated_text.strip().split("\n"):
                line = line.strip()
                m = re.match(r"\[(\d+)\]\s*(.*)", line)
                if m:
                    translations[int(m.group(1))] = m.group(2)

            # Rebuild batch with translations
            for i, (idx, timecode, text) in enumerate(batch):
                if i in translations:
                    translated_blocks.append((idx, timecode, translations[i]))
                else:
                    translated_blocks.append((idx, timecode, text))

            log.info(f"  Batch {batch_num}/{total_batches} translated ({len(translations)} lines)")
            time.sleep(1)  # Rate limiting

        except Exception as e:
            log.error(f"  Claude translation error (batch {batch_num}): {e}")
            # Keep original English for this batch on error
            translated_blocks.extend(batch)

    # Log and track costs
    cost = (total_input_tokens * CLAUDE_INPUT_PRICE + total_output_tokens * CLAUDE_OUTPUT_PRICE) / 1_000_000
    log.info(f"  Translation cost: {total_input_tokens} in + {total_output_tokens} out = ${cost:.4f}")

    # Save usage to state
    try:
        state = load_state()
        costs = state.setdefault("claude_costs", {"total_input_tokens": 0, "total_output_tokens": 0, "total_cost_usd": 0.0, "translations": 0})
        costs["total_input_tokens"] += total_input_tokens
        costs["total_output_tokens"] += total_output_tokens
        costs["total_cost_usd"] += cost
        costs["translations"] += 1
        save_state(state)
    except Exception:
        pass

    return blocks_to_srt(translated_blocks)


def _cascade_search(client, video_path, language, file_hash=None, file_size=0, trace=None):
    """Search OpenSubtitles using a cascade: hash -> IMDB ID -> name variants.
    Returns a list of results (may be empty).
    If trace is a list, appends per-attempt entries describing what was tried."""
    parsed = parse_video(video_path)
    season = parsed.get("season") if parsed["type"] == "episode" else None
    episode = parsed.get("episode") if parsed["type"] == "episode" else None
    lang_label = language.upper()

    # 1. Hash search
    if file_hash:
        time.sleep(DELAY_BETWEEN_API_CALLS)
        try:
            results = client.search_hash(file_hash, file_size)
            log.info(f"  {lang_label} hash search: {len(results)} results")
            if trace is not None:
                trace.append({"provider": "OpenSubtitles", "lang": lang_label,
                              "method": "hash", "query": file_hash, "results": len(results)})
            if results:
                return results
        except Exception as e:
            log.error(f"  {lang_label} hash search error: {e}")
            if trace is not None:
                trace.append({"provider": "OpenSubtitles", "lang": lang_label,
                              "method": "hash", "query": file_hash, "results": 0,
                              "error": str(e)})

    # 2. IMDB ID search
    imdb_id = find_imdb_id(video_path)
    if imdb_id:
        time.sleep(DELAY_BETWEEN_API_CALLS)
        results = client.search_imdb(imdb_id, season, episode, language=language)
        log.info(f"  {lang_label} IMDB search ({imdb_id}): {len(results)} results")
        if trace is not None:
            trace.append({"provider": "OpenSubtitles", "lang": lang_label,
                          "method": "imdb", "query": imdb_id, "results": len(results)})
        if results:
            return results
    elif trace is not None:
        trace.append({"provider": "OpenSubtitles", "lang": lang_label,
                      "method": "imdb", "query": "(non risolto)", "results": 0})

    # 3. Name search cascade
    queries = get_search_queries(video_path)
    for query in queries:
        time.sleep(DELAY_BETWEEN_API_CALLS)
        results = client.search_name(query, season, episode, language=language)
        log.info(f"  {lang_label} name search \"{query}\": {len(results)} results")
        if trace is not None:
            trace.append({"provider": "OpenSubtitles", "lang": lang_label,
                          "method": "nome", "query": query, "results": len(results)})
        if results:
            return results

    return []


def _download_first_valid(client, results, video_path, max_tries=15):
    """Try downloading subtitles from results in score order, skipping placeholders.
    Returns (content_bytes, sub_info) or (None, None)."""
    if not results:
        return None, None

    srt = [r for r in results if (r.get("SubFormat", "") or "").lower() == "srt"] or results
    video_base = os.path.splitext(os.path.basename(video_path))[0].lower()

    scored = []
    for sub in srt:
        score = 0
        sf = (sub.get("SubFileName", "") or "").lower()
        mr = (sub.get("MovieReleaseName", "") or "").lower()
        if sub.get("MatchedBy") == "moviehash":
            score += 100
        if video_base in sf or video_base in mr:
            score += 50
        for tag in ["galaxytv", "webrip", "720p", "1080p", "amzn", "bluray", "web-dl"]:
            if tag in video_base and tag in (sf + mr):
                score += 10
        score += min(int(sub.get("SubDownloadsCnt", 0) or 0) // 100, 20)
        score += int(float(sub.get("SubRating", 0) or 0))
        scored.append((score, sub))

    scored.sort(key=lambda x: x[0], reverse=True)

    for i, (score, sub) in enumerate(scored[:max_tries]):
        time.sleep(DELAY_BETWEEN_API_CALLS)
        content = client.download(sub.get("IDSubtitleFile", ""))
        if content and not is_placeholder_sub(content):
            log.info(f"  Valid sub found (attempt {i+1}): {sub.get('SubFileName', '?')}")
            return content, sub
        elif content:
            log.warning(f"  Placeholder rejected (attempt {i+1}): {sub.get('SubFileName', '?')}")
        else:
            log.warning(f"  Empty download (attempt {i+1}): {sub.get('SubFileName', '?')}")

    log.warning(f"  All {min(len(scored), max_tries)} attempts were placeholders")
    return None, None


def search_and_download_english(client, video_path, file_hash=None, file_size=0, trace=None):
    """Search for English subtitles as fallback. Returns content bytes or None."""
    results = _cascade_search(client, video_path, "eng", file_hash, file_size, trace=trace)

    content, sub = _download_first_valid(client, results, video_path)
    if content:
        log.info(f"  ENG sub downloaded: {sub.get('SubFileName', '?')}")
        return content
    if trace is not None and results:
        trace.append({"provider": "OpenSubtitles", "lang": "ENG",
                      "method": "download", "query": "",
                      "results": len(results), "rejected": "tutti rifiutati (placeholder/invalid)"})
    return None


# =============================================================================
# DOWNLOAD WORKFLOW
# =============================================================================

def _save_sub_and_update_state(video_path, sub_path, source, state):
    """Save subtitle state and send Telegram notification."""
    state["downloaded"][video_path] = {
        "sub": os.path.basename(sub_path),
        "source": source,
        "time": datetime.now().isoformat(),
    }
    state["asked"].pop(video_path, None)
    save_state(state)


def _translate_and_save(eng_content, video_path, state, silent=False, skip_sync=False):
    """Translate English content to Italian and save. Returns True on success.
    Also saves the English subtitle as .en.srt alongside the Italian one.
    skip_sync=True when the English sub is already local (timecodes match the video)."""
    if isinstance(eng_content, bytes):
        eng_text = eng_content.decode("utf-8", errors="ignore")
    elif isinstance(eng_content, str) and os.path.isfile(eng_content):
        try:
            with open(eng_content, "r", encoding="utf-8", errors="ignore") as f:
                eng_text = f.read()
        except Exception as e:
            log.error(f"  Failed to read English subtitle: {e}")
            return False
    else:
        eng_text = eng_content

    # Save English subtitle alongside the video
    en_srt_path = os.path.splitext(video_path)[0] + ".en.srt"
    if not os.path.exists(en_srt_path):
        try:
            with open(en_srt_path, "w", encoding="utf-8") as f:
                f.write(eng_text)
            log.info(f"  Saved English sub: {os.path.basename(en_srt_path)}")
        except Exception as e:
            log.warning(f"  Failed to save English sub: {e}")

    video_name = friendly_name(video_path)

    est_cost, num_blocks = estimate_translation_cost(eng_text)
    log.info(f"  Estimated translation cost: ${est_cost:.4f} ({num_blocks} blocks)")

    translated_srt = translate_srt_with_claude(eng_text, video_name)

    if not translated_srt:
        log.error(f"  Translation failed for: {os.path.basename(video_path)}")
        return False

    sub_path = os.path.splitext(video_path)[0] + ".it.srt"
    with open(sub_path, "w", encoding="utf-8") as f:
        f.write(translated_srt)

    if skip_sync:
        log.info(f"  Skipping sync (local English sub, timecodes already correct)")
    else:
        sync_subtitle(video_path, sub_path)

    # Get actual cost from state (updated by translate_srt_with_claude)
    actual_state = load_state()
    costs = actual_state.get("claude_costs", {})
    total_spent = costs.get("total_cost_usd", 0.0)

    _save_sub_and_update_state(video_path, sub_path, "Claude EN→IT translation", state)
    log.info(f"  ✅ Translated & saved: {os.path.basename(sub_path)}")
    if not silent:
        tg_send(
            f"🤖 Sub ITA tradotto da ENG (Claude):\n"
            f"<b>{friendly_name(video_path)}</b>\n"
            f"📁 {os.path.basename(sub_path)}\n"
            f"💰 Totale speso: ${total_spent:.4f}"
        )
    return True


def format_search_trace(trace):
    """Render a search trace into a compact, Telegram-friendly text block."""
    if not trace:
        return ""
    lines = []
    quota = None
    for entry in trace:
        if "_quota" in entry:
            quota = entry["_quota"]
            continue
        provider = entry.get("provider", "?")
        lang = entry.get("lang", "?")
        method = entry.get("method", "")
        query = entry.get("query", "")
        n = entry.get("results", 0)
        rejected = entry.get("rejected") or entry.get("error")
        loc = f"{provider} {method}".strip()
        q_part = f" '{query}'" if query else ""
        suffix = f" → {rejected}" if rejected else ""
        lines.append(f"  • {loc} {lang}{q_part}: {n}{suffix}")
    if quota is not None:
        lines.append(f"  📊 OpenSubtitles download rimanenti: {quota}")
    return "\n".join(lines)


def do_download(video_path, state, silent=False, translate=True, trace=None):
    """Search and download subtitle for a video file.
    Returns: True (ITA found), "en_only" (EN saved, needs translation), False (nothing found).
    When translate=False, downloads EN sub but does NOT translate (saves money, user decides later).
    If trace is a list, it is populated with per-attempt search entries."""
    fname = os.path.basename(video_path)
    log.info(f"Downloading sub for: {fname}")

    # =================================================================
    # STEP 0: Check for existing subtitles already in the folder
    # =================================================================
    existing = find_existing_srt(video_path)
    if existing:
        if existing["lang"] == "it":
            log.info(f"  Found existing Italian sub: {existing['path']}")
            sub_path = os.path.splitext(video_path)[0] + ".it.srt"
            if existing["path"] != sub_path:
                import shutil
                shutil.copy2(existing["path"], sub_path)
            _save_sub_and_update_state(video_path, sub_path, f"existing file: {os.path.basename(existing['path'])}", state)
            if not silent:
                tg_send(f"✅ Sub ITA trovato nella cartella:\n<b>{friendly_name(video_path)}</b>")
            return True
        elif existing["lang"] == "en":
            en_srt_path = os.path.splitext(video_path)[0] + ".en.srt"
            if existing["path"] != en_srt_path:
                import shutil
                shutil.copy2(existing["path"], en_srt_path)
            log.info(f"  Found existing English sub: {existing['path']}")
            if translate and CLAUDE_API_KEY:
                log.info(f"  Translating existing EN sub...")
                if _translate_and_save(en_srt_path, video_path, state, silent=silent, skip_sync=True):
                    return True
            else:
                return "en_only"

    # =================================================================
    # STEP 1: Search ITA — Subdl then OpenSubtitles, validate sync
    # =================================================================
    subdl = SubdlClient()
    client = OSClient()
    os_logged_in = client.login()
    sub_path = os.path.splitext(video_path)[0] + ".it.srt"

    # --- Subdl ITA ---
    ita_content = subdl.search_and_download(video_path, language="it", trace=trace)
    if ita_content:
        sync_result = validate_sync(video_path, ita_content, sub_path)
        if sync_result and sync_result.get("ok"):
            _save_sub_and_update_state(video_path, sub_path, "Subdl.com", state)
            log.info(f"  ✅ Saved (Subdl, sync score {sync_result['score']:.0f}): {os.path.basename(sub_path)}")
            if not silent:
                tg_send(f"✅ Sub ITA scaricato (Subdl):\n<b>{friendly_name(video_path)}</b>\n📁 {os.path.basename(sub_path)}")
            if os_logged_in:
                client.logout()
            return True
        else:
            score_val = sync_result.get("score", "?") if isinstance(sync_result, dict) else "N/A"
            log.warning(f"  Subdl ITA sync score too low ({score_val}), discarding")
            if trace is not None:
                trace.append({"provider": "Subdl", "lang": "ITA",
                              "method": "sync", "query": "",
                              "results": 1, "rejected": f"sync score troppo basso ({score_val})"})

    # --- OpenSubtitles ITA ---
    if os_logged_in:
        try:
            file_hash, file_size = compute_hash(video_path)
            results = _cascade_search(client, video_path, "ita", file_hash, file_size, trace=trace)
            content, best = _download_first_valid(client, results, video_path)
            if trace is not None and results and not content:
                trace.append({"provider": "OpenSubtitles", "lang": "ITA",
                              "method": "download", "query": "",
                              "results": len(results),
                              "rejected": "tutti rifiutati (placeholder/invalid)"})

            if content:
                sync_result = validate_sync(video_path, content, sub_path)
                if sync_result and sync_result.get("ok"):
                    _save_sub_and_update_state(video_path, sub_path, f"OpenSubtitles: {best.get('SubFileName', '')}", state)
                    log.info(f"  ✅ Saved (OS, sync score {sync_result['score']:.0f}): {os.path.basename(sub_path)}")
                    if not silent:
                        tg_send(f"✅ Sub ITA scaricato (OS):\n<b>{friendly_name(video_path)}</b>\n📁 {os.path.basename(sub_path)}")
                    client.logout()
                    return True
                else:
                    score_val = sync_result.get("score", "?") if isinstance(sync_result, dict) else "N/A"
                    log.warning(f"  OS ITA sync score too low ({score_val}), discarding")
                    if trace is not None:
                        trace.append({"provider": "OpenSubtitles", "lang": "ITA",
                                      "method": "sync", "query": "",
                                      "results": 1, "rejected": f"sync score troppo basso ({score_val})"})
        except Exception as e:
            log.error(f"  OpenSubtitles error: {e}")

    log.warning(f"  No valid Italian subs for: {fname}")

    # =================================================================
    # STEP 2: ENG fallback — download, validate sync, save .en.srt
    # =================================================================
    log.info(f"  Trying English fallback...")

    en_srt_path = os.path.splitext(video_path)[0] + ".en.srt"
    eng_content = subdl.search_and_download(video_path, language="en", trace=trace)

    if not eng_content and os_logged_in:
        file_hash, file_size = compute_hash(video_path)
        eng_content = search_and_download_english(client, video_path, file_hash, file_size, trace=trace)

    if os_logged_in:
        if trace is not None and client.downloads_remaining is not None:
            trace.append({"_quota": client.downloads_remaining})
        client.logout()

    if not eng_content:
        log.warning(f"  No English subs found either for: {fname}")
        state["asked"][video_path] = {"time": datetime.now().isoformat(), "status": "failed"}
        save_state(state)
        if not silent:
            trace_text = format_search_trace(trace)
            msg = f"❌ Nessun sub ITA né ENG trovato per:\n<b>{friendly_name(video_path)}</b>"
            if trace_text:
                msg += f"\n\n<b>Tentativi:</b>\n{trace_text}"
            tg_send(msg)
        return False

    # Try to sync ENG — if sync fails, save anyway (better than nothing)
    sync_result = validate_sync(video_path, eng_content, en_srt_path)
    if not sync_result or not sync_result.get("ok"):
        # validate_sync removed the file on failure — write it back unsyncedtry:
        try:
            with open(en_srt_path, "wb") as f:
                f.write(eng_content if isinstance(eng_content, bytes) else eng_content.encode("utf-8"))
            log.warning(f"  ENG sync failed/low score, saving unsynced: {os.path.basename(en_srt_path)}")
        except Exception as e:
            log.warning(f"  Failed to save English sub: {e}")
    else:
        log.info(f"  Saved synced English sub (score {sync_result['score']:.0f}): {os.path.basename(en_srt_path)}")

    if not translate:
        log.info(f"  ✅ EN sub saved (translation deferred, user will decide)")
        return "en_only"

    if not CLAUDE_API_KEY:
        log.info("  No CLAUDE_API_KEY set, skipping EN->IT translation")
        return "en_only"

    if _translate_and_save(en_srt_path, video_path, state, silent=silent, skip_sync=True):
        return True

    state["asked"][video_path] = {"time": datetime.now().isoformat(), "status": "failed"}
    save_state(state)
    if not silent:
        tg_send(f"❌ Traduzione EN→IT fallita per:\n<b>{friendly_name(video_path)}</b>")
    return False


# =============================================================================
# TELEGRAM INTERACTION
# =============================================================================

def ask_user(video_path, state):
    """Send a Telegram message asking if user wants to download subs (single file)."""
    name = friendly_name(video_path)
    folder = get_series_folder(video_path)
    path_hash = str(abs(hash(video_path)))[:8]

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "✅ Scarica", "callback_data": f"yes:{path_hash}"},
                {"text": "❌ No", "callback_data": f"no:{path_hash}"},
            ],
            [
                {"text": f"🚫 Escludi '{folder}'", "callback_data": f"exclude:{path_hash}"},
            ],
        ]
    }

    result = tg_send(
        f"🎬 <b>Sub ITA mancante</b>\n\n"
        f"📺 {name}\n"
        f"📁 {folder}\n"
        f"📄 {os.path.basename(video_path)}\n\n"
        f"Scarico il sottotitolo italiano?",
        reply_markup=keyboard,
    )

    msg_id = None
    if result and result.get("ok"):
        msg_id = result["result"]["message_id"]

    state["asked"][video_path] = {
        "time": datetime.now().isoformat(),
        "status": "pending",
        "path_hash": path_hash,
        "msg_id": msg_id,
    }
    save_state(state)


def group_by_series(video_paths):
    """Group video paths by their series/movie folder."""
    groups = {}
    for path in video_paths:
        folder = get_series_folder(path)
        groups.setdefault(folder, []).append(path)
    # Sort episodes within each group
    for folder in groups:
        groups[folder].sort()
    return groups


def _send_batch_message(folder, paths, state):
    """Send a single batch message for a group of files. Returns batch_hash."""
    batch_hash = str(abs(hash(folder + str(len(paths)) + paths[0])))[:8]

    episode_list = "\n".join(
        f"  • {friendly_name(p)}" for p in paths[:20]
    )
    if len(paths) > 20:
        episode_list += f"\n  ... e altri {len(paths) - 20}"

    keyboard = {
        "inline_keyboard": [
            [
                {"text": f"✅ Scarica tutti ({len(paths)})", "callback_data": f"batch_yes:{batch_hash}"},
                {"text": "❌ No", "callback_data": f"batch_no:{batch_hash}"},
            ],
            [
                {"text": f"🚫 Escludi '{folder}'", "callback_data": f"grp_exclude:{batch_hash}"},
            ],
        ]
    }

    result = tg_send(
        f"🎬 <b>Sub ITA mancanti</b>\n\n"
        f"📁 <b>{folder}</b> — {len(paths)} file\n\n"
        f"{episode_list}\n\n"
        f"Scarico i sottotitoli italiani?",
        reply_markup=keyboard,
    )

    msg_id = None
    if result and result.get("ok"):
        msg_id = result["result"]["message_id"]

    batches = load_batches()
    batches[batch_hash] = {
        "paths": paths,
        "folder": folder,
        "time": datetime.now().isoformat(),
        "msg_id": msg_id,
    }
    save_batches(batches)

    for p in paths:
        path_hash = str(abs(hash(p)))[:8]
        state["asked"][p] = {
            "time": datetime.now().isoformat(),
            "status": "pending",
            "path_hash": path_hash,
            "batch": batch_hash,
        }

    save_state(state)
    return batch_hash


def ask_user_grouped(missing, state):
    """Send grouped Telegram messages: one per series, one digest for all single films."""
    groups = group_by_series(missing)

    # Separate multi-episode groups from single-file groups
    singles = []
    for folder, paths in groups.items():
        if len(paths) > 1:
            _send_batch_message(folder, paths, state)
            time.sleep(1)
        else:
            singles.append(paths[0])

    # Send individual messages for each single film
    for p in singles:
        ask_user(p, state)
        time.sleep(0.5)


def find_path_by_hash(state, path_hash):
    """Find video path from callback hash."""
    for path, info in state["asked"].items():
        if info.get("path_hash") == path_hash:
            return path
    return None


def process_callbacks(state, excludes):
    """Check for Telegram callback responses."""
    offset = state.get("last_offset", 0)
    updates = tg_get_updates(offset)

    for update in updates:
        state["last_offset"] = update["update_id"] + 1

        # Handle callback queries (button presses)
        cb = update.get("callback_query")
        if cb:
            cb_id = cb["id"]
            data = cb.get("data", "")
            msg_id = cb.get("message", {}).get("message_id")

            if ":" not in data:
                tg_answer_callback(cb_id, "⚠️ Errore")
                continue

            action, path_hash = data.split(":", 1)
            video_path = find_path_by_hash(state, path_hash)

            # Handle batch callbacks
            if action in ("batch_yes", "batch_no", "grp_exclude", "batch_translate", "batch_keep_en"):
                state = load_state()
                batches = load_batches()
                batch = batches.get(path_hash)
                if not batch:
                    tg_answer_callback(cb_id, "⚠️ Batch non trovato")
                    continue

                if action == "batch_translate":
                    tg_answer_callback(cb_id, "🤖 Traduzione avviata...")
                    if msg_id:
                        tg_edit_message(msg_id,
                            f"🤖 <b>Traducendo EN→IT...</b>\n\n"
                            f"[░░░░░░░░░░] 0%\n"
                            f"📊 0/{len(batch['paths'])}")
                    download_queue.put({"type": "translate", "paths": batch["paths"], "msg_id": msg_id})
                    batches.pop(path_hash, None)
                    save_batches(batches)
                    continue

                elif action == "batch_keep_en":
                    tg_answer_callback(cb_id, "🇬🇧 Mantenuti solo ENG")
                    for p in batch.get("paths", []):
                        state["asked"][p] = {"time": datetime.now().isoformat(), "status": "en_only"}
                    if msg_id:
                        tg_edit_message(msg_id, f"🇬🇧 Sub inglesi mantenuti senza traduzione.\nPuoi tradurre in seguito con /translate")
                    batches.pop(path_hash, None)
                    save_batches(batches)
                    save_state(state)
                    continue

                elif action == "batch_yes":
                    pos = queue_position()
                    tg_answer_callback(cb_id, f"⬇️ In coda{f' (pos. {pos+1})' if pos > 0 else ''}...")
                    if msg_id:
                        tg_edit_message(msg_id,
                            f"⬇️ <b>Scaricando sottotitoli...</b>\n\n"
                            f"[░░░░░░░░░░] 0%\n"
                            f"📊 0/{len(batch['paths'])}"
                            + (f"\n⏳ In coda (posizione {pos+1})" if pos > 0 else ""))
                    download_queue.put({"type": "batch", "paths": batch["paths"], "msg_id": msg_id})
                elif action == "grp_exclude":
                    folder = batch.get("folder", "")
                    if folder:
                        excludes.add(folder)
                        save_excludes(excludes)
                        # Remove all pending asks for this folder
                        to_remove = [p for p in state["asked"] if get_series_folder(p).lower() == folder.lower()]
                        for p in to_remove:
                            del state["asked"][p]
                        tg_answer_callback(cb_id, f"🚫 '{folder}' esclusa")
                        if msg_id:
                            tg_edit_message(msg_id, f"🚫 <b>{folder}</b> esclusa dalla ricerca sub ITA.")
                    else:
                        tg_answer_callback(cb_id, "⚠️ Errore")
                else:
                    tg_answer_callback(cb_id, "⏭ Saltato")
                    # Mark all batch paths as "no"
                    for p in batch.get("paths", []):
                        state["asked"][p] = {"time": datetime.now().isoformat(), "status": "no"}
                    if msg_id:
                        tg_edit_message(msg_id, f"⏭ Batch saltato.\nRichiederò tra 3 giorni.")

                batches.pop(path_hash, None)
                save_batches(batches)
                save_state(state)
                continue

            if not video_path:
                tg_answer_callback(cb_id, "⚠️ Non trovato")
                continue

            name = friendly_name(video_path)

            if action == "yes":
                pos = queue_position()
                tg_answer_callback(cb_id, f"⬇️ In coda{f' (pos. {pos+1})' if pos > 0 else ''}...")
                if msg_id:
                    tg_edit_message(msg_id, f"⬇️ Scaricando sub ITA per:\n<b>{name}</b>..."
                                   + (f"\n⏳ In coda (posizione {pos+1})" if pos > 0 else ""))
                download_queue.put({"type": "single", "path": video_path, "msg_id": msg_id})

            elif action == "no":
                tg_answer_callback(cb_id, "⏭ Saltato")
                state["asked"][video_path] = {
                    "time": datetime.now().isoformat(),
                    "status": "no",
                    "path_hash": path_hash,
                }
                save_state(state)
                if msg_id:
                    tg_edit_message(msg_id, f"⏭ Saltato:\n<b>{name}</b>\nRichiederò tra 3 giorni.")

            elif action == "exclude":
                folder = get_series_folder(video_path)
                excludes.add(folder)
                save_excludes(excludes)
                tg_answer_callback(cb_id, f"🚫 '{folder}' esclusa")

                # Remove all pending asks for this folder
                to_remove = [p for p in state["asked"] if get_series_folder(p).lower() == folder.lower()]
                for p in to_remove:
                    del state["asked"][p]
                save_state(state)

                if msg_id:
                    tg_edit_message(msg_id, f"🚫 <b>{folder}</b> esclusa dalla ricerca sub ITA.\nPer rimuoverla, modifica /config/exclude_folders.txt")

        # Handle text commands
        msg = update.get("message")
        if msg and msg.get("text"):
            text = msg["text"].strip().lower()
            chat_id = str(msg["chat"]["id"])

            if chat_id != str(TELEGRAM_CHAT_ID):
                continue

            if text == "/status":
                fresh = load_state()
                pending = sum(1 for v in fresh["asked"].values() if v["status"] == "pending")
                downloaded = len(fresh["downloaded"])
                failed = sum(1 for v in fresh["asked"].values() if v["status"] == "failed")
                excluded = len(excludes)
                tg_send(
                    f"📊 <b>Status</b>\n\n"
                    f"⏳ In attesa di risposta: {pending}\n"
                    f"✅ Scaricati: {downloaded}\n"
                    f"❌ Non trovati: {failed}\n"
                    f"🚫 Cartelle escluse: {excluded}\n"
                    f"   ({', '.join(sorted(excludes)) if excludes else 'nessuna'})"
                )

            elif text == "/scan":
                tg_send("🔍 Avvio scansione manuale...")
                missing = scan_missing(state, excludes)
                tg_send(f"🔍 Trovati {len(missing)} video senza sub ITA")

            elif text == "/costs":
                costs = load_state().get("claude_costs", {})
                total_cost = costs.get("total_cost_usd", 0.0)
                translations = costs.get("translations", 0)
                input_t = costs.get("total_input_tokens", 0)
                output_t = costs.get("total_output_tokens", 0)
                tg_send(
                    f"💰 <b>Costi Claude API</b>\n\n"
                    f"🔄 Traduzioni effettuate: {translations}\n"
                    f"📊 Token usati: {input_t:,} in + {output_t:,} out\n"
                    f"💵 Costo totale: <b>${total_cost:.4f}</b>\n"
                    f"📈 Media per traduzione: ${total_cost / max(translations, 1):.4f}"
                )

            elif text == "/help":
                tg_send(
                    f"🤖 <b>Sub ITA Fetcher</b>\n\n"
                    f"Comandi:\n"
                    f"/status — Stato attuale\n"
                    f"/scan — Forza scansione\n"
                    f"/costs — Costi traduzioni Claude\n"
                    f"/sync — Riallinea tutti i sub ITA ai video\n"
                    f"/translate &lt;nome&gt; — Sync .en.srt e chiedi se tradurre\n"
                    f"/cleanup — Trova e rimuovi sub placeholder/VIP\n"
                    f"/excludes — Lista cartelle escluse\n"
                    f"/reset — Resetta cache (riparte da zero)\n"
                    f"/help — Questo messaggio\n\n"
                    f"<b>Ricerca manuale:</b>\n"
                    f"Scrivi il nome di un film o serie (es. 'Birdman' o 'The Chosen') "
                    f"e cercherò nella libreria i file senza sub ITA."
                )

            elif text == "/excludes":
                if excludes:
                    lines = "\n".join(f"  • {e}" for e in sorted(excludes))
                    tg_send(f"🚫 <b>Cartelle escluse:</b>\n{lines}")
                else:
                    tg_send("🚫 Nessuna cartella esclusa")

            elif text == "/reset":
                state["asked"] = {}
                state["downloaded"] = {}
                save_state(state)
                tg_send("🔄 Cache resettata. La prossima scansione ripartirà da zero.")

            elif text.startswith("/sync"):
                query = text[5:].strip() if len(text) > 5 else ""
                if not query:
                    tg_send("Usa: <code>/sync nome serie o film</code>\nEs: <code>/sync Pluribus</code> o <code>/sync The Chosen</code>\nOppure <code>/sync all</code> per tutti.")
                else:
                    pos = queue_position()
                    result = tg_send(
                        f"🔄 Sync <b>{query}</b> in coda..."
                        + (f"\n⏳ Posizione {pos + 1}" if pos > 0 else "")
                    )
                    msg_id = result["result"]["message_id"] if result and result.get("ok") else None
                    download_queue.put({"type": "sync", "query": query, "msg_id": msg_id})

            elif text.startswith("/translate"):
                query = text[len("/translate"):].strip()
                if not query:
                    tg_send(
                        "Usa: <code>/translate nome film o serie</code>\n"
                        "Sincronizza i .en.srt trovati all'audio e poi chiede se tradurre in italiano."
                    )
                else:
                    pos = queue_position()
                    result = tg_send(
                        f"🔄 Translate prep <b>{query}</b> in coda..."
                        + (f"\n⏳ Posizione {pos + 1}" if pos > 0 else "")
                    )
                    msg_id = result["result"]["message_id"] if result and result.get("ok") else None
                    download_queue.put({"type": "translate_prep", "query": query, "msg_id": msg_id})

            elif text == "/cleanup":
                pos = queue_position()
                result = tg_send(
                    f"🧹 Cleanup in coda..."
                    + (f"\n⏳ Posizione {pos + 1}" if pos > 0 else "")
                )
                msg_id = result["result"]["message_id"] if result and result.get("ok") else None
                download_queue.put({"type": "cleanup", "msg_id": msg_id})

            elif text.startswith("/sub ") or (not text.startswith("/") and len(text) >= 3):
                # Manual search: user typed a movie/series name
                query = text[5:].strip() if text.startswith("/sub ") else text.strip()
                if query:
                    search_and_offer(query, state)

    save_state(state)


# =============================================================================
# MANUAL SEARCH BY NAME
# =============================================================================

def find_videos_by_name(query):
    """Search media folders for videos matching a query string."""
    query_lower = query.lower()
    matches = []

    for media_path in [FILMS_PATH, SERIES_PATH]:
        if not os.path.exists(media_path):
            continue
        for root, dirs, files in os.walk(media_path):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in VIDEO_EXTENSIONS:
                    continue
                full_path = os.path.join(root, fname)

                # Match against filename, folder name, and parent folder
                folder_name = get_series_folder(full_path).lower()
                fname_lower = fname.lower()

                # Also match with dots/underscores replaced by spaces
                fname_clean = re.sub(r"[._\-]", " ", fname_lower)
                folder_clean = re.sub(r"[._\-]", " ", folder_name)
                query_clean = re.sub(r"[._\-]", " ", query_lower)

                if (query_lower in folder_name or query_lower in fname_lower
                    or query_clean in folder_clean or query_clean in fname_clean):
                    matches.append(full_path)

    return matches


def search_and_offer(query, state):
    """Find videos matching query and offer to download subs."""
    tg_send(f"🔍 Cerco '<b>{query}</b>' nella libreria...")

    matches = find_videos_by_name(query)

    if not matches:
        tg_send(f"❌ Nessun video trovato per '<b>{query}</b>'")
        return

    # Separate into: already has sub, missing sub
    with_sub = []
    without_sub = []
    for m in matches:
        if has_italian_sub(m):
            with_sub.append(m)
        else:
            without_sub.append(m)

    if not without_sub:
        tg_send(
            f"✅ Trovati {len(matches)} video per '<b>{query}</b>'\n"
            f"Tutti hanno già il sub ITA!"
        )
        return

    # Report what we found
    tg_send(
        f"📂 Trovati {len(matches)} video per '<b>{query}</b>'\n"
        f"  ✅ Con sub ITA: {len(with_sub)}\n"
        f"  ❌ Senza sub ITA: {len(without_sub)}"
    )

    # If there are many, offer to download all at once
    if len(without_sub) > 1:
        # Create a unique hash for this batch
        batch_hash = str(abs(hash(query + str(len(without_sub)))))[:8]

        batches = load_batches()
        batches[batch_hash] = {
            "paths": without_sub,
            "query": query,
            "time": datetime.now().isoformat(),
        }
        save_batches(batches)

        keyboard = {
            "inline_keyboard": [
                [
                    {"text": f"✅ Scarica tutti ({len(without_sub)})", "callback_data": f"batch_yes:{batch_hash}"},
                    {"text": "❌ No", "callback_data": f"batch_no:{batch_hash}"},
                ],
            ]
        }

        # Show list of files
        file_list = "\n".join(
            f"  • {friendly_name(p)}" for p in without_sub[:15]
        )
        if len(without_sub) > 15:
            file_list += f"\n  ... e altri {len(without_sub) - 15}"

        tg_send(
            f"📋 <b>File senza sub ITA:</b>\n{file_list}\n\n"
            f"Scarico i sottotitoli italiani per tutti?",
            reply_markup=keyboard,
        )
    else:
        # Just one file, ask directly
        video_path = without_sub[0]
        path_hash = str(abs(hash(video_path)))[:8]

        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "✅ Scarica", "callback_data": f"yes:{path_hash}"},
                    {"text": "❌ No", "callback_data": f"no:{path_hash}"},
                ],
            ]
        }

        result = tg_send(
            f"🎬 <b>Sub ITA mancante</b>\n\n"
            f"📄 {friendly_name(video_path)}\n"
            f"📁 {os.path.basename(video_path)}\n\n"
            f"Scarico il sottotitolo italiano?",
            reply_markup=keyboard,
        )

        msg_id = None
        if result and result.get("ok"):
            msg_id = result["result"]["message_id"]

        state["asked"][video_path] = {
            "time": datetime.now().isoformat(),
            "status": "pending",
            "path_hash": path_hash,
            "msg_id": msg_id,
        }
        save_state(state)


def do_sync(query, state, progress_msg_id=None):
    """Sync subtitles for a specific series/film or all."""
    is_all = query.lower() == "all"
    query_lower = query.lower()

    pairs = []
    for media_path in [SERIES_PATH, FILMS_PATH]:
        if not os.path.exists(media_path):
            continue
        for root, dirs, files in os.walk(media_path):
            for f in files:
                if not f.endswith(".it.srt"):
                    continue
                srt_path = os.path.join(root, f)
                folder = get_series_folder(srt_path)

                if not is_all and query_lower not in folder.lower() and query_lower not in f.lower():
                    continue

                video_base = srt_path.rsplit(".it.srt", 1)[0]
                for ext in VIDEO_EXTENSIONS:
                    candidate = video_base + ext
                    if os.path.exists(candidate):
                        pairs.append((candidate, srt_path))
                        break

    if not pairs:
        msg = f"❌ Nessun sub ITA trovato per '<b>{query}</b>'"
        if progress_msg_id:
            tg_edit_message(progress_msg_id, msg)
        else:
            tg_send(msg)
        return

    msg_id = progress_msg_id
    if not msg_id:
        result = tg_send(
            f"🔄 Sync <b>{query}</b>: {len(pairs)} sottotitoli...\n"
            f"[{'░' * 10}] 0%"
        )
        msg_id = result["result"]["message_id"] if result and result.get("ok") else None
    else:
        tg_edit_message(msg_id,
            f"🔄 Sync <b>{query}</b>: {len(pairs)} sottotitoli...\n"
            f"[{'░' * 10}] 0%")

    synced = 0
    failed = 0
    for i, (video_path, srt_path) in enumerate(pairs):
        if msg_id and (i % 3 == 0 or i == len(pairs) - 1):
            bar = _progress_bar(i, len(pairs))
            tg_edit_message(msg_id,
                f"🔄 Sync <b>{query}</b>...\n\n"
                f"{bar}\n"
                f"📊 {i}/{len(pairs)} — ✅ {synced} | ❌ {failed}")
        result = sync_subtitle(video_path, srt_path)
        if result and (result is True or (isinstance(result, dict) and result["ok"])):
            synced += 1
        else:
            failed += 1

    summary = (
        f"🔄 <b>Sync completato — {query}</b>\n\n"
        f"✅ Sincronizzati: {synced}\n"
        f"❌ Falliti: {failed}"
    )
    if msg_id:
        tg_edit_message(msg_id, summary)
    else:
        tg_send(summary)


def do_cleanup(state, progress_msg_id=None):
    """Scan all .it.srt files, remove placeholders and re-queue the matching videos."""
    removed = 0
    requeued = 0
    for media_path in [FILMS_PATH, SERIES_PATH]:
        if not os.path.exists(media_path):
            continue
        for root, dirs, files in os.walk(media_path):
            for f in files:
                if f.endswith(".it.srt"):
                    srt_path = os.path.join(root, f)
                    try:
                        with open(srt_path, "rb") as fh:
                            content = fh.read()
                        if is_placeholder_sub(content):
                            os.remove(srt_path)
                            removed += 1
                            log.info(f"  🗑 Placeholder rimosso: {f}")
                            video_base = srt_path.rsplit(".it.srt", 1)[0]
                            for ext in VIDEO_EXTENSIONS:
                                video_candidate = video_base + ext
                                if os.path.exists(video_candidate):
                                    state["downloaded"].pop(video_candidate, None)
                                    state["asked"].pop(video_candidate, None)
                                    requeued += 1
                                    break
                    except Exception as e:
                        log.error(f"  Errore leggendo {srt_path}: {e}")
    save_state(state)
    summary = (
        f"🧹 <b>Cleanup completato</b>\n\n"
        f"🗑 Placeholder rimossi: {removed}\n"
        f"🔄 Video rimessi in coda: {requeued}\n\n"
        f"{'Alla prossima scansione verranno cercati di nuovo.' if requeued else 'Nessun placeholder trovato.'}"
    )
    if progress_msg_id:
        tg_edit_message(progress_msg_id, summary)
    else:
        tg_send(summary)


def do_batch_download(paths, state, progress_msg_id=None):
    """Download subs for a batch of files with progress on a single Telegram message.
    Step 1: Download EN subs (free). Step 2: Ask user to confirm translation (paid)."""
    total = len(paths)
    ita_found = 0
    en_found = 0
    not_found = 0
    ita_names = []
    en_paths = []
    en_names = []
    failed_names = []

    for i, video_path in enumerate(paths):
        if has_italian_sub(video_path):
            ita_found += 1
            ita_names.append(friendly_name(video_path))
            continue

        if progress_msg_id and (i % 3 == 0 or i == total - 1):
            bar = _progress_bar(i, total)
            tg_edit_message(progress_msg_id,
                f"⬇️ <b>Scaricando sottotitoli...</b>\n\n"
                f"{bar}\n"
                f"📊 {i}/{total} — 🇮🇹 {ita_found} | 🇬🇧 {en_found} | ❌ {not_found}")

        result = do_download(video_path, state, silent=True, translate=False)
        if result is True:
            ita_found += 1
            ita_names.append(friendly_name(video_path))
        elif result == "en_only":
            en_found += 1
            en_paths.append(video_path)
            en_names.append(friendly_name(video_path))
        else:
            not_found += 1
            failed_names.append(friendly_name(video_path))
        time.sleep(1)

    # Build summary
    summary = f"📊 <b>Download completato</b>\n\n"
    summary += f"🇮🇹 Sub ITA trovati: {ita_found}/{total}\n"
    summary += f"🇬🇧 Solo ENG (da tradurre): {en_found}/{total}\n"
    summary += f"❌ Non trovati: {not_found}/{total}"

    if ita_names and len(ita_names) <= 10:
        summary += "\n\n<b>Sub ITA:</b>\n" + "\n".join(f"  🇮🇹 {n}" for n in ita_names)
    if en_names and len(en_names) <= 15:
        summary += "\n\n<b>Solo ENG (servono traduzione):</b>\n" + "\n".join(f"  🇬🇧 {n}" for n in en_names)
    if failed_names and len(failed_names) <= 10:
        summary += "\n\n<b>Non trovati:</b>\n" + "\n".join(f"  ❌ {n}" for n in failed_names)

    if en_paths:
        total_cost, total_blocks = _estimate_batch_translation_cost(en_paths)
        summary += f"\n\n💰 <b>Costo traduzione stimato: ${total_cost:.2f}</b> ({total_blocks} blocchi)"

        translate_hash = str(abs(hash(tuple(en_paths))))[:8]
        batches = load_batches()
        batches[translate_hash] = {
            "paths": en_paths,
            "type": "translate",
        }
        save_batches(batches)

        keyboard = {"inline_keyboard": [
            [
                {"text": f"🤖 Traduci in italiano (${total_cost:.2f})", "callback_data": f"batch_translate:{translate_hash}"},
                {"text": "🇬🇧 Tieni solo ENG", "callback_data": f"batch_keep_en:{translate_hash}"},
            ]
        ]}
        if progress_msg_id:
            tg_edit_message(progress_msg_id, summary, reply_markup=keyboard)
        else:
            tg_send(summary, reply_markup=keyboard)
    else:
        if progress_msg_id:
            tg_edit_message(progress_msg_id, summary)
        else:
            tg_send(summary)


def do_translate_prep(query, state, progress_msg_id=None):
    """For videos matching `query`, sync their .en.srt to audio, then ask the
    user (via Telegram) whether to translate them to Italian.

    Flow:
      1. Find videos matching the query
      2. Keep only those with a .en.srt but no .it.srt
      3. Sync each .en.srt to the video audio (ffsubsync)
      4. Estimate total cost and post a single message with Traduci/Tieni ENG buttons
    """
    matches = find_videos_by_name(query)
    if not matches:
        msg = f"❌ Nessun video trovato per '<b>{query}</b>'"
        if progress_msg_id:
            tg_edit_message(progress_msg_id, msg)
        else:
            tg_send(msg)
        return

    eligible = []
    already_it = []
    no_en = []
    for video_path in matches:
        if has_italian_sub(video_path):
            already_it.append(video_path)
            continue
        en_srt = find_english_sub(video_path)
        if not en_srt:
            no_en.append(video_path)
            continue
        eligible.append((video_path, en_srt))

    if not eligible:
        summary = f"📊 <b>Translate prep '{query}'</b>\n\n"
        summary += f"🇮🇹 Già con sub ITA: {len(already_it)}\n"
        summary += f"🇬🇧 Senza .en.srt: {len(no_en)}\n\n"
        summary += "Niente da tradurre. Usa prima la ricerca per scaricare il sub EN."
        if progress_msg_id:
            tg_edit_message(progress_msg_id, summary)
        else:
            tg_send(summary)
        return

    total = len(eligible)
    synced = 0
    for i, (video_path, en_srt) in enumerate(eligible):
        if progress_msg_id:
            bar = _progress_bar(i, total)
            tg_edit_message(
                progress_msg_id,
                f"🔄 <b>Sync EN su audio...</b>\n\n{bar}\n📊 {i}/{total} sincronizzati\n\n"
                f"<i>{friendly_name(video_path)}</i>",
            )
        try:
            sync_subtitle(video_path, en_srt)
            synced += 1
        except Exception as e:
            log.warning(f"  translate_prep sync failed for {video_path}: {e}")

    en_paths = [vp for vp, _ in eligible]
    total_cost, total_blocks = _estimate_batch_translation_cost(en_paths)

    summary = f"📊 <b>Translate prep '{query}'</b>\n\n"
    summary += f"🔄 Sincronizzati: {synced}/{total}\n"
    if already_it:
        summary += f"🇮🇹 Già con sub ITA (saltati): {len(already_it)}\n"
    if no_en:
        summary += f"🇬🇧 Senza .en.srt (saltati): {len(no_en)}\n"
    summary += f"\n💰 <b>Costo traduzione stimato: ${total_cost:.2f}</b> ({total_blocks} blocchi)"

    translate_hash = str(abs(hash(tuple(en_paths))))[:8]
    batches = load_batches()
    batches[translate_hash] = {"paths": en_paths, "type": "translate"}
    save_batches(batches)

    keyboard = {"inline_keyboard": [[
        {"text": f"🤖 Traduci in italiano (${total_cost:.2f})", "callback_data": f"batch_translate:{translate_hash}"},
        {"text": "🇬🇧 Tieni solo ENG", "callback_data": f"batch_keep_en:{translate_hash}"},
    ]]}
    if progress_msg_id:
        tg_edit_message(progress_msg_id, summary, reply_markup=keyboard)
    else:
        tg_send(summary, reply_markup=keyboard)


def _estimate_batch_translation_cost(paths):
    """Estimate total translation cost for a list of video paths with English sub files.
    Accepts .en.srt, .eng.srt, .english.srt."""
    total_cost = 0.0
    total_blocks = 0
    for video_path in paths:
        en_srt = find_english_sub(video_path)
        if not en_srt:
            continue
        try:
            with open(en_srt, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            cost, blocks = estimate_translation_cost(content)
            total_cost += cost
            total_blocks += blocks
        except Exception:
            pass
    return total_cost, total_blocks


def do_batch_translate(paths, state, progress_msg_id=None):
    """Translate a batch of EN subs to IT. Called after user confirms translation."""
    total = len(paths)
    success = 0
    failed = 0
    success_names = []
    failed_names = []

    for i, video_path in enumerate(paths):
        if has_italian_sub(video_path):
            success += 1
            success_names.append(friendly_name(video_path))
            continue

        en_srt = find_english_sub(video_path)
        if not en_srt:
            failed += 1
            failed_names.append(friendly_name(video_path))
            continue

        if progress_msg_id and (i % 2 == 0 or i == total - 1):
            bar = _progress_bar(i, total)
            tg_edit_message(progress_msg_id,
                f"🤖 <b>Traducendo EN→IT...</b>\n\n"
                f"{bar}\n"
                f"📊 {i}/{total} — ✅ {success} | ❌ {failed}")

        if _translate_and_save(en_srt, video_path, state, silent=True, skip_sync=True):
            success += 1
            success_names.append(friendly_name(video_path))
        else:
            failed += 1
            failed_names.append(friendly_name(video_path))
        time.sleep(1)

    actual_state = load_state()
    costs = actual_state.get("claude_costs", {})
    total_spent = costs.get("total_cost_usd", 0.0)

    summary = f"🤖 <b>Traduzione completata</b>\n\n"
    summary += f"✅ Tradotti: {success}/{total}\n❌ Falliti: {failed}/{total}\n"
    summary += f"💰 Costo totale sessione: ${total_spent:.4f}"

    if success_names and len(success_names) <= 10:
        summary += "\n\n<b>Tradotti:</b>\n" + "\n".join(f"  ✅ {n}" for n in success_names)
    if failed_names and len(failed_names) <= 10:
        summary += "\n\n<b>Falliti:</b>\n" + "\n".join(f"  ❌ {n}" for n in failed_names)

    if progress_msg_id:
        tg_edit_message(progress_msg_id, summary)
    else:
        tg_send(summary)


def _progress_bar(current, total, width=10):
    """Generate a text progress bar."""
    if total == 0:
        return ""
    filled = int(width * current / total)
    bar = "▓" * filled + "░" * (width - filled)
    pct = int(100 * current / total)
    return f"[{bar}] {pct}%"


# =============================================================================
# MAIN LOOP
# =============================================================================

# =============================================================================
# DOWNLOAD QUEUE
# =============================================================================

download_queue = Queue()


def _queue_worker(state_ref):
    """Background worker that processes download requests sequentially."""
    while True:
        try:
            job = download_queue.get(timeout=5)
        except Empty:
            continue
        try:
            job_type = job.get("type", "single")
            state = load_state()
            if job_type == "batch":
                paths = job["paths"]
                msg_id = job.get("msg_id")
                log.info(f"  Queue: processing batch of {len(paths)} files")
                do_batch_download(paths, state, progress_msg_id=msg_id)
            elif job_type == "translate":
                paths = job["paths"]
                msg_id = job.get("msg_id")
                log.info(f"  Queue: translating batch of {len(paths)} files")
                do_batch_translate(paths, state, progress_msg_id=msg_id)
            elif job_type == "sync":
                query = job["query"]
                msg_id = job.get("msg_id")
                log.info(f"  Queue: sync '{query}'")
                do_sync(query, state, progress_msg_id=msg_id)
            elif job_type == "translate_prep":
                query = job["query"]
                msg_id = job.get("msg_id")
                log.info(f"  Queue: translate_prep '{query}'")
                do_translate_prep(query, state, progress_msg_id=msg_id)
            elif job_type == "cleanup":
                msg_id = job.get("msg_id")
                log.info("  Queue: cleanup placeholders")
                do_cleanup(state, progress_msg_id=msg_id)
            else:
                video_path = job["path"]
                msg_id = job.get("msg_id")
                name = friendly_name(video_path)
                log.info(f"  Queue: processing {os.path.basename(video_path)}")
                trace = []
                result = do_download(video_path, state, silent=True, translate=False, trace=trace)
                if result is True and msg_id:
                    tg_edit_message(msg_id, f"✅ Sub ITA scaricato per:\n<b>{name}</b>")
                elif result == "en_only" and msg_id:
                    en_srt = find_english_sub(video_path)
                    cost, blocks = (0, 0)
                    if en_srt:
                        with open(en_srt, "r", encoding="utf-8", errors="ignore") as f:
                            cost, blocks = estimate_translation_cost(f.read())
                    tr_hash = str(abs(hash(video_path)))[:8]
                    batches = load_batches()
                    batches[tr_hash] = {"paths": [video_path], "type": "translate"}
                    save_batches(batches)
                    keyboard = {"inline_keyboard": [
                        [
                            {"text": f"🤖 Traduci (${cost:.2f})", "callback_data": f"batch_translate:{tr_hash}"},
                            {"text": "🇬🇧 Tieni ENG", "callback_data": f"batch_keep_en:{tr_hash}"},
                        ]
                    ]}
                    tg_edit_message(msg_id,
                        f"🇬🇧 Sub ENG trovato per:\n<b>{name}</b>\n\n"
                        f"💰 Tradurre in italiano? Costo: <b>${cost:.2f}</b> ({blocks} blocchi)",
                        reply_markup=keyboard)
                elif not result and msg_id:
                    trace_text = format_search_trace(trace)
                    fail_msg = f"❌ Nessun sub ITA né ENG trovato per:\n<b>{name}</b>\nRiproverò tra 24h."
                    if trace_text:
                        fail_msg += f"\n\n<b>Tentativi:</b>\n{trace_text}"
                    tg_edit_message(msg_id, fail_msg)
        except Exception as e:
            log.error(f"  Queue worker error: {e}", exc_info=True)
        finally:
            download_queue.task_done()


def queue_position():
    """Return current queue size."""
    return download_queue.qsize()


def main():
    log.info("=== Sub ITA Fetcher starting ===")
    log.info(f"Series path: {SERIES_PATH}")
    log.info(f"Films path: {FILMS_PATH}")
    log.info(f"Scan interval: {SCAN_INTERVAL}s")

    state = load_state()
    excludes = load_excludes()

    # Start background download worker
    worker = Thread(target=_queue_worker, args=(state,), daemon=True)
    worker.start()

    # Send startup message
    tg_send("🚀 <b>Sub ITA Fetcher avviato!</b>\nDigita /help per i comandi.")

    while True:
        try:
            # Process any pending Telegram callbacks/commands
            process_callbacks(state, excludes)

            # Scan for missing subs
            excludes = load_excludes()  # Reload in case user edited file
            missing = scan_missing(state, excludes)

            if missing:
                log.info(f"Found {len(missing)} videos missing Italian subs")
                ask_user_grouped(missing, state)

            # Wait, but check for callbacks more frequently
            for _ in range(SCAN_INTERVAL // 5):
                time.sleep(5)
                process_callbacks(state, excludes)

        except KeyboardInterrupt:
            log.info("Shutting down...")
            tg_send("🛑 Sub ITA Fetcher spento.")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(30)


if __name__ == "__main__":
    main()
