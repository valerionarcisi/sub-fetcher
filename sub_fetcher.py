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

# =============================================================================
# CONFIGURATION
# =============================================================================

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# OpenSubtitles.org credentials (optional, anonymous works but with lower limits)
OS_USERNAME = os.environ.get("OS_USERNAME", "")
OS_PASSWORD = os.environ.get("OS_PASSWORD", "")
OS_LANGUAGE = "ita"
OS_USER_AGENT = "NASSubFetcher v1.0"

# Subdl.com API (primary subtitle provider)
SUBDL_API_KEY = os.environ.get("SUBDL_API_KEY", "")
SUBDL_API_URL = "https://api.subdl.com/api/v1/subtitles"
SUBDL_DOWNLOAD_URL = "https://dl.subdl.com/subtitle"

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
LOG_FILE = "/config/sub_fetcher.log"

# Timing
SCAN_INTERVAL = 300  # 5 minutes
DELAY_BETWEEN_API_CALLS = 2  # seconds
RETRY_AFTER_HOURS = 72  # don't re-ask for 3 days after "no"
FAILED_RETRY_HOURS = 24  # retry failed downloads after 24h

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


def tg_edit_message(message_id, text):
    return tg_request("editMessageText", {
        "chat_id": TELEGRAM_CHAT_ID,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    })


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

def parse_video(filepath):
    fname = os.path.basename(filepath)
    name_no_ext = os.path.splitext(fname)[0]

    # Series: S01E01
    m = re.search(r"(.+?)[.\s_-]+[Ss](\d{1,2})[Ee](\d{1,2})", name_no_ext)
    if m:
        show = m.group(1).replace(".", " ").replace("_", " ").strip()
        return {"type": "episode", "name": show, "season": int(m.group(2)), "episode": int(m.group(3))}

    # Movie: Name.Year
    m = re.search(r"(.+?)[.\s_-]+(\d{4})[.\s_-]", name_no_ext)
    if m:
        return {"type": "movie", "name": m.group(1).replace(".", " ").replace("_", " ").strip(), "year": int(m.group(2))}

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


def find_imdb_id(video_path):
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


def find_existing_srt(video_path):
    """Check for existing subtitle files in the video's directory."""
    base = os.path.splitext(video_path)[0]
    video_dir = os.path.dirname(video_path)
    video_base_name = os.path.splitext(os.path.basename(video_path))[0].lower()

    # Check for English subtitles
    for suffix in [".en.srt", ".eng.srt", ".english.srt"]:
        path = base + suffix
        if os.path.exists(path):
            return {"lang": "en", "path": path}

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

class OSClient:
    API_URL = "https://api.opensubtitles.org/xml-rpc"

    def __init__(self):
        self.server = ServerProxy(self.API_URL)
        self.token = None

    def login(self):
        try:
            r = self.server.LogIn(OS_USERNAME, OS_PASSWORD, OS_LANGUAGE, OS_USER_AGENT)
            if r.get("status", "").startswith("200"):
                self.token = r["token"]
                log.info("OpenSubtitles.org: logged in")
                return True
            log.error(f"OS login failed: {r.get('status')}")
            return False
        except Exception as e:
            log.error(f"OS login error: {e}")
            return False

    def logout(self):
        if self.token:
            try:
                self.server.LogOut(self.token)
            except Exception:
                pass
            self.token = None

    def search_hash(self, file_hash, file_size):
        try:
            r = self.server.SearchSubtitles(self.token, [{
                "sublanguageid": OS_LANGUAGE,
                "moviehash": file_hash,
                "moviebytesize": str(file_size),
            }])
            if r.get("status", "").startswith("200"):
                return r.get("data", []) or []
        except Exception as e:
            log.error(f"OS hash search error: {e}")
        return []

    def search_imdb(self, imdb_id, season=None, episode=None, language=None):
        try:
            lang = language or OS_LANGUAGE
            # OpenSubtitles expects numeric IMDB ID (without "tt" prefix)
            imdb_num = imdb_id.lstrip("t")
            params = {"sublanguageid": lang, "imdbid": imdb_num}
            if season is not None:
                params["season"] = str(season)
            if episode is not None:
                params["episode"] = str(episode)
            r = self.server.SearchSubtitles(self.token, [params])
            if r.get("status", "").startswith("200"):
                return r.get("data", []) or []
        except Exception as e:
            log.error(f"OS IMDB search error: {e}")
        return []

    def search_name(self, query, season=None, episode=None, language=None):
        try:
            lang = language or OS_LANGUAGE
            params = {"sublanguageid": lang, "query": query}
            if season is not None:
                params["season"] = str(season)
            if episode is not None:
                params["episode"] = str(episode)
            r = self.server.SearchSubtitles(self.token, [params])
            if r.get("status", "").startswith("200"):
                return r.get("data", []) or []
        except Exception as e:
            log.error(f"OS name search error: {e}")
        return []

    def download(self, sub_id):
        try:
            r = self.server.DownloadSubtitles(self.token, [str(sub_id)])
            if r.get("status", "").startswith("200"):
                data = r.get("data", [])
                if data:
                    return gzip.decompress(base64.b64decode(data[0]["data"]))
        except Exception as e:
            log.error(f"OS download error: {e}")
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

    def search_and_download(self, video_path, language="it"):
        """Search and download best subtitle from Subdl. Returns content bytes or None."""
        parsed = parse_video(video_path)
        season = parsed.get("season") if parsed["type"] == "episode" else None
        episode = parsed.get("episode") if parsed["type"] == "episode" else None
        lang_label = language.upper()

        # 1. Search by IMDB ID
        imdb_id = find_imdb_id(video_path)
        if imdb_id:
            time.sleep(DELAY_BETWEEN_API_CALLS)
            results = self.search("", language=language, imdb_id=imdb_id, season=season, episode=episode)
            if results:
                content = self._try_download(results, video_path)
                if content:
                    return content

        # 2. Search by name cascade
        queries = get_search_queries(video_path)
        for query in queries:
            time.sleep(DELAY_BETWEEN_API_CALLS)
            results = self.search(query, language=language, season=season, episode=episode)
            if results:
                content = self._try_download(results, video_path)
                if content:
                    return content

        return None

    def _try_download(self, results, video_path, max_tries=3):
        """Try downloading from results, picking best match first."""
        video_base = os.path.splitext(os.path.basename(video_path))[0].lower()
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

        for i, (score, sub) in enumerate(scored[:max_tries]):
            time.sleep(DELAY_BETWEEN_API_CALLS)
            content = self.download(sub)
            if not content:
                continue
            if is_placeholder_sub(content):
                log.warning(f"  Subdl result {i+1} looks like placeholder, trying next")
                continue
            # Reject forced/incomplete subs (fewer than 10 dialogue blocks)
            block_count = len(re.findall(rb"\d+\r?\n\d{2}:\d{2}:\d{2}", content))
            if block_count < 10:
                log.warning(f"  Subdl result {i+1} has only {block_count} blocks (likely forced/signs-only), trying next")
                continue
            return content

        return None


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

def sync_subtitle(video_path, srt_path):
    """Sync subtitle to video audio using ffsubsync. Overwrites srt_path in place."""
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
        offset_match = _re.search(r"offset seconds:\s*([\d.-]+)", log_content)
        framerate_match = _re.search(r"framerate scale factor:\s*([\d.]+)", log_content)
        if os.path.exists(tmp_out) and os.path.getsize(tmp_out) > 0:
            shutil.move(tmp_out, srt_path)
            offset = offset_match.group(1) if offset_match else "?"
            fps = framerate_match.group(1) if framerate_match else "1.000"
            log.info(f"  🔄 Synced: {os.path.basename(srt_path)} (offset: {offset}s, fps_scale: {fps})")
            return True
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


# =============================================================================
# SUBTITLE VALIDATION (detect VIP placeholders)
# =============================================================================

# Patterns that indicate a fake/placeholder subtitle
PLACEHOLDER_PATTERNS = [
    b"opensubtitles",
    b"vip member",
    b"osdb.link",
    b"get subtitles",
    b"become a member",
    b"advertis",
]


def is_placeholder_sub(content):
    """Check if downloaded subtitle content is a VIP placeholder/ad."""
    try:
        # Try to decode as text
        text = content.decode("utf-8", errors="ignore").lower()
    except Exception:
        text = str(content).lower()

    # Check for known placeholder patterns
    for pattern in PLACEHOLDER_PATTERNS:
        if pattern.decode().lower() in text:
            return True

    # Count actual SRT blocks (number\ntimecode\ntext)
    blocks = re.findall(r"\d+\s*\n\d{2}:\d{2}:\d{2}", text)
    if len(blocks) < 3:
        return True  # A real subtitle has way more than 3 blocks

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
    Returns (estimated_cost_usd, num_blocks)."""
    blocks = parse_srt(srt_content)
    if not blocks:
        return 0.0, 0
    # Rough token estimate: ~1.3 tokens per word, subtitle text + prompt overhead
    total_text = " ".join(text for _, _, text in blocks if text.strip())
    word_count = len(total_text.split())
    input_tokens = int(word_count * 1.3) + (len(blocks) // 100 + 1) * 150  # prompt overhead per batch
    output_tokens = int(input_tokens * 1.1)  # output is ~same size (translation)
    cost = (input_tokens * CLAUDE_INPUT_PRICE + output_tokens * CLAUDE_OUTPUT_PRICE) / 1_000_000
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


def _cascade_search(client, video_path, language, file_hash=None, file_size=0):
    """Search OpenSubtitles using a cascade: hash -> IMDB ID -> name variants.
    Returns a list of results (may be empty)."""
    parsed = parse_video(video_path)
    season = parsed.get("season") if parsed["type"] == "episode" else None
    episode = parsed.get("episode") if parsed["type"] == "episode" else None
    lang_label = language.upper()

    # 1. Hash search
    if file_hash:
        time.sleep(DELAY_BETWEEN_API_CALLS)
        try:
            r = client.server.SearchSubtitles(client.token, [{
                "sublanguageid": language,
                "moviehash": file_hash,
                "moviebytesize": str(file_size),
            }])
            if r.get("status", "").startswith("200"):
                results = r.get("data", []) or []
                log.info(f"  {lang_label} hash search: {len(results)} results")
                if results:
                    return results
        except Exception as e:
            log.error(f"  {lang_label} hash search error: {e}")

    # 2. IMDB ID search
    imdb_id = find_imdb_id(video_path)
    if imdb_id:
        time.sleep(DELAY_BETWEEN_API_CALLS)
        results = client.search_imdb(imdb_id, season, episode, language=language)
        log.info(f"  {lang_label} IMDB search ({imdb_id}): {len(results)} results")
        if results:
            return results

    # 3. Name search cascade
    queries = get_search_queries(video_path)
    for query in queries:
        time.sleep(DELAY_BETWEEN_API_CALLS)
        results = client.search_name(query, season, episode, language=language)
        log.info(f"  {lang_label} name search \"{query}\": {len(results)} results")
        if results:
            return results

    return []


def _download_first_valid(client, results, video_path, max_tries=5):
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


def search_and_download_english(client, video_path, file_hash=None, file_size=0):
    """Search for English subtitles as fallback. Returns content bytes or None."""
    results = _cascade_search(client, video_path, "eng", file_hash, file_size)

    content, sub = _download_first_valid(client, results, video_path)
    if content:
        log.info(f"  ENG sub downloaded: {sub.get('SubFileName', '?')}")
        return content

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


def do_download(video_path, state, silent=False):
    """Search and download subtitle for a video file. Returns True on success.
    When silent=True, suppresses per-file Telegram messages (used in batch mode)."""
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
        elif existing["lang"] == "en" and CLAUDE_API_KEY:
            log.info(f"  Found existing English sub: {existing['path']}, translating...")
            if not silent:
                tg_send(f"📖 Sub ENG trovato nella cartella, traduco in ITA...\n<b>{friendly_name(video_path)}</b>")
            if _translate_and_save(existing["path"], video_path, state, silent=silent, skip_sync=True):
                return True

    # =================================================================
    # STEP 1: Subdl.com — search ITA (primary, no VIP placeholders)
    # =================================================================
    subdl = SubdlClient()
    ita_content = subdl.search_and_download(video_path, language="it")
    if ita_content:
        sub_path = os.path.splitext(video_path)[0] + ".it.srt"
        with open(sub_path, "wb") as f:
            f.write(ita_content)
        sync_subtitle(video_path, sub_path)
        _save_sub_and_update_state(video_path, sub_path, "Subdl.com", state)
        log.info(f"  ✅ Saved (Subdl): {os.path.basename(sub_path)}")
        if not silent:
            tg_send(f"✅ Sub ITA scaricato (Subdl):\n<b>{friendly_name(video_path)}</b>\n📁 {os.path.basename(sub_path)}")
        return True

    # =================================================================
    # STEP 2: OpenSubtitles — hash search ITA (fallback, best for exact match)
    # =================================================================
    client = OSClient()
    os_logged_in = client.login()

    if os_logged_in:
        try:
            file_hash, file_size = compute_hash(video_path)
            results = _cascade_search(client, video_path, "ita", file_hash, file_size)
            content, best = _download_first_valid(client, results, video_path)

            if content:
                sub_path = os.path.splitext(video_path)[0] + ".it.srt"
                with open(sub_path, "wb") as f:
                    f.write(content)
                sync_subtitle(video_path, sub_path)
                _save_sub_and_update_state(video_path, sub_path, f"OpenSubtitles: {best.get('SubFileName', '')}", state)
                log.info(f"  ✅ Saved (OS): {os.path.basename(sub_path)}")
                if not silent:
                    tg_send(f"✅ Sub ITA scaricato (OS):\n<b>{friendly_name(video_path)}</b>\n📁 {os.path.basename(sub_path)}")
                client.logout()
                return True
        except Exception as e:
            log.error(f"  OpenSubtitles error: {e}")

    log.warning(f"  No Italian subs found for: {fname}")

    # =================================================================
    # STEP 3: ENG fallback — Subdl → OS → translate with Claude
    # =================================================================
    if not CLAUDE_API_KEY:
        log.info("  No CLAUDE_API_KEY set, skipping EN->IT translation fallback")
        if os_logged_in:
            client.logout()
        state["asked"][video_path] = {"time": datetime.now().isoformat(), "status": "failed"}
        save_state(state)
        if not silent:
            tg_send(f"❌ Nessun sub ITA trovato per:\n<b>{friendly_name(video_path)}</b>\n(CLAUDE_API_KEY non configurata)")
        return False

    log.info(f"  Trying English fallback + Claude translation...")

    # Try Subdl ENG first
    eng_content = subdl.search_and_download(video_path, language="en")

    # Fallback: OpenSubtitles ENG
    if not eng_content and os_logged_in:
        file_hash, file_size = compute_hash(video_path)
        eng_content = search_and_download_english(client, video_path, file_hash, file_size)

    if not eng_content:
        log.warning(f"  No English subs found either for: {fname}")
        if os_logged_in:
            client.logout()
        state["asked"][video_path] = {"time": datetime.now().isoformat(), "status": "failed"}
        save_state(state)
        if not silent:
            tg_send(f"❌ Nessun sub ITA né ENG trovato per:\n<b>{friendly_name(video_path)}</b>")
        return False

    if os_logged_in:
        client.logout()

    # Sync the downloaded English sub to video audio BEFORE translating
    # so both .en.srt and .it.srt end up with correct timecodes
    en_srt_path = os.path.splitext(video_path)[0] + ".en.srt"
    try:
        if isinstance(eng_content, bytes):
            with open(en_srt_path, "wb") as f:
                f.write(eng_content)
        else:
            with open(en_srt_path, "w", encoding="utf-8") as f:
                f.write(eng_content)
        log.info(f"  Saved English sub: {os.path.basename(en_srt_path)}")
        sync_subtitle(video_path, en_srt_path)
    except Exception as e:
        log.warning(f"  Failed to save/sync English sub: {e}")

    # Translate from the synced .en.srt (skip_sync=True: timecodes already correct)
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

    state.setdefault("batches", {})[batch_hash] = {
        "paths": paths,
        "folder": folder,
        "time": datetime.now().isoformat(),
        "msg_id": msg_id,
    }

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

    if not singles:
        return

    # If there's only 1 single file, send individual message
    if len(singles) == 1:
        ask_user(singles[0], state)
        return

    # Multiple single files: group them into one digest message
    batch_hash = str(abs(hash("digest" + str(len(singles)) + singles[0])))[:8]

    file_list = "\n".join(
        f"  • {friendly_name(p)}" for p in singles[:20]
    )
    if len(singles) > 20:
        file_list += f"\n  ... e altri {len(singles) - 20}"

    keyboard = {
        "inline_keyboard": [
            [
                {"text": f"✅ Scarica tutti ({len(singles)})", "callback_data": f"batch_yes:{batch_hash}"},
                {"text": "❌ Salta tutti", "callback_data": f"batch_no:{batch_hash}"},
            ],
        ]
    }

    result = tg_send(
        f"🎬 <b>Sub ITA mancanti — {len(singles)} film</b>\n\n"
        f"{file_list}\n\n"
        f"Scarico i sottotitoli italiani?",
        reply_markup=keyboard,
    )

    msg_id = None
    if result and result.get("ok"):
        msg_id = result["result"]["message_id"]

    state.setdefault("batches", {})[batch_hash] = {
        "paths": singles,
        "folder": "film_digest",
        "time": datetime.now().isoformat(),
        "msg_id": msg_id,
    }

    for p in singles:
        path_hash = str(abs(hash(p)))[:8]
        state["asked"][p] = {
            "time": datetime.now().isoformat(),
            "status": "pending",
            "path_hash": path_hash,
            "batch": batch_hash,
        }

    save_state(state)


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
            if action in ("batch_yes", "batch_no", "grp_exclude"):
                batch = state.get("batches", {}).get(path_hash)
                if not batch:
                    tg_answer_callback(cb_id, "⚠️ Batch non trovato")
                    continue

                if action == "batch_yes":
                    tg_answer_callback(cb_id, "⬇️ Scarico...")
                    if msg_id:
                        tg_edit_message(msg_id,
                            f"⬇️ <b>Scaricando sottotitoli...</b>\n\n"
                            f"[░░░░░░░░░░] 0%\n"
                            f"📊 0/{len(batch['paths'])}")
                    do_batch_download(batch["paths"], state, progress_msg_id=msg_id)
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

                state.get("batches", {}).pop(path_hash, None)
                save_state(state)
                continue

            if not video_path:
                tg_answer_callback(cb_id, "⚠️ Non trovato")
                continue

            name = friendly_name(video_path)

            if action == "yes":
                tg_answer_callback(cb_id, "⬇️ Scarico...")
                if msg_id:
                    tg_edit_message(msg_id, f"⬇️ Scaricando sub ITA per:\n<b>{name}</b>...")
                success = do_download(video_path, state)
                if not success and msg_id:
                    tg_edit_message(msg_id, f"❌ Non trovato sub ITA per:\n<b>{name}</b>\nRiproverò tra 24h.")

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
                pending = sum(1 for v in state["asked"].values() if v["status"] == "pending")
                downloaded = len(state["downloaded"])
                failed = sum(1 for v in state["asked"].values() if v["status"] == "failed")
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
                costs = state.get("claude_costs", {})
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
                    do_sync(query, state)

            elif text == "/cleanup":
                tg_send("🧹 Scansione sottotitoli placeholder in corso...")
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
                                        # Find matching video and remove from downloaded state
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
                tg_send(
                    f"🧹 <b>Cleanup completato</b>\n\n"
                    f"🗑 Placeholder rimossi: {removed}\n"
                    f"🔄 Video rimessi in coda: {requeued}\n\n"
                    f"{'Alla prossima scansione verranno cercati di nuovo.' if requeued else 'Nessun placeholder trovato.'}"
                )

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

                if query_lower in folder_name or query_lower in fname_lower:
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

        # Store the batch in state
        state.setdefault("batches", {})[batch_hash] = {
            "paths": without_sub,
            "query": query,
            "time": datetime.now().isoformat(),
        }
        save_state(state)

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


def do_sync(query, state):
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
        tg_send(f"❌ Nessun sub ITA trovato per '<b>{query}</b>'")
        return

    result = tg_send(
        f"🔄 Sync <b>{query}</b>: {len(pairs)} sottotitoli...\n"
        f"[{'░' * 10}] 0%"
    )
    msg_id = result["result"]["message_id"] if result and result.get("ok") else None

    synced = 0
    failed = 0
    for i, (video_path, srt_path) in enumerate(pairs):
        if msg_id and (i % 3 == 0 or i == len(pairs) - 1):
            bar = _progress_bar(i, len(pairs))
            tg_edit_message(msg_id,
                f"🔄 Sync <b>{query}</b>...\n\n"
                f"{bar}\n"
                f"📊 {i}/{len(pairs)} — ✅ {synced} | ❌ {failed}")
        if sync_subtitle(video_path, srt_path):
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


def do_batch_download(paths, state, progress_msg_id=None):
    """Download subs for a batch of files with progress on a single Telegram message."""
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

        # Update progress on existing message
        if progress_msg_id and (i % 3 == 0 or i == total - 1):
            bar = _progress_bar(i, total)
            tg_edit_message(progress_msg_id,
                f"⬇️ <b>Scaricando sottotitoli...</b>\n\n"
                f"{bar}\n"
                f"📊 {i}/{total} — ✅ {success} | ❌ {failed}")

        result = do_download(video_path, state, silent=True)
        if result:
            success += 1
            success_names.append(friendly_name(video_path))
        else:
            failed += 1
            failed_names.append(friendly_name(video_path))
        time.sleep(1)

    # Final summary
    summary = f"📊 <b>Batch completato</b>\n\n✅ Scaricati: {success}/{total}\n❌ Non trovati: {failed}/{total}"
    if success_names and len(success_names) <= 10:
        summary += "\n\n<b>Scaricati:</b>\n" + "\n".join(f"  ✅ {n}" for n in success_names)
    if failed_names and len(failed_names) <= 10:
        summary += "\n\n<b>Non trovati:</b>\n" + "\n".join(f"  ❌ {n}" for n in failed_names)

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

def main():
    log.info("=== Sub ITA Fetcher starting ===")
    log.info(f"Series path: {SERIES_PATH}")
    log.info(f"Films path: {FILMS_PATH}")
    log.info(f"Scan interval: {SCAN_INTERVAL}s")

    state = load_state()
    excludes = load_excludes()

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
