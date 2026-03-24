# Sub ITA Fetcher - Technical Specification

## Full Flow Overview

```
VIDEO FILE DISCOVERED
        |
        v
    [SCAN PHASE]
        |-- Has .it.srt / .it.hi.srt already? → SKIP
        |-- Folder in exclude list? → SKIP
        |-- Italian audio track (ffprobe)? → SKIP (e.g. "La Battaglia di Algeri")
        |-- Already asked/downloaded/failed (within cooldown)? → SKIP
        |
        v
    [TELEGRAM NOTIFICATION]
        |-- Multiple episodes in same series → 1 grouped message
        |-- Multiple single films → 1 digest message
        |-- Single file alone → 1 individual message
        |
        v
    USER CLICKS "SCARICA"
        |
        v
    [STEP 0: CHECK LOCAL FILES]
        |-- Found .it.srt / .ita.srt in folder? → DONE
        |-- Found .en.srt / .eng.srt in folder? → Translate + Sync + DONE
        |-- Found generic .srt? → Detect language:
        |     |-- Italian → Copy as .it.srt → DONE
        |     |-- English → Translate + Sync + DONE
        |     |-- Unknown → Continue
        |
        v
    [STEP 1: SUBDL.COM — SEARCH ITA (primary, no VIP placeholders)]
        |-- Search by IMDB ID (from .nfo files)
        |-- Search by name cascade (filename → folder → cleaned folder)
        |-- Download from ZIP → Validate → Sync + DONE
        |
        v
    [STEP 2: OPENSUBTITLES — SEARCH ITA (fallback)]
        |-- Hash search (file hash + size)
        |-- IMDB ID search
        |-- Name search cascade
        |-- Try up to 5 results (skip VIP placeholders) → Sync + DONE
        |
        v
    [STEP 3: ENGLISH FALLBACK + CLAUDE TRANSLATION]
        |-- Subdl.com ENG search (IMDB → name)
        |-- OpenSubtitles ENG search (hash → IMDB → name, try up to 5)
        |-- Save .en.srt (keep English original)
        |-- Translate EN→IT with Claude API (batches of 100 blocks)
        |-- Save .it.srt + Sync with ffsubsync
        |-- Track cost in state.json → DONE
        |
        v
    FAIL: Mark as "failed", retry after 24h
```

## Subtitle Providers

### Subdl.com (Primary)
- **API**: REST `https://api.subdl.com/api/v1/subtitles`
- **Auth**: API key (free, via env `SUBDL_API_KEY`)
- **Search**: by film name, IMDB ID, TMDB ID, season/episode
- **Download**: ZIP files from `https://dl.subdl.com/subtitle/{id}.zip`, extracts first .srt
- **Advantages**: No VIP placeholders, real subtitles, 64+ languages
- **Rate limits**: Per API key

### OpenSubtitles.org (Fallback)
- **API**: XML-RPC `https://api.opensubtitles.org/xml-rpc`
- **Auth**: Username/password (optional, anonymous works with lower limits)
- **Search**: by file hash, IMDB ID, name + season/episode
- **Download**: gzip+base64 encoded via XML-RPC
- **Issue**: Free users get VIP placeholder subtitles (fake ads). Bot tries up to 5 results to find a real one.

## Key Components

### Italian Audio Detection (`has_italian_audio`)
Uses `ffprobe` to inspect audio stream metadata. Checks `language` tag for "ita"/"it"/"italian" and `title` tag for Italian keywords. Videos with Italian audio are skipped (no need for Italian subtitles).

### Subtitle Sync (`sync_subtitle`)
Uses `ffsubsync` to align subtitle timecodes to the video's audio track via Voice Activity Detection. Analyzes when speech occurs in the audio and aligns subtitle timecodes accordingly. Calculates both time offset (seconds) and framerate scale factor for different video releases. Uses `os.system()` shell execution (not `subprocess.run` with pipes, which interferes with ffsubsync's `rich` library). Non-blocking: if sync fails, the unsynchronized subtitle is kept. Timeout: 5 minutes.

**Sync strategy:**
- **Downloaded subs (Subdl/OS)**: Sync `.en.srt` to audio FIRST (with `min_score=1000` validation), then translate to `.it.srt` preserving synced timecodes.
- **Local `.en.srt` already present**: Skip sync (timecodes already match the video), translate directly.
- **Score validation**: ffsubsync returns a confidence score. Syncs with score < min_score are logged as warnings (sub may not match the video).
- The `/sync` command re-syncs existing `.it.srt` files on demand.

### Dual Subtitle Save
When translating English subtitles, both versions are saved:
- `video.en.srt` — original English
- `video.it.srt` — Italian translation by Claude

### VIP Placeholder Detection (`is_placeholder_sub`)
Rejects fake subtitles by checking for known ad patterns ("opensubtitles", "vip member", "osdb.link"), fewer than 3 blocks, or single blocks spanning >10 minutes. `_download_first_valid()` tries up to 5 results before giving up.

### Forced/Signs-Only Sub Rejection
Subtitles that only contain foreign language signs or forced dialogue (e.g. `eng-forced.srt`) are rejected:
- **Scoring penalty**: -200 points for subs with "forced", "signs", "songs" in release name
- **ZIP extraction**: Prefers non-forced `.srt` files within ZIP archives
- **Block count check**: Rejects downloaded subs with fewer than 10 dialogue blocks (forced subs typically have very few)

### IMDB ID Discovery (`find_imdb_id`)
Searches `.nfo` files (Sonarr/Radarr) in the video's directory and parent directory. Extracts IMDB ID via regex `tt\d{7,}`. Used by both Subdl and OpenSubtitles for more accurate search.

### Search Query Generation (`get_search_queries`)
Returns a deduplicated list of search terms:
1. Name from filename (e.g. "Pluribus")
2. Series folder name (e.g. "PLUR1BUS")
3. Cleaned folder name, alpha-only (e.g. "PLURIBUS")

### Claude API Cost Tracking
Each translation tracks `input_tokens`, `output_tokens`, and cost (USD) in `state.json`. Accessible via `/costs` Telegram command. Pre-translation estimate shown in logs.

## Telegram UX

### Notifications
- **Series with multiple episodes**: 1 grouped message per series with episode list + "Scarica tutti" button
- **Films**: 1 individual message per film with Scarica/No/Escludi buttons

### Batch Progress
Single message updated in-place with progress bar:
```
⬇️ Scaricando sottotitoli...
[▓▓▓▓░░░░░░] 40%
📊 4/10 — ✅ 3 | ❌ 1
```
Final summary with success/failure lists replaces progress message.

### Download Queue
All download requests (single films, batch series) go through a thread-safe FIFO queue processed by a background worker. This prevents concurrent downloads from interfering and allows multiple "Scarica" clicks without blocking the Telegram callback handler. Queue position is shown when multiple downloads are pending.

### Silent Mode
During batch downloads, per-file Telegram messages are suppressed. Only the final batch summary is shown.

## Telegram Bot Commands
| Command | Description |
|---|---|
| `/status` | Pending/downloaded/failed counts |
| `/scan` | Force manual scan |
| `/costs` | Claude API translation costs |
| `/cleanup` | Remove placeholder subtitles |
| `/excludes` | List excluded folders |
| `/reset` | Clear state cache |
| `/help` | Help |
| `/sync [name]` | Sync all/matching .it.srt to video audio |
| `/sub <name>` | Manual search by title |
| `<text>` | Search for videos matching text |

## Configuration (Environment Variables)
| Variable | Required | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot API token |
| `TELEGRAM_CHAT_ID` | Yes | Target chat ID |
| `SUBDL_API_KEY` | Yes | Subdl.com API key (free) |
| `CLAUDE_API_KEY` | No | Enables EN→IT translation fallback |
| `CLAUDE_MODEL` | No | Default: `claude-sonnet-4-20250514` |
| `OS_USERNAME` | No | OpenSubtitles.org username |
| `OS_PASSWORD` | No | OpenSubtitles.org password |

## Dependencies
| Dependency | Purpose | Install |
|---|---|---|
| Python 3.11 | Runtime | Docker base image |
| ffmpeg/ffprobe | Audio language detection | `apt-get install ffmpeg` |
| ffsubsync | Subtitle sync to audio | `pip install ffsubsync` |
| Subdl.com API | Primary subtitle provider | API key via env var |
| OpenSubtitles XML-RPC | Fallback subtitle provider | Built-in (stdlib xmlrpc) |
| Claude API | EN→IT translation | API key via env var |

## File Structure
```
/app/sub_fetcher.py          # Main application
/config/state.json           # Persistent state (incl. claude_costs)
/config/exclude_folders.txt  # Excluded folders list
/config/sub_fetcher.log      # Application log
/media/series/               # Series (read-only mount)
/media/films/                # Films (read-only mount)
```

## Testing
Run: `python3 test_sub_fetcher.py -v`

Test coverage (48 tests):
- `find_imdb_id`: NFO parsing
- `detect_language_from_srt`: Italian/English/unknown detection
- `find_existing_srt`: English, generic, missing, Italian-tagged SRT
- `get_search_queries`: Deduplication, folder vs filename extraction
- `parse_video`: Episode, movie, unknown (filename cleanup)
- `_cascade_search`: Mock-based cascade logic
- `group_by_series`: Grouping, sorting, single-file
- `_progress_bar`: Progress bar rendering
- `has_italian_audio`: Mock ffprobe with Italian/English/no-tags/missing
- `SubdlClient`: ZIP extraction, empty ZIP, lang map, missing API key
- `SubdlForcedFiltering`: Scoring penalty, block count rejection
- `SubdlZipPreferNonForced`: ZIP non-forced preference, forced fallback
- `SyncSkipLogic`: skip_sync parameter acceptance
- `SyncSubtitleReturn`: min_score parameter
- `DownloadQueue`: queue existence, position, put/get
- `AskUserGroupedFilmsSingle`: films get individual messages
