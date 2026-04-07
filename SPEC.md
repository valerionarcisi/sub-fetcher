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
        |-- Found .en.srt / .eng.srt in folder? → "en_only" (ask to translate)
        |-- Found generic .srt? → Detect language:
        |     |-- Italian → Copy as .it.srt → DONE
        |     |-- English → "en_only" (ask to translate)
        |     |-- Unknown → Continue
        |
        v
    [STEP 1: SUBDL.COM — SEARCH ITA (primary, no VIP placeholders)]
        |-- Search by IMDB ID (from .nfo files)
        |-- Search by name cascade (filename → folder → cleaned folder)
        |-- Episode matching: +500 for correct S01E01, -1000 for wrong episode
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
    [STEP 3: ENGLISH FALLBACK — DOWNLOAD ONLY (FREE)]
        |-- Subdl.com ENG search (IMDB → name)
        |-- OpenSubtitles ENG search (hash → IMDB → name, try up to 5)
        |-- Save .en.srt
        |-- Sync .en.srt to audio with ffsubsync
        |-- Return "en_only" result
        |
        v
    [TELEGRAM: ASK USER TO TRANSLATE]
        |-- Show cost estimate: "Tradurre in italiano? Costo: $X.XX"
        |-- User clicks "Traduci" → translate EN→IT (paid)
        |-- User clicks "Tieni ENG" → keep English only (free)
        |
        v (if user confirms translation)
    [STEP 4: CLAUDE TRANSLATION]
        |-- Read synced .en.srt
        |-- Translate EN→IT with Claude API (batches of 100 blocks)
        |-- Save .it.srt with SAME timecodes as synced .en.srt
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
- **Episode matching**: Scoring system ensures correct episode is selected (+500 match, -1000 mismatch)

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
- **Downloaded EN subs**: Sync `.en.srt` to audio FIRST (with `min_score=1000` validation), THEN translate to `.it.srt` preserving synced timecodes. The `.it.srt` is NEVER synced separately.
- **Local `.en.srt` already present**: Skip sync (timecodes already match the video), translate directly.
- **Downloaded ITA subs**: Sync `.it.srt` directly to audio (no English intermediate).
- **Score validation**: ffsubsync returns a confidence score. Syncs with score < min_score are logged as warnings.
- The `/sync` command re-syncs existing `.it.srt` files on demand.

### Two-Phase Download (Free + Paid)
Download and translation are separated into two phases:
1. **Phase 1 (Free)**: Search and download subtitles. ITA subs saved directly. EN subs saved as `.en.srt` and synced to audio.
2. **Phase 2 (Paid, user-confirmed)**: Translate EN→IT with Claude API. Cost estimate shown BEFORE user confirms. User can keep EN-only for free.

This prevents accidental spending on Claude API translations.

### Dual Subtitle Save
When translating English subtitles, both versions are saved:
- `video.en.srt` — original English (synced to audio)
- `video.it.srt` — Italian translation by Claude (same timecodes as synced EN)

### VIP Placeholder Detection (`is_placeholder_sub`)
Rejects fake subtitles by checking for known ad patterns ("opensubtitles", "vip member", "osdb.link"), fewer than 3 blocks, or single blocks spanning >10 minutes. `_download_first_valid()` tries up to 5 results before giving up.

### Forced/Signs-Only Sub Rejection
Subtitles that only contain foreign language signs or forced dialogue (e.g. `eng-forced.srt`) are rejected:
- **Scoring penalty**: -200 points for subs with "forced", "signs", "songs" in release name
- **ZIP extraction**: Prefers non-forced `.srt` files within ZIP archives
- **Block count check**: Rejects downloaded subs with fewer than 10 dialogue blocks

### Episode Matching (Subdl Scoring)
When searching for series episodes, the scoring system ensures the correct episode is downloaded:
- **Correct episode** (e.g. S01E01 sub for S01E01 video): **+500 points**
- **Wrong episode** (e.g. S01E08 sub for S01E01 video): **-1000 points**
- This prevents downloading a random episode's subtitles

### Filename Parsing (`parse_video`)
Extracts `{type, name, year|season+episode}` from the basename:
1. **Scraper noise stripping** (`_strip_scraper_noise`) — removes leading tracker/scraper prefixes via two regexes applied in a loop: domain-like prefixes (`www.SceneTime.com -`, `rarbg.to.`) and bracketed tags (`[YTS.MX]`, `(RARBG)`). Loops until stable to handle chained prefixes.
2. **Series regex** — `(.+?)[.\s_-]+[Ss]\d+[Ee]\d+` for `SxxEyy` patterns.
3. **Movie regex** — `(.+?)[.\s_-]*\(?(\d{4})\)?(?:[.\s_\-)\]]|$)` supports year with or without parentheses, with or without trailing separator. Crucial for files like `Title (2002).mkv` where `)` is not a separator.
4. **Fallback** — strips common quality/codec tags (`720p`, `x264`, `bluray`, etc.) and returns `type: "unknown"`.
5. **Title normalization** (`_clean_title`) — converts dots/underscores to spaces but preserves internal hyphens (so `Punch-Drunk` is not split).

### IMDB ID Discovery (`find_imdb_id`)
Searches `.nfo` files (Sonarr/Radarr) in the video's directory and parent directory. Extracts IMDB ID via regex `tt\d{7,}`. Used by both Subdl and OpenSubtitles for more accurate search.

### Search Query Generation (`get_search_queries`)
Returns a deduplicated list of search terms:
1. Name from filename (e.g. "Pluribus")
2. Series folder name (e.g. "PLUR1BUS")
3. Cleaned folder name, alpha-only (e.g. "PLURIBUS")

### Claude API Cost Tracking
Each translation tracks `input_tokens`, `output_tokens`, and cost (USD) in `state.json`. Accessible via `/costs` Telegram command. Pre-translation estimate shown on Telegram before user confirms.

## Telegram UX

### Notifications
- **Series with multiple episodes**: 1 grouped message per series with episode list + "Scarica tutti" button
- **Films**: 1 individual message per film with Scarica/No/Escludi buttons

### Two-Phase Batch Flow
1. User clicks "Scarica tutti (9)" → downloads EN subs (free)
2. Bot shows summary: "🇮🇹 3 ITA found, 🇬🇧 6 EN only. Tradurre? Costo: $0.57"
3. User clicks "Traduci in italiano ($0.57)" → translation starts
4. Or clicks "Tieni solo ENG" → keeps English subs only (free)

### Batch Progress
Single message updated in-place with progress bar:
```
⬇️ Scaricando sottotitoli...
[▓▓▓▓░░░░░░] 40%
📊 4/10 — 🇮🇹 2 | 🇬🇧 1 | ❌ 1
```

### Download Queue
All download/translate requests go through a thread-safe FIFO queue processed by a background worker. Supports job types: `batch` (download), `translate` (EN→IT), `single` (individual film). Queue position shown when multiple jobs are pending.

### Manual Search
Type any text in Telegram to search. Handles dots/underscores in filenames (e.g. "Pluribus S01E01" matches "Pluribus.S01E01.720p.x264-FENiX.mkv").

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
| gcc + libc6-dev | Build webrtcvad (ffsubsync dep) | `apt-get install gcc libc6-dev` |
| Subdl.com API | Primary subtitle provider | API key via env var |
| OpenSubtitles XML-RPC | Fallback subtitle provider | Built-in (stdlib xmlrpc) |
| Claude API | EN→IT translation | API key via env var |

## File Structure
```
/app/sub_fetcher.py          # Main application
/config/state.json           # Persistent state (asked, downloaded, last_offset, claude_costs)
/config/batches.json         # Pending download/translate batches (separate file to avoid race conditions)
/config/exclude_folders.txt  # Excluded folders list
/config/sub_fetcher.log      # Application log
/media/series/               # Series (read-only mount)
/media/films/                # Films (read-only mount)
```

## Thread Safety

The main loop and the queue worker run in separate threads and both read/write persistent state. To avoid race conditions:

- `state.json` is owned by the main thread. The queue worker loads it locally at job start, makes changes, and saves it. The main thread also saves it every 5 seconds (for `last_offset` persistence).
- `batches.json` is owned independently: any thread reads/writes it via `load_batches()`/`save_batches()`. Since batches are only written at discrete moments (job complete, scan notify) and read only on button press, there is no write contention.
- The previous bug: batches were stored inside `state["batches"]`. The main thread's unconditional `save_state()` call at the end of `process_callbacks` used a stale in-memory state without the queue worker's newly-added batches, silently wiping them. Moving batches to a separate file eliminates this entirely.

## Testing
Run: `python3 -m unittest test_sub_fetcher -v`

### Pre-commit hook
The repo ships a tracked hook at `.githooks/pre-commit` that runs the full test suite before every commit. On first clone, activate it once:
```sh
git config core.hooksPath .githooks
```

## Future Ideas

Non-committed brainstorm of features that would be easy to add on top of the current architecture. Pick whichever becomes painful first.

### TMDb fallback lookup
When all providers return zero results, query TMDb by cleaned title + year to obtain the canonical title and IMDb ID, then retry the search by IMDb ID. Would rescue cases where the filename contains typos, alternate titles, or localized names. Requires a free TMDb API key.

### `/retitle <video> <new title>` command
Allow the user to manually override the parsed title for a specific file from Telegram. The override is stored in `state.json` and reused on next search. Useful when `parse_video` misfires on edge cases (anime with fansub tags, documentaries with unusual naming).

### Provider success-rate stats
Track per-provider hits/misses in `state.json` and expose via `/stats`. Helps decide when to reorder the provider cascade or drop one.

### Notification when a previously-failed item is finally found
Failures are retried after 24h. Today the user only sees the retry if they open Telegram at the right moment. A one-shot "📬 Finalmente trovato: X" notification on success would close the loop.

### Multi-language support
Currently hardcoded to Italian. Generalizing to a `TARGET_LANGS` env var (e.g. `it,es`) would let the same bot serve mixed-language households. Scoring, regex, and Claude translation prompt already parameterize cleanly on language code.

### inotify / watchdog instead of polling
Main loop scans the filesystem on a timer. Switching to `watchdog` would cut idle CPU and make new releases appear on Telegram within seconds instead of minutes. Keep the periodic scan as a safety net for missed events.

### `/undo` last download
Keep the last N downloaded subtitle paths in a ring buffer. `/undo` deletes the most recent `.it.srt` and re-marks the video as pending. Useful when a wrong subtitle slipped through the placeholder detector.

### Subtitle quality score in notifications
When multiple candidates exist, show the chosen one's score and provider in the success message (e.g. `✅ Punch-Drunk Love — Subdl (score: 780)`). Makes it easier to spot low-confidence matches that may need `/sync` or manual review.
