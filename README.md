# Sub ITA Fetcher

Automated Italian subtitle downloader with Telegram bot interface. Scans your media library, finds videos missing Italian subtitles, and downloads/translates them automatically.

## Features

- **Multi-provider search**: Subdl.com (primary) + OpenSubtitles.com REST v1 (fallback)
- **TMDb lookup**: resolves IMDb ID from title + year when no `.nfo` file is present, so search runs on a canonical ID instead of a dirty filename
- **Two-phase download**: EN subs downloaded for free, translation is optional and cost-estimated upfront
- **AI translation**: English → Italian via Claude API with cost estimate before confirmation
- **Audio sync**: ffsubsync aligns subtitles to video audio (handles different releases)
- **Episode matching**: Ensures correct episode subtitles are downloaded (not random episodes)
- **Italian audio detection**: Skips films already in Italian (via ffprobe)
- **VIP placeholder rejection**: Detects and skips fake/ad subtitles from OpenSubtitles
- **Forced sub filtering**: Rejects signs-only/forced subtitles, prefers full dialogue
- **Telegram bot**: Grouped notifications, batch downloads with progress bar, download queue
- **Dual save**: Keeps both `.en.srt` (synced) and `.it.srt` when translating
- **Cost tracking**: Tracks Claude API usage and costs, shows estimate before translating

## How It Works

```
Scan media folders → Find missing .it.srt → Group by series/film
        ↓
Auto-enqueue download (no "Scarica?" prompt — subs are always wanted)
        ↓
Phase 1 (FREE):
  1. Check local folder for existing subs
  2. Search Subdl.com (ITA) → download directly
  3. Search OpenSubtitles (ITA) → download (skip placeholders)
  4. Search Subdl.com (ENG) → save .en.srt + sync to audio
  5. Search OpenSubtitles (ENG) → save .en.srt + sync to audio
        ↓
Summary on Telegram: "🇮🇹 3 ITA, 🇬🇧 6 EN only. Tradurre? Costo: $0.57"
        ↓
Phase 2 (PAID, user confirms):
  User clicks "Traduci ($0.57)" → Claude translates EN→IT
  User clicks "Tieni solo ENG" → keeps English only (free)
```

## Quick Start

### 1. Get API Keys

- **Telegram Bot**: Create via [@BotFather](https://t.me/BotFather)
- **Subdl.com**: Register at [subdl.com](https://subdl.com) for a free API key
- **OpenSubtitles.com** (recommended): Register at [opensubtitles.com](https://www.opensubtitles.com), then create an API consumer at `/en/consumers` with `dev_mode` enabled (100 downloads/day free). Copy the API key.
- **TMDb** (recommended): Register at [themoviedb.org](https://www.themoviedb.org/settings/api), request a Developer key (free, unlimited). Used to resolve IMDb IDs from title + year.
- **Claude API** (optional): Get from [console.anthropic.com](https://console.anthropic.com)

### 2. Docker Compose

Add to your `docker-compose.yml`:

```yaml
sub-fetcher:
  build:
    context: ./sub-fetcher
    dockerfile: Dockerfile
  container_name: sub-fetcher
  restart: unless-stopped
  volumes:
    - /path/to/series:/media/series
    - /path/to/films:/media/films
    - ./sub-fetcher:/config
  environment:
    - TELEGRAM_BOT_TOKEN=your_token
    - TELEGRAM_CHAT_ID=your_chat_id
    - SUBDL_API_KEY=your_subdl_key
    - OPENSUBTITLES_API_KEY=your_opensubtitles_consumer_key   # recommended
    - TMDB_API_KEY=your_tmdb_v3_api_key                       # recommended
    - CLAUDE_API_KEY=your_claude_key                          # optional
```

### 3. Run

```bash
docker compose up -d --build sub-fetcher
```

## Telegram Commands

| Command | Description |
|---|---|
| `/status` | Current state (pending/downloaded/failed) |
| `/scan` | Force a manual scan |
| `/costs` | Claude API translation costs |
| `/sync [name]` | Sync subtitles to video audio |
| `/translate <name>` | Sync English subs (`.en.srt`, `.eng.srt`, `.english.srt`) matching `<name>` to audio, then ask to translate EN→IT |
| `/delete <name>` | Delete all subs (IT/EN variants) for videos matching `<name>` and re-queue them for a fresh download |
| `/cleanup` | Find and remove placeholder subtitles |
| `/excludes` | List excluded folders |
| `/reset` | Clear cache, rescan from scratch |
| `/help` | Show all commands |

Type any text to search your media library (handles dots in filenames).

## Subtitle Search Cascade

For each video, the bot tries multiple strategies in order:

1. **Local files**: Check if `.it.srt` or `.en.srt` / `.eng.srt` already exists in the folder
2. **Filename parsing**: Strips scraper/tracker prefixes (`www.SceneTime.com -`, `[YTS.MX]`) and handles year in parentheses (`Film (2002).mkv`)
3. **IMDb ID resolution**: Reads `.nfo` files written by Radarr/Sonarr; falls back to TMDb lookup by title + year
4. **Try ITA**: Subdl → OpenSubtitles → validate sync (score ≥ 800) → save `.it.srt` if good
5. **Always try ENG too**: Subdl → OpenSubtitles → validate sync → save `.en.srt` if good (runs regardless of whether ITA was found, so EN is always available as backup or for later translation)
6. **Outcome**:
   - **Both saved** → notify "ITA scaricato" + list both files
   - **Only ENG saved** → notify "ENG salvato, tradurre?" with `[Traduci]` / `[Tieni ENG]` buttons
   - **Neither saved** → notify failure with full search trace and OpenSubtitles quota remaining
7. **Failure trace** shows all attempted providers, methods, sync scores, and OpenSubtitles download quota

## Architecture

Single Python file (`sub_fetcher.py`), no frameworks. Runs as a long-lived process inside Docker.

| Component | Technology |
|---|---|
| Runtime | Python 3.11 (slim) |
| Subtitle sync | ffsubsync (Voice Activity Detection) |
| Audio detection | ffprobe (Italian audio track detection) |
| Primary provider | Subdl.com REST API |
| Fallback provider | OpenSubtitles.com REST API v1 |
| IMDb ID resolution | `.nfo` files → TMDb search fallback |
| Translation | Claude API (batches of 100 subtitle blocks) |
| Bot interface | Telegram Bot API (polling) |
| State | JSON file (`/config/state.json`) |
| Queue | Thread-safe FIFO (download, translate, single jobs) |

## File Structure

```
sub_fetcher.py           # Main application (single file)
test_sub_fetcher.py      # Unit tests (55 tests)
Dockerfile               # Container definition
SPEC.md                  # Technical specification
AGENTS.md                # Agent/AI coding guidelines
```

## Development

### Run tests
```bash
python3 -m unittest test_sub_fetcher -v
```

### Pre-commit hook
A tracked hook in `.githooks/pre-commit` runs the full test suite before every commit. Activate it once after cloning:
```bash
git config core.hooksPath .githooks
```

### Rebuild and restart
```bash
docker compose up -d --build sub-fetcher
```

### View logs
```bash
docker logs -f sub-fetcher --tail 50
```

## License

Private project.
