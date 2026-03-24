# Sub ITA Fetcher

Automated Italian subtitle downloader with Telegram bot interface. Scans your media library, finds videos missing Italian subtitles, and downloads/translates them automatically.

## Features

- **Multi-provider search**: Subdl.com (primary) + OpenSubtitles.org (fallback)
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
Ask user via Telegram (1 message per series, individual for films)
        ↓
User clicks "Scarica tutti" → Phase 1 (FREE):
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
- **Claude API** (optional): Get from [console.anthropic.com](https://console.anthropic.com)
- **OpenSubtitles** (optional): Register at [opensubtitles.org](https://www.opensubtitles.org)

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
    - CLAUDE_API_KEY=your_claude_key        # optional
    - OS_USERNAME=your_os_username           # optional
    - OS_PASSWORD=your_os_password           # optional
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
| `/cleanup` | Find and remove placeholder subtitles |
| `/excludes` | List excluded folders |
| `/reset` | Clear cache, rescan from scratch |
| `/help` | Show all commands |

Type any text to search your media library (handles dots in filenames).

## Subtitle Search Cascade

For each video, the bot tries multiple strategies in order:

1. **Local files**: Check if `.it.srt` or `.en.srt` already exists in the folder
2. **Subdl.com ITA**: Search by IMDB ID (from `.nfo` files) or by name, with episode matching
3. **OpenSubtitles ITA**: Search by file hash, IMDB ID, or name (tries up to 5 results, skipping VIP placeholders)
4. **Subdl.com ENG**: Save `.en.srt`, sync to audio, ask user to translate
5. **OpenSubtitles ENG**: Same as above

## Architecture

Single Python file (`sub_fetcher.py`), no frameworks. Runs as a long-lived process inside Docker.

| Component | Technology |
|---|---|
| Runtime | Python 3.11 (slim) |
| Subtitle sync | ffsubsync (Voice Activity Detection) |
| Audio detection | ffprobe (Italian audio track detection) |
| Primary provider | Subdl.com REST API |
| Fallback provider | OpenSubtitles.org XML-RPC |
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
python3 test_sub_fetcher.py -v
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
