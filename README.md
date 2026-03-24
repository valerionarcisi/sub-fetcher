# Sub ITA Fetcher

Automated Italian subtitle downloader with Telegram bot interface. Scans your media library, finds videos missing Italian subtitles, and downloads/translates them automatically.

## Features

- **Multi-provider search**: Subdl.com (primary) + OpenSubtitles.org (fallback)
- **AI translation**: English → Italian via Claude API when no Italian sub exists
- **Audio sync**: ffsubsync aligns subtitles to video audio (handles different releases)
- **Italian audio detection**: Skips films already in Italian (via ffprobe)
- **VIP placeholder rejection**: Detects and skips fake/ad subtitles from OpenSubtitles
- **Telegram bot**: Grouped notifications, batch downloads with progress bar, manual commands
- **Dual save**: Keeps both `.en.srt` and `.it.srt` when translating
- **Cost tracking**: Tracks Claude API usage and costs

## How It Works

```
Scan media folders → Find missing .it.srt → Group by series/film
        ↓
Ask user via Telegram (1 message per series, 1 digest for films)
        ↓
User clicks "Scarica tutti" → Download workflow:
  1. Check local folder for existing .en.srt → translate
  2. Search Subdl.com (ITA) → download
  3. Search OpenSubtitles (ITA) → download (skip placeholders)
  4. Search Subdl.com (ENG) → translate with Claude
  5. Search OpenSubtitles (ENG) → translate with Claude
        ↓
Sync to video audio with ffsubsync → Save .it.srt
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
    - /path/to/series:/media/series:ro
    - /path/to/films:/media/films:ro
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

Type any text to search your media library.

## Subtitle Search Cascade

For each video, the bot tries multiple strategies in order:

1. **Local files**: Check if `.en.srt` already exists in the folder
2. **Subdl.com**: Search by IMDB ID (from `.nfo` files) or by name
3. **OpenSubtitles.org**: Search by file hash, IMDB ID, or name (tries up to 5 results, skipping VIP placeholders)
4. **English fallback**: Repeat steps 2-3 for English, then translate to Italian via Claude API

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

## File Structure

```
sub_fetcher.py           # Main application (single file)
test_sub_fetcher.py      # Unit tests (37 tests)
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
