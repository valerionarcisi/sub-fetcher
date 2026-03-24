# Sub ITA Fetcher - Agent Guidelines

## Project Overview
Telegram-interactive Italian subtitle downloader. Scans media folders, finds videos missing Italian subs, asks via Telegram whether to download them, and saves them with the correct filename. Runs as a long-lived Docker container.

## Architecture
Single-file Python 3 application (`sub_fetcher.py`). External deps: `ffmpeg` (audio detection via ffprobe), `ffsubsync` (subtitle-to-audio sync). Two subtitle providers: **Subdl.com** (primary, no VIP placeholders) and **OpenSubtitles.org** (fallback). Translations via Claude API save both `.en.srt` and `.it.srt`. Containerized via `Dockerfile` (python:3.11-slim + gcc for webrtcvad build).

### Key Components
- **SubdlClient**: primary provider, REST API, downloads ZIP archives. Episode matching (+500 correct, -1000 wrong) prevents downloading wrong episode subs.
- **OSClient**: fallback provider, XML-RPC, `_download_first_valid()` tries up to 5 results skipping VIP placeholders
- **Two-phase download**: Phase 1 downloads EN subs (free). Phase 2 translates to IT (paid, user-confirmed with cost estimate).
- **Claude API integration**: translates English subtitles to Italian in batches of 100 blocks. Cost estimated before user confirms.
- **ffsubsync integration**: aligns subtitle timecodes to video audio via Voice Activity Detection. Uses `os.system()` (not `subprocess.run` with pipes — pipes interfere with ffsubsync's `rich` library). EN subs synced BEFORE translation so IT inherits correct timecodes.
- **ffprobe integration**: detects Italian audio tracks to skip Italian-language films
- **Telegram Bot**: user interaction (grouped notifications for series, individual for films, batch downloads, progress bar, download queue, translation confirmation with cost)
- **Download Queue**: thread-safe FIFO processing download/translate/single jobs sequentially
- **State management**: JSON file tracking asked/downloaded/failed/excluded/en_only state + Claude costs
- **Media scanner**: walks `/media/series` and `/media/films` for missing `.it.srt`

## How to Run Tests
```bash
python3 test_sub_fetcher.py -v
```
Tests use only `unittest` (stdlib). The test harness patches `/config` paths to a temp directory. 55 tests covering parsing, search cascade, grouping, placeholder detection, Subdl client, ffprobe mocking, episode matching, two-phase download, manual search with dots, queue job types.

## Code Conventions
- Minimal external dependencies (stdlib + ffsubsync only)
- Logging via Python `logging` module to file + stdout
- All Telegram messages in Italian
- SRT files saved as `<video_name>.it.srt` (and `.en.srt` when translating)
- State persisted in `/config/state.json`
- No comments explaining *what* — use self-documenting code. Comments only for *why*.

## Configuration (Environment Variables)
| Variable | Required | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot API token |
| `TELEGRAM_CHAT_ID` | Yes | Target chat ID for notifications |
| `SUBDL_API_KEY` | Yes | Subdl.com API key (free registration) |
| `CLAUDE_API_KEY` | No | Enables EN→IT translation fallback |
| `CLAUDE_MODEL` | No | Default: `claude-sonnet-4-20250514` |
| `OS_USERNAME` | No | OpenSubtitles.org username |
| `OS_PASSWORD` | No | OpenSubtitles.org password |

## Docker
```bash
docker compose up -d --build sub-fetcher
```

## Telegram Commands
`/status`, `/scan`, `/costs`, `/sync [name]`, `/cleanup`, `/excludes`, `/reset`, `/help`
Free text searches the media library (handles dots/underscores in filenames).
