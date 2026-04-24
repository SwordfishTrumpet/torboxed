# TorBoxed ✨

**Your Trakt Watchlist, Automatically in Torbox**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)]()

> Tired of manually copying movies from Trakt to Torbox? Let TorBoxed handle it! It automatically discovers trending & popular content from Trakt and adds it to your Torbox account—with smart quality upgrades when better versions become available.

---

## 🎯 What TorBoxed Does For You

**🎬 Discover & Sync Automatically**  
Pulls movies and shows from 24 curated Trakt lists—trending, popular, most-watched, anticipated releases, and even your liked lists. No more manually browsing and adding!

**📈 Smart Quality Upgrades**  
Automatically replaces lower-quality versions with better ones (1080p → 4K, WEB-DL → Blu-ray) when they become available in Torbox's cache.

**⚡ Set It and Forget It**  
Runs fully automatically with built-in cron/Docker scheduling. Set it up once, and your Torbox library stays fresh without lifting a finger.

---

## 🚀 Quick Start (5 Minutes)

### Option 1: Native (uv + Python)

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/torboxed.git
cd torboxed

# 2. Create your .env file with API keys
cat > .env << 'EOF'
TORBOX_API_KEY=your_torbox_api_key_here
TRAKT_CLIENT_ID=your_trakt_client_id_here
TRAKT_CLIENT_SECRET=your_trakt_client_secret_here
EOF

# 3. Initialize the database
uv run torboxed.py --init

# 4. Run your first sync!
uv run torboxed.py
```

**That's it!** TorBoxed will start syncing content immediately.

### Option 2: Docker (Great for Servers)

```bash
# 1. Clone and set up
git clone https://github.com/yourusername/torboxed.git
cd torboxed

# 2. Create .env file (same as above)
cat > .env << 'EOF'
TORBOX_API_KEY=your_torbox_api_key_here
TRAKT_CLIENT_ID=your_trakt_client_id_here
TRAKT_CLIENT_SECRET=your_trakt_client_secret_here
EOF

# 3. Build and run
docker-compose up --build
docker run --rm -v $(pwd)/data:/data torboxed:latest --init
docker run --rm -v $(pwd)/data:/data torboxed:latest
```

**Want it to run automatically every day?** Just add `--cron-setup`:
```bash
uv run torboxed.py --cron-setup  # Interactive setup helper
```

---

## 🎪 How It Works

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────────┐
│   Trakt Lists   │────▶│   TorBoxed  │────▶│  Search Sources  │
│  (24 sources)   │     │             │     │ (Zilean/Prowlarr)│
└─────────────────┘     └─────────────┘     └──────────────────┘
                                                        │
                              ┌─────────────────────────┘
                              ▼
                       ┌──────────────┐
                       │ Torbox Cache │
                       │   Check      │
                       └──────────────┘
                              │
                              ▼
                       ┌──────────────┐
                       │   Added to   │
                       │   Your Acc   │
                       └──────────────┘
```

**The Magic:**
1. **Discovers** content from curated Trakt lists (trending, popular, etc.)
2. **Searches** multiple sources to find the best torrents by IMDb ID
3. **Checks** Torbox cache to ensure instant availability
4. **Adds** only cached torrents (no waiting for downloads!)
5. **Tracks** everything so you never get duplicates
6. **Upgrades** quality automatically when better versions appear

---

## ✨ What Makes It Special?

| Feature | Why You'll Love It |
|---------|-------------------|
| **🚀 Instant Only** | Only adds content already cached on Torbox—no waiting! |
| **🔄 Smart Upgrades** | Auto-replaces lower quality with +500 point improvement threshold |
| **🛡️ No Duplicates** | Smart tracking by IMDb ID means zero duplicates, ever |
| **📺 Full TV Support** | Handles complete series, individual seasons, multi-season packs |
| **⏱️ Rate Limited** | Respects API limits with automatic backoff—won't get you banned |
| **🔒 Secure** | API keys redacted from logs, secure lock files, SSL verification |
| **🎯 Idempotent** | Run it once or a thousand times—same result, no duplicates |

---

## ❓ Quick FAQ

**Q: Do I need to be technical to use this?**  
A: Not at all! If you can copy-and-paste commands, you're good to go. The defaults work out of the box.

**Q: Will it download things I don't want?**  
A: No—it only syncs from Trakt's curated lists (trending, popular, anticipated). You control which sources to use.

**Q: What if a movie isn't available on Torbox yet?**  
A: TorBoxed skips it and will check again on the next run. Once it's cached, it'll be added automatically!

**Q: Is my data safe?**  
A: Yes! Your database is stored locally, API keys are secured, and sensitive info is never logged.

**Q: Can I customize which lists to sync?**  
A: Absolutely! You can choose from 24 sources including your personal liked lists on Trakt.

**Q: How do quality upgrades work?**  
A: TorBoxed scores each release (resolution + source + codec + audio). When a +500+ point better version appears, it upgrades automatically.

---

## 📊 Quality Scoring Explained

TorBoxed automatically calculates quality scores to pick the best releases:

| Component | Best Option | Points |
|-----------|-------------|--------|
| **Resolution** | 4K/2160p | 4000 |
| | 1080p | 2500 |
| | 720p | 1500 |
| **Source** | Blu-ray | 1000 |
| | WEB-DL | 900 |
| | HDTV | 700 |
| **Codec** | AV1 | 800 |
| | H.265/HEVC | 600 |
| | H.264 | 500 |
| **Audio** | DTS-HD MA | 600 |
| | Dolby TrueHD | 550 |
| | DTS | 400 |

**Example:** A 4K Blu-ray HEVC release with DTS-HD MA scores **~6250 points**—that's "max quality" and won't be upgraded further!

---

## 🎮 Common Commands

```bash
# Run a sync (the main event!)
uv run torboxed.py

# Run with verbose output to see what's happening
uv run torboxed.py --verbose

# Test your setup before syncing
uv run torboxed.py --test

# Set up automatic daily syncing
uv run torboxed.py --cron-setup

# Check if cron is set up
uv run torboxed.py --cron-status

# View stats about what you've synced
uv run torboxed.py --stats

# See recently added items
uv run torboxed.py --recent 20
```

---

## 📺 TV Show Season Support

TorBoxed handles TV shows intelligently:

- **Detects seasons** automatically from torrent names (S01, S02, Complete, etc.)
- **Adds all seasons** that are available, not just one
- **Tracks independently**—each season can be upgraded separately
- **Complete series packs** supported alongside individual seasons

Example output:
```
By type:
  movie       : 25
  show        : 8 shows (15 seasons/episodes)

Recent upgrades:
  • Breaking Bad [S03] (2010)
    Score: 2400 -> 3500
```

---

## 🔧 Simple Configuration

**The defaults work great**, but you can customize:

```bash
# Use only specific sources (e.g., just trending)
sqlite3 torboxed.db "UPDATE config SET sources = '[\"movies/trending\", \"shows/trending\"]' WHERE id = 1;"

# Include your Trakt liked lists (requires access token)
sqlite3 torboxed.db "UPDATE config SET sources = '[\"users/liked\", \"movies/trending\"]' WHERE id = 1;"
```

**Available sources include:**
- Movies: trending, popular, watched (weekly/monthly/yearly/all), collected, anticipated, box office
- Shows: trending, popular, watched, collected, anticipated
- Personal: liked lists (requires `TRAKT_ACCESS_TOKEN`)

---

## 🔐 Getting API Keys

### Torbox API Key
1. Sign in at https://torbox.app
2. Go to Settings → API
3. Copy your API key

### Trakt API Credentials
1. Visit https://trakt.tv/oauth/applications
2. Click "New Application"
3. Name: `TorBoxed`
4. Redirect URI: `urn:ietf:wg:oauth:2.0:oob`
5. Save the Client ID and Client Secret

**For liked lists support**, you'll also need a Trakt Access Token (see [Trakt device auth docs](https://trakt.docs.apiary.io/#reference/authentication-devices)).

---

## 🐳 Docker Tips

```bash
# Quick one-off sync
docker run --rm -v $(pwd)/data:/data torboxed:latest

# With verbose output
docker-compose run --rm torboxed --verbose

# Check stats
docker run --rm -v $(pwd)/data:/data torboxed:latest --stats
```

**Data persistence:** All your data (database, logs) is stored in `./data/` so it survives container restarts.

---

## 🆘 Need Help?

- **Something not working?** Run `uv run torboxed.py --test` to verify your setup
- **No results?** Make sure you have at least one search source configured (Zilean, Prowlarr, or Jackett)
- **Quality questions?** Check the Quality Scoring section above
- **Still stuck?** Open an issue on GitHub with your `--verbose` output

---

## 🏗️ Architecture (For the Curious)

TorBoxed uses a cascading search strategy:

1. **Zilean PostgreSQL** (optional, most accurate) - searches by IMDb ID
2. **Prowlarr** (fallback) - searches configured indexers by title
3. **Jackett** (alternative fallback) - searches by title
4. **Torbox Cache Check** - verifies instant availability
5. **SQLite Database** - tracks state for idempotency and upgrades

**Requirements:**
- Python 3.9+
- `httpx` (HTTP client)
- `guessit` (quality parsing)
- Optional: `psycopg` for Zilean database support

---

## 📄 License

MIT License - use it, modify it, share it freely!

---

## 📝 Recent Updates

### 2026-04-24: Bug Fixes & Code Quality
- **Fixed:** Prowlarr/Jackett API key loading now uses `.env` file (was bypassing lazy loader)
- **Fixed:** systemd timer setup now writes service files to `/tmp` for install commands
- **Fixed:** Torrent hash tracking during sync to prevent re-adding same hash within a run
- **Improved:** Extracted `_display_title()`, `_get_filter_config()`, `_get_searcher_list()` helpers to eliminate code duplication
- **Improved:** Fixed misleading comment in season conflict resolution logic
- **Improved:** Removed unnecessary `COMMIT` in database migration
- **Total Tests:** 182 passing

### 2026-04-23: Bug Fixes & Code Quality
- **Fixed:** Empty infohash handling - torrents without valid hashes are now properly filtered out before attempting to add to Torbox
- **Improved:** All bare `except Exception` clauses converted to specific exception types for better error handling
- **Improved:** Added named constants for all magic numbers (timeouts, limits, thresholds)
- **Improved:** Code structure - subprocess imports moved to top of file
- **Added:** Comprehensive test coverage for `check_cached` method (7 new tests)
- **Added:** Complete test suite for retry/backoff logic (8 new tests)
- **Total Tests:** 182 passing

---

**Happy watching! 🍿**  
*Made with ❤️ for media enthusiasts who love automation.*
