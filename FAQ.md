# TorBoxed FAQ (Frequently Asked Questions)

This document provides detailed answers to common questions about TorBoxed. If you don't find what you're looking for here, please check the [README](README.md) or open an issue on GitHub.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Requirements & Compatibility](#requirements--compatibility)
3. [Search Sources (Zilean, Prowlarr, Jackett)](#search-sources-zilean-prowlarr-jackett)
4. [Quality Scoring & Upgrades](#quality-scoring--upgrades)
5. [TV Show Support](#tv-show-support)
6. [Rate Limiting & Performance](#rate-limiting--performance)
7. [Configuration](#configuration)
8. [Docker & Deployment](#docker--deployment)
9. [Troubleshooting](#troubleshooting)
10. [Security & Privacy](#security--privacy)

---

## Getting Started

### Q: Do I need to be technical to use this?

**A:** Not at all! If you can copy-and-paste commands, you're good to go. The defaults work out of the box.

TorBoxed is designed with simplicity in mind:
- **One-command installation**: `uv run torboxed.py --init` sets everything up
- **Sensible defaults**: Works immediately without any configuration
- **Clear error messages**: If something goes wrong, you'll know exactly what to fix
- **Built-in help**: Run `--test` to verify your setup

For the most basic setup, you only need:
1. A Torbox API key (get it from [torbox.app](https://torbox.app))
2. Trakt API credentials (get them from [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications))

That's it! With just these two pieces of information, TorBoxed will start syncing trending movies and shows to your Torbox account.

### Q: What are the exact steps to get started?

**A:** Here's the complete step-by-step process:

**Step 1: Get API Keys**
- **Torbox**: Sign in → Settings → API → Copy your API key
- **Trakt**: Visit [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications) → New Application:
  - Name: `TorBoxed`
  - Redirect URI: `urn:ietf:wg:oauth:2.0:oob`
  - Save the Client ID and Client Secret

**Step 2: Clone and Setup**
```bash
git clone https://github.com/SwordfishTrumpet/torboxed.git
cd torboxed
```

**Step 3: Create `.env` file**
```bash
cat > .env << 'EOF'
TORBOX_API_KEY=your_torbox_api_key_here
TRAKT_CLIENT_ID=your_trakt_client_id_here
TRAKT_CLIENT_SECRET=your_trakt_client_secret_here
EOF
```

**Step 4: Initialize and Run**
```bash
# Initialize the database (one-time setup)
uv run torboxed.py --init

# Run your first sync!
uv run torboxed.py
```

**Step 5: Optional - Set up automatic syncing**
```bash
# Set up daily automatic syncing
uv run torboxed.py --cron-setup
```

### Q: Can I use an AI assistant to help me install?

**A:** Yes! You can use AI coding assistants like OpenCode, Claude Code, Gemini CLI, or Codex to help you install and configure TorBoxed interactively.

Simply:
1. Clone the repository
2. Start your AI assistant (e.g., `opencode`)
3. Ask: *"Install TorBoxed with my API keys. TORBOX_API_KEY=xxx, TRAKT_CLIENT_ID=yyy, TRAKT_CLIENT_SECRET=zzz"*

The AI will:
- Check for Python/uv and install if needed
- Create your `.env` file with the provided API keys
- Install dependencies
- Initialize the database
- Run a test sync
- Provide next steps

---

## Requirements & Compatibility

### Q: What are the system requirements?

**A:** TorBoxed has minimal requirements:

**Minimum Requirements:**
- Python 3.9 or higher
- ~100 MB disk space
- Internet connection
- Linux/macOS/Windows (with WSL)

**Docker Deployment (Recommended for Servers):**
- Docker and Docker Compose
- ~200 MB disk space (includes Python and dependencies)

**Memory Usage:**
- Typical: ~50-100 MB RAM
- Peak during sync: ~200 MB RAM (depending on library size)

**Disk Usage:**
- Database: ~1-10 MB per 1000 items synced
- Logs: ~5 MB (rotating, max 3 backups)

### Q: Do I need Zilean to use TorBoxed?

**A:** No, Zilean is optional but **strongly recommended** for the best experience.

**Without Zilean:**
- TorBoxed falls back to Prowlarr or Jackett for searches
- Searches use text-based matching (less accurate)
- May get false positives or miss some content

**With Zilean:**
- Searches by exact IMDb ID (most accurate)
- Faster searches (database queries vs HTTP API calls)
- Better metadata and completeness information
- Superior season pack detection

To use Zilean, you need:
1. A running Zilean PostgreSQL database
2. The `psycopg` Python package installed (`uv pip install psycopg[binary]`)
3. Correct database connection string (default: `postgresql://zilean:zilean_password@postgres:5432/zilean`)

### Q: Can I use TorBoxed with Real Debrid instead of Torbox?

**A:** Yes! TorBoxed supports multiple debrid services:

**Configuration for Real Debrid:**
```bash
# Add to your .env file:
DEBRID_SERVICE=realdebrid
REAL_DEBRID_API_KEY=your_realdebrid_api_key_here
```

**Supported Debrid Services:**
- **Torbox** (default): Fast, unlimited downloads, good cache hit rate
- **Real Debrid**: Established service, works with many addons

The sync logic is identical for both services—TorBoxed handles the API differences internally.

---

## Search Sources (Zilean, Prowlarr, Jackett)

### Q: What is Zilean and why is it important?

**A:** Zilean is a high-performance torrent metadata database that serves as the **primary search backend** for TorBoxed.

**What Zilean Does:**
- Stores millions of torrent records with IMDb ID associations
- Enables precise searches by IMDb ID (`tt1234567`) instead of error-prone title text matching
- Provides complete metadata: torrent hash, size, title, IMDb association
- Supports TV season packs and multi-season collections

**Why It's Essential:**
- **Accuracy**: No false positives from similar title matches
- **Speed**: PostgreSQL database queries vs HTTP API calls
- **Completeness**: Finds complete series packs and season packs automatically
- **Instant Cache Check**: TorBoxed queries Zilean first, then checks Torbox cache

**Zilean Architecture:**
```
┌─────────────────────────────────────────────────────────┐
│                    Zilean Service                        │
│                   (Docker: port 8181)                    │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐      ┌─────────────────────────────┐   │
│  │  Zilean API │◄────►│    PostgreSQL Database      │   │
│  │  (C#/.NET)  │      │  (torrents, IMDb mappings)   │   │
│  └─────────────┘      └─────────────────────────────┘   │
│         ▲                                               │
│         │ HTTP /torrents endpoint                        │
│         │                                               │
│  ┌─────────────┐                                        │
│  │  Ingest     │ (Feed new torrents from indexers)       │
│  │  Worker     │                                        │
│  └─────────────┘                                        │
└─────────────────────────────────────────────────────────┘
```

### Q: How do I set up Zilean?

**A:** Zilean setup depends on your deployment method:

**Docker Setup (Recommended):**

Zilean is typically managed through the Hound/Ingest ecosystem:
```bash
# Zilean Docker setup is in the hound repository
cd ~/hound/docker

# First time setup (fix permissions)
sudo ./manage-zilean.sh setup

# Start services
docker compose up -d

# Verify health
./manage-zilean.sh health
```

**Connection Details:**
- **Default URL**: `postgresql://zilean:zilean_password@postgres:5432/zilean`
- **Host**: `postgres` (Docker service name for DNS resolution)
- **Port**: `5432`
- **Database**: `zilean`
- **User**: `zilean`

**Override**: Set `ZILEAN_DATABASE_URL` in your `.env` file if using a custom setup.

**Monitoring Zilean:**
```bash
# Check torrent count
docker exec postgres psql -U zilean -d zilean -c 'SELECT COUNT(*) FROM "Torrents";'

# Database size
docker exec postgres psql -U zilean -d zilean -c "SELECT pg_size_pretty(pg_database_size('zilean'));"

# Recent ingested torrents
docker exec postgres psql -U zilean -d zilean -c 'SELECT "RawTitle", "ImdbId", "IngestedAt" FROM "Torrents" ORDER BY "IngestedAt" DESC LIMIT 5;'
```

### Q: What happens if Zilean isn't available?

**A:** TorBoxed gracefully falls back to alternative search sources:

**Search Priority Order:**
1. **Zilean PostgreSQL** (PRIMARY): Searches by exact IMDb ID
2. **Prowlarr** (Fallback): Text-based search via local indexer manager
3. **Jackett** (Fallback): Alternative text-based search

**Fallback Behavior:**
- If Zilean connection fails, TorBoxed automatically tries Prowlarr
- If Prowlarr fails, it tries Jackett
- If all fail, that item is skipped and retried on next sync

**Important Note:** When using text-based search (Prowlarr/Jackett), TorBoxed cannot verify IMDb IDs as accurately. You may see warnings about this in logs.

### Q: How do I configure Prowlarr or Jackett?

**A:** Add their API keys to your `.env` file:

**Prowlarr:**
```bash
PROWLARR_API_KEY=your_prowlarr_api_key_here
```

**Jackett:**
```bash
JACKETT_API_KEY=your_jackett_api_key_here
```

**Getting API Keys:**
- **Prowlarr**: Settings → General → API Key
- **Jackett**: Dashboard → API Key (at the bottom of the page)

**Note**: You only need these if Zilean is unavailable or you want additional search coverage.

---

## Quality Scoring & Upgrades

### Q: How does quality scoring work?

**A:** TorBoxed automatically calculates quality scores to pick the best releases. Each component contributes points:

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

**Example Calculations:**
- 4K Blu-ray HEVC with DTS-HD MA: **~6250 points** (max quality)
- 1080p WEB-DL H.264 with AAC: **~3900 points** (good quality)
- 720p HDTV with stereo: **~2200 points** (acceptable)

**Max Quality Threshold**: 6000 points
- Releases scoring 6000+ are considered "max quality"
- TorBoxed won't search for upgrades for these releases
- This saves API calls and avoids unnecessary replacements

### Q: How do quality upgrades work?

**A:** TorBoxed uses a **completeness hierarchy** combined with quality scoring:

**Completeness Levels (from best to worst):**
1. **Complete Series Pack** - All seasons of a show
2. **Multi-Season Pack** - 2+ seasons together
3. **Season Pack** - Single complete season
4. **Episodes** - Individual episodes

**Upgrade Rules:**

**Rule 1: Completeness Always Wins**
- A complete series pack is ALWAYS preferred over individual seasons, regardless of quality
- A season pack is ALWAYS preferred over individual episodes
- This ensures your library is as complete as possible

**Rule 2: Same Level = Quality Threshold**
- Within the same completeness level, upgrades trigger when quality improves by **+500 points or more**
- Example: 1080p WEB-DL (3900) → 1080p Blu-ray (4900) = +1000 points ✓ Upgrade
- Example: 1080p WEB-DL (3900) → 1080p WEB-DL with DTS (4100) = +200 points ✗ No upgrade

**Rule 3: Never Downgrade Completeness**
- A series pack will NEVER be replaced by individual episodes, even if the episodes are 4K and the pack is 1080p
- This prevents fragmentation of your library

### Q: Can I customize quality preferences?

**A:** Yes, but it requires database modification. The defaults are optimized for most users.

**To modify quality preferences:**
```bash
# Example: Only prefer Blu-ray sources
sqlite3 torboxed.db "UPDATE config SET quality_prefs = '{\"source\": {\"Blu-ray\": 1000, \"WEB-DL\": 500, \"HDTV\": 100}}' WHERE id = 1;"
```

**Note**: Quality preferences are stored as JSON in the database. Modifying them requires understanding the scoring system. The defaults work well for most users.

### Q: Why does my 4K release keep getting "upgraded"?

**A:** This shouldn't happen if the release scores 6000+ points (max quality threshold).

**Check:**
1. Run with `--verbose` to see the quality score
2. Verify the release has proper metadata (resolution, source, codec, audio)
3. Check if the IMDb ID matches correctly

**Common Causes:**
- Release missing audio codec info (could lower score below 6000)
- Multiple versions with same hash but different metadata
- Incomplete metadata from search source

If you see unnecessary upgrades, check the verbose logs and open an issue on GitHub.

---

## TV Show Support

### Q: How does TorBoxed handle TV shows?

**A:** TorBoxed has sophisticated TV show support with intelligent season handling:

**Key Features:**
- **Completeness hierarchy**: Prefers complete series > season packs > episodes
- **Multi-season awareness**: Handles packs spanning multiple seasons (S01-S05)
- **Per-season tracking**: Each season tracked independently for upgrades
- **Automatic season detection**: Parses S01, S02, Complete, etc. from torrent names
- **No fragmentation**: Never downgrades completeness (series pack won't be replaced by episodes)

**Example Scenario:**
```
Breaking Bad:
- First sync: Finds Complete Series Pack (S01-S05) → Adds as "complete"
- Later finds: Season 3 pack in 4K → Ignored (completeness level lower)
- Later finds: Complete Series Pack in 4K → Upgrades (same completeness, +1000 points)
```

### Q: Does TorBoxed track individual episodes?

**A:** No, TorBoxed tracks at the season level, not individual episodes.

**Why?**
- **API Efficiency**: Adding 100 individual episodes = 100 API calls
- **Completeness**: Season packs are almost always preferred
- **Simplicity**: Season-level tracking is sufficient for most users

**What This Means:**
- If you have Season 1 pack, TorBoxed won't add individual S01E01, S01E02, etc.
- If a season pack is missing specific episodes, that's a content issue, not a TorBoxed issue
- Complete series packs are the ideal outcome

### Q: What if a season pack is missing some episodes?

**A:** TorBoxed doesn't verify episode-by-episode completeness. It relies on:

1. **Search source metadata**: Zilean, Prowlarr, or Jackett should provide accurate season information
2. **Release naming conventions**: Well-named packs include episode ranges
3. **File size heuristics**: Very small packs might be flagged (see `COMPLETE_PACK_MIN_SIZE` constant)

**If you find incomplete packs:**
- This is a content/metadata issue at the source
- TorBoxed can't verify every episode without downloading
- Consider reporting to your indexer or using a different source

---

## Rate Limiting & Performance

### Q: Why is syncing slow?

**A:** TorBoxed respects API rate limits to avoid getting your account banned.

**Current Rate Limits:**
- **Torbox**: 60 creations per hour (~1 per minute)
- **Torbox**: 300 general requests per minute (plenty for searches)
- **Real Debrid**: 1 request per second (conservative)
- **Real Debrid**: 60 creations per hour (~1 per minute)
- **Trakt**: ~1.67 requests per second (conservative)

**Why This Matters:**
- First syncs with many items will take time
- Example: 100 new items = ~100 minutes minimum
- Subsequent runs are much faster (only checking for upgrades and new items)

**TorBoxed's Safety Margins:**
- Waits 65 seconds between adds (5-second buffer over the 60/hour limit)
- Implements exponential backoff on rate limit errors
- Tracks rate limit state to avoid hitting limits repeatedly

### Q: Can I speed up the sync?

**A:** Only by having fewer items to sync. The rate limits are enforced by the debrid services, not TorBoxed.

**Strategies:**
1. **Filter sources**: Use only specific Trakt lists instead of all 24
2. **Use quality filters**: Skip lower quality releases
3. **Already cached**: If items are already in Torbox, they're skipped instantly
4. **Patience**: First sync is slow; subsequent syncs are fast

### Q: What does "Max retries exceeded" mean?

**A:** This usually indicates rate limiting or network issues.

**Common Causes:**
1. **Rate limiting**: Hit the service's request limit
2. **Network issues**: Intermittent connectivity problems
3. **API errors**: Service temporarily unavailable

**TorBoxed's Response:**
- Implements exponential backoff (waits longer between retries)
- Maximum 3 retries per request
- Logs detailed error information with `--verbose`

**Solutions:**
- Wait and retry (rate limits reset over time)
- Check service status pages
- Run with `--verbose` to see exact errors
- Ensure stable internet connection

### Q: Why do I see "Backing off..." messages?

**A:** This is TorBoxed's rate limiter in action. When a service returns a 429 (Too Many Requests) error, TorBoxed:

1. **Detects the rate limit**: Reads the response status code
2. **Calculates backoff**: Waits with exponential increase (1s, 2s, 4s, 8s...)
3. **Coordinates across calls**: Uses `mark_rate_limited()` to share state
4. **Retries**: Attempts the request again after waiting

This prevents your account from being banned and ensures reliable syncing.

---

## Configuration

### Q: Can I customize which lists to sync?

**A:** Absolutely! TorBoxed supports 24 different Trakt sources.

**Available Sources:**

**Movies:**
- `movies/trending` - Currently trending movies
- `movies/popular` - Most popular movies
- `movies/watched/weekly` - Most watched this week
- `movies/watched/monthly` - Most watched this month
- `movies/watched/yearly` - Most watched this year
- `movies/watched/all` - Most watched all time
- `movies/collected/weekly` - Most collected this week
- `movies/collected/monthly` - Most collected this month
- `movies/collected/yearly` - Most collected this year
- `movies/collected/all` - Most collected all time
- `movies/anticipated` - Upcoming anticipated releases
- `movies/boxoffice` - Current box office hits

**Shows:**
- `shows/trending` - Currently trending shows
- `shows/popular` - Most popular shows
- `shows/watched/weekly` - Most watched this week
- `shows/watched/monthly` - Most watched this month
- `shows/watched/yearly` - Most watched this year
- `shows/watched/all` - Most watched all time
- `shows/collected/weekly` - Most collected this week
- `shows/collected/monthly` - Most collected this month
- `shows/collected/yearly` - Most collected this year
- `shows/collected/all` - Most collected all time
- `shows/anticipated` - Upcoming anticipated releases

**Personal:**
- `users/liked` - Your liked lists on Trakt (requires `TRAKT_ACCESS_TOKEN`)

**To customize:**
```bash
# Use only specific sources (e.g., just trending)
sqlite3 torboxed.db "UPDATE config SET sources = '[\"movies/trending\", \"shows/trending\"]' WHERE id = 1;"

# Include your Trakt liked lists
sqlite3 torboxed.db "UPDATE config SET sources = '[\"users/liked\", \"movies/trending\"]' WHERE id = 1;"
```

### Q: How do I get a Trakt access token for liked lists?

**A:** Access tokens are required for personal liked lists.

**Steps:**
1. Visit [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications)
2. Click on your TorBoxed application
3. Use the device authentication flow (see [Trakt device auth docs](https://trakt.docs.apiary.io/#reference/authentication-devices))
4. Add the token to your `.env`:
```bash
TRAKT_ACCESS_TOKEN=your_access_token_here
```

**Note**: Access tokens expire and need periodic refreshing. TorBoxed handles expired tokens gracefully (skips personal lists but continues with public ones).

### Q: Can I filter by quality or other criteria?

**A:** Yes, TorBoxed supports filter configuration via the database.

**Filter Options (JSON format in database):**
```json
{
  "min_resolution": "1080p",
  "max_resolution": "4K",
  "min_size_gb": 1,
  "max_size_gb": 50,
  "required_audio": ["DTS", "Dolby"],
  "excluded_sources": ["CAM", "TS"]
}
```

**To set filters:**
```bash
sqlite3 torboxed.db "UPDATE config SET filters = '{\"min_resolution\": \"1080p\"}' WHERE id = 1;"
```

**Note**: Filter support varies by search source. Zilean supports most filters; text-based sources (Prowlarr/Jackett) support fewer.

---

## Docker & Deployment

### Q: Should I use Docker or native installation?

**A:** Both work well. Choose based on your needs:

**Use Docker if:**
- Running on a server/NAS
- Want isolated, reproducible environment
- Need automatic restarts and scheduling
- Running alongside Zilean (shared Docker network)

**Use Native if:**
- Developing or modifying TorBoxed
- Want faster startup times
- Prefer direct control over Python environment
- Running on desktop/laptop

### Q: How do I set up Docker?

**A:** Docker setup is straightforward:

**Step 1: Clone and Configure**
```bash
git clone https://github.com/SwordfishTrumpet/torboxed.git
cd torboxed

# Create .env file
cat > .env << 'EOF'
TORBOX_API_KEY=your_torbox_api_key_here
TRAKT_CLIENT_ID=your_trakt_client_id_here
TRAKT_CLIENT_SECRET=your_trakt_client_secret_here
EOF
```

**Step 2: Build and Initialize**
```bash
# Build the image
docker compose up --build

# Initialize database
docker run --rm -v $(pwd)/data:/data torboxed:latest --init
```

**Step 3: Run Sync**
```bash
# One-off sync
docker run --rm -v $(pwd)/data:/data torboxed:latest

# With verbose output
docker compose run --rm torboxed --verbose
```

**Data Persistence:**
- All data stored in `./data/` directory
- Database: `./data/torboxed.db`
- Logs: `./data/torboxed.log`
- Survives container restarts

### Q: Can I use Docker Compose for scheduling?

**A:** Yes, Docker Compose can run TorBoxed on a schedule using additional services.

**Example `docker-compose.yml` with scheduling:**
```yaml
services:
  torboxed:
    build: .
    volumes:
      - ./data:/data
      - ./.env:/data/.env:ro
    command: --verbose
    # Or use a scheduler sidecar
  
  # Optional: Use Ofelia or similar for scheduling
  scheduler:
    image: mcuadros/ofelia:latest
    command: daemon --docker
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    labels:
      ofelia.job-run.torboxed.schedule: "0 0 2 * * *"  # Daily at 2 AM
      ofelia.job-run.torboxed.container: "torboxed-torboxed-1"
```

**Alternative**: Set up cron on the host to run Docker commands.

### Q: How do I check Docker logs?

**A:**
```bash
# View logs from latest run
docker logs $(docker ps -lq)

# Follow logs in real-time
docker compose logs -f torboxed

# View specific number of lines
docker logs --tail 100 torboxed
```

---

## Troubleshooting

### Q: Something isn't working. How do I debug?

**A:** Follow this systematic approach:

**Step 1: Run Self-Test**
```bash
uv run torboxed.py --test
```
This checks:
- Environment setup
- API key validity
- Database connectivity
- Search source availability

**Step 2: Run with Verbose Output**
```bash
uv run torboxed.py --verbose
```
Shows:
- Every API call
- Quality scores
- Upgrade decisions
- Error details

**Step 3: Check Logs**
```bash
# View recent log entries
tail -n 100 torboxed.log

# Search for errors
grep -i error torboxed.log

# Check database state
sqlite3 torboxed.db "SELECT * FROM processed ORDER BY processed_at DESC LIMIT 10;"
```

**Step 4: Verify API Keys**
- Torbox: [torbox.app](https://torbox.app) → Settings → API
- Trakt: [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications)

### Q: I'm getting "No results found" for everything

**A:** This usually indicates a search source problem.

**Check:**
1. **Zilean running?**
   ```bash
   docker exec postgres psql -U zilean -d zilean -c 'SELECT COUNT(*) FROM "Torrents";'
   ```

2. **Prowlarr/Jackett configured?**
   - Verify API keys in `.env`
   - Check if services are running
   - Test with their web interfaces

3. **Search limits?**
   - Some indexers block automated searches
   - Check if APIs are returning results manually

**Solutions:**
- Ensure at least one search source is working
- Try different search sources
- Check firewall/network connectivity

### Q: TorBoxed keeps trying to add the same torrent

**A:** This could indicate:

1. **Hash tracking issue**: The torrent hash isn't being recorded
2. **Database corruption**: The processed table is inconsistent
3. **Duplicate in search results**: Same torrent appears multiple times

**Solutions:**
- Check the processed table: `sqlite3 torboxed.db "SELECT imdb_id, action, COUNT(*) FROM processed GROUP BY imdb_id, action;"`
- Look for duplicates or inconsistent states
- Clear and re-init if necessary: backup, delete DB, `--init`, restore relevant data

### Q: Why are some items "skipped"?

**A:** Items are skipped for various reasons:

| Reason | Explanation |
|--------|-------------|
| `already_exists` | Already in your debrid account |
| `no_cached_torrents` | Not available in debrid cache |
| `already_processed` | Processed in a previous run |
| `max_quality` | Already at max quality, no upgrade needed |
| `filtered` | Didn't meet filter criteria |
| `error` | An error occurred during processing |

**Check skip reasons:**
```bash
sqlite3 torboxed.db "SELECT imdb_id, title, action, reason FROM processed WHERE action='skipped' ORDER BY processed_at DESC LIMIT 20;"
```

### Q: What if a movie isn't available on Torbox yet?

**A:** TorBoxed skips it and will check again on the next run.

**How It Works:**
1. Searches for torrents via Zilean/Prowlarr/Jackett
2. Checks if any are cached on Torbox/Real Debrid
3. If none cached: Skips with reason `no_cached_torrents`
4. On next sync: Retries the same movie/show
5. Once cached: Adds automatically!

**Patience is key.** Content appears in cache as users request it. Popular content usually appears quickly; niche content may take longer.

### Q: How do I reset everything and start fresh?

**A:**

**Option 1: Keep database, clear processed items**
```bash
# Clear all processed records (keeps config)
sqlite3 torboxed.db "DELETE FROM processed;"
```

**Option 2: Full reset**
```bash
# Backup first (optional)
cp torboxed.db torboxed.db.backup

# Remove database and re-init
rm torboxed.db
torboxed.db torboxed.log
uv run torboxed.py --init
```

**Note**: Resetting clears your sync history. TorBoxed will re-process everything, but it won't create duplicates (hash-based deduplication still works).

---

## Security & Privacy

### Q: Is my data safe?

**A:** Yes! TorBoxed implements multiple security measures:

**Local-First Architecture:**
- Database stored locally (`torboxed.db`)
- No cloud storage or external logging
- All processing happens on your machine

**API Key Security:**
- Keys stored in `.env` file (not in code)
- Keys redacted from all logs
- Example log: `Authorization: Bearer ***REDACTED***`

**Secure Defaults:**
- Lock file prevents concurrent runs (race condition protection)
- Path validation prevents directory traversal
- Input sanitization on all user inputs
- SQL injection protection via parameterized queries

**Network Security:**
- SSL certificate verification enabled
- IPv4-only transport (avoids broken IPv6 issues)
- Timeout limits prevent hanging connections

### Q: What data does TorBoxed collect?

**A:** TorBoxed is privacy-focused:

**What We Store Locally:**
- IMDb IDs and titles of synced content
- Quality scores and torrent hashes
- Sync timestamps and actions
- Your API keys (in `.env`)

**What We DON'T Collect:**
- No telemetry or analytics
- No cloud sync
- No external logging services
- No personal information beyond what's needed for syncing

**What We Send to External APIs:**
- API keys (for authentication)
- IMDb IDs (to search for content)
- Torrent hashes (to check cache status)

We only communicate with:
- Trakt.tv (to discover content)
- Torbox/Real Debrid (to add torrents)
- Zilean/Prowlarr/Jackett (to search torrents)

### Q: How do I secure my API keys?

**A:** Follow these best practices:

1. **File Permissions**: Restrict `.env` file access
   ```bash
   chmod 600 .env
   ```

2. **Never Commit**: Ensure `.env` is in `.gitignore`
   ```bash
   echo ".env" >> .gitignore
   ```

3. **Use Strong Keys**: Rotate keys periodically
   - Torbox: Generate new keys in Settings
   - Trakt: Create new applications or rotate credentials

4. **Monitor Usage**: Check your debrid service's activity logs

### Q: Can someone steal my API keys from logs?

**A:** No, TorBoxed automatically redacts sensitive information from logs.

**Redaction Patterns:**
- API keys: `api_key=abc123` → `api_key=***REDACTED***`
- Authorization headers: `Authorization: Bearer token` → `Authorization: Bearer ***REDACTED***`
- Passwords: `password=secret` → `password=***REDACTED***`

**Even in verbose mode**, sensitive data is never exposed. This is a core security feature.

---

## Additional Resources

- [README.md](README.md) - Main documentation
- [TODO.md](TODO.md) - Known issues and planned features
- [GitHub Issues](https://github.com/SwordfishTrumpet/torboxed/issues) - Bug reports and feature requests

**Happy watching! 🍿**
