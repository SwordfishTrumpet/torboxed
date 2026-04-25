#!/usr/bin/env python3
"""torboxed - Sync Trakt.tv curated movies and shows to Torbox."""

import sqlite3
import json
import httpx
import os
import sys
import time
import argparse
import logging
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Set
from abc import ABC, abstractmethod
from dataclasses import dataclass
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
import re
import shlex
import shutil
import subprocess
import tempfile
import unicodedata

# Import guessit for quality parsing
try:
    from guessit import guessit
except ImportError:
    print("Error: guessit not installed. Run: pip install guessit")
    sys.exit(1)

# Import psycopg for Zilean database connection (optional)
psycopg = None
try:
    import psycopg
except ImportError:
    pass  # psycopg is optional - Zilean features will be disabled

# =============================================================================
# SECURITY FUNCTIONS (must be defined before constants)
# =============================================================================

def _validate_cron_expression(expr: str) -> bool:
    """Validate cron expression format (5 fields).
    
    VULN-002 Fix: Prevent command injection via cron expression validation.
    
    Args:
        expr: Cron expression to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not expr or not expr.strip():
        return False
    
    expr = expr.strip()
    fields = expr.split()
    
    # Must have exactly 5 fields
    if len(fields) != 5:
        return False
    
    # Pattern for valid cron field characters
    # Allows: digits, commas, dashes, slashes, asterisks, question marks, 
    #         L, W, # for special characters in day fields
    field_pattern = r'^[\d*,/\-?LW#]+$'
    
    for field in fields:
        if not re.match(field_pattern, field):
            return False
    
    return True


def get_lock_path() -> Path:
    """Get secure lock file path in user's runtime directory.
    
    Uses user-specific temp directory with UID suffix for consistency.
    Avoids XDG_RUNTIME_DIR because cron environment lacks it,
    which would cause different lock paths and break mutual exclusion.
    
    Returns:
        Path to secure lock file location
    """
    return Path(tempfile.gettempdir()) / f'torboxed-{os.getuid()}.lock'


# Paths (using secure defaults)
DB_PATH = Path("torboxed.db")
ENV_PATH = Path(".env")
LOG_PATH = Path("torboxed.log")
LOCK_PATH = get_lock_path()

# Logging configuration
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB
MAX_LOG_BACKUPS = 3

# API Endpoints
TRAKT_BASE_URL = "https://api.trakt.tv"
TORBOX_BASE_URL = "https://api.torbox.app"
REAL_DEBRID_BASE_URL = "https://api.real-debrid.com/rest/1.0"

# Rate limiting config
# Torbox general endpoints: 300/min per API key
# Torbox creation endpoints (createtorrent, etc): 60/hour = 1/min
# BUG-R4 FIX: Increased from 60.0 to 65.0 to be safer and prevent max retries exceeded
TORBOX_RATE_LIMIT = 0.2        # 5 req/sec for general endpoints (300/min)
TORBOX_CREATION_LIMIT = 65.0   # 1 req/min for createtorrent endpoint (65s to be safe)
REAL_DEBRID_RATE_LIMIT = 1.0         # 1 req/sec (conservative, no public limit documented)
REAL_DEBRID_CREATION_LIMIT = 65.0     # 1 req/min for addMagnet endpoint
TRAKT_RATE_LIMIT = 0.6         # ~1.67 requests per second (conservative for Trakt)

# HTTP Timeouts (seconds)
DEFAULT_TIMEOUT_SHORT = 30.0   # For simple API calls (Trakt, Telegram)
DEFAULT_TIMEOUT_LONG = 60.0    # For search operations (Prowlarr, Jackett, Torbox)
DEFAULT_TIMEOUT_CREATION = 120.0  # For torrent creation (large multi-season packs need more time)

# Search result limits
SEARCH_LIMIT_IMDB = 200        # Higher limit for IMDb ID searches (ensure season packs)
SEARCH_LIMIT_TITLE = 50        # Standard limit for title searches
TORBOX_LIST_LIMIT = 5000       # Max torrents per request from Torbox mylist

# Discovery threshold for dropped torrent detection
# If we discover fewer than 95% of tracked torrents, skip clearing to avoid false positives
DISCOVERY_COMPLETENESS_THRESHOLD = 0.95

# Complete series pack minimum size threshold (bytes)
# Large complete packs with unknown resolution metadata may still be high quality
COMPLETE_PACK_MIN_SIZE = 10 * 1024**3  # 10 GB

# Trakt pagination
TRAKT_PER_PAGE = 100           # Maximum items per page (Trakt API max)

# Zilean database config
# Default connection string for Docker network (postgres container IP)
ZILEAN_DEFAULT_DB_URL = "postgresql://zilean:zilean_password@172.20.0.2:5432/zilean"

# Valid time periods for Trakt API endpoints
VALID_PERIODS = ["weekly", "monthly", "yearly", "all"]


# =============================================================================
# ADDITIONAL SECURITY FUNCTIONS
# =============================================================================

def sanitize_error_text(text: str) -> str:
    """Remove potentially sensitive data from error messages.
    
    VULN-005 Fix: Prevent information disclosure via verbose error logging.
    Filters out API keys, tokens, passwords, and other sensitive data.
    
    Args:
        text: Raw error text that may contain sensitive information
        
    Returns:
        Sanitized text with sensitive data redacted
    """
    if not text:
        return ""
    
    # Patterns for common sensitive data (case-insensitive)
    patterns = [
        (r'(Authorization|Bearer|token|api[_-]?key)["\'\s]*[:=]\s*["\']?[^\s"\'\r\n,}]+', r'\1=***REDACTED***'),
        (r'(password|passwd|pwd)["\'\s]*[:=]\s*["\']?[^\s"\'\r\n,}]+', r'\1=***REDACTED***'),
        (r'(secret|client_secret)["\'\s]*[:=]\s*["\']?[^\s"\'\r\n,}]+', r'\1=***REDACTED***'),
        (r'[a-f0-9]{64}', r'***HASH64***'),  # SHA-256 hashes (torrent hashes)
        (r'[a-f0-9]{40}', r'***HASH40***'),  # SHA-1 hashes (magnet links)
        (r'bearer\s+[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]*', r'Bearer ***JWT_TOKEN***'),
    ]
    
    sanitized = text
    for pattern, replacement in patterns:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
    
    return sanitized[:500]  # Truncate after sanitization


def sanitize_response_error(response: Any) -> str:
    """Safely extract and sanitize error text from an HTTP response.
    
    VULN-005 Fix: Shared helper for sanitizing error text from HTTP responses.
    Handles UnicodeDecodeError and missing text attributes gracefully.
    
    Args:
        response: HTTP response object with .text attribute
        
    Returns:
        Sanitized error text string
    """
    try:
        raw_error = response.text[:1000] if response.text else ""
        return sanitize_error_text(raw_error)
    except (UnicodeDecodeError, AttributeError):
        return "<unable to decode response>"


def validate_db_path(path: Path) -> Path:
    """Validate database path is within allowed directories.
    
    VULN-004 Fix: Prevent path traversal via DB_PATH environment variable.
    
    Args:
        path: Database path to validate
        
    Returns:
        Validated path
        
    Raises:
        ValueError: If path is outside allowed directories
    """
    resolved = path.resolve()
    
    # Allowed root directories (in order of preference)
    allowed_roots = [
        Path.home() / '.local' / 'share' / 'torboxed',
        Path('/data'),  # Docker mount
        Path.cwd(),
        Path(tempfile.gettempdir()),  # Temp directory for tests
    ]
    
    # Check if path is within any allowed root
    for root in allowed_roots:
        try:
            resolved.relative_to(root)
            return resolved
        except ValueError:
            continue
    
    # Special case: allow paths that are children of the current working directory
    try:
        resolved.relative_to(Path.cwd())
        return resolved
    except ValueError:
        pass
    
    raise ValueError(f"DB_PATH {path} is outside allowed directories. "
                     f"Allowed: home/.local/share/torboxed, /data, current directory")


def validate_log_path(path: Path) -> Path:
    """Validate log path is within allowed directories.
    
    VULN-004 Fix: Prevent path traversal via LOG_PATH environment variable.
    
    Args:
        path: Log path to validate
        
    Returns:
        Validated path
        
    Raises:
        ValueError: If path is outside allowed directories
    """
    resolved = path.resolve()
    
    # Allowed root directories
    allowed_roots = [
        Path.home() / '.local' / 'share' / 'torboxed',
        Path('/data'),
        Path('/var/log'),
        Path.cwd(),
        Path(tempfile.gettempdir()),  # Temp directory for tests
    ]
    
    # Check if path is within any allowed root
    for root in allowed_roots:
        try:
            resolved.relative_to(root)
            return resolved
        except ValueError:
            continue
    
    # Special case: allow paths that are children of the current working directory
    try:
        resolved.relative_to(Path.cwd())
        return resolved
    except ValueError:
        pass
    
    raise ValueError(f"LOG_PATH {path} is outside allowed directories. "
                     f"Allowed: home/.local/share/torboxed, /data, /var/log, current directory")


class RateLimitedLogHandler(RotatingFileHandler):
    """Rotating file handler with rate limiting for repeated messages.
    
    VULN-007 Fix: Prevent log flooding during rapid error conditions.
    
    Rate limits identical messages that repeat within a time window.
    """
    
    def __init__(self, *args, max_repeats: int = 10, window_seconds: int = 60, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_message: Optional[str] = None
        self.repeat_count: int = 0
        self.last_log_time: float = 0
        self.max_repeats = max_repeats
        self.window_seconds = window_seconds
        self._suppress_message_shown: bool = False
    
    def emit(self, record):
        """Emit log record with rate limiting."""
        current_time = time.time()
        message = self.format(record)
        
        # Reset counter if window has passed
        if current_time - self.last_log_time > self.window_seconds:
            self.repeat_count = 0
            self._suppress_message_shown = False
        
        # Check if this is a repeat message
        if message == self.last_message:
            self.repeat_count += 1
            if self.repeat_count > self.max_repeats:
                # Suppress repeated messages after threshold
                if not self._suppress_message_shown:
                    suppressed = logging.LogRecord(
                        record.name, record.levelno, record.pathname, record.lineno,
                        f"... ({self.repeat_count} similar messages suppressed) ...",
                        None, record.exc_info
                    )
                    super().emit(suppressed)
                    self._suppress_message_shown = True
                return
        else:
            self.repeat_count = 0
            self._suppress_message_shown = False
            self.last_message = message
        
        self.last_log_time = current_time
        super().emit(record)


@dataclass
class QualityInfo:
    """Parsed quality information from a torrent name."""
    resolution: str = "Unknown"
    source: str = "Unknown"
    codec: str = "Unknown"
    audio: str = "Unknown"
    score: int = 0
    label: str = "Unknown"


@dataclass
class SeasonInfo:
    """Parsed season information from a torrent name."""
    seasons: List[int]  # List of season numbers (e.g., [1], [1, 2, 3], [1, 2, 3, 4, 5])
    is_complete: bool  # True if this is a complete series pack
    season_label: str  # Human-readable label (e.g., "S01", "S01-S05", "Complete")
    is_pack: bool  # True if season pack (S01), False if individual episode (S01E01)
    episode: Optional[int] = None  # Episode number if individual episode


@dataclass
class TorrentResult:
    """A torrent result from Torbox search."""
    name: str
    magnet: str
    availability: bool
    size: int
    quality: QualityInfo
    hash: str = ""
    seeders: int = 0
    leechers: int = 0
    season_info: Optional[SeasonInfo] = None  # Season info for TV shows


class RateLimiter:
    """Simple rate limiter for API requests with optional debug logging.
    
    Tracks last_successful_request separately from last_request to ensure
    proper rate limiting even after failed requests.
    """
    def __init__(self, min_interval: float, name: str = "API"):
        self.min_interval = min_interval
        self.last_successful_request = 0
        self.name = name
    
    def wait(self):
        """Wait if needed to respect rate limit."""
        elapsed = time.time() - self.last_successful_request
        if elapsed < self.min_interval:
            wait_time = self.min_interval - elapsed
            logger.debug("Rate limiting %s: waiting %.2fs", self.name, wait_time)
            time.sleep(wait_time)
    
    def mark_success(self):
        """Mark a request as successfully completed."""
        self.last_successful_request = time.time()


class ZileanClient:
    """Zilean PostgreSQL database client for searching torrents by IMDb ID or title.
    
    Connects directly to the Zilean database (same database used by zilean-api)
    to query torrents with full IMDb ID search support.
    
    Requires ZILEAN_DATABASE_URL in .env (defaults to local Docker setup).
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize Zilean client.
        
        Args:
            database_url: PostgreSQL connection string. If None, loads from ZILEAN_DATABASE_URL env var.
        """
        self.database_url = database_url or self._get_database_url_from_env()
        self._connection: Optional[Any] = None
    
    def _get_database_url_from_env(self) -> str:
        """Load ZILEAN_DATABASE_URL from environment."""
        return get_env().get("ZILEAN_DATABASE_URL", ZILEAN_DEFAULT_DB_URL)
    
    def is_configured(self) -> bool:
        """Check if Zilean database is configured and psycopg is available."""
        if psycopg is None:
            return False
        return bool(self.database_url)
    
    def _get_connection(self):
        """Get or create database connection."""
        if psycopg is None:
            raise ImportError("psycopg is not installed. Run: pip install psycopg")
        if self._connection is None or self._connection.closed:
            self._connection = psycopg.connect(self.database_url)
        return self._connection
    
    def close(self):
        """Close database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert database row to dictionary with proper field names."""
        # Map from PascalCase (DB) to snake_case/camelCase for consistency
        return {
            "infoHash": row[0],
            "rawTitle": row[1],
            "parsedTitle": row[2],
            "imdbId": row[3],
            "category": row[4],
            "year": row[5],
            "resolution": row[6],
            "quality": row[7],
            "codec": row[8],
            "size": row[9],
            "ingestedAt": row[10],
            "seasons": row[11],
            "complete": row[12],
            "audio": row[13],
        }
    
    def _build_torrent_result(self, torrent: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build a torrent result dict from a database row dict.
        
        Shared by search_by_imdb() and search() to avoid duplication.
        
        Args:
            torrent: Dict from _row_to_dict()
            
        Returns:
            Formatted torrent dict or None if infohash is invalid
        """
        info_hash = torrent["infoHash"]
        if not info_hash or len(info_hash) != 40:
            logger.debug("Skipping torrent without valid infohash (wrong length): %s", 
                       torrent.get("rawTitle") or torrent.get("parsedTitle") or "Unknown")
            return None
        try:
            int(info_hash, 16)
        except ValueError:
            logger.debug("Skipping torrent with invalid hex infohash: %s", 
                       torrent.get("rawTitle") or torrent.get("parsedTitle") or "Unknown")
            return None
        
        name = torrent["rawTitle"] or torrent["parsedTitle"] or "Unknown"
        torrent["magnet"] = encode_magnet_link(name, info_hash)
        torrent["hash"] = normalize_hash(info_hash)
        torrent["title"] = name
        torrent["name"] = name
        try:
            torrent["size"] = int(torrent["size"]) if torrent["size"] else 0
        except (ValueError, TypeError):
            torrent["size"] = 0
        torrent["seeds"] = 0
        torrent["peers"] = 0
        torrent["source"] = "zilean"
        return torrent

    def search_by_imdb(self, imdb_id: str, category: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for torrents by IMDb ID.
        
        Args:
            imdb_id: IMDb ID (e.g., "tt1234567")
            category: Optional category filter ('movie' or 'tvSeries')
            limit: Maximum number of results to return
            
        Returns:
            List of torrent dicts with infoHash, title, size, etc.
        """
        if not imdb_id or not imdb_id.startswith("tt"):
            logger.debug("Invalid IMDb ID format: %s", imdb_id)
            return []
        
        try:
            conn = self._get_connection()
            
            # Essential columns for torrent results
            sql = """
                SELECT 
                    "InfoHash", "RawTitle", "ParsedTitle", "ImdbId", "Category",
                    "Year", "Resolution", "Quality", "Codec", "Size",
                    "IngestedAt", "Seasons", "Complete", "Audio"
                FROM "Torrents"
                WHERE "ImdbId" = %s
            """
            params = [imdb_id]
            
            if category:
                sql += ' AND "Category" = %s'
                params.append(category)
            
            sql += ' ORDER BY "IngestedAt" DESC LIMIT %s'
            params.append(limit)
            
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                
                results = []
                for row in rows:
                    torrent = self._row_to_dict(row)
                    result = self._build_torrent_result(torrent)
                    if result is not None:
                        results.append(result)
                
                logger.debug("Zilean found %d torrents for IMDb ID: %s", len(results), imdb_id)
                return results
                
        except Exception as e:
            logger.debug("Zilean search traceback for IMDb ID '%s':", imdb_id, exc_info=True)
            if psycopg is not None and isinstance(e, psycopg.Error):
                logger.warning("Zilean database error for IMDb ID '%s': %s", imdb_id, e)
            else:
                logger.warning("Zilean search error for IMDb ID '%s': %s", imdb_id, e)
            return []
    
    def search(self, query: str, category: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for torrents by title query.
        
        Args:
            query: Search query string
            category: Optional category filter ('movie' or 'tvSeries')
            limit: Maximum number of results to return
            
        Returns:
            List of torrent dicts with infoHash, title, size, etc.
        """
        if not query:
            return []
        
        try:
            conn = self._get_connection()
            
            # Use prefix search for better performance (like zilean-api)
            search_term = f"%{query}%"
            
            sql = """
                SELECT 
                    "InfoHash", "RawTitle", "ParsedTitle", "ImdbId", "Category",
                    "Year", "Resolution", "Quality", "Codec", "Size",
                    "IngestedAt", "Seasons", "Complete", "Audio"
                FROM "Torrents"
                WHERE ("RawTitle" ILIKE %s OR "ParsedTitle" ILIKE %s)
            """
            params = [search_term, search_term]
            
            if category:
                sql += ' AND "Category" = %s'
                params.append(category)
            
            sql += ' ORDER BY "IngestedAt" DESC LIMIT %s'
            params.append(limit)
            
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                
                results = []
                for row in rows:
                    torrent = self._row_to_dict(row)
                    result = self._build_torrent_result(torrent)
                    if result is not None:
                        results.append(result)
                
                logger.info("Zilean found %d torrents for: %s", len(results), query)
                return results
                
        except Exception as e:
            logger.debug("Zilean search traceback for query '%s':", query, exc_info=True)
            if psycopg is not None and isinstance(e, psycopg.Error):
                logger.warning("Zilean database error for query '%s': %s", query, e)
            else:
                logger.warning("Zilean search error for query '%s': %s", query, e)
            return []


class ProwlarrClient:
    """Prowlarr API client for searching torrents.
    
    Connects to local Prowlarr instance for torrent search.
    Acts as a fallback when Zilean database is not configured.
    
    API: http://prowlarr-ingest:9696/api/v1
    Rate limit: 0.5 req/sec (respectful to indexers)
    """
    
    def __init__(self, api_key: Optional[str] = None, base_url: str = "http://prowlarr-ingest:9696"):
        """Initialize Prowlarr client.
        
        Args:
            api_key: Prowlarr API key (from PROWLARR_API_KEY env var)
            base_url: Prowlarr base URL (default: http://prowlarr-ingest:9696)
        """
        self.api_key = api_key or get_env().get("PROWLARR_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        
        self.client = httpx.Client(
            base_url=self.base_url,
            headers={
                "X-Api-Key": self.api_key,
                "User-Agent": "torboxed/1.0",
                "Accept": "application/json",
            },
            timeout=DEFAULT_TIMEOUT_LONG
        )
    
    def is_configured(self) -> bool:
        """Check if Prowlarr is configured (has API key)."""
        return bool(self.api_key)
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
    
    def _extract_infohash(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract infohash from Prowlarr result using shared helper."""
        return extract_infohash_from_item(
            item,
            hash_fields=["infoHash"],
            magnet_fields=["magnetUrl"],
            validate_guid=False
        )
    
    def search(self, query: str, categories: Optional[List[int]] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for torrents via Prowlarr.
        
        Args:
            query: Search query string (title + year recommended)
            categories: Optional list of category IDs (2000=movies, 5000=tv)
            limit: Maximum number of results to return
            
        Returns:
            List of torrent dicts with infoHash, title, size, etc.
        """
        if not query or not self.is_configured():
            return []
        
        try:
            # Rate limiting
            prowlarr_limiter.wait()
            
            # Build search params
            params: Dict[str, Any] = {
                "query": query,
                "limit": limit,
            }
            if categories:
                params["categories"] = ",".join(str(c) for c in categories)
            
            url = f"{self.base_url}/api/v1/search"
            response = self.client.get(url, params=params)
            
            # Mark successful request for rate limiting
            prowlarr_limiter.mark_success()
            
            if response.status_code == 429:
                logger.warning("Prowlarr rate limited for: %s", query)
                return []
            
            if response.status_code != 200:
                logger.debug("Prowlarr returned %d for: %s", response.status_code, query)
                return []
            
            data = response.json()
            
            # Response is a list of results
            if not isinstance(data, list):
                logger.debug("Prowlarr returned unexpected format for: %s", query)
                return []
            
            results = []
            for item in data[:limit]:
                infohash = self._extract_infohash(item)
                if not infohash:
                    logger.debug("Skipping Prowlarr result without infohash: %s", item.get("title", "unknown"))
                    continue
                
                name = item.get("title", "Unknown")
                
                # Parse size (in bytes)
                try:
                    size = int(item.get("size", 0))
                except (ValueError, TypeError):
                    size = 0
                
                # Parse seeders/leechers
                try:
                    seeders = int(item.get("seeders", 0))
                    leechers = int(item.get("leechers", 0))
                except (ValueError, TypeError):
                    seeders = 0
                    leechers = 0
                
                # Build magnet link
                magnet_url = item.get("magnetUrl", "")
                
                results.append(build_search_result(
                    name=name,
                    infohash=infohash,
                    magnet_link=magnet_url,
                    size=size,
                    seeders=seeders,
                    leechers=leechers,
                    source=item.get("indexer", "prowlarr"),
                ))
            
            if results:
                logger.info("Prowlarr found %d torrents for: %s", len(results), query)
            else:
                logger.debug("Prowlarr returned 0 valid torrents for: %s", query)
            
            return results
            
        except httpx.TimeoutException:
            logger.warning("Prowlarr timeout for: %s", query)
            return []
        except httpx.RequestError as e:
            logger.debug("Prowlarr request traceback for '%s':", query, exc_info=True)
            logger.warning("Prowlarr request error for '%s': %s", query, e)
            return []
        except (OSError, ValueError) as e:
            logger.debug("Prowlarr error traceback for '%s':", query, exc_info=True)
            logger.warning("Prowlarr error for '%s': %s", query, e)
            return []


class JackettClient:
    """Jackett API client for searching torrents.
    
    Connects to local Jackett instance for torrent search.
    Acts as a fallback when Zilean database and Prowlarr are not configured.
    
    API: http://localhost:9117/api/v2.0/indexers/all/results
    Rate limit: 0.5 req/sec (respectful to indexers)
    """
    
    def __init__(self, api_key: Optional[str] = None, base_url: str = "http://localhost:9117"):
        """Initialize Jackett client.
        
        Args:
            api_key: Jackett API key (from JACKETT_API_KEY env var)
            base_url: Jackett base URL (default: http://localhost:9117)
        """
        self.api_key = api_key or get_env().get("JACKETT_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        
        self.client = httpx.Client(
            base_url=self.base_url,
            headers={
                "User-Agent": "torboxed/1.0",
                "Accept": "application/json",
            },
            timeout=DEFAULT_TIMEOUT_LONG
        )
    
    def is_configured(self) -> bool:
        """Check if Jackett is configured (has API key)."""
        return bool(self.api_key)
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
    
    def _extract_infohash(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract infohash from Jackett result using shared helper."""
        return extract_infohash_from_item(
            item,
            hash_fields=["infohash", "InfoHash"],
            magnet_fields=["magnetUrl", "MagnetUri", "link"],
            validate_guid=True
        )
    
    def search(self, query: str, categories: Optional[List[int]] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Search for torrents via Jackett.
        
        Args:
            query: Search query string (title + year recommended)
            categories: Optional list of category IDs (2000=movies, 5000=tv)
            limit: Maximum number of results to return
            
        Returns:
            List of torrent dicts with infoHash, title, size, etc.
        """
        if not query or not self.is_configured():
            return []
        
        try:
            # Rate limiting
            jackett_limiter.wait()
            
            # Build search params
            params: Dict[str, Any] = {
                "apikey": self.api_key,
                "Query": query,
            }
            
            # Map categories to Jackett format if provided
            if categories:
                # Jackett uses comma-separated category list
                params["category"] = ",".join(str(c) for c in categories)
            
            url = f"{self.base_url}/api/v2.0/indexers/all/results"
            response = self.client.get(url, params=params)
            
            # Mark successful request for rate limiting
            jackett_limiter.mark_success()
            
            if response.status_code == 429:
                logger.warning("Jackett rate limited for: %s", query)
                return []
            
            if response.status_code != 200:
                logger.debug("Jackett returned %d for: %s", response.status_code, query)
                return []
            
            data = response.json()
            
            # Response has "Results" key containing list of results
            results_data = data.get("Results", []) if isinstance(data, dict) else []
            
            if not isinstance(results_data, list):
                logger.debug("Jackett returned unexpected format for: %s", query)
                return []
            
            results = []
            for item in results_data[:limit]:
                infohash = self._extract_infohash(item)
                if not infohash:
                    logger.debug("Skipping Jackett result without infohash: %s", item.get("Title", item.get("title", "unknown")))
                    continue
                
                name = item.get("Title") or item.get("title", "Unknown")
                
                # Parse size (in bytes)
                try:
                    size = int(item.get("Size", 0))
                except (ValueError, TypeError):
                    size = 0
                
                # Parse seeders/leechers
                try:
                    seeders = int(item.get("Seeders", 0))
                    leechers = int(item.get("Peers", 0)) - seeders
                    if leechers < 0:
                        leechers = 0
                except (ValueError, TypeError):
                    seeders = 0
                    leechers = 0
                
                # Build magnet link
                magnet_url = item.get("MagnetUri") or item.get("magnetUrl", "")
                if not magnet_url:
                    link = item.get("Link", "")
                    if link and link.startswith("magnet:"):
                        magnet_url = link
                
                # Get indexer name
                indexer = item.get("Tracker") or item.get("TrackerId") or item.get("indexer", "jackett")
                
                results.append(build_search_result(
                    name=name,
                    infohash=infohash,
                    magnet_link=magnet_url,
                    size=size,
                    seeders=seeders,
                    leechers=leechers,
                    source=indexer.lower() if isinstance(indexer, str) else "jackett",
                ))
            
            if results:
                logger.info("Jackett found %d torrents for: %s", len(results), query)
            else:
                logger.debug("Jackett returned 0 valid torrents for: %s", query)
            
            return results
            
        except httpx.TimeoutException:
            logger.warning("Jackett timeout for: %s", query)
            return []
        except httpx.RequestError as e:
            logger.debug("Jackett request traceback for '%s':", query, exc_info=True)
            logger.warning("Jackett request error for '%s': %s", query, e)
            return []
        except (OSError, ValueError) as e:
            logger.debug("Jackett error traceback for '%s':", query, exc_info=True)
            logger.warning("Jackett error for '%s': %s", query, e)
            return []


# Global rate limiters
torbox_limiter = RateLimiter(TORBOX_RATE_LIMIT, name="Torbox")
torbox_creation_limiter = RateLimiter(TORBOX_CREATION_LIMIT, name="Torbox-Creation")
trakt_limiter = RateLimiter(TRAKT_RATE_LIMIT, name="Trakt")
prowlarr_limiter = RateLimiter(2.0, name="Prowlarr")  # 0.5 req/sec to be respectful
jackett_limiter = RateLimiter(2.0, name="Jackett")  # 0.5 req/sec to be respectful


# Global logger instance
logger = logging.getLogger("torboxed")


# =============================================================================
# TELEGRAM NOTIFICATIONS
# =============================================================================

class TelegramNotifier:
    """Telegram Bot API client for sending sync notifications.
    
    Sends notifications for:
    - New content added to Torbox
    - Quality upgrades
    - Sync summaries
    - Errors (optional)
    
    Rate limited to 1 message per second to respect Telegram's API.
    Gracefully degrades if Telegram is unavailable - logs warning but
    doesn't block the sync process.
    
    Configuration via environment variables:
    - TELEGRAM_BOT_TOKEN: Bot token from @BotFather
    - TELEGRAM_CHAT_ID: Chat ID to send messages to
    
    Notification settings via telegram_settings dict:
    - notify_added: New content added (default: True)
    - notify_upgraded: Quality upgrade (default: False)
    - notify_summary: Sync summary (default: True)
    - notify_error: Error notification (default: True)
    """
    
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None,
                 telegram_settings: Optional[Dict[str, Any]] = None):
        """Initialize Telegram notifier.
        
        Args:
            bot_token: Telegram bot token (or from TELEGRAM_BOT_TOKEN env var)
            chat_id: Telegram chat ID (or from TELEGRAM_CHAT_ID env var)
            telegram_settings: Dict with notification preferences
        """
        self.bot_token = bot_token or get_env().get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or get_env().get("TELEGRAM_CHAT_ID", "")
        self._rate_limiter = RateLimiter(1.0, name="Telegram")  # 1 msg/sec
        self._client: Optional[httpx.Client] = None
        
        # Default: notify on added, summary, error; NOT on upgraded
        self.settings = telegram_settings or {
            "notify_added": True,
            "notify_upgraded": False,
            "notify_summary": True,
            "notify_error": True
        }
    
    def is_configured(self) -> bool:
        """Check if Telegram is configured (has bot token and chat ID)."""
        return bool(self.bot_token and self.chat_id)
    
    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url="https://api.telegram.org",
                timeout=DEFAULT_TIMEOUT_SHORT,
                headers={"User-Agent": "torboxed/1.0"}
            )
        return self._client
    
    def close(self):
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None
    
    def _send_message(self, text: str) -> bool:
        """Send a message to Telegram.
        
        Args:
            text: Message text to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            return False
        
        try:
            # Rate limit
            self._rate_limiter.wait()
            
            client = self._get_client()
            url = f"/bot{self.bot_token}/sendMessage"
            
            response = client.post(url, json={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            })
            
            if response.status_code == 200:
                self._rate_limiter.mark_success()
                logger.debug("Telegram notification sent successfully")
                return True
            else:
                error_data = response.json() if response.text else {}
                error_msg = error_data.get("description", f"HTTP {response.status_code}")
                logger.warning("Failed to send Telegram notification: %s", error_msg)
                return False
                
        except httpx.TimeoutException:
            logger.warning("Telegram notification timeout")
            return False
        except httpx.RequestError as e:
            logger.debug("Telegram notification traceback:", exc_info=True)
            logger.warning("Telegram request error: %s", e)
            return False
        except (OSError, ValueError) as e:
            logger.debug("Telegram notification traceback:", exc_info=True)
            logger.warning("Error sending Telegram notification: %s", e)
            return False
    
    def notify_added(self, title: str, year: int, quality_label: str, 
                     quality_score: int, content_type: str = "movie",
                     season: Optional[str] = None, source: str = "",
                     imdb_id: Optional[str] = None) -> bool:
        """Notify that new content was added.
        
        Args:
            title: Content title
            year: Release year
            quality_label: Quality description (e.g., "1080p BluRay")
            quality_score: Quality score
            content_type: 'movie' or 'show'
            season: Season info for TV shows (e.g., "S01")
            source: Trakt source that triggered the add
            imdb_id: IMDb ID for linking (e.g., "tt0137523")
            
        Returns:
            True if notification sent, False otherwise
        """
        if not self.settings.get("notify_added", True):
            logger.debug("Telegram notify_added disabled by settings")
            return False
        
        type_str = "Movie" if content_type == "movie" else "TV Show"
        title_str = f"{title} ({year})"
        if season and season != "unknown":
            title_str = f"{title} [{season}] ({year})"
        
        source_str = f"\nSource: {source}" if source else ""
        
        # Add IMDb link if available
        imdb_str = ""
        if imdb_id:
            imdb_str = f"\n<a href=\"https://www.imdb.com/title/{imdb_id}/\">View on IMDb</a>"
        
        text = f"TorBoxed: Added {type_str}\n\n<b>{title_str}</b>\nQuality: {quality_label} (Score: {quality_score}){source_str}{imdb_str}"
        return self._send_message(text)
    
    def notify_upgraded(self, title: str, year: int, old_quality: str, old_score: int,
                        new_quality: str, new_score: int, content_type: str = "movie",
                        season: Optional[str] = None) -> bool:
        """Notify that content was upgraded to better quality.
        
        Args:
            title: Content title
            year: Release year
            old_quality: Previous quality label
            old_score: Previous quality score
            new_quality: New quality label
            new_score: New quality score
            content_type: 'movie' or 'show'
            season: Season info for TV shows
            
        Returns:
            True if notification sent, False otherwise
        """
        if not self.settings.get("notify_upgraded", False):
            logger.debug("Telegram notify_upgraded disabled by settings")
            return False
        
        type_str = "movie" if content_type == "movie" else "TV show"
        title_str = f"{title} ({year})"
        if season and season != "unknown":
            title_str = f"{title} [{season}] ({year})"
        
        text = (f"TorBoxed: Upgraded {type_str}\n\n"
                f"{title_str}\n\n"
                f"Quality improved:\n"
                f"  {old_quality} (Score: {old_score})\n"
                f"  -> {new_quality} (Score: {new_score})")
        return self._send_message(text)
    
    def notify_summary(self, added: int, upgraded: int, skipped: int, failed: int,
                       duration_seconds: float, movies: int = 0, shows: int = 0) -> bool:
        """Send sync summary notification.
        
        Args:
            added: Number of items added
            upgraded: Number of items upgraded
            skipped: Number of items skipped
            failed: Number of items that failed
            duration_seconds: How long the sync took
            movies: Number of movies processed
            shows: Number of TV shows processed
            
        Returns:
            True if notification sent, False otherwise
        """
        if not self.settings.get("notify_summary", True):
            logger.debug("Telegram notify_summary disabled by settings")
            return False
        
        duration_mins = int(duration_seconds // 60)
        duration_secs = int(duration_seconds % 60)
        duration_str = f"{duration_mins}m {duration_secs}s" if duration_mins > 0 else f"{duration_secs}s"
        
        content_parts = []
        if movies > 0:
            content_parts.append(f"{movies} movie{'s' if movies != 1 else ''}")
        if shows > 0:
            content_parts.append(f"{shows} TV season{'s' if shows != 1 else ''}")
        
        content_str = f" ({', '.join(content_parts)})" if content_parts else ""
        
        text = (f"TorBoxed: Sync Complete{content_str}\n\n"
                f"Results:\n"
                f"  Added: {added}\n"
                f"  Upgraded: {upgraded}\n"
                f"  Skipped: {skipped}\n"
                f"  Failed: {failed}\n\n"
                f"Duration: {duration_str}")
        return self._send_message(text)
    
    def notify_error(self, error_message: str, context: str = "") -> bool:
        """Notify about an error (optional, for critical failures).
        
        Args:
            error_message: Error description
            context: Additional context about what failed
            
        Returns:
            True if notification sent, False otherwise
        """
        if not self.settings.get("notify_error", True):
            logger.debug("Telegram notify_error disabled by settings")
            return False
        
        context_str = f"\nContext: {context}" if context else ""
        text = f"TorBoxed: Error{context_str}\n\n{error_message}"
        return self._send_message(text)


# Global Telegram notifier instance (lazy-initialized)
_telegram_notifier: Optional[TelegramNotifier] = None


def get_telegram_notifier(telegram_settings: Optional[Dict[str, Any]] = None) -> TelegramNotifier:
    """Get or create Telegram notifier instance (lazy-loaded).
    
    Args:
        telegram_settings: Optional dict with notification preferences.
                         If provided, creates new instance with these settings.
    
    Returns:
        TelegramNotifier instance
    """
    global _telegram_notifier
    if _telegram_notifier is None or telegram_settings is not None:
        _telegram_notifier = TelegramNotifier(telegram_settings=telegram_settings)
    return _telegram_notifier


def get_telegram_bot_token() -> Optional[str]:
    """Get Telegram bot token (lazy-loaded)."""
    return get_env().get("TELEGRAM_BOT_TOKEN")


def get_telegram_chat_id() -> Optional[str]:
    """Get Telegram chat ID (lazy-loaded)."""
    return get_env().get("TELEGRAM_CHAT_ID")


def setup_logging(verbose: bool = False, log_to_file: bool = True) -> logging.Logger:
    """Configure logging with console and optional rotating file handlers.

    Args:
        verbose: Enable DEBUG level for console output
        log_to_file: Enable rotating file handler

    Returns:
        Configured logger instance
    """
    log = logging.getLogger("torboxed")
    log.setLevel(logging.DEBUG)
    log.handlers = []  # Clear existing handlers

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_level = logging.DEBUG if verbose else logging.INFO
    console_handler.setLevel(console_level)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    log.addHandler(console_handler)

    # Rotating file handler with rate limiting (VULN-007)
    if log_to_file:
        file_handler = RateLimitedLogHandler(
            LOG_PATH,
            maxBytes=MAX_LOG_SIZE,
            backupCount=MAX_LOG_BACKUPS,
            encoding="utf-8",
            max_repeats=100,  # Allow up to 100 similar messages per minute
            window_seconds=60
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_format)
        log.addHandler(file_handler)

    log.debug("Logging initialized (verbose=%s, file=%s)", verbose, log_to_file)
    return log


def log_result(action: str, title: str, details: Dict[str, Any] = None):
    """Log a sync result with consistent formatting.

    Args:
        action: The action performed (added, upgraded, skipped, failed)
        title: Content title
        details: Additional context to log
    """
    details = details or {}
    detail_str = " | ".join(f"{k}={v}" for k, v in details.items())
    if detail_str:
        logger.info("[%s] %s | %s", action.upper(), title, detail_str)
    else:
        logger.info("[%s] %s", action.upper(), title)


def load_env() -> Dict[str, str]:
    """Parse .env file manually (no python-dotenv dependency)."""
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key] = value
    return env


# Global cache for lazy-loaded API keys
_env_cache: Optional[Dict[str, str]] = None


def get_env() -> Dict[str, str]:
    """Lazy-load environment variables from .env file."""
    global _env_cache
    if _env_cache is None:
        _env_cache = load_env()
    return _env_cache


def get_torbox_key() -> Optional[str]:
    """Get Torbox API key (lazy-loaded)."""
    return get_env().get("TORBOX_API_KEY")


def get_real_debrid_key() -> Optional[str]:
    """Get Real Debrid API key (lazy-loaded)."""
    return get_env().get("REAL_DEBRID_API_KEY")


def get_debrid_service() -> str:
    """Get the configured debrid service name (lazy-loaded).

    Returns:
        "torbox" or "real_debrid". Defaults to "torbox".
    """
    service = get_env().get("DEBRID_SERVICE", "torbox").lower().strip()
    if service not in ("torbox", "real_debrid"):
        logger.warning("Unknown DEBRID_SERVICE '%s', defaulting to 'torbox'", service)
        return "torbox"
    return service


def get_trakt_id() -> Optional[str]:
    """Get Trakt Client ID (lazy-loaded)."""
    return get_env().get("TRAKT_CLIENT_ID")


def get_trakt_secret() -> Optional[str]:
    """Get Trakt Client Secret (lazy-loaded)."""
    return get_env().get("TRAKT_CLIENT_SECRET")


def get_trakt_access_token() -> Optional[str]:
    """Get Trakt Access Token for authenticated API calls (lazy-loaded)."""
    return get_env().get("TRAKT_ACCESS_TOKEN")


@contextmanager
def get_db():
    """Context manager for database connections with error handling."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        yield conn
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def migrate_db():
    """Migrate database schema.
    
    VULN-008 Fix: Creates timestamped backup before migration for data safety.
    
    Handles:
    - Multi-season tracking (season column)
    - Telegram notification settings (telegram_settings column)
    
    Returns:
        True if any migration was performed, False if already up to date
    """
    with get_db() as conn:
        migrations_performed = False
        
        # Migration 1: Check if season column exists in processed table
        cursor = conn.execute("PRAGMA table_info(processed)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "season" not in columns:
            # VULN-008: Create timestamped backup before migration
            backup_path = DB_PATH.parent / f"torboxed.db.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            try:
                shutil.copy2(DB_PATH, backup_path)
                logger.info("Created database backup: %s", backup_path)
            except (OSError, shutil.Error) as e:
                logger.debug("Backup creation traceback:", exc_info=True)
                logger.error("Failed to create backup, aborting migration: %s", e)
                return False
            
            logger.info("Migrating database schema to support multi-season tracking...")
            
            try:
                # Start transaction for migration
                conn.execute("BEGIN TRANSACTION")
                
                # Backup existing data
                conn.execute("ALTER TABLE processed RENAME TO processed_backup")
                
                # Create new table with season column
                conn.execute('''
                    CREATE TABLE processed (
                        imdb_id TEXT NOT NULL,
                        season TEXT DEFAULT 'unknown',
                        title TEXT NOT NULL,
                        year INTEGER,
                        content_type TEXT CHECK(content_type IN ('movie', 'show')),
                        action TEXT CHECK(action IN ('added', 'upgraded', 'skipped', 'failed')),
                        reason TEXT,
                        torbox_id TEXT,
                        magnet TEXT,
                        quality_score INTEGER,
                        quality_label TEXT,
                        replaced_id TEXT,
                        replaced_score INTEGER,
                        processed_at TEXT,
                        PRIMARY KEY (imdb_id, season)
                    )
                ''')
                
                # Migrate data - set season to 'unknown' for all existing records
                conn.execute('''
                    INSERT INTO processed 
                    (imdb_id, season, title, year, content_type, action, reason, 
                     torbox_id, magnet, quality_score, quality_label, replaced_id, 
                     replaced_score, processed_at)
                    SELECT 
                        imdb_id, 'unknown', title, year, content_type, action, reason,
                        torbox_id, magnet, quality_score, quality_label, replaced_id,
                        replaced_score, processed_at
                    FROM processed_backup
                ''')
                
                # Recreate indexes
                conn.execute("CREATE INDEX IF NOT EXISTS idx_action ON processed(action)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON processed(content_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_processed_at ON processed(processed_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_imdb_id ON processed(imdb_id)")
                
                # Drop backup table
                conn.execute("DROP TABLE processed_backup")
                
                conn.commit()
                logger.info("Database migration completed successfully")
                migrations_performed = True
                
                # VULN-008: Only remove backup after successful commit
                try:
                    backup_path.unlink()
                    logger.info("Removed temporary backup: %s", backup_path)
                except (OSError, PermissionError) as e:
                    logger.debug("Backup removal traceback:", exc_info=True)
                    logger.warning("Could not remove backup file %s: %s", backup_path, e)
                
            except (sqlite3.Error, OSError) as e:
                # Rollback on error
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                logger.debug("Migration traceback:", exc_info=True)
                logger.error("Migration failed: %s", e)
                logger.info("Database backup preserved at: %s", backup_path)
                raise
        
        # Migration 2: Check if telegram_settings column exists in config table
        cursor = conn.execute("PRAGMA table_info(config)")
        config_columns = [row[1] for row in cursor.fetchall()]
        
        if "telegram_settings" not in config_columns:
            logger.info("Migrating database schema to add telegram_settings...")
            try:
                conn.execute("ALTER TABLE config ADD COLUMN telegram_settings TEXT")
                conn.commit()
                logger.info("Added telegram_settings column to config table")
                migrations_performed = True
            except sqlite3.Error as e:
                logger.debug("Telegram settings migration traceback:", exc_info=True)
                logger.error("Failed to add telegram_settings column: %s", e)
                # Don't raise - this is non-critical
        
        return migrations_performed


def init_db():
    """Create tables if not exist, insert default config."""
    with get_db() as conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                sources TEXT,
                limits TEXT,
                quality_prefs TEXT,
                filters TEXT,
                telegram_settings TEXT
            );
            
            CREATE TABLE IF NOT EXISTS processed (
                imdb_id TEXT NOT NULL,
                season TEXT DEFAULT 'unknown',
                title TEXT NOT NULL,
                year INTEGER,
                content_type TEXT CHECK(content_type IN ('movie', 'show')),
                action TEXT CHECK(action IN ('added', 'upgraded', 'skipped', 'failed')),
                reason TEXT,
                torbox_id TEXT,
                magnet TEXT,
                quality_score INTEGER,
                quality_label TEXT,
                replaced_id TEXT,
                replaced_score INTEGER,
                processed_at TEXT,
                PRIMARY KEY (imdb_id, season)
            );
            
            CREATE INDEX IF NOT EXISTS idx_action ON processed(action);
            CREATE INDEX IF NOT EXISTS idx_type ON processed(content_type);
            CREATE INDEX IF NOT EXISTS idx_processed_at ON processed(processed_at);
            CREATE INDEX IF NOT EXISTS idx_imdb_id ON processed(imdb_id);
            
            -- Insert default config if empty
            INSERT OR IGNORE INTO config (id, sources, limits, quality_prefs, filters, telegram_settings)
            VALUES (1, 
                '["movies/trending", "movies/popular", "movies/watched/weekly", "movies/watched/monthly", "movies/watched/yearly", "movies/watched/all", "movies/collected/weekly", "movies/collected/monthly", "movies/collected/yearly", "movies/collected/all", "movies/anticipated", "movies/boxoffice", "shows/trending", "shows/popular", "shows/watched/weekly", "shows/watched/monthly", "shows/watched/yearly", "shows/watched/all", "shows/collected/weekly", "shows/collected/monthly", "shows/collected/yearly", "shows/collected/all", "shows/anticipated"]',
                '{}',
                '{"preferred": "1080p", "min_seeds": 5}',
                '{"min_year": 1900, "exclude": ["CAM", "TS", "HDCAM"], "min_resolution_score": 800}',
                '{"notify_added": true, "notify_upgraded": false, "notify_summary": true, "notify_error": true}'
            );
        ''')
        conn.commit()
    
    # Check if migration needed
    migrate_db()
    
    logger.info("Database initialized successfully.")


def get_config() -> Dict[str, Any]:
    """Load app config from database."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM config WHERE id=1").fetchone()
        if not row:
            return {}
        
        # Default telegram settings: notify on added, summary, error; NOT on upgraded
        default_telegram_settings = {
            "notify_added": True,
            "notify_upgraded": False,
            "notify_summary": True,
            "notify_error": True
        }
        
        telegram_settings = json.loads(row["telegram_settings"]) if row["telegram_settings"] else default_telegram_settings
        
        return {
            "sources": json.loads(row["sources"]) if row["sources"] else [],
            "limits": json.loads(row["limits"]) if row["limits"] else {},
            "quality": json.loads(row["quality_prefs"]) if row["quality_prefs"] else {},
            "filters": json.loads(row["filters"]) if row["filters"] else {},
            "telegram": telegram_settings,
        }


def is_processed(imdb_id: str, season: str = "unknown") -> bool:
    """Check if we've already handled this item.
    
    For TV shows, checks the specific season. For movies, season defaults to 'unknown'.
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM processed WHERE imdb_id=? AND season=?", (imdb_id, season)
        ).fetchone()
        return row is not None


def get_processed_item(imdb_id: str, season: str = "unknown") -> Optional[Dict[str, Any]]:
    """Get details of a processed item if it exists.
    
    For TV shows, gets the specific season record.
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM processed WHERE imdb_id=? AND season=?", (imdb_id, season)
        ).fetchone()
        if row:
            return dict(row)
        return None


def get_processed_show_seasons(imdb_id: str) -> List[Dict[str, Any]]:
    """Get all processed seasons for a TV show.
    
    Returns:
        List of all season records for the given IMDB ID.
    """
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM processed WHERE imdb_id=? AND content_type='show'",
            (imdb_id,)
        ).fetchall()
        return [dict(row) for row in rows]


def record_processed(imdb_id: str, title: str, year: int, content_type: str,
                     action: str, reason: str, torbox_id: Optional[str] = None,
                     magnet: Optional[str] = None, quality_score: Optional[int] = None,
                     quality_label: Optional[str] = None, replaced_id: Optional[str] = None,
                     replaced_score: Optional[int] = None, season: str = "unknown"):
    """Record what we did.
    
    For TV shows, includes season identifier to track each season separately.
    """
    with get_db() as conn:
        conn.execute('''
            INSERT OR REPLACE INTO processed 
            (imdb_id, season, title, year, content_type, action, reason, torbox_id, magnet, 
             quality_score, quality_label, replaced_id, replaced_score, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (imdb_id, season, title, year, content_type, action, reason, torbox_id, magnet,
              quality_score, quality_label, replaced_id, replaced_score, datetime.now(timezone.utc).isoformat()))
        conn.commit()


def reset_item(imdb_id: str, season: Optional[str] = None) -> int:
    """Remove item from processed to force re-processing.
    
    Args:
        imdb_id: The IMDB ID to reset
        season: Optional specific season to reset. If None, resets ALL seasons for shows.
        
    Returns:
        Number of records deleted
    """
    with get_db() as conn:
        if season:
            cursor = conn.execute("DELETE FROM processed WHERE imdb_id=? AND season=?", 
                                (imdb_id, season))
        else:
            # Delete all records for this IMDB ID (handles both movies and all show seasons)
            cursor = conn.execute("DELETE FROM processed WHERE imdb_id=?", (imdb_id,))
        conn.commit()
        return cursor.rowcount


def get_stats() -> Dict[str, Any]:
    """Get statistics about processed items."""
    with get_db() as conn:
        stats = {}
        
        # Count by action
        rows = conn.execute("""
            SELECT action, COUNT(*) as count FROM processed GROUP BY action
        """).fetchall()
        stats["by_action"] = {row["action"]: row["count"] for row in rows}
        
        # Count by type
        rows = conn.execute("""
            SELECT content_type, COUNT(*) as count FROM processed GROUP BY content_type
        """).fetchall()
        stats["by_type"] = {row["content_type"]: row["count"] for row in rows}
        
        # Total
        total = conn.execute("SELECT COUNT(*) as count FROM processed").fetchone()
        stats["total"] = total["count"] if total else 0
        
        # Recent upgrades
        upgrades = conn.execute("""
            SELECT * FROM processed 
            WHERE action='upgraded' 
            ORDER BY processed_at DESC 
            LIMIT 5
        """).fetchall()
        stats["recent_upgrades"] = [dict(row) for row in upgrades]
        
        return stats


def get_recent(limit: int = 10) -> List[Dict[str, Any]]:
    """Get last N processed items."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM processed 
            ORDER BY processed_at DESC 
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(row) for row in rows]


# ============================================================================
# QUALITY SCORING
# ============================================================================

RESOLUTION_SCORES = {
    "2160p": 4000, "4K": 4000, "UHD": 4000,
    "1440p": 3000, "2K": 3000,
    "1080p": 2500, "1080i": 2400,
    "720p": 1500,
    "576p": 1000, "PAL": 1000,
    "480p": 800, "NTSC": 800,
    "360p": 400,
    "240p": 200,
}

# Maximum quality threshold - if score >= this, consider it "maxed out"
MAX_QUALITY_SCORE = 7000  # 2160p (4000) + Blu-ray (1000) + HEVC (600) + DTS-HD MA (650) = 6250
# Round up to 7000 to account for variations (AV1 = 800, BDRemux = 1050, etc.)

def is_max_quality(quality_score: int) -> bool:
    """Check if quality score indicates maximum quality (no need to upgrade)."""
    return quality_score >= MAX_QUALITY_SCORE


def normalize_search_query(query: str) -> str:
    """Normalize search query by removing punctuation and accents.
    
    Removes colons, hyphens, commas, and accent characters to improve
    search matching against torrent titles that may have different formatting.
    
    Args:
        query: Original search query (e.g., "Spider-Man: Beyond the Spider-Verse")
        
    Returns:
        Normalized query (e.g., "Spider Man Beyond the Spider Verse")
    """
    # Remove accent characters by decomposing and removing combining marks
    query = ''.join(
        c for c in unicodedata.normalize('NFKD', query)
        if not unicodedata.combining(c)
    )
    # Remove specific punctuation characters
    for char in ':,-':
        query = query.replace(char, ' ')
    # Replace multiple spaces with single space and strip
    return ' '.join(query.split())

SOURCE_SCORES = {
    "Blu-ray": 1000, "BluRay": 1000, "BD": 1000, "BDRemux": 1050,
    "WEB-DL": 900, "WEB": 900, "Web": 900, "WEBRip": 800,
    "HDTV": 700, "HDTVRip": 650,
    "DVD": 500, "DVDRip": 450,
    "HDRip": 400,
    "BRRip": 350,
    "Camera": 100, "CAM": 100, "HDCAM": 100, "HD Camera": 100,
    "Telesync": 50, "TS": 50, "TC": 75, "Telecine": 75,
}

CODEC_SCORES = {
    "AV1": 800,
    "H.265": 600, "HEVC": 600, "x265": 600, "H265": 600,
    "H.264": 500, "AVC": 500, "x264": 500, "H264": 500,
    "MPEG-2": 300,
    "XviD": 200, "DivX": 200,
    "MPEG-1": 100,
}

AUDIO_SCORES = {
    "DTS-HD MA": 650, "DTS-HD Master Audio": 650,
    "DTS-HD": 600,
    "TrueHD": 550, "Dolby TrueHD": 550,
    "DTS-ES": 450,
    "DTS": 400,
    "DD+": 350, "E-AC-3": 350, "Dolby Digital Plus": 350,
    "AC-3": 300, "DD": 300, "Dolby Digital": 300,
    "AAC": 250,
    "MP3": 100,
}


def parse_quality(torrent_name: str) -> QualityInfo:
    """Parse quality information from torrent name using GuessIt."""
    parsed = guessit(torrent_name)
    
    resolution = parsed.get("screen_size", "Unknown")
    source = parsed.get("source", "Unknown")
    codec = parsed.get("video_codec", "Unknown")
    audio = parsed.get("audio_codec", "Unknown")
    
    # Handle cases where GuessIt returns lists instead of strings
    if isinstance(resolution, list):
        resolution = resolution[0] if resolution else "Unknown"
    if isinstance(source, list):
        source = source[0] if source else "Unknown"
    if isinstance(codec, list):
        codec = codec[0] if codec else "Unknown"
    if isinstance(audio, list):
        audio = audio[0] if audio else "Unknown"
    
    # Calculate score
    score = 0
    score += RESOLUTION_SCORES.get(resolution, 0)
    score += SOURCE_SCORES.get(source, 0)
    score += CODEC_SCORES.get(codec, 0)
    score += AUDIO_SCORES.get(audio, 0)
    
    # Create human-readable label
    parts = []
    if resolution != "Unknown":
        parts.append(resolution)
    if source != "Unknown":
        parts.append(source)
    if codec != "Unknown":
        parts.append(codec)
    label = " ".join(parts) if parts else "Unknown"
    
    return QualityInfo(
        resolution=resolution,
        source=source,
        codec=codec,
        audio=audio,
        score=score,
        label=label
    )


def parse_season_info(torrent_name: str) -> Optional[SeasonInfo]:
    """Parse season information from torrent name using GuessIt.
    
    Handles various season formats:
    - Show.S01.1080p... → Season pack (is_pack=True)
    - Show.S01E01.1080p... → Individual episode (is_pack=False)
    - Show.S01-S05.1080p... → Seasons 1-5 (multi-season pack, is_pack=True)
    - Show.Complete.1080p... → All seasons (is_pack=True)
    - Show.Season.1.1080p... → Season 1 (alternative format, is_pack=True)
    
    Args:
        torrent_name: The torrent name to parse
        
    Returns:
        SeasonInfo with parsed season data, or None if not a TV show/no season info
        
    Note:
        Individual episodes (S01E01) are detected but marked as is_pack=False.
        The sync logic can then decide whether to include episodes or wait for packs.
    """
    parsed = guessit(torrent_name)
    
    # Check if this is a TV show (has season or episode info)
    season_data = parsed.get("season")
    episode_data = parsed.get("episode")
    
    # Check for "Complete" in the name (complete series pack)
    name_lower = torrent_name.lower()
    complete_keywords = ['complete', 'full series', 'entire series', 'all seasons']
    has_complete_keyword = any(keyword in name_lower for keyword in complete_keywords)
    
    # If no season, episode, or complete keyword, it's likely a movie
    if season_data is None and episode_data is None and not has_complete_keyword:
        return None
    
    # Handle season data
    seasons = []
    is_complete = False
    is_pack = True  # Default to pack unless we detect individual episode
    episode = None
    
    if season_data is not None:
        # GuessIt returns either a single int or a list of ints
        if isinstance(season_data, int):
            seasons = [season_data]
        elif isinstance(season_data, list):
            seasons = sorted([s for s in season_data if isinstance(s, int)])
    elif episode_data is not None:
        # Has episode but no explicit season - assume Season 1
        seasons = [1]
    
    # Check if this is an individual episode vs season pack
    if episode_data is not None:
        is_pack = False  # Individual episode
        if isinstance(episode_data, int):
            episode = episode_data
        elif isinstance(episode_data, list) and episode_data:
            episode = episode_data[0]
    
    # Handle complete series detection
    if has_complete_keyword:
        is_complete = True
        is_pack = True  # Complete series is always a pack
        if not seasons:
            # Complete series without specific season range - mark specially
            seasons = [0]  # 0 indicates unknown number of seasons
    
    # Generate season label
    if is_complete:
        if len(seasons) > 1 and seasons[0] != 0:
            season_label = f"S{seasons[0]:02d}-S{seasons[-1]:02d}"
        else:
            season_label = "Complete"
    elif len(seasons) == 1:
        if is_pack:
            season_label = f"S{seasons[0]:02d}"
        else:
            # Individual episode
            season_label = f"S{seasons[0]:02d}E{episode:02d}" if episode else f"S{seasons[0]:02d}"
    elif len(seasons) > 1:
        season_label = f"S{seasons[0]:02d}-S{seasons[-1]:02d}"
    else:
        season_label = "Unknown"
    
    return SeasonInfo(
        seasons=seasons,
        is_complete=is_complete,
        season_label=season_label,
        is_pack=is_pack,
        episode=episode
    )


def is_better_quality(new_score: int, current_score: int, threshold: int = 500) -> bool:
    """Check if new quality is significantly better (meets threshold)."""
    return new_score >= current_score + threshold


# ============================================================================
# MAGNET LINK ENCODING
# ============================================================================

def encode_magnet_link(name: str, info_hash: str) -> str:
    """Create a properly encoded magnet link.
    
    Handles non-ASCII characters by URL-encoding the name component.
    This ensures compatibility with APIs that expect ASCII-only magnet links.
    
    Args:
        name: The display name for the torrent
        info_hash: The torrent info hash (40 hex characters)
        
    Returns:
        Properly formatted and encoded magnet link
    """
    encoded_name = urllib.parse.quote(name.encode('utf-8'), safe='')
    return f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_name}"


def normalize_hash(hash_str: Optional[str]) -> str:
    """Normalize a hash string to lowercase.
    
    Args:
        hash_str: The hash string to normalize
        
    Returns:
        Lowercase hash string, or empty string if None
    """
    return hash_str.lower() if hash_str else ""


def extract_infohash_from_item(item: Dict[str, Any], 
                                hash_fields: Optional[List[str]] = None,
                                magnet_fields: Optional[List[str]] = None,
                                validate_guid: bool = False) -> Optional[str]:
    """Extract infohash from a torrent item dictionary.
    
    This is a shared helper used by ProwlarrClient and JackettClient to avoid
    code duplication when extracting torrent hashes from various API formats.
    
    Args:
        item: Dictionary containing torrent info from indexer API
        hash_fields: List of field names to check for direct hash (case-insensitive)
                    e.g., ["infoHash", "infohash", "InfoHash"]
        magnet_fields: List of field names to check for magnet URL
                    e.g., ["magnetUrl", "MagnetUri", "link"]
        validate_guid: If True, validate that guid is a 40-char hex string
        
    Returns:
        Lowercase infohash string, or None if not found
    """
    # Try direct hash fields
    if hash_fields:
        for field in hash_fields:
            infohash = item.get(field)
            if infohash:
                return infohash.lower()
    
    # Parse from magnet URLs
    if magnet_fields:
        for field in magnet_fields:
            magnet_url = item.get(field, "")
            if magnet_url and magnet_url.startswith("magnet:"):
                try:
                    parsed = urllib.parse.urlparse(magnet_url)
                    params = urllib.parse.parse_qs(parsed.query)
                    xt = params.get("xt", [""])[0]
                    if xt.startswith("urn:btih:"):
                        return xt[9:].lower()  # Remove "urn:btih:" prefix
                except (ValueError, AttributeError, KeyError):
                    pass
    
    # Check if guid is a 40-char hash
    guid = item.get("guid", "")
    if guid and len(guid) == 40:
        if not validate_guid or all(c in "0123456789abcdefABCDEF" for c in guid):
            return guid.lower()
    
    return None


def build_search_result(name: str, infohash: str, magnet_link: str = "",
                        size: int = 0, seeders: int = 0, leechers: int = 0,
                        source: str = "", imdb_id: str = "") -> Dict[str, Any]:
    """Build a standardized search result dict.
    
    Shared helper used by ProwlarrClient and JackettClient to avoid
    code duplication when building result dicts from indexer APIs.
    
    Args:
        name: Torrent display name/title
        infohash: Torrent info hash
        magnet_link: Magnet URL (constructed if empty)
        size: Torrent size in bytes
        seeders: Number of seeders
        leechers: Number of leechers
        source: Source name (indexer name)
        imdb_id: IMDb ID if available
        
    Returns:
        Standardized result dict
    """
    if not magnet_link:
        magnet_link = encode_magnet_link(name, infohash)
    return {
        "title": name,
        "name": name,
        "hash": normalize_hash(infohash),
        "magnet": magnet_link,
        "size": size,
        "seeds": seeders,
        "peers": leechers,
        "source": source,
        "imdbId": imdb_id,
    }


class APIError(Exception):
    """API request error."""
    def __init__(self, message: str, status_code: int = None, retry_after: int = None):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class RateLimitError(APIError):
    """Rate limit exceeded even after retries - item should be retried later."""
    pass


class APIResponseError(APIError):
    """API response validation error."""
    pass


def validate_response(data: Any, required_keys: List[str], context: str = "") -> Dict[str, Any]:
    """Validate that response data contains required keys.
    
    Args:
        data: Response data (expected to be dict)
        required_keys: List of required keys
        context: Context string for error messages
        
    Returns:
        The data dict if valid
        
    Raises:
        APIResponseError: If data is None, not a dict, or missing required keys
    """
    if data is None:
        raise APIResponseError(f"{context}: Response data is None")
    
    if not isinstance(data, dict):
        raise APIResponseError(f"{context}: Expected dict, got {type(data).__name__}")
    
    missing = [key for key in required_keys if key not in data]
    if missing:
        raise APIResponseError(f"{context}: Missing required keys: {', '.join(missing)}")
    
    return data


def validate_list_response(data: Any, item_validator=None, context: str = "") -> List[Dict[str, Any]]:
    """Validate that response data is a list of dicts.
    
    Args:
        data: Response data (expected to be list)
        item_validator: Optional function to validate each item
        context: Context string for error messages
        
    Returns:
        The data list if valid (empty list if None)
        
    Raises:
        APIResponseError: If data is not a list
    """
    if data is None:
        return []
    
    if not isinstance(data, list):
        raise APIResponseError(f"{context}: Expected list, got {type(data).__name__}")
    
    if item_validator:
        valid_items = []
        for i, item in enumerate(data):
            try:
                valid_items.append(item_validator(item))
            except APIResponseError as e:
                logger.debug("%s: Skipping invalid item at index %d: %s", context, i, e)
                continue
        return valid_items
    
    return data


def make_request_with_backoff(client: httpx.Client, method: str, url: str, 
                                max_retries: int = 3, **kwargs) -> httpx.Response:
    """Make HTTP request with exponential backoff for server errors and timeouts.
    
    Note: Does NOT retry on 429 (rate limit) - let the caller's rate limiter handle that.
    """
    retries = 0
    backoff = 1
    
    while retries < max_retries:
        try:
            response = client.request(method, url, **kwargs)
            
            # Handle rate limiting (429 Too Many Requests) - return immediately
            # Let the caller's rate limiter handle the timing
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    logger.warning("Rate limited (429) on %s %s. Retry-After: %ss", 
                                  method, url.split('/')[-1], retry_after)
                else:
                    logger.warning("Rate limited (429) on %s %s. Letting rate limiter handle retry.", 
                                  method, url.split('/')[-1])
                return response  # Return immediately, don't retry internally
            
            # Handle server errors (5xx) - retry with backoff
            if response.status_code >= 500:
                if retries < max_retries - 1:
                    logger.warning("Server error %d. Retrying in %ds...", response.status_code, backoff)
                    time.sleep(backoff)
                    retries += 1
                    backoff *= 2
                    continue
            
            # Handle client errors (4xx) - don't retry
            if response.status_code >= 400:
                # VULN-005: Sanitize error text to prevent information disclosure
                error_text = sanitize_response_error(response)
                raise APIError(
                    f"HTTP {response.status_code}: {error_text}",
                    status_code=response.status_code
                )
            
            return response
            
        except httpx.TimeoutException:
            logger.warning("Timeout (request waited 60s). Retrying in ~%ds...", 60 + backoff)
            time.sleep(backoff)
            retries += 1
            backoff = min(backoff * 2, 60)
                
        except httpx.RequestError as e:
            if retries < max_retries - 1:
                logger.warning("Request error: %s. Retrying in %ds...", e, backoff)
                time.sleep(backoff)
                retries += 1
                backoff *= 2
            else:
                raise APIError(f"Max retries exceeded: {e}", status_code=0)
    
    raise APIError("Max retries exceeded", status_code=0)


class TraktClient:
    """Trakt.tv API client with rate limiting and optional authentication."""
    
    def __init__(self, client_id: str, access_token: Optional[str] = None):
        self.client_id = client_id
        self.access_token = access_token
        # VULN-006: Explicitly enable SSL verification
        self.client = httpx.Client(
            base_url=TRAKT_BASE_URL,
            headers={
                "Content-Type": "application/json",
                "trakt-api-version": "2",
                "trakt-api-key": client_id,
            },
            timeout=DEFAULT_TIMEOUT_SHORT,
            verify=True
        )
    
    def _request(self, method: str, path: str, use_auth: bool = False, **kwargs) -> Any:
        """Make rate-limited request to Trakt API.
        
        Args:
            method: HTTP method
            path: API endpoint path
            use_auth: If True, adds Authorization header with access token
            **kwargs: Additional request arguments
        """
        # Build headers
        headers = kwargs.pop("headers", {})
        
        # Add authentication header if required and token available
        if use_auth and self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        elif use_auth and not self.access_token:
            logger.warning("Authentication required but no access token available")
            return None
        
        trakt_limiter.wait()
        url = f"{TRAKT_BASE_URL}{path}"
        response = make_request_with_backoff(self.client, method, url, headers=headers, **kwargs)
        
        # Handle 429 specially - return without marking success so rate limiter works
        if response.status_code == 429:
            logger.debug("Got 429 for %s, will retry after rate limiter wait", path)
            return None
        
        if response.status_code >= 400:
            # VULN-005: Sanitize error text to prevent information disclosure
            error_text = sanitize_response_error(response)
            raise APIError(f"Trakt API error: {response.status_code} - {error_text}", 
                          status_code=response.status_code)
        
        # Mark successful request for rate limiting
        trakt_limiter.mark_success()
        
        return response.json() if response.text else None
    
    def get_trending_movies(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get trending movies from Trakt."""
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/movies/trending{query}")
        # Validate response is a list of items with 'movie' key
        return validate_list_response(
            data, 
            context="Trakt trending movies"
        )
    
    def get_popular_movies(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get popular movies from Trakt."""
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/movies/popular{query}")
        # Validate response is a list of movie objects
        return validate_list_response(
            data,
            context="Trakt popular movies"
        )
    
    def get_trending_shows(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get trending shows from Trakt."""
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/shows/trending{query}")
        # Validate response is a list of items with 'show' key
        return validate_list_response(
            data,
            context="Trakt trending shows"
        )
    
    def get_popular_shows(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get popular shows from Trakt."""
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/shows/popular{query}")
        # Validate response is a list of show objects
        return validate_list_response(
            data,
            context="Trakt popular shows"
        )
    
    # =========================================================================
    # WATCHED MOVIES (all periods)
    # =========================================================================
    
    def get_watched_movies(self, period: str = "weekly", limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get most watched movies for a specific period.
        
        Args:
            period: One of 'weekly', 'monthly', 'yearly', 'all'
            limit: Maximum number of items to return (None = no limit)
        """
        if period not in VALID_PERIODS:
            period = "weekly"
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/movies/watched/{period}{query}")
        return validate_list_response(
            data,
            context=f"Trakt watched movies ({period})"
        )
    
    # =========================================================================
    # COLLECTED MOVIES (all periods)
    # =========================================================================
    
    def get_collected_movies(self, period: str = "weekly", limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get most collected movies for a specific period.
        
        Args:
            period: One of 'weekly', 'monthly', 'yearly', 'all'
            limit: Maximum number of items to return (None = no limit)
        """
        if period not in VALID_PERIODS:
            period = "weekly"
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/movies/collected/{period}{query}")
        return validate_list_response(
            data,
            context=f"Trakt collected movies ({period})"
        )
    
    # =========================================================================
    # OTHER MOVIE LISTS
    # =========================================================================
    
    def get_anticipated_movies(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get most anticipated upcoming movies."""
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/movies/anticipated{query}")
        return validate_list_response(
            data,
            context="Trakt anticipated movies"
        )
    
    def get_boxoffice_movies(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get box office top 10 movies."""
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/movies/boxoffice{query}")
        return validate_list_response(
            data,
            context="Trakt boxoffice movies"
        )
    
    # =========================================================================
    # WATCHED SHOWS (all periods)
    # =========================================================================
    
    def get_watched_shows(self, period: str = "weekly", limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get most watched shows for a specific period.
        
        Args:
            period: One of 'weekly', 'monthly', 'yearly', 'all'
            limit: Maximum number of items to return (None = no limit)
        """
        if period not in VALID_PERIODS:
            period = "weekly"
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/shows/watched/{period}{query}")
        return validate_list_response(
            data,
            context=f"Trakt watched shows ({period})"
        )
    
    # =========================================================================
    # COLLECTED SHOWS (all periods)
    # =========================================================================
    
    def get_collected_shows(self, period: str = "weekly", limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get most collected shows for a specific period.
        
        Args:
            period: One of 'weekly', 'monthly', 'yearly', 'all'
            limit: Maximum number of items to return (None = no limit)
        """
        if period not in VALID_PERIODS:
            period = "weekly"
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/shows/collected/{period}{query}")
        return validate_list_response(
            data,
            context=f"Trakt collected shows ({period})"
        )
    
    # =========================================================================
    # OTHER SHOW LISTS
    # =========================================================================
    
    def get_anticipated_shows(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get most anticipated upcoming shows."""
        query = f"?limit={limit}" if limit else ""
        data = self._request("GET", f"/shows/anticipated{query}")
        return validate_list_response(
            data,
            context="Trakt anticipated shows"
        )
    
    def get_all_content(self, sources: List[str]) -> List[Dict[str, Any]]:
        """Get content from specified Trakt sources.
        
        Supports all 23 public curated Trakt lists:
        - Movies: trending, popular, watched/{weekly,monthly,yearly,all}, 
                  collected/{weekly,monthly,yearly,all}, anticipated, boxoffice
        - Shows: trending, popular, watched/{weekly,monthly,yearly,all},
                  collected/{weekly,monthly,yearly,all}, anticipated
        
        Returns ALL items from ALL sources (no limits, no deduplication at API level).
        Database handles duplicates via is_processed() check.
        """
        content = []
        
        for source in sources:
            try:
                # Handle special source: "users/liked" for authenticated liked lists
                if source == "users/liked":
                    liked_items = self.get_liked_list_items()
                    content.extend(liked_items)
                    continue
                
                # Parse source format: "type/category/period" or "type/category"
                parts = source.split("/")
                content_type = parts[0] if len(parts) > 0 else ""
                category = parts[1] if len(parts) > 1 else ""
                period = parts[2] if len(parts) > 2 else "weekly"  # default period
                
                # Handle based on content type and category
                if content_type == "movies":
                    items = self._fetch_movies(source, category, period)
                    for movie in items:
                        content.append({
                            "imdb_id": self._extract_imdb_id(movie.get("ids", {})),
                            "title": movie.get("title", "Unknown"),
                            "year": movie.get("year", 0),
                            "type": "movie",
                            "source": source
                        })
                
                elif content_type == "shows":
                    items = self._fetch_shows(source, category, period)
                    for show in items:
                        content.append({
                            "imdb_id": self._extract_imdb_id(show.get("ids", {})),
                            "title": show.get("title", "Unknown"),
                            "year": show.get("year", 0),
                            "type": "show",
                            "source": source
                        })
                
                else:
                    logger.warning("Unknown source type: %s", source)
                    
            except APIError as e:
                logger.error("Error fetching %s: %s", source, e)
                continue
        
        logger.info("Fetched %d items from %d sources", len(content), len(sources))
        return content
    
    def _fetch_by_category(self, content_type: str, category: str, period: str) -> List[Dict[str, Any]]:
        """Fetch movies or shows based on category and period.
        
        Args:
            content_type: "movies" or "shows"
            category: trending, popular, watched, collected, anticipated, boxoffice
            period: weekly, monthly, yearly, all
            
        Returns:
            List of content dicts (movie or show objects)
        """
        key = "movie" if content_type == "movies" else "show"
        
        if category == "trending":
            items = (self.get_trending_movies() if content_type == "movies" 
                     else self.get_trending_shows())
            return [item.get(key, {}) for item in items]
        
        elif category == "popular":
            return (self.get_popular_movies() if content_type == "movies" 
                    else self.get_popular_shows())
        
        elif category == "watched":
            items = (self.get_watched_movies(period) if content_type == "movies" 
                     else self.get_watched_shows(period))
            return [item.get(key, {}) for item in items]
        
        elif category == "collected":
            items = (self.get_collected_movies(period) if content_type == "movies" 
                     else self.get_collected_shows(period))
            return [item.get(key, {}) for item in items]
        
        elif category == "anticipated":
            items = (self.get_anticipated_movies() if content_type == "movies" 
                     else self.get_anticipated_shows())
            return [item.get(key, {}) for item in items]
        
        elif category == "boxoffice" and content_type == "movies":
            return self.get_boxoffice_movies()
        
        else:
            logger.warning("Unknown %s category: %s", content_type, category)
            return []
    
    def _fetch_movies(self, source: str, category: str, period: str) -> List[Dict[str, Any]]:
        return self._fetch_by_category("movies", category, period)
    
    def _fetch_shows(self, source: str, category: str, period: str) -> List[Dict[str, Any]]:
        return self._fetch_by_category("shows", category, period)
    
    def _extract_imdb_id(self, ids: Dict[str, str]) -> str:
        """Extract IMDB ID from Trakt IDs."""
        imdb = ids.get("imdb", "")
        if imdb:
            return imdb
        # Fallback: create from trakt ID
        trakt_id = ids.get("trakt", "")
        return f"tt{trakt_id}" if trakt_id else ""
    
    # =========================================================================
    # LIKED LISTS (Authenticated)
    # =========================================================================
    
    def get_liked_lists(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get user's liked lists from Trakt.
        
        This endpoint requires authentication with an access token.
        Returns all lists the user has liked across Trakt.
        
        Args:
            limit: Maximum number of liked lists to return (None = fetch all pages)
            
        Returns:
            List of liked list objects containing list metadata
        """
        all_liked_lists = []
        page = 1
        per_page = TRAKT_PER_PAGE
        
        while True:
            # Build query with pagination
            if limit:
                query = f"?page={page}&limit={min(per_page, limit - len(all_liked_lists))}"
            else:
                query = f"?page={page}&limit={per_page}"
            
            data = self._request("GET", f"/users/likes/lists{query}", use_auth=True)
            
            if data is None:
                logger.warning("Failed to fetch liked lists - authentication may be required")
                break
            
            # Validate response is a list
            validated = validate_list_response(
                data,
                context="Trakt liked lists"
            )
            
            # Extract list info from each liked item
            for item in validated:
                if isinstance(item, dict) and "list" in item:
                    all_liked_lists.append(item)
            
            # Check if we've reached the limit or got fewer items than requested
            if limit and len(all_liked_lists) >= limit:
                break
            
            if len(validated) < per_page:
                # No more pages
                break
            
            page += 1
        
        logger.info("Found %d liked lists from Trakt", len(all_liked_lists))
        return all_liked_lists
    
    def get_list_items(self, username: str, list_id: str, 
                       item_types: str = "movie,show") -> List[Dict[str, Any]]:
        """Get items from a specific Trakt list.
        
        Supports both public and private lists (private requires auth).
        Fetches ALL items by paginating through all pages.
        
        Args:
            username: Username of the list owner
            list_id: List ID (can be slug like 'my-favorite-movies' or numeric)
            item_types: Comma-separated types to fetch (default: "movie,show")
            
        Returns:
            List of items from the list with type, movie/show data, and IDs
        """
        all_items = []
        page = 1
        per_page = TRAKT_PER_PAGE
        
        while True:
            try:
                # Build URL with pagination - can use slug or numeric ID
                url = f"/users/{username}/lists/{list_id}/items/{item_types}?page={page}&limit={per_page}"
                data = self._request("GET", url)
                
                if data is None:
                    if page == 1:
                        logger.debug("No data returned for list %s/%s", username, list_id)
                    break
                
                # Validate response
                validated = validate_list_response(
                    data,
                    context=f"Trakt list items ({username}/{list_id})"
                )
                
                if not validated:
                    break
                
                all_items.extend(validated)
                
                # Check if we got fewer items than requested (no more pages)
                if len(validated) < per_page:
                    break
                
                page += 1
                
            except APIError as e:
                logger.warning("Error fetching list %s/%s page %d: %s", username, list_id, page, e)
                break
        
        logger.debug("Fetched %d items from list %s/%s", len(all_items), username, list_id)
        return all_items
    
    def get_liked_list_items(self) -> List[Dict[str, Any]]:
        """Get all items from all liked lists.
        
        This is a convenience method that:
        1. Fetches all liked lists (requires authentication)
        2. Fetches ALL items from each liked list (no limits)
        3. Deduplicates by IMDB ID
        4. Returns combined list of all unique items
        
        Returns:
            List of unique content items from all liked lists
        """
        # Step 1: Get liked lists (requires auth)
        liked_lists = self.get_liked_lists()
        
        if not liked_lists:
            logger.info("No liked lists found or authentication not configured")
            return []
        
        # Step 2: Fetch items from each list
        all_items = []
        seen_ids = set()
        
        for liked in liked_lists:
            list_data = liked.get("list", {})
            list_name = list_data.get("name", "Unknown")
            list_ids = list_data.get("ids", {})
            list_id = list_ids.get("slug") or list_ids.get("trakt", "")
            
            user_data = list_data.get("user", {})
            username = user_data.get("ids", {}).get("slug", "")
            
            if not username or not list_id:
                logger.debug("Skipping liked list without username/list_id: %s", list_name)
                continue
            
            logger.info("Fetching items from liked list: %s (by %s)", list_name, username)
            
            # Fetch items from this list
            items = self.get_list_items(username, list_id)
            
            # Process items and deduplicate
            for item in items:
                item_type = item.get("type", "")
                
                if item_type == "movie":
                    movie_data = item.get("movie", {})
                    ids = movie_data.get("ids", {})
                    imdb_id = ids.get("imdb", "")
                    
                    if imdb_id and imdb_id not in seen_ids:
                        seen_ids.add(imdb_id)
                        all_items.append({
                            "imdb_id": imdb_id,
                            "title": movie_data.get("title", "Unknown"),
                            "year": movie_data.get("year", 0),
                            "type": "movie",
                            "source": f"users/liked",
                            "list_name": list_name,
                            "list_owner": username
                        })
                
                elif item_type == "show":
                    show_data = item.get("show", {})
                    ids = show_data.get("ids", {})
                    imdb_id = ids.get("imdb", "")
                    
                    if imdb_id and imdb_id not in seen_ids:
                        seen_ids.add(imdb_id)
                        all_items.append({
                            "imdb_id": imdb_id,
                            "title": show_data.get("title", "Unknown"),
                            "year": show_data.get("year", 0),
                            "type": "show",
                            "source": f"users/liked",
                            "list_name": list_name,
                            "list_owner": username
                        })
        
        logger.info("Collected %d unique items from %d liked lists", len(all_items), len(liked_lists))
        return all_items


class DebridClient(ABC):
    """Abstract base class for debrid service clients.

    Owns the shared torrent search infrastructure (Zilean/Prowlarr/Jackett).
    Subclasses implement debrid-specific operations: cache checking, account
    listing, magnet adding, and torrent removal.

    Template method pattern: search_torrents() runs the shared search
    pipeline, calling the subclass's check_cached() for availability.
    """

    def __init__(self):
        # Shared search backends (independent of any debrid service)
        self.searcher_zilean = ZileanClient()
        self.searcher_prowlarr = ProwlarrClient()
        self.searcher_jackett = JackettClient()

    @abstractmethod
    def check_cached(self, hashes: List[str]) -> Dict[str, bool]:
        """Check which torrent hashes are instantly available.

        Args:
            hashes: List of torrent hashes (lowercase hex strings).

        Returns:
            Dict mapping each hash to True (available) or False (not available).
        """
        ...

    @abstractmethod
    def get_my_torrents(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch all torrents currently in the debrid account.

        Returns:
            List of torrent dicts if successful, None if API call failed.
            Each dict must contain at least: "id", "name", "hash".
        """
        ...

    @abstractmethod
    def add_torrent(self, magnet: str, title: str = "") -> Optional[str]:
        """Add a torrent by magnet link to the debrid service.

        Args:
            magnet: Full magnet URI (must pass format validation).
            title: Human-readable title for logging.

        Returns:
            Torrent ID string if successful, None if failed.

        Raises:
            RateLimitError: If rate limited after all retries.
        """
        ...

    @abstractmethod
    def remove_torrent(self, torrent_id: str) -> bool:
        """Remove a torrent from the debrid account.

        Args:
            torrent_id: The service-specific torrent identifier.

        Returns:
            True if successfully removed, False otherwise.
        """
        ...

    # ---- Concrete shared methods ----

    def find_existing_by_hash(self, torrent_hash: str) -> Optional[Dict[str, Any]]:
        """Check if a torrent already exists in account by hash."""
        my_torrents = self.get_my_torrents()
        if not my_torrents:
            return None
        for torrent in my_torrents:
            if torrent.get("hash", "").lower() == torrent_hash.lower():
                return torrent
        return None

    def search_torrents(self, query: str, search_type: str = "movie",
                        imdb_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for torrents via Zilean database with Prowlarr/Jackett fallback.

        Flow:
        1. Search Zilean database by IMDb ID (if available) or title
        2. Fallback to Prowlarr if Zilean not configured or no results
        3. Fallback to Jackett if Prowlarr not configured or no results
        4. Check which torrents are cached via subclass check_cached()
        5. Return torrents with availability info

        Args:
            query: Search query string (title).
            search_type: Type of content ('movie' or 'show').
            imdb_id: Optional IMDb ID for more accurate search.

        Returns:
            List of torrent dicts with availability info.
        """
        # Determine which searchers are available
        zilean_available = getattr(self, 'searcher_zilean', None) and self.searcher_zilean.is_configured()
        prowlarr_available = getattr(self, 'searcher_prowlarr', None) and self.searcher_prowlarr.is_configured()
        jackett_available = getattr(self, 'searcher_jackett', None) and self.searcher_jackett.is_configured()

        services = []
        if zilean_available:
            services.append("Zilean")
        if prowlarr_available:
            services.append("Prowlarr")
        if jackett_available:
            services.append("Jackett")

        if services:
            logger.info("Searching for: %s (%s) via %s", query, search_type, " → ".join(services))
        else:
            logger.info("Searching for: %s (%s) - no searchers configured!", query, search_type)

        all_search_results = []

        category = None
        prowlarr_categories = None
        if search_type == "movie":
            category = "movie"
            prowlarr_categories = [2000]
        elif search_type == "show":
            category = "tvSeries"
            prowlarr_categories = [5000]

        # Try Zilean first (if configured)
        if zilean_available:
            try:
                if imdb_id:
                    logger.debug("Searching Zilean by IMDb ID: %s", imdb_id)
                    imdb_results = self.searcher_zilean.search_by_imdb(
                        imdb_id, category=category, limit=SEARCH_LIMIT_IMDB)
                    if imdb_results:
                        logger.info("Zilean found %d torrents by IMDb ID: %s",
                                     len(imdb_results), imdb_id)
                        all_search_results.extend(imdb_results)

                if not all_search_results:
                    logger.debug("Searching Zilean by title: %s", query)
                    title_results = self.searcher_zilean.search(
                        query, category=category, limit=SEARCH_LIMIT_TITLE)
                    if title_results:
                        logger.info("Zilean found %d torrents by title: %s",
                                     len(title_results), query)
                        all_search_results.extend(title_results)

            except (OSError, ValueError) as e:
                logger.debug("Zilean search traceback:", exc_info=True)
                logger.warning("Zilean search failed, will try Prowlarr fallback: %s", e)
        else:
            logger.debug("Zilean not configured, using Prowlarr")

        # Fallback to Prowlarr
        if not all_search_results and prowlarr_available:
            logger.debug("Searching Prowlarr as fallback: %s", query)
            prowlarr_results = self.searcher_prowlarr.search(
                query, categories=prowlarr_categories, limit=SEARCH_LIMIT_TITLE)
            if prowlarr_results:
                logger.info("Prowlarr found %d torrents for: %s",
                             len(prowlarr_results), query)
                all_search_results.extend(prowlarr_results)

        # Fallback to Jackett
        if not all_search_results and jackett_available:
            logger.debug("Searching Jackett as fallback: %s", query)
            jackett_results = self.searcher_jackett.search(
                query, limit=SEARCH_LIMIT_TITLE)
            if jackett_results:
                logger.info("Jackett found %d torrents for: %s",
                             len(jackett_results), query)
                all_search_results.extend(jackett_results)

        if not all_search_results:
            logger.warning("No torrents found for: %s", query)
            return []

        # Remove duplicates by hash
        seen_hashes = set()
        unique_results = []
        for torrent in all_search_results:
            hash_value = torrent.get("hash", "").lower()
            if hash_value and hash_value not in seen_hashes:
                seen_hashes.add(hash_value)
                unique_results.append(torrent)

        if len(unique_results) < len(all_search_results):
            logger.debug("Removed %d duplicate torrents",
                          len(all_search_results) - len(unique_results))

        all_search_results = unique_results

        # Get hashes and check availability via subclass
        hashes = [t.get("hash", "").lower() for t in all_search_results if t.get("hash")]

        if not hashes:
            logger.warning("No hashes found in search results for: %s", query)
            return []

        cached_status = self.check_cached(hashes)

        results = []
        cached_count = 0
        for torrent in all_search_results:
            hash_value = torrent.get("hash", "").lower()
            is_cached = cached_status.get(hash_value, False)
            if is_cached:
                cached_count += 1

            results.append({
                "title": torrent.get("name", ""),
                "name": torrent.get("name", ""),
                "hash": hash_value,
                "magnet": torrent.get("magnet", ""),
                "size": torrent.get("size", 0),
                "availability": is_cached,
                "seeders": torrent.get("seeds", 0),
                "leechers": torrent.get("peers", 0),
            })

        logger.info("Found %d torrents from Zilean/Prowlarr/Jackett, %d cached",
                     len(results), cached_count)
        return results

    def get_cached_torrents(self, query: str, content_type: str = "movie",
                            excluded_sources: Optional[List[str]] = None,
                            min_resolution_score: int = 800,
                            imdb_id: Optional[str] = None) -> List[TorrentResult]:
        """Search and return only cached (instantly available) torrents.

        Template method — uses search_torrents() with subclass check_cached().
        Filters results by quality and resolution thresholds.
        """
        if excluded_sources is None:
            excluded_sources = ["CAM", "TS", "HDCAM"]

        results = self.search_torrents(query, content_type, imdb_id)
        cached = []

        excluded_lower = [excl.lower() for excl in excluded_sources]
        skipped_low_res = 0

        for item in results:
            if item.get("availability", False):
                name = item.get("title", item.get("name", "Unknown"))
                quality = parse_quality(name)

                if any(excl in quality.source.lower() for excl in excluded_lower):
                    continue
                if any(excl in name.lower() for excl in excluded_lower):
                    continue

                resolution_score = RESOLUTION_SCORES.get(quality.resolution, 0)
                if resolution_score < min_resolution_score:
                    # Special case: complete series packs with unknown resolution but large size
                    if resolution_score == 0 and content_type == "show":
                        size = item.get("size", 0)
                        season_info = parse_season_info(name)
                        if (season_info and season_info.is_complete and
                                size >= COMPLETE_PACK_MIN_SIZE):
                            pass  # Allow through
                        else:
                            skipped_low_res += 1
                            continue
                    else:
                        skipped_low_res += 1
                        continue

                magnet = item.get("magnet", "").strip()
                if not magnet:
                    continue

                cached.append(TorrentResult(
                    name=name,
                    magnet=magnet,
                    availability=True,
                    size=item.get("size", 0),
                    quality=quality,
                    hash=item.get("hash", ""),
                    seeders=item.get("seeders", 0),
                    leechers=item.get("leechers", 0),
                    season_info=parse_season_info(name),
                ))

        if cached:
            logger.info("Found %d cached torrents for: %s (skipped %d low-res)",
                         len(cached), query, skipped_low_res)
        else:
            logger.info("No cached torrents found for: %s", query)

        return cached


class TorboxClient(DebridClient):
    """Torbox API client with rate limiting and torrent search.

    Search chain (inherited from DebridClient):
    1. Zilean PostgreSQL database (most accurate, IMDb ID support)
    2. Prowlarr API (fallback, requires local instance)
    3. Jackett API (fallback, requires local instance)
    """
    
    def __init__(self, api_key: str):
        super().__init__()  # Creates searcher_zilean, searcher_prowlarr, searcher_jackett
        self.api_key = api_key
        # VULN-006: Explicitly enable SSL verification
        self.client = httpx.Client(
            base_url=TORBOX_BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
            },
            timeout=DEFAULT_TIMEOUT_LONG,
            verify=True
        )
    
    CREATION_MAX_RETRIES = 10

    def _request(self, method: str, path: str, use_creation_limiter: bool = False, 
                 max_retries: int = 3, **kwargs) -> Any:
        """Make rate-limited request to Torbox API.
        
        Args:
            method: HTTP method
            path: API endpoint path
            use_creation_limiter: Use slower rate limit for creation endpoints (createtorrent, etc.)
            max_retries: Max retries for 429 errors (applies to both creation and non-creation)
            **kwargs: Additional request arguments
            
        Raises:
            RateLimitError: If 429 persists after all retries
            APIError: For other API errors
        """
        effective_max_retries = self.CREATION_MAX_RETRIES if use_creation_limiter else max_retries
        retries = 0
        
        while retries <= effective_max_retries:
            # Use appropriate rate limiter
            if use_creation_limiter:
                torbox_creation_limiter.wait()
            else:
                torbox_limiter.wait()
            
            url = f"{TORBOX_BASE_URL}{path}"
            response = make_request_with_backoff(self.client, method, url, **kwargs)
            
            # Handle 429 specially
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)
                elif use_creation_limiter:
                    wait_time = min(60 * (2 ** min(retries, 4)), 300)
                else:
                    wait_time = 0
                
                retries += 1
                if retries > effective_max_retries:
                    logger.warning("Rate limit (429) exhausted after %d retries for %s", 
                                  effective_max_retries, path)
                    raise RateLimitError(
                        f"Rate limited (429) on {path} after {effective_max_retries} retries",
                        status_code=429
                    )
                
                logger.warning("Rate limited (429) on %s. Waiting %ds before retry (attempt %d/%d)...", 
                              path, wait_time, retries, effective_max_retries)
                if wait_time > 0:
                    time.sleep(wait_time)
                continue
            
            if response.status_code >= 400:
                # VULN-005: Sanitize error text to prevent information disclosure
                error_text = sanitize_response_error(response)
                raise APIError(f"Torbox API error: {response.status_code} - {error_text}",
                              status_code=response.status_code)
            
            # Mark successful request for rate limiting
            if use_creation_limiter:
                torbox_creation_limiter.mark_success()
            else:
                torbox_limiter.mark_success()
            
            return response.json() if response.text else None
    
    def check_cached(self, hashes: List[str]) -> Dict[str, bool]:
        """Check if torrents are cached on Torbox.
        
        Args:
            hashes: List of torrent hashes to check
            
        Returns:
            Dict mapping hash to True/False (cached or not)
        """
        if not hashes:
            return {}
        
        try:
            # Use POST to checkcached endpoint with JSON data
            response = self._request(
                "POST",
                "/v1/api/torrents/checkcached",
                json={"hashes": hashes}  # Send as JSON
            )
            
            if response and isinstance(response, dict):
                # Parse response - Torbox returns which hashes are cached
                data = response.get("data", {})
                if isinstance(data, dict):
                    # Data is a dict of hash -> cache info (or empty if not cached)
                    result = {}
                    for h in hashes:
                        h_lower = normalize_hash(h)
                        # If hash exists in data and has info, it's cached
                        # Check using lowercase hash for consistency
                        is_cached = h_lower in data and data[h_lower] is not None
                        result[h_lower] = is_cached
                    return result
            
            return {h.lower(): False for h in hashes}
            
        except (APIError, APIResponseError) as e:
            logger.debug("Cache check traceback:", exc_info=True)
            logger.debug("Error checking cached torrents: %s", e)
            return {h.lower(): False for h in hashes}
    
    def get_search_engines(self) -> List[Dict[str, Any]]:
        """Get user's configured search engines (Prowlarr/Jackett)."""
        try:
            response = self._request("GET", "/v1/api/user/settings/searchengines")
            if response and isinstance(response, dict):
                data = response.get("data", [])
                if isinstance(data, list):
                    return data
            return []
        except (APIError, APIResponseError) as e:
            logger.debug("Search engines traceback:", exc_info=True)
            logger.debug("Error getting search engines: %s", e)
            return []

    def get_my_torrents(self) -> Optional[List[Dict[str, Any]]]:
        """Get all torrents in user's Torbox account.

        Handles pagination to fetch all torrents (API has default limit of 1000).

        Returns:
            List of torrent dicts if successful, None if API call failed
        """
        all_torrents: List[Dict[str, Any]] = []
        offset = 0
        limit = TORBOX_LIST_LIMIT  # Max per request (API allows >1000, 5000 covers most users in 1 call)

        try:
            while True:
                response = self._request(
                    "GET",
                    "/v1/api/torrents/mylist",
                    params={"offset": offset, "limit": limit}
                )
                # Validate response is a dict with 'data' key
                if response is None:
                    return None
                validated = validate_response(
                    response,
                    required_keys=["data"],
                    context="Torbox mylist"
                )
                batch = validated.get("data", [])
                if not batch:
                    break  # No more torrents

                all_torrents.extend(batch)

                # If we got fewer than limit, we've reached the end
                if len(batch) < limit:
                    break

                offset += limit

            return all_torrents
        except (APIError, APIResponseError) as e:
            logger.error("Error getting my torrents: %s", e)
            return None
    
    def add_torrent(self, magnet: str, title: str = "") -> Optional[str]:
        """Add a torrent to Torbox by magnet link.
        
        Returns:
            Torrent ID if successful, None if failed
            
        Raises:
            RateLimitError: If rate limited even after retries - caller should NOT mark as failed
        """
        # Validate magnet format before sending
        # Strip whitespace and validate properly formatted magnet link
        if magnet:
            magnet = magnet.strip()
        if not magnet or not magnet.startswith("magnet:?xt=urn:btih:"):
            logger.warning("Invalid magnet format for %s: %r", title[:50], magnet[:60] if magnet else "None")
            return None
        # Additional validation: ensure there's a non-empty hash after btih:
        if "btih:" in magnet:
            hash_part = magnet.split("btih:", 1)[1]
            if "&" in hash_part:
                hash_part = hash_part.split("&", 1)[0]
            if not hash_part:
                logger.warning("Empty hash in magnet for %s", title[:50])
                return None
        
        # Extract hash from magnet for logging
        try:
            if "btih:" in magnet:
                parts = magnet.split("btih:", 1)
                if len(parts) > 1:
                    hash_part = parts[1]
                    # Extract up to & or end of string, then take first 16 chars
                    if "&" in hash_part:
                        hash_part = hash_part.split("&", 1)[0]
                    magnet_hash = hash_part[:16] if hash_part else "unknown"
                else:
                    magnet_hash = "unknown"
            else:
                magnet_hash = "unknown"
        except (IndexError, ValueError):
            magnet_hash = "unknown"
        
        try:
            logger.debug("Adding torrent to Torbox: %s (hash: %s...)", title[:50], magnet_hash)
            # DEBUG: Log full magnet link for troubleshooting
            logger.debug("Magnet link for %s: %s", title[:50], magnet[:100] if magnet else "None")
            response = self._request(
                "POST",
                "/v1/api/torrents/createtorrent",
                use_creation_limiter=True,  # 1 req/min limit
                data={"magnet": magnet},
                timeout=DEFAULT_TIMEOUT_CREATION  # Extended timeout for large multi-season packs
            )
            # Validate response structure
            if response is None:
                logger.warning("Failed to add torrent (no response): %s", title)
                return None
            
            validated = validate_response(
                response,
                required_keys=["success"],
                context="Torbox add torrent"
            )
            
            if validated.get("success", False):
                data = validated.get("data", {})
                if data and isinstance(data, dict):
                    torrent_id = data.get("torrent_id")
                    if torrent_id:
                        logger.debug("Added torrent %s: %s", torrent_id, title)
                        return torrent_id
                logger.warning("Failed to add torrent (missing torrent_id): %s", title)
            else:
                error_msg = validated.get("error", "unknown")
                detail_msg = validated.get("detail", "no details")
                logger.warning("Failed to add torrent (API error: %s - %s): %s", 
                              error_msg, detail_msg, title[:60])
                # Log full magnet link for debugging if it's a magnet error
                if "BOZO_TORRENT" in str(error_msg).upper() or "MAGNET" in str(detail_msg).upper():
                    logger.debug("Magnet link that failed: %s...", magnet[:100])
            return None
        except RateLimitError:
            # Re-raise rate limit errors so caller knows NOT to mark as permanently failed
            raise
        except (APIError, APIResponseError) as e:
            logger.error("Error adding torrent %s: %s", title, e)
            return None
    
    def remove_torrent(self, torrent_id: str) -> bool:
        """Remove a torrent from Torbox."""
        try:
            response = self._request(
                "POST",
                "/v1/api/torrents/controltorrent",
                json={"torrent_id": torrent_id, "operation": "delete"}
            )
            # Validate response has success field
            if response is None:
                logger.warning("Failed to remove torrent %s (no response)", torrent_id)
                return False
            
            validated = validate_response(
                response,
                required_keys=["success"],
                context="Torbox remove torrent"
            )
            
            success = validated.get("success", False)
            if success:
                logger.debug("Removed torrent %s", torrent_id)
            else:
                logger.warning("Failed to remove torrent %s (API returned failure)", torrent_id)
            return success
        except (APIError, APIResponseError) as e:
            logger.error("Error removing torrent %s: %s", torrent_id, e)
            return False


# ============================================================================
# REAL DEBRID CLIENT
# ============================================================================

class RealDebridClient(DebridClient):
    """Real Debrid API client implementing the DebridClient interface."""

    def __init__(self, api_key: str):
        super().__init__()  # Creates searcher_zilean, searcher_prowlarr, searcher_jackett
        self.api_key = api_key
        self.client = httpx.Client(
            base_url=REAL_DEBRID_BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
            },
            timeout=DEFAULT_TIMEOUT_LONG,
            verify=True
        )
        # Create rate limiters for Real Debrid
        self._limiter = RateLimiter(REAL_DEBRID_RATE_LIMIT)
        self._creation_limiter = RateLimiter(REAL_DEBRID_CREATION_LIMIT)

    CREATION_MAX_RETRIES = 10

    def _request(self, method: str, path: str, use_creation_limiter: bool = False,
                 max_retries: int = 3, **kwargs) -> Any:
        """Make rate-limited request to Real Debrid API.

        Args:
            method: HTTP method
            path: API endpoint path
            use_creation_limiter: Use slower rate limit for addMagnet endpoint
            max_retries: Max retries for 429 errors
            **kwargs: Additional request arguments

        Raises:
            RateLimitError: If rate limited after all retries
            APIError: For other API errors
        """
        effective_max_retries = self.CREATION_MAX_RETRIES if use_creation_limiter else max_retries
        retries = 0

        while retries <= effective_max_retries:
            # Use appropriate rate limiter
            if use_creation_limiter:
                self._creation_limiter.wait()
            else:
                self._limiter.wait()

            url = f"{REAL_DEBRID_BASE_URL}{path}"
            response = make_request_with_backoff(self.client, method, url, **kwargs)

            # Handle 429 specially
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)
                elif use_creation_limiter:
                    wait_time = min(60 * (2 ** min(retries, 4)), 300)
                else:
                    wait_time = 0

                retries += 1
                if retries > effective_max_retries:
                    logger.warning("Rate limit (429) exhausted after %d retries for %s",
                                  effective_max_retries, path)
                    raise RateLimitError(
                        f"Rate limited (429) on {path} after {effective_max_retries} retries",
                        status_code=429
                    )

                logger.warning("Rate limited (429) on %s. Waiting %ds before retry (attempt %d/%d)...",
                              path, wait_time, retries, effective_max_retries)
                if wait_time > 0:
                    time.sleep(wait_time)
                continue

            if response.status_code >= 400:
                error_text = sanitize_response_error(response)
                raise APIError(f"Real Debrid API error: {response.status_code} - {error_text}",
                              status_code=response.status_code)

            # Mark successful request for rate limiting
            if use_creation_limiter:
                self._creation_limiter.mark_success()
            else:
                self._limiter.mark_success()

            return response.json() if response.text else None

    def check_cached(self, hashes: List[str]) -> Dict[str, bool]:
        """Check if torrents are cached on Real Debrid.

        Args:
            hashes: List of torrent hashes to check

        Returns:
            Dict mapping hash to True/False (cached or not)
        """
        if not hashes:
            return {}

        try:
            # RD expects hashes separated by '/' in the URL path
            hash_path = "/".join(normalize_hash(h) for h in hashes)
            response = self._request(
                "GET",
                f"/torrents/instantAvailability/{hash_path}"
            )

            if response and isinstance(response, dict):
                result = {}
                for h in hashes:
                    h_lower = normalize_hash(h)
                    # In RD, if hash key exists and has 'rd' field with content, it's cached
                    hash_data = response.get(h_lower, {})
                    is_cached = isinstance(hash_data, dict) and bool(hash_data.get("rd"))
                    result[h_lower] = is_cached
                return result

            return {h.lower(): False for h in hashes}

        except (APIError, APIResponseError) as e:
            logger.debug("Cache check traceback:", exc_info=True)
            logger.debug("Error checking cached torrents: %s", e)
            return {h.lower(): False for h in hashes}

    def get_my_torrents(self) -> Optional[List[Dict[str, Any]]]:
        """Get all torrents in user's Real Debrid account.

        Handles pagination to fetch all torrents.

        Returns:
            List of torrent dicts if successful, None if API call failed.
            Normalizes 'filename' to 'name' for consistency.
        """
        all_torrents: List[Dict[str, Any]] = []
        page = 1
        limit = 100  # RD default/limit per page

        try:
            while True:
                response = self._request(
                    "GET",
                    "/torrents",
                    params={"page": page, "limit": limit}
                )

                if response is None:
                    return None

                # RD returns list directly, not wrapped in {"data": [...]}
                if not isinstance(response, list):
                    logger.error("Unexpected response format from Real Debrid: %s", type(response))
                    return None

                if not response:
                    break  # No more torrents

                # Normalize keys: RD uses 'filename' instead of 'name'
                for torrent in response:
                    if isinstance(torrent, dict):
                        if "filename" in torrent and "name" not in torrent:
                            torrent["name"] = torrent["filename"]

                all_torrents.extend(response)

                # If we got fewer than limit, we've reached the end
                if len(response) < limit:
                    break

                page += 1

            return all_torrents

        except (APIError, APIResponseError) as e:
            logger.error("Error getting my torrents: %s", e)
            return None

    def add_torrent(self, magnet: str, title: str = "") -> Optional[str]:
        """Add a torrent to Real Debrid by magnet link.

        Args:
            magnet: Full magnet URI
            title: Human-readable title for logging

        Returns:
            Torrent ID if successful, None if failed

        Raises:
            RateLimitError: If rate limited even after retries
        """
        # Validate magnet format
        if magnet:
            magnet = magnet.strip()
        if not magnet or not magnet.startswith("magnet:?xt=urn:btih:"):
            logger.warning("Invalid magnet format for %s", title[:60] if title else "unknown")
            return None

        try:
            # RD uses form-encoded data for addMagnet
            response = self._request(
                "POST",
                "/torrents/addMagnet",
                use_creation_limiter=True,
                data={"magnet": magnet}  # Form-encoded
            )

            if response and isinstance(response, dict):
                torrent_id = response.get("id")
                if torrent_id:
                    logger.info("Added torrent to Real Debrid: %s", title[:60] if title else "unknown")
                    return str(torrent_id)
                else:
                    logger.warning("Failed to add torrent (no ID returned): %s", title[:60] if title else "unknown")
            else:
                logger.warning("Failed to add torrent (no response): %s", title[:60] if title else "unknown")

            return None

        except RateLimitError:
            raise
        except (APIError, APIResponseError) as e:
            logger.error("Error adding torrent %s: %s", title[:60] if title else "unknown", e)
            return None

    def remove_torrent(self, torrent_id: str) -> bool:
        """Remove a torrent from Real Debrid.

        Args:
            torrent_id: The Real Debrid torrent identifier

        Returns:
            True if successfully removed, False otherwise
        """
        try:
            response = self._request(
                "DELETE",
                f"/torrents/delete/{torrent_id}"
            )

            # RD returns 204 No Content on success, which results in None response
            # from _request since response.text would be empty
            logger.debug("Removed torrent %s", torrent_id)
            return True

        except (APIError, APIResponseError) as e:
            # Handle 404 as already removed
            if hasattr(e, 'status_code') and e.status_code == 404:
                logger.debug("Torrent %s not found (already removed)", torrent_id)
                return True
            logger.error("Error removing torrent %s: %s", torrent_id, e)
            return False


def create_debrid_client() -> Optional[DebridClient]:
    """Create the configured debrid client based on DEBRID_SERVICE env var.

    Returns:
        DebridClient instance (TorboxClient or RealDebridClient),
        or None if the required API key is not configured.
    """
    service = get_debrid_service()

    if service == "real_debrid":
        api_key = get_real_debrid_key()
        if not api_key:
            logger.error("DEBRID_SERVICE=real_debrid but REAL_DEBRID_API_KEY not set in .env")
            return None
        logger.info("Using Real Debrid as debrid service")
        return RealDebridClient(api_key)

    else:
        # Default: Torbox
        api_key = get_torbox_key()
        if not api_key:
            logger.error("DEBRID_SERVICE=torbox but TORBOX_API_KEY not set in .env")
            return None
        logger.info("Using Torbox as debrid service")
        return TorboxClient(api_key)


# ============================================================================
# TORBOX DISCOVERY
# ============================================================================

def discover_existing_torrents(torbox_client: TorboxClient) -> Optional[Tuple[Dict[str, str], Set[str]]]:
    """Discover all torrents currently in Torbox account.
    
    Returns mapping of IMDB ID to Torbox torrent ID by:
    1. First trying direct ID matching (reliable)
    2. Then falling back to name matching for any unmatched items
    
    Also returns a set of all torrent hashes in the account for duplicate prevention.
    
    Args:
        torbox_client: TorboxClient instance
        
    Returns:
        Tuple of (imdb_id -> torbox_id mapping, set of all hashes in account),
        or None if API call failed,
        or ({}, set()) if account is empty
    """
    logger.info("Discovering existing torrents in Torbox account...")
    my_torrents = torbox_client.get_my_torrents()
    
    # Check if API call failed (None) vs empty account ([])
    if my_torrents is None:
        logger.error("Failed to discover torrents - Torbox API error")
        return None
    
    if not my_torrents:
        logger.info("No torrents found in Torbox account (empty account)")
        return {}, set()
    
    logger.info("Found %d torrents in Torbox account", len(my_torrents))

    # DUPLICATE DETECTION: Find and remove duplicate hashes
    hash_to_torrents = {}
    for torrent in my_torrents:
        torrent_hash = torrent.get("hash", "").lower()
        if torrent_hash:
            if torrent_hash not in hash_to_torrents:
                hash_to_torrents[torrent_hash] = []
            hash_to_torrents[torrent_hash].append(torrent)
    
    # Find hashes with multiple torrents
    duplicates = {h: torrents for h, torrents in hash_to_torrents.items() if len(torrents) > 1}
    if duplicates:
        removed_count = 0
        for hash_val, torrents in duplicates.items():
            # Keep the first one, remove the rest
            logger.info("Found %d duplicate torrents with hash %s...", len(torrents), hash_val[:16])
            for dup in torrents[1:]:  # Skip the first one
                dup_id = dup.get("id")
                dup_name = dup.get("name", "Unknown")[:50]
                try:
                    if torbox_client.remove_torrent(dup_id):
                        logger.info("Removed duplicate torrent: %s (ID: %s)", dup_name, dup_id)
                        removed_count += 1
                    else:
                        logger.warning("Failed to remove duplicate torrent: %s (ID: %s)", dup_name, dup_id)
                except (APIError, APIResponseError) as e:
                    logger.debug("Duplicate removal traceback:", exc_info=True)
                    logger.warning("Error removing duplicate torrent %s: %s", dup_id, e)
        
        if removed_count > 0:
            logger.info("Removed %d duplicate torrent(s) from account", removed_count)
            # Re-fetch the torrent list after cleanup
            my_torrents = torbox_client.get_my_torrents()
            if my_torrents is None:
                logger.error("Failed to re-fetch torrents after cleanup")
                return None
            logger.info("Now have %d torrents after duplicate removal", len(my_torrents))

    # Load all processed items from database to match against
    # For shows, we need season info to handle multi-season packs
    with get_db() as conn:
        rows = conn.execute(
            "SELECT imdb_id, title, year, torbox_id, season, content_type FROM processed WHERE torbox_id IS NOT NULL"
        ).fetchall()
    
    # Build lookup structures
    db_by_torbox_id = {}  # torbox_id -> (imdb_id, season)
    db_by_title_year = {}  # title:year -> list of (imdb_id, season, content_type)
    
    for row in rows:
        # Direct ID lookup (most reliable)
        if row['torbox_id']:
            db_by_torbox_id[str(row['torbox_id'])] = (row['imdb_id'], row['season'])
        
        # Fallback name lookup - group by title+year to handle multi-season shows
        key = f"{row['title'].lower()}:{row['year']}"
        if key not in db_by_title_year:
            db_by_title_year[key] = []
        db_by_title_year[key].append((row['imdb_id'], row['season'], row['content_type']))
    
    imdb_to_torbox = {}
    account_hashes = set()  # All hashes in account for duplicate prevention
    id_matches = 0
    name_matches = 0
    unmatched = []
    multi_season_updates = []  # Track multi-season packs that need DB updates
    
    for torrent in my_torrents:
        torbox_id = str(torrent.get("id", ""))
        torrent_hash = torrent.get("hash", "").lower()
        
        # Collect all hashes for duplicate prevention (even if matching fails)
        if torrent_hash:
            account_hashes.add(torrent_hash)
        
        if not torbox_id:
            continue
        
        # Method 1: Direct ID matching (most reliable)
        if torbox_id in db_by_torbox_id:
            imdb_id, season = db_by_torbox_id[torbox_id]
            imdb_to_torbox[imdb_id] = torbox_id
            id_matches += 1
            logger.debug("Matched torrent by ID: Torbox ID %s -> IMDB %s (season: %s)", torbox_id, imdb_id, season)
            continue
        
        # Method 2: Name matching (fallback with multi-season pack support)
        torrent_name = torrent.get("name", "")
        if not torrent_name:
            unmatched.append(torrent)
            continue
        
        # Parse torrent name to extract title, year, and season info
        try:
            parsed = guessit(torrent_name)
            torrent_title = parsed.get("title", "").lower()
            torrent_year = parsed.get("year", None)
            # Handle case where guessit returns a list for year
            if isinstance(torrent_year, list):
                torrent_year = torrent_year[0] if torrent_year else None
        except (ValueError, TypeError) as e:
            logger.debug("Guessit traceback for '%s':", torrent_name, exc_info=True)
            logger.debug("Failed to parse torrent name '%s': %s", torrent_name, e)
            unmatched.append(torrent)
            continue
        
        if not torrent_title:
            unmatched.append(torrent)
            continue
        
        # Parse season info for potential multi-season packs
        season_info = parse_season_info(torrent_name)
        
        # Try to match against database items by title + year
        matched = False
        for key, show_records in db_by_title_year.items():
            db_title, db_year = key.rsplit(":", 1)
            db_year = int(db_year) if db_year.isdigit() else None
            
            # Title must match (allow partial match for series)
            title_match = db_title in torrent_title or torrent_title in db_title
            
            # Year should match if available (allow ±1 year tolerance)
            year_match = True
            if torrent_year and db_year:
                year_match = abs(torrent_year - db_year) <= 1
            
            if title_match and year_match:
                # Get the first show record for the match
                imdb_id, season, content_type = show_records[0]
                
                # For TV shows with multi-season packs, check if we need to update other seasons
                if content_type == 'show' and season_info and len(season_info.seasons) > 1:
                    # This is a multi-season pack - record for all matching seasons
                    matched_seasons = []
                    for show_imdb_id, show_season, show_content_type in show_records:
                        # Check if this season is covered by the pack
                        if show_season.startswith('S') and show_season[1:3].isdigit():
                            season_num = int(show_season[1:3])
                            if season_num in season_info.seasons:
                                multi_season_updates.append({
                                    'imdb_id': show_imdb_id,
                                    'season': show_season,
                                    'torbox_id': torbox_id,
                                    'title': db_title,
                                    'year': db_year or torrent_year or 0
                                })
                                matched_seasons.append(show_season)
                    
                    if matched_seasons:
                        logger.debug("Multi-season pack '%s' matched to %s: %s (Torbox ID: %s)",
                                    torrent_name, imdb_id, matched_seasons, torbox_id)
                
                imdb_to_torbox[imdb_id] = torbox_id
                name_matches += 1
                logger.debug("Matched torrent by name '%s' to IMDB %s (Torbox ID: %s)",
                            torrent_name, imdb_id, torbox_id)
                matched = True
                break
        
        if not matched:
            unmatched.append(torrent)
    
    # Update database with multi-season pack info for any newly matched seasons
    if multi_season_updates:
        logger.info("Updating %d additional season records for multi-season packs...", len(multi_season_updates))
        with get_db() as conn:
            for update in multi_season_updates:
                # Only update if this season doesn't already have this torbox_id
                existing = conn.execute(
                    "SELECT torbox_id FROM processed WHERE imdb_id=? AND season=?",
                    (update['imdb_id'], update['season'])
                ).fetchone()
                
                if not existing or not existing['torbox_id']:
                    conn.execute('''
                        INSERT OR REPLACE INTO processed 
                        (imdb_id, season, title, year, content_type, action, reason, torbox_id, 
                         quality_score, quality_label, processed_at)
                        VALUES (?, ?, ?, ?, 'show', 'added', 'multi_season_pack_discovery', ?, 
                                NULL, NULL, ?)
                    ''', (update['imdb_id'], update['season'], update['title'], update['year'], 
                          update['torbox_id'], datetime.now(timezone.utc).isoformat()))
                    logger.debug("Updated season %s for %s with torbox_id %s", 
                                update['season'], update['imdb_id'], update['torbox_id'])
        
        logger.info("Updated database with %d multi-season pack associations", len(multi_season_updates))
    
    total_matches = id_matches + name_matches
    if unmatched:
        logger.debug("%d torrents in Torbox could not be matched to database (manual adds)", len(unmatched))
    
    logger.info("Discovered %d existing torrents in Torbox (ID matches: %d, name matches: %d, multi-season updates: %d, total hashes: %d)",
               total_matches, id_matches, name_matches, len(multi_season_updates), len(account_hashes))
    return imdb_to_torbox, account_hashes


def verify_and_clear_dropped_torrents(existing_torrents: Optional[Tuple[Dict[str, str], Set[str]]]) -> int:
    """Verify database records against discovered torrents and clear dropped ones.
    
    Compares all processed items with torbox_id against the discovered torrents
    from Torbox. If a torrent is in the database but NOT in the discovery results,
    it means Torbox dropped it and we need to re-add it.
    
    SAFETY: Only clears records if discovery succeeded. If existing_torrents is None
    (API failure), returns 0 without clearing anything.
    
    Args:
        existing_torrents: Tuple of (imdb_to_torbox mapping, account_hashes set) from discovery,
                         or None if discovery failed
        
    Returns:
        Number of records cleared (dropped torrents detected), or 0 if discovery failed
    """
    # Safety check: skip verification if discovery failed
    if existing_torrents is None:
        logger.warning("Skipping dropped torrent verification - discovery failed (API error)")
        return 0
    
    # Unpack the tuple
    imdb_to_torbox, _ = existing_torrents
    
    logger.debug("Verifying database records against discovered torrents...")
    
    # Build set of discovered torbox_ids for fast lookup
    discovered_ids = set(imdb_to_torbox.values())
    
    # SAFETY CHECK: Get total tracked torrents count for comparison
    with get_db() as conn:
        total_tracked = conn.execute(
            "SELECT COUNT(DISTINCT torbox_id) FROM processed WHERE torbox_id IS NOT NULL"
        ).fetchone()[0]
    
    # SAFETY CHECK: If we discovered significantly fewer torrents than tracked, 
    # discovery might be incomplete - skip clearing to avoid false positives
    if len(discovered_ids) < total_tracked * DISCOVERY_COMPLETENESS_THRESHOLD:  # Allow 5% variance
        logger.warning(
            "Discovery seems incomplete (found %d of %d tracked torrents). "
            "Skipping dropped torrent verification to avoid false clears.",
            len(discovered_ids), total_tracked
        )
        return 0
    
    # Get all processed items with torbox_id from database
    with get_db() as conn:
        rows = conn.execute(
            "SELECT imdb_id, season, title, torbox_id FROM processed WHERE torbox_id IS NOT NULL"
        ).fetchall()
    
    cleared_count = 0
    dropped_items = []
    
    for row in rows:
        db_torbox_id = row["torbox_id"]
        imdb_id = row["imdb_id"]
        season = row["season"]
        title = row["title"]
        
        # Check if this torbox_id is still in the account
        if db_torbox_id not in discovered_ids:
            # Torrent was dropped from Torbox - clear the record
            logger.info("Detected dropped torrent: %s (season: %s, torbox_id: %s)",
                       title, season, db_torbox_id)
            
            # Reset this item (will force re-processing)
            reset_count = reset_item(imdb_id, season if season != "unknown" else None)
            cleared_count += reset_count
            dropped_items.append(f"{title} ({season})")
    
    if cleared_count > 0:
        logger.info("Cleared %d dropped torrent record(s): %s", 
                   cleared_count, ", ".join(dropped_items))
    else:
        logger.debug("All %d database records verified - no dropped torrents detected", len(rows))
    
    return cleared_count


def cleanup_unmatched_torrents() -> None:
    """Remove torrents from Torbox that can't be matched to the database.
    
    Unmatched torrents are problematic because:
    1. They can't be tracked for quality upgrades
    2. They can't be properly deduplicated
    3. They have no association with Trakt items
    
    This function identifies all unmatched torrents, lists them, and optionally
    removes them after user confirmation.
    
    SAFETY: Requires user confirmation before removal.
    """
    logger.info("Scanning for unmatched torrents...")
    
    # Get API key
    torbox_key = get_torbox_key()
    if not torbox_key:
        logger.error("TORBOX_API_KEY not found in .env file")
        return
    
    # Get all torrents from Torbox
    torbox = TorboxClient(torbox_key)
    my_torrents = torbox.get_my_torrents()
    
    if my_torrents is None:
        logger.error("Failed to fetch torrents from Torbox")
        return
    
    if not my_torrents:
        logger.info("No torrents in Torbox account")
        return
    
    logger.info("Found %d torrents in Torbox account", len(my_torrents))
    
    # Get all torbox_ids from database
    with get_db() as conn:
        rows = conn.execute("SELECT DISTINCT torbox_id FROM processed WHERE torbox_id IS NOT NULL").fetchall()
    
    db_torbox_ids = {str(row['torbox_id']) for row in rows}
    logger.info("Database has %d tracked torrents", len(db_torbox_ids))
    
    # Find unmatched torrents
    unmatched = []
    for torrent in my_torrents:
        torbox_id = str(torrent.get("id", ""))
        if torbox_id and torbox_id not in db_torbox_ids:
            unmatched.append({
                'id': torbox_id,
                'name': torrent.get('name', 'Unknown'),
                'hash': torrent.get('hash', '')[:16] + '...' if torrent.get('hash') else 'N/A'
            })
    
    if not unmatched:
        logger.info("All torrents are matched to database. Nothing to cleanup.")
        return
    
    # Show unmatched torrents
    print(f"\n{'='*80}")
    print(f"UNMATCHED TORRENTS: {len(unmatched)}")
    print(f"{'='*80}")
    print("These torrents cannot be tracked for upgrades or deduplication:\n")
    
    for i, t in enumerate(unmatched, 1):
        print(f"{i}. ID: {t['id']}")
        print(f"   Name: {t['name'][:70]}{'...' if len(t['name']) > 70 else ''}")
        print(f"   Hash: {t['hash']}")
        print()
    
    print(f"{'='*80}")
    print(f"Total: {len(unmatched)} torrents will be REMOVED from Torbox")
    print(f"{'='*80}")
    
    # Require confirmation
    response = input("\nType 'REMOVE' to delete these torrents (or anything else to cancel): ")
    
    if response.strip() != 'REMOVE':
        print("\nCancelled. No torrents were removed.")
        return
    
    # Remove unmatched torrents
    print("\nRemoving unmatched torrents...")
    removed_count = 0
    failed_count = 0
    
    for t in unmatched:
        try:
            if torbox.remove_torrent(t['id']):
                print(f"  ✓ Removed: {t['name'][:50]}...")
                removed_count += 1
            else:
                print(f"  ✗ Failed: {t['name'][:50]}...")
                failed_count += 1
        except (APIError, APIResponseError) as e:
            print(f"  ✗ Error removing {t['id']}: {e}")
            failed_count += 1
    
    print(f"\n{'='*80}")
    print(f"Done! Removed {removed_count} torrents, failed {failed_count}")
    print(f"{'='*80}")
    
    if removed_count > 0:
        print(f"\nNote: If any of these were from Trakt lists, they will be re-added")
        print(f"on the next sync with proper tracking (if cached torrents are found).")


# ============================================================================
# SYNC LOGIC
# ============================================================================

class SyncEngine:
    """Main synchronization logic."""
    
    def __init__(self, torbox_client: TorboxClient, trakt_client: TraktClient,
                 config: Dict[str, Any], telegram_notifier: Optional[TelegramNotifier] = None):
        self.torbox = torbox_client
        self.trakt = trakt_client
        self.config = config
        self.filters = config.get("filters", {})
        self.telegram = telegram_notifier
        self.account_hashes: Set[str] = set()  # All hashes in account for duplicate prevention
        # Pre-compile regex patterns for excluded keywords (optimization)
        self._exclude_patterns: List[Tuple[str, Any]] = []
        exclude_keywords = self.filters.get("exclude", [])
        for keyword in exclude_keywords:
            escaped = re.escape(keyword)
            pattern = rf"(^|[^a-zA-Z0-9]){escaped}($|[^a-zA-Z0-9])"
            self._exclude_patterns.append((keyword, re.compile(pattern, re.IGNORECASE)))
        # Track per-run sync stats for accurate summary notifications
        self._sync_stats = {
            "added": 0,
            "upgraded": 0,
            "skipped": 0,
            "failed": 0,
            "movies": 0,
            "shows": 0
        }
    
    def _increment_stats(self, action: str, content_type: str) -> None:
        """Increment per-run sync stats counter."""
        if hasattr(self, '_sync_stats') and action in self._sync_stats:
            self._sync_stats[action] += 1
        if hasattr(self, '_sync_stats') and content_type in ("movie", "show"):
            self._sync_stats[f"{content_type}s"] += 1
    
    def get_sync_stats(self) -> Dict[str, int]:
        """Get per-run sync statistics."""
        return dict(self._sync_stats)
    
    def _send_telegram(self, action: str, **kwargs) -> None:
        """Send Telegram notification with error handling.
        
        Args:
            action: Notification action name ('added', 'upgraded', etc.) for logging
            **kwargs: Forwarded to the appropriate telegram.notify_* method
        """
        if not self.telegram or not self.telegram.is_configured():
            return
        try:
            if action == "added":
                self.telegram.notify_added(**kwargs)
            elif action == "upgraded":
                self.telegram.notify_upgraded(**kwargs)
        except (httpx.RequestError, OSError) as e:
            logger.debug("Telegram %s notification traceback:", action, exc_info=True)
            logger.debug("Failed to send Telegram %s notification: %s", action, e)
    
    def _is_hash_in_account(self, torrent_hash: Optional[str]) -> bool:
        """Check if a torrent hash already exists in the account.
        
        Args:
            torrent_hash: The hash to check (may be None or empty)
            
        Returns:
            True if hash exists in account, False otherwise
        """
        return bool(torrent_hash and torrent_hash.lower() in self.account_hashes)

    @staticmethod
    def _display_title(title: str, content_type: str, season_key: str = "unknown") -> str:
        """Build display title with optional season suffix."""
        if content_type == "show" and season_key != "unknown":
            return f"{title} ({season_key})"
        return title
    
    def should_filter(self, item: Dict[str, Any]) -> Tuple[bool, str]:
        """Check if item passes filters.

        Uses word boundary matching to avoid false positives
        (e.g., "TS" matching "Thunderbolts*").
        """
        min_year = self.filters.get("min_year", 2000)

        if item.get("year", 0) < min_year:
            return True, f"year {item.get('year')} < {min_year}"

        title = item.get("title", "")
        # Use pre-compiled patterns for efficiency
        for keyword, pattern in self._exclude_patterns:
            if pattern.search(title):
                return True, f"excluded keyword: {keyword}"

        return False, ""

    def _get_filter_config(self) -> Tuple[List[str], int]:
        """Get excluded sources and min resolution score from config."""
        excluded_sources = self.config.get("filters", {}).get("exclude", ["CAM", "TS", "HDCAM"])
        min_resolution_score = self.config.get("filters", {}).get("min_resolution_score", 800)
        return excluded_sources, min_resolution_score

    def _get_searcher_list(self) -> str:
        """Build a human-readable list of configured searchers."""
        searchers = []
        if self.torbox.searcher_zilean.is_configured():
            searchers.append("Zilean")
        if self.torbox.searcher_prowlarr.is_configured():
            searchers.append("Prowlarr")
        if self.torbox.searcher_jackett.is_configured():
            searchers.append("Jackett")
        return " → ".join(searchers) if searchers else "none configured"
    
    def process_content(self, content: Dict[str, Any], existing_torrents: Dict[str, str]) -> bool:
        """Process a single content item (add or upgrade).
        
        For TV shows, this processes ALL available seasons separately.
        For movies, processes as a single item.
        
        Args:
            content: Content item from Trakt
            existing_torrents: Mapping of IMDB IDs to Torbox torrent IDs from discovery phase
            
        Returns:
            True if any action was taken (add or upgrade), False otherwise
        """
        imdb_id = content.get("imdb_id", "")
        title = content.get("title", "Unknown")
        year = content.get("year", 0)
        content_type = content.get("type", "movie")
        
        logger.debug("Processing %s (%s, %s)", title, year, imdb_id)
        
        if not imdb_id:
            logger.warning("No IMDB ID for %s, skipping", title)
            return False
        
        # Check filters
        filtered, reason = self.should_filter(content)
        if filtered:
            logger.info("Filtered: %s (%s)", title, reason)
            log_result("skipped", title, {"reason": f"filtered: {reason}"})
            record_processed(imdb_id, title, year, content_type, "skipped", 
                           f"filtered: {reason}")
            self._increment_stats("skipped", content_type)
            return False
        
        # Process based on content type
        if content_type == "show":
            return self._process_show(content, existing_torrents)
        else:
            return self._process_movie(content, existing_torrents)
    
    def _process_movie(self, content: Dict[str, Any], existing_torrents: Dict[str, str]) -> bool:
        """Process a movie (single item)."""
        imdb_id = content.get("imdb_id", "")
        title = content.get("title", "Unknown")
        year = content.get("year", 0)
        
        # Check if already processed
        existing = get_processed_item(imdb_id, "unknown")
        
        # If already have max quality, skip searching entirely
        if existing and existing.get("torbox_id"):
            current_score = existing.get("quality_score") or 0
            if is_max_quality(current_score):
                logger.info("Max quality reached for %s (score: %d) - skipping search", 
                           title, current_score)
                log_result("skipped", title, {"reason": "max_quality", "score": current_score})
                return True
        
        # Check if already exists in Torbox (from discovery phase)
        torbox_id = existing_torrents.get(imdb_id)
        if torbox_id:
            logger.info("Already in Torbox: %s (torbox_id: %s)", title, torbox_id)
            record_processed(imdb_id, title, year, "movie", "skipped", 
                           "already_in_torbox", torbox_id=torbox_id,
                           quality_score=(existing or {}).get("quality_score") or 0)
            self._increment_stats("skipped", "movie")
            return True
        
        # Search for cached content
        search_query = normalize_search_query(f"{title} {year}" if year else title)
        searcher_list = self._get_searcher_list()
        logger.info("Searching indexers for: %s (using: %s)", search_query, searcher_list)
        excluded_sources, min_resolution_score = self._get_filter_config()
        cached = self.torbox.get_cached_torrents(search_query, "movie", excluded_sources, min_resolution_score, imdb_id)
        
        if not cached:
            logger.info("No cached results for: %s", title)
            log_result("skipped", title, {"reason": "not_cached"})
            record_processed(imdb_id, title, year, "movie", "skipped", 
                           "not_cached")
            self._increment_stats("skipped", "movie")
            return False
        
        # Get best available quality
        best = cached[0]
        logger.info("Best quality found: %s (score: %d)", best.quality.label, best.quality.score)
        
        # Check if already have this movie
        if existing and existing.get("torbox_id"):
            return self._handle_upgrade(imdb_id, title, year, "movie", existing, cached)
        
        # Check if best torrent's hash already exists in account (manual add or discovery miss)
        if self._is_hash_in_account(best.hash):
            logger.info("Already in Torbox: %s (hash match)", title)
            log_result("skipped", title, {"reason": "already_in_torbox_by_hash", "hash": best.hash[:16] if best.hash else "unknown"})
            record_processed(imdb_id, title, year, "movie", "skipped",
                           "already_in_torbox_by_hash",
                           quality_score=best.quality.score)
            self._increment_stats("skipped", "movie")
            return True
        
        return self._handle_new_addition(imdb_id, title, year, "movie", cached, 0)
    
    def _process_show(self, content: Dict[str, Any], existing_torrents: Dict[str, str]) -> bool:
        """Process a TV show (multiple seasons).
        
        This method:
        1. Searches for torrents
        2. Groups by season
        3. Processes each season independently
        4. Returns True if any season was added or upgraded
        """
        imdb_id = content.get("imdb_id", "")
        title = content.get("title", "Unknown")
        year = content.get("year", 0)
        
        # Check if already exists in Torbox (from discovery phase) - skip for now
        # Per-season discovery happens during individual season processing
        
        # Search for cached content
        search_query = normalize_search_query(f"{title} {year}" if year else title)
        searcher_list = self._get_searcher_list()
        logger.info("Searching indexers for show: %s (using: %s)", search_query, searcher_list)
        excluded_sources, min_resolution_score = self._get_filter_config()
        cached = self.torbox.get_cached_torrents(search_query, "show", excluded_sources, min_resolution_score, imdb_id)
        
        if not cached:
            logger.info("No cached results for: %s", title)
            log_result("skipped", title, {"reason": "not_cached"})
            record_processed(imdb_id, title, year, "show", "skipped", 
                           "not_cached")
            self._increment_stats("skipped", "show")
            return False
        
        # Group torrents by season
        seasons_map, skip_reason = self._group_by_season(cached)
        
        if not seasons_map:
            if skip_reason == "all_duplicates":
                logger.info("All torrents already in account for: %s", title)
                log_result("skipped", title, {"reason": "already_in_account"})
                record_processed(imdb_id, title, year, "show", "skipped", 
                               "already_in_account")
                self._increment_stats("skipped", "show")
            elif skip_reason == "no_season_info":
                logger.warning("Found torrents but couldn't parse season info for: %s", title)
                log_result("skipped", title, {"reason": "no_season_info"})
                record_processed(imdb_id, title, year, "show", "skipped", 
                               "no_season_info")
                self._increment_stats("skipped", "show")
            else:
                logger.warning("Found torrents but none selected for: %s", title)
                log_result("skipped", title, {"reason": "none_selected"})
                record_processed(imdb_id, title, year, "show", "skipped", 
                               "none_selected")
                self._increment_stats("skipped", "show")
            return False
        
        logger.info("Found %d unique seasons for %s", len(seasons_map), title)
        
        # Process each season
        any_action_taken = False
        for season_key, torrent in seasons_map.items():
            season_processed = self._process_season(
                imdb_id, title, year, season_key, torrent, existing_torrents
            )
            if season_processed:
                any_action_taken = True
        
        return any_action_taken
    
    def _group_by_season(self, cached: List[TorrentResult]) -> Tuple[Dict[str, TorrentResult], str]:
        """Group cached torrents by season with smart pack prioritization.
        
        Hierarchy: Series pack > Season packs > Episodes
        
        - Complete/Series packs get their own entry (key: "Complete")
        - Season packs (S01, S02, etc.) grouped by season
        - Individual episodes only used if NO pack available for that season
        
        Args:
            cached: List of TorrentResult objects (already sorted by quality)
            
        Returns:
            Tuple of (seasons_map, skip_reason) where:
            - seasons_map: Dict mapping season_key to best TorrentResult
            - skip_reason: String explaining why torrents were skipped (for better logging)
        """
        # Separate into categories
        complete_packs: List[TorrentResult] = []  # Complete series
        season_packs: Dict[int, List[TorrentResult]] = {}  # Season packs by season number
        episodes: Dict[int, List[TorrentResult]] = {}  # Individual episodes by season
        
        # Track why we might return empty results
        has_valid_season_info = False
        
        for torrent in cached:
            if torrent.season_info is None:
                continue
            has_valid_season_info = True
            
            info = torrent.season_info
            
            if info.is_complete:
                # Complete series pack
                complete_packs.append(torrent)
            elif info.is_pack:
                # Season pack (S01, S02, etc.)
                for season_num in info.seasons:
                    if season_num not in season_packs:
                        season_packs[season_num] = []
                    season_packs[season_num].append(torrent)
            else:
                # Individual episode - only use if we don't have a pack for this season
                for season_num in info.seasons:
                    if season_num not in episodes:
                        episodes[season_num] = []
                    episodes[season_num].append(torrent)
        
        # Build final result with prioritization and duplicate checking
        best_per_season: Dict[str, TorrentResult] = {}
        skipped_hashes = 0
        
        # Helper to check if hash is already in account
        def is_hash_duplicate(torrent: TorrentResult) -> bool:
            return self._is_hash_in_account(torrent.hash)
        
        # 1. Add complete series pack if available (highest priority)
        if complete_packs:
            # Find first complete pack not already in account
            for pack in complete_packs:
                if not is_hash_duplicate(pack):
                    best_per_season["Complete"] = pack
                    logger.debug("Selected complete series pack: %s (score: %d)",
                                pack.name, pack.quality.score)
                    break
            else:
                # All complete packs are duplicates
                skipped_hashes += len(complete_packs)
                logger.debug("Skipped %d complete series pack(s) - hash already in account",
                            len(complete_packs))
        
        # 2. Add season packs for each season (medium priority)
        for season_num, torrents in season_packs.items():
            season_key = f"S{season_num:02d}"
            if torrents:
                # Find first torrent not already in account
                for torrent in torrents:
                    if not is_hash_duplicate(torrent):
                        best_per_season[season_key] = torrent
                        logger.debug("Selected season pack for %s: %s (score: %d)",
                                   season_key, torrent.name, torrent.quality.score)
                        break
                else:
                    # All season packs are duplicates
                    skipped_hashes += 1
                    logger.debug("Skipped season pack %s - hash already in account", season_key)
        
        # 3. Add individual episodes only if no pack available for that season (lowest priority)
        # When no season pack is available, collect ALL individual episodes for the season
        for season_num, torrents in episodes.items():
            season_key = f"S{season_num:02d}"
            # Only add episodes if we don't have a season pack for this season
            if season_key not in best_per_season and torrents:
                episodes_added = 0
                episodes_skipped = 0
                # Add ALL episodes not already in account (not just the best one)
                for episode in torrents:
                    if not is_hash_duplicate(episode):
                        # Use episode-specific key (S01E01) instead of season key
                        episode_key = episode.season_info.season_label if episode.season_info else season_key
                        best_per_season[episode_key] = episode
                        episodes_added += 1
                    else:
                        episodes_skipped += 1
                
                if episodes_added > 0:
                    logger.debug("No season pack for %s, using %d episode(s): best=%s (score: %d)",
                               season_key, episodes_added, torrents[0].name, torrents[0].quality.score)
                if episodes_skipped > 0:
                    skipped_hashes += episodes_skipped
                    logger.debug("Skipped %d episode(s) for %s - hash already in account", 
                               episodes_skipped, season_key)
        
        if skipped_hashes > 0:
            logger.debug("Skipped %d torrent(s) with hash already in account", skipped_hashes)
        
        # 4. Resolve conflict: if Complete pack was selected alongside individual
        #    season packs, pick whichever offers better quality to avoid duplicates.
        if "Complete" in best_per_season:
            other_keys = [k for k in best_per_season if k != "Complete"]
            if other_keys:
                complete_score = best_per_season["Complete"].quality.score
                # Complete pack covers all seasons, so when it wins,
                # remove all individual entries (season packs and episodes).
                season_pack_keys = [k for k in other_keys if re.match(r'^S\d{2}$', k)]
                if season_pack_keys:
                    if any(best_per_season[k].quality.score > complete_score
                           for k in season_pack_keys):
                        del best_per_season["Complete"]
                        logger.debug("Preferring individual season packs over lower-quality "
                                    "complete pack (score: %d)", complete_score)
                    else:
                        for k in other_keys:
                            del best_per_season[k]
                        logger.debug("Complete pack (score: %d) preferred over individual "
                                    "season packs", complete_score)
        
        logger.info("Selected %d torrents: %s", 
                   len(best_per_season), 
                   ", ".join(f"{k}({v.quality.score})" for k, v in best_per_season.items()))
        
        # Determine why we might have empty results
        if not best_per_season:
            if skipped_hashes > 0 and has_valid_season_info:
                skip_reason = "all_duplicates"
            elif not has_valid_season_info:
                skip_reason = "no_season_info"
            else:
                skip_reason = "unknown"
        else:
            skip_reason = ""
        
        return best_per_season, skip_reason
    
    def _process_season(self, imdb_id: str, title: str, year: int, season_key: str,
                       torrent: TorrentResult, existing_torrents: Dict[str, str]) -> bool:
        """Process a single season of a TV show.
        
        Args:
            imdb_id: Show IMDB ID
            title: Show title
            year: Show year
            season_key: Season identifier (e.g., "S01", "Complete")
            torrent: Best torrent for this season
            existing_torrents: Mapping of discovered existing torrents
            
        Returns:
            True if season was added or upgraded
        """
        logger.debug("Processing season %s of %s", season_key, title)
        
        # FIRST: Check database for this specific season (per-season granularity)
        # This is the authoritative check - database records are always season-aware
        existing = get_processed_item(imdb_id, season_key)
        
        # CRITICAL: Check if record was created by discovery vs actually added.
        # Discovery creates records with action='skipped' and reason='already_in_torbox' 
        # or 'multi_season_pack_discovery' - these are PHANTOM records that associate
        # episodes with a torrent ID but never actually added the individual episode.
        # We must re-process these to actually add the content.
        is_phantom_record = False
        if existing and existing.get("action") == "skipped":
            reason = existing.get("reason", "")
            if reason in ("already_in_torbox", "multi_season_pack_discovery", "already_in_account"):
                is_phantom_record = True
                logger.debug("Phantom record detected for %s %s - will re-process for actual addition", 
                           title, season_key)
        
        # If already have max quality for this season, skip
        if existing and existing.get("torbox_id") and not is_phantom_record:
            current_score = existing.get("quality_score") or 0
            if is_max_quality(current_score):
                logger.info("Max quality reached for %s %s (score: %d) - skipping", 
                           title, season_key, current_score)
                return True
        
        # Check if this season is already in Torbox (database record with upgrade potential)
        # Skip phantom records - they need actual addition, not upgrade check
        if existing and existing.get("torbox_id") and not is_phantom_record:
            # For upgrades, pass a single-item list since we only check the best one
            return self._handle_upgrade(imdb_id, title, year, "show", existing, [torrent], season_key)
        
        # SECOND: Check if this show was already discovered in Torbox (from discovery phase)
        # This handles multi-season packs where discovery found the show but this
        # specific season doesn't have a database record yet (e.g., newly matched season)
        # 
        # CRITICAL: Skip this check for episode-level items (S01E01 format).
        # Discovery matches at the show/season-pack level, not episode level.
        # If we skip episodes here, they never get added even when they're missing.
        torbox_id = existing_torrents.get(imdb_id)
        is_episode = 'E' in season_key or len(season_key) > 3  # S01E01 vs S01
        
        if torbox_id and not existing and not is_episode:
            # Only skip for season-level items (S01, S02) or complete series
            # Episode-level items should proceed to hash check or addition
            display_title = self._display_title(title, "show", season_key)
            logger.info("Already in Torbox: %s (torbox_id: %s, discovered in multi-season pack)", 
                       display_title, torbox_id)
            record_processed(imdb_id, title, year, "show", "skipped",
                           "already_in_torbox", torbox_id=torbox_id,
                           quality_score=torrent.quality.score, season=season_key)
            self._increment_stats("skipped", "show")
            return True
        
        # Check if torrent hash already exists in account (manual add or discovery miss)
        if self._is_hash_in_account(torrent.hash):
            display_title = self._display_title(title, "show", season_key)
            logger.info("Already in Torbox: %s (hash match)", display_title)
            log_result("skipped", display_title, {"reason": "already_in_torbox_by_hash", "hash": torrent.hash[:16] if torrent.hash else "unknown"})
            record_processed(imdb_id, title, year, "show", "skipped",
                           "already_in_torbox_by_hash",
                           quality_score=torrent.quality.score, season=season_key)
            self._increment_stats("skipped", "show")
            return True
        
        # Pass as list to support fallback torrents
        return self._handle_new_addition(imdb_id, title, year, "show", [torrent], 0, season_key)
    
    def _handle_new_addition(self, imdb_id: str, title: str, year: int,
                              content_type: str, cached: List[TorrentResult],
                              torrent_index: int, season_key: str = "unknown") -> bool:
        """Handle adding new content (movie or show season).

        Tries each torrent in order until one succeeds. If all fail,
        records a permanent failure.
        """
        # Get the torrent to try at current index
        if torrent_index >= len(cached):
            # Exhausted all torrents
            display_title = self._display_title(title, content_type, season_key)
            logger.error("All %d torrent(s) failed to add for: %s", len(cached), display_title)
            record_processed(imdb_id, title, year, content_type, "failed",
                           "all_torrents_failed", season=season_key)
            log_result("failed", display_title, {"reason": "all_torrents_failed"})
            self._increment_stats("failed", content_type)
            return False

        torrent = cached[torrent_index]
        display_title = self._display_title(title, content_type, season_key)
        
        # DUPLICATE PREVENTION: Check if this hash already exists in account
        # This catches torrents that weren't matched during discovery (e.g., database records cleared)
        if self._is_hash_in_account(torrent.hash):
            logger.info("Skipping duplicate torrent (hash already in account): %s", display_title)
            log_result("skipped", display_title, {"reason": "already_in_account_by_hash", "hash": torrent.hash[:16] if torrent.hash else "unknown"})
            record_processed(imdb_id, title, year, content_type, "skipped",
                           "already_in_account_by_hash", 
                           quality_score=torrent.quality.score, season=season_key)
            return True

        if torrent_index == 0:
            logger.info("Adding new content: %s", display_title)
        else:
            logger.info("Trying next best torrent (%d/%d): %s",
                       torrent_index + 1, len(cached), torrent.quality.label)

        try:
            new_id = self.torbox.add_torrent(torrent.magnet, torrent.name)
        except RateLimitError:
            logger.warning("Rate limit hit when adding %s - will retry next run", display_title)
            log_result("skipped", display_title, {"reason": "rate_limited", "retry": True})
            self._increment_stats("skipped", content_type)
            return False

        if new_id:
            # Track hash in account set to prevent re-adding same hash this run
            if torrent.hash:
                self.account_hashes.add(torrent.hash.lower())
            record_processed(
                imdb_id, title, year, content_type, "added", "success",
                torbox_id=new_id, magnet=torrent.magnet,
                quality_score=torrent.quality.score, quality_label=torrent.quality.label,
                season=season_key
            )
            log_result("added", display_title,
                      {"quality": torrent.quality.label, "score": torrent.quality.score})
            self._increment_stats("added", content_type)
            self._send_telegram(
                "added",
                title=title,
                year=year,
                quality_label=torrent.quality.label,
                quality_score=torrent.quality.score,
                content_type=content_type,
                season=season_key if season_key != "unknown" else None,
                imdb_id=imdb_id
            )
            return True
        else:
            # Failed to add this torrent - try the next one if available
            logger.warning("Failed to add torrent: %s (trying next best)", torrent.name[:60])
            return self._handle_new_addition(imdb_id, title, year, content_type,
                                            cached, torrent_index + 1, season_key)
    
    def _handle_upgrade(self, imdb_id: str, title: str, year: int,
                         content_type: str, existing: Dict[str, Any],
                         cached: List[TorrentResult],
                         season_key: str = "unknown", torrent_index: int = 0) -> bool:
        """Handle upgrading existing content (movie or show season).

        Tries each torrent in order until one succeeds. If all fail,
        keeps the existing torrent.
        """
        display_title = self._display_title(title, content_type, season_key)
        current_score = existing.get("quality_score") or 0
        old_id = existing.get("torbox_id")

        # Check if we've exhausted all torrents
        if torrent_index >= len(cached):
            logger.error("All %d torrent(s) failed to add for upgrade: %s", len(cached), display_title)
            record_processed(
                imdb_id, title, year, content_type, "failed", "upgrade_all_failed",
                replaced_id=old_id, replaced_score=current_score,
                season=season_key
            )
            log_result("failed", display_title, {"reason": "upgrade_all_failed"})
            self._increment_stats("failed", content_type)
            return False

        torrent = cached[torrent_index]

        # Check if this is significant upgrade
        if not is_better_quality(torrent.quality.score, current_score):
            logger.debug("Current quality sufficient for %s (score: %d vs %d)",
                        display_title, current_score, torrent.quality.score)
            log_result("skipped", display_title, {"reason": "current_better", "score": current_score})
            record_processed(imdb_id, title, year, content_type, "skipped",
                           "current_better", torbox_id=old_id,
                           quality_score=current_score, season=season_key)
            return False

        if torrent_index == 0:
            logger.info("Upgrade detected for %s: %d -> %d",
                       display_title, current_score, torrent.quality.score)
        else:
            logger.info("Trying next best torrent for upgrade (%d/%d): %s",
                       torrent_index + 1, len(cached), torrent.quality.label)

        # Add new torrent FIRST (safer - if this fails, we still have old)
        try:
            new_id = self.torbox.add_torrent(torrent.magnet, torrent.name)
        except RateLimitError:
            logger.warning("Rate limit hit during upgrade for %s - will retry next run", display_title)
            log_result("skipped", display_title, {"reason": "rate_limited", "retry": True})
            self._increment_stats("skipped", content_type)
            return False

        if not new_id:
            # Failed to add this torrent - try the next one if it's still better
            logger.warning("Failed to add upgrade torrent: %s (trying next best)", torrent.name[:60])
            return self._handle_upgrade(imdb_id, title, year, content_type, existing,
                                       cached, season_key, torrent_index + 1)

        # New torrent added successfully - now safe to remove old
        logger.debug("New torrent added: %s, removing old: %s", new_id, old_id)
        if self.torbox.remove_torrent(old_id):
            logger.debug("Removed old torrent: %s", old_id)
        else:
            logger.warning("Failed to remove old torrent: %s (but new one is added)", old_id)

        # Track new hash in account set to prevent re-adding same hash this run
        if torrent.hash:
            self.account_hashes.add(torrent.hash.lower())

        # Record successful upgrade
        record_processed(
            imdb_id, title, year, content_type, "upgraded", "quality_better",
            torbox_id=new_id, magnet=torrent.magnet,
            quality_score=torrent.quality.score, quality_label=torrent.quality.label,
            replaced_id=old_id, replaced_score=current_score,
            season=season_key
        )
        log_result("upgraded", display_title, {
            "quality": torrent.quality.label,
            "score": torrent.quality.score,
            "old_score": current_score
        })
        self._increment_stats("upgraded", content_type)
        old_quality = existing.get("quality_label", "Unknown")
        self._send_telegram(
            "upgraded",
            title=title,
            year=year,
            old_quality=old_quality,
            old_score=current_score,
            new_quality=torrent.quality.label,
            new_score=torrent.quality.score,
            content_type=content_type,
            season=season_key if season_key != "unknown" else None
        )
        return True
    
    def sync(self):
        """Run full sync."""
        # BUG-R2 FIX: Ensure database migration runs BEFORE any database queries
        migrate_db()
        
        sources = self.config.get("sources", [])
        
        logger.info("="*60)
        logger.info("Starting sync - %s", datetime.now(timezone.utc).isoformat())
        logger.info("Sources: %s", ", ".join(sources))
        logger.info("="*60)
        
        # STEP 1: Fetch all content from Trakt (NO LIMITS)
        logger.info("Fetching content from Trakt...")
        content = self.trakt.get_all_content(sources)
        logger.info("Found %d items from Trakt", len(content))
        
        # STEP 2: Sort so missing/incomplete items come FIRST
        # Discovery adds items to the database, so we must check state BEFORE
        # discovery runs. Otherwise everything looks "existing".
        # 
        # Prioritize shows that likely need episode collection:
        # - NEW: not in database at all
        # - INCOMPLETE: has individual episodes (S01E01) rather than season packs (S01)
        #   This catches shows like The Pitt where only S02E07 exists and we need more
        # - EXISTING: has at least one season pack (complete coverage for that season)
        def _get_priority(item: Dict[str, Any]) -> int:
            """Return priority tier: 0=new, 1=incomplete, 2=existing."""
            imdb_id = item.get("imdb_id", "")
            if not imdb_id:
                return 0  # No IMDb ID = treat as new
            
            if item.get("type") == "show":
                seasons = get_processed_show_seasons(imdb_id)
                if not seasons:
                    return 0  # New show - not in database
                # Check for individual episodes (S01E01 format vs S01 format)
                # Any episode-level entry means incomplete season coverage
                has_episodes = any(
                    'E' in s.get('season', '') or len(s.get('season', '')) > 3
                    for s in seasons
                )
                if has_episodes:
                    return 1  # Incomplete - has individual episodes that likely need more
                return 2  # Existing - only has season-level entries
            else:
                # Movies - binary: exists or not
                return 0 if get_processed_item(imdb_id) is None else 2
        
        new_items = [item for item in content if _get_priority(item) == 0]
        incomplete_items = [item for item in content if _get_priority(item) == 1]
        existing_items = [item for item in content if _get_priority(item) == 2]
        
        content = new_items + incomplete_items + existing_items
        
        total_missing = len(new_items) + len(incomplete_items)
        if total_missing > 0:
            logger.info("Prioritizing %d new + %d incomplete items before %d existing items", 
                       len(new_items), len(incomplete_items), len(existing_items))
        
        # STEP 3: Discover existing torrents in Torbox (AFTER sorting)
        # This prevents re-adding duplicates from previous runs during processing
        discovery_result = discover_existing_torrents(self.torbox)
        
        # Verify and clear dropped torrents (only if discovery succeeded)
        # Safety: If discovery failed (None), we use empty dict and continue
        # This may re-add some items but won't lose data
        cleared_count = verify_and_clear_dropped_torrents(discovery_result)
        if cleared_count > 0:
            logger.info("Cleared %d dropped torrent records - will re-add if available", cleared_count)
        
        # Handle discovery failure gracefully
        if discovery_result is None:
            logger.warning("Discovery failed - proceeding with empty existing_torrents (may re-add items)")
            existing_torrents = {}
            self.account_hashes = set()
        else:
            existing_torrents, self.account_hashes = discovery_result
        
        logger.info("Discovered %d existing torrents in Torbox (%d unique hashes)", 
                   len(existing_torrents), len(self.account_hashes))
        
        # Reset per-run stats
        self._sync_stats = {"added": 0, "upgraded": 0, "skipped": 0, "failed": 0, "movies": 0, "shows": 0}
        
        # Process each item
        for i, item in enumerate(content, 1):
            content_type = item.get('type', 'movie')
            type_label = "show" if content_type == "show" else "movie"
            logger.info("[%d/%d] %s (%s) [%s]", i, len(content), 
                       item.get('title', 'Unknown'), item.get('year', '?'), type_label)
            
            self.process_content(item, existing_torrents)
        
        logger.info("="*60)
        logger.info("Sync complete!")
        logger.info("  Added:    %d", self._sync_stats["added"])
        logger.info("  Upgraded: %d", self._sync_stats["upgraded"])
        logger.info("  Skipped:  %d", self._sync_stats["skipped"])
        logger.info("  Failed:   %d", self._sync_stats["failed"])
        logger.info("="*60)


# ============================================================================
# TEST & VERIFICATION
# ============================================================================

def run_self_test():
    """Run a comprehensive self-test without requiring API keys.
    
    This allows users to verify their setup before running actual syncs.
    Tests include:
    1. Environment file validation
    2. Database connectivity
    3. Dependencies check
    4. Configuration validation
    5. Dry-run of quality parsing
    
    Returns:
        True if all tests pass, False otherwise
    """
    all_passed = True
    results = []
    
    logger.info("="*60)
    logger.info("TorBoxed Self-Test")
    logger.info("="*60)
    
    # Test 1: Environment file
    logger.info("\n[1/6] Checking environment file...")
    if ENV_PATH.exists():
        env = load_env()
        has_torbox = bool(env.get("TORBOX_API_KEY"))
        has_trakt_id = bool(env.get("TRAKT_CLIENT_ID"))
        has_trakt_secret = bool(env.get("TRAKT_CLIENT_SECRET"))
        
        if has_torbox and has_trakt_id:
            logger.info("  ✓ .env file exists with API keys")
            results.append(("Environment file", True, "Found with API keys"))
        else:
            missing = []
            if not has_torbox:
                missing.append("TORBOX_API_KEY")
            if not has_trakt_id:
                missing.append("TRAKT_CLIENT_ID")
            logger.warning("  ⚠ .env exists but missing: %s", ", ".join(missing))
            results.append(("Environment file", False, f"Missing: {', '.join(missing)}"))
            all_passed = False
    else:
        logger.error("  ✗ .env file not found")
        results.append(("Environment file", False, "File not found"))
        all_passed = False
    
    # Test 2: Database
    logger.info("\n[2/6] Checking database...")
    try:
        if DB_PATH.exists():
            with get_db() as conn:
                # Test read
                conn.execute("SELECT 1").fetchone()
                # Check tables
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                table_names = [t[0] for t in tables]
                has_config = "config" in table_names
                has_processed = "processed" in table_names
                
                if has_config and has_processed:
                    logger.info("  ✓ Database exists with required tables")
                    results.append(("Database", True, "All tables present"))
                else:
                    missing_tables = []
                    if not has_config:
                        missing_tables.append("config")
                    if not has_processed:
                        missing_tables.append("processed")
                    logger.warning("  ⚠ Database missing tables: %s", ", ".join(missing_tables))
                    results.append(("Database", False, f"Missing: {', '.join(missing_tables)}"))
                    all_passed = False
        else:
            logger.warning("  ⚠ Database not initialized (run --init)")
            results.append(("Database", False, "Not initialized"))
            all_passed = False
    except sqlite3.Error as e:
        logger.debug("Database check traceback:", exc_info=True)
        logger.error("  ✗ Database error: %s", e)
        results.append(("Database", False, str(e)))
        all_passed = False
    
    # Test 3: Dependencies
    logger.info("\n[3/6] Checking dependencies...")
    try:
        import httpx
        logger.info("  ✓ httpx installed (%s)", httpx.__version__)
        results.append(("httpx", True, httpx.__version__))
    except ImportError:
        logger.error("  ✗ httpx not installed")
        results.append(("httpx", False, "Not installed"))
        all_passed = False
    
    try:
        from guessit import __version__ as guessit_version
        logger.info("  ✓ guessit installed (%s)", guessit_version)
        results.append(("guessit", True, guessit_version))
    except ImportError:
        logger.error("  ✗ guessit not installed")
        results.append(("guessit", False, "Not installed"))
        all_passed = False
    
    # Test 4: Python version
    logger.info("\n[4/6] Checking Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 9:
        logger.info("  ✓ Python %d.%d.%d (compatible)", version.major, version.minor, version.micro)
        results.append(("Python version", True, f"{version.major}.{version.minor}.{version.micro}"))
    else:
        logger.error("  ✗ Python %d.%d.%d (requires 3.9+)", version.major, version.minor, version.micro)
        results.append(("Python version", False, f"{version.major}.{version.minor}.{version.micro}"))
        all_passed = False
    
    # Test 5: Quality parsing (dry-run)
    logger.info("\n[5/6] Testing quality parsing...")
    try:
        test_names = [
            "Movie.2024.1080p.BluRay.x264-TEST",
            "Show.S01E01.2160p.WEB-DL.H265-TEST",
            "Movie.2023.720p.HDTV.x264-TEST",
        ]
        for name in test_names:
            quality = parse_quality(name)
            logger.info("  ✓ '%s' -> score: %d", name, quality.score)
        results.append(("Quality parsing", True, "Working"))
    except (ValueError, TypeError) as e:
        logger.debug("Quality parsing traceback:", exc_info=True)
        logger.error("  ✗ Quality parsing failed: %s", e)
        results.append(("Quality parsing", False, str(e)))
        all_passed = False
    
    # Test 6: Configuration validation
    logger.info("\n[6/6] Checking configuration...")
    try:
        if DB_PATH.exists():
            config = get_config()
            sources = config.get("sources", [])
            filters = config.get("filters", {})
            
            if sources:
                logger.info("  ✓ Config loaded: %d sources, filters=%s", 
                           len(sources), bool(filters))
                results.append(("Configuration", True, f"{len(sources)} sources"))
            else:
                logger.warning("  ⚠ Config has no sources")
                results.append(("Configuration", False, "No sources"))
                all_passed = False
        else:
            logger.warning("  ⚠ Cannot check config (database not initialized)")
            results.append(("Configuration", False, "DB not initialized"))
    except (sqlite3.Error, json.JSONDecodeError) as e:
        logger.debug("Configuration traceback:", exc_info=True)
        logger.error("  ✗ Config error: %s", e)
        results.append(("Configuration", False, str(e)))
        all_passed = False
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("Test Summary")
    logger.info("="*60)
    
    passed = sum(1 for _, result, _ in results if result)
    failed = sum(1 for _, result, _ in results if not result)
    
    for test_name, result, details in results:
        symbol = "✓" if result else "✗"
        status = "PASS" if result else "FAIL"
        logger.info("  [%s] %-25s: %s (%s)", symbol, test_name, status, details)
    
    logger.info("-"*60)
    if all_passed:
        logger.info("All tests passed! ✓")
        logger.info("Run 'uv run torboxed.py' to start syncing.")
    else:
        logger.warning("Some tests failed. Please fix the issues above.")
        if failed > 0:
            logger.info("\nRecommendations:")
            if not any(r[0] == "Environment file" and r[1] for r in results):
                logger.info("  • Create .env file: cat > .env << 'EOF'")
                logger.info("      TORBOX_API_KEY=your_key_here")
                logger.info("      TRAKT_CLIENT_ID=your_id_here")
                logger.info("      TRAKT_CLIENT_SECRET=your_secret_here")
                logger.info("      EOF")
            if not any(r[0] == "Database" and r[1] for r in results):
                logger.info("  • Initialize database: uv run torboxed.py --init")
            if not any(r[0] == "httpx" and r[1] for r in results):
                logger.info("  • Install dependencies: uv sync")
    
    logger.info("="*60)
    return all_passed


# ============================================================================
# CRON SETUP
# ============================================================================

def setup_cron():
    """Interactive cron job setup for daily automation.
    
    Guides the user through setting up a cron job for daily sync.
    Can either add a crontab entry or show instructions for systemd.
    """
    logger.info("="*60)
    logger.info("TorBoxed Cron Setup")
    logger.info("="*60)
    logger.info("This will help you set up automated daily syncing.")
    logger.info("")
    
    # Get current working directory
    cwd = Path.cwd()
    logger.info("Working directory: %s", cwd)
    
    # Check for uv
    uv_path = shutil.which("uv")
    if uv_path:
        logger.info("uv found at: %s", uv_path)
    else:
        logger.error("uv not found in PATH. Cannot create cron job.")
        logger.info("Please install uv first:")
        logger.info("  curl -LsSf https://astral.sh/uv/install.sh | sh")
        logger.info("Or add uv to your PATH and try again.")
        return  # Exit early, don't create broken cron job
    
    logger.info("")
    logger.info("Choose your automation method:")
    logger.info("  1. Cron (simple, works on all systems)")
    logger.info("  2. Systemd timer (Linux with systemd)")
    logger.info("  3. Just show instructions (dry-run)")
    logger.info("")
    
    # Get user choice
    choice = input("Enter choice [1/2/3]: ").strip() or "3"
    
    if choice == "1":
        _setup_cron_crontab(cwd, uv_path)
    elif choice == "2":
        _setup_systemd_timer(cwd, uv_path)
    else:
        _show_cron_instructions(cwd, uv_path)
    
    logger.info("")
    logger.info("="*60)
    logger.info("Cron setup complete!")
    logger.info("="*60)


def _setup_cron_crontab(cwd: Path, uv_path: str):
    """Setup using crontab with shell escaping to prevent command injection.
    
    VULN-002 Fix: Uses shlex.quote to properly escape all shell arguments.
    Validates custom cron expressions to prevent injection attacks.
    """
    logger.info("\nSetting up cron job...")
    logger.info("Available schedules:")
    logger.info("  1. Daily at 2:00 AM (recommended)")
    logger.info("  2. Daily at 6:00 AM")
    logger.info("  3. Twice daily at 6:00 AM and 6:00 PM")
    logger.info("  4. Every 12 hours (12:00 AM and 12:00 PM)")
    logger.info("  5. Custom (you'll enter the cron expression)")
    logger.info("")
    
    schedule_choice = input("Choose schedule [1/2/3/4/5]: ").strip() or "1"
    
    schedules = {
        "1": "0 2 * * *",
        "2": "0 6 * * *",
        "3": "0 6,18 * * *",
        "4": "0 */12 * * *",
    }
    
    if schedule_choice in schedules:
        schedule = schedules[schedule_choice]
    elif schedule_choice == "5":
        # VULN-002: Validate custom cron expression to prevent injection
        custom_schedule = input("Enter cron expression (e.g., '0 2 * * *'): ").strip()
        if not custom_schedule:
            schedule = "0 2 * * *"
        elif not _validate_cron_expression(custom_schedule):
            logger.error("Invalid cron expression format. Expected 5 fields (minute hour day month weekday)")
            logger.info("Example: '0 2 * * *' for daily at 2 AM")
            return
        else:
            schedule = custom_schedule
    else:
        logger.error("Invalid choice. Please select 1-5.")
        return
    
    # VULN-002: Validate inputs before use
    if not cwd.exists():
        logger.error("Invalid working directory: %s", cwd)
        return
    if not uv_path or not Path(uv_path).exists():
        logger.error("Invalid uv path: %s", uv_path)
        return
    
    # VULN-002: Use shlex.quote to properly escape shell arguments
    safe_cwd = shlex.quote(str(cwd))
    safe_uv_path = shlex.quote(uv_path)
    log_file = cwd / "torboxed-cron.log"
    safe_log_file = shlex.quote(str(log_file))
    
    # Create the cron command with escaped paths
    cron_cmd = f'cd {safe_cwd} && {safe_uv_path} run torboxed.py > {safe_log_file} 2>&1'
    cron_line = f"{schedule} {cron_cmd}"
    
    logger.info("\nCron job to add:")
    logger.info("  %s", cron_line)
    logger.info("")
    
    confirm = input("Add this to crontab? [y/N]: ").strip().lower()
    
    if confirm == "y":
        try:
            # Get current crontab
            result = subprocess.run(
                ["crontab", "-l"],
                capture_output=True,
                text=True
            )
            current_crontab = result.stdout if result.returncode == 0 else ""
            
            # Check if entry already exists
            if "torboxed.py" in current_crontab:
                logger.warning("TorBoxed cron job already exists!")
                replace = input("Replace existing? [y/N]: ").strip().lower()
                if replace != "y":
                    logger.info("Cancelled.")
                    return
                # Remove existing lines
                lines = current_crontab.split("\n")
                lines = [l for l in lines if "torboxed.py" not in l]
                current_crontab = "\n".join(lines)
            
            # Add new entry
            new_crontab = current_crontab.rstrip() + "\n" + cron_line + "\n"
            
            # Install new crontab
            process = subprocess.Popen(
                ["crontab", "-"],
                stdin=subprocess.PIPE,
                text=True
            )
            process.communicate(input=new_crontab)
            
            if process.returncode == 0:
                logger.info("✓ Cron job added successfully!")
                logger.info("Log file: %s", log_file)
                logger.info("\nTo verify, run: crontab -l")
            else:
                logger.error("Failed to add cron job")
        except (OSError, subprocess.SubprocessError) as e:
            logger.debug("Cron setup traceback:", exc_info=True)
            logger.error("Error setting up cron: %s", e)
            logger.info("\nYou can manually add this to crontab:")
            logger.info("  %s", cron_line)
    else:
        logger.info("Cancelled. You can manually add:")
        logger.info("  %s", cron_line)


def _setup_systemd_timer(cwd: Path, uv_path: str):
    """Show systemd timer setup instructions."""
    service_name = input("Service name [torboxed]: ").strip() or "torboxed"
    
    service_content = f"""[Unit]
Description=TorBoxed Sync
After=network.target

[Service]
Type=oneshot
WorkingDirectory={cwd}
Environment="PATH=$HOME/.local/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart={uv_path} run torboxed.py
User={os.getenv('USER', 'yourusername')}
"""
    
    timer_content = f"""[Unit]
Description=Run TorBoxed daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
"""
    
    # Write files to /tmp so install instructions are actionable
    service_path = Path(f"/tmp/{service_name}.service")
    timer_path = Path(f"/tmp/{service_name}.timer")
    service_path.write_text(service_content)
    timer_path.write_text(timer_content)
    
    logger.info("\nSystemd service file: %s", service_path)
    logger.info("-"*60)
    logger.info(service_content.strip())
    logger.info("-"*60)
    
    logger.info("\nSystemd timer file: %s", timer_path)
    logger.info("-"*60)
    logger.info(timer_content.strip())
    logger.info("-"*60)
    
    logger.info("\nTo install:")
    logger.info("  sudo cp %s /etc/systemd/system/", service_path)
    logger.info("  sudo cp %s /etc/systemd/system/", timer_path)
    logger.info("  sudo systemctl daemon-reload")
    logger.info("  sudo systemctl enable %s.timer", service_name)
    logger.info("  sudo systemctl start %s.timer", service_name)
    logger.info("  sudo systemctl list-timers | grep %s", service_name)


def _show_cron_instructions(cwd: Path, uv_path: str):
    """Show instructions for manual setup."""
    log_file = cwd / "torboxed-cron.log"
    
    logger.info("\n--- Cron Setup Instructions ---\n")
    logger.info("Add this line to your crontab (run: crontab -e):")
    logger.info("")
    logger.info("# Daily at 2:00 AM")
    logger.info('0 2 * * * cd %s && %s run torboxed.py > %s 2>&1',
               cwd, uv_path, log_file)
    logger.info("")
    logger.info("Or for every 12 hours:")
    logger.info('0 */12 * * * cd %s && %s run torboxed.py > %s 2>&1',
               cwd, uv_path, log_file)
    logger.info("")
    logger.info("--- Systemd Timer Instructions ---\n")
    logger.info("See README.md for full systemd setup instructions.")


def _setup_telegram_interactive():
    """Interactive setup for Telegram notifications during --init.
    
    Guides user through configuring Telegram bot for notifications.
    Creates or updates .env file with TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.
    """
    logger.info("\n" + "="*60)
    logger.info("Telegram Notifications Setup (Optional)")
    logger.info("="*60)
    logger.info("Get notified when TorBoxed adds or upgrades content!")
    logger.info("")
    logger.info("To set up Telegram notifications:")
    logger.info("1. Message @BotFather on Telegram and create a new bot")
    logger.info("2. Copy the bot token provided")
    logger.info("3. Message @userinfobot to get your Chat ID")
    logger.info("")
    
    choice = input("Set up Telegram notifications now? [y/N]: ").strip().lower()
    
    if choice != "y":
        logger.info("Skipping Telegram setup. You can add it later to your .env file.")
        logger.info("See README.md for manual configuration instructions.")
        return
    
    logger.info("")
    bot_token = input("Enter your Telegram Bot Token: ").strip()
    
    if not bot_token:
        logger.warning("No bot token provided. Skipping Telegram setup.")
        return
    
    chat_id = input("Enter your Telegram Chat ID: ").strip()
    
    if not chat_id:
        logger.warning("No chat ID provided. Skipping Telegram setup.")
        return
    
    # Validate the token format (should be numbers:alphanumeric)
    if ":" not in bot_token or len(bot_token) < 20:
        logger.warning("Bot token format looks invalid. Expected format: 123456789:ABCdef...")
        confirm = input("Continue anyway? [y/N]: ").strip().lower()
        if confirm != "y":
            return
    
    # Test the configuration
    logger.info("\nTesting Telegram configuration...")
    notifier = TelegramNotifier(bot_token=bot_token, chat_id=chat_id)
    
    if not notifier.is_configured():
        logger.error("Invalid configuration. Please try again.")
        return
    
    # Send test message
    test_result = notifier._send_message(
        "TorBoxed test message! Notifications are now configured. You'll receive updates when content is added or upgraded."
    )
    
    notifier.close()
    
    if test_result:
        logger.info("Test message sent successfully!")
        
        # Update .env file
        env_lines = []
        telegram_vars = {
            "TELEGRAM_BOT_TOKEN": bot_token,
            "TELEGRAM_CHAT_ID": chat_id
        }
        
        if ENV_PATH.exists():
            # Read existing .env
            with open(ENV_PATH, "r") as f:
                env_lines = f.readlines()
            
            # Remove existing Telegram lines
            env_lines = [line for line in env_lines if not line.startswith("TELEGRAM_")]
        
        # Add Telegram variables
        env_lines.append(f"\n# Telegram Notifications\n")
        for key, value in telegram_vars.items():
            env_lines.append(f"{key}={value}\n")
        
        # Write back
        with open(ENV_PATH, "w") as f:
            f.writelines(env_lines)
        
        logger.info("Telegram configuration saved to .env file!")
        logger.info("You'll receive notifications when:")
        logger.info("  - New content is added to Torbox")
        logger.info("  - Content is upgraded to better quality")
    else:
        logger.error("Failed to send test message. Please check your bot token and chat ID.")
        logger.info("You can try setting this up later by adding these to your .env file:")
        logger.info(f"  TELEGRAM_BOT_TOKEN={bot_token}")
        logger.info(f"  TELEGRAM_CHAT_ID={chat_id}")


def show_cron_status():
    """Show current cron job status."""
    logger.info("="*60)
    logger.info("TorBoxed Cron Status")
    logger.info("="*60)
    
    try:
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.info("No crontab found for current user.")
            return
        
        lines = result.stdout.split("\n")
        torboxed_lines = [l for l in lines if "torboxed" in l.lower()]
        
        if torboxed_lines:
            logger.info("Found TorBoxed cron jobs:")
            for line in torboxed_lines:
                if line.strip() and not line.startswith("#"):
                    logger.info("  • %s", line)
        else:
            logger.info("No TorBoxed cron jobs found.")
            logger.info("Run 'uv run torboxed.py --cron-setup' to set up automation.")
        
        # Check for log files
        log_files = list(Path.cwd().glob("torboxed*.log*"))
        if log_files:
            logger.info("\nLog files found:")
            for log in sorted(log_files):
                size = log.stat().st_size
                logger.info("  • %s (%.1f KB)", log.name, size/1024)
                
    except (OSError, subprocess.SubprocessError) as e:
        logger.debug("Cron status traceback:", exc_info=True)
        logger.error("Error checking cron status: %s", e)

def show_stats():
    """Display sync statistics."""
    stats = get_stats()
    
    logger.info("="*60)
    logger.info("TorBoxed Sync Statistics")
    logger.info("="*60)
    
    logger.info("Total items processed: %d", stats.get('total', 0))
    
    logger.info("By action:")
    for action, count in stats.get('by_action', {}).items():
        logger.info("  %-12s: %d", action, count)
    
    logger.info("By type:")
    for ctype, count in stats.get('by_type', {}).items():
        logger.info("  %-12s: %d", ctype, count)
    
    # Count TV seasons
    try:
        with get_db() as conn:
            season_count = conn.execute(
                "SELECT COUNT(*) as count FROM processed WHERE content_type='show'"
            ).fetchone()
            if season_count and season_count['count'] > 0:
                logger.info("  %-12s: %d seasons/episodes", "TV seasons", season_count['count'])
    except sqlite3.Error:
        # Ignore database errors when just displaying stats
        pass
    
    recent_upgrades = stats.get('recent_upgrades', [])
    if recent_upgrades:
        logger.info("Recent upgrades:")
        for upgrade in recent_upgrades:
            season_info = f" [{upgrade.get('season', '')}]" if upgrade.get('season') and upgrade.get('season') != 'unknown' else ""
            logger.info("  • %s%s (%s)", upgrade['title'], season_info, upgrade['year'])
            logger.info("    Score: %d -> %d", upgrade.get('replaced_score', 0), upgrade.get('quality_score', 0))
            logger.info("    At: %s", upgrade.get('processed_at', 'unknown'))
    
    logger.info("="*60)


def show_recent(limit: int = 10):
    """Display recently processed items."""
    items = get_recent(limit)
    
    logger.info("="*60)
    logger.info("Last %d Processed Items", len(items))
    logger.info("="*60)
    
    for i, item in enumerate(items, 1):
        action_symbol = {
            "added": "+",
            "upgraded": "↑",
            "skipped": "→",
            "failed": "✗"
        }.get(item.get("action", ""), "?")
        
        # Build title with season info for shows
        title = item.get('title', 'Unknown')
        season = item.get('season', 'unknown')
        if item.get('content_type') == 'show' and season and season != 'unknown':
            title = f"{title} [{season}]"
        
        logger.info("%2d. [%s] %s (%s)", i, action_symbol, title, item.get('year', '?'))
        logger.info("     Action: %s | Reason: %s", item.get('action', 'unknown'), item.get('reason', 'unknown'))
        logger.info("     Type: %s | IMDB: %s", item.get('content_type', 'unknown'), item.get('imdb_id', 'unknown'))
        if item.get('quality_label'):
            logger.info("     Quality: %s (score: %d)", item.get('quality_label'), item.get('quality_score', 0))
        logger.info("     At: %s", item.get('processed_at', 'unknown'))
    
    logger.info("="*60)


def check_and_acquire_lock() -> bool:
    """Check if another instance is running and acquire lock if not.

    Uses UID-based lock file in /tmp for consistent path across
    all execution environments (cron, shell, systemd).
    Symlink-safe with atomic O_CREAT|O_EXCL creation.

    Returns True if lock acquired, False if another instance is running.
    """
    import atexit
    
    # Ensure parent directory exists
    lock_dir = LOCK_PATH.parent
    lock_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if lock file exists
    if LOCK_PATH.exists():
        # Security: Check if it's a symlink (could be pointing to sensitive file)
        if LOCK_PATH.is_symlink():
            logger.warning("Lock file is a symlink - potential attack detected. Removing.")
            LOCK_PATH.unlink()
        elif not LOCK_PATH.is_file():
            logger.warning("Lock file is not a regular file. Cannot acquire lock.")
            return False
        else:
            # Check if the PID is still running
            try:
                old_pid = int(LOCK_PATH.read_text().strip())
                # Check if process exists (kill -0 check)
                os.kill(old_pid, 0)
                # Process is still running
                return False
            except (ValueError, OSError, ProcessLookupError):
                # Stale lock file, process not running
                LOCK_PATH.unlink()
    
    # Create lock file atomically with O_CREAT | O_EXCL
    # This prevents race conditions where another process creates the file
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        with os.fdopen(fd, 'w') as f:
            f.write(str(os.getpid()))
    except FileExistsError:
        # Another process created the file between our check and open
        return False
    
    # Register cleanup on exit
    def release_lock():
        try:
            LOCK_PATH.unlink(missing_ok=True)
        except (OSError, PermissionError):
            # Ignore lock cleanup errors
            pass
    
    atexit.register(release_lock)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="torboxed - Sync Trakt.tv content to Torbox",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --init              # Initialize database
  %(prog)s                     # Run sync
  %(prog)s --verbose           # Run sync with verbose logging
  %(prog)s --test              # Run self-test without API keys
  %(prog)s --cron-setup        # Set up automated syncing
  %(prog)s --cron-status       # Check cron job status
  %(prog)s --stats             # Show statistics
  %(prog)s --recent            # Show last 10 processed items
  %(prog)s --reset tt1234567   # Reset item for re-processing
  %(prog)s --cleanup-unmatched # Remove untracked torrents (orphaned)
        """
    )
    
    parser.add_argument("--init", action="store_true",
                       help="Initialize database with default config")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose (DEBUG) logging")
    parser.add_argument("--test", action="store_true",
                       help="Run self-test to verify setup (no API keys needed)")
    parser.add_argument("--cron-setup", action="store_true",
                       help="Interactive setup for daily cron automation")
    parser.add_argument("--cron-status", action="store_true",
                       help="Show current cron job status")
    parser.add_argument("--stats", action="store_true",
                       help="Show sync statistics")
    parser.add_argument("--recent", nargs="?", const=10, type=int, metavar="N",
                       help="Show last N processed items (default: 10)")
    parser.add_argument("--reset", metavar="IMDB_ID",
                       help="Reset specific item for re-processing")
    parser.add_argument("--cleanup-unmatched", action="store_true",
                       help="Remove torrents from Torbox that can't be matched to database (orphaned)")
    
    args = parser.parse_args()
    
    # Setup logging early (before any output)
    setup_logging(verbose=args.verbose)
    
    # Check for overlapping runs (but skip for non-sync commands)
    skip_lock_commands = {'--init', '--test', '--cron-setup', '--cron-status', '--stats', '--recent', '--reset', '--cleanup-unmatched'}
    if not any(getattr(args, cmd.lstrip('-').replace('-', '_'), False) for cmd in skip_lock_commands):
        if not check_and_acquire_lock():
            logger.warning("Another instance of torboxed is already running. Exiting.")
            sys.exit(0)
    
    # Initialize
    if args.init:
        init_db()
        _setup_telegram_interactive()
        return
    
    # Self-test (no API keys needed)
    if args.test:
        success = run_self_test()
        sys.exit(0 if success else 1)
    
    # Cron setup (no API keys needed)
    if args.cron_setup:
        setup_cron()
        return
    
    # Cron status (no API keys needed)
    if args.cron_status:
        show_cron_status()
        return
    
    # Ensure database exists
    if not DB_PATH.exists():
        logger.error("Database not found. Run: python torboxed.py --init")
        sys.exit(1)
    
    # Run database migration if needed (for existing databases)
    migrate_db()
    
    # Stats
    if args.stats:
        show_stats()
        return
    
    # Recent
    if args.recent:
        show_recent(args.recent)
        return
    
    # Reset
    if args.reset:
        deleted_count = reset_item(args.reset)
        if deleted_count > 0:
            logger.info("Reset %s for re-processing (%d record(s) deleted)", args.reset, deleted_count)
        else:
            logger.error("Item %s not found in database", args.reset)
        return
    
    # Cleanup unmatched torrents
    if args.cleanup_unmatched:
        cleanup_unmatched_torrents()
        return
    
    # Check API keys (lazy-loaded)
    torbox_key = get_torbox_key()
    trakt_id = get_trakt_id()
    
    if not torbox_key:
        logger.error("TORBOX_API_KEY not found in .env file")
        sys.exit(1)
    if not trakt_id:
        logger.error("TRAKT_CLIENT_ID not found in .env file")
        sys.exit(1)
    
    # Run sync
    telegram = None
    sync_start_time = time.time()
    try:
        config = get_config()
        if not config:
            logger.error("Could not load config from database")
            sys.exit(1)
        
        # Initialize Telegram notifier if configured
        telegram_settings = config.get("telegram", {})
        telegram = get_telegram_notifier(telegram_settings=telegram_settings)
        if telegram.is_configured():
            logger.info("Telegram notifications enabled")
        
        torbox = TorboxClient(torbox_key)
        trakt = TraktClient(trakt_id, get_trakt_access_token())
        engine = SyncEngine(torbox, trakt, config, telegram)
        engine.sync()
        
        # Send sync summary notification
        if telegram and telegram.is_configured():
            try:
                duration = time.time() - sync_start_time
                run_stats = engine.get_sync_stats()
                telegram.notify_summary(
                    added=run_stats.get("added", 0),
                    upgraded=run_stats.get("upgraded", 0),
                    skipped=run_stats.get("skipped", 0),
                    failed=run_stats.get("failed", 0),
                    duration_seconds=duration,
                    movies=run_stats.get("movies", 0),
                    shows=run_stats.get("shows", 0)
                )
            except (httpx.RequestError, OSError) as e:
                logger.debug("Telegram summary traceback:", exc_info=True)
                logger.debug("Failed to send Telegram summary: %s", e)
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        # Top-level catch-all - log full traceback for fatal errors
        logger.debug("Fatal error traceback:", exc_info=True)
        logger.exception("Fatal error during sync: %s", e)
        sys.exit(1)
    finally:
        # Clean up Telegram notifier
        if telegram:
            telegram.close()


if __name__ == "__main__":
    main()
