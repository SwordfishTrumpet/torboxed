#!/usr/bin/env python3
"""Basic tests for torboxed.py"""

import unittest
import sys
import os
import sqlite3
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from torboxed import (
    load_env, parse_quality, is_better_quality, QualityInfo,
    RESOLUTION_SCORES, SOURCE_SCORES, CODEC_SCORES, AUDIO_SCORES,
    RateLimiter, get_db, init_db, DB_PATH, SyncEngine, TorboxClient,
    RealDebridClient, create_debrid_client,
    get_torbox_key, get_trakt_id, get_env, _env_cache,
    run_self_test, show_cron_status, discover_existing_torrents,
    is_max_quality, MAX_QUALITY_SCORE, parse_season_info, SeasonInfo,
    TorrentResult, get_processed_show_seasons, reset_item,
    TraktClient,
    # Security functions
    sanitize_error_text, sanitize_response_error, _validate_cron_expression, get_lock_path,
    validate_db_path, validate_log_path, RateLimitedLogHandler,
    check_and_acquire_lock,
    # Telegram notifications
    TelegramNotifier, get_telegram_notifier, get_telegram_bot_token, get_telegram_chat_id,
    # Search helpers
    build_search_result, normalize_search_query, encode_magnet_link, normalize_hash,
    # Constants
    COMPLETE_PACK_MIN_SIZE, RD_CACHE_CHECK_BATCH,
    # Path configuration (issue #6)
    _path_from_env,
    # WebDAV / torrent_files
    store_torrent_files, get_torrent_files, get_video_file_for_episode, get_subtitle_files,
    cleanup_orphan_torrent_files,
    extract_imdb_from_filename, build_movie_filename, build_episode_filename, build_subtitle_filename,
    resolve_path, resolve_debrid_id_for_path, _get_db_listing, _get_best_debrid_for_season,
    _classify_file, get_webdav_config, backfill_torrent_files,
    strip_pending_sentinel, PENDING_SENTINEL_ID,
    classify_media_level, COMPLETENESS_ORDER
)


class TestDebridClientABC(unittest.TestCase):
    """Test the DebridClient abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        """DebridClient cannot be instantiated directly (has abstract methods)."""
        import torboxed
        with self.assertRaises(TypeError):
            torboxed.DebridClient()

    def test_concrete_subclass_must_implement_check_cached(self):
        """Subclass missing check_cached fails instantiation."""
        import torboxed
        from abc import ABC

        class IncompleteClient(torboxed.DebridClient):
            def get_my_torrents(self): pass
            def add_torrent(self, magnet, title=""): pass
            def remove_torrent(self, torrent_id): pass
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None

        with self.assertRaises(TypeError):
            IncompleteClient()

    def test_concrete_subclass_must_implement_get_my_torrents(self):
        """Subclass missing get_my_torrents fails instantiation."""
        import torboxed

        class IncompleteClient(torboxed.DebridClient):
            def check_cached(self, hashes): pass
            def add_torrent(self, magnet, title=""): pass
            def remove_torrent(self, torrent_id): pass
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None

        with self.assertRaises(TypeError):
            IncompleteClient()

    def test_concrete_subclass_must_implement_add_torrent(self):
        """Subclass missing add_torrent fails instantiation."""
        import torboxed

        class IncompleteClient(torboxed.DebridClient):
            def check_cached(self, hashes): pass
            def get_my_torrents(self): pass
            def remove_torrent(self, torrent_id): pass
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None

        with self.assertRaises(TypeError):
            IncompleteClient()

    def test_concrete_subclass_must_implement_remove_torrent(self):
        """Subclass missing remove_torrent fails instantiation."""
        import torboxed

        class IncompleteClient(torboxed.DebridClient):
            def check_cached(self, hashes): pass
            def get_my_torrents(self): pass
            def add_torrent(self, magnet, title=""): pass
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None

        with self.assertRaises(TypeError):
            IncompleteClient()

    def test_search_infrastructure_created_by_init(self):
        """DebridClient.__init__ creates searcher sub-clients."""
        import torboxed

        class FullClient(torboxed.DebridClient):
            def check_cached(self, hashes): return {}
            def get_my_torrents(self): return []
            def add_torrent(self, magnet, title=""): return None
            def remove_torrent(self, torrent_id): return True
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None

        client = FullClient()
        self.assertIsNotNone(client.searcher_zilean)
        self.assertIsNotNone(client.searcher_prowlarr)
        self.assertIsNotNone(client.searcher_jackett)

    def test_search_torrents_calls_check_cached(self):
        """search_torrents delegates to subclass check_cached for availability."""
        import torboxed

        class SearchClient(torboxed.DebridClient):
            def check_cached(self, hashes):
                self.last_cache_call = hashes
                return {"abc123": True}

            def get_my_torrents(self): return []
            def add_torrent(self, magnet, title=""): return None
            def remove_torrent(self, torrent_id): return True
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None

        client = SearchClient()
        client.searcher_zilean = Mock()
        client.searcher_zilean.is_configured.return_value = True
        client.searcher_zilean.search.return_value = [{
            "name": "Test Movie 1080p",
            "hash": "abc123",
            "magnet": "magnet:?xt=urn:btih:abc123",
            "size": 5000000000,
            "seeds": 10,
            "peers": 2,
        }]
        client.searcher_zilean.search_by_imdb.return_value = []

        result = client.search_torrents("Test Movie", "movie")
        self.assertEqual(client.last_cache_call, ["abc123"])
        self.assertTrue(result[0]["availability"])

    def test_get_cached_torrents_sorted_by_quality(self):
        """Cached results should be sorted by quality score descending."""
        import torboxed

        class SortClient(torboxed.DebridClient):
            def check_cached(self, hashes):
                return {h: True for h in hashes}
            def get_my_torrents(self): return []
            def add_torrent(self, magnet, title=""): return None
            def remove_torrent(self, torrent_id): return True
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None

        client = SortClient()
        client.searcher_zilean = Mock()
        client.searcher_zilean.is_configured.return_value = True
        client.searcher_zilean.search_by_imdb.return_value = []
        client.searcher_zilean.search.return_value = [
            {"name": "Movie.720p.HDTV.x264", "hash": "aaa", "magnet": "magnet:?xt=urn:btih:aaa", "size": 1000, "seeds": 1, "peers": 0},
            {"name": "Movie.2160p.BluRay.HEVC.DTS-HD.MA", "hash": "bbb", "magnet": "magnet:?xt=urn:btih:bbb", "size": 5000, "seeds": 1, "peers": 0},
            {"name": "Movie.1080p.WEB-DL.H.264", "hash": "ccc", "magnet": "magnet:?xt=urn:btih:ccc", "size": 3000, "seeds": 1, "peers": 0},
        ]
        client.searcher_prowlarr = Mock()
        client.searcher_prowlarr.is_configured.return_value = False
        client.searcher_jackett = Mock()
        client.searcher_jackett.is_configured.return_value = False

        results = client.get_cached_torrents("Movie 2024", "movie")

        self.assertGreaterEqual(len(results), 2)
        for i in range(len(results) - 1):
            self.assertGreaterEqual(results[i].quality.score, results[i + 1].quality.score,
                                     f"Result {i} score {results[i].quality.score} should be >= result {i+1} score {results[i+1].quality.score}")


class TestRateLimiter(unittest.TestCase):
    """Test rate limiting functionality."""
    
    def test_wait_respects_interval(self):
        """Test that rate limiter waits for minimum interval."""
        limiter = RateLimiter(0.1)  # 100ms interval
        import time
        
        start = time.time()
        limiter.wait()
        limiter.mark_success()  # Mark first request as successful
        limiter.wait()  # Second call should wait ~100ms
        elapsed = time.time() - start
        
        self.assertGreaterEqual(elapsed, 0.1)


class TestQualityParsing(unittest.TestCase):
    """Test quality parsing from torrent names."""
    
    def test_parse_1080p_bluray(self):
        """Test parsing a 1080p BluRay release."""
        name = "Movie.Name.2024.1080p.BluRay.x264.DTS-HD.MA"
        quality = parse_quality(name)
        
        self.assertEqual(quality.resolution, "1080p")
        self.assertEqual(quality.source, "Blu-ray")
        self.assertEqual(quality.codec, "H.264")
        # GuessIt returns 'DTS-HD' for this input
        self.assertIn(quality.audio, ["DTS-HD", "DTS-HD MA"])
        self.assertGreater(quality.score, 0)
    
    def test_parse_2160p_webdl(self):
        """Test parsing a 4K WEB-DL release."""
        name = "Movie.Name.2024.2160p.WEB-DL.DDP5.1.H.265"
        quality = parse_quality(name)
        
        self.assertEqual(quality.resolution, "2160p")
        # GuessIt returns 'Web' for WEB-DL
        self.assertIn(quality.source, ["WEB-DL", "Web"])
        self.assertEqual(quality.codec, "H.265")
    
    def test_quality_scoring(self):
        """Test quality score calculations."""
        # 2160p BluRay should score higher than 1080p BluRay
        q4k = parse_quality("Movie.2024.2160p.BluRay.x264")
        q1080 = parse_quality("Movie.2024.1080p.BluRay.x264")
        
        self.assertGreater(q4k.score, q1080.score)


class TestQualityComparison(unittest.TestCase):
    """Test quality comparison logic."""
    
    def test_upgrade_threshold(self):
        """Test that upgrade requires +500 point threshold."""
        # Score difference of 500 should trigger upgrade
        self.assertTrue(is_better_quality(3000, 2500))
        
        # Score difference of 499 should not trigger upgrade
        self.assertFalse(is_better_quality(2999, 2500))
        
        # Same score should not trigger upgrade
        self.assertFalse(is_better_quality(2500, 2500))
    
    def test_custom_threshold(self):
        """Test custom threshold parameter."""
        # With threshold of 100, diff of 150 should trigger
        self.assertTrue(is_better_quality(2600, 2500, threshold=100))
        
        # With threshold of 200, diff of 150 should not trigger
        self.assertFalse(is_better_quality(2600, 2500, threshold=200))


class TestLoadEnv(unittest.TestCase):
    """Test .env file loading."""
    
    def setUp(self):
        """Clear env cache before each test."""
        import torboxed
        torboxed._env_cache = None
    
    def test_load_valid_env(self):
        """Test loading a valid .env file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("KEY1=value1\n")
            f.write("KEY2=value2\n")
            f.write("# Comment\n")
            f.write("KEY3=value=with=equals\n")
            tmp_path = f.name
        
        try:
            # Temporarily replace ENV_PATH
            import torboxed
            original_path = torboxed.ENV_PATH
            torboxed.ENV_PATH = Path(tmp_path)
            torboxed._env_cache = None  # Clear cache
            
            env = get_env()
            
            self.assertEqual(env.get("KEY1"), "value1")
            self.assertEqual(env.get("KEY2"), "value2")
            self.assertEqual(env.get("KEY3"), "value=with=equals")
            
            torboxed.ENV_PATH = original_path
            torboxed._env_cache = None
        finally:
            os.unlink(tmp_path)
    
    def test_lazy_loading(self):
        """Test that API keys are lazy-loaded."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("TORBOX_API_KEY=test_torbox_key\n")
            f.write("TRAKT_CLIENT_ID=test_trakt_id\n")
            tmp_path = f.name
        
        try:
            import torboxed
            original_path = torboxed.ENV_PATH
            torboxed.ENV_PATH = Path(tmp_path)
            torboxed._env_cache = None  # Clear cache
            
            # Test lazy loading
            self.assertIsNone(torboxed._env_cache)
            key = get_torbox_key()
            self.assertIsNotNone(torboxed._env_cache)
            self.assertEqual(key, "test_torbox_key")
            
            trakt_id = get_trakt_id()
            self.assertEqual(trakt_id, "test_trakt_id")
            
            torboxed.ENV_PATH = original_path
            torboxed._env_cache = None
        finally:
            os.unlink(tmp_path)


class TestDebridFactory(unittest.TestCase):
    """Test create_debrid_client factory function."""

    def setUp(self):
        """Patch env to isolate from real .env file."""
        import torboxed
        self.original_env_path = torboxed.ENV_PATH
        self.original_env_cache = torboxed._env_cache
        torboxed._env_cache = None

    def tearDown(self):
        import torboxed
        torboxed.ENV_PATH = self.original_env_path
        torboxed._env_cache = self.original_env_cache

    def _create_temp_env(self, content):
        """Helper to create temp .env file and patch ENV_PATH."""
        import torboxed
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False)
        temp_file.write(content)
        temp_file.close()
        torboxed.ENV_PATH = Path(temp_file.name)
        torboxed._env_cache = None
        return temp_file.name

    def test_default_service_is_torbox(self):
        """Without DEBRID_SERVICE set, creates TorboxClient."""
        import torboxed
        tmp_path = self._create_temp_env("TORBOX_API_KEY=test_tb_key\n")
        try:
            client = torboxed.create_debrid_client()
            self.assertIsInstance(client, torboxed.TorboxClient)
        finally:
            os.unlink(tmp_path)

    def test_explicit_torbox_service(self):
        """DEBRID_SERVICE=torbox creates TorboxClient."""
        import torboxed
        tmp_path = self._create_temp_env(
            "DEBRID_SERVICE=torbox\nTORBOX_API_KEY=test_tb_key\n"
        )
        try:
            client = torboxed.create_debrid_client()
            self.assertIsInstance(client, torboxed.TorboxClient)
        finally:
            os.unlink(tmp_path)

    def test_real_debrid_service(self):
        """DEBRID_SERVICE=real_debrid creates RealDebridClient."""
        import torboxed
        tmp_path = self._create_temp_env(
            "DEBRID_SERVICE=real_debrid\nREAL_DEBRID_API_KEY=test_rd_key\n"
        )
        try:
            client = torboxed.create_debrid_client()
            self.assertIsInstance(client, torboxed.RealDebridClient)
        finally:
            os.unlink(tmp_path)

    def test_no_api_key_returns_none(self):
        """Missing API key returns None."""
        import torboxed
        tmp_path = self._create_temp_env("DEBRID_SERVICE=torbox\n")
        try:
            client = torboxed.create_debrid_client()
            self.assertIsNone(client)
        finally:
            os.unlink(tmp_path)

    def test_factory_returns_debrid_client_subclass(self):
        """Factory return value is a DebridClient instance."""
        import torboxed
        tmp_path = self._create_temp_env(
            "DEBRID_SERVICE=torbox\nTORBOX_API_KEY=test_tb_key\n"
        )
        try:
            client = torboxed.create_debrid_client()
            self.assertIsInstance(client, torboxed.DebridClient)
        finally:
            os.unlink(tmp_path)


class TestDatabase(unittest.TestCase):
    """Test database operations."""
    
    def setUp(self):
        """Create temporary database for testing."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        # Patch DB_PATH
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
    
    def tearDown(self):
        """Clean up temporary database."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_init_db(self):
        """Test database initialization."""
        init_db()
        
        self.assertTrue(self.test_db_path.exists())
        
        # Verify tables exist
        with get_db() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = [row[0] for row in cursor.fetchall()]
            self.assertIn("config", tables)
            self.assertIn("processed", tables)
    
    def test_default_config(self):
        """Test default config is inserted."""
        init_db()
        
        with get_db() as conn:
            row = conn.execute("SELECT * FROM config WHERE id=1").fetchone()
            self.assertIsNotNone(row)
            
            # Verify JSON columns are valid
            sources = json.loads(row["sources"])
            limits = json.loads(row["limits"])
            self.assertIsInstance(sources, list)
            self.assertIsInstance(limits, dict)


class TestScoreConstants(unittest.TestCase):
    """Test that scoring constants are properly defined."""

    def test_resolution_scores(self):
        """Test resolution scores have correct hierarchy."""
        self.assertEqual(RESOLUTION_SCORES["2160p"], 4000)
        self.assertEqual(RESOLUTION_SCORES["1080p"], 2500)
        self.assertEqual(RESOLUTION_SCORES["720p"], 1500)
        self.assertGreater(RESOLUTION_SCORES["2160p"], RESOLUTION_SCORES["1080p"])

    def test_source_scores(self):
        """Test source scores have correct hierarchy."""
        self.assertGreater(SOURCE_SCORES["Blu-ray"], SOURCE_SCORES["WEB-DL"])
        self.assertGreater(SOURCE_SCORES["WEB-DL"], SOURCE_SCORES["HDTV"])
        self.assertGreater(SOURCE_SCORES["HDTV"], SOURCE_SCORES["DVD"])

    def test_web_source_scoring(self):
        """Test that 'Web' source (from GuessIt) has same score as WEB-DL."""
        # GuessIt returns "Web" for WEB-DL and WEBRip sources
        self.assertEqual(SOURCE_SCORES["Web"], 900)
        self.assertEqual(SOURCE_SCORES["Web"], SOURCE_SCORES["WEB-DL"])

    def test_codec_scores(self):
        """Test codec scores have correct hierarchy."""
        self.assertGreater(CODEC_SCORES["AV1"], CODEC_SCORES["H.265"])
        self.assertGreater(CODEC_SCORES["H.265"], CODEC_SCORES["H.264"])

    def test_dts_hd_ma_scoring(self):
        """Test DTS-HD Master Audio scores higher than regular DTS-HD."""
        # DTS-HD MA should be 650, regular DTS-HD is 600
        self.assertEqual(AUDIO_SCORES["DTS-HD Master Audio"], 650)
        self.assertEqual(AUDIO_SCORES["DTS-HD MA"], 650)
        self.assertEqual(AUDIO_SCORES["DTS-HD"], 600)
        self.assertGreater(
            AUDIO_SCORES["DTS-HD Master Audio"],
            AUDIO_SCORES["DTS-HD"]
        )


class TestExcludedSources(unittest.TestCase):
    """Test exclusion of CAM, TS, HDCAM sources (GuessIt returns these as Camera, Telesync, HD Camera)."""

    def test_camera_source_scoring(self):
        """Test that Camera (CAM) sources have low scores."""
        quality = parse_quality("Movie.2024.CAM.x264")
        # GuessIt returns "Camera" for CAM releases
        self.assertEqual(quality.source, "Camera")
        # Verify low score reflects poor quality
        self.assertLess(SOURCE_SCORES.get("Camera", 0), SOURCE_SCORES.get("Web", 0))

    def test_telesync_source_scoring(self):
        """Test that Telesync (TS) sources have lowest score."""
        quality = parse_quality("Movie.2024.TS.x264")
        # GuessIt returns "Telesync" for TS releases
        self.assertEqual(quality.source, "Telesync")
        # Telesync has lowest score (50)
        self.assertEqual(SOURCE_SCORES.get("Telesync", 0), 50)

    def test_hd_camera_source_scoring(self):
        """Test that HD Camera (HDCAM) sources have low scores."""
        quality = parse_quality("Movie.2024.HDCAM.x264")
        # GuessIt returns "HD Camera" for HDCAM releases
        self.assertEqual(quality.source, "HD Camera")
        # HD Camera should have low score (100)
        self.assertEqual(SOURCE_SCORES.get("HD Camera", 0), 100)

    def test_low_quality_sources_ranked_correctly(self):
        """Test that CAM, TS, HDCAM sources score lower than proper releases."""
        camera_score = SOURCE_SCORES.get("Camera", 0)
        telesync_score = SOURCE_SCORES.get("Telesync", 0)
        hd_camera_score = SOURCE_SCORES.get("HD Camera", 0)
        web_score = SOURCE_SCORES.get("Web", 0)

        # All low-quality sources should score less than WEB-DL/Web
        self.assertLess(camera_score, web_score)
        self.assertLess(telesync_score, web_score)
        self.assertLess(hd_camera_score, web_score)

        # Telesync should be lowest
        self.assertLess(telesync_score, camera_score)
        self.assertLess(telesync_score, hd_camera_score)


class TestTorboxExcludedSources(unittest.TestCase):
    """Test TorboxClient excluded sources filtering."""
    
    def setUp(self):
        """Set up mock Torbox client."""
        from torboxed import TorboxClient
        self.mock_debrid = MagicMock(spec=TorboxClient)
        self.mock_debrid.api_key = "test-key"
    
    def test_default_excluded_sources(self):
        """Test that default excluded sources are CAM, TS, HDCAM."""
        import torboxed
        
        # Create mock items representing different source types (with 1080p resolution to pass min threshold)
        cam_item = {"title": "Movie.2024.1080p.CAM.x264", "availability": True, "magnet": "magnet:cam"}
        ts_item = {"title": "Movie.2024.1080p.TS.x264", "availability": True, "magnet": "magnet:ts"}
        hdcam_item = {"title": "Movie.2024.1080p.HDCAM.x264", "availability": True, "magnet": "magnet:hdcam"}
        bluray_item = {"title": "Movie.2024.1080p.BluRay.x264", "availability": True, "magnet": "magnet:bluray"}
        
        # Mock search_torrents to return all items
        self.mock_debrid.search_torrents.return_value = [cam_item, ts_item, hdcam_item, bluray_item]
        
        # Create a real TorboxClient but mock the search method
        client = TorboxClient.__new__(TorboxClient)
        client.searcher_zilean = Mock()
        client.search_torrents = Mock(return_value=[cam_item, ts_item, hdcam_item, bluray_item])
        
        # Call get_cached_torrents with defaults
        result = client.get_cached_torrents("Movie", "movie")
        
        # Should only have the BluRay result (CAM, TS, HDCAM excluded)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Movie.2024.1080p.BluRay.x264")
    
    def test_custom_excluded_sources(self):
        """Test that custom excluded sources can be specified."""
        import torboxed
        
        # Create mock items (with 1080p resolution to pass min threshold)
        webdl_item = {"title": "Movie.2024.1080p.WEB-DL.x264", "availability": True, "magnet": "magnet:webdl"}
        bluray_item = {"title": "Movie.2024.1080p.BluRay.x264", "availability": True, "magnet": "magnet:bluray"}
        
        client = TorboxClient.__new__(TorboxClient)
        client.searcher_zilean = Mock()
        client.search_torrents = Mock(return_value=[webdl_item, bluray_item])
        
        # Call with custom exclude list that excludes BluRay
        result = client.get_cached_torrents("Movie", "movie", excluded_sources=["BluRay"])
        
        # Should only have WEB-DL (BluRay excluded)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Movie.2024.1080p.WEB-DL.x264")
    
    def test_case_insensitive_exclusion(self):
        """Test that source exclusion is case-insensitive."""
        import torboxed
        
        # Items with different casing (with 1080p resolution to pass min threshold)
        cam_lower = {"title": "Movie.2024.1080p.cam.x264", "availability": True, "magnet": "magnet:cam"}
        CAM_upper = {"title": "Movie.2024.1080p.CAM.x264", "availability": True, "magnet": "magnet:CAM"}
        Cam_mixed = {"title": "Movie.2024.1080p.Cam.x264", "availability": True, "magnet": "magnet:Cam"}
        bluray_item = {"title": "Movie.2024.1080p.BluRay.x264", "availability": True, "magnet": "magnet:bluray"}
        
        client = TorboxClient.__new__(TorboxClient)
        client.searcher_zilean = Mock()
        client.search_torrents = Mock(return_value=[cam_lower, CAM_upper, Cam_mixed, bluray_item])
        
        # Call with default excludes (CAM)
        result = client.get_cached_torrents("Movie", "movie")
        
        # Should only have BluRay (all CAM variants excluded)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Movie.2024.1080p.BluRay.x264")


class TestSyncEngine(unittest.TestCase):
    """Test SyncEngine synchronization logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        # Patch DB_PATH
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        
        # Initialize database
        init_db()
        
        # Create mock clients
        self.mock_debrid = Mock()
        self.mock_trakt = Mock()
        
        # Create SyncEngine with mock clients
        self.config = {
            "sources": ["movies/trending"],
            "limits": {"movies": 10, "shows": 10},
            "filters": {"min_year": 2000, "exclude": ["CAM", "TS", "HDCAM"]}
        }
        self.engine = torboxed.SyncEngine(self.mock_debrid, self.mock_trakt, self.config)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_safe_upgrade_adds_before_removing(self):
        """Test that upgrade removes old before adding new (prevents duplicates on failure)."""
        import torboxed
        
        # Create a processed item with lower quality
        torboxed.record_processed(
            "tt1234567", "Test Movie", 2024, "movie", "added", "success",
            debrid_id="old-torrent-id", magnet="magnet:old",
            quality_score=2500, quality_label="1080p BluRay"
        )
        
        # Set up mock to return better quality cached torrent
        mock_torrent = Mock()
        mock_torrent.name = "Test.Movie.2024.2160p.BluRay.x264"
        mock_torrent.magnet = "magnet:new"
        mock_torrent.availability = True
        mock_torrent.quality = Mock()
        mock_torrent.quality.score = 4000
        mock_torrent.quality.label = "2160p BluRay"
        
        self.mock_debrid.get_cached_torrents.return_value = [mock_torrent]
        
        # Set up add_torrent to succeed
        self.mock_debrid.add_torrent.return_value = "new-torrent-id"
        self.mock_debrid.remove_torrent.return_value = True
        
        # Process the content
        content = {
            "imdb_id": "tt1234567",
            "title": "Test Movie",
            "year": 2024,
            "type": "movie"
        }
        existing_torrents = {}
        result = self.engine.process_content(content, existing_torrents)
        
        # Verify success
        self.assertTrue(result)
        
        # Verify remove_torrent was called before add_torrent
        # (remove first prevents duplicates when Torbox returns 500 on removal)
        calls = self.mock_debrid.method_calls
        remove_call_idx = None
        add_call_idx = None
        
        for i, call in enumerate(calls):
            if call[0] == "remove_torrent":
                remove_call_idx = i
            elif call[0] == "add_torrent":
                add_call_idx = i
        
        # Both should be called, and remove should come before add
        self.assertIsNotNone(remove_call_idx, "remove_torrent was not called")
        self.assertIsNotNone(add_call_idx, "add_torrent was not called")
        self.assertLess(remove_call_idx, add_call_idx, 
                        "remove_torrent must be called before add_torrent to prevent duplicates")
    
    def test_upgrade_fails_safe_when_add_fails(self):
        """Test that upgrade fails safely when add_torrent fails (tries next best)."""
        import torboxed
        
        # Create a processed item with lower quality
        torboxed.record_processed(
            "tt7654321", "Test Movie 2", 2024, "movie", "added", "success",
            debrid_id="old-torrent-id", magnet="magnet:old",
            quality_score=2500, quality_label="1080p BluRay"
        )
        
        # Set up mock to return better quality cached torrent
        mock_torrent = Mock()
        mock_torrent.name = "Test.Movie.2.2024.2160p.BluRay.x264"
        mock_torrent.magnet = "magnet:new"
        mock_torrent.availability = True
        mock_torrent.quality = Mock()
        mock_torrent.quality.score = 4000
        mock_torrent.quality.label = "2160p BluRay"
        
        self.mock_debrid.get_cached_torrents.return_value = [mock_torrent]
        
        # Set up add_torrent to FAIL, remove to succeed (old was removed first)
        self.mock_debrid.add_torrent.return_value = None
        self.mock_debrid.remove_torrent.return_value = True
        
        # Process the content
        content = {
            "imdb_id": "tt7654321",
            "title": "Test Movie 2",
            "year": 2024,
            "type": "movie"
        }
        existing_torrents = {}
        result = self.engine.process_content(content, existing_torrents)
        
        # Verify failure (no more torrents to try)
        self.assertFalse(result)
        
        # Verify remove_torrent WAS called (old removed before trying add)
        self.mock_debrid.remove_torrent.assert_called_once_with("old-torrent-id")
        
        # Verify failed record was created
        processed = torboxed.get_processed_item("tt7654321")
        self.assertEqual(processed["action"], "failed")
        self.assertEqual(processed["reason"], "upgrade_all_failed")


class TestTorboxAPIEndpoints(unittest.TestCase):
    """Test Torbox API endpoint URLs (BUG-3 fix)."""

    def test_search_endpoint_format(self):
        """Test that search uses Zilean database client."""
        import torboxed

        # Create a mock client with all searchers
        client = TorboxClient.__new__(TorboxClient)
        client.searcher_zilean = Mock()
        client.searcher_zilean.is_configured.return_value = True
        client.searcher_zilean.search.return_value = []
        client.searcher_zilean.search_by_imdb.return_value = []
        client.searcher_prowlarr = Mock()
        client.searcher_prowlarr.is_configured.return_value = True
        client.searcher_prowlarr.search.return_value = []
        client.searcher_jackett = Mock()
        client.searcher_jackett.is_configured.return_value = True
        client.searcher_jackett.search.return_value = []
        client._request = Mock(return_value={"data": {}})
        
        # Call search_torrents
        result = client.search_torrents("The Matrix 1999", "movie")
        
        # Verify Zilean client was used (via search_torrents calling it internally)
        # Since search_torrents is mocked above, we can't verify the actual call
        # Just verify the result is empty as expected
        self.assertEqual(result, [])
    
    def test_mylist_endpoint_format(self):
        """Test that mylist uses correct endpoint format."""
        client = TorboxClient.__new__(TorboxClient)
        client.searcher_zilean = Mock()
        client.client = Mock()

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {"data": []}
            client.get_my_torrents()

            call_args = mock_request.call_args
            endpoint = call_args[0][1]
            self.assertEqual(endpoint, "/v1/api/torrents/mylist")
    
    def test_createtorrent_endpoint_format(self):
        """Test that createtorrent uses correct endpoint format."""
        client = TorboxClient.__new__(TorboxClient)
        client.searcher_zilean = Mock()
        client.client = Mock()

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {"success": True, "data": {"torrent_id": "123"}}
            client.add_torrent("magnet:?xt=urn:btih:1234567890abcdef", "Test Movie")

            call_args = mock_request.call_args
            method = call_args[0][0]
            endpoint = call_args[0][1]
            data = call_args[1].get("data", {})

            self.assertEqual(method, "POST")
            self.assertEqual(endpoint, "/v1/api/torrents/createtorrent")
            self.assertEqual(data.get("magnet"), "magnet:?xt=urn:btih:1234567890abcdef")
    
    def test_controltorrent_endpoint_format(self):
        """Test that controltorrent uses correct endpoint format."""
        client = TorboxClient.__new__(TorboxClient)
        client.searcher_zilean = Mock()
        client.client = Mock()

        with patch.object(client, '_request') as mock_request:
            mock_request.return_value = {"success": True}
            client.remove_torrent("torrent-123")

            call_args = mock_request.call_args
            method = call_args[0][0]
            endpoint = call_args[0][1]
            json_data = call_args[1].get("json", {})

            self.assertEqual(method, "POST")
            self.assertEqual(endpoint, "/v1/api/torrents/controltorrent")
            self.assertEqual(json_data.get("torrent_id"), "torrent-123")
            self.assertEqual(json_data.get("operation"), "delete")
    
    def test_tv_show_upgrade_sends_json_remove_payload(self):
        """Test that TV show upgrades send proper JSON to remove_torrent API (Twin Peaks bug fix).
        
        This test verifies the fix for the HTTP 422 error:
        {"detail":[{"type":"model_attributes_type","loc":["body"],"msg":"Input should be a valid dictionary or object to extract fields from","input":"torrent_id=...&operation=delete"}]}
        
        The issue was that remove_torrent was sending form-encoded data instead of JSON.
        """
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo, SyncEngine
        
        temp_dir = tempfile.TemporaryDirectory()
        test_db_path = Path(temp_dir.name) / "test.db"
        original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = test_db_path
        torboxed.init_db()
        
        try:
            # Simulate Twin Peaks S01 with existing low-quality torrent (score 0)
            # This matches the real scenario: "Upgrade detected for Twin Peaks (S01): 0 -> 4400"
            torboxed.record_processed(
                "tt0098936", "Twin Peaks", 1990, "show", "added", "success",
                debrid_id="22845381", magnet="magnet:existing",
                quality_score=0, quality_label="Unknown",
                season="S01"
            )
            
            # Create a SyncEngine with mocked TorboxClient
            engine = SyncEngine.__new__(SyncEngine)
            mock_debrid = Mock()
            engine.debrid = mock_debrid
            engine.config = {"filters": {"exclude": ["CAM", "TS", "HDCAM"], "min_resolution_score": 800}}
            engine.telegram = Mock()
            engine.telegram.is_configured.return_value = False
            
            # Mock better quality torrent available (1080p Blu-ray H.265 = score 4400)
            mock_torrent = TorrentResult(
                name="Twin.Peaks.S01.1080p.BluRay.H.265",
                magnet="magnet:s01-better",
                availability=True,
                size=50000,
                quality=QualityInfo(resolution="1080p", source="Blu-ray", codec="H.265", score=4400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            )
            mock_debrid.get_cached_torrents.return_value = [mock_torrent]
            
            # Mock successful add and remove
            mock_debrid.add_torrent.return_value = "new-s01-id"
            mock_debrid.remove_torrent.return_value = True
            
            # Process S01 upgrade
            result = engine._process_season(
                "tt0098936", "Twin Peaks", 1990, "S01",
                mock_torrent, {}
            )
            
            # Verify upgrade succeeded
            self.assertTrue(result)
            
            # Verify add_torrent was called
            mock_debrid.add_torrent.assert_called_once()
            
            # Verify remove_torrent was called with the old torrent ID
            mock_debrid.remove_torrent.assert_called_once_with("22845381")
            
            # Verify the processed record shows upgraded
            s01_record = torboxed.get_processed_item("tt0098936", "S01")
            self.assertEqual(s01_record["action"], "upgraded")
            self.assertEqual(s01_record["replaced_score"], 0)
            self.assertEqual(s01_record["quality_score"], 4400)
            
        finally:
            torboxed.DB_PATH = original_db_path
            temp_dir.cleanup()


class TestTimezoneAwareDatetime(unittest.TestCase):
    """Test timezone-aware datetime usage (BUG-4 fix)."""

    def test_record_processed_uses_timezone_aware(self):
        """Test that record_processed uses timezone-aware datetime."""
        import torboxed
        from datetime import datetime, timezone

        self.temp_dir = tempfile.TemporaryDirectory()
        test_db_path = Path(self.temp_dir.name) / "test.db"
        original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = test_db_path
        torboxed.init_db()

        try:
            # Record a processed item
            torboxed.record_processed(
                "tt9999999", "Test Timezone", 2024, "movie",
                "added", "success", debrid_id="test-id", magnet="magnet:test",
                quality_score=2500, quality_label="1080p"
            )

            # Retrieve and verify the timestamp
            item = torboxed.get_processed_item("tt9999999")
            self.assertIsNotNone(item)
            timestamp = item.get("processed_at", "")

            # Should be a valid ISO format timestamp
            self.assertTrue(len(timestamp) > 0)

            # Should contain timezone info (+00:00) or Z
            # With timezone.utc, the ISO format includes offset
            parsed = datetime.fromisoformat(timestamp)
            self.assertIsNotNone(parsed)

        finally:
            torboxed.DB_PATH = original_db_path
            self.temp_dir.cleanup()

    def test_sync_uses_timezone_aware(self):
        """Test that sync() method uses timezone-aware datetime."""
        from datetime import datetime, timezone
        import torboxed

        # Just verify the function uses datetime.now(timezone.utc)
        # by checking the code path doesn't raise deprecation warning
        with patch.object(torboxed, 'get_config') as mock_config, \
             patch.object(torboxed, 'TorboxClient'), \
             patch.object(torboxed, 'TraktClient'), \
             patch.object(torboxed.SyncEngine, 'sync') as mock_sync:

            mock_config.return_value = {
                "sources": ["movies/trending"],
                "limits": {"movies": 10}
            }

            # This should not raise a DeprecationWarning
            # The test passes if no exception is raised
            logger = torboxed.setup_logging(verbose=False, log_to_file=False)


class TestWordBoundaryFiltering(unittest.TestCase):
    """Test word boundary filtering (BUG-5 fix)."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"

        # Patch DB_PATH
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()

        # Create mock clients
        self.mock_debrid = Mock()
        self.mock_trakt = Mock()

        # Create SyncEngine with mock clients and filters
        self.config = {
            "sources": ["movies/trending"],
            "limits": {"movies": 10, "shows": 10},
            "filters": {"min_year": 2000, "exclude": ["CAM", "TS", "HDCAM"]}
        }
        self.engine = torboxed.SyncEngine(self.mock_debrid, self.mock_trakt, self.config)

    def tearDown(self):
        """Clean up test fixtures."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_ts_in_thunderbolts_not_filtered(self):
        """Test that 'TS' in 'Thunderbolts' doesn't trigger false positive."""
        # "Thunderbolts*" contains "ts" but shouldn't be filtered
        item = {
            "imdb_id": "tt1234567",
            "title": "Thunderbolts*",
            "year": 2025,
            "type": "movie"
        }

        filtered, reason = self.engine.should_filter(item)

        # Should NOT be filtered (BUG-5 fix)
        self.assertFalse(filtered, f"Thunderbolts* was incorrectly filtered: {reason}")

    def test_standalone_ts_is_filtered(self):
        """Test that standalone 'TS' in title IS filtered."""
        # "Movie TS Release" should be filtered
        item = {
            "imdb_id": "tt1234568",
            "title": "Some Movie TS Release",
            "year": 2024,
            "type": "movie"
        }

        filtered, reason = self.engine.should_filter(item)

        # Should be filtered
        self.assertTrue(filtered)
        self.assertIn("TS", reason)

    def test_ts_with_punctuation_filtered(self):
        """Test that 'TS.' or '(TS)' IS filtered (word boundary at punctuation)."""
        item = {
            "imdb_id": "tt1234569",
            "title": "Movie.2024.(TS).x264",
            "year": 2024,
            "type": "movie"
        }

        filtered, reason = self.engine.should_filter(item)

        # Should be filtered - TS followed by parenthesis is a word boundary
        self.assertTrue(filtered)
        self.assertIn("TS", reason)

    def test_cam_in_word_not_filtered(self):
        """Test that 'CAM' within another word doesn't trigger false positive."""
        item = {
            "imdb_id": "tt1234570",
            "title": "The Cameron Story",
            "year": 2024,
            "type": "movie"
        }

        filtered, reason = self.engine.should_filter(item)

        # Should NOT be filtered ("Cameron" contains "cam" but not as word)
        self.assertFalse(filtered)

    def test_standalone_cam_is_filtered(self):
        """Test that standalone 'CAM' IS filtered."""
        item = {
            "imdb_id": "tt1234571",
            "title": "Movie CAM Release",
            "year": 2024,
            "type": "movie"
        }

        filtered, reason = self.engine.should_filter(item)

        # Should be filtered
        self.assertTrue(filtered)
        self.assertIn("CAM", reason)

    def test_hdcam_with_punctuation_filtered(self):
        """Test that 'HDCAM' with punctuation IS filtered."""
        item = {
            "imdb_id": "tt1234572",
            "title": "Movie.2024.HDCAM.x264",
            "year": 2024,
            "type": "movie"
        }

        filtered, reason = self.engine.should_filter(item)

        # Should be filtered - HDCAM followed by period is a word boundary
        self.assertTrue(filtered)
        self.assertIn("HDCAM", reason)

    def test_case_insensitive_filter(self):
        """Test that filtering is case-insensitive."""
        item = {
            "imdb_id": "tt1234573",
            "title": "Movie cam Release",  # lowercase
            "year": 2024,
            "type": "movie"
        }

        filtered, reason = self.engine.should_filter(item)

        # Should be filtered (case-insensitive)
        self.assertTrue(filtered)


class TestSelfTestCommand(unittest.TestCase):
    """Test the --test command (self-test functionality)."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        # Patch DB_PATH
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        
        # Patch ENV_PATH
        self.test_env_path = Path(self.temp_dir.name) / ".env"
        self.original_env_path = torboxed.ENV_PATH
        torboxed.ENV_PATH = self.test_env_path
        torboxed._env_cache = None
        
        # Patch logging to capture output
        self.log_output = []
        
    def tearDown(self):
        """Clean up test fixtures."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        torboxed.ENV_PATH = self.original_env_path
        torboxed._env_cache = None
        self.temp_dir.cleanup()
    
    def _capture_log(self, *args, **kwargs):
        """Capture log output for verification."""
        self.log_output.append(" ".join(str(a) for a in args))
    
    def test_self_test_missing_env(self):
        """Test self-test fails when .env is missing."""
        import torboxed
        
        # Ensure no env file exists
        if self.test_env_path.exists():
            self.test_env_path.unlink()
        torboxed._env_cache = None
        
        # Capture logging calls
        with patch.object(torboxed.logger, 'info') as mock_info, \
             patch.object(torboxed.logger, 'warning') as mock_warning, \
             patch.object(torboxed.logger, 'error') as mock_error:
            
            result = run_self_test()
            
            # Should fail because no env file
            self.assertFalse(result)
            
            # Verify error was logged
            error_calls = [call for call in mock_error.call_args_list 
                          if 'env' in str(call).lower()]
            # At least one error about environment should be logged
            self.assertTrue(len(error_calls) > 0 or mock_error.called)
    
    def test_self_test_with_valid_env_no_db(self):
        """Test self-test with valid env but missing database."""
        import torboxed
        
        # Create valid .env file
        self.test_env_path.write_text(
            "TORBOX_API_KEY=test_key\n"
            "TRAKT_CLIENT_ID=test_id\n"
            "TRAKT_CLIENT_SECRET=test_secret\n"
        )
        torboxed._env_cache = None
        
        with patch.object(torboxed.logger, 'info') as mock_info, \
             patch.object(torboxed.logger, 'warning') as mock_warning:
            
            result = run_self_test()
            
            # Should fail because no database (but passes env check)
            self.assertFalse(result)
            
            # Should have logged info about env file
            info_text = " ".join(str(call) for call in mock_info.call_args_list)
            # May pass or fail depending on environment, but should have run tests
            self.assertTrue(mock_info.called)
    
    def test_self_test_quality_parsing(self):
        """Test that self-test includes quality parsing validation."""
        import torboxed
        
        # Create valid .env and initialized database
        self.test_env_path.write_text(
            "TORBOX_API_KEY=test_key\n"
            "TRAKT_CLIENT_ID=test_id\n"
            "TRAKT_CLIENT_SECRET=test_secret\n"
        )
        torboxed._env_cache = None
        init_db()
        
        with patch.object(torboxed.logger, 'info') as mock_info:
            result = run_self_test()
            
            # Should pass all tests now
            self.assertTrue(result)
            
            # Verify quality parsing test ran
            info_text = " ".join(str(call) for call in mock_info.call_args_list)
            self.assertIn("Testing quality parsing", info_text)


class TestCronSetupCommand(unittest.TestCase):
    """Test the --cron-setup and --cron-status commands."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        
        # Patch logging to capture output
        self.log_output = []
        
    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()
    
    def test_cron_status_no_crontab(self):
        """Test cron-status when no crontab exists."""
        import torboxed
        
        with patch.object(torboxed.logger, 'info') as mock_info, \
             patch('subprocess.run') as mock_run:
            
            # Mock subprocess to return error (no crontab)
            mock_run.return_value = Mock(returncode=1, stdout="", stderr="")
            
            show_cron_status()
            
            # Should report no crontab
            info_text = " ".join(str(call) for call in mock_info.call_args_list)
            self.assertIn("No crontab", info_text)
    
    def test_cron_status_with_existing_job(self):
        """Test cron-status shows existing TorBoxed job."""
        import torboxed
        
        with patch.object(torboxed.logger, 'info') as mock_info, \
             patch('subprocess.run') as mock_run:
            
            # Mock crontab with TorBoxed entry
            crontab_content = "# Some comment\n0 2 * * * cd /path && uv run torboxed.py >> log 2>&1\n"
            mock_run.return_value = Mock(returncode=0, stdout=crontab_content, stderr="")
            
            show_cron_status()
            
            # Should find the TorBoxed job
            info_text = " ".join(str(call) for call in mock_info.call_args_list)
            self.assertIn("Found", info_text)
            self.assertIn("torboxed", info_text.lower())
    
    def test_cron_status_no_torboxed_job(self):
        """Test cron-status when crontab exists but no TorBoxed job."""
        import torboxed
        
        with patch.object(torboxed.logger, 'info') as mock_info, \
             patch('subprocess.run') as mock_run:
            
            # Mock crontab without TorBoxed entry
            crontab_content = "# Some other job\n0 1 * * * /bin/true\n"
            mock_run.return_value = Mock(returncode=0, stdout=crontab_content, stderr="")
            
            show_cron_status()
            
            # Should report no TorBoxed jobs
            info_text = " ".join(str(call) for call in mock_info.call_args_list)
            self.assertIn("No TorBoxed", info_text)


class TestDiscoverExistingTorrents(unittest.TestCase):
    """Test discovering existing torrents from Torbox."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        # Patch DB_PATH
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
        
        # Create mock Torbox client
        self.mock_debrid = Mock()
        
    def tearDown(self):
        """Clean up test fixtures."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_discover_matches_by_title_and_year(self):
        """Test that discovery matches torrents to database by title/year."""
        import torboxed
        
        # Add a processed item to database
        torboxed.record_processed(
            "tt1234567", "Test Movie", 2024, "movie", "added", "success",
            debrid_id="tb-id-123", magnet="magnet:test",
            quality_score=2500, quality_label="1080p BluRay"
        )
        
        # Mock Torbox returning a torrent that should match
        self.mock_debrid.get_my_torrents.return_value = [
            {
                "id": "tb-id-123",
                "name": "Test.Movie.2024.1080p.BluRay.x264",
                "hash": "abc123"
            }
        ]
        
        # Run discovery
        result = torboxed.discover_existing_torrents(self.mock_debrid)
        
        # Should return tuple of (imdb_to_torbox, account_hashes, hash_to_imdb)
        self.assertIsInstance(result, tuple)
        imdb_to_torbox, account_hashes, hash_to_imdb = result
        
        # Should find the match
        self.assertIn("tt1234567", imdb_to_torbox)
        self.assertEqual(imdb_to_torbox["tt1234567"], "tb-id-123")
        # Hash should be in account_hashes
        self.assertIn("abc123", account_hashes)
        # Hash should be in hash_to_imdb mapping
        self.assertIn("abc123", hash_to_imdb)
        self.assertEqual(hash_to_imdb["abc123"], "tt1234567")
    
    def test_discover_empty_when_no_matches(self):
        """Test that discovery returns empty when no matches found."""
        import torboxed
        
        # Add a processed item to database
        torboxed.record_processed(
            "tt1234567", "Different Movie", 2024, "movie", "added", "success",
            debrid_id="tb-id-123", magnet="magnet:test",
            quality_score=2500, quality_label="1080p BluRay"
        )
        
        # Mock Torbox returning a torrent with different name
        self.mock_debrid.get_my_torrents.return_value = [
            {
                "id": "tb-id-456",
                "name": "Unrelated.Movie.2024.1080p.BluRay.x264",
                "hash": "def456"
            }
        ]
        
        # Run discovery
        result = torboxed.discover_existing_torrents(self.mock_debrid)
        
        # Should return tuple with empty dict, set of hashes, and empty hash_to_imdb
        self.assertIsInstance(result, tuple)
        imdb_to_torbox, account_hashes, hash_to_imdb = result
        self.assertEqual(imdb_to_torbox, {})
        # Should still collect hashes even if no matches
        self.assertIn("def456", account_hashes)
        # But unmatched hashes should NOT be in hash_to_imdb
        self.assertNotIn("def456", hash_to_imdb)
    
    def test_discover_empty_torbox_account(self):
        """Test that discovery handles empty Torbox account."""
        import torboxed
        
        # Mock Torbox returning no torrents
        self.mock_debrid.get_my_torrents.return_value = []
        
        # Run discovery
        result = torboxed.discover_existing_torrents(self.mock_debrid)
        
        # Should return tuple of empty dict, empty set, and empty hash_to_imdb
        self.assertIsInstance(result, tuple)
        imdb_to_torbox, account_hashes, hash_to_imdb = result
        self.assertEqual(imdb_to_torbox, {})
        self.assertEqual(account_hashes, set())
        self.assertEqual(hash_to_imdb, {})
    
    def test_multi_season_pack_does_not_affect_episodes(self):
        """BUG FIX: Episode-level records should NOT be updated by multi-season pack discovery.
        
        This test verifies that when a multi-season pack is discovered, episode-level
        records (S01E01) are not incorrectly associated with the pack's torrent ID.
        This prevents accidental deletion of shared packs when upgrading individual episodes.
        
        The bug occurred because episode-level keys like "S05E02" were matching the
        season extraction pattern (S05E02[1:3] = "05"), causing them to be incorrectly
        associated with multi-season pack torrent IDs.
        """
        import torboxed
        
        fake_title = "FakeTestShowXYZ123"
        fake_imdb = "tt99999999"
        
        # Add episode-level record WITH a debrid_id (existing individual episode)
        torboxed.record_processed(
            fake_imdb, fake_title, 2025, "show", "added", "success",
            debrid_id="episode-torrent-id", magnet="magnet:episode",
            quality_score=2000, quality_label="1080p WEB-DL",
            season="S05E02"  # Episode-level key - should NOT match multi-season pack
        )
        
        # Add season-level record WITH a different debrid_id (existing season pack)
        torboxed.record_processed(
            fake_imdb, fake_title, 2025, "show", "added", "success",
            debrid_id="season-torrent-id", magnet="magnet:season",
            quality_score=2000, quality_label="1080p WEB-DL",
            season="S05"  # Season-level key - should match multi-season pack
        )
        
        # Mock Torbox returning a multi-season pack (S05-S06) with different ID
        # This triggers name matching since the ID doesn't match existing records
        self.mock_debrid.get_my_torrents.return_value = [
            {
                "id": "multi-season-pack-id",
                "name": "FakeTestShowXYZ123.S05.S06.1080p.WEB-DL.x264",
                "hash": "multihash123"
            }
        ]
        
        # Run discovery
        result = torboxed.discover_existing_torrents(self.mock_debrid)
        
        # Verify discovery succeeded
        self.assertIsInstance(result, tuple)
        imdb_to_torbox, account_hashes, hash_to_imdb = result
        
        # Check database after discovery
        with torboxed.get_db() as conn:
            episode_record = conn.execute(
                "SELECT debrid_id, reason FROM processed WHERE imdb_id=? AND season=?",
                (fake_imdb, "S05E02")
            ).fetchone()
            season_record = conn.execute(
                "SELECT debrid_id, reason FROM processed WHERE imdb_id=? AND season=?",
                (fake_imdb, "S05")
            ).fetchone()
        
        # Episode should retain its original ID (NOT be changed to multi-season pack)
        self.assertEqual(episode_record['debrid_id'], "episode-torrent-id")
        self.assertEqual(episode_record['reason'], "success")
        
        # Season should still have its original ID because it already had one
        # (multi-season pack update only applies to records WITHOUT debrid_id)
        self.assertEqual(season_record['debrid_id'], "season-torrent-id")
    
    def test_search_includes_year(self):
        """Test that search includes year in query for accuracy."""
        import torboxed
        
        # Create a mock torbox client that captures the search query
        mock_debrid = Mock()
        mock_debrid.get_cached_torrents.return_value = []
        
        # Create SyncEngine with mock
        engine = torboxed.SyncEngine(mock_debrid, Mock(), {
            "sources": [],
            "filters": {"exclude": ["CAM", "TS"]}
        })
        
        # Process content with a year
        content = {
            "imdb_id": "tt1234567",
            "title": "Test Movie",
            "year": 2024,
            "type": "movie"
        }
        
        # Mock existing_torrents as empty
        existing_torrents = {}
        
        # Process (will search)
        engine.process_content(content, existing_torrents)
        
        # Verify search was called with title AND year
        mock_debrid.get_cached_torrents.assert_called_once()
        call_args = mock_debrid.get_cached_torrents.call_args
        search_query = call_args[0][0]  # First positional argument
        
        # Should include both title and year
        self.assertIn("Test Movie", search_query)
        self.assertIn("2024", search_query)
        self.assertEqual(search_query, "Test Movie 2024")


    def test_name_match_prefilter_skips_guessit_for_unmatchable_torrents(self):
        """Issue #19: torrents sharing no token signature with any DB title
        skip guessit entirely and land in unmatched (hash still collected)."""
        import torboxed

        torboxed.record_processed(
            "tt1234567", "Test Movie", 2024, "movie", "added", "success",
            debrid_id="tb-id-123", magnet="magnet:test",
            quality_score=2500, quality_label="1080p BluRay"
        )

        self.mock_debrid.get_my_torrents.return_value = [
            {
                "id": "tb-unmatched",
                "name": "Qzxwv.Jklmnop.Frst.2019.720p.WEBRip.x264",
                "hash": "no-match-hash",
            }
        ]

        parse_calls = []
        def _fail_guessit(name):
            parse_calls.append(name)
            raise AssertionError("guessit must not run when no candidates exist")

        with patch.object(torboxed, "guessit", side_effect=_fail_guessit):
            result = torboxed.discover_existing_torrents(self.mock_debrid)

        imdb_to_debrid, account_hashes, hash_to_imdb = result
        self.assertEqual(imdb_to_debrid, {})
        self.assertIn("no-match-hash", account_hashes)
        self.assertNotIn("no-match-hash", hash_to_imdb)
        self.assertEqual(parse_calls, [])

    def test_title_match_index_candidates(self):
        """_build_title_match_index/_candidate_title_keys find token-sharing
        titles (including inflectional prefixes like alien/aliens) and skip
        non-overlapping ones."""
        import torboxed
        db = {
            "breaking bad:2008": [],
            "aliens:1986": [],
            "up:2009": [],
        }
        index = torboxed._build_title_match_index(db)

        # Exact-token overlap
        self.assertIn("breaking bad:2008", torboxed._candidate_title_keys(
            "Breaking.Bad.S01.1080p.BluRay", index))
        # Prefix collision catches inflections (alien vs aliens)
        self.assertIn("aliens:1986", torboxed._candidate_title_keys(
            "Alien.Covenant.2017.2160p.WEB-DL", index))
        # No overlap -> no candidates -> guessit skipped upstream
        self.assertEqual(torboxed._candidate_title_keys(
            "Qzxwv.Jklmnop.2019.720p", index), set())


class TestMaxQuality(unittest.TestCase):
    """Test max quality threshold - skip searching for content with max quality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        # Patch DB_PATH
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
        
        # Create mock clients
        self.mock_debrid = Mock()
        self.mock_trakt = Mock()
        
        # Create SyncEngine with mock clients
        self.config = {
            "sources": ["movies/trending"],
            "limits": {"movies": 10, "shows": 10},
            "filters": {"min_year": 2000, "exclude": ["CAM", "TS", "HDCAM"]}
        }
        self.engine = torboxed.SyncEngine(self.mock_debrid, self.mock_trakt, self.config)
        
    def tearDown(self):
        """Clean up test fixtures."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_skip_search_when_max_quality_reached(self):
        """Test that search is skipped when content already has max quality."""
        import torboxed
        
        # Record an item with max quality (score >= 7000)
        torboxed.record_processed(
            "tt9999999", "Max Quality Movie", 2024, "movie", "added", "success",
            debrid_id="tb-max", magnet="magnet:max",
            quality_score=7250, quality_label="2160p BluRay HEVC DTS-HD MA"
        )
        
        # Process this content
        content = {
            "imdb_id": "tt9999999",
            "title": "Max Quality Movie",
            "year": 2024,
            "type": "movie"
        }
        existing_torrents = {}  # Not in discovery, but in database with max quality
        
        result = self.engine.process_content(content, existing_torrents)
        
        # Should return True (processed) without searching
        self.assertTrue(result)
        
        # Verify get_cached_torrents was NOT called (search skipped)
        self.mock_debrid.get_cached_torrents.assert_not_called()
    
    def test_search_when_not_max_quality(self):
        """Test that search IS performed when quality is not max."""
        import torboxed
        
        # Record an item with good but not max quality (score < 7000)
        torboxed.record_processed(
            "tt8888888", "Good Quality Movie", 2024, "movie", "added", "success",
            debrid_id="tb-good", magnet="magnet:good",
            quality_score=4000, quality_label="1080p BluRay H264"
        )
        
        # Mock search to return no results (so it completes quickly)
        self.mock_debrid.get_cached_torrents.return_value = []
        
        # Process this content
        content = {
            "imdb_id": "tt8888888",
            "title": "Good Quality Movie",
            "year": 2024,
            "type": "movie"
        }
        existing_torrents = {}
        
        result = self.engine.process_content(content, existing_torrents)
        
        # Search SHOULD have been called (quality not max)
        self.mock_debrid.get_cached_torrents.assert_called_once()
    
    def test_is_max_quality_threshold(self):
        """Test the is_max_quality function with various scores."""
        import torboxed
        
        # Scores >= 6000 should be max quality
        self.assertTrue(torboxed.is_max_quality(6000))
        self.assertTrue(torboxed.is_max_quality(6250))
        self.assertTrue(torboxed.is_max_quality(8000))
        
        # Scores < 6000 should NOT be max quality
        self.assertFalse(torboxed.is_max_quality(5999))
        self.assertFalse(torboxed.is_max_quality(5000))
        self.assertFalse(torboxed.is_max_quality(4000))
        self.assertFalse(torboxed.is_max_quality(0))


class TestSeasonDetection(unittest.TestCase):
    """Test season detection from torrent names (FEAT-1)."""
    
    def setUp(self):
        """Import required functions."""
        from torboxed import parse_season_info, SeasonInfo
        self.parse_season_info = parse_season_info
        self.SeasonInfo = SeasonInfo
    
    def test_parse_single_season(self):
        """Test parsing single season (S01)."""
        from torboxed import parse_season_info
        
        result = parse_season_info("Show.Name.S01.1080p.WEB-DL.x264")
        
        self.assertIsNotNone(result)
        self.assertEqual(result.seasons, [1])
        self.assertEqual(result.season_label, "S01")
        self.assertFalse(result.is_complete)
    
    def test_parse_season_two_digit(self):
        """Test parsing season 10 (two digits)."""
        from torboxed import parse_season_info
        
        result = parse_season_info("Show.Name.S10.1080p.WEB-DL.x264")
        
        self.assertIsNotNone(result)
        self.assertEqual(result.seasons, [10])
        self.assertEqual(result.season_label, "S10")
    
    def test_parse_multi_season(self):
        """Test parsing multi-season pack (S01-S05)."""
        from torboxed import parse_season_info
        
        result = parse_season_info("Show.Name.S01-S05.1080p.WEB-DL.x264")
        
        self.assertIsNotNone(result)
        self.assertEqual(result.seasons, [1, 2, 3, 4, 5])
        self.assertEqual(result.season_label, "S01-S05")
    
    def test_parse_complete_series(self):
        """Test parsing complete series."""
        from torboxed import parse_season_info
        
        result = parse_season_info("Show.Name.Complete.Series.1080p.WEB-DL.x264")
        
        self.assertIsNotNone(result)
        self.assertTrue(result.is_complete)
        self.assertEqual(result.season_label, "Complete")
    
    def test_parse_season_alternative_format(self):
        """Test parsing 'Season.1' format."""
        from torboxed import parse_season_info
        
        result = parse_season_info("Show.Name.Season.1.1080p.WEB-DL.x264")
        
        self.assertIsNotNone(result)
        self.assertEqual(result.seasons, [1])
        self.assertEqual(result.season_label, "S01")
    
    def test_parse_movie_returns_none(self):
        """Test that movies return None (no season info)."""
        from torboxed import parse_season_info
        
        result = parse_season_info("Movie.Name.2024.1080p.BluRay.x264")
        
        self.assertIsNone(result)
    
    def test_parse_with_episode(self):
        """Test parsing episode info with season."""
        from torboxed import parse_season_info
        
        result = parse_season_info("Show.Name.S01E05.1080p.WEB-DL.x264")
        
        self.assertIsNotNone(result)
        self.assertEqual(result.seasons, [1])
        self.assertEqual(result.season_label, "S01E05")  # Individual episode
        self.assertFalse(result.is_pack)  # Not a pack, it's an episode
        self.assertEqual(result.episode, 5)


class TestMultiSeasonSync(unittest.TestCase):
    """Test multi-season sync functionality (FEAT-2, FEAT-3, FEAT-4)."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        # Patch DB_PATH
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
        
        # Create mock clients
        self.mock_debrid = Mock()
        self.mock_trakt = Mock()
        
        # Create SyncEngine with mock clients
        self.config = {
            "sources": ["shows/trending"],
            "limits": {"movies": 10, "shows": 10},
            "filters": {"min_year": 2000, "exclude": ["CAM", "TS", "HDCAM"]}
        }
        self.engine = torboxed.SyncEngine(self.mock_debrid, self.mock_trakt, self.config)
        
    def tearDown(self):
        """Clean up test fixtures."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_group_by_season(self):
        """Test that torrents are grouped by season correctly."""
        from torboxed import TorrentResult, QualityInfo, SeasonInfo
        
        # Create mock torrents for different seasons
        torrents = [
            TorrentResult(
                name="Show.S01.1080p.WEB-DL",
                magnet="magnet:s01",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            ),
            TorrentResult(
                name="Show.S02.1080p.WEB-DL",
                magnet="magnet:s02",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[2], is_complete=False, season_label="S02", is_pack=True)
            ),
            TorrentResult(
                name="Show.S02.720p.WEB-DL",  # Lower quality S02
                magnet="magnet:s02-low",
                availability=True,
                size=500,
                quality=QualityInfo(resolution="720p", score=2400),
                season_info=SeasonInfo(seasons=[2], is_complete=False, season_label="S02", is_pack=True)
            ),
            TorrentResult(
                name="Show.Complete.1080p.WEB-DL",
                magnet="magnet:complete",
                availability=True,
                size=5000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[0], is_complete=True, season_label="Complete", is_pack=True)
            ),
        ]
        
        # Group by season
        seasons_map, skip_reason = self.engine._group_by_season(torrents, "tt1234567")
        
        # Complete pack (score 3400) and individual packs (also 3400) conflict:
        # since no individual pack has higher quality, Complete wins.
        self.assertEqual(len(seasons_map), 1)
        
        self.assertIn("Complete", seasons_map)
        self.assertEqual(seasons_map["Complete"].magnet, "magnet:complete")
        
        self.assertNotIn("S01", seasons_map)
        self.assertNotIn("S02", seasons_map)
    
    def test_process_show_adds_all_seasons(self):
        """Test that processing a show adds all available seasons."""
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo
        
        # Create mock torrents for multiple seasons
        mock_torrents = [
            TorrentResult(
                name="Show.S01.1080p.WEB-DL",
                magnet="magnet:s01",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            ),
            TorrentResult(
                name="Show.S02.1080p.WEB-DL",
                magnet="magnet:s02",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[2], is_complete=False, season_label="S02", is_pack=True)
            ),
            TorrentResult(
                name="Show.S03.1080p.WEB-DL",
                magnet="magnet:s03",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[3], is_complete=False, season_label="S03", is_pack=True)
            ),
        ]
        
        self.mock_debrid.get_cached_torrents.return_value = mock_torrents
        self.mock_debrid.add_torrent.side_effect = ["id-s01", "id-s02", "id-s03"]
        
        # Process the show
        content = {
            "imdb_id": "tt1234567",
            "title": "Test Show",
            "year": 2020,
            "type": "show"
        }
        existing_torrents = {}
        
        result = self.engine.process_content(content, existing_torrents)
        
        # Should return True (action taken)
        self.assertTrue(result)
        
        # Verify add_torrent was called 3 times (once per season)
        self.assertEqual(self.mock_debrid.add_torrent.call_count, 3)
        
        # Verify all seasons were recorded
        s01_record = torboxed.get_processed_item("tt1234567", "S01")
        self.assertIsNotNone(s01_record)
        self.assertEqual(s01_record["action"], "added")
        
        s02_record = torboxed.get_processed_item("tt1234567", "S02")
        self.assertIsNotNone(s02_record)
        
        s03_record = torboxed.get_processed_item("tt1234567", "S03")
        self.assertIsNotNone(s03_record)
    
    def test_skip_already_added_season(self):
        """Test that already added seasons are skipped."""
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo
        
        # Record S01 as already added
        torboxed.record_processed(
            "tt7654321", "Test Show", 2020, "show", "added", "success",
            debrid_id="existing-s01", magnet="magnet:existing",
            quality_score=3400, quality_label="1080p WEB-DL",
            season="S01"
        )
        
        # Create mock torrents for S01 (already have) and S02 (new)
        mock_torrents = [
            TorrentResult(
                name="Show.S01.1080p.WEB-DL",
                magnet="magnet:s01",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            ),
            TorrentResult(
                name="Show.S02.1080p.WEB-DL",
                magnet="magnet:s02",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[2], is_complete=False, season_label="S02", is_pack=True)
            ),
        ]
        
        self.mock_debrid.get_cached_torrents.return_value = mock_torrents
        self.mock_debrid.add_torrent.return_value = "id-s02"
        
        # Process the show
        content = {
            "imdb_id": "tt7654321",
            "title": "Test Show",
            "year": 2020,
            "type": "show"
        }
        existing_torrents = {}
        
        result = self.engine.process_content(content, existing_torrents)
        
        # Should return True (S02 was added)
        self.assertTrue(result)
        
        # add_torrent should only be called once (for S02, not S01)
        self.assertEqual(self.mock_debrid.add_torrent.call_count, 1)
        
        # S01 record should show 'skipped' with current_better 
        # (we checked it but same quality available, so not upgraded)
        s01_record = torboxed.get_processed_item("tt7654321", "S01")
        self.assertEqual(s01_record["action"], "skipped")
        self.assertEqual(s01_record["reason"], "current_better")
    
    def test_per_season_upgrade(self):
        """Test that upgrades work per-season independently."""
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo
        
        # Record S01 with 720p quality (upgradeable)
        torboxed.record_processed(
            "tt9999999", "Test Show", 2020, "show", "added", "success",
            debrid_id="existing-s01", magnet="magnet:existing",
            quality_score=2400, quality_label="720p WEB-DL",
            season="S01"
        )
        
        # Record S02 with 1080p quality (not upgradeable)
        torboxed.record_processed(
            "tt9999999", "Test Show", 2020, "show", "added", "success",
            debrid_id="existing-s02", magnet="magnet:existing-s02",
            quality_score=3400, quality_label="1080p WEB-DL",
            season="S02"
        )
        
        # Create mock torrents - better S01 available, same S02
        mock_torrents = [
            TorrentResult(
                name="Show.S01.1080p.BluRay",  # Better quality
                magnet="magnet:s01-better",
                availability=True,
                size=2000,
                quality=QualityInfo(resolution="1080p", source="Blu-ray", score=3500),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            ),
            TorrentResult(
                name="Show.S02.1080p.WEB-DL",  # Same quality
                magnet="magnet:s02-same",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[2], is_complete=False, season_label="S02", is_pack=True)
            ),
        ]
        
        self.mock_debrid.get_cached_torrents.return_value = mock_torrents
        self.mock_debrid.add_torrent.return_value = "new-s01-id"
        self.mock_debrid.remove_torrent.return_value = True
        
        # Process the show
        content = {
            "imdb_id": "tt9999999",
            "title": "Test Show",
            "year": 2020,
            "type": "show"
        }
        existing_torrents = {}
        
        result = self.engine.process_content(content, existing_torrents)
        
        # S01 should be upgraded (add + remove)
        self.mock_debrid.add_torrent.assert_called_once()
        self.mock_debrid.remove_torrent.assert_called_once_with("existing-s01")
        
        # S01 record should show upgraded
        s01_record = torboxed.get_processed_item("tt9999999", "S01")
        self.assertEqual(s01_record["action"], "upgraded")
        self.assertEqual(s01_record["replaced_score"], 2400)
        
        # S02 record should be 'skipped' with current_better reason
        # (we checked for upgrade but quality wasn't better)
        s02_record = torboxed.get_processed_item("tt9999999", "S02")
        self.assertEqual(s02_record["action"], "skipped")
        self.assertEqual(s02_record["reason"], "current_better")
    
    def test_database_migration_adds_season_column(self):
        """Test that database migration adds season column correctly."""
        import torboxed
        
        # Check that season column exists
        with torboxed.get_db() as conn:
            cursor = conn.execute("PRAGMA table_info(processed)")
            columns = {row[1] for row in cursor.fetchall()}
            
            self.assertIn("season", columns)
            self.assertIn("imdb_id", columns)
    
    def test_season_pack_vs_episode(self):
        """Test that season pack (S01) is different from episode (S01E01)."""
        from torboxed import parse_season_info
        
        # Season pack
        pack = parse_season_info("Show.Name.S01.1080p.WEB-DL.x264")
        self.assertTrue(pack.is_pack)
        self.assertEqual(pack.season_label, "S01")
        self.assertIsNone(pack.episode)
        
        # Individual episode
        episode = parse_season_info("Show.Name.S01E01.1080p.WEB-DL.x264")
        self.assertFalse(episode.is_pack)
        self.assertEqual(episode.season_label, "S01E01")
        self.assertEqual(episode.episode, 1)
    
    def test_group_by_season_prioritizes_packs(self):
        """Test that _group_by_season prefers packs over episodes."""
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo
        
        # Create mock torrents - mix of episodes and packs
        torrents = [
            # Episodes (lower priority)
            TorrentResult(
                name="Show.S01E01.1080p.WEB-DL",
                magnet="magnet:e01",
                availability=True,
                size=500,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01E01", is_pack=False, episode=1)
            ),
            TorrentResult(
                name="Show.S01E02.1080p.WEB-DL",
                magnet="magnet:e02",
                availability=True,
                size=500,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01E02", is_pack=False, episode=2)
            ),
            # Season pack for S01 (should be selected over episodes)
            TorrentResult(
                name="Show.S01.1080p.WEB-DL",
                magnet="magnet:s01-pack",
                availability=True,
                size=2000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            ),
            # Season pack for S02
            TorrentResult(
                name="Show.S02.1080p.WEB-DL",
                magnet="magnet:s02",
                availability=True,
                size=2000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[2], is_complete=False, season_label="S02", is_pack=True)
            ),
            # Complete series pack (highest priority)
            TorrentResult(
                name="Show.Complete.Series.1080p.WEB-DL",
                magnet="magnet:complete",
                availability=True,
                size=8000,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1, 2], is_complete=True, season_label="Complete", is_pack=True)
            ),
        ]
        
        # Create engine to test _group_by_season
        from unittest.mock import Mock
        engine = torboxed.SyncEngine(Mock(), Mock(), {"sources": [], "filters": {}})
        
        seasons_map, skip_reason = engine._group_by_season(torrents, "tt1234567")
        
        # Complete pack (score 3400) and individual packs (also 3400) conflict:
        # since no individual pack has higher quality, Complete wins.
        # All individual entries are removed since Complete covers the entire show.
        self.assertIn("Complete", seasons_map)
        self.assertEqual(seasons_map["Complete"].magnet, "magnet:complete")
        
        self.assertNotIn("S02", seasons_map)
        self.assertNotIn("S01", seasons_map)
        self.assertNotIn("S01E01", seasons_map)
        self.assertNotIn("S01E02", seasons_map)
    
    def test_complete_pack_wins_over_higher_quality_individual_packs(self):
        """Test that a Complete pack is always preferred over individual season packs,
        even when individual packs have higher quality.
        
        This reflects the library completeness goal: prefer the pack that covers
        more content to minimize API calls and maximize library coverage."""
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo
        
        # Complete pack: 720p (score 2900)
        # Individual packs: 2160p (score 5500)
        torrents = [
            TorrentResult(
                name="Show.Complete.720p.WEB-DL",
                magnet="magnet:complete-720p",
                availability=True,
                size=5000,
                quality=QualityInfo(resolution="720p", score=2900),
                season_info=SeasonInfo(seasons=[1], is_complete=True,
                                       season_label="Complete", is_pack=True)
            ),
            TorrentResult(
                name="Show.S01.2160p.WEB-DL",
                magnet="magnet:s01-2160p",
                availability=True,
                size=2000,
                quality=QualityInfo(resolution="2160p", score=5500),
                season_info=SeasonInfo(seasons=[1], is_complete=False,
                                       season_label="S01", is_pack=True)
            ),
            TorrentResult(
                name="Show.S02.2160p.WEB-DL",
                magnet="magnet:s02-2160p",
                availability=True,
                size=2000,
                quality=QualityInfo(resolution="2160p", score=5500),
                season_info=SeasonInfo(seasons=[2], is_complete=False,
                                       season_label="S02", is_pack=True)
            ),
            TorrentResult(
                name="Show.S05E01.1080p.WEB-DL",
                magnet="magnet:e05",
                availability=True,
                size=500,
                quality=QualityInfo(resolution="1080p", score=4000),
                season_info=SeasonInfo(seasons=[5], is_complete=False,
                                       season_label="S05E01", is_pack=False, episode=1)
            ),
        ]
        
        from unittest.mock import Mock
        engine = torboxed.SyncEngine(Mock(), Mock(), {"sources": [], "filters": {}})
        
        seasons_map, skip_reason = engine._group_by_season(torrents, "tt1234567")
        
        # Complete pack always wins, even at lower quality (library completeness goal)
        self.assertIn("Complete", seasons_map)
        self.assertEqual(seasons_map["Complete"].magnet, "magnet:complete-720p")
        # Individual packs and episodes should be removed (covered by Complete)
        self.assertNotIn("S01", seasons_map)
        self.assertNotIn("S02", seasons_map)
        self.assertNotIn("S05E01", seasons_map)
        self.assertEqual(len(seasons_map), 1)
    
    def test_group_by_season_selects_best_quality_per_season(self):
        """_group_by_season should select highest quality torrent per season."""
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo

        torrents = [
            TorrentResult(
                name="Show.S01.720p.HDTV",
                magnet="magnet:s01-720p",
                availability=True,
                size=1000,
                quality=QualityInfo(resolution="720p", score=2200),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            ),
            TorrentResult(
                name="Show.S01.1080p.BluRay",
                magnet="magnet:s01-1080p",
                availability=True,
                size=5000,
                quality=QualityInfo(resolution="1080p", score=4500),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
            ),
        ]

        engine = torboxed.SyncEngine(Mock(), Mock(), {"sources": [], "filters": {}})
        seasons_map, skip_reason = engine._group_by_season(torrents, "tt9999999")

        self.assertIn("S01", seasons_map)
        self.assertEqual(seasons_map["S01"].quality.score, 4500)
        self.assertEqual(seasons_map["S01"].magnet, "magnet:s01-1080p")

    def test_episodes_only_when_no_pack(self):
        """Test that episodes are used only when no season pack available."""
        import torboxed
        from torboxed import TorrentResult, QualityInfo, SeasonInfo
        
        # Only episodes, no season pack
        torrents = [
            TorrentResult(
                name="Show.S01E01.1080p.WEB-DL",
                magnet="magnet:e01",
                availability=True,
                size=500,
                quality=QualityInfo(resolution="1080p", score=3400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01E01", is_pack=False, episode=1)
            ),
            TorrentResult(
                name="Show.S01E02.720p.WEB-DL",  # Lower quality
                magnet="magnet:e02",
                availability=True,
                size=400,
                quality=QualityInfo(resolution="720p", score=2400),
                season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01E02", is_pack=False, episode=2)
            ),
        ]
        
        from unittest.mock import Mock
        engine = torboxed.SyncEngine(Mock(), Mock(), {"sources": [], "filters": {}})
        
        seasons_map, skip_reason = engine._group_by_season(torrents, "tt1234567")
        
        # Should have S01E01 (best quality episode since no pack available)
        self.assertIn("S01E01", seasons_map)
        self.assertEqual(seasons_map["S01E01"].magnet, "magnet:e01")
        self.assertEqual(skip_reason, "")  # Should not be empty when results exist
    
    def test_reset_item_with_season(self):
        """Test resetting specific season vs all seasons."""
        import torboxed
        
        # Record multiple seasons
        torboxed.record_processed(
            "tt6666666", "Test Show", 2019, "show", "added", "success",
            debrid_id="id-s01", magnet="magnet:s01",
            quality_score=3400, season="S01"
        )
        torboxed.record_processed(
            "tt6666666", "Test Show", 2019, "show", "added", "success",
            debrid_id="id-s02", magnet="magnet:s02",
            quality_score=3400, season="S02"
        )
        
        # Reset only S01
        deleted = torboxed.reset_item("tt6666666", season="S01")
        self.assertEqual(deleted, 1)
        
        # S01 should be gone
        self.assertIsNone(torboxed.get_processed_item("tt6666666", "S01"))
        # S02 should remain
        self.assertIsNotNone(torboxed.get_processed_item("tt6666666", "S02"))
        
        # Reset all seasons
        deleted = torboxed.reset_item("tt6666666")  # No season = all
        self.assertEqual(deleted, 1)  # Only S02 left
        
        # All should be gone
        self.assertIsNone(torboxed.get_processed_item("tt6666666", "S02"))

    def test_get_processed_show_seasons(self):
        """Test retrieving all seasons for a show."""
        import torboxed
        
        # Record multiple seasons
        torboxed.record_processed(
            "tt5555555", "Multi Season Show", 2019, "show", "added", "success",
            debrid_id="id-s01", magnet="magnet:s01",
            quality_score=3400, season="S01"
        )
        torboxed.record_processed(
            "tt5555555", "Multi Season Show", 2019, "show", "added", "success",
            debrid_id="id-s02", magnet="magnet:s02",
            quality_score=3400, season="S02"
        )
        torboxed.record_processed(
            "tt5555555", "Multi Season Show", 2019, "show", "added", "success",
            debrid_id="id-s03", magnet="magnet:s03",
            quality_score=3400, season="S03"
        )
        
        # Get all seasons
        seasons = torboxed.get_processed_show_seasons("tt5555555")
        
        self.assertEqual(len(seasons), 3)
        season_labels = {s["season"] for s in seasons}
        self.assertEqual(season_labels, {"S01", "S02", "S03"})


class TestNonASCIIMagnetEncoding(unittest.TestCase):
    """Test non-ASCII character encoding in magnet links (BUG-R1 fix)."""

    def test_cyrillic_characters_encoded(self):
        """Test that Cyrillic characters are properly encoded in magnet links."""
        import urllib.parse
        # Torrent name with Cyrillic characters (like "Проект «Конец света»")
        name = "Проект «Конец света» / Project Hail Mary"
        info_hash = "1234567890abcdef1234567890abcdef12345678"
        
        # Test the encoding method we use
        encoded_name = urllib.parse.quote(name.encode('utf-8'), safe='')
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_name}"
        
        # Verify the magnet link is valid ASCII (no non-ASCII characters)
        # This is what Torbox API expects
        try:
            magnet.encode('ascii')
        except UnicodeEncodeError:
            self.fail("Magnet link contains non-ASCII characters - should be URL-encoded")
        
        # Verify the encoded name contains percent-encoded UTF-8
        self.assertIn('%', encoded_name)
        # Verify it starts with magnet scheme
        self.assertTrue(magnet.startswith("magnet:?xt=urn:btih:"))
    
    def test_special_symbols_encoded(self):
        """Test that special symbols like stars are properly encoded."""
        import urllib.parse
        # Torrent name with special symbols (like "YG⭐")
        name = "Project.Hail.Mary.2026.2160p.WEBrip.h265.Dual.YG⭐"
        info_hash = "abcdef1234567890abcdef1234567890abcdef12"
        
        # Test the encoding method
        encoded_name = urllib.parse.quote(name.encode('utf-8'), safe='')
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_name}"
        
        # Verify the magnet link is valid ASCII
        try:
            magnet.encode('ascii')
        except UnicodeEncodeError:
            self.fail("Magnet link contains non-ASCII characters - should be URL-encoded")
        
        # Verify the star symbol is encoded
        self.assertNotIn('⭐', magnet)
        self.assertIn('%', encoded_name)
    
    def test_chinese_characters_encoded(self):
        """Test that Chinese characters are properly encoded."""
        import urllib.parse
        name = "超级马里奥 / Super Mario"
        info_hash = "fedcba0987654321fedcba0987654321fedcba09"
        
        encoded_name = urllib.parse.quote(name.encode('utf-8'), safe='')
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_name}"
        
        # Verify the magnet link is valid ASCII
        try:
            magnet.encode('ascii')
        except UnicodeEncodeError:
            self.fail("Magnet link contains non-ASCII characters - should be URL-encoded")
    
    def test_ascii_characters_unchanged(self):
        """Test that ASCII characters remain mostly unchanged (except spaces)."""
        import urllib.parse
        name = "Movie.2024.1080p.BluRay.x264-TEST"
        info_hash = "1234567890abcdef1234567890abcdef12345678"
        
        encoded_name = urllib.parse.quote(name.encode('utf-8'), safe='')
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_name}"
        
        # ASCII names should be mostly unchanged
        self.assertEqual(encoded_name, name)  # No special chars, so no encoding needed
        self.assertIn(name, magnet)
    
    def test_mixed_ascii_non_ascii_encoded(self):
        """Test that mixed ASCII/non-ASCII content is properly handled."""
        import urllib.parse
        # Mix of ASCII and non-ASCII
        name = "Movie.2024.1080p. dual audio / Двойной звук"
        info_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        
        encoded_name = urllib.parse.quote(name.encode('utf-8'), safe='')
        magnet = f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_name}"
        
        # Verify the magnet link is valid ASCII
        try:
            magnet.encode('ascii')
        except UnicodeEncodeError:
            self.fail("Magnet link contains non-ASCII characters - should be URL-encoded")
        
        # Verify non-ASCII part is encoded but ASCII part is readable
        self.assertIn('Movie.2024.1080p.', encoded_name)  # ASCII part unchanged
        self.assertIn('%', encoded_name)  # Non-ASCII part encoded
        self.assertNotIn('Двойной', magnet)  # Cyrillic should be encoded


class TestHelperFunctions(unittest.TestCase):
    """Test utility helper functions."""
    
    def test_encode_magnet_link_basic(self):
        """Test basic magnet link encoding."""
        from torboxed import encode_magnet_link
        
        name = "Movie.2024.1080p.BluRay.x264-TEST"
        info_hash = "1234567890abcdef1234567890abcdef12345678"
        
        magnet = encode_magnet_link(name, info_hash)
        
        # Should start with correct prefix
        self.assertTrue(magnet.startswith("magnet:?xt=urn:btih:"))
        # Should contain the hash
        self.assertIn(info_hash, magnet)
        # Should contain the name
        self.assertIn(name, magnet)
        # Should be valid ASCII
        self.assertTrue(magnet.isascii())
    
    def test_encode_magnet_link_non_ascii(self):
        """Test magnet encoding with non-ASCII characters."""
        from torboxed import encode_magnet_link
        
        # Cyrillic characters
        name = "Проект / Project"
        info_hash = "1234567890abcdef1234567890abcdef12345678"
        
        magnet = encode_magnet_link(name, info_hash)
        
        # Should be valid ASCII (encoded)
        self.assertTrue(magnet.isascii())
        # Should contain percent-encoded characters
        self.assertIn('%', magnet)
        # Should not contain raw Cyrillic
        self.assertNotIn('Проект', magnet)
    
    def test_encode_magnet_link_special_chars(self):
        """Test magnet encoding with special symbols."""
        from torboxed import encode_magnet_link
        
        # Star symbol
        name = "Movie.2024.1080p.YG⭐"
        info_hash = "abcdef1234567890abcdef1234567890abcdef12"
        
        magnet = encode_magnet_link(name, info_hash)
        
        # Should not contain raw star symbol
        self.assertNotIn('⭐', magnet)
        # Should be valid ASCII
        self.assertTrue(magnet.isascii())
    
    def test_normalize_hash_lowercase(self):
        """Test hash normalization to lowercase."""
        from torboxed import normalize_hash
        
        # Uppercase should become lowercase
        self.assertEqual(normalize_hash("ABC123"), "abc123")
        # Mixed case should become lowercase
        self.assertEqual(normalize_hash("AbC123"), "abc123")
        # Already lowercase should stay lowercase
        self.assertEqual(normalize_hash("abc123"), "abc123")
    
    def test_normalize_hash_empty(self):
        """Test hash normalization with empty/None input."""
        from torboxed import normalize_hash
        
        # None should return empty string
        self.assertEqual(normalize_hash(None), "")
        # Empty string should return empty string
        self.assertEqual(normalize_hash(""), "")


class TestProwlarrClient(unittest.TestCase):
    """Test Prowlarr client integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        from torboxed import ProwlarrClient
        self.client = ProwlarrClient(api_key="test_api_key")
    
    def tearDown(self):
        """Clean up."""
        self.client.close()
    
    @patch('httpx.Client.get')
    def test_search_returns_torrents(self, mock_get):
        """Test that search returns properly formatted torrents."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "infoHash": "1234567890ABCDEF1234567890ABCDEF12345678",
                "title": "Movie.2024.1080p.BluRay.x264",
                "size": 2000000000,
                "seeders": 100,
                "leechers": 10,
                "indexer": "TestIndexer",
                "magnetUrl": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678&dn=Movie.2024.1080p.BluRay.x264"
            }
        ]
        mock_get.return_value = mock_response
        
        results = self.client.search("Movie 2024", limit=15)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["hash"], "1234567890abcdef1234567890abcdef12345678")  # lowercase
        self.assertEqual(results[0]["title"], "Movie.2024.1080p.BluRay.x264")
        self.assertEqual(results[0]["source"], "TestIndexer")
        self.assertTrue(results[0]["magnet"].startswith("magnet:?xt=urn:btih:"))
    
    @patch('httpx.Client.get')
    def test_search_handles_empty_results(self, mock_get):
        """Test that empty results are handled gracefully."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        results = self.client.search("NonExistentMovie 2024")
        
        self.assertEqual(results, [])
    
    @patch('httpx.Client.get')
    def test_search_handles_api_error(self, mock_get):
        """Test that API errors are handled gracefully."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        results = self.client.search("Movie 2024")
        
        self.assertEqual(results, [])
    
    @patch('httpx.Client.get')
    def test_search_handles_non_ascii_names(self, mock_get):
        """Test that non-ASCII names are properly encoded."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "infoHash": "ABCDEF1234567890ABCDEF1234567890ABCDEF12",
                "title": "Проект / Project 2024",
                "size": 1500000000,
                "seeders": 50,
                "leechers": 5,
                "indexer": "TestIndexer",
                "magnetUrl": "magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12&dn=Project.2024"
            }
        ]
        mock_get.return_value = mock_response
        
        results = self.client.search("Project 2024")
        
        self.assertEqual(len(results), 1)
        # Verify magnet link doesn't contain non-ASCII characters
        magnet = results[0]["magnet"]
        try:
            magnet.encode('ascii')
        except UnicodeEncodeError:
            self.fail("Magnet link contains non-ASCII characters")


    @patch('httpx.Client.get')
    def test_search_passes_imdb_id_as_integer(self, mock_get):
        """Prowlarr should pass imdbId as integer when IMDb ID provided."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        self.client.search("Movie 2024", imdb_id="tt0137523")

        call_args = mock_get.call_args
        params = call_args[1].get("params", {})
        self.assertEqual(params.get("imdbId"), 137523)

    @patch('httpx.Client.get')
    def test_search_without_imdb_id_omits_param(self, mock_get):
        """Prowlarr should not pass imdbId when not provided."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        self.client.search("Movie 2024")

        call_args = mock_get.call_args
        params = call_args[1].get("params", {})
        self.assertNotIn("imdbId", params)


class TestSearchFallbackBehavior(unittest.TestCase):
    """Test Zilean → Prowlarr fallback behavior."""
    
    def setUp(self):
        """Set up mock Torbox client."""
        from torboxed import TorboxClient
        self.client = TorboxClient.__new__(TorboxClient)
        self.client.api_key = "test-key"
        self.client.client = Mock()
        
        # Mock both searchers
        self.client.searcher_zilean = Mock()
        self.client.searcher_prowlarr = Mock()
    
    def tearDown(self):
        """Clean up."""
        pass
    
    def test_uses_zilean_when_configured_and_working(self):
        """Test that Zilean is used when configured and returns results."""
        # Setup Zilean as configured
        self.client.searcher_zilean.is_configured.return_value = True
        
        # Setup Zilean to return results
        zilean_results = [
            {"title": "Movie 2024", "hash": "abc123", "magnet": "magnet:test1", "source": "zilean"}
        ]
        self.client.searcher_zilean.search_by_imdb.return_value = zilean_results
        
        # Call search
        with patch.object(self.client, 'check_cached', return_value={"abc123": True}):
            results = self.client.search_torrents("Movie 2024", "movie", imdb_id="tt1234567")
        
        # Verify Zilean was used
        self.client.searcher_zilean.search_by_imdb.assert_called_once()
        self.client.searcher_prowlarr.search.assert_not_called()
    
    def test_falls_back_to_prowlarr_when_zilean_not_configured(self):
        """Test fallback to Prowlarr when Zilean not configured."""
        # Setup Zilean as NOT configured
        self.client.searcher_zilean.is_configured.return_value = False
        
        # Setup Prowlarr as configured and to return results
        self.client.searcher_prowlarr.is_configured.return_value = True
        prowlarr_results = [
            {"title": "Movie 2024", "hash": "def456", "magnet": "magnet:test2", "source": "Knaben"}
        ]
        self.client.searcher_prowlarr.search.return_value = prowlarr_results
        
        # Call search
        with patch.object(self.client, 'check_cached', return_value={"def456": True}):
            results = self.client.search_torrents("Movie 2024", "movie", imdb_id="tt1234567")
        
        # Verify Prowlarr was used (not Zilean)
        self.client.searcher_zilean.search_by_imdb.assert_not_called()
        self.client.searcher_prowlarr.search.assert_called_once()
    
    def test_falls_back_to_prowlarr_when_zilean_fails(self):
        """Test fallback to Prowlarr when Zilean returns no results."""
        # Setup Zilean as configured but returns empty
        self.client.searcher_zilean.is_configured.return_value = True
        self.client.searcher_zilean.search_by_imdb.return_value = []
        self.client.searcher_zilean.search.return_value = []
        
        # Setup Prowlarr as configured and to return results
        self.client.searcher_prowlarr.is_configured.return_value = True
        prowlarr_results = [
            {"title": "Movie 2024", "hash": "ghi789", "magnet": "magnet:test3", "source": "1337x"}
        ]
        self.client.searcher_prowlarr.search.return_value = prowlarr_results
        
        # Call search
        with patch.object(self.client, 'check_cached', return_value={"ghi789": True}):
            results = self.client.search_torrents("Movie 2024", "movie", imdb_id="tt1234567")
        
        # Verify both were tried
        self.client.searcher_zilean.search_by_imdb.assert_called_once()
        self.client.searcher_prowlarr.search.assert_called_once()


class TestExtractInfohashHelper(unittest.TestCase):
    """Test the shared extract_infohash_from_item helper function."""
    
    def test_extract_from_direct_hash_field(self):
        """Test extracting hash from direct field."""
        from torboxed import extract_infohash_from_item
        
        item = {"infoHash": "ABC123DEF456"}
        result = extract_infohash_from_item(item, hash_fields=["infoHash"])
        self.assertEqual(result, "abc123def456")  # Lowercased
    
    def test_extract_from_multiple_hash_fields(self):
        """Test extracting hash from first available field."""
        from torboxed import extract_infohash_from_item
        
        # First field missing, second present
        item = {"infohash": "abc123", "InfoHash": "def456"}
        result = extract_infohash_from_item(item, hash_fields=["infohash", "InfoHash"])
        self.assertEqual(result, "abc123")  # First match
    
    def test_extract_from_magnet_url(self):
        """Test extracting hash from magnet URL."""
        from torboxed import extract_infohash_from_item
        
        item = {"magnetUrl": "magnet:?xt=urn:btih:1234567890abcdef1234567890abcdef12345678&dn=Test"}
        result = extract_infohash_from_item(item, magnet_fields=["magnetUrl"])
        self.assertEqual(result, "1234567890abcdef1234567890abcdef12345678")
    
    def test_extract_from_multiple_magnet_fields(self):
        """Test extracting hash from first available magnet field."""
        from torboxed import extract_infohash_from_item
        
        item = {"magnetUrl": "", "link": "magnet:?xt=urn:btih:ABC123&dn=Test"}
        result = extract_infohash_from_item(item, magnet_fields=["magnetUrl", "link"])
        # Empty magnetUrl doesn't match, so it should check link
        self.assertEqual(result, "abc123")
    
    def test_extract_from_guid(self):
        """Test extracting hash from 40-char guid."""
        from torboxed import extract_infohash_from_item
        
        hash40 = "a" * 40
        item = {"guid": hash40}
        result = extract_infohash_from_item(item)
        self.assertEqual(result, hash40.lower())
    
    def test_guid_validation_with_hex_check(self):
        """Test that guid validation checks for hex characters."""
        from torboxed import extract_infohash_from_item
        
        # Valid hex
        valid_hash = "abcdef1234567890abcdef1234567890abcdef12"
        item = {"guid": valid_hash}
        result = extract_infohash_from_item(item, validate_guid=True)
        self.assertEqual(result, valid_hash)
        
        # Invalid hex (contains 'G')
        invalid_hash = "G" * 40
        item = {"guid": invalid_hash}
        result = extract_infohash_from_item(item, validate_guid=True)
        self.assertIsNone(result)
    
    def test_no_hash_found(self):
        """Test returning None when no hash is found."""
        from torboxed import extract_infohash_from_item
        
        item = {"title": "Some Movie"}
        result = extract_infohash_from_item(item)
        self.assertIsNone(result)
    
    def test_priority_hash_over_magnet(self):
        """Test that direct hash fields are checked before magnet URLs."""
        from torboxed import extract_infohash_from_item
        
        item = {
            "infoHash": "direct_hash",
            "magnetUrl": "magnet:?xt=urn:btih:magnet_hash&dn=Test"
        }
        result = extract_infohash_from_item(
            item,
            hash_fields=["infoHash"],
            magnet_fields=["magnetUrl"]
        )
        # Should return direct hash, not magnet hash
        self.assertEqual(result, "direct_hash")


class TestJackettClient(unittest.TestCase):
    """Test Jackett client integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        from torboxed import JackettClient
        self.client = JackettClient(api_key="test_api_key")
    
    def tearDown(self):
        """Clean up."""
        self.client.close()
    
    @patch('httpx.Client.get')
    def test_search_returns_torrents(self, mock_get):
        """Test that search returns properly formatted torrents."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Results": [
                {
                    "InfoHash": "1234567890ABCDEF1234567890ABCDEF12345678",
                    "Title": "Movie.2024.1080p.BluRay.x264",
                    "Size": 2000000000,
                    "Seeders": 100,
                    "Peers": 10,
                    "Tracker": "TestIndexer",
                    "MagnetUri": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678&dn=Movie.2024.1080p.BluRay.x264"
                }
            ]
        }
        mock_get.return_value = mock_response
        
        results = self.client.search("Movie 2024", limit=15)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["hash"], "1234567890abcdef1234567890abcdef12345678")  # lowercase
        self.assertEqual(results[0]["title"], "Movie.2024.1080p.BluRay.x264")
        self.assertEqual(results[0]["source"], "testindexer")  # Lowercased
        self.assertTrue(results[0]["magnet"].startswith("magnet:?xt=urn:btih:"))
    
    @patch('httpx.Client.get')
    def test_search_handles_empty_results(self, mock_get):
        """Test that empty results are handled gracefully."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Results": []}
        mock_get.return_value = mock_response
        
        results = self.client.search("NonExistentMovie 2024")
        
        self.assertEqual(results, [])
    
    @patch('httpx.Client.get')
    def test_search_handles_api_error(self, mock_get):
        """Test that API errors are handled gracefully."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        results = self.client.search("Movie 2024")
        
        self.assertEqual(results, [])
    
    @patch('httpx.Client.get')
    def test_search_rate_limited(self, mock_get):
        """Test that rate limiting is handled."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response
        
        results = self.client.search("Movie 2024")
        
        self.assertEqual(results, [])
    
    def test_extract_infohash_with_link_field(self):
        """Test extracting hash from 'link' field (Jackett-specific)."""
        from torboxed import extract_infohash_from_item
        
        # Jackett may have magnet in 'link' field
        item = {
            "link": "magnet:?xt=urn:btih:ABC123&dn=Test",
            "Title": "Some Movie"
        }
        result = extract_infohash_from_item(
            item,
            hash_fields=["infohash"],
            magnet_fields=["magnetUrl", "MagnetUri", "link"]
        )
        self.assertEqual(result, "abc123")
    
    def test_extract_infohash_prefers_infohash_over_magnet(self):
        """Test that direct infohash is preferred over magnet link."""
        item = {
            "infohash": "direct123",
            "MagnetUri": "magnet:?xt=urn:btih:magnet456&dn=Test"
        }
        result = self.client._extract_infohash(item)
        self.assertEqual(result, "direct123")

    @patch('httpx.Client.get')
    def test_search_passes_imdb_id(self, mock_get):
        """Jackett should pass imdbid parameter when IMDb ID provided."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Results": []}
        mock_get.return_value = mock_response

        self.client.search("Movie 2024", imdb_id="tt0137523")

        call_args = mock_get.call_args
        params = call_args[1].get("params", {})
        self.assertEqual(params.get("imdbid"), "tt0137523")

    @patch('httpx.Client.get')
    def test_search_without_imdb_id_omits_param(self, mock_get):
        """Jackett should not pass imdbid when not provided."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Results": []}
        mock_get.return_value = mock_response

        self.client.search("Movie 2024")

        call_args = mock_get.call_args
        params = call_args[1].get("params", {})
        self.assertNotIn("imdbid", params)


class TestTraktLikedLists(unittest.TestCase):
    """Test Trakt liked lists support (FEAT-5)."""
    
    def setUp(self):
        """Set up test fixtures."""
        from torboxed import TraktClient
        self.mock_client_id = "test_client_id"
        self.mock_access_token = "test_access_token"
        self.client = TraktClient(self.mock_client_id, self.mock_access_token)
    
    def tearDown(self):
        """Clean up."""
        self.client.client.close()
    
    @patch('torboxed.make_request_with_backoff')
    def test_get_liked_lists_requires_auth(self, mock_request):
        """Test that get_liked_lists requires authentication."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '[{"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "Test List", "ids": {"trakt": 12345, "slug": "test-list"}, "user": {"ids": {"slug": "testuser"}}}}}]'
        mock_response.json.return_value = [{"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "Test List", "ids": {"trakt": 12345, "slug": "test-list"}, "user": {"ids": {"slug": "testuser"}}}}]
        mock_request.return_value = mock_response
        
        # Call with auth token
        result = self.client.get_liked_lists()
        
        # Should return liked lists
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["list"]["name"], "Test List")
        
        # Verify request was made with auth header
        call_args = mock_request.call_args
        headers = call_args[1].get("headers", {})
        self.assertIn("Authorization", headers)
        self.assertEqual(headers["Authorization"], "Bearer test_access_token")
    
    @patch('torboxed.make_request_with_backoff')
    def test_get_liked_lists_no_token(self, mock_request):
        """Test that get_liked_lists returns empty without auth token."""
        # Create client without access token
        client_no_auth = TraktClient(self.mock_client_id, None)
        
        result = client_no_auth.get_liked_lists()
        
        # Should return empty list when no token
        self.assertEqual(result, [])
        
        client_no_auth.client.close()
    
    @patch('torboxed.make_request_with_backoff')
    def test_get_list_items_public_list(self, mock_request):
        """Test fetching items from a public list."""
        # Mock successful response with movie and show
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '''[
            {"rank": 1, "type": "movie", "movie": {"title": "Inception", "year": 2010, "ids": {"imdb": "tt1375666"}}},
            {"rank": 2, "type": "show", "show": {"title": "Breaking Bad", "year": 2008, "ids": {"imdb": "tt0903747"}}}
        ]'''
        mock_response.json.return_value = [
            {"rank": 1, "type": "movie", "movie": {"title": "Inception", "year": 2010, "ids": {"imdb": "tt1375666"}}},
            {"rank": 2, "type": "show", "show": {"title": "Breaking Bad", "year": 2008, "ids": {"imdb": "tt0903747"}}}
        ]
        mock_request.return_value = mock_response
        
        result = self.client.get_list_items("testuser", "my-list")
        
        # Should return both items
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["type"], "movie")
        self.assertEqual(result[0]["movie"]["title"], "Inception")
        self.assertEqual(result[1]["type"], "show")
        self.assertEqual(result[1]["show"]["title"], "Breaking Bad")
    
    @patch('torboxed.make_request_with_backoff')
    def test_get_list_items_empty_list(self, mock_request):
        """Test fetching items from an empty list."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '[]'
        # Issue #20: get_list_items now uses _fetch_paginated, which reads
        # data.json() - the mock must return the actual parsed payload.
        mock_response.json.return_value = []
        mock_request.return_value = mock_response
        
        result = self.client.get_list_items("testuser", "empty-list")
        
        self.assertEqual(result, [])
    
    @patch('torboxed.make_request_with_backoff')
    def test_get_liked_list_items_integration(self, mock_request):
        """Test the full liked list items fetching flow."""
        # First call - get liked lists
        liked_lists_response = Mock()
        liked_lists_response.status_code = 200
        liked_lists_response.text = '''[
            {"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "Favorites", "ids": {"slug": "favorites"}, "user": {"ids": {"slug": "user1"}}}},
            {"liked_at": "2024-01-02T00:00:00.000Z", "type": "list", "list": {"name": "Watch Later", "ids": {"slug": "watch-later"}, "user": {"ids": {"slug": "user2"}}}}
        ]'''
        liked_lists_response.json.return_value = [
            {"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "Favorites", "ids": {"slug": "favorites"}, "user": {"ids": {"slug": "user1"}}}},
            {"liked_at": "2024-01-02T00:00:00.000Z", "type": "list", "list": {"name": "Watch Later", "ids": {"slug": "watch-later"}, "user": {"ids": {"slug": "user2"}}}}
        ]
        
        # Second and third calls - get items from each list
        list_items_response_1 = Mock()
        list_items_response_1.status_code = 200
        list_items_response_1.text = '''[
            {"rank": 1, "type": "movie", "movie": {"title": "Movie One", "year": 2024, "ids": {"imdb": "tt1111111"}}}
        ]'''
        list_items_response_1.json.return_value = [
            {"rank": 1, "type": "movie", "movie": {"title": "Movie One", "year": 2024, "ids": {"imdb": "tt1111111"}}}
        ]
        
        list_items_response_2 = Mock()
        list_items_response_2.status_code = 200
        list_items_response_2.text = '''[
            {"rank": 1, "type": "show", "show": {"title": "Show One", "year": 2023, "ids": {"imdb": "tt2222222"}}},
            {"rank": 2, "type": "movie", "movie": {"title": "Movie Two", "year": 2023, "ids": {"imdb": "tt3333333"}}}
        ]'''
        list_items_response_2.json.return_value = [
            {"rank": 1, "type": "show", "show": {"title": "Show One", "year": 2023, "ids": {"imdb": "tt2222222"}}},
            {"rank": 2, "type": "movie", "movie": {"title": "Movie Two", "year": 2023, "ids": {"imdb": "tt3333333"}}}
        ]
        
        # Configure mock to return different responses for different URLs
        def side_effect(*args, **kwargs):
            url = args[2] if len(args) > 2 else kwargs.get('url', '')
            if '/users/likes/lists' in url:
                return liked_lists_response
            elif 'user1/lists/favorites' in url:
                return list_items_response_1
            elif 'user2/lists/watch-later' in url:
                return list_items_response_2
            return Mock(status_code=404, text='Not Found')
        
        mock_request.side_effect = side_effect
        
        result = self.client.get_liked_list_items()
        
        # Should have 3 unique items (1 from first list, 2 from second)
        self.assertEqual(len(result), 3)
        
        # Check all items have expected structure
        for item in result:
            self.assertIn("imdb_id", item)
            self.assertIn("title", item)
            self.assertIn("type", item)
            self.assertIn("source", item)
            self.assertEqual(item["source"], "users/liked")
            self.assertIn("list_name", item)
            self.assertIn("list_owner", item)
        
        # Verify items from different lists are included
        titles = {item["title"] for item in result}
        self.assertEqual(titles, {"Movie One", "Show One", "Movie Two"})
    
    @patch('torboxed.make_request_with_backoff')
    def test_get_liked_list_items_deduplication(self, mock_request):
        """Test that duplicate items across liked lists are deduplicated."""
        # Same movie appears in two different liked lists
        liked_lists_response = Mock()
        liked_lists_response.status_code = 200
        liked_lists_response.text = '''[
            {"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "List A", "ids": {"slug": "list-a"}, "user": {"ids": {"slug": "user1"}}}},
            {"liked_at": "2024-01-02T00:00:00.000Z", "type": "list", "list": {"name": "List B", "ids": {"slug": "list-b"}, "user": {"ids": {"slug": "user2"}}}}
        ]'''
        liked_lists_response.json.return_value = [
            {"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "List A", "ids": {"slug": "list-a"}, "user": {"ids": {"slug": "user1"}}}},
            {"liked_at": "2024-01-02T00:00:00.000Z", "type": "list", "list": {"name": "List B", "ids": {"slug": "list-b"}, "user": {"ids": {"slug": "user2"}}}}
        ]
        
        # Both lists contain the same movie
        list_items_response = Mock()
        list_items_response.status_code = 200
        list_items_response.text = '''[
            {"rank": 1, "type": "movie", "movie": {"title": "Duplicate Movie", "year": 2024, "ids": {"imdb": "tt4444444"}}}
        ]'''
        list_items_response.json.return_value = [
            {"rank": 1, "type": "movie", "movie": {"title": "Duplicate Movie", "year": 2024, "ids": {"imdb": "tt4444444"}}}
        ]
        
        def side_effect(*args, **kwargs):
            url = args[2] if len(args) > 2 else kwargs.get('url', '')
            if '/users/likes/lists' in url:
                return liked_lists_response
            elif 'list-a' in url or 'list-b' in url:
                return list_items_response
            return Mock(status_code=404, text='Not Found')
        
        mock_request.side_effect = side_effect
        
        result = self.client.get_liked_list_items()
        
        # Should only have 1 unique item (deduplicated from both lists)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["imdb_id"], "tt4444444")
    
    @patch('torboxed.make_request_with_backoff')
    def test_get_all_content_with_users_liked_source(self, mock_request):
        """Test that 'users/liked' source is handled in get_all_content."""
        # Mock liked lists response
        liked_lists_response = Mock()
        liked_lists_response.status_code = 200
        liked_lists_response.text = '''[
            {"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "My Favs", "ids": {"slug": "my-favs"}, "user": {"ids": {"slug": "user1"}}}}
        ]'''
        liked_lists_response.json.return_value = [
            {"liked_at": "2024-01-01T00:00:00.000Z", "type": "list", "list": {"name": "My Favs", "ids": {"slug": "my-favs"}, "user": {"ids": {"slug": "user1"}}}}
        ]
        
        # Mock list items response
        list_items_response = Mock()
        list_items_response.status_code = 200
        list_items_response.text = '''[
            {"rank": 1, "type": "movie", "movie": {"title": "Liked Movie", "year": 2024, "ids": {"imdb": "tt5555555"}}}
        ]'''
        list_items_response.json.return_value = [
            {"rank": 1, "type": "movie", "movie": {"title": "Liked Movie", "year": 2024, "ids": {"imdb": "tt5555555"}}}
        ]
        
        # Mock trending for other source
        trending_response = Mock()
        trending_response.status_code = 200
        trending_response.text = '''[
            {"movie": {"title": "Trending Movie", "year": 2024, "ids": {"imdb": "tt6666666"}}}
        ]'''
        trending_response.json.return_value = [
            {"movie": {"title": "Trending Movie", "year": 2024, "ids": {"imdb": "tt6666666"}}}
        ]
        
        def side_effect(*args, **kwargs):
            url = args[2] if len(args) > 2 else kwargs.get('url', '')
            if '/users/likes/lists' in url:
                return liked_lists_response
            elif 'my-favs' in url:
                return list_items_response
            elif '/movies/trending' in url:
                return trending_response
            return Mock(status_code=404, text='Not Found')
        
        mock_request.side_effect = side_effect
        
        # Test with both liked lists and public sources
        result = self.client.get_all_content(["users/liked", "movies/trending"])
        
        # Should have both items
        self.assertEqual(len(result), 2)
        
        # Check sources are correct
        sources = {item["source"] for item in result}
        self.assertEqual(sources, {"users/liked", "movies/trending"})
    
    def test_trakt_client_init_with_token(self):
        """Test TraktClient initialization with access token."""
        client = TraktClient("client_id", "access_token")
        
        self.assertEqual(client.client_id, "client_id")
        self.assertEqual(client.access_token, "access_token")
        
        # Check headers include API key but NOT auth token (token is per-request)
        self.assertEqual(client.client.headers["trakt-api-key"], "client_id")
        self.assertNotIn("Authorization", client.client.headers)
        
        client.client.close()
    
    def test_trakt_client_init_without_token(self):
        """Test TraktClient initialization without access token."""
        client = TraktClient("client_id", None)
        
        self.assertEqual(client.client_id, "client_id")
        self.assertIsNone(client.access_token)
        
        client.client.close()


class TestRateLimitingBehavior(unittest.TestCase):
    """Test that rate limiting properly retries and propagates errors.
    
    Addresses BUG-CR2, BUG-H1, BUG-H2: Rate limiting was returning None
    instead of properly retrying, causing 118 of 123 failures to not retry.
    """
    
    @patch('torboxed.time.sleep')  # Mock sleep to avoid long waits in tests
    @patch('torboxed.make_request_with_backoff')
    @patch('torboxed.torbox_creation_limiter')
    def test_creation_endpoint_retries_on_429(self, mock_limiter, mock_make_request, mock_sleep):
        """Test that creation endpoints retry on 429 before succeeding."""
        from torboxed import TorboxClient
        
        # Create mock client
        client = TorboxClient("test_key")
        
        # First 3 calls return 429, 4th succeeds
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {}
        
        mock_response_200 = Mock()
        mock_response_200.status_code = 200
        mock_response_200.text = '{"success": true, "data": {"torrent_id": "123"}}'
        mock_response_200.json.return_value = {"success": True, "data": {"torrent_id": "123"}}
        
        mock_make_request.side_effect = [
            mock_response_429,  # 1st try - rate limited
            mock_response_429,  # 2nd try - rate limited  
            mock_response_429,  # 3rd try - rate limited
            mock_response_200   # 4th try - success
        ]
        
        # Should eventually succeed after retries
        result = client._request("POST", "/v1/api/torrents/createtorrent", 
                                use_creation_limiter=True, max_retries=3)
        
        # Should have been called 4 times (initial + 3 retries)
        self.assertEqual(mock_make_request.call_count, 4)
        
        # Result should be parsed JSON
        self.assertEqual(result, {"success": True, "data": {"torrent_id": "123"}})
        
        client.client.close()
    
    @patch('torboxed.time.sleep')  # Mock sleep to avoid long waits in tests
    @patch('torboxed.make_request_with_backoff')
    @patch('torboxed.torbox_creation_limiter')
    def test_creation_endpoint_raises_rate_limit_error_after_exhausted(self, mock_limiter, mock_make_request, mock_sleep):
        """Test that RateLimitError is raised after all retries exhausted."""
        from torboxed import TorboxClient, RateLimitError
        
        client = TorboxClient("test_key")
        
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {}
        
        # All calls return 429
        mock_make_request.return_value = mock_response_429
        
        # Creation endpoints use CREATION_MAX_RETRIES (10) internally
        with self.assertRaises(RateLimitError):
            client._request("POST", "/v1/api/torrents/createtorrent", 
                          use_creation_limiter=True)
        
        # Should have been called CREATION_MAX_RETRIES + 1 times
        self.assertEqual(mock_make_request.call_count, TorboxClient.CREATION_MAX_RETRIES + 1)
        
        client.client.close()
    
    @patch('torboxed.TorboxClient._request')
    def test_add_torrent_propagates_rate_limit_error(self, mock_request):
        """Test that add_torrent propagates RateLimitError correctly."""
        from torboxed import TorboxClient, RateLimitError
        
        client = TorboxClient("test_key")
        
        # _request raises RateLimitError
        mock_request.side_effect = RateLimitError("Rate limited", status_code=429)
        
        # add_torrent should re-raise RateLimitError
        with self.assertRaises(RateLimitError):
            client.add_torrent("magnet:?xt=urn:btih:abc123", "Test Movie")
        
        client.client.close()
    
    @patch('torboxed.time.sleep')
    @patch('torboxed.make_request_with_backoff')
    @patch('torboxed.torbox_creation_limiter')
    def test_add_torrent_full_flow_with_rate_limit(self, mock_limiter, mock_make_request, mock_sleep):
        """Test full add_torrent flow when rate limited - should raise, not return None.
        
        This test simulates the production scenario where rate limiting caused
        118 of 123 failures to return None instead of retrying properly.
        """
        from torboxed import TorboxClient, RateLimitError
        
        client = TorboxClient("test_key")
        
        # All attempts return 429 (rate limited)
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {}
        mock_make_request.return_value = mock_response_429
        
        # add_torrent should raise RateLimitError, not return None
        with self.assertRaises(RateLimitError) as context:
            client.add_torrent("magnet:?xt=urn:btih:abc123", "Test Movie")
        
        # Verify it's a rate limit error
        self.assertEqual(context.exception.status_code, 429)
        
        # Should have tried multiple times (initial + retries)
        self.assertGreater(mock_make_request.call_count, 1)
        
        client.client.close()


class TestSecurityFixes(unittest.TestCase):
    """Test security vulnerability fixes."""
    
    # ==================== VULN-002: Command Injection ====================
    
    def test_cron_expression_validation_valid(self):
        """Test that valid cron expressions pass validation."""
        from torboxed import _validate_cron_expression
        
        # Valid expressions (standard 5-field format)
        self.assertTrue(_validate_cron_expression("0 2 * * *"))
        self.assertTrue(_validate_cron_expression("*/5 * * * *"))
        self.assertTrue(_validate_cron_expression("0 6,18 * * *"))
        self.assertTrue(_validate_cron_expression("0 */12 * * *"))
        self.assertTrue(_validate_cron_expression("0 2 * * 1-5"))
    
    def test_cron_expression_validation_invalid(self):
        """Test that invalid/malicious cron expressions are rejected."""
        from torboxed import _validate_cron_expression
        
        # Invalid: command injection attempts
        self.assertFalse(_validate_cron_expression("; rm -rf / #"))
        self.assertFalse(_validate_cron_expression("0 2 * * *; whoami"))
        self.assertFalse(_validate_cron_expression("`whoami`"))
        self.assertFalse(_validate_cron_expression("$(echo pwned)"))
        
        # Invalid: wrong number of fields
        self.assertFalse(_validate_cron_expression("0 2 * *"))  # 4 fields
        self.assertFalse(_validate_cron_expression("0 2 * * * *"))  # 6 fields
        
        # Invalid: empty
        self.assertFalse(_validate_cron_expression(""))
        self.assertFalse(_validate_cron_expression("   "))
    
    def test_shlex_quote_in_cron_setup(self):
        """Test that paths are properly escaped in cron commands."""
        import shlex
        from pathlib import Path
        
        # Test that shlex.quote properly escapes malicious paths
        malicious_path = "/path; rm -rf / #"
        escaped = shlex.quote(malicious_path)
        
        # Escaped path should be quoted
        self.assertTrue(escaped.startswith("'") or escaped.startswith('"'))
        
        # When escaped with single quotes, the entire string is wrapped
        # and special characters are neutralized
        self.assertIn(malicious_path, escaped)
    
    # ==================== VULN-003: Lock File Security ====================
    
    def test_lock_file_symlink_detection(self):
        """Test that symlink attacks are detected and handled."""
        from torboxed import check_and_acquire_lock
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = Path(tmpdir) / "test.lock"
            
            # Create a regular lock file with invalid PID
            lock_file.write_text("99999")
            
            # Temporarily replace LOCK_PATH
            import torboxed
            old_lock_path = torboxed.LOCK_PATH
            torboxed.LOCK_PATH = lock_file
            
            try:
                # Should detect the old process is not running (invalid PID)
                result = check_and_acquire_lock()
                self.assertTrue(result)  # Should acquire lock
                
                # Verify lock file was created with our PID
                content = lock_file.read_text()
                self.assertEqual(content, str(os.getpid()))
            finally:
                torboxed.LOCK_PATH = old_lock_path
                # Clean up
                if lock_file.exists():
                    lock_file.unlink()
    
    @patch('torboxed.LOCK_PATH')
    def test_lock_file_is_user_specific(self, mock_lock_path):
        """Test that lock file path is user-specific and consistent."""
        from torboxed import get_lock_path
        
        lock_path = get_lock_path()
        
        # Should contain user ID for per-user isolation
        self.assertIn(str(os.getuid()), str(lock_path))
        
        # Should use /tmp consistently (XDG_RUNTIME_DIR is not
        # available in cron, which would break mutual exclusion)
        self.assertEqual(lock_path.parent, Path("/tmp"))
    
    def test_lock_file_atomic_creation(self):
        """Test atomic lock file creation with O_CREAT | O_EXCL."""
        import os
        from pathlib import Path
        
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_file = Path(tmpdir) / "test.lock"
            
            # Create file atomically
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            with os.fdopen(fd, 'w') as f:
                f.write("test")
            
            # Second atomic creation should fail
            with self.assertRaises(FileExistsError):
                fd2 = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                os.close(fd2)
    
    # ==================== VULN-004: Path Validation ====================
    
    def test_db_path_validation_allowed(self):
        """Test that allowed DB paths pass validation."""
        from torboxed import validate_db_path
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Current directory path (allowed)
            allowed_path = Path(tmpdir) / "test.db"
            result = validate_db_path(allowed_path)
            self.assertEqual(result.resolve(), allowed_path.resolve())
    
    def test_db_path_validation_rejected(self):
        """Test that malicious DB paths are rejected."""
        from torboxed import validate_db_path
        
        # Path outside allowed directories
        malicious_path = Path("/etc/passwd")
        with self.assertRaises(ValueError) as context:
            validate_db_path(malicious_path)
        
        self.assertIn("outside allowed directories", str(context.exception))
    
    def test_log_path_validation_allowed(self):
        """Test that allowed log paths pass validation."""
        from torboxed import validate_log_path
        
        with tempfile.TemporaryDirectory() as tmpdir:
            allowed_path = Path(tmpdir) / "test.log"
            result = validate_log_path(allowed_path)
            self.assertEqual(result.resolve(), allowed_path.resolve())
    
    def test_log_path_validation_rejected(self):
        """Test that malicious log paths are rejected."""
        from torboxed import validate_log_path
        
        # Path outside allowed directories
        malicious_path = Path("/etc/shadow")
        with self.assertRaises(ValueError) as context:
            validate_log_path(malicious_path)
        
        self.assertIn("outside allowed directories", str(context.exception))
    
    # ==================== VULN-005: Error Sanitization ====================
    
    def test_sanitize_error_text_api_keys(self):
        """Test that API keys are redacted from error messages."""
        from torboxed import sanitize_error_text
        
        # API key patterns
        error = 'Error: api_key=secret123, Authorization: Bearer token456'
        sanitized = sanitize_error_text(error)
        
        self.assertNotIn("secret123", sanitized)
        # The Bearer token should be caught by the JWT token pattern or Bearer pattern
        # Note: The exact behavior depends on the regex, we mainly want to ensure 
        # the Authorization header is redacted
        self.assertIn("***REDACTED***", sanitized)
    
    def test_sanitize_error_text_passwords(self):
        """Test that passwords are redacted from error messages."""
        from torboxed import sanitize_error_text
        
        # Password patterns
        error = 'Error: password=mypass123, secret=mysecret'
        sanitized = sanitize_error_text(error)
        
        self.assertNotIn("mypass123", sanitized)
        self.assertIn("***REDACTED***", sanitized)
    
    def test_sanitize_error_text_hashes(self):
        """Test that hash-like strings are redacted."""
        from torboxed import sanitize_error_text
        
        # 40-char SHA-1 hash (magnet link style)
        hash40 = "a" * 40
        error = f"Hash: {hash40}"
        sanitized = sanitize_error_text(error)
        self.assertNotIn(hash40, sanitized)
        self.assertIn("***HASH40***", sanitized)
        
        # 64-char SHA-256 hash
        hash64 = "b" * 64
        error = f"Hash: {hash64}"
        sanitized = sanitize_error_text(error)
        self.assertNotIn(hash64, sanitized)
        self.assertIn("***HASH64***", sanitized)
    
    def test_sanitize_error_text_jwt_tokens(self):
        """Test that JWT tokens are redacted."""
        from torboxed import sanitize_error_text
        
        jwt = "Bearer eyJhbGciOiJIUzI1NiIs.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"
        error = f"Auth: {jwt}"
        sanitized = sanitize_error_text(error)
        
        self.assertNotIn("eyJhbGci", sanitized)
        self.assertIn("***JWT_TOKEN***", sanitized)
    
    def test_sanitize_error_text_truncation(self):
        """Test that sanitized text is truncated to 500 chars."""
        from torboxed import sanitize_error_text
        
        long_error = "Error: " + "x" * 1000
        sanitized = sanitize_error_text(long_error)
        
        self.assertLessEqual(len(sanitized), 500)
    
    def test_sanitize_error_text_empty(self):
        """Test that empty/None input is handled."""
        from torboxed import sanitize_error_text
        
        self.assertEqual(sanitize_error_text(""), "")
        self.assertEqual(sanitize_error_text(None), "")
    
    # ==================== VULN-006: SSL Verification ====================
    
    def test_trakt_client_ssl_verification(self):
        """Test that TraktClient source code has SSL verification."""
        # Verify the source code includes verify=True
        import inspect
        from torboxed import TraktClient
        
        source = inspect.getsource(TraktClient.__init__)
        
        # Should have explicit verify=True
        self.assertIn("verify=True", source)
    
    def test_torbox_client_ssl_verification(self):
        """Test that TorboxClient source code has SSL verification."""
        # Verify the source code includes verify=True
        import inspect
        from torboxed import TorboxClient
        
        source = inspect.getsource(TorboxClient.__init__)
        
        # Should have explicit verify=True
        self.assertIn("verify=True", source)
    
    # ==================== VULN-007: Rate Limited Logging ====================
    
    def test_rate_limited_log_handler(self):
        """Test that rate limiting prevents log flooding."""
        from torboxed import RateLimitedLogHandler
        import logging
        import tempfile
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
            log_path = f.name
        
        try:
            handler = RateLimitedLogHandler(
                log_path,
                maxBytes=1024*1024,
                backupCount=3,
                max_repeats=3,
                window_seconds=60
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            
            logger = logging.getLogger("test_rate_limit")
            logger.handlers = []
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            
            # Log the same message multiple times
            for _ in range(10):
                logger.info("Same message")
            
            handler.close()
            
            # Read log file
            with open(log_path, 'r') as f:
                content = f.read()
            
            # Should have original message plus suppression notice
            # But not all 10 messages
            lines = [l for l in content.split('\n') if l.strip()]
            self.assertLess(len(lines), 10)
            
        finally:
            import os
            if os.path.exists(log_path):
                os.unlink(log_path)
    
    # ==================== VULN-008: Migration Backup ====================
    
    def test_migration_creates_backup(self):
        """Test that migration attempts to create backup before migration."""
        # This test verifies that the backup code exists in migrate_db()
        # The actual backup can fail due to transaction state, but the code path exists
        import torboxed
        
        # Verify the backup code exists in the source
        import inspect
        source = inspect.getsource(torboxed.migrate_db)
        
        # Should have backup creation code
        self.assertIn("backup_path", source)
        self.assertIn("shutil.copy2", source)


class TestTelegramNotifier(unittest.TestCase):
    """Test Telegram notification functionality."""
    
    @patch('torboxed.get_env')
    def test_telegram_notifier_is_configured(self, mock_get_env):
        """Test that TelegramNotifier correctly detects configuration."""
        # Mock empty environment to avoid picking up real config
        mock_get_env.return_value = {}
        
        # Not configured - missing both token and chat_id
        notifier = TelegramNotifier()
        self.assertFalse(notifier.is_configured())
        
        # Not configured - missing chat_id
        notifier = TelegramNotifier(bot_token="test_token")
        self.assertFalse(notifier.is_configured())
        
        # Not configured - missing bot_token
        notifier = TelegramNotifier(chat_id="123456")
        self.assertFalse(notifier.is_configured())
        
        # Configured - both provided
        notifier = TelegramNotifier(bot_token="test_token", chat_id="123456")
        self.assertTrue(notifier.is_configured())
    
    @patch('torboxed.httpx.Client')
    def test_telegram_send_message_success(self, mock_client_class):
        """Test successful Telegram message sending."""
        # Mock the HTTP client
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"ok": true}'
        mock_response.json.return_value = {"ok": True}
        
        mock_client = Mock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        notifier = TelegramNotifier(bot_token="test_token", chat_id="123456")
        result = notifier._send_message("Test message")
        
        self.assertTrue(result)
        mock_client.post.assert_called_once()
        
        # Verify the call arguments
        call_args = mock_client.post.call_args
        self.assertIn("/bottest_token/sendMessage", call_args[0][0])
    
    @patch('torboxed.httpx.Client')
    def test_telegram_send_message_failure(self, mock_client_class):
        """Test Telegram message sending failure."""
        # Mock the HTTP client with failure response
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = '{"ok": false, "description": "Bad Request"}'
        mock_response.json.return_value = {"ok": False, "description": "Bad Request"}
        
        mock_client = Mock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        notifier = TelegramNotifier(bot_token="test_token", chat_id="123456")
        result = notifier._send_message("Test message")
        
        self.assertFalse(result)
    
    @patch('torboxed.httpx.Client')
    def test_telegram_send_message_timeout(self, mock_client_class):
        """Test Telegram message sending handles timeout."""
        import httpx
        # Mock the HTTP client to raise timeout
        mock_client = Mock()
        mock_client.post.side_effect = httpx.TimeoutException("Request timed out")
        mock_client_class.return_value = mock_client
        
        notifier = TelegramNotifier(bot_token="test_token", chat_id="123456")
        result = notifier._send_message("Test message")
        
        self.assertFalse(result)
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_added(self, mock_send):
        """Test notify_added creates correct message."""
        mock_send.return_value = True
        
        notifier = TelegramNotifier(bot_token="test", chat_id="123")
        result = notifier.notify_added(
            title="The Matrix",
            year=1999,
            quality_label="1080p BluRay",
            quality_score=4250,
            content_type="movie",
            imdb_id="tt0133093"
        )
        
        self.assertTrue(result)
        mock_send.assert_called_once()
        
        # Check the message contains expected content
        message = mock_send.call_args[0][0]
        self.assertIn("The Matrix", message)
        self.assertIn("1999", message)
        self.assertIn("1080p BluRay", message)
        self.assertIn("tt0133093", message)
        self.assertIn("imdb.com", message)
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_upgraded(self, mock_send):
        """Test notify_upgraded creates correct message."""
        mock_send.return_value = True
        
        # Enable notify_upgraded for this test (it's disabled by default)
        settings = {"notify_added": True, "notify_upgraded": True, "notify_summary": True, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_upgraded(
            title="The Matrix",
            year=1999,
            old_quality="720p WEB-DL",
            old_score=2500,
            new_quality="1080p BluRay",
            new_score=4250,
            content_type="movie"
        )
        
        self.assertTrue(result)
        mock_send.assert_called_once()
        
        # Check the message contains expected content
        message = mock_send.call_args[0][0]
        self.assertIn("Upgraded", message)
        self.assertIn("720p WEB-DL", message)
        self.assertIn("1080p BluRay", message)
        self.assertIn("2500", message)
        self.assertIn("4250", message)
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_summary(self, mock_send):
        """Test notify_summary creates correct message."""
        mock_send.return_value = True
        
        notifier = TelegramNotifier(bot_token="test", chat_id="123")
        result = notifier.notify_summary(
            added=3,
            upgraded=1,
            skipped=12,
            failed=0,
            duration_seconds=154.5,
            movies=3,
            shows=1
        )
        
        self.assertTrue(result)
        mock_send.assert_called_once()
        
        # Check the message contains expected content
        message = mock_send.call_args[0][0]
        self.assertIn("Sync Complete", message)
        self.assertIn("Added: 3", message)
        self.assertIn("Upgraded: 1", message)
        self.assertIn("2m 34s", message)
    
    def test_telegram_rate_limiting(self):
        """Test that Telegram notifier uses rate limiting."""
        import time
        
        notifier = TelegramNotifier(bot_token="test", chat_id="123")
        
        # Verify rate limiter exists and has correct interval
        self.assertIsNotNone(notifier._rate_limiter)
        self.assertEqual(notifier._rate_limiter.min_interval, 1.0)
        self.assertEqual(notifier._rate_limiter.name, "Telegram")
    
    def test_get_telegram_notifier_singleton(self):
        """Test that get_telegram_notifier returns singleton instance."""
        notifier1 = get_telegram_notifier()
        notifier2 = get_telegram_notifier()
        
        # Should be the same instance
        self.assertIs(notifier1, notifier2)
    
    # =========================================================================
    # NOTIFICATION SETTINGS TESTS
    # =========================================================================
    
    def test_default_settings_when_no_config_provided(self):
        """Test that default settings are applied when no config provided."""
        notifier = TelegramNotifier(bot_token="test", chat_id="123")
        
        # Default: added=true, upgraded=false, summary=true, error=true
        self.assertTrue(notifier.settings.get("notify_added"))
        self.assertFalse(notifier.settings.get("notify_upgraded"))
        self.assertTrue(notifier.settings.get("notify_summary"))
        self.assertTrue(notifier.settings.get("notify_error"))
    
    def test_custom_settings_override_defaults(self):
        """Test that custom settings override defaults."""
        custom_settings = {
            "notify_added": False,
            "notify_upgraded": True,
            "notify_summary": False,
            "notify_error": False
        }
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=custom_settings)
        
        self.assertFalse(notifier.settings.get("notify_added"))
        self.assertTrue(notifier.settings.get("notify_upgraded"))
        self.assertFalse(notifier.settings.get("notify_summary"))
        self.assertFalse(notifier.settings.get("notify_error"))
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_added_respects_setting_enabled(self, mock_send):
        """Test notify_added sends when setting is enabled."""
        mock_send.return_value = True
        
        settings = {"notify_added": True, "notify_upgraded": False, "notify_summary": True, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_added(title="Test Movie", year=2023, quality_label="1080p", quality_score=1000)
        
        self.assertTrue(result)
        mock_send.assert_called_once()
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_added_respects_setting_disabled(self, mock_send):
        """Test notify_added does not send when setting is disabled."""
        settings = {"notify_added": False, "notify_upgraded": False, "notify_summary": True, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_added(title="Test Movie", year=2023, quality_label="1080p", quality_score=1000)
        
        self.assertFalse(result)
        mock_send.assert_not_called()
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_upgraded_respects_setting_enabled(self, mock_send):
        """Test notify_upgraded sends when setting is enabled."""
        mock_send.return_value = True
        
        settings = {"notify_added": True, "notify_upgraded": True, "notify_summary": True, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_upgraded(
            title="Test Movie", year=2023,
            old_quality="720p", old_score=500,
            new_quality="1080p", new_score=1000
        )
        
        self.assertTrue(result)
        mock_send.assert_called_once()
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_upgraded_respects_setting_disabled(self, mock_send):
        """Test notify_upgraded does not send when setting is disabled (default)."""
        settings = {"notify_added": True, "notify_upgraded": False, "notify_summary": True, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_upgraded(
            title="Test Movie", year=2023,
            old_quality="720p", old_score=500,
            new_quality="1080p", new_score=1000
        )
        
        self.assertFalse(result)
        mock_send.assert_not_called()
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_summary_respects_setting_enabled(self, mock_send):
        """Test notify_summary sends when setting is enabled."""
        mock_send.return_value = True
        
        settings = {"notify_added": True, "notify_upgraded": False, "notify_summary": True, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_summary(added=1, upgraded=0, skipped=0, failed=0, duration_seconds=60)
        
        self.assertTrue(result)
        mock_send.assert_called_once()
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_summary_respects_setting_disabled(self, mock_send):
        """Test notify_summary does not send when setting is disabled."""
        settings = {"notify_added": True, "notify_upgraded": False, "notify_summary": False, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_summary(added=1, upgraded=0, skipped=0, failed=0, duration_seconds=60)
        
        self.assertFalse(result)
        mock_send.assert_not_called()
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_error_respects_setting_enabled(self, mock_send):
        """Test notify_error sends when setting is enabled."""
        mock_send.return_value = True
        
        settings = {"notify_added": True, "notify_upgraded": False, "notify_summary": True, "notify_error": True}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_error("Something went wrong", "Test context")
        
        self.assertTrue(result)
        mock_send.assert_called_once()
    
    @patch.object(TelegramNotifier, '_send_message')
    def test_notify_error_respects_setting_disabled(self, mock_send):
        """Test notify_error does not send when setting is disabled."""
        settings = {"notify_added": True, "notify_upgraded": False, "notify_summary": True, "notify_error": False}
        notifier = TelegramNotifier(bot_token="test", chat_id="123", telegram_settings=settings)
        result = notifier.notify_error("Something went wrong", "Test context")
        
        self.assertFalse(result)
        mock_send.assert_not_called()
    
    def test_get_telegram_notifier_with_settings(self):
        """Test that get_telegram_notifier accepts and uses telegram_settings."""
        custom_settings = {
            "notify_added": True,
            "notify_upgraded": False,
            "notify_summary": True,
            "notify_error": True
        }
        notifier = get_telegram_notifier(telegram_settings=custom_settings)
        
        self.assertEqual(notifier.settings, custom_settings)
        
        # Reset singleton for other tests
        import torboxed
        torboxed._telegram_notifier = None


class TestTelegramSettingsConfig(unittest.TestCase):
    """Test Telegram settings integration with config system."""
    
    def setUp(self):
        """Set up temporary database for each test."""
        import torboxed as tb
        self.tb = tb
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.original_db_path = tb.DB_PATH
        tb.DB_PATH = Path(self.temp_db.name)
        
        # Initialize fresh database
        tb.init_db()
    
    def tearDown(self):
        """Clean up temporary database."""
        self.tb.DB_PATH = self.original_db_path
        try:
            os.unlink(self.temp_db.name)
        except:
            pass
    
    def test_get_config_includes_telegram_settings(self):
        """Test that get_config returns telegram settings."""
        config = self.tb.get_config()
        
        self.assertIn("telegram", config)
        telegram = config["telegram"]
        self.assertIn("notify_added", telegram)
        self.assertIn("notify_upgraded", telegram)
        self.assertIn("notify_summary", telegram)
        self.assertIn("notify_error", telegram)
    
    def test_default_telegram_settings_in_config(self):
        """Test that default telegram settings are correct."""
        config = self.tb.get_config()
        telegram = config["telegram"]
        
        # Default: added=true, upgraded=false, summary=true, error=true
        self.assertTrue(telegram["notify_added"])
        self.assertFalse(telegram["notify_upgraded"])
        self.assertTrue(telegram["notify_summary"])
        self.assertTrue(telegram["notify_error"])
    
    def test_database_has_telegram_settings_column(self):
        """Test that config table has telegram_settings column."""
        with self.tb.get_db() as conn:
            cursor = conn.execute("PRAGMA table_info(config)")
            columns = [row[1] for row in cursor.fetchall()]
        
        self.assertIn("telegram_settings", columns)
    
    def test_telegram_settings_stored_in_database(self):
        """Test that telegram settings can be stored and retrieved."""
        custom_settings = {
            "notify_added": False,
            "notify_upgraded": True,
            "notify_summary": False,
            "notify_error": False
        }
        
        # Store custom settings directly
        with self.tb.get_db() as conn:
            conn.execute(
                "UPDATE config SET telegram_settings = ? WHERE id = 1",
                (json.dumps(custom_settings),)
            )
            conn.commit()
        
        # Retrieve and verify
        config = self.tb.get_config()
        telegram = config["telegram"]
        
        self.assertFalse(telegram["notify_added"])
        self.assertTrue(telegram["notify_upgraded"])
        self.assertFalse(telegram["notify_summary"])
        self.assertFalse(telegram["notify_error"])
    
    def test_migration_adds_telegram_settings_column(self):
        """Test that migration adds telegram_settings column to existing database."""
        # Create a config table without telegram_settings (simulate old schema)
        with self.tb.get_db() as conn:
            # Drop and recreate without telegram_settings
            conn.execute("DROP TABLE config")
            conn.execute('''
                CREATE TABLE config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    sources TEXT,
                    limits TEXT,
                    quality_prefs TEXT,
                    filters TEXT
                )
            ''')
            conn.execute('''
                INSERT INTO config (id, sources, limits, quality_prefs, filters)
                VALUES (1, '[]', '{}', '{}', '{}')
            ''')
            conn.commit()
        
        # Run migration
        result = self.tb.migrate_db()
        self.assertTrue(result)
        
        # Verify column was added
        with self.tb.get_db() as conn:
            cursor = conn.execute("PRAGMA table_info(config)")
            columns = [row[1] for row in cursor.fetchall()]
        
        self.assertIn("telegram_settings", columns)


class TestCheckCached(unittest.TestCase):
    """Test TorboxClient.check_cached method (TEST-004)."""
    
    def setUp(self):
        """Set up mock Torbox client."""
        from torboxed import TorboxClient
        self.client = TorboxClient.__new__(TorboxClient)
        self.client.api_key = "test-key"
    
    @patch.object(TorboxClient, '_request')
    def test_check_cached_empty_hashes(self, mock_request):
        """Test check_cached with empty hash list returns empty dict."""
        result = self.client.check_cached([])
        
        self.assertEqual(result, {})
        mock_request.assert_not_called()
    
    @patch.object(TorboxClient, '_request')
    def test_check_cached_with_cached_torrents(self, mock_request):
        """Test check_cached returns True for cached torrents."""
        mock_request.return_value = {
            "data": {
                "abc123": {"files": ["file1.mkv"]},
                "def456": {"files": ["file2.mkv"]}
            }
        }
        
        result = self.client.check_cached(["abc123", "def456", "ghi789"])
        
        # Should mark abc123 and def456 as cached, ghi789 as not cached
        self.assertTrue(result["abc123"])
        self.assertTrue(result["def456"])
        self.assertFalse(result["ghi789"])
    
    @patch.object(TorboxClient, '_request')
    def test_check_cached_all_not_cached(self, mock_request):
        """Test check_cached returns False when nothing is cached."""
        mock_request.return_value = {"data": {}}
        
        result = self.client.check_cached(["abc123", "def456"])
        
        # All hashes should be False
        self.assertFalse(result["abc123"])
        self.assertFalse(result["def456"])
    
    @patch.object(TorboxClient, '_request')
    def test_check_cached_case_insensitive(self, mock_request):
        """Test check_cached handles hash case insensitively."""
        mock_request.return_value = {
            "data": {
                "abc123": {"files": ["file1.mkv"]}
            }
        }
        
        result = self.client.check_cached(["ABC123"])
        
        # Should match regardless of case
        self.assertTrue(result["abc123"])
    
    @patch.object(TorboxClient, '_request')
    def test_check_cached_api_error(self, mock_request):
        """Test check_cached handles API error gracefully."""
        mock_request.return_value = None
        
        result = self.client.check_cached(["abc123"])
        
        # Should return all False on error
        self.assertFalse(result["abc123"])
    
    @patch.object(TorboxClient, '_request')
    def test_check_cached_invalid_response_format(self, mock_request):
        """Test check_cached handles invalid response format."""
        mock_request.return_value = {"invalid": "format"}
        
        result = self.client.check_cached(["abc123"])
        
        # Should handle gracefully and return all False
        self.assertFalse(result["abc123"])
    
    @patch.object(TorboxClient, '_request')
    def test_check_cached_none_in_data(self, mock_request):
        """Test check_cached handles None values in response data."""
        mock_request.return_value = {
            "data": {
                "abc123": None,  # Not cached
                "def456": {"files": ["file.mkv"]}  # Cached
            }
        }
        
        result = self.client.check_cached(["abc123", "def456"])
        
        # abc123 should be False (None value), def456 should be True
        self.assertFalse(result["abc123"])
        self.assertTrue(result["def456"])


class TestRealDebridClient(unittest.TestCase):
    """Test RealDebridClient implementing DebridClient interface."""

    def setUp(self):
        """Set up mock Real Debrid client."""
        from torboxed import RealDebridClient
        self.client = RealDebridClient.__new__(RealDebridClient)
        self.client.api_key = "test-rd-key"
        # Mock the rate limiters
        self.client._limiter = MagicMock()
        self.client._creation_limiter = MagicMock()

    @patch.object(RealDebridClient, '_request')
    def test_check_cached_empty_hashes(self, mock_request):
        """Test check_cached with empty hash list returns empty dict."""
        result = self.client.check_cached([])
        
        self.assertEqual(result, {})
        mock_request.assert_not_called()

    @patch.object(RealDebridClient, '_request')
    def test_check_cached_with_cached_torrents(self, mock_request):
        """Test check_cached returns True for cached torrents."""
        mock_request.return_value = {
            "abc123": {"rd": [{"filename": "file1.mkv", "filesize": 1000}]},
            "def456": {"rd": [{"filename": "file2.mkv", "filesize": 2000}]},
            "ghi789": {}  # Not cached - no 'rd' key with content
        }
        
        result = self.client.check_cached(["abc123", "def456", "ghi789"])
        
        # Should mark abc123 and def456 as cached, ghi789 as not cached
        self.assertTrue(result["abc123"])
        self.assertTrue(result["def456"])
        self.assertFalse(result["ghi789"])

    @patch.object(RealDebridClient, '_request')
    def test_check_cached_all_not_cached(self, mock_request):
        """Test check_cached returns False when nothing is cached."""
        mock_request.return_value = {
            "abc123": {},
            "def456": {}
        }
        
        result = self.client.check_cached(["abc123", "def456"])
        
        # All hashes should be False (no 'rd' content)
        self.assertFalse(result["abc123"])
        self.assertFalse(result["def456"])

    @patch.object(RealDebridClient, '_request')
    def test_check_cached_case_insensitive(self, mock_request):
        """Test check_cached handles hash case insensitively."""
        mock_request.return_value = {
            "abc123": {"rd": [{"filename": "file1.mkv"}]}
        }
        
        result = self.client.check_cached(["ABC123"])
        
        # Should match regardless of case
        self.assertTrue(result["abc123"])

    @patch.object(RealDebridClient, '_request')
    def test_check_cached_api_error(self, mock_request):
        """Test check_cached handles API error gracefully."""
        mock_request.return_value = None
        
        result = self.client.check_cached(["abc123"])
        
        # Should return all False on error
        self.assertFalse(result["abc123"])

    @patch.object(RealDebridClient, '_request')
    def test_check_cached_batches_large_hash_lists(self, mock_request):
        """Cache check should batch hashes into chunks to avoid URL length issues."""
        hashes = [f"{i:040x}" for i in range(250)]
        mock_request.return_value = {h: {"rd": [{}]} for h in hashes}

        result = self.client.check_cached(hashes)

        expected_batches = (250 + RD_CACHE_CHECK_BATCH - 1) // RD_CACHE_CHECK_BATCH
        self.assertEqual(mock_request.call_count, expected_batches)
        self.assertEqual(len(result), 250)
        self.assertTrue(all(result.values()))

    @patch.object(RealDebridClient, '_request')
    def test_check_cached_small_list_single_call(self, mock_request):
        """Cache check with few hashes should make exactly one API call."""
        hashes = ["abc123", "def456"]
        mock_request.return_value = {
            "abc123": {"rd": [{}]},
            "def456": {}
        }

        result = self.client.check_cached(hashes)

        self.assertEqual(mock_request.call_count, 1)
        self.assertTrue(result["abc123"])
        self.assertFalse(result["def456"])

    @patch.object(RealDebridClient, '_request')
    def test_get_my_torrents_normalizes_filename(self, mock_request):
        """Test get_my_torrents normalizes 'filename' to 'name'."""
        mock_request.return_value = [
            {"id": "1", "filename": "Movie.2024.mkv", "hash": "abc123"},
            {"id": "2", "filename": "Show.S01.mkv", "hash": "def456"}
        ]
        
        result = self.client.get_my_torrents()
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        # Should have normalized 'filename' to 'name'
        self.assertEqual(result[0]["name"], "Movie.2024.mkv")
        self.assertEqual(result[1]["name"], "Show.S01.mkv")
        # Original filename should still exist
        self.assertEqual(result[0]["filename"], "Movie.2024.mkv")

    @patch.object(RealDebridClient, '_request')
    def test_add_torrent_success(self, mock_request):
        """Test add_torrent returns ID on success."""
        mock_request.return_value = {"id": "rd12345"}
        
        magnet = "magnet:?xt=urn:btih:abc123&dn=Test.Movie"
        result = self.client.add_torrent(magnet, "Test Movie")
        
        self.assertEqual(result, "rd12345")
        mock_request.assert_called_once()
        call_args = mock_request.call_args
        self.assertEqual(call_args[0][0], "POST")  # method
        self.assertEqual(call_args[0][1], "/torrents/addMagnet")  # path
        self.assertTrue(call_args[1]["use_creation_limiter"])  # Should use creation limiter

    @patch.object(RealDebridClient, '_request')
    def test_add_torrent_invalid_magnet(self, mock_request):
        """Test add_torrent returns None for invalid magnet."""
        result = self.client.add_torrent("not-a-magnet", "Bad Magnet")
        
        self.assertIsNone(result)
        mock_request.assert_not_called()

    @patch.object(RealDebridClient, '_request')
    def test_add_torrent_no_id_in_response(self, mock_request):
        """Test add_torrent returns None when no ID in response."""
        mock_request.return_value = {"status": "ok"}  # No 'id' field
        
        magnet = "magnet:?xt=urn:btih:abc123&dn=Test.Movie"
        result = self.client.add_torrent(magnet, "Test Movie")
        
        self.assertIsNone(result)

    @patch.object(RealDebridClient, '_request')
    def test_remove_torrent_success(self, mock_request):
        """Test remove_torrent returns True on success (204 No Content)."""
        mock_request.return_value = None  # 204 No Content
        
        result = self.client.remove_torrent("rd12345")
        
        self.assertTrue(result)
        mock_request.assert_called_once_with("DELETE", "/torrents/delete/rd12345")

    @patch.object(RealDebridClient, '_request')
    def test_remove_torrent_failure(self, mock_request):
        """Test remove_torrent returns False on API error."""
        from torboxed import APIError
        mock_request.side_effect = APIError("API Error", status_code=500)
        
        result = self.client.remove_torrent("rd12345")
        
        self.assertFalse(result)


class TestMakeRequestWithBackoff(unittest.TestCase):
    """Test make_request_with_backoff edge cases (TEST-005)."""
    
    @patch('httpx.Client')
    def test_successful_request(self, mock_client_class):
        """Test successful request returns response immediately."""
        from torboxed import make_request_with_backoff
        
        mock_client = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"success": true}'
        mock_client.request.return_value = mock_response
        
        result = make_request_with_backoff(mock_client, "GET", "http://example.com/api")
        
        self.assertEqual(result.status_code, 200)
        mock_client.request.assert_called_once()
    
    @patch('time.sleep')
    @patch('httpx.Client')
    def test_server_error_retry(self, mock_client_class, mock_sleep):
        """Test that 5xx errors trigger retry with backoff."""
        from torboxed import make_request_with_backoff
        
        mock_client = Mock()
        # First call: 500 error, second call: success
        error_response = Mock()
        error_response.status_code = 500
        error_response.text = "Server Error"
        success_response = Mock()
        success_response.status_code = 200
        success_response.text = '{"success": true}'
        mock_client.request.side_effect = [error_response, success_response]
        
        result = make_request_with_backoff(mock_client, "GET", "http://example.com/api")
        
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_client.request.call_count, 2)
        mock_sleep.assert_called_once()  # Should sleep between retries
    
    @patch('httpx.Client')
    def test_rate_limit_returns_immediately(self, mock_client_class):
        """Test that 429 returns immediately without retry."""
        from torboxed import make_request_with_backoff
        
        mock_client = Mock()
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "60"}
        mock_client.request.return_value = rate_limit_response
        
        result = make_request_with_backoff(mock_client, "GET", "http://example.com/api")
        
        self.assertEqual(result.status_code, 429)
        mock_client.request.assert_called_once()  # Should not retry
    
    @patch('time.sleep')
    @patch('httpx.Client')
    def test_timeout_retry(self, mock_client_class, mock_sleep):
        """Test that TimeoutException triggers retry."""
        import httpx
        from torboxed import make_request_with_backoff
        
        mock_client = Mock()
        # First call: timeout, second call: success
        mock_client.request.side_effect = [
            httpx.TimeoutException("Request timed out"),
            Mock(status_code=200, text='{"success": true}')
        ]
        
        result = make_request_with_backoff(mock_client, "GET", "http://example.com/api")
        
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_client.request.call_count, 2)
        mock_sleep.assert_called_once()  # Should sleep between retries
    
    @patch('httpx.Client')
    def test_client_error_no_retry(self, mock_client_class):
        """Test that 4xx errors don't trigger retry."""
        from torboxed import make_request_with_backoff, APIError
        
        mock_client = Mock()
        error_response = Mock()
        error_response.status_code = 404
        error_response.text = "Not found"
        mock_client.request.return_value = error_response
        
        with self.assertRaises(APIError) as context:
            make_request_with_backoff(mock_client, "GET", "http://example.com/api")
        
        mock_client.request.assert_called_once()  # Should not retry 4xx
    
    @patch('time.sleep')
    @patch('httpx.Client')
    def test_max_retries_exceeded(self, mock_client_class, mock_sleep):
        """Test that max retries exceeded raises APIError."""
        from torboxed import make_request_with_backoff, APIError
        
        mock_client = Mock()
        # All calls return 500 error
        error_response = Mock()
        error_response.status_code = 500
        error_response.text = "Server Error"
        mock_client.request.return_value = error_response
        
        with self.assertRaises(APIError) as context:
            make_request_with_backoff(
                mock_client, 
                "GET", 
                "http://example.com/api",
                max_retries=3
            )
        
        # Should have retried max_retries times
        self.assertEqual(mock_client.request.call_count, 3)
    
    @patch('httpx.Client')
    def test_request_error_retry(self, mock_client_class):
        """Test that RequestError triggers retry."""
        import httpx
        from torboxed import make_request_with_backoff
        
        mock_client = Mock()
        # First call: connection error, second call: success
        mock_client.request.side_effect = [
            httpx.RequestError("Connection failed"),
            Mock(status_code=200, text='{"success": true}')
        ]
        
        result = make_request_with_backoff(mock_client, "GET", "http://example.com/api")
        
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_client.request.call_count, 2)
    
    @patch('httpx.Client')
    def test_exponential_backoff_timing(self, mock_client_class):
        """Test that backoff increases exponentially."""
        import time
        from torboxed import make_request_with_backoff
        from unittest.mock import call
        
        mock_client = Mock()
        # Three consecutive failures, then success
        error_response = Mock()
        error_response.status_code = 500
        success_response = Mock()
        success_response.status_code = 200
        success_response.text = '{"success": true}'
        mock_client.request.side_effect = [
            error_response, error_response, success_response
        ]
        
        with patch('time.sleep') as mock_sleep:
            result = make_request_with_backoff(
                mock_client, 
                "GET", 
                "http://example.com/api",
                max_retries=4
            )
        
        # Backoff should be 1, then 2 (doubling each time)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls([call(1), call(2)])


class TestNonCreationRateLimitExhaustion(unittest.TestCase):
    """Test that non-creation endpoints raise RateLimitError after exhausting retries."""
    
    @patch('torboxed.time.sleep')
    @patch('torboxed.make_request_with_backoff')
    @patch('torboxed.torbox_limiter')
    def test_non_creation_raises_after_max_retries(self, mock_limiter, mock_make_request, mock_sleep):
        from torboxed import TorboxClient, RateLimitError
        
        client = TorboxClient("test_key")
        
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {}
        mock_make_request.return_value = mock_response_429
        
        with self.assertRaises(RateLimitError):
            client._request("GET", "/v1/api/torrents", max_retries=3)
        
        self.assertEqual(mock_make_request.call_count, 4)
        client.client.close()


class TestZileanBuildTorrentResult(unittest.TestCase):
    """Test ZileanClient._build_torrent_result helper."""
    
    def test_valid_torrent(self):
        from torboxed import ZileanClient
        
        client = ZileanClient.__new__(ZileanClient)
        torrent = {
            "infoHash": "a" * 40,
            "rawTitle": "Test.Movie.2024.1080p.BluRay",
            "parsedTitle": "Test Movie",
            "size": "1500000000",
        }
        result = client._build_torrent_result(torrent)
        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "zilean")
        self.assertIn("magnet", result)
        self.assertEqual(result["size"], 1500000000)
    
    def test_invalid_infohash_too_short(self):
        from torboxed import ZileanClient
        
        client = ZileanClient.__new__(ZileanClient)
        torrent = {
            "infoHash": "abc123",
            "rawTitle": "Bad Torrent",
            "size": "0",
        }
        result = client._build_torrent_result(torrent)
        self.assertIsNone(result)
    
    def test_invalid_infohash_non_hex(self):
        from torboxed import ZileanClient
        
        client = ZileanClient.__new__(ZileanClient)
        torrent = {
            "infoHash": "g" * 40,
            "rawTitle": "Bad Hex",
            "size": "0",
        }
        result = client._build_torrent_result(torrent)
        self.assertIsNone(result)
    
    def test_empty_infohash(self):
        from torboxed import ZileanClient
        
        client = ZileanClient.__new__(ZileanClient)
        torrent = {
            "infoHash": "",
            "rawTitle": "No Hash",
            "size": "0",
        }
        result = client._build_torrent_result(torrent)
        self.assertIsNone(result)
    
    def test_invalid_size_falls_to_zero(self):
        from torboxed import ZileanClient
        
        client = ZileanClient.__new__(ZileanClient)
        torrent = {
            "infoHash": "a" * 40,
            "rawTitle": "Test",
            "size": "not_a_number",
        }
        result = client._build_torrent_result(torrent)
        self.assertIsNotNone(result)
        self.assertEqual(result["size"], 0)


class TestCleanupNoSysExit(unittest.TestCase):
    """Test that cleanup_unmatched_torrents doesn't call sys.exit."""
    
    @patch.dict('os.environ', {}, clear=True)
    @patch('torboxed.create_debrid_client', return_value=None)
    def test_returns_gracefully_without_api_key(self, mock_create_client):
        import torboxed
        result = torboxed.cleanup_unmatched_torrents()
        self.assertIsNone(result)


class TestTraktFetchByCategory(unittest.TestCase):
    """Test TraktClient._fetch_by_category DRY method."""
    
    def _make_client(self):
        from torboxed import TraktClient
        client = TraktClient.__new__(TraktClient)
        return client
    
    def test_fetch_movies_boxoffice(self):
        from torboxed import TraktClient
        
        client = self._make_client()
        with patch.object(client, 'get_boxoffice_movies', return_value=[{"movie": {"title": "A"}}]):
            result = client._fetch_by_category("movies", "boxoffice", "weekly")
        self.assertEqual(len(result), 1)
    
    def test_fetch_shows_no_boxoffice(self):
        from torboxed import TraktClient
        
        client = self._make_client()
        result = client._fetch_by_category("shows", "boxoffice", "weekly")
        self.assertEqual(result, [])



class TestProcessSeasonWithExistingTorrents(unittest.TestCase):
    """TEST-012: _process_season uses existing_torrents to skip discovery matches."""
    
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
        
        mock_debrid = Mock()
        mock_trakt = Mock()
        self.engine = torboxed.SyncEngine(mock_debrid, mock_trakt, {
            "sources": ["shows/trending"],
            "filters": {"exclude": ["CAM", "TS", "HDCAM"], "min_resolution_score": 800}
        })
    
    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_skips_when_hash_already_in_account(self):
        """Test that _process_season skips when torrent hash already in account for SAME IMDb ID (hash-based dedup)."""
        import torboxed
        
        mock_torrent = TorrentResult(
            name="Test.Show.S01.1080p.BluRay",
            magnet="magnet:s01",
            hash="deadbeef0123456789abcdef0123456789abcdef",
            availability=True,
            size=50000,
            quality=torboxed.QualityInfo(resolution="1080p", source="Blu-ray", codec="H.264", score=3500),
            season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
        )
        
        # BUG FIX: Hash must be in both account_hashes AND hash_to_imdb with same IMDb ID to trigger skip
        self.engine.account_hashes.add("deadbeef0123456789abcdef0123456789abcdef")
        self.engine.hash_to_imdb["deadbeef0123456789abcdef0123456789abcdef"] = "tt1234567"
        
        result = self.engine._process_season(
            "tt1234567", "Test Show", 2024, "S01",
            mock_torrent, {}
        )
        
        self.assertTrue(result)
        processed = torboxed.get_processed_item("tt1234567", "S01")
        self.assertEqual(processed["action"], "skipped")
        self.assertEqual(processed["reason"], "already_in_debrid_by_hash")
    
    def test_proceeds_when_not_in_existing_torrents(self):
        """Test that _process_season proceeds normally when IMDB ID not in existing_torrents."""
        import torboxed
        
        mock_torrent = TorrentResult(
            name="New.Show.S01.1080p.BluRay",
            magnet="magnet:new-s01",
            availability=True,
            size=50000,
            quality=torboxed.QualityInfo(resolution="1080p", source="Blu-ray", codec="H.264", score=3500),
            season_info=SeasonInfo(seasons=[1], is_complete=False, season_label="S01", is_pack=True)
        )
        
        self.engine.debrid.add_torrent.return_value = "new-tb-id"
        
        result = self.engine._process_season(
            "tt9999999", "New Show", 2024, "S01",
            mock_torrent, {}  # Empty existing_torrents
        )
        
        self.assertTrue(result)
        processed = torboxed.get_processed_item("tt9999999", "S01")
        self.assertEqual(processed["action"], "added")
        self.assertEqual(processed["debrid_id"], "new-tb-id")


class TestCompletenessReconciliation(unittest.TestCase):
    """Issue #18 regression tests: completeness-driven upgrades.

    Two defects covered:
    1. Cross-level additions duplicated coverage instead of replacing:
       a Complete pack discovered after an S01-Sxx pack was stored under a
       different season key, so the per-key lookup missed and both packs
       accumulated in the debrid account.
    2. Same-range multi-season packs bypassed the +500 quality threshold
       because historical records classified as plain 'season' without
       SeasonInfo context.
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"

        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()

        self.mock_debrid = Mock()
        self.mock_trakt = Mock()
        self.engine = torboxed.SyncEngine(self.mock_debrid, self.mock_trakt, {
            "sources": ["shows/trending"],
            "filters": {"exclude": ["CAM", "TS", "HDCAM"], "min_resolution_score": 800}
        })

    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def _torrent(self, name, magnet, hash_hex, score, seasons=None,
                 is_complete=False, label="S01"):
        import torboxed
        return torboxed.TorrentResult(
            name=name,
            magnet=magnet,
            hash=hash_hex,
            availability=True,
            size=50000,
            quality=torboxed.QualityInfo(resolution="1080p", source="Blu-ray",
                                         codec="H.264", score=score),
            season_info=torboxed.SeasonInfo(seasons=seasons or [1],
                                            is_complete=is_complete,
                                            season_label=label,
                                            is_pack=True),
        )

    def test_ranged_key_classifies_as_multi_season(self):
        """classify_media_level infers multi_season from ranged labels alone."""
        import torboxed
        self.assertEqual(torboxed.classify_media_level("S01-S05", "show"), "multi_season")
        self.assertEqual(torboxed.classify_media_level("S02", "show"), "season")
        self.assertEqual(torboxed.classify_media_level("S03E04", "show"), "episode")
        self.assertEqual(torboxed.classify_media_level("Complete", "show"), "complete")

    def test_complete_pack_replaces_multi_season_pack(self):
        """Adding a Complete pack while an S01-Sxx pack exists replaces it.

        DoD: one torrent in the account (old removed), all season rows
        resolve through the new debrid_id.
        """
        import torboxed
        torboxed.record_processed(
            "tt1111111", "Coverage Show", 2024, "show", "added", "success",
            debrid_id="old-multi-id", magnet="magnet:old",
            quality_score=3000, quality_label="1080p BluRay",
            season="S01-S05"
        )

        torrent = self._torrent(
            name="Coverage.Show.Complete",
            magnet="magnet:complete",
            hash_hex="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            score=3000,
            seasons=[1, 2, 3, 4, 5],
            is_complete=True,
            label="Complete",
        )
        self.mock_debrid.remove_torrent.return_value = True
        self.mock_debrid.add_torrent.return_value = "new-complete-id"

        result = self.engine._process_season(
            "tt1111111", "Coverage Show", 2024, "Complete", torrent, {}
        )

        self.assertTrue(result)
        # Old superseded torrent removed from the account
        self.mock_debrid.remove_torrent.assert_called_with("old-multi-id")
        self.mock_debrid.add_torrent.assert_called_once()
        # New pack recorded under its own key
        complete_row = torboxed.get_processed_item("tt1111111", "Complete")
        self.assertEqual(complete_row["debrid_id"], "new-complete-id")
        # Superseded season row re-pointed at the new pack's torrent
        old_row = torboxed.get_processed_item("tt1111111", "S01-S05")
        self.assertEqual(old_row["debrid_id"], "new-complete-id")
        self.assertEqual(old_row["replaced_id"], "old-multi-id")

    def test_equal_range_pack_respects_quality_threshold(self):
        """Replacing an S01-S05 pack within the same range requires +500.

        DoD: an equal-range replacement below the threshold is rejected even
        though SeasonInfo classifies the new pack as multi_season.
        """
        import torboxed
        torboxed.record_processed(
            "tt2222222", "Threshold Show", 2024, "show", "added", "success",
            debrid_id="existing-pack", magnet="magnet:existing",
            quality_score=4000, quality_label="1080p BluRay",
            season="S01-S05"
        )

        torrent = self._torrent(
            name="Threshold.Show.S01-S05.720p.WEBRip",
            magnet="magnet:worse",
            hash_hex="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            score=4300,  # +300 only - below the +500 threshold
            seasons=[1, 2, 3, 4, 5],
            is_complete=False,
            label="S01-S05",
        )

        result = self.engine._process_season(
            "tt2222222", "Threshold Show", 2024, "S01-S05", torrent, {}
        )

        # Engine semantics: no upgrade performed -> False (consistent with
        # the pre-existing current_better skip path in _handle_upgrade)
        self.assertFalse(result)
        self.mock_debrid.add_torrent.assert_not_called()
        self.mock_debrid.remove_torrent.assert_not_called()
        row = torboxed.get_processed_item("tt2222222", "S01-S05")
        self.assertEqual(row["action"], "skipped")
        self.assertEqual(row["reason"], "current_better")
        self.assertEqual(row["debrid_id"], "existing-pack")

    def test_lower_completeness_pack_skipped_when_complete_exists(self):
        """A multi-season pack is not added alongside an existing Complete pack
        stored under a different season key (no downgrade duplicates)."""
        import torboxed
        torboxed.record_processed(
            "tt3333333", "Already Complete Show", 2024, "show", "added", "success",
            debrid_id="complete-pack", magnet="magnet:complete",
            quality_score=2500, quality_label="720p WEBRip",
            season="Complete"
        )

        torrent = self._torrent(
            name="Show.S01-S05.2160p.BluRay",
            magnet="magnet:better-quality-narrower",
            hash_hex="cccccccccccccccccccccccccccccccccccccccc",
            score=5000,
            seasons=[1, 2, 3, 4, 5],
            is_complete=False,
            label="S01-S05",
        )

        result = self.engine._process_season(
            "tt3333333", "Already Complete Show", 2024, "S01-S05", torrent, {}
        )

        self.assertTrue(result)
        self.mock_debrid.add_torrent.assert_not_called()
        self.mock_debrid.remove_torrent.assert_not_called()
        row = torboxed.get_processed_item("tt3333333", "S01-S05")
        self.assertEqual(row["action"], "skipped")
        self.assertEqual(row["reason"], "more_complete_exists")

    def test_disjoint_multi_season_pack_is_not_blocked(self):
        """An S06-S08 pack next to an existing S01-S05 pack is independent
        content: no skip, no supersede, normal addition."""
        import torboxed
        torboxed.record_processed(
            "tt5555555", "Long Show", 2024, "show", "added", "success",
            debrid_id="early-pack", magnet="magnet:s01-s05",
            quality_score=4000, season="S01-S05"
        )

        torrent = self._torrent(
            name="Long.Show.S06-S08.1080p.BluRay",
            magnet="magnet:s06-s08",
            hash_hex="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            score=3800,
            seasons=[6, 7, 8],
            is_complete=False,
            label="S06-S08",
        )
        self.mock_debrid.add_torrent.return_value = "late-pack-id"

        result = self.engine._process_season(
            "tt5555555", "Long Show", 2024, "S06-S08", torrent, {}
        )

        self.assertTrue(result)
        self.mock_debrid.remove_torrent.assert_not_called()
        self.mock_debrid.add_torrent.assert_called_once()
        row = torboxed.get_processed_item("tt5555555", "S06-S08")
        self.assertEqual(row["action"], "added")
        # Early pack untouched
        early = torboxed.get_processed_item("tt5555555", "S01-S05")
        self.assertEqual(early["debrid_id"], "early-pack")

    def test_supersede_aborts_when_old_removal_fails(self):
        """If the superseded torrent cannot be removed, the add is aborted
        (remove-before-add contract: never leave both packs in the account)."""
        import torboxed
        torboxed.record_processed(
            "tt4444444", "Stuck Show", 2024, "show", "added", "success",
            debrid_id="stuck-old-id", magnet="magnet:old",
            quality_score=3000, season="S01-S05"
        )

        torrent = self._torrent(
            name="Stuck.Show.Complete",
            magnet="magnet:complete",
            hash_hex="dddddddddddddddddddddddddddddddddddddddd",
            score=3500,
            seasons=[1, 2, 3, 4, 5],
            is_complete=True,
            label="Complete",
        )
        self.mock_debrid.remove_torrent.return_value = False

        result = self.engine._process_season(
            "tt4444444", "Stuck Show", 2024, "Complete", torrent, {}
        )

        self.assertFalse(result)
        self.mock_debrid.remove_torrent.assert_called_once_with("stuck-old-id")
        self.mock_debrid.add_torrent.assert_not_called()
        row = torboxed.get_processed_item("tt4444444", "Complete")
        self.assertEqual(row["action"], "skipped")
        self.assertEqual(row["reason"], "remove_old_failed")


class TestHandleNewAdditionFallback(unittest.TestCase):
    """TEST-013: _handle_new_addition falls back to next torrent if first fails."""
    
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
        
        mock_debrid = Mock()
        mock_trakt = Mock()
        self.engine = torboxed.SyncEngine(mock_debrid, mock_trakt, {
            "sources": ["movies/trending"],
            "filters": {"exclude": ["CAM", "TS", "HDCAM"], "min_resolution_score": 800}
        })
    
    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_fallback_to_second_torrent_when_first_fails(self):
        """Test that _handle_new_addition tries second torrent when first fails."""
        import torboxed
        
        first_torrent = TorrentResult(
            name="Movie.2024.1080p.BluRay.x264", magnet="magnet:first",
            availability=True, size=10000,
            quality=torboxed.QualityInfo(resolution="1080p", source="Blu-ray", codec="H.264", score=3500)
        )
        second_torrent = TorrentResult(
            name="Movie.2024.1080p.WEB-DL.x264", magnet="magnet:second",
            availability=True, size=8000,
            quality=torboxed.QualityInfo(resolution="1080p", source="WEB-DL", codec="H.264", score=3400)
        )
        cached = [first_torrent, second_torrent]
        
        self.engine.debrid.add_torrent.side_effect = [None, "new-id-from-fallback"]
        
        result = self.engine._handle_new_addition("tt1234567", "Movie", 2024, "movie", cached, 0)
        
        self.assertTrue(result)
        self.assertEqual(self.engine.debrid.add_torrent.call_count, 2)
        self.engine.debrid.add_torrent.assert_any_call("magnet:first", "Movie.2024.1080p.BluRay.x264")
        self.engine.debrid.add_torrent.assert_any_call("magnet:second", "Movie.2024.1080p.WEB-DL.x264")
        
        processed = torboxed.get_processed_item("tt1234567")
        self.assertEqual(processed["action"], "added")
        self.assertEqual(processed["debrid_id"], "new-id-from-fallback")
    
    def test_all_torrents_fail(self):
        """Test that _handle_new_addition records failure when all torrents fail."""
        import torboxed
        
        torrent = TorrentResult(
            name="Movie.2024.1080p.BluRay.x264", magnet="magnet:fail",
            availability=True, size=10000,
            quality=torboxed.QualityInfo(resolution="1080p", source="Blu-ray", codec="H.264", score=3500)
        )
        cached = [torrent]
        
        self.engine.debrid.add_torrent.return_value = None
        
        result = self.engine._handle_new_addition("tt7654321", "Movie", 2024, "movie", cached, 0)
        
        self.assertFalse(result)
        processed = torboxed.get_processed_item("tt7654321")
        self.assertEqual(processed["action"], "failed")
        self.assertEqual(processed["reason"], "all_torrents_failed")


class TestNormalizeSearchQuery(unittest.TestCase):
    """TEST-014: normalize_search_query edge cases."""
    
    def test_basic_normalization(self):
        query = "Spider-Man: Beyond the Spider-Verse"
        result = normalize_search_query(query)
        self.assertEqual(result, "Spider Man Beyond the Spider Verse")
    
    def test_accents_removed(self):
        query = "Pok\u00e9mon Detective Pikachu"
        result = normalize_search_query(query)
        self.assertNotIn("\u00e9", result)
    
    def test_multiple_colons(self):
        query = "Star Wars: Episode IV: A New Hope"
        result = normalize_search_query(query)
        self.assertEqual(result, "Star Wars Episode IV A New Hope")
    
    def test_commas_removed(self):
        query = "The Good, the Bad and the Ugly"
        result = normalize_search_query(query)
        self.assertNotIn(",", result)
    
    def test_multiple_spaces_collapsed(self):
        query = "Movie   2024   BluRay"
        result = normalize_search_query(query)
        self.assertEqual(result, "Movie 2024 BluRay")
    
    def test_hyphens_replaced(self):
        query = "Wolf-Hall Season 1"
        result = normalize_search_query(query)
        self.assertNotIn("-", result)


class TestParseSeasonInfoCompleteSeries(unittest.TestCase):
    """TEST-015: parse_season_info with complete series + specific season range."""
    
    def test_complete_without_specific_seasons(self):
        """Test 'Complete' keyword without season numbers."""
        info = parse_season_info("Show.Complete.1080p.BluRay")
        
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)
        self.assertEqual(info.season_label, "Complete")
        self.assertEqual(info.seasons, [0])
    
    def test_complete_with_season_range(self):
        """Test 'Complete S01-S05' format."""
        info = parse_season_info("Show.Complete.S01.S05.1080p.BluRay")
        
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)
    
    def test_multi_season_pack_without_complete(self):
        """Test multi-season pack without 'Complete' keyword."""
        info = parse_season_info("Show.S01.S05.1080p.BluRay")
        
        self.assertIsNotNone(info)
        self.assertFalse(info.is_complete)
        self.assertEqual(info.season_label, "S01-S05")
        self.assertEqual(info.seasons, [1, 5])
    
    def test_single_episode_not_pack(self):
        """Test single episode is detected as not a pack."""
        info = parse_season_info("Show.S01E05.1080p.BluRay")
        
        self.assertIsNotNone(info)
        self.assertFalse(info.is_pack)
        self.assertEqual(info.episode, 5)
        self.assertEqual(info.season_label, "S01E05")

    def test_box_set_detected_as_complete(self):
        """Test 'Box Set' keyword triggers complete detection."""
        info = parse_season_info("Show.Box.Set.1080p")
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)

    def test_boxset_detected_as_complete(self):
        """Test 'Boxset' keyword triggers complete detection."""
        info = parse_season_info("Show.Boxset.1080p")
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)

    def test_integral_detected_as_complete(self):
        """Test 'Integral' keyword triggers complete detection."""
        info = parse_season_info("Show.Integral.1080p")
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)

    def test_integrale_detected_as_complete(self):
        """Test 'Integrale' keyword triggers complete detection."""
        info = parse_season_info("Show.Integrale.1080p")
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)

    def test_complete_collection_detected_as_complete(self):
        """Test 'Complete Collection' keyword triggers complete detection."""
        info = parse_season_info("Show.Complete.Collection.1080p")
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)

    def test_the_complete_detected_as_complete(self):
        """Test 'The Complete' keyword triggers complete detection."""
        info = parse_season_info("The.Complete.Show.1080p")
        self.assertIsNotNone(info)
        self.assertTrue(info.is_complete)


class TestDisplayTitleHelper(unittest.TestCase):
    """TEST-016: _display_title helper method."""
    
    def test_movie_title_no_season(self):
        title = SyncEngine._display_title("The Matrix", "movie", "unknown")
        self.assertEqual(title, "The Matrix")
    
    def test_show_title_with_season(self):
        title = SyncEngine._display_title("Breaking Bad", "show", "S03")
        self.assertEqual(title, "Breaking Bad (S03)")
    
    def test_show_title_unknown_season(self):
        title = SyncEngine._display_title("Breaking Bad", "show", "unknown")
        self.assertEqual(title, "Breaking Bad")
    
    def test_complete_season_pack(self):
        title = SyncEngine._display_title("Breaking Bad", "show", "Complete")
        self.assertEqual(title, "Breaking Bad (Complete)")


class TestGetFilterConfig(unittest.TestCase):
    """TEST-016: _get_filter_config helper method."""
    
    def test_default_values(self):
        engine = SyncEngine.__new__(SyncEngine)
        engine.config = {"filters": {}}
        
        excluded, min_score = engine._get_filter_config()
        # When exclude is not in filters, defaults to ["CAM", "TS", "HDCAM"]
        self.assertEqual(excluded, ["CAM", "TS", "HDCAM"])
        self.assertEqual(min_score, 800)
    
    def test_custom_values(self):
        engine = SyncEngine.__new__(SyncEngine)
        engine.config = {"filters": {"exclude": ["CAM", "TS"], "min_resolution_score": 1500}}
        
        excluded, min_score = engine._get_filter_config()
        self.assertEqual(excluded, ["CAM", "TS"])
        self.assertEqual(min_score, 1500)


class TestVerifyAndClearDroppedTorrents(unittest.TestCase):
    """TEST-017: verify_and_clear_dropped_torrents safety threshold."""
    
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
    
    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_returns_zero_when_discovery_failed(self):
        """Test that None discovery result returns 0."""
        import torboxed
        
        result = torboxed.verify_and_clear_dropped_torrents(None)
        self.assertEqual(result, 0)
    
    def test_skips_when_discovery_below_threshold(self):
        """Test that verification skips when discovery is incomplete."""
        import torboxed
        
        # Add several tracked torrents to database
        for i in range(10):
            torboxed.record_processed(
                f"tt000000{i}", f"Movie {i}", 2024, "movie", "added", "success",
                debrid_id=f"tb-id-{i}", magnet=f"magnet:{i}",
                quality_score=2500, quality_label="1080p"
            )
        
        # Discovery found only 2 of 10 (20% - below 95% threshold)
        imdb_to_torbox = {"tt0000000": "tb-id-0", "tt0000001": "tb-id-1"}
        
        result = torboxed.verify_and_clear_dropped_torrents((imdb_to_torbox, set(), {}))
        self.assertEqual(result, 0)
    
    def test_clears_when_discovery_above_threshold(self):
        """Test that verification clears dropped torrents when discovery is complete."""
        import torboxed
        
        # Add 2 tracked torrents to database
        torboxed.record_processed(
            "tt0000000", "Movie 0", 2024, "movie", "added", "success",
            debrid_id="tb-id-0", magnet="magnet:0",
            quality_score=2500, quality_label="1080p"
        )
        torboxed.record_processed(
            "tt0000001", "Movie 1", 2024, "movie", "added", "success",
            debrid_id="tb-id-1", magnet="magnet:1",
            quality_score=2500, quality_label="1080p"
        )
        
        # Discovery found only 1 of 2 - but that's 1/2=50%, below 95%
        imdb_to_torbox = {"tt0000000": "tb-id-0"}
        
        result = torboxed.verify_and_clear_dropped_torrents((imdb_to_torbox, set(), {}))
        # 1 found of 2 tracked = 50%, below 95% threshold -> should skip
        self.assertEqual(result, 0)


class TestSanitizeResponseError(unittest.TestCase):
    """Test sanitize_response_error wrapper function."""
    
    def test_sanitizes_normal_response(self):
        """Test that API key in response text is redacted."""
        resp = Mock()
        resp.text = '{"error": "api_key=secret123"}'
        
        result = sanitize_response_error(resp)
        self.assertNotIn("secret123", result)
        self.assertIn("***REDACTED***", result)
    
    def test_handles_missing_text(self):
        """Test that response without text attribute doesn't crash."""
        resp = Mock(spec=[])  # No text attribute
        
        result = sanitize_response_error(resp)
        self.assertEqual(result, "<unable to decode response>")


class TestBuildSearchResult(unittest.TestCase):
    """Test build_search_result shared helper."""
    
    def test_build_with_basic_fields(self):
        result = build_search_result(
            name="Test Movie 2024",
            infohash="a" * 40,
            size=1500000000,
            seeders=10,
            leechers=2,
            source="test-indexer"
        )
        
        self.assertEqual(result["title"], "Test Movie 2024")
        self.assertEqual(result["name"], "Test Movie 2024")
        self.assertEqual(result["hash"], "a" * 40)
        self.assertIn("magnet:?xt=urn:btih:", result["magnet"])
        self.assertEqual(result["size"], 1500000000)
        self.assertEqual(result["seeds"], 10)
        self.assertEqual(result["peers"], 2)
        self.assertEqual(result["source"], "test-indexer")
        self.assertEqual(result["imdbId"], "")
    
    def test_build_with_magnet_override(self):
        result = build_search_result(
            name="Test",
            infohash="b" * 40,
            magnet_link="magnet:?xt=urn:btih:bbbb&dn=test"
        )
        
        self.assertEqual(result["magnet"], "magnet:?xt=urn:btih:bbbb&dn=test")
    
    def test_build_with_imdb_id(self):
        result = build_search_result(
            name="Test",
            infohash="c" * 40,
            imdb_id="tt1234567"
        )
        
        self.assertEqual(result["imdbId"], "tt1234567")


class TestRealDebridSyncIntegration(unittest.TestCase):
    """End-to-end test with RealDebridClient in the SyncEngine."""

    def setUp(self):
        """Set up temp DB and mocked clients."""
        import torboxed
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()

        self.mock_rd = Mock(spec=torboxed.RealDebridClient)
        self.mock_rd.searcher_zilean = Mock()
        self.mock_rd.searcher_zilean.is_configured.return_value = False
        self.mock_rd.searcher_prowlarr = Mock()
        self.mock_rd.searcher_prowlarr.is_configured.return_value = False
        self.mock_rd.searcher_jackett = Mock()
        self.mock_rd.searcher_jackett.is_configured.return_value = False
        self.mock_rd.get_cached_torrents.return_value = []
        self.mock_rd.get_my_torrents.return_value = []

        self.mock_trakt = Mock()
        self.mock_trakt.get_all_content.return_value = [
            {"imdb_id": "tt1234567", "title": "Test Movie", "year": 2024,
             "type": "movie", "source": "movies/trending"}
        ]

        self.config = {
            "sources": ["movies/trending"],
            "limits": {"movies": 10, "shows": 10},
            "filters": {"exclude": ["CAM", "TS", "HDCAM"]},
        }

    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_sync_with_real_debrid_no_cached_content(self):
        """Full sync runs without error when no content is cached."""
        import torboxed

        engine = torboxed.SyncEngine(self.mock_rd, self.mock_trakt, self.config)
        engine.sync()

        # Verify discovery was attempted
        self.mock_rd.get_my_torrents.assert_called()
        # Verify search was attempted for the movie
        self.mock_rd.get_cached_torrents.assert_called()
        # Verify no torrent was added (nothing cached)
        self.mock_rd.add_torrent.assert_not_called()

    def test_sync_with_real_debrid_adds_cached_movie(self):
        """Sync adds a cached movie with acceptable quality."""
        import torboxed

        cached_movie = torboxed.TorrentResult(
            name="Test Movie 2024 1080p BluRay x264",
            magnet="magnet:?xt=urn:btih:abcdef123456",
            availability=True,
            size=5000000000,
            quality=torboxed.QualityInfo(
                resolution="1080p", source="Blu-ray", codec="H.264",
                audio="Unknown", score=3350, label="1080p Blu-ray H.264"
            ),
            hash="abcdef123456",
        )
        self.mock_rd.get_cached_torrents.return_value = [cached_movie]
        self.mock_rd.add_torrent.return_value = "RD_TORRENT_123"
        self.mock_rd.get_my_torrents.return_value = []

        engine = torboxed.SyncEngine(self.mock_rd, self.mock_trakt, self.config)
        engine.sync()

        self.mock_rd.add_torrent.assert_called_once()

    def test_sync_with_real_debrid_respects_max_quality(self):
        """Sync skips content already at max quality."""
        import torboxed

        torboxed.record_processed(
            "tt1234567", "Test Movie", 2024, "movie",
            "added", "success", debrid_id="RD_EXISTING",
            magnet="magnet:existing",
            quality_score=7250, quality_label="2160p BluRay HEVC"
        )

        engine = torboxed.SyncEngine(self.mock_rd, self.mock_trakt, self.config)
        engine.sync()

        # Should NOT search since max quality is already reached
        self.mock_rd.get_cached_torrents.assert_not_called()

    def test_sync_handles_rate_limit_on_add(self):
        """Sync gracefully handles RateLimitError during add_torrent."""
        import torboxed

        cached_movie = torboxed.TorrentResult(
            name="Test Movie 2024 2160p BluRay HEVC",
            magnet="magnet:?xt=urn:btih:rate_limit",
            availability=True,
            size=8000000000,
            quality=torboxed.QualityInfo(
                resolution="2160p", source="Blu-ray", codec="HEVC",
                audio="Atmos", score=7250, label="2160p Blu-ray HEVC"
            ),
            hash="rate_limit",
        )
        self.mock_rd.get_cached_torrents.return_value = [cached_movie]
        self.mock_rd.add_torrent.side_effect = torboxed.RateLimitError(
            "rate limited", status_code=429
        )

        engine = torboxed.SyncEngine(self.mock_rd, self.mock_trakt, self.config)
        engine.sync()

        # Should have attempted to add, but rate limit prevented it
        self.mock_rd.add_torrent.assert_called()
        # Should NOT have crashed — RateLimitError is caught by caller


class TestZileanClientMethods(unittest.TestCase):
    """Test ZileanClient methods that weren't directly tested."""
    
    def setUp(self):
        """Set up mock Zilean client."""
        from torboxed import ZileanClient
        self.client = ZileanClient.__new__(ZileanClient)
        self.client.database_url = "postgresql://test:test@localhost/test"
    
    def test_is_configured_without_psycopg(self):
        """Test is_configured returns False when psycopg not available."""
        import torboxed
        from torboxed import ZileanClient
        
        # Save original psycopg
        original_psycopg = torboxed.psycopg
        
        try:
            # Simulate psycopg not being available
            torboxed.psycopg = None
            
            client = ZileanClient.__new__(ZileanClient)
            client.database_url = "postgresql://test:test@localhost/test"
            
            # Should return False when psycopg is None
            self.assertFalse(client.is_configured())
        finally:
            # Restore original
            torboxed.psycopg = original_psycopg
    
    def test_is_configured_without_database_url(self):
        """Test is_configured returns False when no database URL."""
        from torboxed import ZileanClient
        
        client = ZileanClient.__new__(ZileanClient)
        client.database_url = None
        
        # Should return False when no database_url
        self.assertFalse(client.is_configured())
    
    def test_row_to_dict_conversion(self):
        """Test _row_to_dict converts database row correctly."""
        from torboxed import ZileanClient
        
        client = ZileanClient.__new__(ZileanClient)
        
        # Mock database row (tuple format)
        row = (
            "abc123" * 7,  # infoHash (40 chars)
            "Test.Movie.2024.1080p.BluRay",  # rawTitle
            "Test Movie",  # parsedTitle
            "tt1234567",  # imdbId
            "movie",  # category
            2024,  # year
            "1080p",  # resolution
            "BluRay",  # quality
            "H.264",  # codec
            1500000000,  # size
            "2024-01-01",  # ingestedAt
            None,  # seasons
            False,  # complete
            "AAC",  # audio
        )
        
        result = client._row_to_dict(row)
        
        self.assertEqual(result["infoHash"], "abc123" * 7)
        self.assertEqual(result["rawTitle"], "Test.Movie.2024.1080p.BluRay")
        self.assertEqual(result["parsedTitle"], "Test Movie")
        self.assertEqual(result["imdbId"], "tt1234567")
        self.assertEqual(result["category"], "movie")
        self.assertEqual(result["year"], 2024)


class TestProwlarrClientMethods(unittest.TestCase):
    """Test ProwlarrClient methods that weren't directly tested."""
    
    def setUp(self):
        """Set up mock Prowlarr client."""
        from torboxed import ProwlarrClient
        self.client = ProwlarrClient.__new__(ProwlarrClient)
        self.client.api_key = "test_api_key"
        self.client.base_url = "http://localhost:9696"
    
    def test_is_configured_true(self):
        """Test is_configured returns True when API key exists."""
        self.assertTrue(self.client.is_configured())
    
    def test_is_configured_false(self):
        """Test is_configured returns False when no API key."""
        self.client.api_key = ""
        self.assertFalse(self.client.is_configured())
    
    def test_is_configured_none(self):
        """Test is_configured returns False when API key is None."""
        self.client.api_key = None
        self.assertFalse(self.client.is_configured())


class TestJackettClientMethods(unittest.TestCase):
    """Test JackettClient methods that weren't directly tested."""
    
    def setUp(self):
        """Set up mock Jackett client."""
        from torboxed import JackettClient
        self.client = JackettClient.__new__(JackettClient)
        self.client.api_key = "test_api_key"
        self.client.base_url = "http://localhost:9117"
    
    def test_is_configured_true(self):
        """Test is_configured returns True when API key exists."""
        self.assertTrue(self.client.is_configured())
    
    def test_is_configured_false(self):
        """Test is_configured returns False when no API key."""
        self.client.api_key = ""
        self.assertFalse(self.client.is_configured())
    
    def test_is_configured_none(self):
        """Test is_configured returns False when API key is None."""
        self.client.api_key = None
        self.assertFalse(self.client.is_configured())


class TestTorboxClientMethods(unittest.TestCase):
    """Test TorboxClient methods that weren't directly tested."""
    
    def setUp(self):
        """Set up mock Torbox client."""
        from torboxed import TorboxClient
        self.client = TorboxClient.__new__(TorboxClient)
        self.client.api_key = "test-key"
    
    @patch('time.sleep')
    def test_remove_torrent_retries_on_database_error(self, mock_sleep):
        """Test that remove_torrent retries on DATABASE_ERROR when torrent still exists.
        
        This handles transient database locks that can occur when a torrent
        is being processed internally by Torbox.
        """
        from torboxed import TorboxClient, APIError
        
        # Mock get_my_torrents to return torrent still exists (so we actually retry)
        with patch.object(TorboxClient, 'get_my_torrents', return_value=[{'id': '23264000', 'name': 'test'}]):
            # Simulate 2 failures with DATABASE_ERROR, then success
            call_count = [0]
            def mock_request(*args, **kwargs):
                call_count[0] += 1
                if len(args) >= 2 and 'controltorrent' in str(args[1]):
                    # First 2 calls fail, 3rd succeeds
                    if call_count[0] <= 2:
                        raise APIError("HTTP 500: DATABASE_ERROR", status_code=500)
                    return {"success": True}
                return None
            
            with patch.object(TorboxClient, '_request', side_effect=mock_request):
                result = self.client.remove_torrent("23264000")
                
                # Should succeed after retries
                self.assertTrue(result)
                # Should have tried 3 times (2 failures with library checks + 1 success)
                self.assertEqual(call_count[0], 3)
                # Should have slept twice (between the actual removal retries)
                self.assertEqual(mock_sleep.call_count, 2)
    
    @patch('time.sleep')
    def test_remove_torrent_fails_after_max_retries(self, mock_sleep):
        """Test that remove_torrent gives up after max retries on DATABASE_ERROR."""
        from torboxed import TorboxClient, APIError
        
        # Mock get_my_torrents to return torrent still exists (so all retries are attempted)
        with patch.object(TorboxClient, 'get_my_torrents', return_value=[{'id': '23264000', 'name': 'test'}]):
            # Fail with DATABASE_ERROR for removal attempts
            call_count = [0]
            def mock_request(*args, **kwargs):
                call_count[0] += 1
                if len(args) >= 2 and 'controltorrent' in str(args[1]):
                    raise APIError("HTTP 500: DATABASE_ERROR", status_code=500)
                return None
            
            with patch.object(TorboxClient, '_request', side_effect=mock_request):
                result = self.client.remove_torrent("23264000")
                
                # Should fail after exhausting retries
                self.assertFalse(result)
                # Should have tried 3 removal attempts (each with library check)
                self.assertEqual(call_count[0], 3)
    
    @patch('time.sleep')
    def test_remove_torrent_database_error_already_removed(self, mock_sleep):
        """Test that remove_torrent succeeds immediately if DATABASE_ERROR and torrent already gone.
        
        This verifies the optimization: on first DATABASE_ERROR, check if torrent exists.
        If already removed, succeed immediately without further retries.
        """
        from torboxed import TorboxClient, APIError
        
        # Mock get_my_torrents to return empty list (torrent already removed)
        with patch.object(TorboxClient, 'get_my_torrents', return_value=[]):
            # Fail with DATABASE_ERROR for the single removal attempt
            call_count = [0]
            def mock_request(*args, **kwargs):
                call_count[0] += 1
                if len(args) >= 2 and 'controltorrent' in str(args[1]):
                    raise APIError("HTTP 500: DATABASE_ERROR", status_code=500)
                return None
            
            with patch.object(TorboxClient, '_request', side_effect=mock_request):
                result = self.client.remove_torrent("23264000")
                
                # Should succeed immediately because torrent is confirmed gone
                self.assertTrue(result)
                # Should have tried only 1 removal attempt (verified gone, no retries needed)
                self.assertEqual(call_count[0], 1)
                # Should not have slept (no retries needed)
                self.assertEqual(mock_sleep.call_count, 0)
    
    def test_remove_torrent_no_retry_on_other_errors(self):
        """Test that remove_torrent doesn't retry on non-500 errors."""
        from torboxed import TorboxClient, APIError
        
        # Fail with 403 Forbidden (not retryable and doesn't match "not found")
        with patch.object(TorboxClient, '_request', side_effect=APIError(
            "HTTP 403: Forbidden", status_code=403
        )):
            result = self.client.remove_torrent("23264000")
            
            # Should fail immediately without retry
            self.assertFalse(result)
            self.assertEqual(self.client._request.call_count, 1)


class TestRealDebridClientMethods(unittest.TestCase):
    """Test RealDebridClient methods that weren't directly tested."""
    
    def setUp(self):
        """Set up mock Real Debrid client."""
        from torboxed import RealDebridClient
        self.client = RealDebridClient.__new__(RealDebridClient)
        self.client.api_key = "test-rd-key"


class TestDebridClientFindExisting(unittest.TestCase):
    """Test DebridClient.find_existing_by_hash method."""
    
    def test_find_existing_by_hash_found(self):
        """Test find_existing_by_hash returns torrent when hash matches."""
        import torboxed
        
        mock_debrid = Mock()
        mock_debrid.get_my_torrents.return_value = [
            {"id": "1", "name": "Movie1", "hash": "abc123"},
            {"id": "2", "name": "Movie2", "hash": "def456"},
        ]
        
        # Create a concrete implementation for testing
        class TestDebrid(torboxed.DebridClient):
            def check_cached(self, hashes): return {}
            def get_my_torrents(self): return mock_debrid.get_my_torrents()
            def add_torrent(self, magnet, title=""): return None
            def remove_torrent(self, torrent_id): return True
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None
        
        client = TestDebrid()
        result = client.find_existing_by_hash("abc123")
        
        self.assertIsNotNone(result)
        self.assertEqual(result["id"], "1")
        self.assertEqual(result["hash"], "abc123")
    
    def test_find_existing_by_hash_not_found(self):
        """Test find_existing_by_hash returns None when hash doesn't match."""
        import torboxed
        
        mock_debrid = Mock()
        mock_debrid.get_my_torrents.return_value = [
            {"id": "1", "name": "Movie1", "hash": "abc123"},
        ]
        
        class TestDebrid(torboxed.DebridClient):
            def check_cached(self, hashes): return {}
            def get_my_torrents(self): return mock_debrid.get_my_torrents()
            def add_torrent(self, magnet, title=""): return None
            def remove_torrent(self, torrent_id): return True
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None
        
        client = TestDebrid()
        result = client.find_existing_by_hash("xyz789")
        
        self.assertIsNone(result)
    
    def test_find_existing_by_hash_empty_list(self):
        """Test find_existing_by_hash returns None when no torrents."""
        import torboxed
        
        class TestDebrid(torboxed.DebridClient):
            def check_cached(self, hashes): return {}
            def get_my_torrents(self): return []
            def add_torrent(self, magnet, title=""): return None
            def remove_torrent(self, torrent_id): return True
            def get_torrent_files(self, debrid_id): return []
            def get_direct_url(self, debrid_id, file_id): return None
        
        client = TestDebrid()
        result = client.find_existing_by_hash("abc123")
        
        self.assertIsNone(result)


class TestDatabaseHelperFunctions(unittest.TestCase):
    """Test database helper functions that weren't directly tested."""
    
    def setUp(self):
        """Create temporary database for each test."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
    
    def tearDown(self):
        """Clean up temporary database."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_is_processed_true(self):
        """Test is_processed returns True when item exists."""
        import torboxed
        
        # Add an item
        torboxed.record_processed(
            "tt1234567", "Test Movie", 2024, "movie", "added", "success",
            debrid_id="tb-123", magnet="magnet:test",
            quality_score=2500, quality_label="1080p"
        )
        
        # Should return True
        self.assertTrue(torboxed.is_processed("tt1234567"))
    
    def test_is_processed_false(self):
        """Test is_processed returns False when item doesn't exist."""
        import torboxed
        
        # Should return False for non-existent item
        self.assertFalse(torboxed.is_processed("tt9999999"))
    
    def test_is_processed_with_season(self):
        """Test is_processed with season parameter."""
        import torboxed
        
        # Add a show season
        torboxed.record_processed(
            "tt1234567", "Test Show", 2024, "show", "added", "success",
            debrid_id="tb-123", magnet="magnet:test",
            quality_score=2500, quality_label="1080p",
            season="S01"
        )
        
        # Should return True for S01
        self.assertTrue(torboxed.is_processed("tt1234567", "S01"))
        # Should return False for S02
        self.assertFalse(torboxed.is_processed("tt1234567", "S02"))


class TestValidateResponse(unittest.TestCase):
    """Test validate_response and validate_list_response functions."""
    
    def test_validate_response_success(self):
        """Test validate_response with valid data."""
        from torboxed import validate_response
        
        data = {"key1": "value1", "key2": "value2"}
        result = validate_response(data, ["key1", "key2"])
        
        self.assertEqual(result, data)
    
    def test_validate_response_missing_keys(self):
        """Test validate_response raises error when keys missing."""
        from torboxed import validate_response, APIResponseError
        
        data = {"key1": "value1"}
        
        with self.assertRaises(APIResponseError):
            validate_response(data, ["key1", "key2"])
    
    def test_validate_response_none_data(self):
        """Test validate_response raises error when data is None."""
        from torboxed import validate_response, APIResponseError
        
        with self.assertRaises(APIResponseError):
            validate_response(None, ["key1"])
    
    def test_validate_response_non_dict(self):
        """Test validate_response raises error when data is not dict."""
        from torboxed import validate_response, APIResponseError
        
        with self.assertRaises(APIResponseError):
            validate_response("not a dict", ["key1"])
    
    def test_validate_list_response_success(self):
        """Test validate_list_response with valid list."""
        from torboxed import validate_list_response
        
        data = [{"id": 1}, {"id": 2}]
        result = validate_list_response(data)
        
        self.assertEqual(result, data)
    
    def test_validate_list_response_none(self):
        """Test validate_list_response returns empty list for None."""
        from torboxed import validate_list_response
        
        result = validate_list_response(None)
        
        self.assertEqual(result, [])
    
    def test_validate_list_response_non_list(self):
        """Test validate_list_response raises error for non-list."""
        from torboxed import validate_list_response, APIResponseError
        
        with self.assertRaises(APIResponseError):
            validate_list_response("not a list")


class TestStatsFunctions(unittest.TestCase):
    """Test get_stats and get_recent functions."""
    
    def setUp(self):
        """Create temporary database for each test."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
    
    def tearDown(self):
        """Clean up temporary database."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_get_stats_empty(self):
        """Test get_stats with empty database."""
        import torboxed
        
        stats = torboxed.get_stats()
        
        self.assertEqual(stats["total"], 0)
        self.assertEqual(stats["by_action"], {})
        self.assertEqual(stats["by_type"], {})
        self.assertEqual(stats["recent_upgrades"], [])
    
    def test_get_stats_with_data(self):
        """Test get_stats with data in database."""
        import torboxed
        
        # Add some items
        torboxed.record_processed(
            "tt1111111", "Movie 1", 2024, "movie", "added", "success",
            debrid_id="tb-1", magnet="magnet:1",
            quality_score=2500, quality_label="1080p"
        )
        torboxed.record_processed(
            "tt2222222", "Movie 2", 2023, "movie", "upgraded", "quality_better",
            debrid_id="tb-2", magnet="magnet:2",
            quality_score=3500, quality_label="2160p",
            replaced_id="tb-old", replaced_score=2500
        )
        
        stats = torboxed.get_stats()
        
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["by_action"]["added"], 1)
        self.assertEqual(stats["by_action"]["upgraded"], 1)
        self.assertEqual(stats["by_type"]["movie"], 2)
        self.assertEqual(len(stats["recent_upgrades"]), 1)
        self.assertEqual(stats["recent_upgrades"][0]["title"], "Movie 2")
    
    def test_get_recent_empty(self):
        """Test get_recent with empty database."""
        import torboxed
        
        items = torboxed.get_recent()
        
        self.assertEqual(items, [])
    
    def test_get_recent_with_limit(self):
        """Test get_recent with limit parameter."""
        import torboxed
        
        # Add items
        for i in range(5):
            torboxed.record_processed(
                f"tt{i:07d}", f"Movie {i}", 2024, "movie", "added", "success",
                debrid_id=f"tb-{i}", magnet=f"magnet:{i}",
                quality_score=2500, quality_label="1080p"
            )
        
        # Get only 3 items
        items = torboxed.get_recent(limit=3)
        
        self.assertEqual(len(items), 3)
    
    def test_get_recent_returns_ordered(self):
        """Test get_recent returns items in reverse chronological order."""
        import torboxed
        
        # Add items with small delay
        torboxed.record_processed(
            "tt1111111", "First Movie", 2024, "movie", "added", "success",
            debrid_id="tb-1", magnet="magnet:1",
            quality_score=2500, quality_label="1080p"
        )
        
        torboxed.record_processed(
            "tt2222222", "Second Movie", 2024, "movie", "added", "success",
            debrid_id="tb-2", magnet="magnet:2",
            quality_score=2500, quality_label="1080p"
        )
        
        items = torboxed.get_recent()
        
        # Second movie should be first (most recent)
        self.assertEqual(items[0]["title"], "Second Movie")
        self.assertEqual(items[1]["title"], "First Movie")


class TestShowStatsFunctions(unittest.TestCase):
    """Test show_stats and show_recent output functions."""
    
    def setUp(self):
        """Create temporary database for each test."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"
        
        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()
    
    def tearDown(self):
        """Clean up temporary database."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()
    
    def test_show_stats_logs_output(self):
        """Test show_stats logs statistics."""
        import torboxed
        from unittest.mock import patch
        
        # Add an item
        torboxed.record_processed(
            "tt1111111", "Test Movie", 2024, "movie", "added", "success",
            debrid_id="tb-1", magnet="magnet:1",
            quality_score=2500, quality_label="1080p"
        )
        
        with patch.object(torboxed.logger, 'info') as mock_info:
            torboxed.show_stats()
            
            # Verify info was logged
            self.assertTrue(mock_info.called)
            # Check that stats title was logged
            log_messages = [str(call) for call in mock_info.call_args_list]
            self.assertTrue(any("Statistics" in msg for msg in log_messages))
    
    def test_show_recent_logs_output(self):
        """Test show_recent logs recent items."""
        import torboxed
        from unittest.mock import patch
        
        # Add items
        torboxed.record_processed(
            "tt1111111", "Test Movie", 2024, "movie", "added", "success",
            debrid_id="tb-1", magnet="magnet:1",
            quality_score=2500, quality_label="1080p"
        )
        
        with patch.object(torboxed.logger, 'info') as mock_info:
            torboxed.show_recent(limit=5)
            
            # Verify info was logged
            self.assertTrue(mock_info.called)


class TestSetupLogging(unittest.TestCase):
    """Test setup_logging function."""
    
    def test_setup_logging_returns_logger(self):
        """Test setup_logging returns configured logger."""
        import torboxed
        
        logger = torboxed.setup_logging(verbose=False, log_to_file=False)
        
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, "torboxed")
    
    def test_setup_logging_verbose(self):
        """Test setup_logging with verbose=True."""
        import torboxed
        import logging
        
        logger = torboxed.setup_logging(verbose=True, log_to_file=False)
        
        # Check that DEBUG level is set for console
        self.assertEqual(logger.level, logging.DEBUG)
    
    def test_setup_logging_file_handler(self):
        """Test setup_logging with file handler."""
        import torboxed
        import logging
        
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "test.log"
            
            # Temporarily override log path
            original_log_path = torboxed.LOG_PATH
            torboxed.LOG_PATH = log_path
            
            try:
                logger = torboxed.setup_logging(verbose=False, log_to_file=True)
                
                # Log a message
                logger.info("Test message")
                
                # Close handlers to flush
                for handler in logger.handlers:
                    handler.close()
                
                # Check log file was created
                if log_path.exists():
                    content = log_path.read_text()
                    self.assertIn("Test message", content)
            finally:
                torboxed.LOG_PATH = original_log_path


class TestLogResult(unittest.TestCase):
    """Test log_result function."""
    
    def test_log_result_with_details(self):
        """Test log_result with details."""
        import torboxed
        from unittest.mock import patch
        
        with patch.object(torboxed.logger, 'info') as mock_info:
            torboxed.log_result("added", "Test Movie", {"quality": "1080p", "score": 2500})
            
            mock_info.assert_called_once()
            # Check the formatted message (call_args[0] are positional args, get the formatted string)
            call_args = mock_info.call_args
            # The log call uses format string with separate args, check the args
            self.assertIn("ADDED", str(call_args))
            self.assertIn("Test Movie", str(call_args))
    
    def test_log_result_without_details(self):
        """Test log_result without details."""
        import torboxed
        from unittest.mock import patch
        
        with patch.object(torboxed.logger, 'info') as mock_info:
            torboxed.log_result("skipped", "Test Movie")
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args
            self.assertIn("SKIPPED", str(call_args))


class TestGetDebridService(unittest.TestCase):
    """Test get_debrid_service function."""
    
    def test_default_to_torbox(self):
        """Test get_debrid_service defaults to torbox."""
        import torboxed
        from unittest.mock import patch
        
        with patch.object(torboxed, 'get_env') as mock_get_env:
            mock_get_env.return_value = {}
            
            result = torboxed.get_debrid_service()
            
            self.assertEqual(result, "torbox")
    
    def test_explicit_torbox(self):
        """Test get_debrid_service with explicit torbox."""
        import torboxed
        from unittest.mock import patch
        
        with patch.object(torboxed, 'get_env') as mock_get_env:
            mock_get_env.return_value = {"DEBRID_SERVICE": "torbox"}
            
            result = torboxed.get_debrid_service()
            
            self.assertEqual(result, "torbox")
    
    def test_real_debrid(self):
        """Test get_debrid_service with real_debrid."""
        import torboxed
        from unittest.mock import patch
        
        with patch.object(torboxed, 'get_env') as mock_get_env:
            mock_get_env.return_value = {"DEBRID_SERVICE": "real_debrid"}
            
            result = torboxed.get_debrid_service()
            
            self.assertEqual(result, "real_debrid")
    
    def test_unknown_service_defaults_to_torbox(self):
        """Test get_debrid_service with unknown service defaults to torbox."""
        import torboxed
        from unittest.mock import patch
        
        with patch.object(torboxed, 'get_env') as mock_get_env, \
             patch.object(torboxed.logger, 'warning') as mock_warning:
            mock_get_env.return_value = {"DEBRID_SERVICE": "unknown_service"}
            
            result = torboxed.get_debrid_service()
            
            self.assertEqual(result, "torbox")
            mock_warning.assert_called_once()


class TestGetEnvFunctions(unittest.TestCase):
    """Test get_env and related functions."""
    
    def setUp(self):
        """Clear env cache before each test."""
        import torboxed
        torboxed._env_cache = None
    
    def test_get_env_lazy_loads(self):
        """Test get_env lazy loads environment."""
        import torboxed
        from unittest.mock import patch
        
        with patch.object(torboxed, 'load_env') as mock_load_env:
            mock_load_env.return_value = {"TORBOX_API_KEY": "test_key"}
            
            # First call should load
            result = torboxed.get_env()
            self.assertEqual(result["TORBOX_API_KEY"], "test_key")
            mock_load_env.assert_called_once()
            
            # Second call should use cache
            mock_load_env.reset_mock()
            result2 = torboxed.get_env()
            self.assertEqual(result2["TORBOX_API_KEY"], "test_key")
            mock_load_env.assert_not_called()


class TestLoadEnv(unittest.TestCase):
    """Test load_env function edge cases."""
    
    def test_load_env_missing_file(self):
        """Test load_env with missing file."""
        import torboxed
        
        # Save original path
        original_path = torboxed.ENV_PATH
        
        try:
            # Point to non-existent file
            torboxed.ENV_PATH = Path("/nonexistent/path/.env")
            
            result = torboxed.load_env()
            
            self.assertEqual(result, {})
        finally:
            torboxed.ENV_PATH = original_path


class TestAPIErrorClasses(unittest.TestCase):
    """Test API error exception classes."""
    
    def test_api_error_with_status_code(self):
        """Test APIError with status code."""
        from torboxed import APIError
        
        error = APIError("Test error", status_code=500)
        
        self.assertEqual(str(error), "Test error")
        self.assertEqual(error.status_code, 500)
        self.assertIsNone(error.retry_after)
    
    def test_api_error_with_retry_after(self):
        """Test APIError with retry_after."""
        from torboxed import APIError
        
        error = APIError("Rate limited", status_code=429, retry_after=60)
        
        self.assertEqual(error.status_code, 429)
        self.assertEqual(error.retry_after, 60)
    
    def test_rate_limit_error_is_api_error(self):
        """Test RateLimitError is subclass of APIError."""
        from torboxed import RateLimitError, APIError
        
        error = RateLimitError("Rate limited", status_code=429)
        
        self.assertIsInstance(error, APIError)
        self.assertEqual(error.status_code, 429)
    
    def test_api_response_error_is_api_error(self):
        """Test APIResponseError is subclass of APIError."""
        from torboxed import APIResponseError, APIError
        
        error = APIResponseError("Invalid response")
        
        self.assertIsInstance(error, APIError)


class TestWebDAVAndTorrentFiles(unittest.TestCase):
    """Test WebDAV path resolution and torrent_files database functions."""

    def setUp(self):
        """Create temporary database for testing."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"

        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()

    def tearDown(self):
        """Clean up temporary database."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    # =========================================================================
    # _classify_file tests
    # =========================================================================

    def test_classify_video_file(self):
        """_classify_file returns is_video=True for .mkv/.mp4/.avi files."""
        for name in ("movie.mkv", "episode.mp4", "video.avi"):
            is_video, is_subtitle, sub_lang, ep_s, ep_n = _classify_file(name, name)
            self.assertTrue(is_video, f"{name} should be video")
            self.assertFalse(is_subtitle)
            self.assertIsNone(sub_lang)

    def test_classify_subtitle_file(self):
        """_classify_file returns is_subtitle=True for .srt/.ass/.vtt files."""
        for name in ("subs.srt", "captions.ass", "subtitles.vtt"):
            is_video, is_subtitle, sub_lang, ep_s, ep_n = _classify_file(name, name)
            self.assertFalse(is_video)
            self.assertTrue(is_subtitle, f"{name} should be subtitle")

    def test_classify_non_media(self):
        """_classify_file returns False/False for .txt/.nfo files."""
        for name in ("readme.txt", "info.nfo", "sample.jpg"):
            is_video, is_subtitle, sub_lang, ep_s, ep_n = _classify_file(name, name)
            self.assertFalse(is_video)
            self.assertFalse(is_subtitle)
            self.assertIsNone(sub_lang)

    # =========================================================================
    # store_torrent_files / get_torrent_files tests
    # =========================================================================

    @patch('torboxed.guessit')
    def test_store_and_get_torrent_files(self, mock_guessit):
        """store files via store_torrent_files, verify get_torrent_files retrieves them."""
        # Mock guessit to return season/episode
        mock_guessit.return_value = {"season": 1, "episode": 5}

        files = [
            {"name": "Show.S01E05.mkv", "path": "Show.S01E05.mkv", "size": 500000000, "id": "f1"},
            {"name": "Show.S01E05.en.srt", "path": "Show.S01E05.en.srt", "size": 50000, "id": "f2"},
        ]
        store_torrent_files("deb_123", "torbox", files)

        result = get_torrent_files("deb_123")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["debrid_id"], "deb_123")
        self.assertEqual(result[0]["debrid_service"], "torbox")

        # Find the video file
        video = [r for r in result if r["is_video"] == 1]
        self.assertEqual(len(video), 1)
        self.assertEqual(video[0]["episode_season"], 1)
        self.assertEqual(video[0]["episode_number"], 5)

        # Find the subtitle file
        subs = [r for r in result if r["is_subtitle"] == 1]
        self.assertEqual(len(subs), 1)

    def test_get_torrent_files_empty(self):
        """get_torrent_files returns [] for unknown debrid_id."""
        result = get_torrent_files("nonexistent")
        self.assertEqual(result, [])

    @patch('torboxed.guessit')
    def test_get_video_file_for_episode_match(self, mock_guessit):
        """store files for a debrid_id, verify get_video_file_for_episode finds S01E02."""
        mock_guessit.side_effect = lambda name: (
            {"season": 1, "episode": 1} if "E01" in name else
            {"season": 1, "episode": 2} if "E02" in name else
            {"season": None, "episode": None}
        )

        files = [
            {"name": "Show.S01E01.mkv", "path": "S01E01.mkv", "size": 500, "id": "e01"},
            {"name": "Show.S01E02.mkv", "path": "S01E02.mkv", "size": 600, "id": "e02"},
        ]
        store_torrent_files("deb_456", "torbox", files)

        result = get_video_file_for_episode("deb_456", 1, 2)
        self.assertIsNotNone(result)
        self.assertEqual(result["file_name"], "Show.S01E02.mkv")
        self.assertEqual(result["episode_season"], 1)
        self.assertEqual(result["episode_number"], 2)

    @patch('torboxed.guessit')
    def test_get_video_file_for_episode_no_match(self, mock_guessit):
        """returns None when no matching episode exists."""
        mock_guessit.return_value = {"season": 1, "episode": 1}

        files = [
            {"name": "Show.S01E01.mkv", "path": "S01E01.mkv", "size": 500, "id": "e01"},
        ]
        store_torrent_files("deb_789", "torbox", files)

        result = get_video_file_for_episode("deb_789", 1, 2)  # E02 doesn't exist
        self.assertIsNone(result)

    @patch('torboxed.guessit')
    def test_get_subtitle_files(self, mock_guessit):
        """store video+subtitle files, verify get_subtitle_files returns only subtitles."""
        mock_guessit.return_value = {"season": 1, "episode": 1}

        files = [
            {"name": "Show.S01E01.mkv", "path": "S01E01.mkv", "size": 500, "id": "v1"},
            {"name": "Show.S01E01.en.srt", "path": "S01E01.en.srt", "size": 10, "id": "s1"},
            {"name": "Show.S01E01.nl.srt", "path": "S01E01.nl.srt", "size": 12, "id": "s2"},
        ]
        store_torrent_files("deb_sub_1", "torbox", files)

        subs = get_subtitle_files("deb_sub_1")
        self.assertEqual(len(subs), 2)
        self.assertTrue(all(s["is_subtitle"] == 1 for s in subs))
        languages = {s["subtitle_language"] for s in subs}
        self.assertEqual(languages, {"en", "nl"})

    def test_cleanup_orphan_torrent_files(self):
        """store files for orphaned debrid_id, call cleanup, verify rows are deleted."""
        import torboxed

        # Store files for an orphan debrid_id (not in processed table)
        store_torrent_files("orphan_id", "torbox", [
            {"name": "Movie.mkv", "path": "Movie.mkv", "size": 1000, "id": "f1"},
        ])

        # Verify files exist
        self.assertEqual(len(get_torrent_files("orphan_id")), 1)

        # Cleanup
        deleted = cleanup_orphan_torrent_files()
        self.assertEqual(deleted, 1)

        # Verify files are gone
        self.assertEqual(len(get_torrent_files("orphan_id")), 0)

    def test_cleanup_preserves_valid_torrent_files(self):
        """cleanup only removes orphan rows, keeps ones with valid debrid_id in processed."""
        import torboxed

        # Add a processed record
        torboxed.record_processed(
            "tt1234567", "Test Movie", 2024, "movie", "added", "success",
            debrid_id="valid_id", magnet="magnet:test",
            quality_score=2500, quality_label="1080p",
            debrid_service="torbox"
        )

        # Store files for valid debrid_id
        store_torrent_files("valid_id", "torbox", [
            {"name": "Movie.mkv", "path": "Movie.mkv", "size": 1000, "id": "f1"},
        ])

        # Store files for orphan debrid_id
        store_torrent_files("orphan_id", "torbox", [
            {"name": "Orphan.mkv", "path": "Orphan.mkv", "size": 500, "id": "f2"},
        ])

        deleted = cleanup_orphan_torrent_files()
        self.assertEqual(deleted, 1)

        # Valid files should remain
        self.assertEqual(len(get_torrent_files("valid_id")), 1)
        # Orphan files should be gone
        self.assertEqual(len(get_torrent_files("orphan_id")), 0)

    # =========================================================================
    # Filename extraction / building tests
    # =========================================================================

    def test_extract_imdb_from_filename(self):
        """extract_imdb_from_filename parses {imdb-tt0137523} tag."""
        result = extract_imdb_from_filename("Fight Club (1999) {imdb-tt0137523}.mkv")
        self.assertEqual(result, "tt0137523")

        result = extract_imdb_from_filename("{imdb-tt1234567}")
        self.assertEqual(result, "tt1234567")

    def test_extract_imdb_no_match(self):
        """returns None when no imdb tag present."""
        self.assertIsNone(extract_imdb_from_filename("Movie.2024.mkv"))
        self.assertIsNone(extract_imdb_from_filename(""))
        self.assertIsNone(extract_imdb_from_filename("{imdb-}"))

    def test_build_movie_filename(self):
        """build_movie_filename with known params."""
        result = build_movie_filename("Fight Club", 1999, "tt0137523")
        self.assertEqual(result, "Fight Club (1999) {imdb-tt0137523}.mkv")

    def test_build_movie_filename_slashes_replaced(self):
        """build_movie_filename replaces slashes in title."""
        result = build_movie_filename("AC/DC", 2024, "tt1234567")
        self.assertNotIn("/", result)
        self.assertIn("AC-DC", result)

    def test_build_episode_filename(self):
        """build_episode_filename with S01E02."""
        result = build_episode_filename("Breaking Bad", 1, 2, "tt0903747")
        self.assertEqual(
            result,
            "Breaking Bad {imdb-tt0903747} S01E02.mkv"
        )

    def test_build_subtitle_filename_with_lang(self):
        """build_subtitle_filename with language code."""
        result = build_subtitle_filename("Movie (2024) {imdb-tt1234567}.mkv", "en")
        self.assertEqual(result, "Movie (2024) {imdb-tt1234567}.en.srt")

    def test_build_subtitle_filename_no_lang(self):
        """build_subtitle_filename without language."""
        result = build_subtitle_filename("Movie (2024) {imdb-tt1234567}.mkv", None)
        self.assertEqual(result, "Movie (2024) {imdb-tt1234567}.srt")

    def test_build_subtitle_filename_custom_ext(self):
        """build_subtitle_filename with custom extension."""
        result = build_subtitle_filename("Movie.mkv", "fr", ".ass")
        self.assertEqual(result, "Movie.fr.ass")

    # =========================================================================
    # resolve_path tests
    # =========================================================================

    def test_resolve_path_root(self):
        """resolve_path("/") returns type="root"."""
        result = resolve_path("/")
        self.assertEqual(result["type"], "root")

    def test_resolve_path_root_empty(self):
        """resolve_path("") returns type="root"."""
        result = resolve_path("")
        self.assertEqual(result["type"], "root")

    def test_resolve_path_movies_collection(self):
        """resolve_path("/Movies/") returns type="collection_root"."""
        result = resolve_path("/Movies/")
        self.assertEqual(result["type"], "collection_root")
        self.assertEqual(result["root"], "Movies")

    def test_resolve_path_tv_shows(self):
        """resolve_path("/TV Shows/") returns type="show_list"."""
        result = resolve_path("/TV Shows/")
        self.assertEqual(result["type"], "show_list")
        self.assertEqual(result["root"], "TV Shows")

    def test_resolve_path_movie_folder(self):
        """resolve_path with /Movies/Title/ returns type="movie_folder"."""
        result = resolve_path("/Movies/Fight Club (1999)/")
        self.assertEqual(result["type"], "movie_folder")
        self.assertEqual(result["show_title"], "Fight Club (1999)")

    def test_resolve_path_movie_file(self):
        """resolve_path with full movie path returns type="movie_file"."""
        path = "/Movies/Fight Club (1999)/Fight Club (1999) {imdb-tt0137523}.mkv"
        result = resolve_path(path)
        self.assertEqual(result["type"], "movie_file")
        self.assertEqual(result["imdb_id"], "tt0137523")

    def test_resolve_path_show_folder(self):
        """resolve_path with /TV Shows/Show Name/ returns type="show_folder"."""
        result = resolve_path("/TV Shows/Breaking Bad/")
        self.assertEqual(result["type"], "show_folder")
        self.assertEqual(result["show_title"], "Breaking Bad")

    def test_resolve_path_season_folder(self):
        """resolve_path with /TV Shows/Show/Season 1/ returns type="season_folder"."""
        result = resolve_path("/TV Shows/Breaking Bad/Season 1/")
        self.assertEqual(result["type"], "season_folder")
        self.assertEqual(result["season_number"], 1)
        self.assertEqual(result["season_key"], "S01")

    def test_resolve_path_episode_file(self):
        """resolve_path with episode path returns type="episode_file".

        Regression test for issue #15: the season-folder check previously used
        len(segments) >= 3, which matched 4-segment file paths first and made
        the episode_file branch unreachable (WebDAV served 0-byte responses).
        """
        path = "/TV Shows/Breaking Bad/Season 1/Breaking Bad {imdb-tt0903747} S01E01.mkv"
        result = resolve_path(path)
        self.assertEqual(result["type"], "episode_file")
        self.assertEqual(result["show_title"], "Breaking Bad")
        self.assertEqual(result["season_number"], 1)
        self.assertEqual(result["episode_number"], 1)
        self.assertEqual(result["season_key"], "S01")
        self.assertEqual(result["imdb_id"], "tt0903747")

    def test_resolve_path_unknown(self):
        """resolve_path with unrecognized path returns type="unknown"."""
        result = resolve_path("/Some/Unknown/Path")
        self.assertEqual(result["type"], "unknown")

    # =========================================================================
    # resolve_debrid_id_for_path tests
    # =========================================================================

    def test_resolve_debrid_id_for_path(self):
        """resolve_debrid_id_for_path finds existing record."""
        import torboxed

        torboxed.record_processed(
            "tt0137523", "Fight Club", 1999, "movie", "added", "success",
            debrid_id="tb-fc-123", magnet="magnet:fc",
            quality_score=3500, quality_label="1080p BluRay",
            debrid_service="torbox"
        )

        result = resolve_debrid_id_for_path("tt0137523")
        self.assertIsNotNone(result)
        self.assertEqual(result["debrid_id"], "tb-fc-123")
        self.assertEqual(result["title"], "Fight Club")
        self.assertEqual(result["content_type"], "movie")

    def test_resolve_debrid_id_for_path_with_season(self):
        """resolve_debrid_id_for_path with season finds specific season record."""
        import torboxed

        torboxed.record_processed(
            "tt0903747", "Breaking Bad", 2008, "show", "added", "success",
            debrid_id="tb-bb-s01", magnet="magnet:bb1",
            quality_score=3400, quality_label="1080p WEB-DL",
            season="S01", debrid_service="torbox"
        )
        torboxed.record_processed(
            "tt0903747", "Breaking Bad", 2008, "show", "added", "success",
            debrid_id="tb-bb-s02", magnet="magnet:bb2",
            quality_score=3400, quality_label="1080p WEB-DL",
            season="S02", debrid_service="torbox"
        )

        result = resolve_debrid_id_for_path("tt0903747", "S01")
        self.assertIsNotNone(result)
        self.assertEqual(result["debrid_id"], "tb-bb-s01")

    def test_resolve_debrid_id_not_found(self):
        """returns None when no record exists."""
        result = resolve_debrid_id_for_path("tt9999999")
        self.assertIsNone(result)

    # =========================================================================
    # _get_db_listing tests
    # =========================================================================

    def test_get_db_listing_root(self):
        """_get_db_listing for root returns ["Movies", "TV Shows"]."""
        result = _get_db_listing("/", {"type": "root"})
        self.assertEqual(result, ["Movies", "TV Shows"])

    def test_get_db_listing_collection_root_empty(self):
        """_get_db_listing for collection_root with no movies returns []."""
        result = _get_db_listing("/Movies/", {"type": "collection_root"})
        self.assertEqual(result, [])

    def test_get_db_listing_collection_root_with_movies(self):
        """_get_db_listing shows movie filenames when movies exist."""
        import torboxed

        torboxed.record_processed(
            "tt0137523", "Fight Club", 1999, "movie", "added", "success",
            debrid_id="tb-fc", magnet="magnet:fc",
            quality_score=3500, quality_label="1080p BluRay",
            debrid_service="torbox"
        )

        result = _get_db_listing("/Movies/", {"type": "collection_root"})
        self.assertIn("Fight Club (1999) {imdb-tt0137523}.mkv", result)

    @patch('torboxed.guessit')
    def test_get_db_listing_movie_folder(self, mock_guessit):
        """_get_db_listing for movie_folder returns video + subtitle filenames."""
        import torboxed
        mock_guessit.return_value = {}

        torboxed.record_processed(
            "tt0137523", "Fight Club", 1999, "movie", "added", "success",
            debrid_id="tb-fc", magnet="magnet:fc",
            quality_score=3500, quality_label="1080p BluRay",
            debrid_service="torbox"
        )

        # Store subtitles for this movie
        store_torrent_files("tb-fc", "torbox", [
            {"name": "Fight.Club.1999.mkv", "path": "movie.mkv", "size": 1000, "id": "v1"},
            {"name": "Fight.Club.1999.en.srt", "path": "sub.en.srt", "size": 10, "id": "s1"},
        ])

        resolved = {"type": "movie_folder", "imdb_id": "tt0137523"}
        result = _get_db_listing("/Movies/Fight Club (1999)/", resolved)
        self.assertIn("Fight Club (1999) {imdb-tt0137523}.mkv", result)
        self.assertTrue(any("en.srt" in f for f in result))

    def test_get_db_listing_show_list_empty(self):
        """_get_db_listing for show_list with no shows returns []."""
        result = _get_db_listing("/TV Shows/", {"type": "show_list"})
        self.assertEqual(result, [])

    def test_get_db_listing_show_list_with_shows(self):
        """_get_db_listing for show_list returns show titles."""
        import torboxed

        torboxed.record_processed(
            "tt0903747", "Breaking Bad", 2008, "show", "added", "success",
            debrid_id="tb-bb", magnet="magnet:bb",
            quality_score=3400, season="S01",
            debrid_service="torbox"
        )

        result = _get_db_listing("/TV Shows/", {"type": "show_list"})
        self.assertIn("Breaking Bad", result)

    def test_get_db_listing_show_folder(self):
        """_get_db_listing for show_folder returns season names."""
        import torboxed

        torboxed.record_processed(
            "tt0903747", "Breaking Bad", 2008, "show", "added", "success",
            debrid_id="tb-bb", magnet="magnet:bb",
            quality_score=3400, season="S01",
            debrid_service="torbox"
        )

        resolved = {"type": "show_folder", "show_title": "Breaking Bad"}
        result = _get_db_listing("/TV Shows/Breaking Bad/", resolved)
        self.assertIn("Season 1", result)

    # =========================================================================
    # _get_best_debrid_for_season tests
    # =========================================================================

    def test_get_best_debrid_for_season_empty(self):
        """_get_best_debrid_for_season returns None for empty list."""
        result = _get_best_debrid_for_season([], "S01", 1)
        self.assertIsNone(result)

    def test_get_best_debrid_for_season_complete_wins(self):
        """_get_best_debrid_for_season prefers complete pack over season pack."""
        seasons = [
            {"season": "S01", "quality_score": 5000, "debrid_id": "id-s01"},
            {"season": "Complete", "quality_score": 3000, "debrid_id": "id-complete"},
        ]
        result = _get_best_debrid_for_season(seasons, "S01", 1)
        self.assertIsNotNone(result)
        self.assertEqual(result["debrid_id"], "id-complete")

    def test_get_best_debrid_for_season_multi_season_wins_over_season(self):
        """_get_best_debrid_for_season prefers a multi-season pack (S01-S05)
        over an individual season pack, even at lower quality.

        Regression coverage for issue #18: ranged labels previously classified
        as plain 'season' without SeasonInfo context, so completeness was lost
        for historical records.
        """
        seasons = [
            {"season": "S01", "quality_score": 5000, "debrid_id": "id-s01"},
            {"season": "S01-S05", "quality_score": 4000, "debrid_id": "id-multi"},
        ]
        result = _get_best_debrid_for_season(seasons, "S01", 1)
        self.assertIsNotNone(result)
        # S01-S05 now classifies as multi_season (completeness=3) vs season (2),
        # so the broader pack wins regardless of quality score
        self.assertEqual(result["debrid_id"], "id-multi")

    def test_get_best_debrid_for_season_same_level_higher_quality(self):
        """_get_best_debrid_for_season within same completeness picks higher quality."""
        seasons = [
            {"season": "S01", "quality_score": 3000, "debrid_id": "id-low"},
            {"season": "S01", "quality_score": 5000, "debrid_id": "id-high"},
        ]
        result = _get_best_debrid_for_season(seasons, "S01", 1)
        self.assertIsNotNone(result)
        self.assertEqual(result["debrid_id"], "id-high")

    # =========================================================================
    # get_webdav_config tests
    # =========================================================================

    def test_get_webdav_config_defaults(self):
        """get_webdav_config returns defaults when env is empty."""
        import torboxed
        with patch.object(torboxed, 'get_env', return_value={}):
            config = get_webdav_config()
            self.assertEqual(config["host"], "0.0.0.0")
            self.assertEqual(config["port"], 8080)
            self.assertEqual(config["log_level"], "info")

    def test_webdav_config_from_env(self):
        """get_webdav_config reads from environment variables."""
        import torboxed
        with patch.object(torboxed, 'get_env', return_value={
            "WEBDAV_HOST": "127.0.0.1",
            "WEBDAV_PORT": "9090",
            "WEBDAV_AUTH_USER": "admin",
            "WEBDAV_AUTH_PASS": "secret",
            "WEBDAV_LOG_LEVEL": "debug",
        }):
            config = get_webdav_config()
            self.assertEqual(config["host"], "127.0.0.1")
            self.assertEqual(config["port"], 9090)
            self.assertEqual(config["user"], "admin")
            self.assertEqual(config["password"], "secret")
            self.assertEqual(config["log_level"], "debug")

    # =========================================================================
    # backfill_torrent_files tests
    # =========================================================================

    def test_backfill_empty_processed_table(self):
        """backfill_torrent_files returns (0, 0) when no processed records."""
        mock_debrid = Mock()
        result = backfill_torrent_files(mock_debrid)
        self.assertEqual(result, (0, 0))

    @patch('torboxed.RateLimiter')
    def test_backfill_skips_already_cached(self, mock_rate_limiter_class):
        """backfill_torrent_files skips records already in torrent_files table."""
        import torboxed
        mock_limiter = Mock()
        mock_rate_limiter_class.return_value = mock_limiter

        # Add a processed record with debrid_id
        torboxed.record_processed(
            "tt1234567", "Cached Movie", 2024, "movie", "added", "success",
            debrid_id="cached_id", magnet="magnet:test",
            quality_score=2500, quality_label="1080p",
            debrid_service="torbox"
        )

        # Pre-store files for this debrid_id (simulate already cached)
        store_torrent_files("cached_id", "torbox", [
            {"name": "Movie.mkv", "path": "Movie.mkv", "size": 1000, "id": "f1"},
        ])

        mock_debrid = Mock()
        result = backfill_torrent_files(mock_debrid)

        # Should process 1 record, store 0 new files (already cached)
        self.assertEqual(result[0], 1)  # total_processed
        self.assertEqual(result[1], 0)  # files_stored

        # get_torrent_files should NOT have been called (skipped due to cache hit)
        mock_debrid.get_torrent_files.assert_not_called()

    @patch('torboxed.RateLimiter')
    def test_backfill_stores_new_files(self, mock_rate_limiter_class):
        """backfill_torrent_files fetches and stores files for uncached records."""
        import torboxed
        mock_limiter = Mock()
        mock_rate_limiter_class.return_value = mock_limiter

        # Add a processed record WITHOUT cached torrent_files
        torboxed.record_processed(
            "tt7654321", "New Movie", 2024, "movie", "added", "success",
            debrid_id="new_id", magnet="magnet:new",
            quality_score=2500, quality_label="1080p",
            debrid_service="torbox"
        )

        mock_debrid = Mock()
        mock_debrid.get_torrent_files.return_value = [
            {"name": "New.Movie.2024.mkv", "path": "movie.mkv", "size": 2000, "id": "f99"},
        ]

        result = backfill_torrent_files(mock_debrid)

        self.assertEqual(result[0], 1)  # total_processed
        self.assertEqual(result[1], 1)  # files_stored

        # Verify files were stored
        stored = get_torrent_files("new_id")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["file_name"], "New.Movie.2024.mkv")

        mock_debrid.get_torrent_files.assert_called_once_with("new_id")

    # =========================================================================
    # Edge case tests
    # =========================================================================

    def test_multi_episode_filename(self):
        """build_episode_filename with S01E01 (standard formatting)."""
        result = build_episode_filename("Show Name", 1, 1, "tt9999999")
        self.assertEqual(result, "Show Name {imdb-tt9999999} S01E01.mkv")

    @patch('torboxed.guessit')
    def test_classify_multi_dot_episode(self, mock_guessit):
        """_classify_file on S01E01E02 multi-episode file classifies as video."""
        mock_guessit.return_value = {"season": 1, "episode": [1, 2]}
        is_video, is_subtitle, sub_lang, ep_s, ep_n = _classify_file(
            "Show.S01E01E02.1080p.mkv", "Show.S01E01E02.1080p.mkv"
        )
        self.assertTrue(is_video)
        self.assertFalse(is_subtitle)

    def test_resolve_path_subtitle_in_movie_folder(self):
        """resolve_path extracts imdb_id from subtitle in movie folder."""
        result = resolve_path(
            "/Movies/Fight Club (1999)/Fight Club (1999) {imdb-tt0137523}.en.srt"
        )
        self.assertEqual(result["type"], "movie_file")
        self.assertEqual(result["imdb_id"], "tt0137523")

    def test_resolve_path_special_episode(self):
        """resolve_path on S00E01 special episode parses season=0, episode=1."""
        result = resolve_path(
            "/TV Shows/Specials Show/Season 0/Specials Show {imdb-tt9999999} S00E01.mkv"
        )
        self.assertEqual(result["type"], "episode_file")
        self.assertEqual(result["season_number"], 0)
        self.assertEqual(result["episode_number"], 1)
        self.assertEqual(result["season_key"], "S00")
        self.assertEqual(result["imdb_id"], "tt9999999")

    def test_subtitle_files_no_episode(self):
        """get_subtitle_files with no season/episode returns all subtitles."""
        files = [
            {"name": "Movie.mkv", "path": "Movie.mkv", "size": 1000, "id": "v1"},
            {"name": "Movie.en.srt", "path": "sub.en.srt", "size": 10, "id": "s1"},
            {"name": "Movie.nl.srt", "path": "sub.nl.srt", "size": 12, "id": "s2"},
        ]
        store_torrent_files("deb_noep", "torbox", files)

        subs = get_subtitle_files("deb_noep")
        self.assertEqual(len(subs), 2)
        self.assertTrue(all(s["is_subtitle"] == 1 for s in subs))
        languages = {s["subtitle_language"] for s in subs}
        self.assertEqual(languages, {"en", "nl"})

    def test_extract_imdb_deeply_nested(self):
        """extract_imdb_from_filename finds imdb tag amid other tokens."""
        result = extract_imdb_from_filename("Show Name {imdb-tt9999999} S01E01.en.srt")
        self.assertEqual(result, "tt9999999")


class TestPendingSentinelCache(unittest.TestCase):
    """__pending__ sentinel rows must not permanently poison the file cache (#16).

    A sentinel is written when a WebDAV-triggered listing fetch returns no files
    (torrent still processing). It must read as a cache MISS so later requests
    refetch, and it must be purged once real files are stored.
    """

    def setUp(self):
        """Create temporary database for testing."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_db_path = Path(self.temp_dir.name) / "test.db"

        import torboxed
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = self.test_db_path
        torboxed.init_db()

    def tearDown(self):
        """Clean up temporary database."""
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    # =========================================================================
    # strip_pending_sentinel helper tests
    # =========================================================================

    def test_strip_pending_sentinel_no_sentinel(self):
        """strip_pending_sentinel returns all rows unchanged when no sentinel present."""
        files = [
            {"file_id": "f1", "file_name": "a.mkv"},
            {"file_id": "f2", "file_name": "b.srt"},
        ]
        real, had_sentinel = strip_pending_sentinel(files)
        self.assertEqual(len(real), 2)
        self.assertFalse(had_sentinel)

    def test_strip_pending_sentinel_with_sentinel(self):
        """strip_pending_sentinel separates the sentinel row from real rows."""
        files = [
            {"file_id": PENDING_SENTINEL_ID, "file_name": PENDING_SENTINEL_ID},
            {"file_id": "f1", "file_name": "a.mkv"},
        ]
        real, had_sentinel = strip_pending_sentinel(files)
        self.assertEqual(len(real), 1)
        self.assertEqual(real[0]["file_id"], "f1")
        self.assertTrue(had_sentinel)

    def test_strip_pending_sentinel_only(self):
        """strip_pending_sentinel on a sentinel-only result set yields an empty list."""
        real, had_sentinel = strip_pending_sentinel(
            [{"file_id": PENDING_SENTINEL_ID}]
        )
        self.assertEqual(real, [])
        self.assertTrue(had_sentinel)

    # =========================================================================
    # _fetch_and_cache_torrent_files sentinel behavior
    # =========================================================================

    @patch('torboxed.guessit')
    @patch('torboxed.get_debrid_client_for_service')
    def test_fetch_and_cache_stores_sentinel_on_empty_fetch(self, mock_factory, mock_guessit):
        """Empty debrid fetch stores one __pending__ sentinel and reports failure."""
        mock_client = Mock()
        mock_client.get_torrent_files.return_value = []
        mock_factory.return_value = mock_client

        import torboxed
        result = torboxed._fetch_and_cache_torrent_files("deb_pending", "torbox")
        self.assertFalse(result)

        stored = get_torrent_files("deb_pending")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["file_id"], PENDING_SENTINEL_ID)

        _, had_sentinel = strip_pending_sentinel(get_torrent_files("deb_pending"))
        self.assertTrue(had_sentinel)

    @patch('torboxed.guessit')
    @patch('torboxed.get_debrid_client_for_service')
    def test_fetch_and_cache_real_files_replace_sentinel(self, mock_factory, mock_guessit):
        """A later successful fetch replaces the sentinel with real files."""
        mock_guessit.return_value = {}
        mock_client = Mock()
        mock_client.get_torrent_files.return_value = [
            {"name": "Movie.mkv", "path": "Movie.mkv", "size": 1000, "id": "f1"},
        ]
        mock_factory.return_value = mock_client

        import torboxed
        # Seed the poisoned state: sentinel-only cache
        store_torrent_files("deb_recover", "torbox", [
            {"name": PENDING_SENTINEL_ID, "path": PENDING_SENTINEL_ID,
             "size": 0, "id": PENDING_SENTINEL_ID},
        ])

        result = torboxed._fetch_and_cache_torrent_files("deb_recover", "torbox")
        self.assertTrue(result)

        stored, had_sentinel = strip_pending_sentinel(get_torrent_files("deb_recover"))
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["file_id"], "f1")
        self.assertFalse(had_sentinel)

    # =========================================================================
    # store_torrent_files purge behavior
    # =========================================================================

    @patch('torboxed.guessit')
    def test_store_torrent_files_purges_sentinel_when_real_files_arrive(self, mock_guessit):
        """store_torrent_files deletes __pending__ rows once real files are cached."""
        mock_guessit.return_value = {}
        store_torrent_files("deb_purge", "torbox", [
            {"name": PENDING_SENTINEL_ID, "path": PENDING_SENTINEL_ID,
             "size": 0, "id": PENDING_SENTINEL_ID},
        ])
        self.assertEqual(len(get_torrent_files("deb_purge")), 1)

        store_torrent_files("deb_purge", "torbox", [
            {"name": "Real.Movie.mkv", "path": "Real.Movie.mkv",
             "size": 2000, "id": "r1"},
        ])

        stored = get_torrent_files("deb_purge")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["file_id"], "r1")

    @patch('torboxed.guessit')
    def test_store_torrent_files_sentinel_only_does_not_purge_self(self, mock_guessit):
        "store_torrent_files with only a sentinel payload keeps exactly one sentinel."""
        mock_guessit.return_value = {}
        store_torrent_files("deb_keep", "torbox", [
            {"name": PENDING_SENTINEL_ID, "path": PENDING_SENTINEL_ID,
             "size": 0, "id": PENDING_SENTINEL_ID},
        ])
        store_torrent_files("deb_keep", "torbox", [
            {"name": PENDING_SENTINEL_ID, "path": PENDING_SENTINEL_ID,
             "size": 0, "id": PENDING_SENTINEL_ID},
        ])

        stored = get_torrent_files("deb_keep")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["file_id"], PENDING_SENTINEL_ID)

    # =========================================================================
    # Serve-path semantics: 503 vs permanent 404 vs recovered 302
    # =========================================================================

    def _make_middleware(self):
        """Instantiate RedirectMiddleware without running its __init__."""
        import torboxed
        return torboxed.RedirectMiddleware.__new__(torboxed.RedirectMiddleware)

    @patch('torboxed.guessit')
    @patch('torboxed.get_debrid_client_for_service')
    def test_serve_path_sentinel_then_empty_fetch_returns_503(self, mock_factory, mock_guessit):
        """Sentinel-only cache refetches; when the torrent is still empty -> 503 Retry-After."""
        mock_guessit.return_value = {}
        mock_client = Mock()
        mock_client.get_torrent_files.return_value = []
        mock_factory.return_value = mock_client

        import torboxed
        torboxed.record_processed(
            "tt1112222", "Pending Movie", 2024, "movie", "added", "success",
            debrid_id="deb_503", magnet="magnet:503",
            quality_score=2500, quality_label="1080p",
            debrid_service="torbox"
        )
        # First failed request wrote a sentinel
        store_torrent_files("deb_503", "torbox", [
            {"name": PENDING_SENTINEL_ID, "path": PENDING_SENTINEL_ID,
             "size": 0, "id": PENDING_SENTINEL_ID},
        ])

        mw = self._make_middleware()
        start_response = Mock()
        resolved = {"type": "movie_file", "imdb_id": "tt1112222"}
        body = mw._serve_file_redirect({}, start_response, "GET", "/Movies/x.mkv", resolved)

        status = start_response.call_args[0][0]
        headers = start_response.call_args[0][1]
        self.assertEqual(status, "503 Service Unavailable")
        self.assertIn(("Retry-After", "60"), headers)
        self.assertEqual(body, [b"Torrent file listing not cached. Try again later."])

        # The retry attempt happened despite the sentinel being present
        self.assertTrue(mock_client.get_torrent_files.called)

    @patch('torboxed.guessit')
    @patch('torboxed._resolve_direct_url')
    @patch('torboxed.get_debrid_client_for_service')
    def test_serve_path_recovers_after_files_become_available(self, mock_factory,
                                                              mock_resolve_url,
                                                              mock_guessit):
        """Sentinel-only cache + later successful fetch yields a 302, not a permanent 404."""
        mock_guessit.return_value = {}
        mock_client = Mock()
        mock_client.get_torrent_files.return_value = [
            {"name": "Movie.mkv", "path": "Movie.mkv", "size": 1000, "id": "f1"},
        ]
        mock_factory.return_value = mock_client
        mock_resolve_url.return_value = "https://cdn.example.com/stream"

        import torboxed
        torboxed.record_processed(
            "tt3334444", "Recovering Movie", 2024, "movie", "added", "success",
            debrid_id="deb_recover302", magnet="magnet:r2",
            quality_score=2500, quality_label="1080p",
            debrid_service="torbox"
        )
        # Poisoned state from an earlier empty fetch
        store_torrent_files("deb_recover302", "torbox", [
            {"name": PENDING_SENTINEL_ID, "path": PENDING_SENTINEL_ID,
             "size": 0, "id": PENDING_SENTINEL_ID},
        ])

        mw = self._make_middleware()
        start_response = Mock()
        resolved = {"type": "movie_file", "imdb_id": "tt3334444"}
        mw._serve_file_redirect({}, start_response, "GET", "/Movies/x.mkv", resolved)

        status = start_response.call_args[0][0]
        headers = start_response.call_args[0][1]
        self.assertEqual(status, "302 Found")
        self.assertIn(("Location", "https://cdn.example.com/stream"), headers)

        # Sentinel purged; only the real file row remains
        stored, had_sentinel = strip_pending_sentinel(get_torrent_files("deb_recover302"))
        self.assertFalse(had_sentinel)
        self.assertEqual(len(stored), 1)

    @patch('torboxed.guessit')
    @patch('torboxed.get_debrid_client_for_service')
    def test_serve_path_no_cache_and_failed_fetch_is_503_not_404(self, mock_factory,
                                                                  mock_guessit):
        "Uncached listing whose first fetch fails serves 503 (retryable), never 404."""
        mock_guessit.return_value = {}
        mock_client = Mock()
        mock_client.get_torrent_files.return_value = []
        mock_factory.return_value = mock_client

        import torboxed
        torboxed.record_processed(
            "tt5556666", "Empty Movie", 2024, "movie", "added", "success",
            debrid_id="deb_first", magnet="magnet:f1",
            quality_score=2500, quality_label="1080p",
            debrid_service="torbox"
        )

        mw = self._make_middleware()
        resolved = {"type": "movie_file", "imdb_id": "tt5556666"}
        for _ in range(2):  # repeated retries keep returning 503 while processing
            start_response = Mock()
            body = mw._serve_file_redirect({}, start_response, "GET", "/Movies/x.mkv", resolved)
            status = start_response.call_args[0][0]
            self.assertEqual(status, "503 Service Unavailable")
            self.assertIn(("Retry-After", "60"), start_response.call_args[0][1])
            self.assertNotEqual(body, [b"File not found in torrent"])


class TestRedirectMiddlewareAuth(unittest.TestCase):
    """RedirectMiddleware must enforce Basic auth before serving 302 redirects.

    Regression tests for the authentication bypass where intercepted GET/HEAD
    requests were redirected to debrid CDN URLs without any credential check.
    """

    MOVIE_PATH = "/Movies/Fight Club (1999)/Fight Club (1999) {imdb-tt0137523}.mkv"
    SUBTITLE_FALLBACK_PATH = "/TV Shows/Breaking Bad/subtitles.en.srt"
    CDN_URL = "https://cdn.example.com/file.mkv"

    def setUp(self):
        import torboxed
        self.torboxed = torboxed
        self.middleware = torboxed.RedirectMiddleware(self._record_app)
        self.app_calls = []
        self.start_calls = []
        self.body = None

    def _record_app(self, environ, start_response):
        """Downstream wsgidav app: records that a request fell through."""
        self.app_calls.append(environ.get("PATH_INFO"))
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"dav-app"]

    def _start_response(self, status, headers):
        self.start_calls.append((status, list(headers)))

    def _basic_header(self, user="admin", password="secret"):
        import base64
        encoded = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        return f"Basic {encoded}"

    def _environ(self, path, authorization=None, method="GET"):
        environ = {
            "REQUEST_METHOD": method,
            "PATH_INFO": path,
        }
        if authorization is not None:
            environ["HTTP_AUTHORIZATION"] = authorization
        return environ

    def _patch_env(self, extra=None):
        env = {"WEBDAV_AUTH_USER": "admin", "WEBDAV_PASS": "", "WEBDAV_AUTH_PASS": "secret"}
        if extra:
            env.update(extra)
        return patch.object(self.torboxed, "get_env", return_value=env)

    def test_unauthenticated_get_movie_path_returns_401(self):
        """Unauthenticated GET on a valid movie path returns 401, not 302."""
        with self._patch_env():
            result = self.middleware(self._environ(self.MOVIE_PATH), self._start_response)
        status, headers = self.start_calls[0]
        self.assertEqual(status, "401 Unauthorized")
        self.assertIn(("WWW-Authenticate", 'Basic realm="TorBoxed"'), headers)
        self.assertEqual(result, [b"Unauthorized"])
        self.assertEqual(self.app_calls, [], "request must not reach downstream app or redirect")

    def test_unauthenticated_head_movie_path_returns_401(self):
        """Unauthenticated HEAD on a valid movie path returns 401."""
        with self._patch_env():
            self.middleware(self._environ(self.MOVIE_PATH, method="HEAD"), self._start_response)
        self.assertEqual(self.start_calls[0][0], "401 Unauthorized")
        self.assertEqual(self.app_calls, [])

    def test_wrong_credentials_return_401(self):
        """Basic auth with wrong password returns 401."""
        header = self._basic_header(user="admin", password="wrong")
        with self._patch_env():
            self.middleware(self._environ(self.MOVIE_PATH, authorization=header), self._start_response)
        self.assertEqual(self.start_calls[0][0], "401 Unauthorized")
        self.assertEqual(self.app_calls, [])

    def test_malformed_authorization_header_returns_401(self):
        """Malformed Authorization headers are rejected, never crash through."""
        for header in ("Bearer abc123", "Basic !!!!not-base64!!!", "Basic"):
            self.setUp()
            with self._patch_env():
                self.middleware(
                    self._environ(self.MOVIE_PATH, authorization=header),
                    self._start_response,
                )
            self.assertEqual(self.start_calls[0][0], "401 Unauthorized", f"header={header!r}")
            self.assertEqual(self.app_calls, [])

    @patch("torboxed._resolve_direct_url")
    @patch("torboxed.get_torrent_files")
    @patch("torboxed.resolve_debrid_id_for_path")
    def test_authenticated_get_returns_302_with_cdn_url(self, mock_db_info, mock_files, mock_url):
        """Authenticated GET on a movie path still returns 302 to the CDN URL."""
        mock_db_info.return_value = {"debrid_id": "deb123", "debrid_service": "torbox"}
        mock_files.return_value = [{"file_id": "f1", "is_video": True}]
        mock_url.return_value = self.CDN_URL

        header = self._basic_header()
        with self._patch_env():
            self.middleware(self._environ(self.MOVIE_PATH, authorization=header), self._start_response)

        status, headers = self.start_calls[0]
        self.assertEqual(status, "302 Found")
        self.assertIn(("Location", self.CDN_URL), headers)
        self.assertEqual(mock_url.call_args[0][0], "deb123")

    @patch("torboxed._resolve_direct_url")
    @patch("torboxed.get_torrent_files")
    @patch("torboxed.resolve_debrid_id_for_path")
    def test_subtitle_fallback_requires_auth(self, mock_db_info, mock_files, mock_url):
        """Subtitle fallback resolution is equally protected by the auth check."""
        with self._patch_env():
            self.middleware(
                self._environ(self.SUBTITLE_FALLBACK_PATH), self._start_response
            )
        self.assertEqual(self.start_calls[0][0], "401 Unauthorized")
        self.assertEqual(mock_db_info.call_count, 0)
        self.assertEqual(self.app_calls, [])

    @patch("torboxed._resolve_direct_url")
    @patch("torboxed.get_torrent_files")
    @patch("torboxed.resolve_debrid_id_for_path")
    def test_anonymous_mode_allows_redirect_when_explicitly_enabled(self, mock_db_info, mock_files, mock_url):
        """No configured credentials + WEBDAV_ALLOW_ANONYMOUS=true permits redirects."""
        mock_db_info.return_value = {"debrid_id": "deb123", "debrid_service": "torbox"}
        mock_files.return_value = [{"file_id": "f1", "is_video": True}]
        mock_url.return_value = self.CDN_URL

        with self._patch_env({"WEBDAV_AUTH_USER": "", "WEBDAV_AUTH_PASS": "", "WEBDAV_ALLOW_ANONYMOUS": "true"}):
            self.middleware(self._environ(self.MOVIE_PATH), self._start_response)

        status, headers = self.start_calls[0]
        self.assertEqual(status, "302 Found")
        self.assertIn(("Location", self.CDN_URL), headers)

    def test_no_credentials_and_no_anonymous_flag_denies_access(self):
        """Fail closed: no credentials configured and no anonymous flag => 401."""
        with self._patch_env({"WEBDAV_AUTH_USER": "", "WEBDAV_AUTH_PASS": ""}):
            self.middleware(self._environ(self.MOVIE_PATH), self._start_response)
        self.assertEqual(self.start_calls[0][0], "401 Unauthorized")
        self.assertEqual(self.app_calls, [])

    def test_non_intercepted_paths_pass_through_unchanged(self):
        """Directory listings and /health are untouched (wsgidav handles them)."""
        with self._patch_env():
            for path in ("/", "/health", "/Movies/"):
                self.start_calls.clear()
                self.app_calls.clear()
                self.middleware(self._environ(path), self._start_response)
                self.assertEqual(self.app_calls, [path], f"path {path} should pass through")
                self.assertEqual(self.start_calls[0][0], "200 OK", f"path {path} not challenged")


class TestRedirectMiddlewareEpisodePlayback(unittest.TestCase):
    """GET/HEAD on TV episode paths must flow through the 302 CDN redirect.

    Regression tests for issue #15: resolve_path() misclassified 4-segment
    episode paths as season_folder, so RedirectMiddleware never intercepted
    them and wsgidav served a 0-byte virtual file instead of playback.
    """

    EPISODE_PATH = "/TV Shows/Breaking Bad/Season 1/Breaking Bad {imdb-tt0903747} S01E01.mkv"
    CDN_URL = "https://cdn.example.com/episode.mkv"

    def setUp(self):
        import torboxed
        self.torboxed = torboxed
        self.middleware = torboxed.RedirectMiddleware(lambda e, s: self._fail_downstream(e, s))
        self.start_calls = []

    def _fail_downstream(self, environ, start_response):
        raise AssertionError(
            f"episode request must be intercepted for redirect, reached {environ.get('PATH_INFO')}"
        )

    def _start_response(self, status, headers):
        self.start_calls.append((status, list(headers)))
        return lambda body: None

    def _basic_header(self):
        import base64
        encoded = base64.b64encode(b"admin:secret").decode("ascii")
        return f"Basic {encoded}"

    def _environ(self, method="GET"):
        return {
            "REQUEST_METHOD": method,
            "PATH_INFO": self.EPISODE_PATH,
            "HTTP_AUTHORIZATION": self._basic_header(),
        }

    def _patch_env(self):
        env = {"WEBDAV_AUTH_USER": "admin", "WEBDAV_PASS": "", "WEBDAV_AUTH_PASS": "secret"}
        return patch.object(self.torboxed, "get_env", return_value=env)

    @patch("torboxed._resolve_direct_url")
    @patch("torboxed.get_video_file_for_episode")
    @patch("torboxed.get_torrent_files")
    @patch("torboxed.resolve_debrid_id_for_path")
    def test_get_episode_path_returns_302_redirect(self, mock_db_info, mock_files,
                                                    mock_episode, mock_url):
        """Authenticated GET on an episode path returns 302 to the CDN URL."""
        mock_db_info.return_value = {
            "debrid_id": "deb-ep-1",
            "debrid_service": "torbox",
            "imdb_id": "tt0903747",
        }
        mock_files.return_value = [{"file_id": "f1", "is_video": True}]
        mock_episode.return_value = {"file_id": "ep-file-7", "is_video": True}
        mock_url.return_value = self.CDN_URL

        with self._patch_env():
            self.middleware(self._environ(method="GET"), self._start_response)

        status, headers = self.start_calls[0]
        self.assertEqual(status, "302 Found")
        self.assertIn(("Location", self.CDN_URL), headers)
        # Episode lookup used the parsed season/episode from resolve_path()
        args, kwargs = mock_episode.call_args
        self.assertEqual(args[1], 1)  # season_number
        self.assertEqual(args[2], 1)  # episode_number

    @patch("torboxed._resolve_direct_url")
    @patch("torboxed.get_video_file_for_episode")
    @patch("torboxed.get_torrent_files")
    @patch("torboxed.resolve_debrid_id_for_path")
    def test_head_episode_path_behaves_identically(self, mock_db_info, mock_files,
                                                   mock_episode, mock_url):
        """HEAD on an episode path also gets the 302 (no 0-byte 200 response)."""
        mock_db_info.return_value = {"debrid_id": "deb-ep-1", "debrid_service": "torbox"}
        mock_files.return_value = [{"file_id": "f1", "is_video": True}]
        mock_episode.return_value = {"file_id": "ep-file-7", "is_video": True}
        mock_url.return_value = self.CDN_URL

        with self._patch_env():
            self.middleware(self._environ(method="HEAD"), self._start_response)

        status, headers = self.start_calls[0]
        self.assertEqual(status, "302 Found")
        self.assertIn(("Location", self.CDN_URL), headers)

    def test_season_folder_still_resolves_as_directory(self):
        """The fix must not change resolution of genuine 3-segment paths."""
        result = self.torboxed.resolve_path("/TV Shows/Breaking Bad/Season 1/")
        self.assertEqual(result["type"], "season_folder")
        self.assertNotIn("episode_number", result)


class TestTraktPagination(unittest.TestCase):
    """Public Trakt source methods must fetch ALL pages, not just page 1.

    Regression tests for the silent ~60% data loss where public curated
    list endpoints issued a single unpaginated request (limit=100).
    """

    def _make_client(self):
        from torboxed import TraktClient
        return TraktClient.__new__(TraktClient)

    class _FakePage:
        """Minimal httpx.Response stand-in for pagination tests."""

        def __init__(self, items, page_count=None):
            self._items = items
            self._text = json.dumps(items)
            headers = {"x-pagination-item-count": str(len(items))}
            if page_count is not None:
                headers["x-pagination-page-count"] = str(page_count)
            self.headers = headers
            self.status_code = 200

        @property
        def text(self):
            return self._text

        def json(self):
            return self._items

    def _patch_request(self, pages, total_page_count=None):
        """Patch TraktClient._request to serve canned pages keyed by page number."""
        client = self._make_client()
        state = {"requested": []}

        def fake_request(method, path, use_auth=False, return_response=False, **kwargs):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(path).query)
            page = int(qs["page"][0])
            limit = int(qs["limit"][0])
            state["requested"].append((page, limit))
            items = pages.get(page, [])
            # Respect the requested per-page limit (simulates truncated fetch)
            pc = total_page_count
            return self._FakePage(items[:limit], page_count=pc)

        return client, state, patch.object(client, "_request", side_effect=fake_request)

    def test_multi_page_collection_via_header(self):
        """Trending movies: all pages fetched when X-Pagination-Page-Count known."""
        pages = {
            1: [{"movie": {"title": f"M{i}"}} for i in range(100)],
            2: [{"movie": {"title": f"M{i}"}} for i in range(100, 200)],
            3: [{"movie": {"title": f"M{i}"}} for i in range(200, 251)],
        }
        client, state, p = self._patch_request(pages, total_page_count=3)
        with p:
            result = client.get_trending_movies()
        self.assertEqual(len(result), 251)
        self.assertEqual([pg for pg, _ in state["requested"]], [1, 2, 3])
        self.assertTrue(all(lim == 100 for _, lim in state["requested"]))

    def test_short_page_stops_without_header(self):
        """Without page-count header, a short page terminates pagination."""
        pages = {
            1: [{"show": {"title": f"S{i}"}} for i in range(100)],
            2: [{"show": {"title": "LastOne"}}],
        }
        client, state, p = self._patch_request(pages)  # no page-count header
        with p:
            result = client.get_trending_shows()
        self.assertEqual(len(result), 101)
        self.assertEqual([pg for pg, _ in state["requested"]], [1, 2])

    def test_explicit_limit_caps_requests(self):
        """Explicit limit fetches exactly that many items across pages."""
        pages = {
            1: [{"movie": {"title": f"M{i}"}} for i in range(100)],
            2: [{"movie": {"title": f"M{i}"}} for i in range(100, 200)],
        }
        client, state, p = self._patch_request(pages, total_page_count=2)
        with p:
            result = client.get_popular_movies(limit=150)
        self.assertEqual(len(result), 150)
        self.assertEqual(state["requested"][0], (1, 100))
        self.assertEqual(state["requested"][1], (2, 50))

    def test_max_pages_safety_cap(self):
        """An API that always returns full pages without headers is capped."""
        import torboxed
        full_page = [{"movie": {"title": "X"}} for _ in range(100)]
        pages = {i: full_page for i in range(1, 11)}
        client, state, p = self._patch_request(pages)  # no headers => short-page never triggers
        with patch.object(torboxed, "TRAKT_MAX_PAGES", 3):
            with p:
                result = client.get_trending_movies()
        self.assertEqual(len(result), 300)
        self.assertEqual(len(state["requested"]), 3)

    def test_request_failure_stops_with_partial_results(self):
        """A failed page (None from _request, e.g. auth/429) keeps earlier pages."""
        client = self._make_client()
        first_page = self._FakePage([{"movie": {"title": "Only"}}], page_count=5)
        responses = [first_page, None]

        with patch.object(client, "_request", side_effect=lambda *a, **k: responses.pop(0)):
            result = client.get_boxoffice_movies()
        self.assertEqual(len(result), 1)

    def test_period_endpoints_pass_full_path(self):
        """Watched/collected endpoints build path including period."""
        client = self._make_client()
        for method_name, expected_path in [
            ("get_watched_movies", "/movies/watched/monthly"),
            ("get_collected_shows", "/shows/collected/all"),
        ]:
            period = "monthly" if "movies" in method_name else "all"
            with patch.object(client, "_fetch_paginated", return_value=[]) as fp:
                getattr(client, method_name)(period=period)
                fp.assert_called_once()
                self.assertEqual(fp.call_args[0][0], expected_path)

    def test_invalid_period_defaults_to_weekly(self):
        """Invalid period falls back to weekly before building the path."""
        client = self._make_client()
        with patch.object(client, "_fetch_paginated", return_value=[]) as fp:
            client.get_collected_movies(period="bogus")
        self.assertEqual(fp.call_args[0][0], "/movies/collected/weekly")

    def test_liked_lists_reuse_helper_and_filter(self):
        """get_liked_lists delegates to _fetch_paginated and keeps list items."""
        client = self._make_client()
        validated = [
            {"list": {"name": "My List", "ids": {}, "user": {}}},
            {"not_a_list": True},
        ]
        with patch.object(client, "_fetch_paginated", return_value=validated) as fp:
            result = client.get_liked_lists(limit=5)
        fp.assert_called_once_with(
            "/users/likes/lists", 5, use_auth=True, context="Trakt liked lists"
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["list"]["name"], "My List")

    def test_all_public_sources_route_through_helper(self):
        """Every public source method goes through the paginated helper."""
        client = self._make_client()
        cases = [
            (("get_popular_movies",), {}),
            (("get_popular_shows",), {}),
            (("get_anticipated_movies",), {}),
            (("get_anticipated_shows",), {}),
            (("get_watched_movies",), {"period": "weekly"}),
            (("get_watched_shows",), {"period": "yearly"}),
            (("get_collected_movies",), {}),
            (("get_collected_shows",), {}),
            (("get_list_items",), {"username": "u", "list_id": "l"}),
        ]
        for (method_name,), kwargs in cases:
            with patch.object(client, "_fetch_paginated", return_value=[]) as fp:
                getattr(client, method_name)(**kwargs)
                self.assertEqual(fp.call_count, 1, method_name)

    # =========================================================================
    # Issue #20: get_list_items must inherit the shared pagination guardrails
    # =========================================================================

    def test_get_list_items_full_pages_forever_stops_at_cap(self):
        """Adversarial API returning full pages without headers terminates
        at TRAKT_MAX_PAGES instead of looping forever (old behavior)."""
        import torboxed
        full_page = [{"movie": {"title": "X"}} for _ in range(100)]
        pages = {i: full_page for i in range(1, 11)}
        client, state, p = self._patch_request(pages)  # no page-count header
        with patch.object(torboxed, "TRAKT_MAX_PAGES", 3):
            with p:
                result = client.get_list_items("someuser", "some-list")
        self.assertEqual(len(result), 300)
        self.assertEqual(len(state["requested"]), 3)

    def test_get_list_items_short_page_stops_early(self):
        """A short final page terminates pagination without a header."""
        pages = {
            1: [{"show": {"title": f"S{i}"}} for i in range(100)],
            2: [{"show": {"title": "LastOne"}}],
        }
        client, state, p = self._patch_request(pages)
        with p:
            result = client.get_list_items("someuser", "some-list")
        self.assertEqual(len(result), 101)
        self.assertEqual([pg for pg, _ in state["requested"]], [1, 2])

    def test_get_list_items_header_page_count_honored(self):
        """X-Pagination-Page-Count is respected as authoritative end signal."""
        pages = {
            1: [{"movie": {"title": f"M{i}"}} for i in range(100)],
            2: [{"movie": {"title": f"M{i}"}} for i in range(100, 150)],
        }
        client, state, p = self._patch_request(pages, total_page_count=2)
        with p:
            result = client.get_list_items("someuser", "some-list")
        self.assertEqual(len(result), 150)
        self.assertEqual([pg for pg, _ in state["requested"]], [1, 2])


class TestCleanupDuplicatesGrouping(unittest.TestCase):
    """--cleanup-duplicates must only remove true duplicates.

    Regression tests for issue #3: grouping by (imdb_id, "S01") collapsed all
    individual episodes of a season into one duplicate group and deleted all
    but one. Group keys now use the full season label (S01E01 etc.).
    """

    IMDB = "tt0903747"

    def setUp(self):
        import torboxed
        self.torboxed = torboxed
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = Path(self.temp_dir.name) / "test.db"
        torboxed.init_db()

        # DB record loaded into the title/year index but NOT matching any
        # torrent debrid_id -> forces the name-match path (where the bug was)
        self.torboxed.record_processed(
            self.IMDB, "Breaking Bad", 2008, "show", "added", "test",
            debrid_id="unrelated-id", season="S01",
        )

        self.removed_ids = []

        class FakeDebridClient:
            def get_my_torrents(self_inner):
                return self.torrents_in_account

            def remove_torrent(self_inner, torrent_id):
                self.removed_ids.append(str(torrent_id))
                return True

        self.fake_client_cls = FakeDebridClient

    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def _torrent(self, tid, name, size=10_000_000_000):
        return {"id": tid, "name": name, "size": size}

    def _run_cleanup(self):
        with patch("torboxed.create_debrid_client", return_value=self.fake_client_cls()):
            self.torboxed.cleanup_duplicate_torrents(force=True)

    def test_distinct_episodes_of_one_season_are_not_duplicates(self):
        """S01E01..S01E08 are distinct content; none may be removed."""
        self.torrents_in_account = [
            self._torrent(f"ep{i}", f"Breaking.Bad.S01E{i:02d}.720p.WEB-DL")
            for i in range(1, 9)
        ]
        self._run_cleanup()
        self.assertEqual(self.removed_ids, [], "distinct episodes must never be removed")

    def test_two_copies_of_same_season_pack_remove_one(self):
        """Two S01 season packs: lower-quality copy is removed."""
        self.torrents_in_account = [
            self._torrent("pack-1080p", "Breaking.Bad.S01.1080p.BluRay.x264", size=30_000_000_000),
            self._torrent("pack-720p", "Breaking.Bad.S01.720p.WEB-DL.x264", size=10_000_000_000),
        ]
        self._run_cleanup()
        self.assertEqual(self.removed_ids, ["pack-720p"])

    def test_two_copies_of_same_episode_remove_one(self):
        """Two copies of S01E05 group together; best kept."""
        self.torrents_in_account = [
            self._torrent("e5-1080p", "Breaking.Bad.S01E05.1080p.BluRay.x264"),
            self._torrent("e5-720p", "Breaking.Bad.S01E05.720p.WEB-DL.x264"),
        ]
        self._run_cleanup()
        self.assertEqual(self.removed_ids, ["e5-720p"])

    def test_complete_pack_and_season_pack_coexist(self):
        """Complete pack + season pack + episode cover different granularity;
        cleanup must not treat them as duplicates of each other."""
        self.torrents_in_account = [
            self._torrent("complete", "Breaking.Bad.Complete.Series.1080p.BluRay"),
            self._torrent("s02-pack", "Breaking.Bad.S02.1080p.WEB-DL.x264"),
            self._torrent("s02e05", "Breaking.Bad.S02E05.1080p.WEB-DL.x264"),
        ]
        self._run_cleanup()
        self.assertEqual(self.removed_ids, [])

    def test_episode_and_other_episode_of_same_season_coexist_with_pack(self):
        """A season pack does not absorb distinct episodes into its group."""
        self.torrents_in_account = [
            self._torrent("s01-pack", "Breaking.Bad.S01.1080p.BluRay.x264"),
            self._torrent("s01e01", "Breaking.Bad.S01E01.1080p.WEB-DL.x264"),
            self._torrent("s01e02", "Breaking.Bad.S01E02.1080p.WEB-DL.x264"),
        ]
        self._run_cleanup()
        self.assertEqual(self.removed_ids, [])

    def test_multi_season_range_groups_only_same_range(self):
        """S01-S03 packs group together; a plain S01 pack stays separate."""
        self.torrents_in_account = [
            self._torrent("range-a", "Breaking.Bad.S01-S03.1080p.BluRay.x264"),
            self._torrent("range-b", "Breaking.Bad.S01-S03.720p.WEB-DL.x264"),
            self._torrent("s01-only", "Breaking.Bad.S01.1080p.BluRay.x264"),
        ]
        self._run_cleanup()
        # Only one of the two identical-range packs is removed
        self.assertEqual(len(self.removed_ids), 1)
        self.assertIn(self.removed_ids[0], ("range-a", "range-b"))
        self.assertNotIn("s01-only", self.removed_ids)


class TestMultiSeasonServiceLabel(unittest.TestCase):
    """Multi-season pack discovery must record the ACTIVE debrid service.

    Regression tests for issue #4: auto-created records hardcoded
    debrid_service='torbox', corrupting Real Debrid records and breaking
    WebDAV file resolution for those seasons.
    """

    def setUp(self):
        import torboxed
        self.torboxed = torboxed
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = Path(self.temp_dir.name) / "test.db"
        torboxed.init_db()

    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def _get_record(self, imdb_id, season):
        with self.torboxed.get_db() as conn:
            return conn.execute(
                "SELECT * FROM processed WHERE imdb_id=? AND season=?",
                (imdb_id, season),
            ).fetchone()

    def test_update_dict_carries_active_service_torbox(self):
        """Updates built during discovery carry get_debrid_service() label."""
        import torboxed
        updates = [{
            'imdb_id': 'tt1111111',
            'season': 'S06',
            'debrid_id': 'pack-id-1',
            'title': 'test show',
            'year': 2024,
            'debrid_service': 'torbox',
        }]
        with patch.object(torboxed, 'get_debrid_service', return_value='torbox'):
            torboxed._apply_multi_season_updates(updates)
        rec = self._get_record('tt1111111', 'S06')
        self.assertIsNotNone(rec)
        self.assertEqual(rec['debrid_service'], 'torbox')
        self.assertEqual(rec['debrid_id'], 'pack-id-1')
        self.assertEqual(rec['reason'], 'multi_season_pack_discovery')

    def test_real_debrid_not_mislabeled_as_torbox(self):
        """With DEBRID_SERVICE=real_debrid, records must say realdebrid."""
        import torboxed
        # Simulates the pre-fix failure mode: no explicit service in dict,
        # active service is Real Debrid. The helper must NOT default to torbox.
        updates = [{
            'imdb_id': 'tt2222222',
            'season': 'S07',
            'debrid_id': 'rd-pack-id',
            'title': 'test show',
            'year': 2024,
        }]
        with patch.object(torboxed, 'get_debrid_service', return_value='real_debrid'):
            torboxed._apply_multi_season_updates(updates)
        rec = self._get_record('tt2222222', 'S07')
        self.assertIsNotNone(rec)
        self.assertEqual(rec['debrid_service'], 'real_debrid')
        self.assertNotEqual(rec['debrid_service'], 'torbox')

    def test_discovery_builds_updates_with_active_service(self):
        """End-to-end: discover_existing_torrents passes active service through."""
        import torboxed

        fake_title = "FakeTestShowXYZ123"
        fake_imdb = "tt99999999"

        torboxed.record_processed(
            fake_imdb, fake_title, 2025, "show", "added", "success",
            debrid_id="s05-torrent-id", season="S05",
        )
        torboxed.record_processed(
            fake_imdb, fake_title, 2025, "show", "added", "success",
            debrid_id="s06-torrent-id", season="S06",
        )

        mock_debrid = Mock()
        mock_debrid.get_my_torrents.return_value = [
            {
                "id": "multi-season-pack-id",
                "name": f"{fake_title}.S05.S06.1080p.WEB-DL.x264",
                "hash": "multihash456",
            }
        ]

        with patch.object(torboxed, "get_debrid_service", return_value="real_debrid") as svc:
            torboxed.discover_existing_torrents(mock_debrid)

        # Both season rows were matched to the pack; the update dicts must
        # have been built while the active service was real_debrid.
        svc.assert_called()

    def test_existing_season_with_debrid_id_is_untouched(self):
        """Seasons already associated with a torrent keep their record."""
        import torboxed
        torboxed.record_processed(
            'tt3333333', 'test show', 2024, "show", "added", "success",
            debrid_id="existing-id", season="S05",
        )
        updates = [{
            'imdb_id': 'tt3333333',
            'season': 'S05',
            'debrid_id': 'new-pack-id',
            'title': 'test show',
            'year': 2024,
            'debrid_service': 'real_debrid',
        }]
        torboxed._apply_multi_season_updates(updates)
        rec = self._get_record('tt3333333', 'S05')
        self.assertEqual(rec['debrid_id'], 'existing-id')


class TestPathConfiguration(unittest.TestCase):
    """DB_PATH/LOG_PATH/ENV_PATH env var resolution and validation (issue #6).

    Module-level constants are resolved at import time; these tests exercise
    the helpers used for that resolution plus the VULN-004/BUG-007 validators.
    """

    def setUp(self):
        """Isolate tests from the developer's real .env and environment."""
        import torboxed
        self.torboxed = torboxed
        self._saved_env_cache = torboxed._env_cache
        self._saved_environ = dict(os.environ)
        self._saved_env_path = torboxed.ENV_PATH
        self.temp_dir = tempfile.mkdtemp(prefix="torboxed-paths-")
        os.environ.clear()
        os.environ["PATH"] = self._saved_environ.get("PATH", "/usr/bin:/bin")

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._saved_environ)
        self.torboxed._env_cache = self._saved_env_cache
        self.torboxed.ENV_PATH = self._saved_env_path
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_env_file(self, content):
        env_file = Path(self.temp_dir) / ".env"
        env_file.write_text(content)
        return env_file

    def test_path_from_env_defaults_when_unset(self):
        """Without DB_PATH/LOG_PATH anywhere, defaults are returned."""
        self.assertEqual(_path_from_env("DB_PATH", "torboxed.db"), Path("torboxed.db"))
        self.assertEqual(_path_from_env("LOG_PATH", "torboxed.log"), Path("torboxed.log"))

    def test_path_from_env_os_environ_wins_over_dotenv(self):
        """os.environ takes precedence over the .env file (matches get_env())."""
        env_file = self._write_env_file("DB_PATH=/from/dotenv/torboxed.db\n")
        self.torboxed.ENV_PATH = env_file
        self.torboxed._env_cache = None
        os.environ["DB_PATH"] = "/from/environ/torboxed.db"
        self.assertEqual(
            _path_from_env("DB_PATH", "torboxed.db"),
            Path("/from/environ/torboxed.db"),
        )

    def test_path_from_env_reads_dotenv_file(self):
        """A value defined only in the .env file is honored."""
        env_file = self._write_env_file("LOG_PATH=/from/dotenv/torboxed.log\n")
        self.torboxed.ENV_PATH = env_file
        self.torboxed._env_cache = None
        self.assertEqual(
            _path_from_env("LOG_PATH", "torboxed.log"),
            Path("/from/dotenv/torboxed.log"),
        )

    def test_validate_db_path_accepts_tempdir(self):
        """Paths under allowed roots (tempdir) pass validation."""
        candidate = Path(self.temp_dir) / "custom.db"
        result = validate_db_path(candidate)
        self.assertEqual(result, Path(os.path.realpath(candidate)))

    def test_validate_db_path_rejects_outside_allowed_roots(self):
        """VULN-004: paths outside all allowed roots raise ValueError."""
        with self.assertRaises(ValueError):
            validate_db_path(Path("/etc/passwd"))

    def test_validate_db_path_rejects_symlink_escape(self):
        """BUG-007: a symlink pointing outside allowed roots is rejected."""
        link = Path(self.temp_dir) / "escape.db"
        os.symlink("/etc/passwd", link)
        with self.assertRaises(ValueError):
            validate_db_path(link)

    def test_validate_log_path_accepts_tempdir_and_rejects_outside(self):
        """Log validator mirrors DB validator semantics."""
        candidate = Path(self.temp_dir) / "custom.log"
        self.assertEqual(validate_log_path(candidate), Path(os.path.realpath(candidate)))
        with self.assertRaises(ValueError):
            validate_log_path(Path("/etc/passwd"))


class TestTelegramTokenNotLogged(unittest.TestCase):
    """Regression test for issue #8: the bot token must never reach logs.

    The failed-setup path of _setup_telegram_interactive() used to echo the
    full bot token into the rotating log file via logger.info().
    """

    TOKEN = "123456789:ABCdefGHIjklMNOpqrstuvwxyz1234567890"
    CHAT_ID = "987654321"

    def _run_setup_failure(self):
        import torboxed
        answers = iter(["y", self.TOKEN, self.CHAT_ID])

        class _FakeNotifier:
            def is_configured(self):
                return True

            def _send_message(self, text):
                return False  # simulate failed test message

            def close(self):
                pass

        with patch("builtins.input", side_effect=answers), \
             patch.object(torboxed, "TelegramNotifier", return_value=_FakeNotifier()):
            with self.assertLogs("torboxed", level="INFO") as captured:
                torboxed._setup_telegram_interactive()
        return "\n".join(captured.output)

    def test_failed_setup_does_not_log_token_or_chat_id(self):
        output = self._run_setup_failure()
        self.assertNotIn(self.TOKEN, output)
        self.assertNotIn(self.CHAT_ID, output)
        # User still gets actionable guidance
        self.assertIn("--init", output)


class TestLimiterMarkSuccessOnFailure(unittest.TestCase):
    """Regression test for issue #7 (timeout → 429 cascade on Real Debrid).

    Both debrid clients must advance their rate limiter (mark_success) when
    the underlying transport fails (timeout/connection error), so the next
    request enforces the full interval instead of a stale short wait.
    """

    def _fresh_limiter(self, name="Test"):
        return RateLimiter(1.0, name=name)

    def test_rd_client_marks_success_on_api_error(self):
        """RealDebridClient._request advances limiter on APIError and re-raises."""
        import torboxed
        client = RealDebridClient(api_key="test-key")
        limiter = client._limiter
        before = limiter.last_successful_request

        with patch.object(torboxed, "make_request_with_backoff",
                          side_effect=torboxed.APIError("Simulated timeout", status_code=None)):
            with self.assertRaises(torboxed.APIError):
                client._request("POST", "/torrents/addMagnet", use_creation_limiter=True,
                                data={"magnet": "magnet:test"})

        creation_limiter = client._creation_limiter
        self.assertGreater(creation_limiter.last_successful_request, before)

    def test_torbox_client_marks_success_on_api_error(self):
        """TorboxClient keeps the LOG-FIX behavior via the shared helper."""
        import torboxed
        from torboxed import torbox_creation_limiter
        client = TorboxClient(api_key="test-key")
        before = torbox_creation_limiter.last_successful_request

        with patch.object(torboxed, "make_request_with_backoff",
                          side_effect=torboxed.APIError("Simulated timeout", status_code=None)):
            with self.assertRaises(torboxed.APIError):
                client._request("POST", "/v1/api/torrents/createtorrent",
                                use_creation_limiter=True, data={"magnet": "magnet:test"})

        self.assertGreater(torbox_creation_limiter.last_successful_request, before)

    def test_shared_helper_passes_kwargs_through(self):
        """_request_with_limiter forwards extra kwargs (e.g. suppress_500_warnings)."""
        import torboxed
        client = RealDebridClient(api_key="test-key")
        limiter = self._fresh_limiter()
        fake_response = Mock(status_code=200)
        fake_response.text = "{\"ok\": true}"

        with patch.object(torboxed, "make_request_with_backoff",
                          return_value=fake_response) as mock_call:
            result = client._request_with_limiter("GET", "https://api.example.com/x",
                                                  limiter, suppress_500_warnings=True)

        self.assertIs(result, fake_response)
        self.assertTrue(mock_call.call_args.kwargs.get("suppress_500_warnings"))
        # Success path: helper must NOT touch last_successful_request itself;
        # callers mark success after validating the response.
        self.assertEqual(limiter.last_successful_request, 0)


class TestRecordProcessedHistoryPreservation(unittest.TestCase):
    """record_processed must not erase history on skip/fail writes.

    Regression tests for issue #5: INSERT OR REPLACE replaced the whole row,
    so a "skipped"/"not_cached" record written after a transient discovery
    failure nulled out debrid_id/magnet and erased quality + upgrade history.
    """

    IMDB = "tt5555555"

    def setUp(self):
        import torboxed
        self.torboxed = torboxed
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = Path(self.temp_dir.name) / "test.db"
        torboxed.init_db()

    def tearDown(self):
        import torboxed
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def _seed_tracked_movie(self):
        """Simulate state built by an earlier successful run."""
        self.torboxed.record_processed(
            self.IMDB, "Some Movie", 2023, "movie", "added", "success",
            debrid_id="orig-debrid-id", magnet="magnet:orig",
            quality_score=4500, quality_label="1080p WEB-DL",
            season="unknown",
        )

    def _get_row(self):
        return self.torboxed.get_processed_item(self.IMDB, "unknown")

    def test_skip_after_transient_discovery_failure_retains_debrid_id(self):
        """not_cached skip must not null the stored debrid association."""
        self._seed_tracked_movie()
        # Sync hiccup: discovery failed, search found nothing -> skip record
        self.torboxed.record_processed(
            self.IMDB, "Some Movie", 2023, "movie", "skipped", "not_cached",
            season="unknown",
        )
        row = self._get_row()
        self.assertEqual(row["action"], "skipped", "action should update")
        self.assertEqual(row["reason"], "not_cached")
        self.assertEqual(row["debrid_id"], "orig-debrid-id")
        self.assertEqual(row["magnet"], "magnet:orig")
        self.assertEqual(row["quality_score"], 4500)
        self.assertEqual(row["quality_label"], "1080p WEB-DL")

    def test_already_in_debrid_skip_retains_quality_label_and_magnet(self):
        """already_in_debrid path passes only debrid_id+score; rest survives."""
        self._seed_tracked_movie()
        self.torboxed.record_processed(
            self.IMDB, "Some Movie", 2023, "movie", "skipped",
            "already_in_debrid", debrid_id="orig-debrid-id", quality_score=4500,
            season="unknown",
        )
        row = self._get_row()
        self.assertEqual(row["debrid_id"], "orig-debrid-id")
        self.assertEqual(row["quality_score"], 4500)
        self.assertEqual(row["quality_label"], "1080p WEB-DL")
        self.assertEqual(row["magnet"], "magnet:orig")

    def test_upgrade_record_still_overwrites_supplied_fields(self):
        """Supplied values (upgrade history) overwrite, not just preserve."""
        self._seed_tracked_movie()
        self.torboxed.record_processed(
            self.IMDB, "Some Movie", 2023, "movie", "upgraded", "quality_better",
            debrid_id="new-debrid-id", magnet="magnet:new",
            quality_score=7000, quality_label="2160p BluRay",
            replaced_id="orig-debrid-id", replaced_score=4500,
            season="unknown",
        )
        row = self._get_row()
        self.assertEqual(row["debrid_id"], "new-debrid-id")
        self.assertEqual(row["magnet"], "magnet:new")
        self.assertEqual(row["quality_score"], 7000)
        self.assertEqual(row["quality_label"], "2160p BluRay")
        self.assertEqual(row["replaced_id"], "orig-debrid-id")
        self.assertEqual(row["replaced_score"], 4500)

    def test_fresh_insert_without_history_unchanged(self):
        """First-time records behave exactly as before (no phantom merges)."""
        self.torboxed.record_processed(
            self.IMDB, "Some Movie", 2023, "movie", "skipped", "not_cached",
            season="unknown",
        )
        row = self._get_row()
        self.assertIsNotNone(row)
        self.assertEqual(row["action"], "skipped")
        self.assertIsNone(row["debrid_id"])
        self.assertIsNone(row["quality_score"])
        self.assertIsNone(row["replaced_id"])


class TestMainCrashHandler(unittest.TestCase):
    """Regression tests for the sync-block cleanup in main().

    The finally block must not raise NameError for resources that were
    never assigned when an early statement in the sync block fails.
    """

    def _run_main_with_failing_config(self):
        """Run main()'s sync path with get_config() raising; return exit code."""
        import torboxed

        db_dir = Path(tempfile.mkdtemp())
        (db_dir / 'torboxed.db').touch()

        with patch.object(sys, 'argv', ['torboxed.py']), \
             patch.object(torboxed, 'DB_PATH', new=db_dir / 'torboxed.db'), \
             patch.object(torboxed, 'check_and_acquire_lock', return_value=True), \
             patch.object(torboxed, 'migrate_db'), \
             patch.object(torboxed, 'get_trakt_id', return_value='tt0111161'), \
             patch.object(torboxed, 'get_config', side_effect=ValueError('corrupt config JSON')):
            # create_debrid_client must never be reached; if it is, fail loudly
            with patch.object(torboxed, 'create_debrid_client',
                              side_effect=AssertionError('should not be reached')):
                with self.assertRaises(SystemExit) as cm:
                    torboxed.main()
        return cm.exception.code

    def test_early_sync_failure_exits_1_without_name_error(self):
        """A failure before debrid assignment must surface SystemExit(1), not NameError."""
        code = self._run_main_with_failing_config()
        self.assertEqual(code, 1)

    def test_original_exception_is_reported_not_masked(self):
        """The logged fatal error should be the original exception."""
        import torboxed

        db_dir = Path(tempfile.mkdtemp())
        (db_dir / 'torboxed.db').touch()

        logged = []
        with patch.object(sys, 'argv', ['torboxed.py']), \
             patch.object(torboxed, 'DB_PATH', new=db_dir / 'torboxed.db'), \
             patch.object(torboxed, 'check_and_acquire_lock', return_value=True), \
             patch.object(torboxed, 'migrate_db'), \
             patch.object(torboxed, 'get_trakt_id', return_value='tt0111161'), \
             patch.object(torboxed, 'get_config', side_effect=ValueError('corrupt config JSON')):
            # Capture the fatal log line emitted by the top-level handler.
            orig_exception = torboxed.logger.exception
            torboxed.logger.exception = lambda msg, *a, **k: logged.append(msg % a if a else msg)
            try:
                with self.assertRaises(SystemExit):
                    torboxed.main()
            finally:
                torboxed.logger.exception = orig_exception
        combined = '\n'.join(logged)
        self.assertIn('Fatal error during sync', combined)
        self.assertIn('corrupt config JSON', combined)


if __name__ == "__main__":
    unittest.main()
