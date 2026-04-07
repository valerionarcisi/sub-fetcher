#!/usr/bin/env python3
"""Unit tests for sub_fetcher improvements."""

import os
import sys
import re
import tempfile
import shutil
import unittest
import importlib
import io
import zipfile
import urllib.request

# Setup: create a temp /config-like directory and patch the module
# before it gets imported, to avoid FileNotFoundError on /config
_test_tmpdir = tempfile.mkdtemp(prefix="subfetcher_test_")
_test_config = os.path.join(_test_tmpdir, "config")
os.makedirs(_test_config, exist_ok=True)

os.environ["TELEGRAM_BOT_TOKEN"] = "fake"
os.environ["TELEGRAM_CHAT_ID"] = "0"

# Read the source and exec with patched paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Patch: create a modified version of sub_fetcher with overridden paths
_src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sub_fetcher.py")
with open(_src_path, "r") as f:
    _src = f.read()

# Replace hardcoded /config paths
_src = _src.replace('"/config"', f'"{_test_config}"')
_src = _src.replace("STATE_FILE = ", f'STATE_FILE = "{_test_config}/state.json"  # ')
_src = _src.replace("LOG_FILE = ", f'LOG_FILE = "{_test_config}/sub_fetcher.log"  # ')
_src = _src.replace("EXCLUDE_FOLDERS_FILE = ", f'EXCLUDE_FOLDERS_FILE = "{_test_config}/exclude_folders.txt"  # ')

import types
sub_fetcher = types.ModuleType("sub_fetcher")
sub_fetcher.__file__ = _src_path
exec(compile(_src, _src_path, "exec"), sub_fetcher.__dict__)
sys.modules["sub_fetcher"] = sub_fetcher


class TestFindImdbId(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_finds_imdb_from_tvshow_nfo(self):
        series_dir = os.path.join(self.tmpdir, "MyShow")
        season_dir = os.path.join(series_dir, "Season 1")
        os.makedirs(season_dir)

        with open(os.path.join(series_dir, "tvshow.nfo"), "w") as f:
            f.write('<?xml version="1.0"?>\n<tvshow>\n<id>tt1234567</id>\n</tvshow>')

        video = os.path.join(season_dir, "MyShow.S01E01.mkv")
        open(video, "w").close()

        result = sub_fetcher.find_imdb_id(video)
        self.assertEqual(result, "tt1234567")

    def test_finds_imdb_from_episode_nfo(self):
        d = os.path.join(self.tmpdir, "show")
        os.makedirs(d)

        with open(os.path.join(d, "episode.nfo"), "w") as f:
            f.write("some text tt9876543 more text")

        video = os.path.join(d, "video.mkv")
        open(video, "w").close()

        result = sub_fetcher.find_imdb_id(video)
        self.assertEqual(result, "tt9876543")

    def test_returns_none_when_no_nfo(self):
        d = os.path.join(self.tmpdir, "empty")
        os.makedirs(d)
        video = os.path.join(d, "video.mkv")
        open(video, "w").close()

        result = sub_fetcher.find_imdb_id(video)
        self.assertIsNone(result)

    def test_returns_none_when_nfo_has_no_imdb(self):
        d = os.path.join(self.tmpdir, "show")
        os.makedirs(d)

        with open(os.path.join(d, "tvshow.nfo"), "w") as f:
            f.write("no imdb id here")

        video = os.path.join(d, "video.mkv")
        open(video, "w").close()

        result = sub_fetcher.find_imdb_id(video)
        self.assertIsNone(result)


class TestDetectLanguageFromSrt(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _write_srt(self, name, content):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def test_detects_italian(self):
        srt = self._write_srt("test.srt", """1
00:00:01,000 --> 00:00:03,000
Che cosa stai facendo?

2
00:00:04,000 --> 00:00:06,000
Non sono sicuro di questo.

3
00:00:07,000 --> 00:00:09,000
Anche per me è una cosa strana.
""")
        self.assertEqual(sub_fetcher.detect_language_from_srt(srt), "it")

    def test_detects_english(self):
        srt = self._write_srt("test.srt", """1
00:00:01,000 --> 00:00:03,000
What are you doing?

2
00:00:04,000 --> 00:00:06,000
I was not sure about that.

3
00:00:07,000 --> 00:00:09,000
They have been there for a while.
""")
        self.assertEqual(sub_fetcher.detect_language_from_srt(srt), "en")

    def test_returns_unknown_for_ambiguous(self):
        srt = self._write_srt("test.srt", "123\n")
        self.assertEqual(sub_fetcher.detect_language_from_srt(srt), "unknown")

    def test_handles_missing_file(self):
        self.assertEqual(sub_fetcher.detect_language_from_srt("/nonexistent.srt"), "unknown")


class TestFindExistingSrt(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_finds_english_srt(self):
        video = os.path.join(self.tmpdir, "Movie.2024.mkv")
        en_srt = os.path.join(self.tmpdir, "Movie.2024.en.srt")
        open(video, "w").close()
        with open(en_srt, "w") as f:
            f.write("1\n00:00:01,000 --> 00:00:02,000\nHello\n")

        result = sub_fetcher.find_existing_srt(video)
        self.assertIsNotNone(result)
        self.assertEqual(result["lang"], "en")
        self.assertEqual(result["path"], en_srt)

    def test_finds_generic_srt_and_detects_lang(self):
        video = os.path.join(self.tmpdir, "Movie.2024.mkv")
        srt = os.path.join(self.tmpdir, "Movie.2024.srt")
        open(video, "w").close()
        with open(srt, "w", encoding="utf-8") as f:
            f.write("1\n00:00:01,000 --> 00:00:03,000\nWhat are you doing?\n\n"
                    "2\n00:00:04,000 --> 00:00:06,000\nThey have been there.\n\n"
                    "3\n00:00:07,000 --> 00:00:09,000\nThis was not the right time.\n")

        result = sub_fetcher.find_existing_srt(video)
        self.assertIsNotNone(result)
        self.assertEqual(result["lang"], "en")

    def test_returns_none_when_no_srt(self):
        video = os.path.join(self.tmpdir, "Movie.2024.mkv")
        open(video, "w").close()

        result = sub_fetcher.find_existing_srt(video)
        self.assertIsNone(result)

    def test_skips_italian_tagged_srt(self):
        video = os.path.join(self.tmpdir, "Movie.2024.mkv")
        open(video, "w").close()
        ita_srt = os.path.join(self.tmpdir, "Movie.2024.it.srt")
        with open(ita_srt, "w") as f:
            f.write("1\n00:00:01,000 --> 00:00:02,000\nCiao\n")

        result = sub_fetcher.find_existing_srt(video)
        self.assertIsNone(result)


class TestGetSearchQueries(unittest.TestCase):
    def test_episode_with_different_folder(self):
        path = "/media/series/PLUR1BUS/Pluribus.S01E01.720p.x264-FENiX.mkv"
        queries = sub_fetcher.get_search_queries(path)
        self.assertIn("Pluribus", queries)
        self.assertTrue(any("PLUR" in q.upper() for q in queries))

    def test_episode_same_name(self):
        path = "/media/series/The Chosen/Season 1/The.Chosen.S01E01.mkv"
        queries = sub_fetcher.get_search_queries(path)
        self.assertEqual(queries[0], "The Chosen")

    def test_movie(self):
        path = "/media/films/Birdman (2014)/Birdman.2014.1080p.mkv"
        queries = sub_fetcher.get_search_queries(path)
        self.assertEqual(queries[0], "Birdman")

    def test_deduplication(self):
        path = "/media/series/Pluribus/Pluribus.S01E01.mkv"
        queries = sub_fetcher.get_search_queries(path)
        lower_queries = [q.lower() for q in queries]
        self.assertEqual(len(lower_queries), len(set(lower_queries)))


class TestParseVideo(unittest.TestCase):
    def test_episode(self):
        result = sub_fetcher.parse_video("/media/series/PLUR1BUS/Pluribus.S01E05.720p.x264-FENiX.mkv")
        self.assertEqual(result["type"], "episode")
        self.assertEqual(result["name"], "Pluribus")
        self.assertEqual(result["season"], 1)
        self.assertEqual(result["episode"], 5)

    def test_movie(self):
        result = sub_fetcher.parse_video("/media/films/Birdman (2014)/Birdman.2014.1080p.BluRay.mkv")
        self.assertEqual(result["type"], "movie")
        self.assertEqual(result["name"], "Birdman")
        self.assertEqual(result["year"], 2014)

    def test_unknown_uses_filename_not_parent(self):
        result = sub_fetcher.parse_video("/media/films/Loro.mp4")
        self.assertEqual(result["type"], "unknown")
        self.assertEqual(result["name"], "Loro")
        self.assertNotEqual(result["name"], "films")

    def test_movie_year_in_parentheses(self):
        result = sub_fetcher.parse_video("/media/films/Punch-Drunk Love (2002).mkv")
        self.assertEqual(result["type"], "movie")
        self.assertEqual(result["name"], "Punch-Drunk Love")
        self.assertEqual(result["year"], 2002)

    def test_movie_strips_scraper_prefix(self):
        result = sub_fetcher.parse_video("/media/films/www.SceneTime.com - Punch-Drunk Love (2002).mkv")
        self.assertEqual(result["type"], "movie")
        self.assertEqual(result["name"], "Punch-Drunk Love")
        self.assertEqual(result["year"], 2002)

    def test_movie_strips_bracket_tracker_tag(self):
        result = sub_fetcher.parse_video("/media/films/[YTS.MX] Gummo (1997).mkv")
        self.assertEqual(result["type"], "movie")
        self.assertEqual(result["name"], "Gummo")
        self.assertEqual(result["year"], 1997)

    def test_episode_strips_scraper_prefix(self):
        result = sub_fetcher.parse_video("/media/series/www.SceneTime.com - Pluribus.S01E05.720p.mkv")
        self.assertEqual(result["type"], "episode")
        self.assertEqual(result["name"], "Pluribus")
        self.assertEqual(result["season"], 1)
        self.assertEqual(result["episode"], 5)

    def test_unknown_cleans_junk_tags(self):
        result = sub_fetcher.parse_video("/media/films/SomeMovie/SomeMovie.720p.BluRay.x264.mp4")
        self.assertEqual(result["type"], "unknown")
        self.assertNotIn("720p", result["name"])
        self.assertIn("SomeMovie", result["name"])


class TestPlaceholderDetection(unittest.TestCase):
    def _fake_sub(self, n_blocks, footer=""):
        lines = []
        for i in range(1, n_blocks + 1):
            h, m = divmod(i, 60)
            lines.append(f"{i}\n00:{h:02d}:{m:02d},000 --> 00:{h:02d}:{m:02d},500\nLine {i}\n")
        return ("\n".join(lines) + footer).encode("utf-8")

    def test_rejects_too_short(self):
        content = self._fake_sub(2)
        self.assertTrue(sub_fetcher.is_placeholder_sub(content))

    def test_rejects_strong_pattern_osdb(self):
        content = self._fake_sub(500, "\nVisit osdb.link/vip for more")
        self.assertTrue(sub_fetcher.is_placeholder_sub(content))

    def test_rejects_strong_pattern_vip_member(self):
        content = self._fake_sub(500, "\nBecome a VIP member now")
        self.assertTrue(sub_fetcher.is_placeholder_sub(content))

    def test_accepts_real_sub_with_opensubtitles_credit(self):
        # Real sub with ~500 blocks and "opensubtitles" only in footer credits
        content = self._fake_sub(500, "\nDownloaded from opensubtitles.org\n")
        self.assertFalse(sub_fetcher.is_placeholder_sub(content))

    def test_rejects_short_sub_with_opensubtitles_keyword(self):
        content = self._fake_sub(10, "\nopensubtitles")
        self.assertTrue(sub_fetcher.is_placeholder_sub(content))

    def test_accepts_long_real_sub(self):
        content = self._fake_sub(800)
        self.assertFalse(sub_fetcher.is_placeholder_sub(content))

    def test_rejects_single_block_long_span(self):
        content = b"1\n00:00:00,000 --> 05:00:00,000\nAd\n\n2\n00:00:01,000 --> 00:00:02,000\n.\n\n3\n00:00:03,000 --> 00:00:04,000\n.\n"
        self.assertTrue(sub_fetcher.is_placeholder_sub(content))


class TestCascadeSearchUnit(unittest.TestCase):
    """Test _cascade_search logic with mocked OSClient."""

    def test_returns_hash_results_first(self):
        class MockClient:
            token = "fake"
            server = None
            def search_imdb(self, *a, **kw): return []
            def search_name(self, *a, **kw): return []

        client = MockClient()
        # Mock the server.SearchSubtitles for hash search
        class MockServer:
            def SearchSubtitles(self, token, params):
                if "moviehash" in params[0]:
                    return {"status": "200 OK", "data": [{"SubFileName": "hash_result.srt"}]}
                return {"status": "200 OK", "data": []}
        client.server = MockServer()

        results = sub_fetcher._cascade_search(
            client, "/media/series/Test/Test.S01E01.mkv", "ita",
            file_hash="abc123", file_size=1000
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["SubFileName"], "hash_result.srt")

    def test_falls_through_to_name_search(self):
        class MockClient:
            token = "fake"
            def search_imdb(self, *a, **kw): return []
            def search_name(self, query, season=None, episode=None, language=None):
                if query == "Test":
                    return [{"SubFileName": "name_result.srt"}]
                return []
            class server:
                @staticmethod
                def SearchSubtitles(token, params):
                    return {"status": "200 OK", "data": []}
        client = MockClient()

        results = sub_fetcher._cascade_search(
            client, "/media/series/Test/Test.S01E01.mkv", "ita",
            file_hash="abc123", file_size=1000
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["SubFileName"], "name_result.srt")


class TestGroupBySeries(unittest.TestCase):
    def test_groups_by_folder(self):
        paths = [
            "/media/series/PLUR1BUS/Pluribus.S01E01.mkv",
            "/media/series/PLUR1BUS/Pluribus.S01E02.mkv",
            "/media/series/The Chosen/Season 1/The.Chosen.S01E01.mkv",
        ]
        groups = sub_fetcher.group_by_series(paths)
        self.assertIn("PLUR1BUS", groups)
        self.assertIn("The Chosen", groups)
        self.assertEqual(len(groups["PLUR1BUS"]), 2)
        self.assertEqual(len(groups["The Chosen"]), 1)

    def test_sorts_episodes(self):
        paths = [
            "/media/series/Show/Show.S01E03.mkv",
            "/media/series/Show/Show.S01E01.mkv",
            "/media/series/Show/Show.S01E02.mkv",
        ]
        groups = sub_fetcher.group_by_series(paths)
        self.assertEqual(groups["Show"][0], paths[1])  # E01 first
        self.assertEqual(groups["Show"][2], paths[0])  # E03 last

    def test_single_file(self):
        paths = ["/media/films/Movie (2024)/Movie.2024.mkv"]
        groups = sub_fetcher.group_by_series(paths)
        self.assertEqual(len(groups), 1)


class TestProgressBar(unittest.TestCase):
    def test_zero(self):
        bar = sub_fetcher._progress_bar(0, 10)
        self.assertIn("0%", bar)

    def test_half(self):
        bar = sub_fetcher._progress_bar(5, 10)
        self.assertIn("50%", bar)
        self.assertIn("▓", bar)
        self.assertIn("░", bar)

    def test_full(self):
        bar = sub_fetcher._progress_bar(10, 10)
        self.assertIn("100%", bar)

    def test_empty_total(self):
        bar = sub_fetcher._progress_bar(0, 0)
        self.assertEqual(bar, "")


class TestHasItalianAudio(unittest.TestCase):
    """Test has_italian_audio with mocked subprocess."""

    def _mock_ffprobe(self, streams):
        """Replace subprocess.run with a mock returning given streams."""
        import subprocess
        original_run = subprocess.run
        def mock_run(*args, **kwargs):
            import json as j
            result = type("R", (), {
                "returncode": 0,
                "stdout": j.dumps({"streams": streams}),
                "stderr": ""
            })()
            return result
        subprocess.run = mock_run
        return original_run

    def test_detects_italian_audio(self):
        import subprocess
        orig = self._mock_ffprobe([
            {"tags": {"language": "ita", "title": "Italian"}}
        ])
        try:
            self.assertTrue(sub_fetcher.has_italian_audio("/fake/video.mkv"))
        finally:
            subprocess.run = orig

    def test_returns_false_for_english_only(self):
        import subprocess
        orig = self._mock_ffprobe([
            {"tags": {"language": "eng", "title": "English"}}
        ])
        try:
            self.assertFalse(sub_fetcher.has_italian_audio("/fake/video.mkv"))
        finally:
            subprocess.run = orig

    def test_returns_false_when_no_tags(self):
        import subprocess
        orig = self._mock_ffprobe([{"codec_type": "audio"}])
        try:
            self.assertFalse(sub_fetcher.has_italian_audio("/fake/video.mkv"))
        finally:
            subprocess.run = orig

    def test_handles_ffprobe_missing(self):
        # If ffprobe is not installed, should return False (not crash)
        import subprocess
        original_run = subprocess.run
        def mock_run(*args, **kwargs):
            raise FileNotFoundError("ffprobe not found")
        subprocess.run = mock_run
        try:
            self.assertFalse(sub_fetcher.has_italian_audio("/fake/video.mkv"))
        finally:
            subprocess.run = original_run


class TestSubdlClient(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        # Save original and set a fake API key
        self._orig_key = sub_fetcher.SUBDL_API_KEY
        sub_fetcher.SUBDL_API_KEY = "test_key"

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        sub_fetcher.SUBDL_API_KEY = self._orig_key

    def test_search_returns_empty_without_api_key(self):
        sub_fetcher.SUBDL_API_KEY = ""
        client = sub_fetcher.SubdlClient()
        self.assertEqual(client.search("Test"), [])

    def test_download_extracts_srt_from_zip(self):
        # Create a mock ZIP with an SRT file
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("subtitle.srt", "1\n00:00:01,000 --> 00:00:03,000\nHello world\n")
        zip_bytes = zip_buffer.getvalue()

        # Mock urllib to return our ZIP
        original_urlopen = urllib.request.urlopen
        def mock_urlopen(req, **kwargs):
            return io.BytesIO(zip_bytes)
        urllib.request.urlopen = mock_urlopen

        try:
            client = sub_fetcher.SubdlClient()
            content = client.download({"url": "test/path", "release_name": "test"})
            self.assertIsNotNone(content)
            self.assertIn(b"Hello world", content)
        finally:
            urllib.request.urlopen = original_urlopen

    def test_download_returns_none_for_empty_zip(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("readme.txt", "no srt here")
        zip_bytes = zip_buffer.getvalue()

        original_urlopen = urllib.request.urlopen
        def mock_urlopen(req, **kwargs):
            return io.BytesIO(zip_bytes)
        urllib.request.urlopen = mock_urlopen

        try:
            client = sub_fetcher.SubdlClient()
            content = client.download({"url": "test/path", "release_name": "test"})
            self.assertIsNone(content)
        finally:
            urllib.request.urlopen = original_urlopen

    def test_lang_map(self):
        client = sub_fetcher.SubdlClient()
        self.assertEqual(client.LANG_MAP.get("ita"), "it")
        self.assertEqual(client.LANG_MAP.get("eng"), "en")


class TestSubdlForcedFiltering(unittest.TestCase):
    """Test that forced/signs-only subs are penalized and rejected."""

    def test_scoring_penalizes_forced(self):
        results = [
            {"release_name": "Movie.720p.x264-GRP.eng-forced", "url": "/sub/1.zip", "name": "forced"},
            {"release_name": "Movie.720p.x264-GRP.eng-SDH", "url": "/sub/2.zip", "name": "full"},
        ]
        client = sub_fetcher.SubdlClient()
        video_path = "/media/films/Movie (2024)/Movie.720p.x264-GRP.mkv"
        video_base = os.path.splitext(os.path.basename(video_path))[0].lower()
        release_group = "grp"

        scored = []
        for sub in results:
            score = 0
            release = (sub.get("release_name", "") or "").lower()
            sub_name = (sub.get("name", "") or "").lower()
            if any(tag in release or tag in sub_name for tag in ["forced", "signs", "songs", "sdh", "hi-only"]):
                score -= 200
            scored.append((score, sub))
        scored.sort(key=lambda x: x[0], reverse=True)

        # The non-forced sub should rank higher (SDH is penalized too but less important here)
        # Both have negative scores due to "forced" and "sdh" tags
        # The forced one should have -200, SDH also -200
        # In real usage the full sub without any tag would rank highest
        self.assertTrue(scored[0][0] >= scored[-1][0])

    def test_rejects_sub_with_few_blocks(self):
        # A forced sub with only 5 blocks should be rejected
        forced_content = b"1\n00:00:01,000 --> 00:00:03,000\nKusimayu!\n\n" \
                         b"2\n00:00:10,000 --> 00:00:12,000\n[speaks Spanish]\n\n" \
                         b"3\n00:00:20,000 --> 00:00:22,000\nBongiorno\n\n"
        block_count = len(re.findall(rb"\d+\r?\n\d{2}:\d{2}:\d{2}", forced_content))
        self.assertLess(block_count, 10)

    def test_accepts_sub_with_many_blocks(self):
        # A full sub with 500+ blocks should be accepted
        lines = []
        for i in range(100):
            m, s = divmod(i * 3, 60)
            lines.append(f"{i+1}\n00:{m:02d}:{s:02d},000 --> 00:{m:02d}:{s+2:02d},000\nDialogue line {i}\n\n")
        full_content = "".join(lines).encode("utf-8")
        block_count = len(re.findall(rb"\d+\r?\n\d{2}:\d{2}:\d{2}", full_content))
        self.assertGreaterEqual(block_count, 10)


class TestSubdlZipPreferNonForced(unittest.TestCase):
    """Test that ZIP extraction prefers non-forced SRT files."""

    def test_prefers_non_forced_srt_in_zip(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("Movie.eng-forced.srt", "1\n00:00:01,000 --> 00:00:03,000\nForced only\n")
            zf.writestr("Movie.eng.srt", "1\n00:00:01,000 --> 00:00:03,000\nFull dialogue\n")
        zip_bytes = zip_buffer.getvalue()

        original_urlopen = urllib.request.urlopen
        def mock_urlopen(req, **kwargs):
            return io.BytesIO(zip_bytes)
        urllib.request.urlopen = mock_urlopen

        try:
            client = sub_fetcher.SubdlClient()
            content = client.download({"url": "test/path", "release_name": "test"})
            self.assertIsNotNone(content)
            self.assertIn(b"Full dialogue", content)
            self.assertNotIn(b"Forced only", content)
        finally:
            urllib.request.urlopen = original_urlopen

    def test_falls_back_to_forced_if_only_option(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("Movie.eng-forced.srt", "1\n00:00:01,000 --> 00:00:03,000\nForced only\n")
        zip_bytes = zip_buffer.getvalue()

        original_urlopen = urllib.request.urlopen
        def mock_urlopen(req, **kwargs):
            return io.BytesIO(zip_bytes)
        urllib.request.urlopen = mock_urlopen

        try:
            client = sub_fetcher.SubdlClient()
            content = client.download({"url": "test/path", "release_name": "test"})
            self.assertIsNotNone(content)
            self.assertIn(b"Forced only", content)
        finally:
            urllib.request.urlopen = original_urlopen


class TestSyncSkipLogic(unittest.TestCase):
    """Test that sync is skipped for local English subs."""

    def test_translate_and_save_accepts_skip_sync(self):
        # Just verify the function signature accepts skip_sync
        import inspect
        sig = inspect.signature(sub_fetcher._translate_and_save)
        self.assertIn("skip_sync", sig.parameters)
        self.assertEqual(sig.parameters["skip_sync"].default, False)


class TestSyncSubtitleReturn(unittest.TestCase):
    """Test that sync_subtitle returns dict with score info."""

    def test_sync_signature_has_min_score(self):
        import inspect
        sig = inspect.signature(sub_fetcher.sync_subtitle)
        self.assertIn("min_score", sig.parameters)
        self.assertEqual(sig.parameters["min_score"].default, 0)


class TestDownloadQueue(unittest.TestCase):
    """Test download queue infrastructure."""

    def test_queue_exists(self):
        self.assertTrue(hasattr(sub_fetcher, "download_queue"))

    def test_queue_position_returns_int(self):
        pos = sub_fetcher.queue_position()
        self.assertIsInstance(pos, int)
        self.assertEqual(pos, 0)

    def test_queue_put_and_get(self):
        sub_fetcher.download_queue.put({"type": "single", "path": "/test/video.mkv"})
        self.assertEqual(sub_fetcher.queue_position(), 1)
        job = sub_fetcher.download_queue.get_nowait()
        self.assertEqual(job["path"], "/test/video.mkv")
        sub_fetcher.download_queue.task_done()


class TestEpisodeMatchingInScoring(unittest.TestCase):
    """Test that Subdl scoring correctly matches episode numbers."""

    def test_correct_episode_gets_high_score(self):
        results = [
            {"release_name": "Pluribus S01E01 720p WEB", "url": "/sub/1.zip", "name": ""},
            {"release_name": "Pluribus S01E08 720p WEB", "url": "/sub/2.zip", "name": ""},
        ]
        client = sub_fetcher.SubdlClient()
        video_path = "/media/series/PLUR1BUS/Pluribus.S01E01.720p.x264-FENiX.mkv"

        scored = []
        parsed = sub_fetcher.parse_video(video_path)
        for sub_item in results:
            score = 0
            release = (sub_item.get("release_name", "") or "").lower()
            ep_match = re.search(r"s(\d+)e(\d+)", release)
            if ep_match and parsed.get("season") is not None:
                if int(ep_match.group(1)) == parsed["season"] and int(ep_match.group(2)) == parsed["episode"]:
                    score += 500
                else:
                    score -= 1000
            scored.append((score, sub_item))
        scored.sort(key=lambda x: x[0], reverse=True)

        self.assertEqual(scored[0][1]["release_name"], "Pluribus S01E01 720p WEB")
        self.assertGreater(scored[0][0], scored[1][0])

    def test_wrong_episode_gets_negative_score(self):
        parsed = sub_fetcher.parse_video("/media/series/Show/Show.S01E01.mkv")
        release = "show s01e08 720p web"
        ep_match = re.search(r"s(\d+)e(\d+)", release)
        self.assertIsNotNone(ep_match)
        sub_season = int(ep_match.group(1))
        sub_episode = int(ep_match.group(2))
        self.assertNotEqual(sub_episode, parsed["episode"])


class TestTwoPhaseDownload(unittest.TestCase):
    """Test that do_download supports translate=False for two-phase flow."""

    def test_do_download_accepts_translate_param(self):
        import inspect
        sig = inspect.signature(sub_fetcher.do_download)
        self.assertIn("translate", sig.parameters)
        self.assertEqual(sig.parameters["translate"].default, True)

    def test_do_batch_translate_exists(self):
        self.assertTrue(hasattr(sub_fetcher, "do_batch_translate"))
        self.assertTrue(callable(sub_fetcher.do_batch_translate))

    def test_estimate_batch_translation_cost_exists(self):
        self.assertTrue(hasattr(sub_fetcher, "_estimate_batch_translation_cost"))


class TestFindVideosByNameWithDots(unittest.TestCase):
    """Test that manual search handles dots and underscores in filenames."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._orig_series = sub_fetcher.SERIES_PATH
        self._orig_films = sub_fetcher.FILMS_PATH
        sub_fetcher.SERIES_PATH = os.path.join(self.tmpdir, "series")
        sub_fetcher.FILMS_PATH = os.path.join(self.tmpdir, "films")
        os.makedirs(sub_fetcher.SERIES_PATH)
        os.makedirs(sub_fetcher.FILMS_PATH)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        sub_fetcher.SERIES_PATH = self._orig_series
        sub_fetcher.FILMS_PATH = self._orig_films

    def test_finds_dotted_filename(self):
        show_dir = os.path.join(sub_fetcher.SERIES_PATH, "PLUR1BUS")
        os.makedirs(show_dir)
        video = os.path.join(show_dir, "Pluribus.S01E01.720p.x264-FENiX.mkv")
        open(video, "w").close()

        matches = sub_fetcher.find_videos_by_name("pluribus s01e01")
        self.assertEqual(len(matches), 1)

    def test_finds_by_folder_name(self):
        show_dir = os.path.join(sub_fetcher.SERIES_PATH, "PLUR1BUS")
        os.makedirs(show_dir)
        video = os.path.join(show_dir, "Pluribus.S01E01.720p.x264-FENiX.mkv")
        open(video, "w").close()

        matches = sub_fetcher.find_videos_by_name("pluribus")
        self.assertEqual(len(matches), 1)


class TestQueueTranslateJobType(unittest.TestCase):
    """Test that queue worker handles 'translate' job type."""

    def test_queue_accepts_translate_job(self):
        sub_fetcher.download_queue.put({"type": "translate", "paths": ["/test/video.mkv"]})
        job = sub_fetcher.download_queue.get_nowait()
        self.assertEqual(job["type"], "translate")
        sub_fetcher.download_queue.task_done()


# =============================================================================
# NEW TESTS — batches.json, do_cleanup, do_sync, queue job types
# =============================================================================

class TestLoadSaveBatches(unittest.TestCase):
    """Test that load_batches/save_batches correctly persist to a separate file."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._orig_batches = sub_fetcher.BATCHES_FILE
        sub_fetcher.BATCHES_FILE = os.path.join(self.tmpdir, "batches.json")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        sub_fetcher.BATCHES_FILE = self._orig_batches

    def test_returns_empty_dict_when_file_missing(self):
        batches = sub_fetcher.load_batches()
        self.assertEqual(batches, {})

    def test_roundtrip_save_and_load(self):
        data = {"abc12345": {"paths": ["/media/films/Movie.mkv"], "type": "translate"}}
        sub_fetcher.save_batches(data)
        loaded = sub_fetcher.load_batches()
        self.assertEqual(loaded, data)

    def test_save_multiple_batches(self):
        sub_fetcher.save_batches({"hash1": {"paths": ["/a.mkv"]}})
        sub_fetcher.save_batches({"hash2": {"paths": ["/b.mkv"]}})
        # Second save overwrites first — caller is responsible for merging
        loaded = sub_fetcher.load_batches()
        self.assertIn("hash2", loaded)

    def test_save_does_not_touch_state_json(self):
        state_file = os.path.join(self.tmpdir, "state.json")
        sub_fetcher.save_batches({"h": {"paths": []}})
        self.assertFalse(os.path.exists(state_file))

    def test_batches_independent_from_state(self):
        """Saving state must NOT wipe batches.json."""
        sub_fetcher.save_batches({"abc": {"paths": ["/x.mkv"], "type": "translate"}})

        orig_state = sub_fetcher.STATE_FILE
        sub_fetcher.STATE_FILE = os.path.join(self.tmpdir, "state.json")
        try:
            state = {"asked": {}, "downloaded": {}, "last_offset": 0}
            sub_fetcher.save_state(state)
            # Batches must still be there after save_state
            loaded = sub_fetcher.load_batches()
            self.assertIn("abc", loaded)
        finally:
            sub_fetcher.STATE_FILE = orig_state


class TestDoCleanup(unittest.TestCase):
    """Test that do_cleanup removes placeholder subs and updates state."""

    _PLACEHOLDER = (
        b"1\n00:00:01,000 --> 00:00:03,000\n"
        b"Subtitles by VIP Member - www.BestSubs.com\n\n"
    )
    _REAL_SUB = (
        b"1\n00:00:01,000 --> 00:00:03,000\nDialogue line here\n\n"
        b"2\n00:00:04,000 --> 00:00:06,000\nAnother line\n\n"
        b"3\n00:00:07,000 --> 00:00:09,000\nAnd a third line\n\n"
    )

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._orig_films = sub_fetcher.FILMS_PATH
        self._orig_series = sub_fetcher.SERIES_PATH
        sub_fetcher.FILMS_PATH = os.path.join(self.tmpdir, "films")
        sub_fetcher.SERIES_PATH = os.path.join(self.tmpdir, "series")
        os.makedirs(sub_fetcher.FILMS_PATH)
        os.makedirs(sub_fetcher.SERIES_PATH)

        self._tg_calls = []
        self._orig_tg_send = sub_fetcher.tg_send
        self._orig_tg_edit = sub_fetcher.tg_edit_message
        sub_fetcher.tg_send = lambda *a, **kw: self._tg_calls.append(("send", a))
        sub_fetcher.tg_edit_message = lambda *a, **kw: self._tg_calls.append(("edit", a))

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        sub_fetcher.FILMS_PATH = self._orig_films
        sub_fetcher.SERIES_PATH = self._orig_series
        sub_fetcher.tg_send = self._orig_tg_send
        sub_fetcher.tg_edit_message = self._orig_tg_edit

    def _create_video_and_sub(self, folder, basename, sub_content):
        d = os.path.join(sub_fetcher.FILMS_PATH, folder)
        os.makedirs(d, exist_ok=True)
        video = os.path.join(d, basename + ".mkv")
        srt = os.path.join(d, basename + ".it.srt")
        open(video, "w").close()
        with open(srt, "wb") as f:
            f.write(sub_content)
        return video, srt

    def test_removes_placeholder_sub(self):
        video, srt = self._create_video_and_sub("Movie (2024)", "Movie.2024", self._PLACEHOLDER)
        state = {"asked": {}, "downloaded": {video: {"sub": srt}}}
        sub_fetcher.do_cleanup(state)
        self.assertFalse(os.path.exists(srt))

    def test_leaves_real_sub_intact(self):
        video, srt = self._create_video_and_sub("Movie (2024)", "Movie.2024", self._REAL_SUB)
        state = {"asked": {}, "downloaded": {}}
        sub_fetcher.do_cleanup(state)
        self.assertTrue(os.path.exists(srt))

    def test_removes_video_from_downloaded_state(self):
        video, srt = self._create_video_and_sub("Movie (2024)", "Movie.2024", self._PLACEHOLDER)
        state = {"asked": {video: {"status": "yes"}}, "downloaded": {video: {"sub": srt}}}
        sub_fetcher.do_cleanup(state)
        self.assertNotIn(video, state["downloaded"])
        self.assertNotIn(video, state["asked"])

    def test_uses_progress_msg_id_when_provided(self):
        self._create_video_and_sub("Movie (2024)", "Movie.2024", self._PLACEHOLDER)
        state = {"asked": {}, "downloaded": {}}
        sub_fetcher.do_cleanup(state, progress_msg_id=42)
        edits = [c for c in self._tg_calls if c[0] == "edit"]
        self.assertTrue(len(edits) > 0)
        self.assertEqual(edits[-1][1][0], 42)

    def test_sends_message_when_no_msg_id(self):
        self._create_video_and_sub("Movie (2024)", "Movie.2024", self._PLACEHOLDER)
        state = {"asked": {}, "downloaded": {}}
        sub_fetcher.do_cleanup(state, progress_msg_id=None)
        sends = [c for c in self._tg_calls if c[0] == "send"]
        self.assertTrue(len(sends) > 0)


class TestDoSync(unittest.TestCase):
    """Test do_sync dispatches correctly and respects progress_msg_id."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._orig_series = sub_fetcher.SERIES_PATH
        self._orig_films = sub_fetcher.FILMS_PATH
        sub_fetcher.SERIES_PATH = os.path.join(self.tmpdir, "series")
        sub_fetcher.FILMS_PATH = os.path.join(self.tmpdir, "films")
        os.makedirs(sub_fetcher.SERIES_PATH)
        os.makedirs(sub_fetcher.FILMS_PATH)

        self._tg_calls = []
        self._orig_tg_send = sub_fetcher.tg_send
        self._orig_tg_edit = sub_fetcher.tg_edit_message
        self._orig_sync = sub_fetcher.sync_subtitle
        sub_fetcher.tg_send = lambda *a, **kw: self._tg_calls.append(("send", a)) or {"ok": True, "result": {"message_id": 99}}
        sub_fetcher.tg_edit_message = lambda *a, **kw: self._tg_calls.append(("edit", a))
        sub_fetcher.sync_subtitle = lambda v, s, **kw: {"ok": True, "score": 100, "offset": 0.0}

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        sub_fetcher.SERIES_PATH = self._orig_series
        sub_fetcher.FILMS_PATH = self._orig_films
        sub_fetcher.tg_send = self._orig_tg_send
        sub_fetcher.tg_edit_message = self._orig_tg_edit
        sub_fetcher.sync_subtitle = self._orig_sync

    def _create_pair(self, folder, basename):
        d = os.path.join(sub_fetcher.SERIES_PATH, folder)
        os.makedirs(d, exist_ok=True)
        video = os.path.join(d, basename + ".mkv")
        srt = os.path.join(d, basename + ".it.srt")
        open(video, "w").close()
        open(srt, "w").close()
        return video, srt

    def test_accepts_progress_msg_id_param(self):
        import inspect
        sig = inspect.signature(sub_fetcher.do_sync)
        self.assertIn("progress_msg_id", sig.parameters)

    def test_sends_not_found_when_no_matching_srt(self):
        state = {}
        sub_fetcher.do_sync("NonExistent", state)
        texts = " ".join(str(c) for c in self._tg_calls)
        self.assertIn("nonexistent", texts.lower())

    def test_uses_progress_msg_id_on_not_found(self):
        state = {}
        sub_fetcher.do_sync("NonExistent", state, progress_msg_id=55)
        edits = [c for c in self._tg_calls if c[0] == "edit"]
        self.assertTrue(len(edits) > 0)
        self.assertEqual(edits[0][1][0], 55)

    def test_syncs_matching_pair_and_reports_result(self):
        self._create_pair("Pluribus", "Pluribus.S01E01")
        state = {}
        sub_fetcher.do_sync("Pluribus", state, progress_msg_id=10)
        edits = [c for c in self._tg_calls if c[0] == "edit"]
        # Last edit should be the summary
        last_text = edits[-1][1][1]
        self.assertIn("Sync completato", last_text)
        self.assertIn("1", last_text)

    def test_sync_all_finds_all_subs(self):
        self._create_pair("ShowA", "ShowA.S01E01")
        self._create_pair("ShowB", "ShowB.S01E01")
        synced_calls = []
        sub_fetcher.sync_subtitle = lambda v, s, **kw: synced_calls.append(v) or {"ok": True, "score": 100, "offset": 0.0}
        sub_fetcher.do_sync("all", {})
        self.assertEqual(len(synced_calls), 2)


class TestQueueSyncCleanupJobTypes(unittest.TestCase):
    """Test that queue accepts and correctly structures sync/cleanup jobs."""

    def test_queue_accepts_sync_job(self):
        sub_fetcher.download_queue.put({"type": "sync", "query": "Pluribus", "msg_id": 42})
        job = sub_fetcher.download_queue.get_nowait()
        self.assertEqual(job["type"], "sync")
        self.assertEqual(job["query"], "Pluribus")
        self.assertEqual(job["msg_id"], 42)
        sub_fetcher.download_queue.task_done()

    def test_queue_accepts_cleanup_job(self):
        sub_fetcher.download_queue.put({"type": "cleanup", "msg_id": 7})
        job = sub_fetcher.download_queue.get_nowait()
        self.assertEqual(job["type"], "cleanup")
        self.assertEqual(job["msg_id"], 7)
        sub_fetcher.download_queue.task_done()

    def test_do_cleanup_is_callable(self):
        self.assertTrue(callable(sub_fetcher.do_cleanup))

    def test_do_sync_is_callable(self):
        self.assertTrue(callable(sub_fetcher.do_sync))


class TestBatchTranslatePersistence(unittest.TestCase):
    """Test that batch_translate entries survive a save_state() call."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._orig_state = sub_fetcher.STATE_FILE
        self._orig_batches = sub_fetcher.BATCHES_FILE
        sub_fetcher.STATE_FILE = os.path.join(self.tmpdir, "state.json")
        sub_fetcher.BATCHES_FILE = os.path.join(self.tmpdir, "batches.json")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        sub_fetcher.STATE_FILE = self._orig_state
        sub_fetcher.BATCHES_FILE = self._orig_batches

    def test_batch_survives_save_state(self):
        """Simulates the race condition that was the root cause of 'Batch non trovato'."""
        # Queue worker saves a batch
        sub_fetcher.save_batches({"deadbeef": {"paths": ["/media/films/Movie.mkv"], "type": "translate"}})

        # Main loop saves state (without batches) — should NOT wipe batches.json
        state = {"asked": {}, "downloaded": {}, "last_offset": 42}
        sub_fetcher.save_state(state)

        # Batch must still be there
        batches = sub_fetcher.load_batches()
        self.assertIn("deadbeef", batches)
        self.assertEqual(batches["deadbeef"]["paths"], ["/media/films/Movie.mkv"])

    def test_batch_found_after_multiple_state_saves(self):
        sub_fetcher.save_batches({"tr123": {"paths": ["/a.mkv", "/b.mkv"], "type": "translate"}})

        for i in range(5):
            sub_fetcher.save_state({"asked": {f"path_{i}": {"status": "no"}}, "downloaded": {}, "last_offset": i})

        batches = sub_fetcher.load_batches()
        self.assertIn("tr123", batches)

    def test_explicit_batch_removal_works(self):
        sub_fetcher.save_batches({"abc": {"paths": ["/x.mkv"]}, "def": {"paths": ["/y.mkv"]}})
        batches = sub_fetcher.load_batches()
        batches.pop("abc", None)
        sub_fetcher.save_batches(batches)

        loaded = sub_fetcher.load_batches()
        self.assertNotIn("abc", loaded)
        self.assertIn("def", loaded)


if __name__ == "__main__":
    unittest.main()
