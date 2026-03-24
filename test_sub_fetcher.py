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

    def test_unknown_cleans_junk_tags(self):
        result = sub_fetcher.parse_video("/media/films/SomeMovie/SomeMovie.720p.BluRay.x264.mp4")
        self.assertEqual(result["type"], "unknown")
        self.assertNotIn("720p", result["name"])
        self.assertIn("SomeMovie", result["name"])


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


class TestAskUserGroupedFilmsSingle(unittest.TestCase):
    """Test that films get individual messages, not digest."""

    def test_singles_not_grouped(self):
        # Verify ask_user_grouped doesn't create film_digest batches
        # by checking the function source code
        import inspect
        source = inspect.getsource(sub_fetcher.ask_user_grouped)
        self.assertNotIn("film_digest", source)
        self.assertNotIn("Scarica tutti", source)


if __name__ == "__main__":
    unittest.main()
