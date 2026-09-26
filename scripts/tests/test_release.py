"""Exercise release guards and recovery without contacting a registry or publishing."""
import importlib.util
import io
import json
import os
import subprocess
import sys
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

spec = importlib.util.spec_from_file_location("release", Path(__file__).parents[1] / "release.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseTests(unittest.TestCase):
    def test_notes_are_limited_to_the_requested_release(self):
        text = "# Changelog\n\n## Unreleased\n\n- Later\n\n## 0.1.0 - 2026-01-01\n\n- First release\n\n### Changes\n\n- Detail\n\n## 0.0.1 - 2025-01-01\n\n- Old\n"
        self.assertEqual(release.release_notes(text, "0.1.0"),
                         "- First release\n\n### Changes\n\n- Detail\n")

    def test_missing_undated_empty_duplicate_and_future_notes_are_rejected(self):
        for text in (
            "## Unreleased\n- Change",
            "## 0.1.0 — release candidate\n- Change",
            "## 0.1.0 - 2026-01-01\n\n",
            "## 0.1.0 - 2026-01-01\n- A\n## 0.1.0 - 2026-01-02\n- B",
            "## 0.1.0 - 9999-01-01\n- Future",
            "## 0.1.0 - 2026-02-30\n- Invalid date",
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                release.release_notes(text, "0.1.0")

    def make_workspace(self, root):
        (root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.1.0"\n')
        for name in release.PACKAGES:
            (root / name).mkdir()
            content = f'[package]\nname = "{name}"\nversion.workspace = true\n'
            if name == "eventful-rs":
                content += '[dependencies]\neventful-rs-macros = { version = "0.1.0" }\n'
            (root / name / "Cargo.toml").write_text(content)
        (root / "CHANGELOG.md").write_text('## 0.1.0 - 2026-01-01\n\n- First release\n')

    def test_version_and_macro_dependency_must_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_workspace(root)
            self.assertEqual(release.validate_metadata(root, "0.1.0"), "- First release\n")
            for version in ("0.2.0", "v0.1.0", "0.1.0-rc.1", "01.1.0", "$(echo nope)"):
                with self.subTest(version=version), self.assertRaises(ValueError):
                    release.validate_metadata(root, version)
            manifest = root / "eventful-rs/Cargo.toml"
            manifest.write_text(manifest.read_text().replace('version = "0.1.0"', 'version = "0.0.1"'))
            with self.assertRaisesRegex(ValueError, "macro dependency"):
                release.validate_metadata(root, "0.1.0")

    def test_check_command_on_a_committed_release_does_not_change_git(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repository"
            root.mkdir()
            self.make_workspace(root)
            (root / "scripts").mkdir()
            script = root / "scripts/release.py"
            script.write_text(Path(release.__file__).read_text())
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(["git", "-c", "user.name=Release test", "-c",
                            "user.email=release@example.invalid", "-c", "commit.gpgsign=false",
                            "commit", "-qm", "Test release"], cwd=root, check=True)
            notes = Path(directory) / "notes.md"
            result = subprocess.run([sys.executable, "-B", str(script), "check", "0.1.0",
                                     "--notes", str(notes)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(notes.read_text(), "- First release\n")
            self.assertEqual(subprocess.check_output(["git", "status", "--porcelain"], cwd=root), b"")
            self.assertEqual(subprocess.check_output(["git", "tag"], cwd=root), b"")

    def test_dirty_tree_is_rejected(self):
        with patch.object(release, "validate_metadata", return_value="notes"), \
             patch.object(release, "git", return_value=" M Cargo.toml"), \
             self.assertRaisesRegex(ValueError, "commit the release"):
            release.validate("0.1.0")

    def test_tag_on_another_commit_is_rejected(self):
        with patch.object(release, "validate_metadata", return_value="notes"), \
             patch.object(release, "git", side_effect=["", "current"]), \
             patch.object(release.subprocess, "run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = "other\n"
            with self.assertRaisesRegex(ValueError, "different commit"):
                release.validate("0.1.0")
            run.return_value.stdout = "current\n"
            with patch.object(release, "git", side_effect=["", "current"]):
                self.assertEqual(release.validate("0.1.0"), ("notes", "current"))

    def archive(self, sha, dirty=False):
        data = json.dumps({"git": {"sha1": sha, "dirty": dirty}}).encode()
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            entry = tarfile.TarInfo("eventful-rs-0.1.0/.cargo_vcs_info.json")
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
        return buffer.getvalue()

    def test_existing_release_must_have_matching_clean_provenance(self):
        metadata = b'{"version": {"yanked": false}}'
        for sha, dirty, accepted in (("current", False, True), ("other", False, False), ("current", True, False)):
            with self.subTest(sha=sha, dirty=dirty), \
                 patch.object(release, "fetch", side_effect=[metadata, self.archive(sha, dirty)]):
                if accepted:
                    self.assertTrue(release.already_published("eventful-rs", "0.1.0", "current"))
                else:
                    with self.assertRaises(ValueError):
                        release.already_published("eventful-rs", "0.1.0", "current")

    def test_only_not_found_means_unpublished(self):
        for status in (404, 403, 429, 500):
            error = urllib.error.HTTPError("url", status, "failure", {}, io.BytesIO())
            with self.subTest(status=status), patch.object(release, "fetch", side_effect=error):
                if status == 404:
                    self.assertFalse(release.already_published("eventful-rs", "0.1.0", "sha"))
                else:
                    with self.assertRaises(urllib.error.HTTPError):
                        release.already_published("eventful-rs", "0.1.0", "sha")
            error.close()

    def test_yanked_release_is_rejected(self):
        with patch.object(release, "fetch", return_value=b'{"version": {"yanked": true}}'), \
             self.assertRaisesRegex(ValueError, "yanked"):
            release.already_published("eventful-rs", "0.1.0", "sha")

    @patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": "test-only"})
    def test_publish_order_and_partial_release_retry(self):
        for existing, expected in (([False, False], list(release.PACKAGES)),
                                   ([True, False], ["eventful-rs"]), ([True, True], [])):
            with self.subTest(existing=existing), \
                 patch.object(release, "already_published", side_effect=existing), \
                 patch.object(release.subprocess, "run") as run:
                release.publish("0.1.0", "sha")
                self.assertEqual([call.args[0][3] for call in run.call_args_list], expected)
                for call in run.call_args_list:
                    self.assertIn("--locked", call.args[0])
                    self.assertTrue(call.kwargs["check"])

    @patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": "test-only"})
    def test_failed_macro_upload_stops_before_runtime_upload(self):
        failure = subprocess.CalledProcessError(101, ["cargo", "publish"])
        with patch.object(release, "already_published", return_value=False), \
             patch.object(release.subprocess, "run", side_effect=failure) as run, \
             self.assertRaises(subprocess.CalledProcessError):
            release.publish("0.1.0", "sha")
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0][3], "eventful-rs-macros")

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_credentials_stop_before_registry_access(self):
        with patch.object(release, "already_published") as registry, \
             self.assertRaisesRegex(ValueError, "CARGO_REGISTRY_TOKEN"):
            release.publish("0.1.0", "sha")
        registry.assert_not_called()

    @patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": "test-only"})
    def test_conflicting_runtime_prevents_either_upload(self):
        with patch.object(release, "already_published", side_effect=[False, ValueError("conflict")]), \
             patch.object(release.subprocess, "run") as run, self.assertRaises(ValueError):
            release.publish("0.1.0", "sha")
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
