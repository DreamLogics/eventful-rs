#!/usr/bin/env python3
"""Validate release metadata and publish the two crates from one committed revision."""
import argparse
import datetime
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tomllib
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
PACKAGES = ("eventful-rs-macros", "eventful-rs")


def git(*args):
    """Read Git state without shell interpolation."""
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def release_notes(changelog, version):
    """Require one dated, nonempty curated entry for the exact stable version."""
    headers = list(re.finditer(r"^## (.+)$", changelog, re.MULTILINE))
    candidates = [i for i, h in enumerate(headers)
                  if re.match(rf"\[?{re.escape(version)}\]?(?:\s|$)", h[1])]
    if len(candidates) != 1:
        raise ValueError(f"expected exactly one changelog entry for {version}")
    index = candidates[0]
    heading = headers[index]
    match = re.fullmatch(rf"{re.escape(version)} - (\d{{4}}-\d{{2}}-\d{{2}})", heading[1])
    if not match:
        raise ValueError(f"use changelog heading: ## {version} - YYYY-MM-DD")
    if datetime.date.fromisoformat(match[1]) > datetime.date.today():
        raise ValueError("release date must not be in the future")
    end = headers[index + 1].start() if index + 1 < len(headers) else len(changelog)
    notes = changelog[heading.end():end].strip()
    if not re.search(r"^[-*] \S", notes, re.MULTILINE):
        raise ValueError("release notes must include at least one changelog bullet")
    return notes + "\n"


def validate_metadata(root, version):
    """Check the synchronized package versions and extract the release notes."""
    if not re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", version):
        raise ValueError("provide a stable version such as 0.1.0, without a v prefix")
    workspace = tomllib.loads((root / "Cargo.toml").read_text())
    if workspace["workspace"]["package"]["version"] != version:
        raise ValueError("requested version does not match workspace.package.version")
    for name in PACKAGES:
        manifest = tomllib.loads((root / name / "Cargo.toml").read_text())
        package = manifest["package"]
        if package["name"] != name or package["version"] != {"workspace": True}:
            raise ValueError(f"{name} must inherit the shared workspace version")
        if name == "eventful-rs":
            dependency = manifest["dependencies"]["eventful-rs-macros"]
            if dependency["version"] not in (version, "=" + version):
                raise ValueError("macro dependency must match the release version")
    return release_notes((root / "CHANGELOG.md").read_text(), version)


def validate(version):
    """Reject dirty sources or an existing release tag on a different commit."""
    notes = validate_metadata(ROOT, version)
    if git("status", "--porcelain"):
        raise ValueError("commit the release changes before running the release workflow")
    sha = git("rev-parse", "HEAD")
    tag = subprocess.run(["git", "rev-parse", "--verify", f"refs/tags/v{version}^{{commit}}"],
                         cwd=ROOT, capture_output=True, text=True)
    if tag.returncode == 0 and tag.stdout.strip() != sha:
        raise ValueError(f"v{version} already points to a different commit")
    return notes, sha


def fetch(url):
    """Fetch public registry data; transport failures must abort the release."""
    request = urllib.request.Request(url, headers={"User-Agent": "eventful-rs-release"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def already_published(name, version, sha):
    """Allow retries only when the existing crate records this exact Git commit."""
    try:
        metadata = json.loads(fetch(f"https://crates.io/api/v1/crates/{name}/{version}"))
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise
    if metadata["version"]["yanked"]:
        raise ValueError(f"{name} {version} is yanked; choose a new version")
    archive = fetch(f"https://static.crates.io/crates/{name}/{name}-{version}.crate")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as package:
        vcs_file = package.extractfile(f"{name}-{version}/.cargo_vcs_info.json")
        if vcs_file is None:
            raise ValueError(f"{name} has no Git provenance")
        vcs = json.load(vcs_file)["git"]
    if vcs["sha1"] != sha or vcs.get("dirty", False):
        raise ValueError(f"{name} {version} was published from different sources")
    return True


def publish(version, sha):
    """Publish macros before the runtime; Cargo waits for registry indexing."""
    if not os.environ.get("CARGO_REGISTRY_TOKEN"):
        raise ValueError("CARGO_REGISTRY_TOKEN is required for publishing")
    # Check both before uploading either: a conflicting existing runtime must not
    # leave a newly published macro version behind.
    existing = {name: already_published(name, version, sha) for name in PACKAGES}
    for name in PACKAGES:
        if existing[name]:
            print(f"{name} {version} already published from {sha}; skipping", flush=True)
        else:
            subprocess.run(["cargo", "publish", "-p", name, "--locked", "--registry", "crates-io"],
                           cwd=ROOT, check=True)


def main():
    """Expose validation separately so CI previews have no publishing credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "publish"))
    parser.add_argument("version")
    parser.add_argument("--notes", type=Path, help="write the curated GitHub release notes")
    args = parser.parse_args()
    try:
        notes, sha = validate(args.version)
        if args.notes:
            args.notes.write_text(notes)
        if args.command == "publish":
            publish(args.version, sha)
        print(f"Release v{args.version}: {sha}")
    except (ValueError, KeyError, OSError, tarfile.TarError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"release: {error}\n")


if __name__ == "__main__":
    main()
