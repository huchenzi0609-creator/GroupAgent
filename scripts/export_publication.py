"""Export the exact Git index for the GitHub connector, without staging or pushing.

Usage (root owns staging and publication):
  python scripts/export_publication.py export --repository owner/GroupAgent \
      --out runtime/publication/frozen-01 [--allow-env-example]
  python scripts/export_publication.py verify --bundle runtime/publication/frozen-01 \
      --expected-tree TREE_SHA --repository owner/GroupAgent
  python scripts/export_publication.py read-fragment --bundle runtime/publication/frozen-01 \
      --expected-tree TREE_SHA --repository owner/GroupAgent --payload tree --part 0

The real index and refs are untouched. `write-tree` runs with a copied index and
does write ordinary, unreferenced tree objects to this repository's object store,
so a subsequently imported exact remote commit can reference them locally.
No network, Git credentials, working-tree content, filter, hook or global Git
configuration is used. Text is inline `create_tree.content`; binary requires an
explicit reviewed blob SHA and is exported as `create_blob` base64 requests.

Fragments are small JSON envelopes. Concatenate their `data` in part order,
JSON.parse once, and call only the corresponding connector. Do not print contents
from functions.exec. Upload blobs first and require each returned SHA to match;
require the returned tree SHA to equal index_tree_sha before creating any commit.
This module never creates commits/refs or chooses a remote parent.

Gitleaks scans a temporary directory made from these exact blobs. Path/magic/group
ID guards supplement it; neither signatures nor Gitleaks prove arbitrary prose
contains no private conversations. Root must review the staged snapshot. Signed
commits/unknown headers need exact raw reconstruction, not guessed metadata.

Primary contracts checked 2026-11-14:
https://git-scm.com/docs/git-write-tree
https://docs.github.com/en/rest/git/trees#create-a-tree
https://docs.github.com/en/rest/git/blobs#create-a-blob
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any

MAX_FRAGMENT = 20_000
MAX_FILE = 16 * 1024 * 1024
MAX_TOTAL = 64 * 1024 * 1024
ROOTS = {"src", "tests", "docs", "scripts", "deploy", ".github"}
TOP_FILES = {
    "README.md",
    "AGENTS.md",
    "pyproject.toml",
    "uv.lock",
    ".gitignore",
    ".dockerignore",
    ".gitleaks.toml",
    ".gitattributes",
    "LICENSE",
    "LICENSE.md",
    "LICENSE.txt",
    "NOTICE",
    "NOTICE.md",
    "COPYING",
    ".env.example",
}
PRIVATE_PARTS = {
    ".git",
    ".venv",
    ".cache",
    "__pycache__",
    "node_modules",
    ".ssh",
    ".aws",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".hg",
    ".svn",
}
PRIVATE_SUFFIXES = {
    ".sqlite",
    ".sqlite3",
    ".db",
    ".dump",
    ".bak",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".log",
    ".jsonl",
    ".pyc",
    ".pyo",
    ".env",
}
PRIVATE_NAMES = {
    ".ds_store",
    "id_rsa",
    "id_ed25519",
    "credentials",
    "credentials.json",
    "messages.json",
    "members.json",
    "groups.json",
    "group.json",
    "events.json",
    "dsn.txt",
}
SHA = re.compile(r"[a-f0-9]{40}\Z")
GROUP_IDS = re.compile(
    rb"(?<![A-Za-z0-9_])(?:[0-9]{5,}@chatroom|wxid_[A-Za-z0-9_]{6,})(?![A-Za-z0-9_])"
)


class ExportError(Exception):
    def __init__(self, category: str, path: str | None = None, *, object_sha: str | None = None):
        self.category = category
        self.path_hash = hashlib.sha256(path.encode()).hexdigest() if path else None
        self.object_sha = object_sha
        super().__init__(category)

    def report(self) -> dict[str, Any]:
        # A filename itself may contain sensitive text; rejected paths are hashed.
        return {
            "error": self.category,
            "path_sha256": self.path_hash,
            "object_sha": self.object_sha,
        }


def clean_env() -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", os.defpath),
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_LAZY_FETCH": "1",
    }


def git(repo: Path, *args: str, index: Path | None = None) -> bytes:
    env = clean_env()
    if index is not None:
        env["GIT_INDEX_FILE"] = str(index)
    result = subprocess.run(
        [
            "git",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=" + os.devnull,
            "-c",
            "protocol.allow=never",
            "-C",
            str(repo),
            *args,
        ],
        env=env,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise ExportError("git_" + args[0].replace("-", "_") + "_failed")
    return result.stdout


def regular_bytes(path: Path, *, maximum: int = MAX_TOTAL) -> bytes:
    if path.absolute() != path.resolve():
        raise ExportError("redirected_local_path")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > maximum:
                raise ExportError("not_private_regular_file")
            value = source.read(maximum + 1)
            if len(value) > maximum:
                raise ExportError("file_limit")
            return value
    except OSError:
        raise ExportError("local_read_failed") from None


def blob_sha(value: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(value)).encode() + b"\0" + value).hexdigest()


def tree_sha(entries: list[dict[str, Any]]) -> str:
    root: dict[str, Any] = {}
    for entry in entries:
        parts = entry["path"].split("/")
        node = root
        for part in parts[:-1]:
            child = node.setdefault(part, {})
            if not isinstance(child, dict):
                raise ExportError("file_directory_conflict")
            node = child
        if parts[-1] in node:
            raise ExportError("duplicate_index_path")
        node[parts[-1]] = (entry["mode"], entry["sha"])

    def digest(node: dict[str, Any]) -> str:
        data = bytearray()
        for name, value in sorted(
            node.items(),
            key=lambda item: item[0].encode() + (b"/" if isinstance(item[1], dict) else b""),
        ):
            mode, oid = ("40000", digest(value)) if isinstance(value, dict) else value
            data += mode.encode() + b" " + name.encode() + b"\0" + bytes.fromhex(oid)
        return hashlib.sha1(b"tree " + str(len(data)).encode() + b"\0" + data).hexdigest()

    return digest(root)


def check_path(path: str, *, allow_env_example: bool = False) -> None:
    parts = path.split("/")
    name = parts[-1].casefold()
    if (
        not path
        or path.startswith("/")
        or "\\" in path
        or any(ord(c) < 32 or ord(c) == 127 for c in path)
        or any(
            part in {"", ".", ".."}
            or part.casefold() in PRIVATE_PARTS
            or part.endswith(".egg-info")
            for part in parts
        )
        or len(path.encode()) > 4096
    ):
        raise ExportError("unsafe_index_path", path)
    if len(parts) == 1:
        if path not in TOP_FILES:
            raise ExportError("unapproved_publication_root", path)
    elif parts[0] not in ROOTS:
        raise ExportError("private_or_unapproved_root", path)
    if name == ".env.example" and path == ".env.example" and allow_env_example:
        return
    if (
        name == ".env"
        or name.startswith(".env.")
        or name.startswith("config.local.")
        or name in PRIVATE_NAMES
        or PurePosixPath(name).suffix in PRIVATE_SUFFIXES
        or re.search(r"\.(?:db|sqlite3?|sql|dump)(?:-|\.(?:gz|zip|zst))", name)
        or (name.endswith(".sql") and not path.startswith("src/groupagent/persistence/migrations/"))
        or name.endswith(".draft")
    ):
        raise ExportError("private_runtime_or_credential_file", path)


def check_content(path: str, value: bytes) -> None:
    if value.startswith((b"SQLite format 3\0", b"PGDMP")) or re.search(
        rb"(?m)^-- PostgreSQL database dump(?:\r?\n|$)", value[:4096]
    ):
        raise ExportError("database_payload", path)
    if GROUP_IDS.search(value):
        raise ExportError("group_identifier_payload", path)
    if path == ".env.example":
        try:
            for line in value.decode("utf-8").splitlines():
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                key, separator, raw = line.partition("=")
                if not separator or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key.strip()):
                    raise ValueError("template syntax")
                placeholder = raw.strip().strip("\"'")
                if placeholder and not re.fullmatch(
                    r"(?:CONFIGURE|REPLACE|YOUR|CHANGE_ME)(?:[_A-Z0-9]*)|<(?:CONFIGURE|REPLACE|YOUR|CHANGE_ME)[_ A-Z0-9]*>",
                    placeholder,
                ):
                    raise ValueError("template value")
        except (UnicodeError, ValueError):
            raise ExportError("non_placeholder_env_template", path) from None


def scan_exact_snapshot(root: Path) -> dict[str, Any]:
    executable = shutil.which("gitleaks")
    if executable is None:
        raise ExportError("gitleaks_required")
    report = root.parent / "gitleaks-report.json"
    arguments = [
        executable,
        "dir",
        str(root),
        "--redact",
        "--no-banner",
        "--no-color",
        "--report-format",
        "json",
        "--report-path",
        str(report),
    ]
    config = root / ".gitleaks.toml"
    if config.is_file():
        arguments.extend(["--config", str(config)])
    scanned = subprocess.run(
        arguments,
        cwd=root,
        env=clean_env(),
        capture_output=True,
        check=False,
        timeout=120,
    )
    if scanned.returncode:
        raise ExportError("staged_secret_scan_rejected")
    version = (
        subprocess.run(
            [executable, "version"],
            env=clean_env(),
            capture_output=True,
            check=False,
            timeout=10,
        )
        .stdout.decode()
        .strip()
    )
    return {
        "tool": "gitleaks",
        "version": version,
        "scope": "exact staged blob snapshot",
        "exit_code": 0,
    }


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def write_private(path: Path, value: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as target:
        target.write(value)


def fragments(destination: Path, identifier: str, payload: dict, limit: int) -> dict[str, Any]:
    raw, offset = json_text(payload), 0
    parts: list[dict[str, Any]] = []
    directory = destination / identifier
    directory.mkdir(mode=0o700)
    while offset < len(raw):
        sequence = len(parts)
        lower, upper = 1, min(limit, len(raw) - offset)
        while lower < upper:
            middle = (lower + upper + 1) // 2
            size = (
                len(
                    json_text(
                        {
                            "payload_id": identifier,
                            "part": sequence,
                            "data": raw[offset : offset + middle],
                        }
                    )
                )
                + 1
            )
            if size <= limit:
                lower = middle
            else:
                upper = middle - 1
        envelope = (
            json_text(
                {"payload_id": identifier, "part": sequence, "data": raw[offset : offset + lower]}
            )
            + "\n"
        )
        if len(envelope) > limit:
            raise ExportError("fragment_limit")
        name = f"{identifier}/{sequence:06d}.json"
        write_private(destination / name, envelope.encode())
        parts.append(
            {
                "path": name,
                "sha256": hashlib.sha256(envelope.encode()).hexdigest(),
                "chars": len(envelope),
            }
        )
        offset += lower
    return {
        "id": identifier,
        "payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "parts": parts,
    }


def export_index(
    repo: Path,
    destination: Path,
    repository: str,
    *,
    chunk_chars: int = MAX_FRAGMENT,
    allow_env_example: bool = False,
    allowed_binary_shas: frozenset[str] = frozenset(),
    scanner: Callable[[Path], dict[str, Any]] = scan_exact_snapshot,
) -> dict[str, Any]:
    if (
        not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)
        or not 512 <= chunk_chars <= MAX_FRAGMENT
    ):
        raise ExportError("invalid_export_parameters")
    if any(not SHA.fullmatch(value) for value in allowed_binary_shas):
        raise ExportError("invalid_binary_review_sha")
    repo = Path(git(repo.absolute(), "rev-parse", "--show-toplevel").decode().strip()).resolve()
    if git(repo, "rev-parse", "--show-object-format").strip() != b"sha1":
        raise ExportError("github_requires_sha1_repository")
    alternates = Path(
        git(repo, "rev-parse", "--git-path", "objects/info/alternates").decode().strip()
    )
    if not alternates.is_absolute():
        alternates = repo / alternates
    if alternates.exists() or alternates.is_symlink():
        raise ExportError("external_object_alternates_not_supported")
    index_path = Path(git(repo, "rev-parse", "--git-path", "index").decode().strip())
    if not index_path.is_absolute():
        index_path = repo / index_path
    original_index = regular_bytes(index_path)
    destination = destination.absolute()
    if destination != destination.resolve() or destination.exists():
        raise ExportError("output_must_be_new_canonical_directory")
    if destination.is_relative_to(repo):
        if destination.relative_to(repo).parts[0] != "runtime":
            raise ExportError("output_inside_repository_must_be_under_runtime")
    manifest: dict[str, Any] = {
        "schema_version": "groupagent.publication.v1",
        "repository_full_name": repository,
        "index_sha256": hashlib.sha256(original_index).hexdigest(),
        "chunk_char_limit": chunk_chars,
        "files": [],
        "payloads": [],
        "upload_order": [],
        "limits": [
            "Only staged blobs were read, not working-tree modifications or untracked files",
            "Path, group-ID signatures and Gitleaks are not a complete private-conversation/PII detector",
            "Reviewed staged policy and explicit binary approval remain required",
            "Do not create a commit until every blob and remote tree SHA matches this export",
        ],
    }
    entries, elements, binaries = [], [], {}
    with tempfile.TemporaryDirectory(prefix="groupagent-index-export-") as temporary:
        scratch = Path(temporary)
        copied_index = scratch / "index"
        write_private(copied_index, original_index)
        raw_entries = git(repo, "ls-files", "--stage", "-z", index=copied_index)
        for record in raw_entries.split(b"\0"):
            if not record:
                continue
            try:
                metadata, raw_path = record.split(b"\t", 1)
                mode, oid, stage = metadata.decode("ascii").split()
                path = raw_path.decode("utf-8")
            except (ValueError, UnicodeError):
                raise ExportError("unrepresentable_index_entry") from None
            if stage != "0" or mode not in {"100644", "100755"} or not SHA.fullmatch(oid):
                raise ExportError("unmerged_symlink_gitlink_or_unsupported_mode", path)
            check_path(path, allow_env_example=allow_env_example)
            entries.append({"path": path, "mode": mode, "sha": oid})
        if not entries:
            raise ExportError("empty_publication_tree")
        written_tree = git(repo, "write-tree", index=copied_index).decode().strip()
        if tree_sha(entries) != written_tree:
            raise ExportError("index_tree_mismatch_or_intent_to_add")
        manifest["index_tree_sha"] = written_tree
        try:
            head_tree = git(repo, "rev-parse", "--verify", "HEAD^{tree}").decode().strip()
        except ExportError:
            head_tree = None
        manifest["index_matches_head"] = written_tree == head_tree if head_tree else None
        snapshot = scratch / "snapshot"
        snapshot.mkdir(mode=0o700)
        total = 0
        for entry in entries:
            path, oid = entry["path"], entry["sha"]
            if int(git(repo, "cat-file", "-s", oid).strip()) > MAX_FILE:
                raise ExportError("staged_blob_size_limit", path)
            if git(repo, "cat-file", "-t", oid).strip() != b"blob":
                raise ExportError("index_entry_is_not_blob", path)
            value = git(repo, "cat-file", "blob", oid)
            total += len(value)
            if total > MAX_TOTAL or blob_sha(value) != oid:
                raise ExportError("blob_hash_or_total_size_mismatch", path)
            check_content(path, value)
            target = snapshot / path
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            write_private(target, value)
            item = {**entry, "type": "blob"}
            try:
                text = value.decode("utf-8")
                if "\0" in text:
                    raise ValueError("binary NUL")
            except (UnicodeError, ValueError):
                if oid not in allowed_binary_shas:
                    raise ExportError("binary_review_required", path, object_sha=oid) from None
                binaries[oid] = value
                treatment = "base64_blob"
            else:
                assert text.encode("utf-8") == value
                item.pop("sha")
                item["content"] = text
                treatment = "inline_utf8"
            elements.append(item)
            manifest["files"].append(
                {
                    **entry,
                    "bytes": len(value),
                    "sha256": hashlib.sha256(value).hexdigest(),
                    "treatment": treatment,
                }
            )
        manifest["scanner"] = scanner(snapshot)
        if manifest["scanner"].get("exit_code") != 0:
            raise ExportError("staged_secret_scan_rejected")
        if regular_bytes(index_path) != original_index:
            raise ExportError("real_index_changed_during_export")
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if destination.parent != destination.parent.resolve():
            raise ExportError("redirected_output_parent")
        destination.mkdir(mode=0o700)
        for oid, value in sorted(binaries.items()):
            identifier = "blob-" + oid
            manifest["payloads"].append(
                {
                    **fragments(
                        destination,
                        identifier,
                        {
                            "repository_full_name": repository,
                            "encoding": "base64",
                            "content": base64.b64encode(value).decode(),
                        },
                        chunk_chars,
                    ),
                    "connector": "create_blob",
                    "expected_sha": oid,
                }
            )
            manifest["upload_order"].append(identifier)
        manifest["payloads"].append(
            {
                **fragments(
                    destination,
                    "tree",
                    {
                        "repository_full_name": repository,
                        "base_tree_sha": None,
                        "tree_elements": elements,
                    },
                    chunk_chars,
                ),
                "connector": "create_tree",
                "expected_sha": written_tree,
            }
        )
        manifest["upload_order"].append("tree")
        if regular_bytes(index_path) != original_index:
            write_private(destination / "INVALID", b"Index changed; never publish this bundle.\n")
            raise ExportError("real_index_changed_during_export")
        write_private(destination / "manifest.json", (json_text(manifest) + "\n").encode())
    return summary(manifest)


def summary(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "repository": manifest["repository_full_name"],
        "index_tree_sha": manifest["index_tree_sha"],
        "files": len(manifest["files"]),
        "index_matches_head": manifest["index_matches_head"],
        "binary_blobs": len(manifest["payloads"]) - 1,
        "fragments": sum(len(value["parts"]) for value in manifest["payloads"]),
        "scanner": manifest["scanner"],
    }


def load_manifest(bundle: Path, expected_tree: str, repository: str) -> dict[str, Any]:
    if not SHA.fullmatch(expected_tree) or (bundle / "INVALID").exists():
        raise ExportError("invalid_or_invalidated_bundle")
    try:
        value = json.loads(regular_bytes(bundle / "manifest.json"))
    except (ValueError, UnicodeError):
        raise ExportError("invalid_bundle_manifest") from None
    if (
        value.get("schema_version") != "groupagent.publication.v1"
        or value.get("index_tree_sha") != expected_tree
        or value.get("repository_full_name") != repository
    ):
        raise ExportError("unexpected_export_tree")
    return value


def read_fragment(
    bundle: Path, manifest: dict[str, Any], identifier: str, part: int
) -> dict[str, Any]:
    payload = next((item for item in manifest["payloads"] if item["id"] == identifier), None)
    if payload is None or not 0 <= part < len(payload["parts"]):
        raise ExportError("fragment_not_found")
    metadata = payload["parts"][part]
    expected_path = f"{identifier}/{part:06d}.json"
    if not re.fullmatch(r"tree|blob-[a-f0-9]{40}", identifier) or metadata["path"] != expected_path:
        raise ExportError("unsafe_fragment_path")
    raw = regular_bytes(bundle / expected_path, maximum=MAX_FRAGMENT)
    if len(raw) != metadata["chars"] or hashlib.sha256(raw).hexdigest() != metadata["sha256"]:
        raise ExportError("fragment_hash_mismatch")
    value = json.loads(raw)
    if (
        set(value) != {"payload_id", "part", "data"}
        or value["payload_id"] != identifier
        or value["part"] != part
        or not isinstance(value["data"], str)
    ):
        raise ExportError("fragment_identity_mismatch")
    return value


def verify_bundle(bundle: Path, expected_tree: str, repository: str) -> dict[str, Any]:
    manifest = load_manifest(bundle, expected_tree, repository)
    payloads = {}
    for description in manifest["payloads"]:
        identifier = description["id"]
        if identifier in payloads:
            raise ExportError("duplicate_payload_identifier")
        raw = "".join(
            read_fragment(bundle, manifest, identifier, part)["data"]
            for part in range(len(description["parts"]))
        )
        if hashlib.sha256(raw.encode()).hexdigest() != description["payload_sha256"]:
            raise ExportError("payload_hash_mismatch")
        payloads[identifier] = json.loads(raw)
        if payloads[identifier].get("repository_full_name") != manifest["repository_full_name"]:
            raise ExportError("payload_repository_mismatch")
        if identifier == "tree":
            expected_kind, expected_sha = "create_tree", expected_tree
            keys = {"repository_full_name", "base_tree_sha", "tree_elements"}
        elif re.fullmatch(r"blob-[a-f0-9]{40}", identifier):
            expected_kind, expected_sha = "create_blob", identifier[5:]
            keys = {"repository_full_name", "encoding", "content"}
        else:
            raise ExportError("unexpected_payload_identifier")
        if (
            description["connector"] != expected_kind
            or description["expected_sha"] != expected_sha
            or set(payloads[identifier]) != keys
        ):
            raise ExportError("unexpected_connector_payload")
    tree = payloads["tree"]
    if tree["base_tree_sha"] is not None:
        raise ExportError("unexpected_remote_base_tree")
    entries, observed = [], {}
    referenced = {"tree"}
    for entry in tree["tree_elements"]:
        if entry.get("type") != "blob" or entry.get("mode") not in {"100644", "100755"}:
            raise ExportError("unsupported_tree_mode")
        check_path(entry["path"], allow_env_example=True)
        if "content" in entry and "sha" not in entry:
            if set(entry) != {"path", "mode", "type", "content"}:
                raise ExportError("unexpected_tree_fields")
            value = entry["content"].encode("utf-8")
            oid = blob_sha(value)
        elif "sha" in entry and "content" not in entry:
            if set(entry) != {"path", "mode", "type", "sha"}:
                raise ExportError("unexpected_tree_fields")
            oid = entry["sha"]
            referenced.add("blob-" + oid)
            blob = payloads["blob-" + oid]
            if blob["encoding"] != "base64":
                raise ExportError("binary_encoding_mismatch")
            value = base64.b64decode(blob["content"], validate=True)
            if blob_sha(value) != oid:
                raise ExportError("binary_blob_mismatch")
        else:
            raise ExportError("ambiguous_tree_content")
        check_content(entry["path"], value)
        entries.append({"path": entry["path"], "mode": entry["mode"], "sha": oid})
        observed[entry["path"]] = (
            oid,
            entry["mode"],
            hashlib.sha256(value).hexdigest(),
            len(value),
        )
    declared = {
        item["path"]: (item["sha"], item["mode"], item["sha256"], item["bytes"])
        for item in manifest["files"]
    }
    order = sorted(identifier for identifier in referenced if identifier != "tree") + ["tree"]
    if (
        set(payloads) != referenced
        or manifest["upload_order"] != order
        or len(declared) != len(manifest["files"])
    ):
        raise ExportError("unreferenced_or_duplicate_publication_content")
    if observed != declared or tree_sha(entries) != expected_tree:
        raise ExportError("reconstructed_tree_mismatch")
    return summary(manifest)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export")
    export.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    export.add_argument("--repository", required=True)
    export.add_argument("--out", type=Path, required=True)
    export.add_argument("--chunk-chars", type=int, default=MAX_FRAGMENT)
    export.add_argument("--allow-env-example", action="store_true")
    export.add_argument("--allow-binary-sha", action="append", default=[])
    for command in ("verify", "read-fragment", "plan"):
        child = commands.add_parser(command)
        child.add_argument("--bundle", type=Path, required=True)
        child.add_argument("--expected-tree", required=True)
        child.add_argument("--repository", required=True)
        if command == "read-fragment":
            child.add_argument("--payload", required=True)
            child.add_argument("--part", type=int, required=True)
        if command == "plan":
            child.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()
    try:
        if args.command == "export":
            result = export_index(
                args.repo,
                args.out,
                args.repository,
                chunk_chars=args.chunk_chars,
                allow_env_example=args.allow_env_example,
                allowed_binary_shas=frozenset(args.allow_binary_sha),
            )
        elif args.command == "verify":
            result = verify_bundle(args.bundle, args.expected_tree, args.repository)
        elif args.command == "plan":
            manifest = load_manifest(args.bundle, args.expected_tree, args.repository)
            if args.offset < 0:
                raise ExportError("invalid_plan_offset")
            result = {
                "offset": args.offset,
                "total": len(manifest["payloads"]),
                "payloads": [
                    {
                        "id": value["id"],
                        "connector": value["connector"],
                        "expected_sha": value["expected_sha"],
                        "parts": len(value["parts"]),
                    }
                    for value in manifest["payloads"][args.offset : args.offset + 50]
                ],
            }
        else:
            result = read_fragment(
                args.bundle,
                load_manifest(args.bundle, args.expected_tree, args.repository),
                args.payload,
                args.part,
            )
        print(json_text(result))
        return 0
    except ExportError as error:
        print(json_text(error.report()))
        return 2
    except (ValueError, OSError, KeyError, TypeError, subprocess.TimeoutExpired):
        print(json_text({"error": "export_or_bundle_operation_failed"}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
