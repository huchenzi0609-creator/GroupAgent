#!/usr/bin/env python3
"""Root pre-start helper: existing server credentials -> one private tmpfs file.

Install this helper root-owned and not writable by the service account. It does
not modify either source, provision accounts, mount filesystems or start a service.
After preparation the launcher drops to pot-test2; the RAM file may be removed
after that service has stopped. Never pass credential values on the command line.
"""

import argparse
import json
import os
import pwd
import stat
import sys
from pathlib import Path
from uuid import uuid4

RUNTIME_DIRECTORY = Path("/run/pot-test2")
OUTPUT_NAME = "secrets.env"
MAX_SOURCE_BYTES = 1024 * 1024


class PreparationError(Exception):
    def __init__(self, code: str):
        self.code = code


def check(condition, code: str) -> None:
    if not condition:
        raise PreparationError(code)


def private_read(path: Path) -> bytes:
    check(path.is_absolute() and path == path.resolve(strict=True), "source_path_invalid")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "source_not_private_regular")
        check(not info.st_mode & 0o077, "source_permissions_invalid")
        check(info.st_size <= MAX_SOURCE_BYTES, "source_too_large")
        with os.fdopen(fd, "rb", closefd=False) as source:
            content = source.read(MAX_SOURCE_BYTES + 1)
        check(len(content) <= MAX_SOURCE_BYTES, "source_too_large")
        return content
    finally:
        os.close(fd)


def secret_value(value) -> str:
    check(isinstance(value, str) and bool(value.strip()), "credential_value_invalid")
    check(not any(character in value for character in "\r\n\x00"), "credential_value_invalid")
    return value


def payload(config: bytes, token_file: bytes) -> bytes:
    try:
        body = json.loads(config)
        check(isinstance(body, dict), "legacy_config_invalid")
        api_key = secret_value(body.get("api_key"))
        token = token_file.decode("utf-8")
        # Native token files commonly end with exactly one line terminator.
        token = (
            token[:-2] if token.endswith("\r\n") else token[:-1] if token.endswith("\n") else token
        )
        token = secret_value(token)
        check(not any(character.isspace() for character in token), "credential_value_invalid")
        return (
            "DEEPSEEK_API_KEY=" + json.dumps(api_key, ensure_ascii=True) + "\n"
            "GROUPAGENT_CHANNEL_TOKEN=" + json.dumps(token, ensure_ascii=True) + "\n"
        ).encode("utf-8")
    except (ValueError, UnicodeError, RecursionError):
        raise PreparationError("credential_encoding_invalid") from None


def require_tmpfs(directory: Path, mountinfo: Path = Path("/proc/self/mountinfo")) -> None:
    matches = []
    for line in mountinfo.read_text(encoding="utf-8").splitlines():
        left, right = line.split(" - ", 1)
        mountpoint = Path(left.split()[4])
        try:
            directory.relative_to(mountpoint)
        except ValueError:
            continue
        matches.append((len(mountpoint.parts), right.split()[0]))
    check(bool(matches) and max(matches)[1] == "tmpfs", "runtime_not_tmpfs")


def directory_metadata(info, gid: int) -> None:
    check(
        stat.S_ISDIR(info.st_mode)
        and info.st_uid == 0
        and info.st_gid == gid
        and stat.S_IMODE(info.st_mode) == 0o750,
        "runtime_directory_permissions_invalid",
    )


def runtime_directory(gid: int) -> int:
    check(
        RUNTIME_DIRECTORY.parent == RUNTIME_DIRECTORY.parent.resolve(strict=True),
        "runtime_path_invalid",
    )
    require_tmpfs(RUNTIME_DIRECTORY)
    parent = os.open(RUNTIME_DIRECTORY.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        parent_info = os.fstat(parent)
        check(parent_info.st_uid == 0 and not parent_info.st_mode & 0o022, "runtime_parent_invalid")
        try:
            os.mkdir(RUNTIME_DIRECTORY.name, 0o750, dir_fd=parent)
            created = True
        except FileExistsError:
            created = False
        directory = os.open(
            RUNTIME_DIRECTORY.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
        )
        try:
            if created:
                os.fchown(directory, 0, gid)
                os.fchmod(directory, 0o750)
            directory_metadata(os.fstat(directory), gid)
            return directory
        except BaseException:
            os.close(directory)
            raise
    finally:
        os.close(parent)


def atomic_write(directory: int, content: bytes, uid: int, gid: int) -> None:
    try:
        old = os.stat(OUTPUT_NAME, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        old = None
    if old is not None:
        check(
            stat.S_ISREG(old.st_mode)
            and old.st_nlink == 1
            and old.st_uid == uid
            and old.st_gid == gid
            and stat.S_IMODE(old.st_mode) == 0o600,
            "existing_output_invalid",
        )
    temporary = ".secrets-" + uuid4().hex + ".pending"
    fd = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory
    )
    try:
        with os.fdopen(fd, "wb", closefd=False) as output:
            output.write(content)
            output.flush()
        os.fchown(fd, uid, gid)
        os.fchmod(fd, 0o600)
        os.fsync(fd)
        os.replace(temporary, OUTPUT_NAME, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    finally:
        os.close(fd)
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise PreparationError("invalid_arguments")


def main(argv=None) -> int:
    try:
        parser = Parser(description=__doc__)
        parser.add_argument("--legacy-config", required=True, type=Path)
        parser.add_argument("--channel-token", required=True, type=Path)
        parser.add_argument("--runtime-user", default="pot-test2")
        args = parser.parse_args(argv)
        check(os.geteuid() == 0, "root_required")
        helper = Path(__file__)
        info = helper.stat()
        check(
            helper.absolute() == helper.resolve(strict=True)
            and stat.S_ISREG(info.st_mode)
            and info.st_uid == 0
            and not info.st_mode & 0o022,
            "helper_ownership_invalid",
        )
        account = pwd.getpwnam(args.runtime_user)
        check(account.pw_uid > 0 and account.pw_gid > 0, "runtime_account_invalid")
        destination = RUNTIME_DIRECTORY / OUTPUT_NAME
        check(
            args.legacy_config != destination and args.channel_token != destination,
            "source_output_overlap",
        )
        content = payload(private_read(args.legacy_config), private_read(args.channel_token))
        directory = runtime_directory(account.pw_gid)
        try:
            atomic_write(directory, content, account.pw_uid, account.pw_gid)
        finally:
            os.close(directory)
        print("runtime_secrets_prepared")
        return 0
    except PreparationError as error:
        print(error.code, file=sys.stderr)
    except Exception:
        print("runtime_secrets_preparation_failed", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
