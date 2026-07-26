from __future__ import annotations

import logging
import os
import shutil
import uuid
from pathlib import Path

logger = logging.getLogger("runner_wrapper.files")


def publish_directory(
    source: str | Path,
    destination: str | Path,
    *,
    dirs_exist_ok: bool = False,
) -> Path:
    """Publish a directory, preserving metadata when the filesystem allows it."""

    source_path = Path(source)
    destination_path = Path(destination)
    if not source_path.is_dir():
        raise FileNotFoundError(f"source directory not found: {source_path}")
    if destination_path.exists() and not dirs_exist_ok:
        raise FileExistsError(destination_path)

    destination_path.mkdir(parents=True, exist_ok=True)
    directory_pairs: list[tuple[Path, Path]] = []
    preserve_metadata = True

    for root, directory_names, file_names in os.walk(source_path):
        directory_names.sort()
        file_names.sort()
        source_root = Path(root)
        destination_root = destination_path / source_root.relative_to(source_path)
        destination_root.mkdir(parents=True, exist_ok=True)
        directory_pairs.append((source_root, destination_root))

        for directory_name in directory_names:
            (destination_root / directory_name).mkdir(exist_ok=True)

        for file_name in file_names:
            source_file = source_root / file_name
            destination_file = destination_root / file_name
            shutil.copyfile(source_file, destination_file)
            if preserve_metadata:
                preserve_metadata = _preserve_metadata(source_file, destination_file)

    if preserve_metadata:
        for source_dir, destination_dir in reversed(directory_pairs):
            if not _preserve_metadata(source_dir, destination_dir):
                break

    return destination_path


def publish_file(source: str | Path, destination: str | Path) -> Path:
    """Atomically copy a file, preserving metadata when the filesystem allows it."""

    source_path = Path(source)
    destination_path = Path(destination)
    if not source_path.is_file():
        raise FileNotFoundError(f"source file not found: {source_path}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination_path.with_name(
        f".{destination_path.name}.{uuid.uuid4().hex}.part"
    )
    try:
        shutil.copyfile(source_path, temp_path)
        _preserve_metadata(source_path, temp_path)
        os.replace(temp_path, destination_path)
    finally:
        temp_path.unlink(missing_ok=True)

    return destination_path


def _preserve_metadata(source: Path, destination: Path) -> bool:
    try:
        shutil.copystat(source, destination)
        return True
    except OSError as exc:
        logger.warning(
            "Could not preserve file metadata for %s: %s; keeping copied contents",
            destination,
            exc,
        )
        return False
