"""Prepare or verify a portable, checksum-pinned public historical deployment."""

import argparse
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / "deploy/render"
MODEL_FILES = {"manifest.json", "model.json", "thresholds.json", "feature-contract.json"}


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def write_pinned(path: Path, content: bytes) -> None:
    """Do not silently replace a previously prepared, changed artifact."""
    if path.exists() and path.read_bytes() != content:
        raise ValueError("An existing deployment artifact differs from its pinned bytes")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def prepare() -> None:
    # Imported only during explicit operator preparation, never in the build.
    from sqlalchemy import select

    from firewatch.config import Settings
    from firewatch.db.models import ModelVersion, SourceAsset
    from firewatch.db.session import Database
    from firewatch.ml.inference_bundle import load_bundle
    from firewatch.ml.registry_artifacts import managed_path
    from firewatch.paths import filesystem_path

    settings = Settings()
    database = Database(settings)
    files: dict[str, str] = {}
    try:
        with database.session() as session:
            assets = list(session.scalars(select(SourceAsset)))
            for asset in assets:
                if not re.fullmatch(r"[0-9a-f]{64}", asset.content_hash):
                    raise ValueError("Invalid source checksum")
                source = Path(asset.storage_path)
                if not source.resolve().is_relative_to(ROOT.resolve()):
                    raise ValueError("Source file is outside this checkout")
                content = filesystem_path(source).read_bytes()
                if digest(content) != asset.content_hash:
                    raise ValueError("Source bytes differ from their database pin")
                name = f"assets/{asset.content_hash}.bin"
                write_pinned(DESTINATION / name, content)
                files[name] = asset.content_hash
            models = list(session.scalars(select(ModelVersion)))
            for model in models:
                source = managed_path(settings.model_dir, model.artifact_path)
                load_bundle(source, model.artifact_hash, model.version)
                for filename in sorted(MODEL_FILES):
                    content = filesystem_path(source / filename).read_bytes()
                    name = f"model/{model.artifact_path}/{filename}"
                    write_pinned(DESTINATION / name, content)
                    files[name] = digest(content)
    finally:
        database.close()
    manifest = {
        "schema_version": "render-artifacts-v1",
        "source_records": len(assets),
        "unique_sources": sum(name.startswith("assets/") for name in files),
        "models": len(models),
        "files_sha256": dict(sorted(files.items())),
    }
    (DESTINATION / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    verify()


def verify(root: Path = DESTINATION, *, runtime: bool = False) -> None:
    """Check pinned bytes and optionally the runtime; no database or secrets required."""
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest["schema_version"] != "render-artifacts-v1":
        raise ValueError("Unsupported deployment manifest")
    entries = manifest["files_sha256"]
    if not entries:
        raise ValueError("Deployment has no pinned files")
    for name, checksum in entries.items():
        relative = Path(name)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or "\\" in name
            or relative.parts[0] not in {"assets", "model"}
            or not re.fullmatch(r"[0-9a-f]{64}", checksum)
        ):
            raise ValueError("Unsafe deployment artifact path or checksum")
        path = root / relative
        if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("Deployment artifact escapes its root")
        if digest(path.read_bytes()) != checksum:
            raise ValueError("Deployment artifact checksum mismatch")
    actual = {
        p.relative_to(root).as_posix()
        for d in ("assets", "model")
        for p in (root / d).rglob("*")
        if p.is_file()
    }
    if actual != set(entries):
        raise ValueError("Deployment contains unpinned files")
    size = sum((root / name).stat().st_size for name in entries)
    print(f"Verified {len(entries)} deployment files ({size / 1048576:.2f} MiB).")
    if runtime:
        from firewatch.ml.inference_bundle import load_bundle

        manifests = [
            name
            for name in entries
            if name.startswith("model/") and name.endswith("/manifest.json")
        ]
        if not manifests:
            raise ValueError("Deployment has no inference bundle")
        for name in manifests:
            path = root / name
            metadata = json.loads(path.read_text(encoding="utf-8"))
            load_bundle(path.parent, entries[name], metadata["scorer_version"])
        print(
            f"Verified {len(manifests)} inference bundle(s) against installed runtime dependencies."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "verify"))
    parser.add_argument(
        "--runtime",
        action="store_true",
        help="Also load and validate inference bundles after dependencies are installed",
    )
    args = parser.parse_args()
    if args.command == "prepare":
        prepare()
    else:
        verify(runtime=args.runtime)
