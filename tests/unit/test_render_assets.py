"""Deployment builds must reject modified, extra, or escaping artifacts."""

import json

import pytest

from scripts.render_assets import DESTINATION, digest, verify


@pytest.fixture
def package(tmp_path):
    (tmp_path / "assets").mkdir()
    (tmp_path / "model").mkdir()
    content = b"public historical observation\r\n"
    name = f"assets/{digest(content)}.bin"
    (tmp_path / name).write_bytes(content)
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {"schema_version": "render-artifacts-v1", "files_sha256": {name: digest(content)}}
        )
    )
    return tmp_path, name


def test_package_rejects_tampered_and_unpinned_bytes(package):
    root, name = package
    verify(root)
    (root / "assets/extra.bin").write_bytes(b"unreviewed")
    with pytest.raises(ValueError, match="unpinned"):
        verify(root)
    (root / "assets/extra.bin").unlink()
    (root / name).write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        verify(root)


@pytest.mark.parametrize("name", ["../outside", "/outside", r"assets\outside", "other/file"])
def test_package_rejects_paths_outside_reviewed_directories(package, name):
    root, _ = package
    (root / "manifest.json").write_text(
        json.dumps({"schema_version": "render-artifacts-v1", "files_sha256": {name: "0" * 64}})
    )
    with pytest.raises(ValueError, match="Unsafe"):
        verify(root)


def test_runtime_gate_loads_the_delivered_model_and_rejects_incompatible_dependencies(monkeypatch):
    from firewatch.ml import inference_bundle

    verify(DESTINATION, runtime=True)
    actual = inference_bundle.runtime_dependencies()
    monkeypatch.setattr(
        inference_bundle, "runtime_dependencies", lambda: {**actual, "python": "0.0"}
    )
    with pytest.raises(ValueError, match="dependency mismatch"):
        verify(DESTINATION, runtime=True)


def test_runtime_gate_refuses_a_source_only_package(package):
    root, _ = package
    with pytest.raises(ValueError, match="no inference bundle"):
        verify(root, runtime=True)
