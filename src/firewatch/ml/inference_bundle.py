"""Compact non-executable runtime bundles; approval is an external trusted reference.

Building/verifying a bundle never approves it. Registry promotion belongs to
Phase 4.7. Inference checks only these bounded files; it never refits source data.
"""

import hashlib
import importlib.metadata
import json
import platform
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from firewatch.ml.baselines import BASELINE_VERSIONS
from firewatch.ml.forest_artifact import PortableForest, transformed_inputs
from firewatch.ml.priority_thresholds import PriorityThresholds
from firewatch.ml.tree_shap import PathDependentSHAP
from firewatch.paths import filesystem_path
from firewatch.processing.features import MODEL_INPUT_NAMES, UNITS
from firewatch.schemas.common import DTO, Digest, Version
from firewatch.schemas.contracts import FeatureSet

FILES = {"model.json", "thresholds.json", "feature-contract.json"}
MAX_BYTES = 32 * 1024 * 1024


class ApprovedScorer(DTO):
    """Provided by a trusted approval authority, never inferred from bundle metadata."""

    model_id: UUID
    scorer_version: Version
    artifact_path: Path
    manifest_sha256: Digest
    approval_status: Literal["approved"]
    approval_reference: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def supervised(self) -> Self:
        if self.scorer_version in BASELINE_VERSIONS or not self.approval_reference.strip():
            raise ValueError("Supervised inference requires an explicit approval identity")
        return self


def runtime_dependencies() -> dict[str, str]:
    return {
        "python": ".".join(platform.python_version_tuple()[:2]),
        **{n: importlib.metadata.version(n) for n in ("numpy", "pydantic")},
    }


def feature_contract() -> dict[str, Any]:
    return dict(
        feature_version="features-v1",
        model_input_names=list(MODEL_INPUT_NAMES),
        units=dict(UNITS),
        required_observed_features=["frp_max_mw", "frp_sum_mw"],
        max_batch_size=256,
        explanation_method="exact-tree-path-dependent-shap-v1",
        missing_policy="training_median_plus_explicit_missing_flags",
    )


def _json(raw: bytes) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate inference JSON key")
            result[key] = value
        return result

    return json.loads(
        raw,
        object_pairs_hook=unique,
        parse_constant=lambda v: (_ for _ in ()).throw(ValueError("Nonfinite JSON")),
    )


def _file(root: Path, name: str, limit: int = MAX_BYTES) -> bytes:
    root = filesystem_path(root)
    if name not in FILES | {"manifest.json"} or root.is_symlink():
        raise ValueError("Invalid inference artifact path")
    path = root / name
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError("Missing, symlinked or oversized inference artifact")
    with path.open("rb") as stream:
        payload = stream.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("Oversized inference artifact")
    return payload


def _validate(model: PortableForest, thresholds: PriorityThresholds) -> None:
    units = "calibrated_probability" if model.probability_supported else "forest_vote_fraction"
    if (
        thresholds.method != "forest"
        or thresholds.score_units != units
        or thresholds.basis != "development_fbeta"
        or not thresholds.provisional
        or not 0 <= thresholds.medium <= thresholds.critical <= 1
    ):
        raise ValueError("Inference thresholds must match the frozen forest score scale")
    if set(thresholds.fit_row_ids) & (
        set(model.fit_row_ids) | set(model.calibration_fit_ids) | set(model.calibration_audit_ids)
    ):
        raise ValueError("Inference thresholds overlap model fitting or calibration")


class RuntimeForest:
    """Private validated model and compiled explanations retained for this process."""

    def __init__(self, model: PortableForest, thresholds: PriorityThresholds):
        self._model = PortableForest.model_validate_json(model.model_dump_json())
        self._thresholds = PriorityThresholds.model_validate_json(thresholds.model_dump_json())
        _validate(self._model, self._thresholds)
        self._shap = PathDependentSHAP(self._model)

    def describe(self) -> dict[str, Any]:
        """Describe the verified artifact without claiming unused inputs affect scores."""
        used = {
            self._model.feature_names[node.feature]
            for tree in self._model.trees
            for node in tree.nodes
            if node.left != -1
        }
        return {
            "calibration_status": self._model.calibration_status,
            "score_units": self._thresholds.score_units,
            "priority_thresholds_provisional": self._thresholds.provisional,
            "active_features": sorted(used),
            "context_only_features": sorted(set(self._model.feature_names) - used),
        }

    def score(self, feature: FeatureSet) -> tuple[float, float | None, str, dict[str, Any]]:
        from firewatch.ml.explain import forest_explanation

        for name in feature_contract()["required_observed_features"]:
            if feature.values[name] is None:
                raise ValueError("Required satellite inputs are missing; ML scoring withheld")
        vector = transformed_inputs(feature, self._model)
        total = 0.0
        for tree in self._model.trees:
            index = 0
            while tree.nodes[index].left != -1:
                node = tree.nodes[index]
                index = node.left if float(vector[node.feature]) <= node.threshold else node.right
            total += tree.nodes[index].class_values[1]
        # Match the reference estimator's sequential mean and float32 branch decisions.
        raw = min(1.0, max(0.0, total / len(self._model.trees)))
        contributions = self._shap.explain(vector, raw)
        probability = self._model.sigmoid.apply(raw) if self._model.sigmoid else None
        ranking = probability if probability is not None else raw
        explanation = forest_explanation(
            feature,
            contributions,
            self._shap.base_value,
            raw,
            probability is not None,
            self._model.numeric.medians,
        )
        explanation["thresholds"] = self._thresholds.model_dump(mode="json")
        return ranking, probability, self._thresholds.priority(ranking), explanation


def load_bundle(root: Path, expected_hash: str, version: str | None = None) -> RuntimeForest:
    """Verify the trusted digest and compile; no training/source-package dependency."""
    try:
        if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
            raise ValueError("A trusted inference manifest SHA256 is required")
        raw = _file(root, "manifest.json", 65536)
        if hashlib.sha256(raw).hexdigest() != expected_hash:
            raise ValueError("Inference manifest checksum mismatch")
        manifest = _json(raw)
        if (
            manifest["schema_version"] != "inference-bundle-v1"
            or manifest["approval_status"] != "candidate"
            or manifest["dependencies"] != runtime_dependencies()
            or set(manifest["files"]) != FILES
            or (version is not None and manifest["scorer_version"] != version)
            or manifest["scorer_version"] in BASELINE_VERSIONS
            or not isinstance(manifest["scorer_version"], str)
            or not 1 <= len(manifest["scorer_version"]) <= 100
            or not re.fullmatch(r"[0-9a-f]{64}", manifest["source_evaluation_sha256"])
        ):
            raise ValueError("Inference manifest contract/version/dependency mismatch")
        total = len(raw)
        content = {}
        for name in sorted(FILES):
            payload = _file(root, name)
            entry = manifest["files"][name]
            total += len(payload)
            if (
                len(payload) != entry["size"]
                or hashlib.sha256(payload).hexdigest() != entry["sha256"]
                or total > MAX_BYTES
            ):
                raise ValueError("Inference file checksum/size mismatch")
            content[name] = _json(payload)
        if content["feature-contract.json"] != feature_contract():
            raise ValueError("Inference feature schema/order/quality contract mismatch")
        return RuntimeForest(
            PortableForest.model_validate(content["model.json"]),
            PriorityThresholds.model_validate(content["thresholds.json"]),
        )
    except (KeyError, TypeError, AttributeError, RecursionError, OverflowError):
        raise ValueError("Malformed inference bundle") from None


def _write_bundle(
    output: Path,
    model: PortableForest,
    thresholds: PriorityThresholds,
    version: str,
    source_sha256: str,
) -> str:
    """Internal serializer; public construction below verifies evaluation provenance first."""
    if (
        version in BASELINE_VERSIONS
        or not 1 <= len(version) <= 100
        or not re.fullmatch(r"[0-9a-f]{64}", source_sha256)
    ):
        raise ValueError("Invalid supervised bundle version or provenance")
    _validate(model, thresholds)
    if output.exists():
        raise ValueError("Inference output exists; choose a new directory")
    target = output.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".inference-bundle-", dir=target.parent)).resolve()

    def write(name: str, value: Any) -> None:
        (staging / name).write_text(
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n",
            encoding="utf-8",
        )

    try:
        write("model.json", model.model_dump(mode="json"))
        write("thresholds.json", thresholds.model_dump(mode="json"))
        write("feature-contract.json", feature_contract())
        write(
            "manifest.json",
            dict(
                schema_version="inference-bundle-v1",
                approval_status="candidate",
                scorer_version=version,
                source_evaluation_sha256=source_sha256,
                dependencies=runtime_dependencies(),
                files={
                    name: dict(
                        size=(staging / name).stat().st_size,
                        sha256=hashlib.sha256((staging / name).read_bytes()).hexdigest(),
                    )
                    for name in sorted(FILES)
                },
            ),
        )
        sha = hashlib.sha256((staging / "manifest.json").read_bytes()).hexdigest()
        load_bundle(staging, sha, version)
        staging.rename(target)
        return sha
    finally:
        if staging.exists():
            if staging.parent != target.parent or not staging.name.startswith(".inference-bundle-"):
                raise ValueError("Refusing cleanup outside owned inference staging directory")
            shutil.rmtree(staging)


def export_inference_bundle(
    evaluation: Path, output: Path, expected_hash: str, version: str
) -> dict[str, Any]:
    from firewatch.ml.evaluation_models import EvaluationModels
    from firewatch.ml.evaluation_package import verify_evaluation

    verified = verify_evaluation(evaluation, expected_hash)
    base = evaluation if verified["stage"] == "development" else evaluation / "prepared"
    report = _json((base / "evaluation-report.json").read_bytes())
    if report["status"] != "development_frozen":
        raise ValueError("No eligible forest/thresholds; retain the baseline-only scorer")
    models = EvaluationModels.model_validate_json((base / "evaluation-models.json").read_bytes())
    thresholds = PriorityThresholds.model_validate(report["thresholds"]["forest"])
    sha = _write_bundle(output, models.forest, thresholds, version, expected_hash)
    return dict(
        manifest_sha256=sha,
        scorer_version=version,
        approval_status="candidate",
        promotion_performed=False,
        final_test_status=verified["final_test_status"],
    )
