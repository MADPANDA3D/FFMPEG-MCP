from typing import Any


RUBRICS: dict[str, dict[str, Any]] = {
    "social_reel_v1": {
        "description": "Social reel scoring (captions + loudness + black frames).",
        "pass_threshold": 85,
        "weights": {
            "audio.loudness_lufs": 0.15,
            "audio.true_peak_db": 0.08,
            "audio.lra": 0.05,
            "audio.silence_pct": 0.05,
            "audio.clipping_pct": 0.02,
            "video.black_frames_pct": 0.05,
            "video.resolution_ok": 0.1,
            "video.bitrate_ok": 0.05,
            "video.file_size_ok": 0.05,
            "captions.caption_readability_score": 0.2,
            "captions.caption_speed_score": 0.1,
            "captions.safe_zone_violations": 0.1,
        },
        "targets": {
            "loudness_lufs": -16.0,
            "loudness_tolerance": 2.5,
            "true_peak_max_db": -1.5,
            "lra_max": 12.0,
            "silence_pct_max": 5.0,
            "clipping_pct_max": 0.1,
            "black_frames_pct_max": 1.0,
            "caption_speed_wpm_max": 180.0,
            "caption_readability_min": 70.0,
            "safe_zone_violations_max": 0,
            "bitrate_kbps_max": 12000.0,
            "min_width": 720,
            "min_height": 720,
        },
    },
    "testimonial_v1": {
        "description": "Testimonial scoring (speech clarity + captions + safe zones).",
        "pass_threshold": 85,
        "weights": {
            "audio.loudness_lufs": 0.2,
            "audio.true_peak_db": 0.1,
            "audio.lra": 0.1,
            "audio.silence_pct": 0.1,
            "audio.clipping_pct": 0.05,
            "video.resolution_ok": 0.1,
            "video.black_frames_pct": 0.05,
            "captions.caption_readability_score": 0.15,
            "captions.caption_speed_score": 0.1,
            "captions.safe_zone_violations": 0.05,
        },
        "targets": {
            "loudness_lufs": -16.0,
            "loudness_tolerance": 2.5,
            "true_peak_max_db": -1.5,
            "lra_max": 12.0,
            "silence_pct_max": 7.0,
            "clipping_pct_max": 0.1,
            "black_frames_pct_max": 1.0,
            "caption_speed_wpm_max": 170.0,
            "caption_readability_min": 72.0,
            "safe_zone_violations_max": 0,
            "bitrate_kbps_max": 12000.0,
            "min_width": 720,
            "min_height": 720,
        },
    },
}


def list_rubrics() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "description": rubric.get("description"),
            "pass_threshold": rubric.get("pass_threshold"),
        }
        for name, rubric in RUBRICS.items()
    ]


def get_rubric(name: str) -> dict[str, Any]:
    if not name:
        raise ValueError("rubric_name is required")
    rubric = RUBRICS.get(name)
    if not rubric:
        raise ValueError(f"Unknown rubric: {name}")
    return rubric


def describe_rubric(name: str) -> dict[str, Any]:
    rubric = get_rubric(name)
    return {
        "name": name,
        "description": rubric.get("description"),
        "pass_threshold": rubric.get("pass_threshold"),
        "weights": rubric.get("weights", {}),
        "targets": rubric.get("targets", {}),
    }


def _get_value(report: dict[str, Any], path: str) -> Any:
    current: Any = report
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _score_bool(value: Any) -> float | None:
    if value is None:
        return None
    return 100.0 if bool(value) else 0.0


def _score_min(value: Any, minimum: float) -> float | None:
    if value is None:
        return None
    if float(value) >= float(minimum):
        return 100.0
    return max(0.0, 100.0 * (float(value) / float(minimum)))


def _score_max(value: Any, maximum: float, slope: float = 10.0) -> float | None:
    if value is None:
        return None
    if float(value) <= float(maximum):
        return 100.0
    return max(0.0, 100.0 - (float(value) - float(maximum)) * slope)


def _score_target(value: Any, target: float, tolerance: float) -> float | None:
    if value is None:
        return None
    diff = abs(float(value) - float(target))
    if diff >= float(tolerance):
        return 0.0
    return max(0.0, 100.0 * (1.0 - diff / float(tolerance)))


def score_report(report: dict[str, Any], rubric: dict[str, Any]) -> dict[str, Any]:
    targets = rubric.get("targets", {})
    weights = rubric.get("weights", {})
    subscores: dict[str, float] = {}
    weighted_sum = 0.0
    weight_total = 0.0

    def add_score(key: str, score: float | None) -> None:
        nonlocal weighted_sum, weight_total
        if score is None:
            return
        weight = float(weights.get(key, 0))
        if weight <= 0:
            return
        subscores[key] = round(score, 2)
        weighted_sum += score * weight
        weight_total += weight

    add_score(
        "audio.loudness_lufs",
        _score_target(
            _get_value(report, "audio.loudness_lufs"),
            targets.get("loudness_lufs", -16.0),
            targets.get("loudness_tolerance", 2.5),
        ),
    )
    add_score(
        "audio.true_peak_db",
        _score_max(
            _get_value(report, "audio.true_peak_db"),
            targets.get("true_peak_max_db", -1.5),
            slope=40.0,
        ),
    )
    add_score(
        "audio.lra",
        _score_max(
            _get_value(report, "audio.lra"),
            targets.get("lra_max", 12.0),
            slope=8.0,
        ),
    )
    add_score(
        "audio.silence_pct",
        _score_max(
            _get_value(report, "audio.silence_pct"),
            targets.get("silence_pct_max", 5.0),
            slope=6.0,
        ),
    )
    add_score(
        "audio.clipping_pct",
        _score_max(
            _get_value(report, "audio.clipping_pct"),
            targets.get("clipping_pct_max", 0.1),
            slope=120.0,
        ),
    )
    add_score(
        "video.black_frames_pct",
        _score_max(
            _get_value(report, "video.black_frames_pct"),
            targets.get("black_frames_pct_max", 1.0),
            slope=12.0,
        ),
    )
    add_score(
        "video.resolution_ok",
        _score_bool(_get_value(report, "video.resolution_ok")),
    )
    add_score(
        "video.bitrate_ok",
        _score_bool(_get_value(report, "video.bitrate_ok")),
    )
    add_score(
        "video.file_size_ok",
        _score_bool(_get_value(report, "video.file_size_ok")),
    )
    add_score(
        "captions.caption_readability_score",
        _score_min(
            _get_value(report, "captions.caption_readability_score"),
            targets.get("caption_readability_min", 70.0),
        ),
    )
    add_score(
        "captions.caption_speed_score",
        _score_min(
            _get_value(report, "captions.caption_speed_score"),
            80.0,
        ),
    )
    add_score(
        "captions.safe_zone_violations",
        _score_max(
            _get_value(report, "captions.safe_zone_violations"),
            targets.get("safe_zone_violations_max", 0),
            slope=100.0,
        ),
    )

    score = 0.0
    if weight_total > 0:
        score = weighted_sum / weight_total
    threshold = float(rubric.get("pass_threshold", 85))
    return {
        "score": round(score, 2),
        "passed": score >= threshold,
        "pass_threshold": threshold,
        "subscores": subscores,
        "weights_used": {k: weights[k] for k in subscores.keys()},
    }
