import re
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
        "weights_by_aspect": {
            "1x1": {
                "captions.safe_zone_violations": 0.05,
                "captions.caption_readability_score": 0.25,
            },
            "9x16": {
                "captions.safe_zone_violations": 0.15,
            },
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
    "insta_reel_v1": {
        "extends": "social_reel_v1",
        "description": "Instagram reel scoring (caption pacing + safe zones).",
        "targets": {
            "caption_speed_wpm_max": 175.0,
            "caption_readability_min": 72.0,
        },
        "weights": {
            "captions.caption_readability_score": 0.22,
            "captions.caption_speed_score": 0.12,
            "captions.safe_zone_violations": 0.12,
        },
    },
    "youtube_short_v1": {
        "extends": "social_reel_v1",
        "description": "YouTube Short scoring (audio loudness + clarity).",
        "targets": {
            "loudness_lufs": -14.0,
            "loudness_tolerance": 2.0,
            "caption_speed_wpm_max": 190.0,
        },
        "weights": {
            "audio.loudness_lufs": 0.2,
            "audio.true_peak_db": 0.1,
            "captions.caption_speed_score": 0.08,
        },
    },
}


def list_rubrics() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "description": get_rubric(name).get("description"),
            "pass_threshold": get_rubric(name).get("pass_threshold"),
        }
        for name, rubric in RUBRICS.items()
    ]


def get_rubric(name: str) -> dict[str, Any]:
    if not name:
        raise ValueError("rubric_name is required")
    if name not in RUBRICS:
        raise ValueError(f"Unknown rubric: {name}")
    return _resolve_rubric(name, set())


def describe_rubric(name: str) -> dict[str, Any]:
    rubric = get_rubric(name)
    return {
        "name": name,
        "description": rubric.get("description"),
        "pass_threshold": rubric.get("pass_threshold"),
        "weights": rubric.get("weights", {}),
        "weights_by_aspect": rubric.get("weights_by_aspect", {}),
        "weights_by_preset": rubric.get("weights_by_preset", {}),
        "targets": rubric.get("targets", {}),
    }


def _merge_rubric(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = {
        "description": base.get("description"),
        "pass_threshold": base.get("pass_threshold"),
        "weights": dict(base.get("weights", {})),
        "targets": dict(base.get("targets", {})),
        "weights_by_aspect": dict(base.get("weights_by_aspect", {})),
        "weights_by_preset": dict(base.get("weights_by_preset", {})),
    }
    if override.get("description"):
        merged["description"] = override.get("description")
    if override.get("pass_threshold") is not None:
        merged["pass_threshold"] = override.get("pass_threshold")
    if override.get("weights"):
        merged["weights"].update(override.get("weights", {}))
    if override.get("targets"):
        merged["targets"].update(override.get("targets", {}))
    if override.get("weights_by_aspect"):
        merged["weights_by_aspect"].update(override.get("weights_by_aspect", {}))
    if override.get("weights_by_preset"):
        merged["weights_by_preset"].update(override.get("weights_by_preset", {}))
    return merged


def _resolve_rubric(name: str, seen: set[str]) -> dict[str, Any]:
    if name in seen:
        raise ValueError("Rubric extends cycle detected")
    seen.add(name)
    rubric = RUBRICS.get(name, {})
    parent = rubric.get("extends")
    if parent:
        base = _resolve_rubric(parent, seen)
        return _merge_rubric(base, rubric)
    return _merge_rubric(rubric, {})


def _aspect_label_from_preset_name(preset_name: str | None) -> str | None:
    if not preset_name:
        return None
    match = re.search(r"(\\d{2,4})x(\\d{2,4})", preset_name)
    if not match:
        return None
    try:
        width = float(match.group(1))
        height = float(match.group(2))
    except ValueError:
        return None
    if height == 0:
        return None
    ratio = width / height
    targets = {
        "9x16": 9 / 16,
        "1x1": 1.0,
        "4x5": 4 / 5,
        "16x9": 16 / 9,
    }
    closest = min(targets.items(), key=lambda item: abs(item[1] - ratio))
    if abs(closest[1] - ratio) > 0.15:
        return None
    return closest[0]


def _resolve_weights(rubric: dict[str, Any], target_preset: str | None) -> dict[str, Any]:
    weights = dict(rubric.get("weights", {}))
    if not target_preset:
        return weights
    preset_overrides = rubric.get("weights_by_preset", {}) or {}
    if target_preset in preset_overrides:
        weights.update(preset_overrides[target_preset])
    aspect = _aspect_label_from_preset_name(target_preset)
    aspect_overrides = rubric.get("weights_by_aspect", {}) or {}
    if aspect and aspect in aspect_overrides:
        weights.update(aspect_overrides[aspect])
    return weights


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


def score_report(
    report: dict[str, Any],
    rubric: dict[str, Any],
    target_preset: str | None = None,
) -> dict[str, Any]:
    targets = rubric.get("targets", {})
    weights = _resolve_weights(rubric, target_preset)
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
        "weights_used": {k: weights[k] for k in subscores.keys() if k in weights},
    }


def qa_from_report(
    report: dict[str, Any],
    rubric: dict[str, Any],
    target_preset: str | None = None,
) -> dict[str, Any]:
    if not report or not rubric:
        return {"pass": None, "score": None, "failed_checks": [], "recommended_fix": None}
    scored = score_report(report, rubric, target_preset)
    failures = _find_failures(report, rubric, target_preset)
    failed_checks = [item["reason"] for item in failures[:3]]
    recommended_fix = failures[0]["fix"] if failures else None
    return {
        "pass": scored.get("passed"),
        "score": scored.get("score"),
        "failed_checks": failed_checks,
        "recommended_fix": recommended_fix,
    }


def _find_failures(
    report: dict[str, Any],
    rubric: dict[str, Any],
    target_preset: str | None,
) -> list[dict[str, Any]]:
    targets = rubric.get("targets", {})
    weights = _resolve_weights(rubric, target_preset)
    failures: list[dict[str, Any]] = []

    def add_failure(key: str, reason: str, fix: str, severity: float) -> None:
        weight = float(weights.get(key, 0))
        if weight <= 0:
            return
        failures.append(
            {
                "key": key,
                "reason": reason,
                "fix": fix,
                "weight": weight,
                "severity": severity,
            }
        )

    audio = report.get("audio", {}) if isinstance(report, dict) else {}
    video = report.get("video", {}) if isinstance(report, dict) else {}
    captions = report.get("captions", {}) if isinstance(report, dict) else {}

    loudness = audio.get("loudness_lufs")
    if loudness is not None:
        target = float(targets.get("loudness_lufs", -16.0))
        tol = float(targets.get("loudness_tolerance", 2.5))
        diff = abs(float(loudness) - target)
        if diff > tol:
            add_failure(
                "audio.loudness_lufs",
                f"audio loudness {float(loudness):.1f} LUFS outside target",
                "adjust audio_target_lufs",
                diff,
            )

    true_peak = audio.get("true_peak_db")
    if true_peak is not None:
        peak_max = float(targets.get("true_peak_max_db", -1.5))
        if float(true_peak) > peak_max:
            add_failure(
                "audio.true_peak_db",
                f"audio true peak {float(true_peak):.1f} dB exceeds limit",
                "lower audio_true_peak or music_gain",
                float(true_peak) - peak_max,
            )

    lra = audio.get("lra")
    if lra is not None:
        lra_max = float(targets.get("lra_max", 12.0))
        if float(lra) > lra_max:
            add_failure(
                "audio.lra",
                f"audio dynamic range {float(lra):.1f} too high",
                "lower audio_lra",
                float(lra) - lra_max,
            )

    silence_pct = audio.get("silence_pct")
    if silence_pct is not None:
        silence_max = float(targets.get("silence_pct_max", 5.0))
        if float(silence_pct) > silence_max:
            add_failure(
                "audio.silence_pct",
                f"silence {float(silence_pct):.2f}% exceeds limit",
                "enable trim_silence",
                float(silence_pct) - silence_max,
            )

    clipping_pct = audio.get("clipping_pct")
    if clipping_pct is not None:
        clipping_max = float(targets.get("clipping_pct_max", 0.1))
        if float(clipping_pct) > clipping_max:
            add_failure(
                "audio.clipping_pct",
                f"clipping {float(clipping_pct):.3f}% detected",
                "lower music_gain or audio_true_peak",
                float(clipping_pct) - clipping_max,
            )

    black_frames = video.get("black_frames_pct")
    if black_frames is not None:
        black_max = float(targets.get("black_frames_pct_max", 1.0))
        if float(black_frames) > black_max:
            add_failure(
                "video.black_frames_pct",
                f"black frames {float(black_frames):.2f}% detected",
                "set framing_mode=crop",
                float(black_frames) - black_max,
            )

    resolution_ok = video.get("resolution_ok")
    if resolution_ok is False:
        add_failure(
            "video.resolution_ok",
            "resolution does not match target preset",
            "render with target_preset",
            1.0,
        )

    bitrate_ok = video.get("bitrate_ok")
    if bitrate_ok is False:
        add_failure(
            "video.bitrate_ok",
            "bitrate exceeds target",
            "use draft quality or lower bitrate",
            1.0,
        )

    file_size_ok = video.get("file_size_ok")
    if file_size_ok is False:
        add_failure(
            "video.file_size_ok",
            "file size exceeds limit",
            "use draft quality or lower bitrate",
            1.0,
        )

    readability = captions.get("caption_readability_score")
    if readability is not None:
        min_score = float(targets.get("caption_readability_min", 70.0))
        if float(readability) < min_score:
            add_failure(
                "captions.caption_readability_score",
                f"caption readability {float(readability):.1f} below target",
                "reduce caption length or font size",
                min_score - float(readability),
            )

    speed_wpm = captions.get("caption_speed_wpm")
    if speed_wpm is not None:
        speed_max = float(targets.get("caption_speed_wpm_max", 180.0))
        if float(speed_wpm) > speed_max:
            add_failure(
                "captions.caption_speed_score",
                f"caption speed {float(speed_wpm):.1f} wpm too fast",
                "reduce caption words per line",
                float(speed_wpm) - speed_max,
            )

    safe_zone = captions.get("safe_zone_violations")
    if safe_zone is not None:
        safe_zone_max = float(targets.get("safe_zone_violations_max", 0))
        if float(safe_zone) > safe_zone_max:
            add_failure(
                "captions.safe_zone_violations",
                f"captions violate safe zones ({int(safe_zone)})",
                "increase caption safe-zone padding",
                float(safe_zone) - safe_zone_max,
            )

    failures.sort(key=lambda item: (item["weight"], item["severity"]), reverse=True)
    return failures
