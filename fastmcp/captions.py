import re
from typing import Any


TIME_RE = re.compile(
    r"(?P<hours>\d{1,2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})(?P<millis>[.,]\d{1,3})?"
)


def parse_timecode(value: str) -> float | None:
    value = (value or "").strip()
    if not value:
        return None
    match = TIME_RE.search(value)
    if not match:
        return None
    hours = int(match.group("hours"))
    minutes = int(match.group("minutes"))
    seconds = int(match.group("seconds"))
    millis_raw = match.group("millis") or ".0"
    millis = float(millis_raw.replace(",", "."))
    total = hours * 3600 + minutes * 60 + seconds + millis
    return total


def _clean_lines(lines: list[str]) -> list[str]:
    return [line.rstrip("\ufeff").rstrip() for line in lines if line.strip()]


def parse_srt(text: str) -> list[dict[str, Any]]:
    blocks = re.split(r"\n{2,}", text.strip())
    segments: list[dict[str, Any]] = []
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        if "-->" not in lines[0] and len(lines) > 1:
            lines = lines[1:]
        if not lines or "-->" not in lines[0]:
            continue
        times = lines[0].split("-->")
        if len(times) < 2:
            continue
        start = parse_timecode(times[0])
        end = parse_timecode(times[1])
        if start is None or end is None or end <= start:
            continue
        content = "\n".join(lines[1:]).strip()
        if not content:
            continue
        segments.append({"start": float(start), "end": float(end), "text": content})
    return segments


def parse_vtt(text: str) -> list[dict[str, Any]]:
    lines = [line.rstrip() for line in text.splitlines()]
    lines = [line for line in lines if not line.strip().startswith("WEBVTT")]
    blocks = re.split(r"\n{2,}", "\n".join(lines).strip())
    segments: list[dict[str, Any]] = []
    for block in blocks:
        items = [line.strip() for line in block.splitlines() if line.strip()]
        if not items:
            continue
        if "-->" not in items[0] and len(items) > 1:
            items = items[1:]
        if not items or "-->" not in items[0]:
            continue
        times = items[0].split("-->")
        if len(times) < 2:
            continue
        start = parse_timecode(times[0])
        end = parse_timecode(times[1])
        if start is None or end is None or end <= start:
            continue
        content = "\n".join(items[1:]).strip()
        if not content:
            continue
        segments.append({"start": float(start), "end": float(end), "text": content})
    return segments


def normalize_words(words: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for item in words:
        word = str(item.get("word") or "").strip()
        if not word:
            continue
        try:
            start = float(item.get("start"))
            end = float(item.get("end"))
        except (TypeError, ValueError):
            continue
        if end <= start:
            continue
        normalized.append({"word": word, "start": start, "end": end})
    normalized.sort(key=lambda entry: entry["start"])
    return normalized


def wrap_text(text: str, max_chars: int, max_lines: int) -> str:
    words = text.split()
    if not words:
        return ""
    lines: list[str] = []
    current = ""
    for word in words:
        if not current:
            current = word
        elif len(current) + 1 + len(word) <= max_chars:
            current = f"{current} {word}"
        else:
            lines.append(current)
            current = word
        if len(lines) >= max_lines:
            break
    if len(lines) < max_lines and current:
        lines.append(current)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
    if len(lines) == max_lines and len(words) > len(" ".join(lines).split()):
        lines[-1] = lines[-1][: max(0, max_chars - 3)] + "..."
    return "\n".join(lines)


def segments_from_words(
    words: list[dict[str, Any]],
    max_chars: int,
    max_words: int,
    max_gap: float = 0.6,
    highlight_mode: str | None = None,
) -> list[dict[str, Any]]:
    normalized = normalize_words(words)
    if highlight_mode == "word":
        return [
            {"start": item["start"], "end": item["end"], "text": item["word"]}
            for item in normalized
        ]

    segments: list[dict[str, Any]] = []
    buffer: list[dict[str, Any]] = []
    for item in normalized:
        if not buffer:
            buffer.append(item)
            continue
        gap = item["start"] - buffer[-1]["end"]
        text_candidate = " ".join([w["word"] for w in buffer] + [item["word"]])
        if gap > max_gap or len(buffer) >= max_words or len(text_candidate) > max_chars:
            start = buffer[0]["start"]
            end = buffer[-1]["end"]
            text = " ".join(w["word"] for w in buffer)
            segments.append({"start": start, "end": end, "text": text})
            buffer = [item]
        else:
            buffer.append(item)
    if buffer:
        start = buffer[0]["start"]
        end = buffer[-1]["end"]
        text = " ".join(w["word"] for w in buffer)
        segments.append({"start": start, "end": end, "text": text})
    return segments


def sanitize_caption_text(text: str) -> str:
    cleaned = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", text or "")
    return cleaned.strip()


def parse_captions_input(
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    max_chars: int,
    max_lines: int,
    max_words: int,
    highlight_mode: str | None,
) -> list[dict[str, Any]]:
    if captions_srt:
        segments = parse_srt(captions_srt)
    elif captions_vtt:
        segments = parse_vtt(captions_vtt)
    elif words_json:
        segments = segments_from_words(words_json, max_chars, max_words, highlight_mode=highlight_mode)
    else:
        return []

    normalized: list[dict[str, Any]] = []
    for seg in segments:
        text = sanitize_caption_text(seg.get("text") or "")
        if not text:
            continue
        wrapped = wrap_text(text, max_chars, max_lines)
        if not wrapped:
            continue
        normalized.append({"start": seg["start"], "end": seg["end"], "text": wrapped})
    return normalized
