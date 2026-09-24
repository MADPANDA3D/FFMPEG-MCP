def audio_duck_filter_complex(
    sample_rate: int,
    ratio: float,
    threshold: float,
    attack_ms: int,
    release_ms: int,
    music_gain: float,
    voice_gain: float,
) -> str:
    return (
        f"[0:a]aresample={sample_rate},"
        "aformat=sample_fmts=fltp:channel_layouts=stereo,"
        f"volume={music_gain}[music];"
        f"[1:a]aresample={sample_rate},"
        "aformat=sample_fmts=fltp:channel_layouts=stereo,"
        "asplit=2[voice_sidechain][voice_mix];"
        f"[voice_mix]volume={voice_gain}[voice_audible];"
        f"[music][voice_sidechain]sidechaincompress=threshold={threshold}:"
        f"ratio={ratio}:attack={attack_ms}:release={release_ms}[ducked];"
        "[ducked][voice_audible]amix=inputs=2:normalize=0:duration=longest[aout]"
    )
