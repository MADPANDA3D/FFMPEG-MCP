import os
import subprocess
import tempfile
import unittest

from audio_filters import audio_duck_filter_complex


class AudioMixWithBackgroundTests(unittest.TestCase):
    def test_ducking_filter_splits_voice_for_sidechain_and_audible_mix(self):
        filter_complex = audio_duck_filter_complex(
            sample_rate=48000,
            ratio=8.0,
            threshold=0.05,
            attack_ms=20,
            release_ms=250,
            music_gain=0.2,
            voice_gain=1.5,
        )

        self.assertIn("asplit=2[voice_sidechain][voice_mix]", filter_complex)
        self.assertIn("[voice_mix]volume=1.5[voice_audible]", filter_complex)
        self.assertIn("[music][voice_sidechain]sidechaincompress", filter_complex)
        self.assertIn("[ducked][voice_audible]amix", filter_complex)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "mixed.wav")
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "sine=frequency=220:duration=2",
                    "-f",
                    "lavfi",
                    "-i",
                    "sine=frequency=1000:duration=1",
                    "-filter_complex",
                    filter_complex,
                    "-map",
                    "[aout]",
                    "-c:a",
                    "pcm_s16le",
                    "-y",
                    output_path,
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertGreater(os.path.getsize(output_path), 44)


if __name__ == "__main__":
    unittest.main()
