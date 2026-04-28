from __future__ import annotations

import asyncio
import base64
import subprocess
import tempfile
import wave
from pathlib import Path


def pcm16le_to_wav_bytes(pcm: bytes, sample_rate: int = 24000, channels: int = 1) -> bytes:
    """Wrap raw PCM16 little-endian audio bytes in a WAV container."""
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        wav_path = Path(tmp.name)
    try:
        with wave.open(str(wav_path), "wb") as wav:
            wav.setnchannels(channels)
            wav.setsampwidth(2)
            wav.setframerate(sample_rate)
            wav.writeframes(pcm)
        return wav_path.read_bytes()
    finally:
        wav_path.unlink(missing_ok=True)


def decode_base64_audio(data: str) -> bytes:
    if "," in data and data.lstrip().startswith("data:"):
        data = data.split(",", 1)[1]
    return base64.b64decode(data)


async def encoded_audio_to_pcm16_16k_mono(encoded_audio: bytes, suffix: str = ".m4a") -> bytes:
    """Convert Expo recorded audio to PCM16 little-endian / 16000 Hz / mono.

    Requires ffmpeg to be available on PATH. This is intentionally a demo bridge:
    Expo Go can record a compressed file, but it does not provide realtime
    PCM16/16k/20ms buffers. Realtime voice should use a Dev Client or a native
    AudioRecord/AudioTrack module.
    """
    suffix = suffix if suffix.startswith(".") else f".{suffix}"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as src:
        src.write(encoded_audio)
        src_path = Path(src.name)
    with tempfile.NamedTemporaryFile(suffix=".pcm", delete=False) as dst:
        dst_path = Path(dst.name)

    try:
        command = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(src_path),
            "-f",
            "s16le",
            "-acodec",
            "pcm_s16le",
            "-ac",
            "1",
            "-ar",
            "16000",
            str(dst_path),
        ]
        process = await asyncio.to_thread(
            subprocess.run,
            command,
            capture_output=True,
            check=False,
        )
        if process.returncode != 0:
            error = process.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"ffmpeg convert failed: {error}")
        return dst_path.read_bytes()
    finally:
        src_path.unlink(missing_ok=True)
        dst_path.unlink(missing_ok=True)
