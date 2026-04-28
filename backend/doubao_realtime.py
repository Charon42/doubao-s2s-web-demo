from __future__ import annotations

import asyncio
import base64
import gzip
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator

import websockets
from dotenv import load_dotenv
from websockets.asyncio.client import ClientConnection


ENV_PATH = Path(__file__).resolve().parent / ".env"
ENV_LOADED = load_dotenv(ENV_PATH)
if not ENV_LOADED:
    print(f".env 未读取到，当前工作目录: {Path.cwd()}")
    print(f".env 期望路径: {ENV_PATH}")


class DoubaoConfigError(RuntimeError):
    pass


VERSION = 0x1
HEADER_SIZE_UNITS = 0x1

MSG_FULL_CLIENT = 0x1
MSG_AUDIO_ONLY_CLIENT = 0x2
MSG_FULL_SERVER = 0x9
MSG_AUDIO_ONLY_SERVER = 0xB
MSG_FRONTEND_RESULT = 0xC
MSG_ERROR = 0xF

FLAG_NO_SEQUENCE = 0x0
FLAG_POSITIVE_SEQUENCE = 0x1
FLAG_NEGATIVE_SEQUENCE = 0x2
FLAG_NEGATIVE_WITH_SEQUENCE = 0x3
FLAG_WITH_EVENT = 0x4

SERIALIZATION_NONE = 0x0
SERIALIZATION_JSON = 0x1

COMPRESSION_NONE = 0x0
COMPRESSION_GZIP = 0x1

EVENT_START_CONNECTION = 1
EVENT_FINISH_CONNECTION = 2
EVENT_START_SESSION = 100
EVENT_FINISH_SESSION = 102
EVENT_TASK_REQUEST = 200
EVENT_SAY_HELLO = 300
EVENT_END_ASR = 400
EVENT_CHAT_TEXT_QUERY = 501
EVENT_CONVERSATION_CREATE = 510
EVENT_CLIENT_INTERRUPT = 515

EVENT_CONNECTION_STARTED = 50
EVENT_CONNECTION_FAILED = 51
EVENT_CONNECTION_ENDED = 52
EVENT_SESSION_STARTED = 150
EVENT_TTS_ENDED = 359
EVENT_ASR_INFO = 450
EVENT_TTS_RESPONSE = 352
EVENT_ASR_RESPONSE = 451
EVENT_ASR_ENDED = 459
EVENT_CHAT_RESPONSE = 550
EVENT_CHAT_ENDED = 559
EVENT_DIALOG_COMMON_ERROR = 599

PCM_16K_20MS_BYTES = 640
DOUBAO_WS_MAX_QUEUE = 64
DOUBAO_WS_WRITE_LIMIT = 256 * 1024
INTERRUPT_DEDUPE_WINDOW_SEC = 0.8


def _read_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


DEBUG = _read_env("DEBUG", "false").lower() in {"1", "true", "yes", "on"}


def _mask_secret(secret: str) -> str:
    if not secret:
        return "未读取到"
    if len(secret) <= 10:
        return f"已读取到: {secret[:2]}***{secret[-2:]}"
    return f"已读取到: {secret[:6]}***{secret[-4:]}"


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _has_session_id_field(event: int) -> bool:
    return event not in {
        EVENT_START_CONNECTION,
        EVENT_FINISH_CONNECTION,
        EVENT_CONNECTION_STARTED,
        EVENT_CONNECTION_FAILED,
        EVENT_CONNECTION_ENDED,
    }


def _has_connect_id_field(event: int) -> bool:
    return event in {EVENT_CONNECTION_STARTED, EVENT_CONNECTION_FAILED, EVENT_CONNECTION_ENDED}


@dataclass(frozen=True)
class DoubaoPacket:
    message_type: int
    flags: int
    serialization: int
    compression: int
    event: int | None
    session_id: str
    connect_id: str
    payload: bytes
    error_code: int | None = None


def encode_packet(
    *,
    event: int,
    payload: bytes,
    session_id: str = "",
    connect_id: str = "",
    message_type: int = MSG_FULL_CLIENT,
    serialization: int = SERIALIZATION_JSON,
    compression: int = COMPRESSION_NONE,
) -> bytes:
    """Encode Volcengine realtime dialogue binary packet.

    Packet shape follows the documented 4-byte header + optional event fields +
    payload size + payload structure. Numeric fields are big-endian.
    """
    if message_type == MSG_AUDIO_ONLY_CLIENT:
        serialization = SERIALIZATION_NONE

    body = bytearray()
    body.extend(event.to_bytes(4, "big", signed=True))

    if _has_session_id_field(event):
        session_id_bytes = session_id.encode("utf-8")
        body.extend(len(session_id_bytes).to_bytes(4, "big"))
        body.extend(session_id_bytes)

    if connect_id:
        connect_id_bytes = connect_id.encode("utf-8")
        body.extend(len(connect_id_bytes).to_bytes(4, "big"))
        body.extend(connect_id_bytes)

    if compression == COMPRESSION_GZIP and payload:
        payload = gzip.compress(payload)

    body.extend(len(payload).to_bytes(4, "big"))
    body.extend(payload)

    return bytes(
        [
            (VERSION << 4) | HEADER_SIZE_UNITS,
            (message_type << 4) | FLAG_WITH_EVENT,
            (serialization << 4) | compression,
            0x00,
        ]
    ) + bytes(body)


def decode_packet(data: bytes) -> DoubaoPacket:
    if len(data) < 8:
        raise ValueError(f"packet too short: {len(data)}")

    header_size = (data[0] & 0x0F) * 4
    if header_size <= 0 or len(data) < header_size:
        raise ValueError(f"invalid header size: {header_size}")

    message_type = (data[1] >> 4) & 0x0F
    flags = data[1] & 0x0F
    serialization = (data[2] >> 4) & 0x0F
    compression = data[2] & 0x0F
    offset = header_size

    if flags in {FLAG_POSITIVE_SEQUENCE, FLAG_NEGATIVE_SEQUENCE, FLAG_NEGATIVE_WITH_SEQUENCE}:
        if len(data) < offset + 4:
            raise ValueError("missing sequence field")
        offset += 4

    event: int | None = None
    session_id = ""
    connect_id = ""
    if flags & FLAG_WITH_EVENT:
        if len(data) < offset + 4:
            raise ValueError("missing event field")
        event = int.from_bytes(data[offset : offset + 4], "big", signed=True)
        offset += 4

        if _has_session_id_field(event):
            session_id, offset = _read_sized_string(data, offset, "session id")

        if _has_connect_id_field(event):
            connect_id, offset = _read_sized_string(data, offset, "connect id")

    error_code: int | None = None
    if message_type == MSG_ERROR:
        if len(data) < offset + 4:
            raise ValueError("missing error code")
        error_code = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4

    if len(data) < offset + 4:
        raise ValueError("missing payload size")
    payload_size = int.from_bytes(data[offset : offset + 4], "big")
    offset += 4
    if payload_size < 0 or len(data) < offset + payload_size:
        raise ValueError(f"invalid payload size: {payload_size}")

    payload = data[offset : offset + payload_size]
    if compression == COMPRESSION_GZIP and payload:
        payload = gzip.decompress(payload)

    return DoubaoPacket(
        message_type=message_type,
        flags=flags,
        serialization=serialization,
        compression=compression,
        event=event,
        session_id=session_id,
        connect_id=connect_id,
        payload=payload,
        error_code=error_code,
    )


def _read_sized_string(data: bytes, offset: int, field_name: str) -> tuple[str, int]:
    if len(data) < offset + 4:
        raise ValueError(f"missing {field_name} length")
    size = int.from_bytes(data[offset : offset + 4], "big")
    offset += 4
    if len(data) < offset + size:
        raise ValueError(f"invalid {field_name} length: {size}")
    return data[offset : offset + size].decode("utf-8"), offset + size


@dataclass(frozen=True)
class DoubaoRealtimeConfig:
    ws_url: str
    app_id: str
    access_key: str
    resource_id: str
    app_key: str
    model_version: str
    speaker: str
    bot_name: str
    system_role: str
    speaking_style: str
    greeting: str
    input_mod: str
    input_sample_rate: int
    output_sample_rate: int

    @classmethod
    def from_env(cls) -> "DoubaoRealtimeConfig":
        config = cls(
            ws_url=_read_env("DOUBAO_WS_URL", "wss://openspeech.bytedance.com/api/v3/realtime/dialogue"),
            app_id=_read_env("DOUBAO_APP_ID"),
            access_key=_read_env("DOUBAO_ACCESS_KEY"),
            resource_id=_read_env("DOUBAO_RESOURCE_ID", "volc.speech.dialog"),
            app_key=_read_env("DOUBAO_APP_KEY"),
            model_version=_read_env("DOUBAO_MODEL_VERSION", "1.2.1.1"),
            speaker=_read_env("DOUBAO_SPEAKER", "zh_female_vv_jupiter_bigtts"),
            bot_name=_read_env("DOUBAO_BOT_NAME"),
            system_role=_read_env("DOUBAO_SYSTEM_ROLE"),
            speaking_style=_read_env("DOUBAO_SPEAKING_STYLE"),
            greeting=_read_env("DOUBAO_GREETING"),
            input_mod=_read_env("DOUBAO_INPUT_MOD", "keep_alive"),
            input_sample_rate=int(_read_env("DOUBAO_INPUT_SAMPLE_RATE", "16000")),
            output_sample_rate=24000,
        )
        config.validate()
        return config

    def validate(self) -> None:
        missing = []
        if not self.ws_url:
            missing.append("DOUBAO_WS_URL")
        if not self.app_id:
            missing.append("DOUBAO_APP_ID")
        if not self.access_key:
            missing.append("DOUBAO_ACCESS_KEY")
        if not self.resource_id:
            missing.append("DOUBAO_RESOURCE_ID")
        if not self.app_key:
            missing.append("DOUBAO_APP_KEY")
        if missing:
            raise DoubaoConfigError(
                "missing required env: "
                + ", ".join(missing)
                + f"; 当前工作目录={Path.cwd()}; .env路径={ENV_PATH}"
            )


def build_headers(config: DoubaoRealtimeConfig, connect_id: str) -> dict[str, str]:
    return {
        "X-Api-App-ID": config.app_id,
        "X-Api-Access-Key": config.access_key,
        "X-Api-Resource-Id": config.resource_id,
        "X-Api-App-Key": config.app_key,
        "X-Api-Connect-Id": connect_id,
    }


class DoubaoRealtimeClient:
    def __init__(self, config: DoubaoRealtimeConfig | None = None) -> None:
        self.config = config or DoubaoRealtimeConfig.from_env()
        self.connect_id = uuid.uuid4().hex
        self.session_id = uuid.uuid4().hex
        self.ws: ClientConnection | None = None
        self._audio_buffer = bytearray()
        self.current_question_id: str | None = None
        self.current_reply_id: str | None = None
        self.task_requests_sent = 0
        self.task_requests_sent_last_second = 0
        self.last_audio_send_ts = 0.0
        self.last_interrupt_key: str | None = None
        self.last_interrupt_ts = 0.0

    async def connect(self) -> None:
        print(f"DOUBAO_S2S_WS_URL 是否为空: {not bool(self.config.ws_url)}")
        print(f"API Key 是否读取到: {_mask_secret(self.config.access_key)}")
        print("实际使用的 header 字段名: X-Api-App-ID, X-Api-Access-Key, X-Api-Resource-Id, X-Api-App-Key, X-Api-Connect-Id")
        headers = build_headers(self.config, self.connect_id)
        self.ws = await websockets.connect(
            self.config.ws_url,
            additional_headers=headers,
            max_queue=DOUBAO_WS_MAX_QUEUE,
            write_limit=DOUBAO_WS_WRITE_LIMIT,
        )
        await self._send_event(EVENT_START_CONNECTION, {})
        packet = await self._recv_packet()
        if packet.event != EVENT_CONNECTION_STARTED:
            raise RuntimeError(f"unexpected StartConnection response event={packet.event}, payload={_payload_text(packet)}")
        if packet.connect_id:
            self.connect_id = packet.connect_id
        start_session_payload = self._start_session_payload()
        self._log_persona_config()
        print(f"StartSession tts.audio_config: {start_session_payload['tts']['audio_config']}")
        print(f"StartSession dialog.extra: {start_session_payload['dialog']['extra']}")
        await self._send_event(EVENT_START_SESSION, start_session_payload)
        packet = await self._recv_packet()
        if packet.event != EVENT_SESSION_STARTED:
            raise RuntimeError(f"unexpected StartSession response event={packet.event}, payload={_payload_text(packet)}")
        if packet.session_id:
            self.session_id = packet.session_id
        if self.config.greeting:
            await self._send_event(EVENT_SAY_HELLO, {"content": self.config.greeting})

    async def close(self) -> None:
        if not self.ws:
            return
        try:
            await self._send_event(EVENT_FINISH_SESSION, {"session_id": self.session_id})
            await self._send_event(EVENT_FINISH_CONNECTION, {})
        except Exception:
            pass
        await self.ws.close()
        self.ws = None

    async def send_audio(self, pcm16: bytes) -> None:
        if not self.ws:
            return
        self._audio_buffer.extend(pcm16)
        while len(self._audio_buffer) >= PCM_16K_20MS_BYTES:
            chunk = bytes(self._audio_buffer[:PCM_16K_20MS_BYTES])
            del self._audio_buffer[:PCM_16K_20MS_BYTES]
            await self._send_audio_chunk(chunk)

    async def finish_input(self) -> None:
        if not self.ws:
            return
        if self._audio_buffer:
            await self._send_audio_chunk(bytes(self._audio_buffer))
            self._audio_buffer.clear()
        await self._send_event(EVENT_END_ASR, {"session_id": self.session_id})

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        if not self.ws:
            return
        async for message in self.ws:
            if isinstance(message, str):
                yield {"type": "error", "message": message}
                continue
            packet = decode_packet(message)
            payload = _decode_payload_json(packet.payload)
            self._log_server_event(packet, payload)
            self._update_current_ids(payload)
            if packet.event == EVENT_ASR_INFO:
                interrupt_score = _extract_interrupt_score(payload)
                now = time.monotonic()
                interrupt_key = self.current_question_id or packet.session_id or self.session_id
                is_duplicate_interrupt = (
                    interrupt_key == self.last_interrupt_key
                    and now - self.last_interrupt_ts < INTERRUPT_DEDUPE_WINDOW_SEC
                )
                if is_duplicate_interrupt:
                    if DEBUG:
                        print(
                            "skip duplicate ASR_INFO interrupt "
                            f"question_id={self.current_question_id}, interrupt_score={interrupt_score}"
                        )
                    continue
                self.last_interrupt_key = interrupt_key
                self.last_interrupt_ts = now
                try:
                    await self._send_event(EVENT_CLIENT_INTERRUPT, {"session_id": self.session_id})
                except Exception as exc:
                    print(f"send ClientInterrupt failed: {type(exc).__name__}: {exc}")
                yield {
                    "type": "user_speech_start",
                    "event": packet.event,
                    "question_id": self.current_question_id,
                    "reply_id": self.current_reply_id,
                    "interrupt_score": interrupt_score,
                    "raw": payload,
                }
                continue
            event = self._packet_to_frontend_event(packet, payload)
            if event:
                yield event

    async def _send_event(self, event: int, payload: dict[str, Any]) -> None:
        if not self.ws:
            return
        if "session_id" not in payload and _has_session_id_field(event):
            payload = {**payload, "session_id": self.session_id}
        packet = encode_packet(event=event, session_id=self.session_id, payload=_json_bytes(payload))
        await self.ws.send(packet)

    async def _send_audio_chunk(self, chunk: bytes) -> None:
        if not self.ws:
            return
        packet = encode_packet(
            event=EVENT_TASK_REQUEST,
            session_id=self.session_id,
            payload=chunk,
            message_type=MSG_AUDIO_ONLY_CLIENT,
            serialization=SERIALIZATION_NONE,
        )
        await self.ws.send(packet)
        self.task_requests_sent += 1
        self.task_requests_sent_last_second += 1
        now = time.monotonic()
        if DEBUG and self.last_audio_send_ts:
            interval_ms = (now - self.last_audio_send_ts) * 1000
            if interval_ms < 10:
                print(f"audio send interval warning intervalMs={interval_ms:.2f}, chunkBytes={len(chunk)}")
        self.last_audio_send_ts = now

    async def _recv_packet(self) -> DoubaoPacket:
        if not self.ws:
            raise RuntimeError("Doubao WebSocket is not connected")
        message = await self.ws.recv()
        if isinstance(message, str):
            raise RuntimeError(f"unexpected text websocket message: {message}")
        return decode_packet(message)

    def _start_session_payload(self) -> dict[str, Any]:
        return {
            "asr": {
                "language": "zh-CN",
                # TODO: If the official doc requires explicit input audio config,
                # add it here using the documented field name.
            },
            "tts": {
                "speaker": self.config.speaker,
                "audio_config": {
                    "channel": 1,
                    "format": "pcm_s16le",
                    "sample_rate": 24000,
                    "bits": 16,
                },
            },
            "dialog": {
                "bot_name": self.config.bot_name,
                "system_role": self.config.system_role,
                "speaking_style": self.config.speaking_style,
                # TODO: Confirm exact model version field path in the official
                # realtime dialogue doc. Kept here because the doc exposes
                # DOUBAO_MODEL_VERSION as a required runtime option.
                "extra": {
                    "model": self.config.model_version,
                    "input_mod": self.config.input_mod,
                    "enable_conversation_truncate": True,
                },
            },
        }

    def _log_persona_config(self) -> None:
        print("已加载人设配置")
        print(f"bot_name: {self.config.bot_name}")
        print(f"system_role: {self.config.system_role[:30]}")
        print(f"speaking_style: {self.config.speaking_style[:30]}")
        print(f"greeting: {self.config.greeting[:30]}")

    def _update_current_ids(self, payload: dict[str, Any]) -> None:
        question_id = payload.get("question_id")
        reply_id = payload.get("reply_id")
        if isinstance(question_id, str) and question_id:
            self.current_question_id = question_id
        if isinstance(reply_id, str) and reply_id:
            self.current_reply_id = reply_id

    def _event_ids(self, payload: dict[str, Any]) -> dict[str, str | None]:
        question_id = payload.get("question_id")
        reply_id = payload.get("reply_id")
        return {
            "question_id": question_id if isinstance(question_id, str) and question_id else self.current_question_id,
            "reply_id": reply_id if isinstance(reply_id, str) and reply_id else self.current_reply_id,
        }

    def _log_server_event(self, packet: DoubaoPacket, payload: dict[str, Any]) -> None:
        if not DEBUG and packet.event in {EVENT_TTS_RESPONSE, EVENT_ASR_RESPONSE, EVENT_CHAT_RESPONSE}:
            return
        if packet.event == EVENT_TTS_RESPONSE:
            print(f"豆包事件 event_id={packet.event}, audio_bytes={len(packet.payload)}, payload_empty={not bool(packet.payload)}")
            return
        if payload:
            payload_text = json.dumps(payload, ensure_ascii=False)[:500]
        else:
            payload_text = _payload_text(packet)[:500]
        print(f"豆包事件 event_id={packet.event}, payload_empty={not bool(packet.payload)}, payload_500={payload_text}")

    def _packet_to_frontend_event(self, packet: DoubaoPacket, payload: dict[str, Any]) -> dict[str, Any] | None:
        ids = self._event_ids(payload)
        if packet.message_type == MSG_ERROR or packet.event == EVENT_DIALOG_COMMON_ERROR:
            return {
                "type": "error",
                "message": _error_message(payload, packet),
                "event": packet.event,
                **ids,
            }
        if packet.event == EVENT_ASR_RESPONSE:
            asr_text = _extract_asr_text(payload).strip()
            if not asr_text:
                if DEBUG:
                    print("451 empty text")
                return None
            return {
                "type": "user_text",
                "text": asr_text,
                "is_interim": _extract_asr_is_interim(payload),
                "event": packet.event,
                "raw": payload,
                **ids,
            }
        if packet.event == EVENT_CHAT_RESPONSE:
            content = str(payload.get("content") or "").strip()
            if not content:
                return None
            return {"type": "assistant_text", "text": content, "event": packet.event, "raw": payload, **ids}
        if packet.event == EVENT_TTS_RESPONSE:
            audio = packet.payload
            if DEBUG:
                print(f"?? TTSResponse ??????: {len(audio)}")
            if audio.startswith(b"OggS"):
                return {
                    "type": "error",
                    "message": "TTSResponse returned OGG/Opus, not pcm_s16le. Check StartSession tts.audio_config.",
                    "event": packet.event,
                    **ids,
                }
            return {
                "type": "audio",
                "audio": base64.b64encode(audio).decode("ascii"),
                "sample_rate": 24000,
                "event": packet.event,
                **ids,
            }
        if packet.event == EVENT_ASR_ENDED:
            return {"type": "user_speech_end", "event": packet.event, "raw": payload, **ids}
        if packet.event == EVENT_CHAT_ENDED:
            return {"type": "assistant_text_done", "event": packet.event, "raw": payload, **ids}
        if packet.event == EVENT_TTS_ENDED:
            return {"type": "tts_done", "event": packet.event, "raw": payload, **ids}
        if packet.event in {EVENT_CONNECTION_STARTED, EVENT_SESSION_STARTED}:
            return {"type": "event", "event": packet.event, "payload": payload or _payload_text(packet)}
        return {"type": "event", "event": packet.event, "payload": payload or _payload_text(packet), **ids}

def _decode_payload_json(payload: bytes) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _payload_text(packet: DoubaoPacket) -> str:
    try:
        return packet.payload.decode("utf-8")
    except UnicodeDecodeError:
        return f"<{len(packet.payload)} bytes>"


def _extract_text(payload: dict[str, Any]) -> str:
    for key in ("text", "content", "utterance", "transcript"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    for key in ("asr_info", "tts_info", "chat_info"):
        value = payload.get(key)
        if isinstance(value, dict):
            text = _extract_text(value)
            if text:
                return text
    return ""


def _extract_asr_text(payload: dict[str, Any]) -> str:
    results = payload.get("results")
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict) and isinstance(first.get("text"), str):
            return first["text"]
    return ""


def _extract_interrupt_score(payload: dict[str, Any]) -> float | None:
    for source in (payload, payload.get("asr_info")):
        if not isinstance(source, dict):
            continue
        value = source.get("interrupt_score")
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None
    return None


def _extract_asr_is_interim(payload: dict[str, Any]) -> bool:
    results = payload.get("results")
    first = results[0] if isinstance(results, list) and results else {}
    if isinstance(first, dict):
        for key in ("is_interim", "interim"):
            value = first.get(key)
            if isinstance(value, bool):
                return value
        for key in ("is_final", "final"):
            value = first.get(key)
            if isinstance(value, bool):
                return not value
    for key in ("is_interim", "interim"):
        value = payload.get(key)
        if isinstance(value, bool):
            return value
    for key in ("is_final", "final"):
        value = payload.get(key)
        if isinstance(value, bool):
            return not value
    return False


def _error_message(payload: dict[str, Any], packet: DoubaoPacket) -> str:
    if payload:
        for key in ("message", "error", "msg"):
            value = payload.get(key)
            if isinstance(value, str):
                return value
        return json.dumps(payload, ensure_ascii=False)
    if packet.error_code is not None:
        return f"error_code={packet.error_code}, payload={_payload_text(packet)}"
    return _payload_text(packet)


async def cancel_and_wait(*tasks: asyncio.Task[Any]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
