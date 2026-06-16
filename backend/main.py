from __future__ import annotations

import asyncio
import os
import json
import math
import secrets
import traceback
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

try:
    from .audio_utils import decode_base64_audio, encoded_audio_to_pcm16_16k_mono
    from .db import create_conversation, end_conversation, init_db, save_message
    from .doubao_realtime import DoubaoConfigError, DoubaoRealtimeClient, cancel_and_wait
    from .intent import detect_intent
    from .memory import ConversationMemory
except ImportError:
    from audio_utils import decode_base64_audio, encoded_audio_to_pcm16_16k_mono
    from db import create_conversation, end_conversation, init_db, save_message
    from doubao_realtime import DoubaoConfigError, DoubaoRealtimeClient, cancel_and_wait
    from intent import detect_intent
    from memory import ConversationMemory


ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / "frontend"


def _csv_env(name: str, default: str = "") -> list[str]:
    value = os.getenv(name, default)
    return [item.strip() for item in value.split(",") if item.strip()]


def _default_allowed_origins() -> str:
    port = os.getenv("PORT", "8000")
    return f"http://127.0.0.1:{port},http://localhost:{port}"


ALLOWED_ORIGINS = _csv_env("ALLOWED_ORIGINS", _default_allowed_origins())
WS_AUTH_TOKEN = os.getenv("WS_AUTH_TOKEN", "").strip()


def _is_allowed_origin(origin: str | None) -> bool:
    if not origin:
        return True
    return "*" in ALLOWED_ORIGINS or origin in ALLOWED_ORIGINS


def _is_ws_authorized(websocket: WebSocket) -> bool:
    if not _is_allowed_origin(websocket.headers.get("origin")):
        return False
    if not WS_AUTH_TOKEN:
        return True
    token = websocket.query_params.get("token") or websocket.headers.get("sec-websocket-protocol") or ""
    return secrets.compare_digest(token, WS_AUTH_TOKEN)

app = FastAPI(title="Doubao Realtime S2S Web Demo")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Authorization"],
)
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.on_event("startup")
async def startup() -> None:
    await asyncio.to_thread(init_db)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


async def safe_send_json(websocket: WebSocket, data: dict) -> bool:
    try:
        await websocket.send_json(data)
        return True
    except Exception:
        return False


@app.websocket("/ws/call")
async def call_ws(websocket: WebSocket) -> None:
    if not _is_ws_authorized(websocket):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    print("浏览器 WebSocket 已连接")
    browser_closed = False
    tasks: set[asyncio.Task] = set()
    db_tasks: set[asyncio.Task] = set()
    doubao = None
    memory = ConversationMemory()
    conversation_id: int | None = None
    pending_user_text: dict[str, str | int | None] | None = None
    saved_user_questions: set[str] = set()
    assistant_text_buffers: dict[str, dict[str, str | int | None]] = {}
    browser_audio_packets_received = 0
    browser_audio_bytes_received = 0
    mobile_audio_packets_received = 0
    mobile_audio_bytes_received = 0

    def schedule_db_write(func, *args) -> None:
        task = asyncio.create_task(asyncio.to_thread(func, *args))
        db_tasks.add(task)
        task.add_done_callback(db_tasks.discard)

    try:
        doubao = DoubaoRealtimeClient()
        print("准备连接豆包 WebSocket")
        await doubao.connect()
        conversation_id = await asyncio.to_thread(create_conversation, doubao.session_id)
        if conversation_id:
            print(f"conversation created id={conversation_id}, session_id={doubao.session_id}")
        print("豆包 WebSocket 连接成功")
    except DoubaoConfigError as exc:
        print("豆包 WebSocket 连接失败")
        traceback.print_exc()
        await safe_send_json(websocket, {"type": "error", "message": f"{type(exc).__name__}: {exc}"})
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
        return
    except Exception as exc:
        print("豆包 WebSocket 连接失败")
        traceback.print_exc()
        await safe_send_json(
            websocket,
            {"type": "error", "message": f"failed to connect Doubao WebSocket: {type(exc).__name__}: {exc}"},
        )
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
        return

    await safe_send_json(
        websocket,
        {
            "type": "ready",
            "session_id": doubao.session_id,
            "input_sample_rate": doubao.config.input_sample_rate,
            "output_sample_rate": doubao.config.output_sample_rate,
        },
    )

    def remember_user_text(event: dict) -> None:
        nonlocal pending_user_text
        text = str(event.get("text") or "").strip()
        if not text:
            return
        pending_user_text = {
            "text": text,
            "question_id": event.get("question_id"),
            "reply_id": event.get("reply_id"),
            "event_id": event.get("event"),
        }

    def save_pending_user_text(event: dict) -> None:
        nonlocal pending_user_text
        if not conversation_id or not pending_user_text:
            return
        text = str(pending_user_text.get("text") or "").strip()
        if not text:
            return
        question_id = pending_user_text.get("question_id") or event.get("question_id")
        reply_id = pending_user_text.get("reply_id") or event.get("reply_id")
        dedupe_key = str(question_id or reply_id or text)
        if dedupe_key in saved_user_questions:
            pending_user_text = None
            return
        saved_user_questions.add(dedupe_key)
        memory.add("user", text)
        schedule_db_write(save_message, conversation_id, "user", text, question_id, reply_id, 459)
        print(f"intent detected {json.dumps(detect_intent(text), ensure_ascii=False)}")
        pending_user_text = None

    def append_assistant_text(event: dict) -> None:
        text = str(event.get("text") or "")
        if not text:
            return
        reply_id = str(event.get("reply_id") or "__current__")
        item = assistant_text_buffers.setdefault(
            reply_id,
            {
                "text": "",
                "question_id": event.get("question_id"),
                "reply_id": event.get("reply_id"),
                "event_id": event.get("event"),
            },
        )
        item["text"] = str(item.get("text") or "") + text
        item["question_id"] = item.get("question_id") or event.get("question_id")
        item["reply_id"] = item.get("reply_id") or event.get("reply_id")

    def save_assistant_text(event: dict) -> None:
        if not conversation_id:
            return
        reply_id = str(event.get("reply_id") or "__current__")
        item = assistant_text_buffers.pop(reply_id, None)
        if item is None and len(assistant_text_buffers) == 1:
            _, item = assistant_text_buffers.popitem()
        if not item:
            return
        text = str(item.get("text") or "").strip()
        if not text:
            return
        question_id = item.get("question_id") or event.get("question_id")
        saved_reply_id = item.get("reply_id") or event.get("reply_id")
        memory.add("assistant", text)
        schedule_db_write(save_message, conversation_id, "assistant", text, question_id, saved_reply_id, event.get("event") or 559)

    async def browser_to_doubao() -> None:
        nonlocal browser_closed, browser_audio_packets_received, browser_audio_bytes_received
        nonlocal mobile_audio_packets_received, mobile_audio_bytes_received
        while True:
            try:
                message = await websocket.receive()
            except WebSocketDisconnect:
                browser_closed = True
                print("浏览器已断开，停止接收")
                break
            except RuntimeError as exc:
                if 'Cannot call "receive" once a disconnect message has been received' in str(exc):
                    browser_closed = True
                    print("浏览器已断开，停止接收")
                    break
                raise

            if message.get("type") == "websocket.disconnect":
                browser_closed = True
                print("浏览器已断开，停止接收")
                break

            if "bytes" in message and message["bytes"]:
                browser_audio_packets_received += 1
                browser_audio_bytes_received += len(message["bytes"])
                await doubao.send_audio(message["bytes"])
                continue

            text = message.get("text")
            if not text:
                continue
            if text == "__stop__":
                await doubao.finish_input()
                continue
            if text == "__ping__":
                await safe_send_json(websocket, {"type": "pong"})
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("type") == "mobile_audio_file":
                audio_base64 = payload.get("audio")
                if not isinstance(audio_base64, str) or not audio_base64:
                    await safe_send_json(websocket, {"type": "error", "message": "mobile_audio_file missing audio"})
                    continue
                filename = str(payload.get("name") or "expo-recording.m4a")
                suffix = Path(filename).suffix or ".m4a"
                encoded_audio = decode_base64_audio(audio_base64)
                pcm16 = await encoded_audio_to_pcm16_16k_mono(encoded_audio, suffix=suffix)
                mobile_packets = max(1, math.ceil(len(pcm16) / 640))
                mobile_audio_packets_received += mobile_packets
                mobile_audio_bytes_received += len(pcm16)
                print(
                    "mobile audio converted "
                    f"encodedBytes={len(encoded_audio)}, pcm16Bytes={len(pcm16)}, packets={mobile_packets}"
                )
                await doubao.send_audio(pcm16)
                await doubao.finish_input()
                await safe_send_json(
                    websocket,
                    {
                        "type": "mobile_audio_received",
                        "pcm16Bytes": len(pcm16),
                        "packets": mobile_packets,
                        "sampleRate": 16000,
                    },
                )
                continue

    async def audio_stats_logger() -> None:
        last_browser_audio_packets = 0
        last_browser_audio_bytes = 0
        while not browser_closed:
            await asyncio.sleep(2)
            audio_packets_per_sec = browser_audio_packets_received - last_browser_audio_packets
            audio_bytes_per_sec = browser_audio_bytes_received - last_browser_audio_bytes
            task_requests_per_sec = doubao.task_requests_sent_last_second if doubao else 0
            if doubao:
                doubao.task_requests_sent_last_second = 0
            last_browser_audio_packets = browser_audio_packets_received
            last_browser_audio_bytes = browser_audio_bytes_received
            print(
                "audio stats "
                f"windowSec=2, "
                f"browserAudioPacketsPerSec={audio_packets_per_sec}, "
                f"browserAudioBytesPerSec={audio_bytes_per_sec}, "
                f"doubaoTaskRequestsPerSec={task_requests_per_sec}, "
                f"browserAudioPacketsReceived={browser_audio_packets_received}, "
                f"browserAudioBytesReceived={browser_audio_bytes_received}, "
                f"mobileAudioPacketsReceived={mobile_audio_packets_received}, "
                f"mobileAudioBytesReceived={mobile_audio_bytes_received}, "
                f"doubaoTaskRequestsSent={doubao.task_requests_sent if doubao else 0}"
            )

    async def doubao_to_browser() -> None:
        nonlocal browser_closed
        async for event in doubao.events():
            if browser_closed:
                break
            if event["type"] == "user_text":
                remember_user_text(event)
                if not event.get("is_interim"):
                    save_pending_user_text(event)
            elif event["type"] == "user_speech_end":
                save_pending_user_text(event)
            elif event["type"] == "assistant_text":
                append_assistant_text(event)
            elif event["type"] == "assistant_text_done":
                save_assistant_text(event)
            sent = await safe_send_json(websocket, event)
            if not sent:
                browser_closed = True
                break

    try:
        upstream = asyncio.create_task(browser_to_doubao())
        downstream = asyncio.create_task(doubao_to_browser())
        stats_task = asyncio.create_task(audio_stats_logger())
        tasks = {upstream, downstream, stats_task}
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in done:
            try:
                error = task.exception()
            except asyncio.CancelledError:
                continue
            if not error:
                continue
            if isinstance(error, WebSocketDisconnect):
                browser_closed = True
                continue
            if browser_closed:
                continue
            traceback.print_exception(type(error), error, error.__traceback__)
            await safe_send_json(websocket, {"type": "error", "message": f"{type(error).__name__}: {error}"})
    except Exception as error:
        if not browser_closed:
            traceback.print_exception(type(error), error, error.__traceback__)
            await safe_send_json(websocket, {"type": "error", "message": f"{type(error).__name__}: {error}"})
    finally:
        if tasks:
            await cancel_and_wait(*tasks)
        if conversation_id:
            schedule_db_write(end_conversation, conversation_id, None)
        if db_tasks:
            await asyncio.gather(*db_tasks, return_exceptions=True)
        try:
            await doubao.close()
        except Exception:
            if not browser_closed:
                traceback.print_exc()
