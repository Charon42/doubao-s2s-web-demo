import React, { useMemo, useRef, useState } from "react";
import {
  SafeAreaView,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  TouchableOpacity,
  View
} from "react-native";

import { createAudioPlayer } from "./src/audioPlayer";
import { createAudioRecorder } from "./src/audioRecorder";
import { getWebSocketUrl } from "./src/config";
import type { ChatMessage, ServerMessage } from "./src/types";

type ConnectionState = "idle" | "connecting" | "connected" | "closed" | "error";

const makeId = () => `${Date.now()}-${Math.random().toString(16).slice(2)}`;

export default function App() {
  const [connectionState, setConnectionState] = useState<ConnectionState>("idle");
  const [statusText, setStatusText] = useState("未连接");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [logs, setLogs] = useState<string[]>([]);

  const wsRef = useRef<WebSocket | null>(null);
  const isResettingRef = useRef(false);
  const currentUserMessageIdRef = useRef<string | null>(null);
  const currentAssistantMessageIdRef = useRef<string | null>(null);
  const currentAssistantReplyIdRef = useRef<string | number | null>(null);

  const audioPlayer = useMemo(() => createAudioPlayer((message, data) => addLog(message, data)), []);
  const audioRecorder = useMemo(
    () =>
      createAudioRecorder((chunk) => {
        const ws = wsRef.current;
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(chunk);
        }
      }),
    []
  );

  const wsUrl = getWebSocketUrl();
  const canStart = connectionState === "idle" || connectionState === "closed" || connectionState === "error";
  const canStop = connectionState === "connecting" || connectionState === "connected";
  const statusDotStyle = {
    idle: styles.status_idle,
    connecting: styles.status_connecting,
    connected: styles.status_connected,
    closed: styles.status_closed,
    error: styles.status_error
  }[connectionState];

  function addLog(message: string, data?: unknown) {
    const line = `[${new Date().toLocaleTimeString()}] ${message}${data ? ` ${JSON.stringify(data)}` : ""}`;
    setLogs((prev) => [...prev.slice(-80), line]);
  }

  function appendOrUpdateUserText(text: string, isInterim: boolean) {
    if (!text.trim()) return;
    setMessages((prev) => {
      const activeId = currentUserMessageIdRef.current;
      if (activeId) {
        const next = prev.map((item) =>
          item.id === activeId ? { ...item, text, interim: isInterim } : item
        );
        if (!isInterim) currentUserMessageIdRef.current = null;
        return next;
      }

      const id = makeId();
      if (isInterim) currentUserMessageIdRef.current = id;
      return [...prev, { id, role: "user", text, interim: isInterim }];
    });
  }

  function appendAssistantText(text: string, replyId?: string | number | null) {
    if (!text.trim()) return;
    const effectiveReplyId = replyId ?? currentAssistantReplyIdRef.current;

    setMessages((prev) => {
      const existingIndex =
        effectiveReplyId == null
          ? prev.findIndex((item) => item.id === currentAssistantMessageIdRef.current)
          : prev.findIndex(
              (item) => item.role === "assistant" && String(item.replyId) === String(effectiveReplyId)
            );

      if (existingIndex >= 0) {
        const existing = prev[existingIndex];
        currentAssistantMessageIdRef.current = existing.id;
        currentAssistantReplyIdRef.current = existing.replyId ?? effectiveReplyId ?? null;
        return prev.map((item, index) =>
          index === existingIndex ? { ...item, text: `${item.text}${text}` } : item
        );
      }

      const id = makeId();
      currentAssistantMessageIdRef.current = id;
      currentAssistantReplyIdRef.current = effectiveReplyId ?? null;
      return [
        ...prev,
        {
          id,
          role: "assistant",
          text,
          replyId: effectiveReplyId ?? null
        }
      ];
    });
  }

  function resetCallState() {
    if (isResettingRef.current) return;
    isResettingRef.current = true;
    addLog("reset call state");

    currentUserMessageIdRef.current = null;
    currentAssistantMessageIdRef.current = null;
    currentAssistantReplyIdRef.current = null;
    audioPlayer.reset();
    audioRecorder.reset().catch((err) => addLog("audio recorder reset failed", { message: err.message }));

    const ws = wsRef.current;
    if (ws) {
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
        addLog("close old websocket");
        ws.close();
      }
      wsRef.current = null;
    }

    setConnectionState("closed");
    setStatusText("连接已关闭");
    isResettingRef.current = false;
  }

  function handleServerMessage(raw: string) {
    let msg: ServerMessage;
    try {
      msg = JSON.parse(raw) as ServerMessage;
    } catch (err) {
      addLog("invalid server message", { raw });
      return;
    }

    if (msg.type === "ready") {
      setConnectionState("connected");
      setStatusText(msg.session_id ? `已连接：${msg.session_id}` : "已连接");
      addLog("backend ready", msg);
      return;
    }

    if (msg.type === "user_text") {
      const text = typeof msg.text === "string" ? msg.text : "";
      appendOrUpdateUserText(text, !!msg.is_interim);
      addLog("user_text", { text: msg.text, isInterim: !!msg.is_interim, replyId: msg.reply_id ?? null });
      return;
    }

    if (msg.type === "assistant_text") {
      const text = typeof msg.text === "string" ? msg.text : "";
      const replyId = typeof msg.reply_id === "string" || typeof msg.reply_id === "number" ? msg.reply_id : null;
      appendAssistantText(text, replyId);
      addLog("assistant_text", { text: msg.text, replyId });
      return;
    }

    if (msg.type === "assistant_text_done") {
      currentAssistantMessageIdRef.current = null;
      currentAssistantReplyIdRef.current = null;
      addLog("assistant_text_done", { replyId: msg.reply_id ?? null });
      return;
    }

    if (msg.type === "user_speech_start") {
      currentAssistantMessageIdRef.current = null;
      currentAssistantReplyIdRef.current = null;
      audioPlayer.stopAll();
      addLog("user_speech_start", { replyId: msg.reply_id ?? null });
      return;
    }

    if (msg.type === "audio_wav") {
      const audio = typeof msg.audio === "string" ? msg.audio : "";
      audioPlayer.enqueueWavBase64(audio);
      addLog("audio_wav received", { replyId: msg.reply_id ?? null, sampleRate: msg.sampleRate ?? null });
      return;
    }

    if (msg.type === "audio") {
      // Raw PCM is kept for future native playback. Expo Go plays the
      // backend-provided audio_wav message first.
      addLog("audio received", { replyId: msg.reply_id ?? null, sampleRate: msg.sample_rate ?? null });
      return;
    }

    if (msg.type === "mobile_audio_received") {
      addLog("mobile_audio_received", {
        pcm16Bytes: msg.pcm16Bytes ?? 0,
        packets: msg.packets ?? 0,
        sampleRate: msg.sampleRate ?? null
      });
      return;
    }

    if (msg.type === "tts_done") {
      addLog("tts_done", { replyId: msg.reply_id ?? null });
      return;
    }

    if (msg.type === "error") {
      const message = msg.message || msg.detail || "后端返回错误";
      setConnectionState("error");
      setStatusText(String(message));
      addLog("error", msg);
      return;
    }

    addLog("event", msg);
  }

  async function startCall() {
    const granted = await audioRecorder.requestPermission();
    if (!granted) {
      setConnectionState("error");
      setStatusText("请先授予麦克风权限");
      addLog("microphone permission denied");
      return;
    }

    const currentWs = wsRef.current;
    if (currentWs && currentWs.readyState === WebSocket.OPEN) {
      setStatusText("已经连接");
      addLog("already connected");
      return;
    }
    if (currentWs && currentWs.readyState === WebSocket.CONNECTING) {
      setStatusText("正在连接");
      addLog("websocket already connecting");
      return;
    }
    if (currentWs && (currentWs.readyState === WebSocket.CLOSING || currentWs.readyState === WebSocket.CLOSED)) {
      resetCallState();
    }

    currentUserMessageIdRef.current = null;
    currentAssistantMessageIdRef.current = null;
    currentAssistantReplyIdRef.current = null;
    audioPlayer.reset();
    setConnectionState("connecting");
    setStatusText("正在连接后端...");
    addLog("create new websocket", { url: wsUrl });

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;
    ws.onopen = () => {
      setConnectionState("connected");
      setStatusText("WebSocket 已连接，等待 backend ready...");
      addLog("websocket open");
    };
    ws.onmessage = (event) => {
      handleServerMessage(String(event.data));
    };
    ws.onerror = () => {
      setConnectionState("error");
      setStatusText("WebSocket 错误");
      addLog("websocket error");
    };
    ws.onclose = () => {
      addLog("websocket closed");
      resetCallState();
    };
  }

  function stopCall() {
    addLog("stop call");
    audioPlayer.stopAll();
    audioRecorder.stop().catch((err) => addLog("audio recorder stop failed", { message: err.message }));

    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close();
      addLog("websocket closed");
    }
    resetCallState();
  }

  async function recordTestAudio() {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setStatusText("请先开始通话并连接后端");
      addLog("record test skipped: websocket not open");
      return;
    }

    try {
      setStatusText("正在录 2 秒测试音频...");
      addLog("record test audio start");
      const recorded = await audioRecorder.recordTestClip(2000);
      ws.send(
        JSON.stringify({
          type: "mobile_audio_file",
          name: recorded.name,
          mimeType: recorded.mimeType,
          audio: recorded.base64
        })
      );
      setStatusText("测试音频已发送，等待豆包回复");
      addLog("record test audio sent", { uri: recorded.uri, bytesBase64: recorded.base64.length });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setConnectionState("error");
      setStatusText(message);
      addLog("record test audio failed", { message });
    }
  }

  return (
    <SafeAreaView style={styles.safeArea}>
      <StatusBar barStyle="dark-content" />
      <View style={styles.container}>
        <View style={styles.header}>
          <Text style={styles.title}>豆包实时语音</Text>
          <Text style={styles.urlText}>{wsUrl}</Text>
          <View style={styles.statusRow}>
            <View style={[styles.statusDot, statusDotStyle]} />
            <Text style={styles.statusText}>{statusText}</Text>
          </View>
        </View>

        <View style={styles.buttonRow}>
          <TouchableOpacity
            disabled={!canStart}
            onPress={startCall}
            style={[styles.button, styles.startButton, !canStart && styles.disabledButton]}
          >
            <Text style={styles.buttonText}>开始通话</Text>
          </TouchableOpacity>
          <TouchableOpacity
            disabled={!canStop}
            onPress={stopCall}
            style={[styles.button, styles.stopButton, !canStop && styles.disabledButton]}
          >
            <Text style={styles.buttonText}>停止</Text>
          </TouchableOpacity>
        </View>

        <TouchableOpacity
          disabled={!canStop}
          onPress={recordTestAudio}
          style={[styles.testRecordButton, !canStop && styles.disabledButton]}
        >
          <Text style={styles.buttonText}>录 2 秒测试音频</Text>
        </TouchableOpacity>

        <ScrollView style={styles.chatPanel} contentContainerStyle={styles.chatContent}>
          {messages.map((item) => (
            <View
              key={item.id}
              style={[
                styles.messageRow,
                item.role === "user" ? styles.userMessageRow : styles.assistantMessageRow
              ]}
            >
              <View
                style={[
                  styles.bubble,
                  item.role === "user" ? styles.userBubble : styles.assistantBubble,
                  item.interim && styles.interimBubble
                ]}
              >
                <Text style={item.role === "user" ? styles.userBubbleText : styles.assistantBubbleText}>
                  {item.text}
                </Text>
              </View>
            </View>
          ))}
        </ScrollView>

        <View style={styles.logPanel}>
          <Text style={styles.logTitle}>日志</Text>
          <ScrollView>
            {logs.map((line, index) => (
              <Text key={`${line}-${index}`} style={styles.logLine}>
                {line}
              </Text>
            ))}
          </ScrollView>
        </View>
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safeArea: {
    flex: 1,
    backgroundColor: "#f5f7fb"
  },
  container: {
    flex: 1,
    padding: 16,
    gap: 12
  },
  header: {
    gap: 8
  },
  title: {
    fontSize: 24,
    fontWeight: "700",
    color: "#14213d"
  },
  urlText: {
    fontSize: 12,
    color: "#5f6b7a"
  },
  statusRow: {
    minHeight: 36,
    borderRadius: 8,
    backgroundColor: "#ffffff",
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
    paddingHorizontal: 12
  },
  statusDot: {
    width: 10,
    height: 10,
    borderRadius: 5,
    backgroundColor: "#94a3b8"
  },
  status_idle: {
    backgroundColor: "#94a3b8"
  },
  status_connecting: {
    backgroundColor: "#f59e0b"
  },
  status_connected: {
    backgroundColor: "#16a34a"
  },
  status_closed: {
    backgroundColor: "#64748b"
  },
  status_error: {
    backgroundColor: "#dc2626"
  },
  statusText: {
    flex: 1,
    color: "#1f2937",
    fontSize: 14
  },
  buttonRow: {
    flexDirection: "row",
    gap: 12
  },
  button: {
    flex: 1,
    minHeight: 46,
    borderRadius: 8,
    alignItems: "center",
    justifyContent: "center"
  },
  startButton: {
    backgroundColor: "#2563eb"
  },
  stopButton: {
    backgroundColor: "#dc2626"
  },
  testRecordButton: {
    minHeight: 44,
    borderRadius: 8,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "#0f766e"
  },
  disabledButton: {
    backgroundColor: "#a8b1c0"
  },
  buttonText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700"
  },
  chatPanel: {
    flex: 1,
    borderRadius: 8,
    backgroundColor: "#ffffff"
  },
  chatContent: {
    padding: 12,
    gap: 10
  },
  messageRow: {
    width: "100%",
    flexDirection: "row"
  },
  userMessageRow: {
    justifyContent: "flex-end"
  },
  assistantMessageRow: {
    justifyContent: "flex-start"
  },
  bubble: {
    maxWidth: "82%",
    borderRadius: 8,
    paddingHorizontal: 12,
    paddingVertical: 10
  },
  userBubble: {
    backgroundColor: "#2563eb"
  },
  assistantBubble: {
    backgroundColor: "#e7edf5"
  },
  interimBubble: {
    opacity: 0.72
  },
  userBubbleText: {
    color: "#ffffff",
    fontSize: 15,
    lineHeight: 21
  },
  assistantBubbleText: {
    color: "#111827",
    fontSize: 15,
    lineHeight: 21
  },
  logPanel: {
    height: 150,
    borderRadius: 8,
    backgroundColor: "#111827",
    padding: 10
  },
  logTitle: {
    color: "#d1d5db",
    fontWeight: "700",
    marginBottom: 6
  },
  logLine: {
    color: "#cbd5e1",
    fontSize: 11,
    lineHeight: 16
  }
});
