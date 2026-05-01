const startBtn = document.querySelector("#startBtn");
const stopBtn = document.querySelector("#stopBtn");
const statusText = document.querySelector("#statusText");
const chatMessages = document.querySelector("#chatMessages");
const logEl = document.querySelector("#log");

let ws = null;
let inputContext = null;
let playbackAudioContext = null;
let mediaStream = null;
let sourceNode = null;
let processorNode = null;
let outputSampleRate = 24000;
let audioScheduleChain = Promise.resolve();
let audioPlaybackGeneration = 0;
let currentReplyPlayback = null;
let playbackSummaryTimer = null;
let perfStatsTimer = null;
let audioSender = null;
const PLAYBACK_SAMPLE_RATE = 24000;
const INPUT_SAMPLE_RATE = 16000;
const AUDIO_PACKET_BYTES = 640;
const AUDIO_PACKET_INTERVAL_MS = 20;
const MAX_AUDIO_QUEUE_PACKETS = 20;
const DEBUG = new URLSearchParams(location.search).get("debug") === "1" || localStorage.getItem("DEBUG") === "true";
const CallState = Object.freeze({
  IDLE: "IDLE",
  LISTENING: "LISTENING",
  THINKING: "THINKING",
  SPEAKING: "SPEAKING",
  INTERRUPTED: "INTERRUPTED",
  CLOSED: "CLOSED",
});
const MIN_START_BUFFER_SEC = 0.25;
const LONG_REPLY_START_BUFFER_SEC = 0.45;
const MIN_BUFFER_DELAY_SEC = 0.03;
const LOW_BUFFER_AHEAD_SEC = 0.05;
const RECONNECT_DELAY_SEC = 0.06;
const TAIL_LOW_BUFFER_AHEAD_SEC = 0.12;
const TAIL_RECONNECT_DELAY_SEC = 0.08;
const SMALL_CHUNK_BYTES = 4096;
const SMALL_CHUNK_FLUSH_BYTES = 8192;
const TAIL_SMALL_FLUSH_DELAY_MS = 100;
const TAIL_SILENCE_MS = 20;
const TAIL_FADE_MS = 30;
const MAX_TTS_QUEUE_CHUNKS = 200;
const MAX_TTS_BUFFER_SEC = 15;
let currentReplyId = null;
let waitingForNewReply = false;
let interrupted = false;
let interruptedReplyId = null;
let finalUserText = "";
let interimUserText = "";
let audioPacketsSent = 0;
let audioBytesSent = 0;
let audioPacketsDroppedByBackpressure = 0;
let maxWsBufferedAmount = 0;
let browserAudioPacketsReceived = 0;
let browserAudioBytesReceived = 0;
let audioStatsTimer = null;
let noAudioTimer = null;
let wsReady = false;
let backendReady = false;
let recordingStarted = false;
let micProcessorActive = false;
let userStartRequested = false;
let isResetting = false;
let interimUserMessageEl = null;
let activeAssistantMessageEl = null;
let activeAssistantReplyId = null;
let assistantTextByReplyId = new Map();
let latestUserInterimText = "";
let userInterimFlushTimer = null;
let pendingAssistantTextByReplyId = new Map();
let assistantTextFlushTimer = null;
let pageLogLines = [];
let pendingLogLines = [];
let logFlushTimer = null;
let roundId = 0;
let callState = CallState.IDLE;
let currentSessionId = null;
let wsListenersAttached = false;
let wsSendCount = 0;
let wsSendCountLastSecond = 0;
let audioPacketsSentLastSecond = 0;

class AudioSender {
  constructor(getSocket) {
    this.getSocket = getSocket;
    this.queue = [];
    this.remainder = new Uint8Array(0);
    this.intervalId = null;
    this.generation = 0;
    this.droppedPackets = 0;
    this.lastBackpressureWarnAt = 0;
  }

  get isRunning() {
    return this.intervalId !== null;
  }

  get queueLength() {
    return this.queue.length;
  }

  start() {
    if (this.intervalId !== null) {
      console.warn("[audio sender] duplicate send loop detected; stopping old loop");
      this.stop({ clearQueue: false });
    }
    this.generation += 1;
    const generation = this.generation;
    this.intervalId = setInterval(() => {
      if (generation !== this.generation) return;
      this.flushOne();
    }, AUDIO_PACKET_INTERVAL_MS);
  }

  stop({ clearQueue = true } = {}) {
    if (this.intervalId !== null) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.generation += 1;
    if (clearQueue) {
      this.queue = [];
      this.remainder = new Uint8Array(0);
    }
  }

  enqueuePcm16(pcm16) {
    if (!pcm16 || !pcm16.byteLength) return;
    const input = new Uint8Array(pcm16.buffer, pcm16.byteOffset, pcm16.byteLength);
    const combined = new Uint8Array(this.remainder.length + input.length);
    combined.set(this.remainder, 0);
    combined.set(input, this.remainder.length);

    let offset = 0;
    while (offset + AUDIO_PACKET_BYTES <= combined.length) {
      const packet = combined.slice(offset, offset + AUDIO_PACKET_BYTES);
      this.queue.push(packet.buffer);
      offset += AUDIO_PACKET_BYTES;
    }
    this.remainder = combined.slice(offset);

    if (this.queue.length > MAX_AUDIO_QUEUE_PACKETS) {
      const dropCount = this.queue.length - MAX_AUDIO_QUEUE_PACKETS;
      this.queue.splice(0, dropCount);
      this.droppedPackets += dropCount;
      audioPacketsDroppedByBackpressure += dropCount;
      console.warn("[audio sender] drop old packets by backpressure", {
        dropCount,
        queueLength: this.queue.length,
      });
    }
  }

  flushOne() {
    if (!recordingStarted || this.queue.length === 0) return;
    const socket = this.getSocket();
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    const bufferedAmount = socket.bufferedAmount || 0;
    if (bufferedAmount > maxWsBufferedAmount) maxWsBufferedAmount = bufferedAmount;
    if (bufferedAmount > 2 * 1024 * 1024) {
      this.queue.shift();
      this.droppedPackets += 1;
      audioPacketsDroppedByBackpressure += 1;
      const now = performance.now();
      if (now - this.lastBackpressureWarnAt > 1000) {
        this.lastBackpressureWarnAt = now;
        console.warn("[audio sender] drop packet by websocket backpressure", {
          bufferedAmount,
          queueLength: this.queue.length,
        });
      }
      return;
    }
    const packet = this.queue.shift();
    safeWsSend(packet, "audio");
    audioPacketsSent += 1;
    audioPacketsSentLastSecond += 1;
    audioBytesSent += packet.byteLength;
  }
}

function log(message, data) {
  const line = `[${new Date().toLocaleTimeString()}] ${message}`;
  let suffix = "";
  if (data) {
    try {
      const json = JSON.stringify(data);
      suffix = ` ${json.length > 500 ? `${json.slice(0, 500)}...` : json}`;
    } catch (err) {
      suffix = " <unserializable>";
    }
  }
  pendingLogLines.push(`${line}${suffix}`);
  if (!logFlushTimer) {
    logFlushTimer = setTimeout(flushPageLog, 500);
  }
}

function debugLog(message, data) {
  if (DEBUG) log(message, data);
}

function setCallState(nextState, reason = "") {
  if (callState === nextState) return;
  callState = nextState;
  debugLog("state changed", { state: callState, reason });
}

function safeWsSend(data, kind = "control") {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  ws.send(data);
  wsSendCount += 1;
  wsSendCountLastSecond += 1;
  if (DEBUG && kind !== "audio") debugLog("ws send", { kind });
  return true;
}

function flushPageLog() {
  logFlushTimer = null;
  if (!pendingLogLines.length) return;
  pageLogLines.push(...pendingLogLines);
  pendingLogLines = [];
  if (pageLogLines.length > 100) {
    pageLogLines = pageLogLines.slice(-100);
  }
  logEl.textContent = `${pageLogLines.join("\n")}\n`;
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(text) {
  statusText.textContent = text;
}

function websocketUrl() {
  const protocol = location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${location.host}/ws/call`;
}

function floatToPcm16(float32) {
  const pcm = new Int16Array(float32.length);
  for (let i = 0; i < float32.length; i += 1) {
    const sample = Math.max(-1, Math.min(1, float32[i]));
    pcm[i] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
  }
  return pcm;
}

function downsampleTo16k(input, inputSampleRate) {
  if (inputSampleRate === 16000) return input;
  const ratio = inputSampleRate / 16000;
  const outputLength = Math.floor(input.length / ratio);
  const output = new Float32Array(outputLength);
  for (let i = 0; i < outputLength; i += 1) {
    const start = Math.floor(i * ratio);
    const end = Math.min(Math.floor((i + 1) * ratio), input.length);
    let sum = 0;
    for (let j = start; j < end; j += 1) sum += input[j];
    output[i] = sum / Math.max(1, end - start);
  }
  return output;
}

function base64ToArrayBuffer(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
}

function base64DecodedByteLength(base64) {
  if (!base64) return 0;
  const padding = base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0;
  return Math.floor((base64.length * 3) / 4) - padding;
}

function pcm16ToFloat32(arrayBuffer) {
  if (arrayBuffer.byteLength % 2 !== 0) {
    arrayBuffer = arrayBuffer.slice(0, arrayBuffer.byteLength - 1);
  }
  const view = new DataView(arrayBuffer);
  const floats = new Float32Array(Math.floor(arrayBuffer.byteLength / 2));
  for (let i = 0; i < floats.length; i += 1) {
    floats[i] = view.getInt16(i * 2, true) / 0x8000;
  }
  return floats;
}

async function getPlaybackContext(sampleRate = 24000) {
  if (!playbackAudioContext || playbackAudioContext.state === "closed") {
    playbackAudioContext = new AudioContext({ sampleRate: PLAYBACK_SAMPLE_RATE });
  }
  if (playbackAudioContext.state === "suspended") {
    await playbackAudioContext.resume();
  }
  return playbackAudioContext;
}

function createReplyPlayback(replyId) {
  const ctxTime = playbackAudioContext && playbackAudioContext.state !== "closed" ? playbackAudioContext.currentTime : 0;
  return {
    replyId,
    pendingChunks: [],
    pendingSmallChunks: [],
    pendingSmallBytes: 0,
    totalPendingDuration: 0,
    bufferedDuration: 0,
    started: false,
    nextPlayTime: ctxTime ? ctxTime + TAIL_LOW_BUFFER_AHEAD_SEC : 0,
    contextPrimed: !!ctxTime,
    activeSources: new Set(),
    chunkCount: 0,
    bytes: 0,
    lastSummaryAt: 0,
    textDone: false,
    ttsDone: false,
    tailMode: false,
    underrunCount: 0,
    createdAt: performance.now(),
    smallFlushTimer: null,
    delayedChunk: null,
    generation: audioPlaybackGeneration,
  };
}

function initReplyPlayback(replyId, reason) {
  if (!replyId) return null;
  if (currentReplyPlayback && currentReplyPlayback.replyId === replyId) return currentReplyPlayback;
  if (currentReplyPlayback && interrupted) {
    stopAllAudio();
  }
  if (!playbackAudioContext || playbackAudioContext.state === "closed") {
    playbackAudioContext = new AudioContext({ sampleRate: PLAYBACK_SAMPLE_RATE });
  }
  const ctx = playbackAudioContext;
  currentReplyPlayback = createReplyPlayback(replyId);
  currentReplyPlayback.nextPlayTime = ctx.currentTime + TAIL_LOW_BUFFER_AHEAD_SEC;
  currentReplyPlayback.contextPrimed = true;
  startPlaybackSummaryTimer(currentReplyPlayback);
  if (DEBUG) console.log("[reply start]", { replyId, currentTime: ctx.currentTime, reason });
  return currentReplyPlayback;
}

function getOrCreateReplyPlayback(replyId) {
  const effectiveReplyId =
    replyId ||
    (currentReplyPlayback && currentReplyPlayback.replyId) ||
    currentReplyId ||
    "__unknown__";
  if (!currentReplyPlayback || currentReplyPlayback.replyId !== effectiveReplyId) {
    currentReplyPlayback = createReplyPlayback(effectiveReplyId);
    startPlaybackSummaryTimer(currentReplyPlayback);
    if (DEBUG) console.log("[reply start]", { replyId: effectiveReplyId, currentTime: playbackAudioContext ? playbackAudioContext.currentTime : null, reason: "implicit" });
  }
  return currentReplyPlayback;
}

function decodePcm16Chunk(base64Audio, sampleRate = PLAYBACK_SAMPLE_RATE) {
  let arrayBuffer = base64ToArrayBuffer(base64Audio);
  if (arrayBuffer.byteLength % 2 !== 0) {
    arrayBuffer = arrayBuffer.slice(0, arrayBuffer.byteLength - 1);
  }
  const bytes = new Uint8Array(arrayBuffer);
  if (bytes.length >= 4 && bytes[0] === 0x4f && bytes[1] === 0x67 && bytes[2] === 0x67 && bytes[3] === 0x53) {
    log("audio format error", { message: "received OGG/Opus, expected pcm_s16le" });
    return null;
  }
  const floats = pcm16ToFloat32(arrayBuffer);
  return {
    floats,
    bytes: arrayBuffer.byteLength,
    arrayBuffer,
    sampleRate: sampleRate || PLAYBACK_SAMPLE_RATE,
    duration: floats.length / (sampleRate || PLAYBACK_SAMPLE_RATE),
  };
}

function pcmArrayBufferToChunk(arrayBuffer, sampleRate = PLAYBACK_SAMPLE_RATE) {
  if (arrayBuffer.byteLength % 2 !== 0) {
    arrayBuffer = arrayBuffer.slice(0, arrayBuffer.byteLength - 1);
  }
  const floats = pcm16ToFloat32(arrayBuffer);
  return {
    floats,
    bytes: arrayBuffer.byteLength,
    arrayBuffer,
    sampleRate,
    duration: floats.length / sampleRate,
  };
}

function makeSilenceChunk(durationMs = TAIL_SILENCE_MS) {
  const bytes = Math.floor((PLAYBACK_SAMPLE_RATE * durationMs * 2) / 1000);
  const evenBytes = bytes % 2 === 0 ? bytes : bytes + 1;
  return pcmArrayBufferToChunk(new ArrayBuffer(evenBytes), PLAYBACK_SAMPLE_RATE);
}

function fadeOutPcm16Bytes(bytes, validBytes, durationMs = TAIL_FADE_MS) {
  const evenBytes = validBytes % 2 === 0 ? validBytes : validBytes - 1;
  const sampleCount = Math.floor(evenBytes / 2);
  const fadeSamples = Math.min(sampleCount, Math.floor((PLAYBACK_SAMPLE_RATE * durationMs) / 1000));
  if (fadeSamples <= 0) return;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const firstFadeSample = sampleCount - fadeSamples;
  for (let i = 0; i < fadeSamples; i += 1) {
    const sampleIndex = firstFadeSample + i;
    const offset = sampleIndex * 2;
    const sample = view.getInt16(offset, true);
    const scale = 1 - (i + 1) / fadeSamples;
    view.setInt16(offset, Math.round(sample * scale), true);
  }
}

function mergePcmChunks(chunks, appendSilence = false) {
  const totalBytes = chunks.reduce((sum, chunk) => sum + chunk.bytes, 0);
  const silenceBytes = appendSilence ? makeSilenceChunk().bytes : 0;
  const merged = new Uint8Array(totalBytes + silenceBytes);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(new Uint8Array(chunk.arrayBuffer), offset);
    offset += chunk.bytes;
  }
  if (appendSilence) {
    fadeOutPcm16Bytes(merged, totalBytes);
  }
  let buffer = merged.buffer;
  if (buffer.byteLength % 2 !== 0) {
    buffer = buffer.slice(0, buffer.byteLength - 1);
  }
  return pcmArrayBufferToChunk(buffer, PLAYBACK_SAMPLE_RATE);
}

function prepareTailChunk(chunk) {
  if (!chunk) return null;
  const minTailBytes = makeSilenceChunk(Math.max(TAIL_SILENCE_MS, TAIL_FADE_MS)).bytes;
  const outputBytes = Math.max(chunk.bytes, minTailBytes);
  const output = new Uint8Array(outputBytes);
  output.set(new Uint8Array(chunk.arrayBuffer).slice(0, chunk.bytes), 0);
  fadeOutPcm16Bytes(output, chunk.bytes);
  return pcmArrayBufferToChunk(output.buffer, PLAYBACK_SAMPLE_RATE);
}

function clearSmallFlushTimer(playback) {
  if (playback && playback.smallFlushTimer) {
    clearTimeout(playback.smallFlushTimer);
    playback.smallFlushTimer = null;
  }
}

function flushPendingSmallChunks(playback, appendSilence = false) {
  if (!playback || playback.pendingSmallChunks.length === 0) return;
  clearSmallFlushTimer(playback);
  const merged = mergePcmChunks(playback.pendingSmallChunks, appendSilence);
  playback.pendingSmallChunks = [];
  playback.pendingSmallBytes = 0;
  playback.pendingChunks.push(merged);
  playback.totalPendingDuration += merged.duration;
  playback.bufferedDuration += merged.duration;
  trimTtsQueue(playback);
}

function queueDecodedChunk(playback, chunk) {
  if (!playback || !chunk) return;
  playback.chunkCount += 1;
  playback.bytes += chunk.bytes;
  if (chunk.bytes < SMALL_CHUNK_BYTES && !playback.ttsDone) {
    clearSmallFlushTimer(playback);
    playback.pendingSmallChunks.push(chunk);
    playback.pendingSmallBytes += chunk.bytes;
    if (playback.pendingSmallBytes >= SMALL_CHUNK_FLUSH_BYTES) {
      flushPendingSmallChunks(playback);
    }
  } else {
    playback.pendingChunks.push(chunk);
    playback.totalPendingDuration += chunk.duration;
    playback.bufferedDuration += chunk.duration;
  }
  trimTtsQueue(playback);
}

function flushDelayedTtsChunk(playback, asTail = false) {
  if (!playback || !playback.delayedChunk) return;
  const chunk = asTail ? prepareTailChunk(playback.delayedChunk) : playback.delayedChunk;
  playback.delayedChunk = null;
  queueDecodedChunk(playback, chunk);
}

function trimTtsQueue(playback) {
  if (!playback) return;
  while (
    playback.pendingChunks.length + playback.pendingSmallChunks.length > MAX_TTS_QUEUE_CHUNKS ||
    playback.bufferedDuration > MAX_TTS_BUFFER_SEC
  ) {
    const dropped = playback.pendingChunks.shift();
    if (!dropped) break;
    playback.bufferedDuration = Math.max(0, playback.bufferedDuration - dropped.duration);
    playback.totalPendingDuration = Math.max(0, playback.totalPendingDuration - dropped.duration);
    console.warn("[playback] drop old queued tts by backpressure", {
      replyId: playback.replyId,
      ttsQueueSize: getTtsQueueSize(),
      bufferedDuration: playback.bufferedDuration,
    });
  }
}

function scheduleTailSmallFlush(playback) {
  if (!playback || !playback.textDone || playback.ttsDone || playback.pendingSmallBytes <= 0 || playback.smallFlushTimer) {
    return;
  }
  playback.smallFlushTimer = setTimeout(() => {
    playback.smallFlushTimer = null;
    audioScheduleChain = audioScheduleChain
      .then(() => {
        flushPendingSmallChunks(playback, false);
        return flushReplyPlayback(playback, true);
      })
      .catch((err) => console.warn("[playback] tail small flush failed", { replyId: playback.replyId, message: err.message }));
  }, TAIL_SMALL_FLUSH_DELAY_MS);
}

function logPlaybackSummary(playback, ctx, force = false) {
  if (!DEBUG) return;
  const nowMs = performance.now();
  if (!force && nowMs - playback.lastSummaryAt < 1000) return;
  playback.lastSummaryAt = nowMs;
  const bufferAhead = ctx ? playback.nextPlayTime - ctx.currentTime : 0;
  console.log("[audio schedule]", {
    replyId: playback.replyId,
    chunkCount: playback.chunkCount,
    bytes: playback.bytes,
    bufferAhead,
    underrunCount: playback.underrunCount,
    pendingChunks: playback.pendingChunks.length,
    pendingSmallChunks: playback.pendingSmallChunks.length,
    pendingSmallBytes: playback.pendingSmallBytes,
    nextPlayTime: playback.nextPlayTime,
    currentTime: ctx ? ctx.currentTime : null,
    activeSources: playback.activeSources.size,
    textDone: playback.textDone,
    ttsDone: playback.ttsDone,
    tailMode: playback.tailMode,
  });
}

function startPlaybackSummaryTimer(playback) {
  if (playbackSummaryTimer) {
    clearInterval(playbackSummaryTimer);
    playbackSummaryTimer = null;
  }
  playbackSummaryTimer = setInterval(() => {
    if (!currentReplyPlayback || currentReplyPlayback !== playback) {
      clearInterval(playbackSummaryTimer);
      playbackSummaryTimer = null;
      return;
    }
    const ctx = playbackAudioContext && playbackAudioContext.state !== "closed" ? playbackAudioContext : null;
    logPlaybackSummary(playback, ctx, true);
  }, 1000);
}

function stopPlaybackSummaryTimer() {
  if (playbackSummaryTimer) {
    clearInterval(playbackSummaryTimer);
    playbackSummaryTimer = null;
  }
}

function startPerformanceTimer() {
  if (!DEBUG) return;
  if (perfStatsTimer) return;
  perfStatsTimer = setInterval(() => {
    const playback = currentReplyPlayback;
    const ctx = playbackAudioContext && playbackAudioContext.state !== "closed" ? playbackAudioContext : null;
    const bufferAhead = playback && ctx ? playback.nextPlayTime - ctx.currentTime : 0;
    const memory = performance.memory
      ? {
          usedJSHeapSize: performance.memory.usedJSHeapSize,
          totalJSHeapSize: performance.memory.totalJSHeapSize,
          jsHeapSizeLimit: performance.memory.jsHeapSizeLimit,
        }
      : null;
    console.log("[perf]", {
      roundId,
      state: callState,
      sessionId: currentSessionId,
      currentReplyId,
      wsBufferedAmount: ws ? ws.bufferedAmount : 0,
      audioPacketsSentPerSec: audioPacketsSentLastSecond,
      wsSendPerSec: wsSendCountLastSecond,
      audioQueueSize: audioSender ? audioSender.queueLength : 0,
      ttsQueueSize: getTtsQueueSize(),
      playbackLagMs: Math.max(0, -bufferAhead) * 1000,
      activeTimerCount: getActiveTimerCount(),
      activeWsListenerCount: wsListenersAttached ? 4 : 0,
      memoryUsage: memory,
      totalAudioPacketsSent: audioPacketsSent,
      totalWsSend: wsSendCount,
      audioPacketsDroppedByBackpressure,
      maxWsBufferedAmount,
      browserAudioPacketsReceived,
      browserAudioBytesReceived,
      activeSourcesSize: playback ? playback.activeSources.size : 0,
      pendingChunksLength: playback ? playback.pendingChunks.length : 0,
      pendingSmallBytes: playback ? playback.pendingSmallBytes : 0,
      currentReplyTextDone: playback ? playback.textDone : false,
      currentReplyTtsDone: playback ? playback.ttsDone : false,
      audioCtxState: ctx ? ctx.state : null,
      audioCtxCurrentTime: ctx ? ctx.currentTime : null,
      nextPlayTime: playback ? playback.nextPlayTime : null,
      bufferAhead,
      logDomCount: pageLogLines.length,
      recordingStarted,
      micProcessorActive,
    });
    audioPacketsSentLastSecond = 0;
    wsSendCountLastSecond = 0;
  }, 1000);
}

function stopPerformanceTimer() {
  if (perfStatsTimer) {
    clearInterval(perfStatsTimer);
    perfStatsTimer = null;
  }
}

function getTtsQueueSize() {
  const playback = currentReplyPlayback;
  if (!playback) return 0;
  return playback.pendingChunks.length + playback.pendingSmallChunks.length + playback.activeSources.size;
}

function getActiveTimerCount() {
  return [
    perfStatsTimer,
    playbackSummaryTimer,
    audioStatsTimer,
    noAudioTimer,
    userInterimFlushTimer,
    assistantTextFlushTimer,
    logFlushTimer,
    audioSender && audioSender.intervalId,
    currentReplyPlayback && currentReplyPlayback.smallFlushTimer,
  ].filter(Boolean).length;
}

function maybeCleanupReplyPlayback(playback) {
  if (
    playback &&
    playback.ttsDone &&
    playback.pendingChunks.length === 0 &&
    playback.pendingSmallChunks.length === 0 &&
    playback.activeSources.size === 0 &&
    currentReplyPlayback === playback
  ) {
    clearSmallFlushTimer(playback);
    if (DEBUG) console.log("[reply cleanup]", {
      replyId: playback.replyId,
      chunkCount: playback.chunkCount,
      bytes: playback.bytes,
      underrunCount: playback.underrunCount,
    });
    currentReplyPlayback = null;
    stopPlaybackSummaryTimer();
  }
}

function scheduleDecodedChunk(playback, chunk, ctx, isTailChunk = false) {
  if (!playback || playback.generation !== audioPlaybackGeneration) return;
  if (!playback.contextPrimed || playback.nextPlayTime <= 0) {
    playback.nextPlayTime = ctx.currentTime + TAIL_LOW_BUFFER_AHEAD_SEC;
    playback.contextPrimed = true;
  }
  const audioBuffer = ctx.createBuffer(1, chunk.floats.length, PLAYBACK_SAMPLE_RATE);
  audioBuffer.copyToChannel(chunk.floats, 0);
  const source = ctx.createBufferSource();
  const gain = ctx.createGain();
  const now = ctx.currentTime;
  let reconnectTimeline = false;
  let reconnectReason = "";
  let bufferAheadBefore = playback.nextPlayTime - now;
  if (playback.nextPlayTime <= now + 0.02) {
    playback.underrunCount += 1;
  }
  if (playback.tailMode && bufferAheadBefore < TAIL_LOW_BUFFER_AHEAD_SEC) {
    console.warn("[playback] low buffer ahead, reconnecting timeline", {
      replyId: playback.replyId,
      bufferAhead: bufferAheadBefore,
      tailMode: true,
    });
    playback.nextPlayTime = now + TAIL_RECONNECT_DELAY_SEC;
    reconnectTimeline = true;
    reconnectReason = "tail-low-buffer";
  } else if (bufferAheadBefore < LOW_BUFFER_AHEAD_SEC) {
    console.warn("[playback] low buffer ahead, reconnecting timeline", {
      replyId: playback.replyId,
      bufferAhead: bufferAheadBefore,
      tailMode: false,
    });
    playback.nextPlayTime = now + RECONNECT_DELAY_SEC;
    reconnectTimeline = true;
    reconnectReason = "low-buffer";
  }
  bufferAheadBefore = playback.nextPlayTime - now;
  const previousEndTime = playback.nextPlayTime;
  const startAt = Math.max(playback.nextPlayTime, now + MIN_BUFFER_DELAY_SEC);
  const endAt = startAt + audioBuffer.duration;
  const fadeIn = Math.min(0.005, audioBuffer.duration / 4);
  const fadeOut = isTailChunk ? Math.min(TAIL_FADE_MS / 1000, audioBuffer.duration / 2) : Math.min(0.005, audioBuffer.duration / 4);
  const playbackGap = startAt - now;
  const scheduledGap = previousEndTime > 0 ? startAt - previousEndTime : 0;

  source.buffer = audioBuffer;
  source.connect(gain);
  gain.connect(ctx.destination);
  gain.gain.setValueAtTime(0.0001, startAt);
  gain.gain.linearRampToValueAtTime(1, startAt + fadeIn);
  gain.gain.setValueAtTime(1, Math.max(startAt + fadeIn, endAt - fadeOut));
  gain.gain.linearRampToValueAtTime(0.0001, endAt);
  source.onended = () => {
    playback.activeSources.delete(source);
    try {
      source.disconnect();
      gain.disconnect();
    } catch (err) {
      // Source may already be disconnected after an interrupt.
    }
    if (DEBUG) console.log("[source ended]", { replyId: playback.replyId, activeSources: playback.activeSources.size });
    maybeCleanupReplyPlayback(playback);
  };
  playback.activeSources.add(source);
  source.start(startAt);
  playback.nextPlayTime = endAt;
  if (reconnectTimeline) {
    console.warn("[playback] reconnected", {
      replyId: playback.replyId,
      reconnectReason,
      bufferAheadBefore,
      startAt,
      nextPlayTime: playback.nextPlayTime,
      activeSources: playback.activeSources.size,
      underrunCount: playback.underrunCount,
      playbackGap,
      scheduledGap,
    });
  }
  return source;
}

async function flushReplyPlayback(playback, force = false) {
  if (!playback || playback.generation !== audioPlaybackGeneration) return;
  if (force || playback.ttsDone || playback.pendingSmallBytes >= SMALL_CHUNK_FLUSH_BYTES) {
    flushPendingSmallChunks(playback, playback.ttsDone);
  }
  const ctx = await getPlaybackContext(PLAYBACK_SAMPLE_RATE);
  if (playback.generation !== audioPlaybackGeneration) return;
  const dynamicStartBufferSec =
    playback.chunkCount >= 8 || playback.totalPendingDuration > 6 || playback.bytes / (PLAYBACK_SAMPLE_RATE * 2) > 6
      ? LONG_REPLY_START_BUFFER_SEC
      : MIN_START_BUFFER_SEC;
  const shouldStart =
    playback.started ||
    force ||
    playback.bufferedDuration >= dynamicStartBufferSec ||
    playback.pendingChunks.length >= 4;
  if (!shouldStart) {
    logPlaybackSummary(playback, ctx);
    return;
  }
  playback.started = true;
  while (playback.pendingChunks.length) {
    const chunk = playback.pendingChunks.shift();
    playback.bufferedDuration = Math.max(0, playback.bufferedDuration - chunk.duration);
    const isTailChunk =
      playback.ttsDone &&
      playback.pendingChunks.length === 0 &&
      playback.pendingSmallChunks.length === 0 &&
      playback.pendingSmallBytes === 0;
    scheduleDecodedChunk(playback, chunk, ctx, isTailChunk);
  }
  logPlaybackSummary(playback, ctx);
  maybeCleanupReplyPlayback(playback);
}

async function schedulePcm16Chunk(base64Audio, sampleRate = 24000, replyId = null, generation = audioPlaybackGeneration) {
  if (generation !== audioPlaybackGeneration) return null;
  const chunk = decodePcm16Chunk(base64Audio, PLAYBACK_SAMPLE_RATE);
  if (!chunk) return null;
  const playback = getOrCreateReplyPlayback(replyId);
  if (playback.generation !== generation) return null;
  if (playback.delayedChunk) {
    queueDecodedChunk(playback, playback.delayedChunk);
  }
  playback.delayedChunk = chunk;
  scheduleTailSmallFlush(playback);
  await flushReplyPlayback(playback);
  return null;
}

function playPcm16(base64Audio, sampleRate) {
  return schedulePcm16Chunk(base64Audio, sampleRate).catch((err) => {
    log("schedule pcm16 failed", { message: err.message });
    return null;
  });
}

function stopPlayback(markInterrupted = true) {
  const oldReplyId = currentReplyPlayback ? currentReplyPlayback.replyId : currentReplyId;
  const activeCount = currentReplyPlayback ? currentReplyPlayback.activeSources.size : 0;
  if (markInterrupted) {
    if (DEBUG) console.log("[interrupt]", { oldReplyId, activeSourcesBeforeClear: activeCount });
  }
  log("stop all audio");
  audioScheduleChain = Promise.resolve();
  audioPlaybackGeneration += 1;
  const sources = currentReplyPlayback ? [...currentReplyPlayback.activeSources] : [];
  for (const source of sources) {
    try {
      source.onended = null;
      source.stop();
      source.disconnect();
    } catch (err) {
      // Source may already be stopped.
    }
  }
  if (currentReplyPlayback) {
    clearSmallFlushTimer(currentReplyPlayback);
    currentReplyPlayback.pendingChunks = [];
    currentReplyPlayback.pendingSmallChunks = [];
    currentReplyPlayback.pendingSmallBytes = 0;
    currentReplyPlayback.bufferedDuration = 0;
    currentReplyPlayback.delayedChunk = null;
    currentReplyPlayback.activeSources.clear();
  }
  currentReplyPlayback = null;
  stopPlaybackSummaryTimer();
  if (markInterrupted) {
    interrupted = true;
    waitingForNewReply = true;
    interruptedReplyId = oldReplyId;
  }
}

function stopAllAudio(markInterrupted = true) {
  stopPlayback(markInterrupted);
}

function closePlaybackAudioContext() {
  if (playbackAudioContext && playbackAudioContext.state !== "closed") {
    playbackAudioContext.close().catch(() => {});
  }
  playbackAudioContext = null;
}

function updateButtons() {
  const state = ws ? ws.readyState : WebSocket.CLOSED;
  const connectedOrConnecting = state === WebSocket.OPEN || state === WebSocket.CONNECTING;
  startBtn.disabled = connectedOrConnecting;
  stopBtn.disabled = !connectedOrConnecting;
}

function stopRecording() {
  log("stop recording");
  recordingStarted = false;
  micProcessorActive = false;
  if (audioSender) audioSender.stop();
  if (audioStatsTimer) clearInterval(audioStatsTimer);
  if (noAudioTimer) clearTimeout(noAudioTimer);
  audioStatsTimer = null;
  noAudioTimer = null;
  if (userInterimFlushTimer) {
    clearTimeout(userInterimFlushTimer);
    userInterimFlushTimer = null;
  }
  if (assistantTextFlushTimer) {
    clearTimeout(assistantTextFlushTimer);
    assistantTextFlushTimer = null;
  }
  if (processorNode) {
    try {
      processorNode.onaudioprocess = null;
      processorNode.disconnect();
    } catch (err) {
      // Node may already be disconnected.
    }
  }
  if (sourceNode) {
    try {
      sourceNode.disconnect();
    } catch (err) {
      // Node may already be disconnected.
    }
  }
  if (mediaStream) mediaStream.getTracks().forEach((track) => track.stop());
  if (inputContext) inputContext.close().catch(() => {});
  processorNode = null;
  sourceNode = null;
  mediaStream = null;
  inputContext = null;
  updateButtons();
}

function resetCallState() {
  if (isResetting) return;
  isResetting = true;
  log("reset call state");

  recordingStarted = false;
  wsReady = false;
  backendReady = false;
  waitingForNewReply = false;
  interrupted = false;
  interruptedReplyId = null;
  currentReplyId = null;
  currentSessionId = null;
  activeAssistantReplyId = null;
  interimUserMessageEl = null;
  activeAssistantMessageEl = null;
  audioScheduleChain = Promise.resolve();
  audioPacketsSent = 0;
  audioBytesSent = 0;
  userStartRequested = false;
  finalUserText = "";
  interimUserText = "";

  stopRecording();
  stopAllAudio(false);
  closePlaybackAudioContext();
  stopPerformanceTimer();
  if (logFlushTimer) {
    clearTimeout(logFlushTimer);
    flushPageLog();
  }

  if (ws) {
    const oldWs = ws;
    oldWs.onopen = null;
    oldWs.onmessage = null;
    oldWs.onerror = null;
    oldWs.onclose = null;
    wsListenersAttached = false;
    if (oldWs.readyState === WebSocket.OPEN || oldWs.readyState === WebSocket.CONNECTING) {
      log("close old websocket");
      oldWs.close();
    }
    ws = null;
  }

  setStatus("连接已关闭");
  setCallState(CallState.CLOSED, "resetCallState");
  updateButtons();
  isResetting = false;
}

function setCurrentReplyId(replyId, reason) {
  if (!replyId || replyId === currentReplyId) return;
  log("set currentReplyId", { from: currentReplyId, to: replyId, reason });
  currentReplyId = replyId;
}

function switchToNewReply(replyId, reason) {
  if (!replyId) return;
  const changed = replyId !== currentReplyId;
  if (changed) {
    setCurrentReplyId(replyId, reason);
  }
  initReplyPlayback(replyId, reason);
  if (interrupted) {
    interrupted = false;
    interruptedReplyId = null;
    log("interrupted false", { replyId, reason });
  }
  if (waitingForNewReply) {
    waitingForNewReply = false;
    log("waitingForNewReply false", { replyId, reason });
  }
}

function enqueueAudio(msg) {
  const replyId = msg.reply_id || null;
  const bytes = base64DecodedByteLength(msg.audio);
  const playbackReplyId = currentReplyPlayback && currentReplyPlayback.replyId;
  browserAudioPacketsReceived += 1;
  browserAudioBytesReceived += bytes;

  if (interrupted && waitingForNewReply && !replyId) {
    console.warn("[playback] drop audio without reply_id while waiting for new reply");
    return;
  }

  if (interrupted && interruptedReplyId && replyId === interruptedReplyId) {
    console.warn("[playback] drop interrupted old reply audio", { replyId, interruptedReplyId });
    return;
  }

  if (!currentReplyId && replyId) {
    setCurrentReplyId(replyId, "first audio");
    initReplyPlayback(replyId, "first audio");
  } else if (waitingForNewReply && replyId) {
    switchToNewReply(replyId, "audio while waitingForNewReply");
  } else if (interrupted && replyId) {
    switchToNewReply(replyId, "audio after interrupt");
  }

  if (
    interrupted &&
    currentReplyId &&
    replyId &&
    replyId !== currentReplyId &&
    replyId !== playbackReplyId
  ) {
    console.warn("[playback] drop interrupted stale audio", { replyId, currentReplyId, playbackReplyId });
    return;
  }
  if (interrupted && playbackReplyId && replyId && replyId !== playbackReplyId) {
    return;
  }
  const sampleRate = msg.sample_rate || outputSampleRate || 24000;
  const generation = audioPlaybackGeneration;
  audioScheduleChain = audioScheduleChain
    .then(() => schedulePcm16Chunk(msg.audio, sampleRate, replyId, generation))
    .catch((err) => log("schedule pcm16 failed", { message: err.message, replyId }));
}

function scrollChatToBottom() {
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function createMessageElement(role, text, options = {}) {
  const row = document.createElement("div");
  row.className = `chat-row ${role}`;
  if (options.interim) row.classList.add("interim");
  if (options.replyId) row.dataset.replyId = options.replyId;

  const roleEl = document.createElement("div");
  roleEl.className = "chat-role";
  roleEl.textContent = role === "user" ? "你" : "小蔚";

  const bubble = document.createElement("div");
  bubble.className = "chat-bubble";
  bubble.textContent = text;

  row.append(roleEl, bubble);
  return row;
}

function appendMessage(role, text, options = {}) {
  const clean = text || "";
  if (!clean.trim()) return null;
  const row = createMessageElement(role, clean, options);
  chatMessages.appendChild(row);
  scrollChatToBottom();
  return row;
}

function updateLastMessage(role, text, options = {}) {
  const row = options.element || [...chatMessages.querySelectorAll(`.chat-row.${role}`)].at(-1);
  if (!row) return appendMessage(role, text, options);
  const bubble = row.querySelector(".chat-bubble");
  if (bubble) bubble.textContent = text || "";
  row.classList.toggle("interim", !!options.interim);
  if (options.replyId) row.dataset.replyId = options.replyId;
  scrollChatToBottom();
  return row;
}

function appendOrUpdateInterimUserText(text, isFinal = false) {
  if (!text || !text.trim()) return;
  if (!interimUserMessageEl) {
    interimUserMessageEl = appendMessage("user", text, { interim: !isFinal });
  } else {
    updateLastMessage("user", text, { element: interimUserMessageEl, interim: !isFinal });
  }
  if (isFinal && interimUserMessageEl) {
    interimUserMessageEl.classList.remove("interim");
    interimUserMessageEl = null;
  }
}

function flushUserInterimText() {
  userInterimFlushTimer = null;
  if (!latestUserInterimText) return;
  appendOrUpdateInterimUserText(latestUserInterimText, false);
}

function handleUserTextMessage(msg) {
  const text = msg.text || "";
  if (!text.trim()) return;
  if (msg.is_interim) {
    latestUserInterimText = text;
    if (!userInterimFlushTimer) {
      userInterimFlushTimer = setTimeout(flushUserInterimText, 100);
    }
    return;
  }
  if (userInterimFlushTimer) {
    clearTimeout(userInterimFlushTimer);
    userInterimFlushTimer = null;
  }
  latestUserInterimText = "";
  appendOrUpdateInterimUserText(text, true);
  log("user_text final", { text, replyId: msg.reply_id || null });
}

function appendOrUpdateAssistantText(text, replyId) {
  if (!text || !text.trim()) return;
  if (!activeAssistantMessageEl || (replyId && activeAssistantReplyId !== replyId)) {
    activeAssistantReplyId = replyId || activeAssistantReplyId;
    activeAssistantMessageEl = createMessageElement("assistant", "", { replyId: activeAssistantReplyId });
    chatMessages.appendChild(activeAssistantMessageEl);
  }
  const current = assistantTextByReplyId.get(activeAssistantReplyId) || "";
  const next = current + text;
  assistantTextByReplyId.set(activeAssistantReplyId, next);
  updateLastMessage("assistant", next, { element: activeAssistantMessageEl, replyId: activeAssistantReplyId });
}

function flushAssistantText() {
  assistantTextFlushTimer = null;
  for (const [replyId, text] of pendingAssistantTextByReplyId.entries()) {
    appendOrUpdateAssistantText(text, replyId === "__current__" ? currentReplyId : replyId);
  }
  pendingAssistantTextByReplyId.clear();
}

function queueAssistantText(text, replyId) {
  if (!text || !text.trim()) return;
  const key = replyId || "__current__";
  pendingAssistantTextByReplyId.set(key, (pendingAssistantTextByReplyId.get(key) || "") + text);
  if (!assistantTextFlushTimer) {
    assistantTextFlushTimer = setTimeout(flushAssistantText, 100);
  }
}

function flushTextQueues() {
  if (userInterimFlushTimer) {
    clearTimeout(userInterimFlushTimer);
    flushUserInterimText();
  }
  if (assistantTextFlushTimer) {
    clearTimeout(assistantTextFlushTimer);
    flushAssistantText();
  }
}

function finishAssistantMessage() {
  activeAssistantMessageEl = null;
  activeAssistantReplyId = null;
}

function markReplyTextDone(replyId, reason) {
  const playback = currentReplyPlayback;
  if (!playback) return;
  if (replyId && playback.replyId && replyId !== playback.replyId) return;
  playback.textDone = true;
  playback.tailMode = true;
  if (DEBUG) console.log("[text done]", {
    replyId: playback.replyId,
    reason,
    pendingChunks: playback.pendingChunks.length,
    pendingSmallChunks: playback.pendingSmallChunks.length,
    activeSources: playback.activeSources.size,
  });
  scheduleTailSmallFlush(playback);
}

function markReplyTtsDone(replyId, reason) {
  const playback = currentReplyPlayback;
  if (!playback) return;
  if (replyId && playback.replyId && replyId !== playback.replyId) return;
  playback.ttsDone = true;
  playback.tailMode = true;
  if (DEBUG) console.log("[tts done]", {
    replyId: playback.replyId,
    reason,
    pendingChunks: playback.pendingChunks.length,
    pendingSmallChunks: playback.pendingSmallChunks.length,
    hasDelayedChunk: !!playback.delayedChunk,
    activeSources: playback.activeSources.size,
  });
  audioScheduleChain = audioScheduleChain
    .then(() => {
      flushDelayedTtsChunk(playback, true);
      return flushReplyPlayback(playback, true);
    })
    .catch((err) => log("flush tts audio failed", { message: err.message, replyId: playback.replyId }));
}

function resetChatMessages() {
  chatMessages.textContent = "";
  interimUserMessageEl = null;
  activeAssistantMessageEl = null;
  activeAssistantReplyId = null;
  assistantTextByReplyId = new Map();
  pendingAssistantTextByReplyId.clear();
  latestUserInterimText = "";
}

function maybeStartRecording() {
  if (userStartRequested && wsReady && backendReady && !recordingStarted) {
    startRecording().catch((err) => {
      setStatus("启动失败");
      log("start recording failed", { message: err.message });
      cleanupAudio();
    });
  }
}

function handleServerMessage(event) {
  let msg;
  try {
    msg = JSON.parse(event.data);
  } catch (err) {
    log("invalid server message", { raw: event.data, error: err.message });
    return;
  }
  if (msg.type === "ready") {
    backendReady = true;
    currentSessionId = msg.session_id || null;
    outputSampleRate = msg.output_sample_rate || outputSampleRate;
    setStatus(`?????? ${msg.session_id}`);
    log("backend ready", msg);
    updateButtons();
    maybeStartRecording();
    return;
  }
  if (msg.type === "user_text") {
    handleUserTextMessage(msg);
    return;
  }
  if (msg.type === "assistant_text") {
    setCallState(CallState.SPEAKING, "assistant_text");
    const text = msg.text || "";
    if (!text.trim()) return;
    if (msg.reply_id) {
      if (waitingForNewReply || interrupted || msg.reply_id !== currentReplyId) {
        switchToNewReply(msg.reply_id, "assistant_text while waitingForNewReply");
      } else {
        setCurrentReplyId(msg.reply_id, "assistant_text");
      }
    }
    queueAssistantText(text, msg.reply_id || currentReplyId);
    return;
  }
  if (msg.type === "audio") {
    if (msg.event !== 352) {
      log("drop non-352 audio message", { event: msg.event || null });
      return;
    }
    log("audio event 352 received", {
      replyId: msg.reply_id || null,
      sampleRate: msg.sample_rate || null,
      bytes: base64DecodedByteLength(msg.audio),
    });
    setCallState(CallState.SPEAKING, "audio");
    enqueueAudio(msg);
    return;
  }
  if (msg.type === "user_speech_start") {
    setCallState(CallState.INTERRUPTED, "user_speech_start");
    log("waitingForNewReply true", { currentReplyId, speechReplyId: msg.reply_id || null });
    stopAllAudio();
    log("interrupted true", { interruptedReplyId, currentReplyId });
    setCallState(CallState.LISTENING, "interrupt handled");
    interimUserText = "";
    interimUserMessageEl = null;
    log("user_speech_start ???????", { replyId: msg.reply_id || null, currentReplyId });
    return;
  }
  if (msg.type === "user_speech_end") {
    interimUserText = "";
    interimUserMessageEl = null;
    setCallState(CallState.THINKING, "user_speech_end");
    log("user speech end", msg);
    return;
  }
  if (msg.type === "assistant_text_done") {
    flushTextQueues();
    markReplyTextDone(msg.reply_id || currentReplyId, "assistant_text_done");
    finishAssistantMessage();
    log("assistant text done", msg);
    return;
  }
  if (msg.type === "tts_done") {
    markReplyTtsDone(msg.reply_id || currentReplyId, "tts_done");
    log("tts done", msg);
    setCallState(CallState.LISTENING, "tts_done");
    return;
  }
  if (msg.type === "event" && msg.event === 350) {
    const replyId = msg.reply_id || (msg.payload && msg.payload.reply_id) || null;
    if (replyId) {
      switchToNewReply(replyId, "TTSSentenceStart event=350");
    }
    log("event 350", { replyId, currentReplyId, waitingForNewReply, interrupted });
    return;
  }
  if (msg.type === "event" && msg.event === 351) {
    const replyId = msg.reply_id || (msg.payload && msg.payload.reply_id) || currentReplyId;
    const text = (msg.payload && (msg.payload.text || msg.payload.content)) || "";
    if (text && !assistantTextByReplyId.get(replyId)) {
      queueAssistantText(text, replyId);
    }
    if (DEBUG) console.debug("event 351", { replyId, text });
    return;
  }
  if (msg.type === "event" && msg.event === 559) {
    const replyId = msg.reply_id || (msg.payload && msg.payload.reply_id) || currentReplyId;
    flushTextQueues();
    markReplyTextDone(replyId, "event 559");
    finishAssistantMessage();
    log("event 559", { replyId });
    return;
  }
  if (msg.type === "event" && msg.event === 359) {
    const replyId = msg.reply_id || (msg.payload && msg.payload.reply_id) || currentReplyId;
    markReplyTtsDone(replyId, "event 359");
    log("event 359", msg);
    return;
  }
  if (msg.type === "error") {
    setStatus("??");
    log("error", { ...msg, raw: event.data });
    return;
  }
  if (DEBUG) {
    console.debug("unhandled server event", msg);
    log("event", { type: msg.type, event: msg.event || null });
  }
}

async function startRecording() {
  if (!wsReady || !backendReady || recordingStarted) return;
  if (micProcessorActive) return;
  stopRecording();
  recordingStarted = true;
  micProcessorActive = true;
  setCallState(CallState.LISTENING, "startRecording");
  audioPacketsSent = 0;
  audioBytesSent = 0;
  wsSendCount = 0;
  wsSendCountLastSecond = 0;
  audioPacketsSentLastSecond = 0;
  audioPacketsDroppedByBackpressure = 0;
  maxWsBufferedAmount = 0;
  if (audioStatsTimer) clearInterval(audioStatsTimer);
  if (noAudioTimer) clearTimeout(noAudioTimer);
  setStatus("请求麦克风权限...");

  mediaStream = await navigator.mediaDevices.getUserMedia({
    audio: {
      channelCount: 1,
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
    },
  });

  inputContext = new AudioContext();
  await inputContext.resume();
  noAudioTimer = setTimeout(() => {
    if (recordingStarted && audioPacketsSent === 0) {
      const message = "麦克风已请求但没有采集到音频，请检查手机麦克风权限或浏览器兼容性。";
      setStatus(message);
      log("microphone warning", { message });
    }
  }, 1000);
  sourceNode = inputContext.createMediaStreamSource(mediaStream);
  processorNode = inputContext.createScriptProcessor(4096, 1, 1);
  audioSender = audioSender || new AudioSender(() => ws);
  audioSender.stop();
  audioSender.start();
  processorNode.onaudioprocess = (event) => {
    if (!recordingStarted || !audioSender) return;
    const mono = event.inputBuffer.getChannelData(0);
    const pcm16 = floatToPcm16(downsampleTo16k(mono, inputContext.sampleRate));
    audioSender.enqueuePcm16(pcm16);
  };
  sourceNode.connect(processorNode);
  processorNode.connect(inputContext.destination);
  updateButtons();
  setStatus("正在通话");
}

async function startCall() {
  if (ws && ws.readyState === WebSocket.OPEN) {
    setStatus("已经连接");
    log("already connected");
    return;
  }
  if (ws && ws.readyState === WebSocket.CONNECTING) {
    setStatus("正在连接");
    log("websocket already connecting");
    return;
  }
  if (ws && (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED)) {
    resetCallState();
  }

  userStartRequested = true;
  roundId += 1;
  finalUserText = "";
  interimUserText = "";
  currentReplyId = null;
  currentSessionId = null;
  activeAssistantReplyId = null;
  interimUserMessageEl = null;
  activeAssistantMessageEl = null;
  waitingForNewReply = false;
  interrupted = false;
  interruptedReplyId = null;
  audioPacketsSent = 0;
  audioBytesSent = 0;
  wsSendCount = 0;
  wsSendCountLastSecond = 0;
  audioPacketsSentLastSecond = 0;
  audioPacketsDroppedByBackpressure = 0;
  maxWsBufferedAmount = 0;
  browserAudioPacketsReceived = 0;
  browserAudioBytesReceived = 0;
  wsReady = false;
  backendReady = false;
  recordingStarted = false;
  if (audioStatsTimer) clearInterval(audioStatsTimer);
  if (noAudioTimer) clearTimeout(noAudioTimer);
  audioStatsTimer = null;
  noAudioTimer = null;
  stopAllAudio(false);
  setCallState(CallState.IDLE, "startCall");
  startPerformanceTimer();
  updateButtons();
  setStatus("正在连接后端...");

  const wsUrl = websocketUrl();
  log("create new websocket", { url: wsUrl });
  ws = new WebSocket(wsUrl);
  updateButtons();
  ws.binaryType = "arraybuffer";
  wsListenersAttached = true;
  ws.onopen = () => {
    wsReady = true;
    setStatus("后端 WebSocket 已连接，等待 backend ready...");
    log("websocket open");
    updateButtons();
    maybeStartRecording();
  };
  ws.onmessage = handleServerMessage;
  ws.onerror = () => {
    log("websocket error");
  };
  ws.onclose = () => {
    log("websocket closed");
    wsListenersAttached = false;
    resetCallState();
  };
}

function cleanupAudio() {
  stopRecording();
  stopAllAudio(false);
  updateButtons();
}

function stopCall() {
  stopRecording();
  stopAllAudio(false);
  if (ws && ws.readyState === WebSocket.OPEN) {
    safeWsSend("__stop__", "stop");
  }
  if (ws) {
    log("close old websocket");
    ws.close();
  }
  resetCallState();
}

startBtn.addEventListener("click", () => {
  startCall().catch((err) => {
    setStatus("启动失败");
    log("start failed", { message: err.message });
    cleanupAudio();
  });
});
stopBtn.addEventListener("click", stopCall);
window.addEventListener("beforeunload", () => {
  stopRecording();
  stopAllAudio(false);
  closePlaybackAudioContext();
  if (ws && ws.readyState === WebSocket.OPEN) {
    safeWsSend("__stop__", "stop");
    ws.close();
  }
});
