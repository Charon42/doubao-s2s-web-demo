(function () {
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
      if (PAUSE_MIC_WHILE_ASSISTANT_SPEAKING && isAssistantSpeakingForMic()) {
        const dropCount = this.queue.length;
        this.queue = [];
        this.remainder = new Uint8Array(0);
        this.droppedPackets += dropCount;
        audioPacketsDroppedByBackpressure += dropCount;
        return;
      }
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

  window.AudioSender = AudioSender;
})();
