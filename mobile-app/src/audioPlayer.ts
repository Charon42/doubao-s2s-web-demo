import { Audio, AVPlaybackStatus } from "expo-av";
import * as FileSystem from "expo-file-system/legacy";

export type AudioPlayerController = {
  enqueueWavBase64: (audio: string) => void;
  enqueuePcm16Base64: (audio: string, sampleRate?: number) => void;
  stopAll: () => void;
  reset: () => void;
};

type QueuedWav = {
  id: string;
  audio: string;
};

export function createAudioPlayer(onLog?: (message: string, data?: unknown) => void): AudioPlayerController {
  let queue: QueuedWav[] = [];
  let playing = false;
  let currentSound: Audio.Sound | null = null;
  let currentFileUri: string | null = null;
  let sequence = 0;

  async function cleanupCurrent() {
    const sound = currentSound;
    const fileUri = currentFileUri;
    currentSound = null;
    currentFileUri = null;
    if (sound) {
      try {
        await sound.stopAsync();
      } catch {
        // Sound may already be stopped.
      }
      try {
        await sound.unloadAsync();
      } catch {
        // Sound may already be unloaded.
      }
    }
    if (fileUri) {
      await FileSystem.deleteAsync(fileUri, { idempotent: true }).catch(() => {});
    }
  }

  async function playNext() {
    if (playing) return;
    const item = queue.shift();
    if (!item) return;

    playing = true;
    const fileUri = `${FileSystem.cacheDirectory}doubao-tts-${item.id}.wav`;
    currentFileUri = fileUri;

    try {
      await FileSystem.writeAsStringAsync(fileUri, item.audio, {
        encoding: FileSystem.EncodingType.Base64
      });
      const { sound } = await Audio.Sound.createAsync(
        { uri: fileUri },
        { shouldPlay: true },
        (status: AVPlaybackStatus) => {
          if (!status.isLoaded) return;
          if (status.didJustFinish) {
            cleanupCurrent()
              .catch((err) => onLog?.("audio cleanup failed", { message: err.message }))
              .finally(() => {
                playing = false;
                playNext().catch((err) => onLog?.("audio play failed", { message: err.message }));
              });
          }
        }
      );
      currentSound = sound;
      onLog?.("play audio_wav", { fileUri });
    } catch (err) {
      await cleanupCurrent();
      playing = false;
      onLog?.("audio_wav play failed", { message: err instanceof Error ? err.message : String(err) });
      await playNext();
    }
  }

  return {
    enqueueWavBase64(audio: string) {
      if (!audio) return;
      sequence += 1;
      queue.push({ id: `${Date.now()}-${sequence}`, audio });
      playNext().catch((err) => onLog?.("audio play failed", { message: err.message }));
    },
    enqueuePcm16Base64(_audio: string, _sampleRate?: number) {
      // Reserved for the native PCM playback implementation.
      //
      // Doubao realtime output audio is:
      // - PCM16 signed little-endian
      // - 24000 Hz by default
      // - mono
      //
      // The first mobile version only keeps this interface so WebSocket
      // and text bubbles can be verified on Android Expo before native
      // audio playback is added.
    },
    stopAll() {
      queue = [];
      playing = false;
      cleanupCurrent().catch((err) => onLog?.("audio stop failed", { message: err.message }));
    },
    reset() {
      queue = [];
      playing = false;
      cleanupCurrent().catch((err) => onLog?.("audio reset failed", { message: err.message }));
    }
  };
}
