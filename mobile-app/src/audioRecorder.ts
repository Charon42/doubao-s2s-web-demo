import { Audio } from "expo-av";
import * as FileSystem from "expo-file-system/legacy";

export type AudioChunkHandler = (chunk: ArrayBuffer) => void;
export type RecordedAudioFile = {
  uri: string;
  name: string;
  mimeType: string;
  base64: string;
};

export type AudioRecorderController = {
  requestPermission: () => Promise<boolean>;
  recordTestClip: (durationMs?: number) => Promise<RecordedAudioFile>;
  start: () => Promise<void>;
  stop: () => Promise<void>;
  reset: () => Promise<void>;
};

export function createAudioRecorder(_onChunk: AudioChunkHandler): AudioRecorderController {
  let recording: Audio.Recording | null = null;

  async function requestPermission() {
    const permission = await Audio.requestPermissionsAsync();
    return permission.granted;
  }

  return {
    requestPermission,
    async recordTestClip(durationMs = 2000) {
      const granted = await requestPermission();
      if (!granted) {
        throw new Error("麦克风权限未授予");
      }

      await Audio.setAudioModeAsync({
        allowsRecordingIOS: true,
        playsInSilentModeIOS: true,
        shouldDuckAndroid: true
      });

      if (recording) {
        await recording.stopAndUnloadAsync().catch(() => {});
        recording = null;
      }

      const created = await Audio.Recording.createAsync(Audio.RecordingOptionsPresets.HIGH_QUALITY);
      recording = created.recording;
      await new Promise((resolve) => setTimeout(resolve, durationMs));
      await recording.stopAndUnloadAsync();

      const uri = recording.getURI();
      recording = null;
      if (!uri) {
        throw new Error("录音文件为空");
      }

      const base64 = await FileSystem.readAsStringAsync(uri, {
        encoding: FileSystem.EncodingType.Base64
      });
      return {
        uri,
        name: "expo-test-recording.m4a",
        mimeType: "audio/mp4",
        base64
      };
    },
    async start() {
      // Reserved for the native recording implementation.
      //
      // Doubao realtime input audio must be:
      // - PCM16 signed little-endian
      // - 16000 Hz
      // - mono
      // - 20 ms per packet
      // - 640 bytes per packet: 16000 * 0.02 * 1 channel * 2 bytes
      //
      // Expo Go records compressed files. Realtime PCM capture requires a
      // Dev Client or native AudioRecord module. Do not fake realtime input
      // with empty setInterval packets.
    },
    async stop() {
      if (recording) {
        await recording.stopAndUnloadAsync().catch(() => {});
        recording = null;
      }
    },
    async reset() {
      if (recording) {
        await recording.stopAndUnloadAsync().catch(() => {});
        recording = null;
      }
    }
  };
}
