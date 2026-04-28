export const DEFAULT_WS_URL = "ws://192.168.110.210:8000/ws/call";

export function getWebSocketUrl() {
  return process.env.EXPO_PUBLIC_WS_URL || DEFAULT_WS_URL;
}
