# Doubao S2S Mobile App

Expo React Native mobile front end for the existing FastAPI WebSocket backend.

## Configure

Copy `.env.example` to `.env` and set the backend WebSocket URL to the LAN IP of the computer running FastAPI:

```bash
EXPO_PUBLIC_WS_URL=ws://192.168.110.210:8000/ws/call
```

Do not use `127.0.0.1` on a real phone. The phone resolves that to itself, not to your computer.

## Run

```bash
cd mobile-app
npm install
npm run start
```

Then scan the Expo QR code with Expo Go on Android.

The backend should be reachable from the phone, for example:

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

## Scope

The first mobile version verifies WebSocket communication and text bubbles. Native recording and PCM playback are intentionally stubbed in `src/audioRecorder.ts` and `src/audioPlayer.ts`.
