export type ChatRole = "user" | "assistant";

export type ChatMessage = {
  id: string;
  role: ChatRole;
  text: string;
  replyId?: string | number | null;
  interim?: boolean;
};

export type ServerMessage =
  | {
      type: "ready";
      session_id?: string;
      output_sample_rate?: number;
    }
  | {
      type: "user_text";
      text?: string;
      is_interim?: boolean;
      reply_id?: string | number | null;
    }
  | {
      type: "assistant_text";
      text?: string;
      reply_id?: string | number | null;
    }
  | {
      type: "assistant_text_done";
      reply_id?: string | number | null;
    }
  | {
      type: "user_speech_start";
      reply_id?: string | number | null;
    }
  | {
      type: "audio";
      audio?: string;
      reply_id?: string | number | null;
      sample_rate?: number;
    }
  | {
      type: "audio_wav";
      audio?: string;
      reply_id?: string | number | null;
      sampleRate?: number;
    }
  | {
      type: "mobile_audio_received";
      pcm16Bytes?: number;
      packets?: number;
      sampleRate?: number;
    }
  | {
      type: "tts_done";
      reply_id?: string | number | null;
    }
  | {
      type: "error";
      message?: string;
      detail?: string;
      [key: string]: unknown;
    }
  | {
      type: string;
      [key: string]: unknown;
    };
