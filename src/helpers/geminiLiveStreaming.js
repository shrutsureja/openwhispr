const WebSocket = require("ws");
const debugLogger = require("./debugLogger");

const WEBSOCKET_TIMEOUT_MS = 15000;
const DISCONNECT_TIMEOUT_MS = 3000;
const SAMPLE_RATE = 16000;
const COLD_START_BUFFER_MAX = 3 * SAMPLE_RATE * 2; // 3 seconds of 16-bit PCM

const GEMINI_LIVE_WS_URL =
  "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

class GeminiLiveStreaming {
  constructor() {
    this.ws = null;
    this.isConnected = false;
    this.isConnecting = false;
    this.completedSegments = [];
    this.currentPartial = "";
    this.onPartialTranscript = null;
    this.onFinalTranscript = null;
    this.onError = null;
    this.onSessionEnd = null;
    this.pendingResolve = null;
    this.pendingReject = null;
    this.connectionTimeout = null;
    this.closeResolve = null;
    this.isDisconnecting = false;
    this.audioBytesSent = 0;
    this.model = "gemini-live-2.5-flash-native-audio";
    this.coldStartBuffer = [];
    this.coldStartBufferSize = 0;
  }

  getFullTranscript() {
    return this.completedSegments.join(" ");
  }

  async connect(options = {}) {
    const { apiKey, model } = options;
    if (!apiKey) throw new Error("Gemini API key is required");

    if (this.isConnected || this.isConnecting) {
      debugLogger.debug("Gemini Live already connected/connecting");
      return;
    }

    this.isConnecting = true;
    this.model = model || "gemini-live-2.5-flash-native-audio";
    this.completedSegments = [];
    this.currentPartial = "";
    this.audioBytesSent = 0;
    this.coldStartBuffer = [];
    this.coldStartBufferSize = 0;

    const url = `${GEMINI_LIVE_WS_URL}?key=${encodeURIComponent(apiKey)}`;
    debugLogger.debug("Gemini Live connecting", { model: this.model });

    return new Promise((resolve, reject) => {
      this.pendingResolve = resolve;
      this.pendingReject = reject;

      this.connectionTimeout = setTimeout(() => {
        this.isConnecting = false;
        this.cleanup();
        reject(new Error("Gemini Live connection timeout"));
      }, WEBSOCKET_TIMEOUT_MS);

      this.ws = new WebSocket(url);

      this.ws.on("open", () => {
        debugLogger.debug("Gemini Live WebSocket opened, sending setup");
        this._sendSetup();
      });

      this.ws.on("message", (data) => {
        this._handleMessage(data);
      });

      this.ws.on("error", (error) => {
        debugLogger.error("Gemini Live WebSocket error", { error: error.message });
        this.isConnecting = false;
        this.cleanup();
        if (this.pendingReject) {
          this.pendingReject(error);
          this.pendingReject = null;
          this.pendingResolve = null;
        }
        this.onError?.(error);
      });

      this.ws.on("close", (code, reason) => {
        const wasActive = this.isConnected;
        this.isConnecting = false;
        debugLogger.debug("Gemini Live WebSocket closed", {
          code,
          reason: reason?.toString(),
          wasActive,
        });
        if (this.pendingReject) {
          this.pendingReject(new Error(`WebSocket closed before ready (code: ${code})`));
          this.pendingReject = null;
          this.pendingResolve = null;
        }
        if (this.closeResolve) {
          this.closeResolve({ text: this.getFullTranscript() });
        }
        this.cleanup();
        if (wasActive && !this.isDisconnecting) {
          this.onSessionEnd?.({ text: this.getFullTranscript() });
        }
      });
    });
  }

  _sendSetup() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    const setupMsg = {
      setup: {
        model: `models/${this.model}`,
        generationConfig: {
          responseModalities: ["TEXT"],
        },
        inputAudioTranscription: {},
      },
    };

    this.ws.send(JSON.stringify(setupMsg));
  }

  _handleMessage(data) {
    try {
      const msg = JSON.parse(data.toString());

      // Setup complete — server acknowledges with setupComplete
      if (msg.setupComplete !== undefined) {
        debugLogger.debug("Gemini Live setup complete");
        this.isConnected = true;
        this.isConnecting = false;
        clearTimeout(this.connectionTimeout);
        if (this.pendingResolve) {
          this.pendingResolve();
          this.pendingResolve = null;
          this.pendingReject = null;
        }
        return;
      }

      // Server content with input transcription
      if (msg.serverContent) {
        const sc = msg.serverContent;

        // Input audio transcription (what the user said)
        if (sc.inputTranscription) {
          const text = sc.inputTranscription.text || "";
          if (text) {
            // Treat each inputTranscription as a completed segment
            this.completedSegments.push(text.trim());
            this.currentPartial = "";
            const fullText = this.getFullTranscript();
            this.onFinalTranscript?.(fullText);
            debugLogger.debug("Gemini Live input transcription", {
              text: text.slice(0, 100),
              totalSegments: this.completedSegments.length,
            });
          }
          return;
        }

        // Output text parts (model response text — we ignore for transcription-only use)
        if (sc.modelTurn?.parts) {
          for (const part of sc.modelTurn.parts) {
            if (part.text) {
              debugLogger.debug("Gemini Live model response (ignored)", {
                text: part.text.slice(0, 50),
              });
            }
          }
          return;
        }

        // Turn complete
        if (sc.turnComplete) {
          debugLogger.debug("Gemini Live turn complete");
          return;
        }
      }

      // Error from server
      if (msg.error) {
        const errMsg = msg.error.message || "Gemini Live error";
        debugLogger.error("Gemini Live server error", { error: errMsg });
        this.onError?.(new Error(errMsg));
      }
    } catch (err) {
      debugLogger.error("Gemini Live message parse error", { error: err.message });
    }
  }

  sendAudio(pcmBuffer) {
    if (!this.ws) return false;

    if (this.ws.readyState !== WebSocket.OPEN) {
      if (
        this.ws.readyState === WebSocket.CONNECTING &&
        this.coldStartBufferSize < COLD_START_BUFFER_MAX
      ) {
        const copy = Buffer.from(pcmBuffer);
        this.coldStartBuffer.push(copy);
        this.coldStartBufferSize += copy.length;
      }
      return false;
    }

    // Flush cold-start buffer if any
    if (this.coldStartBuffer.length > 0) {
      debugLogger.debug("Gemini Live flushing cold-start buffer", {
        chunks: this.coldStartBuffer.length,
        bytes: this.coldStartBufferSize,
      });
      for (const buf of this.coldStartBuffer) {
        this._sendAudioChunk(buf);
      }
      this.coldStartBuffer = [];
      this.coldStartBufferSize = 0;
    }

    this._sendAudioChunk(pcmBuffer);
    return true;
  }

  _sendAudioChunk(pcmBuffer) {
    const base64Audio = Buffer.from(pcmBuffer).toString("base64");
    this.ws.send(
      JSON.stringify({
        realtimeInput: {
          audio: {
            data: base64Audio,
            mimeType: "audio/pcm;rate=16000",
          },
        },
      })
    );
    this.audioBytesSent += pcmBuffer.length;
  }

  async disconnect() {
    debugLogger.debug("Gemini Live disconnect", {
      audioBytesSent: this.audioBytesSent,
      segments: this.completedSegments.length,
      textLength: this.getFullTranscript().length,
      readyState: this.ws?.readyState,
    });

    if (!this.ws) return { text: this.getFullTranscript() };

    this.isDisconnecting = true;

    if (this.ws.readyState === WebSocket.CONNECTING) {
      this.ws.once("open", () => this.ws?.close());
      const result = { text: this.getFullTranscript() };
      this.isDisconnecting = false;
      return result;
    }

    if (this.ws.readyState === WebSocket.OPEN) {
      if (this.audioBytesSent > 0) {
        const prevOnFinal = this.onFinalTranscript;
        const prevOnError = this.onError;

        // Wait briefly for any last transcription to come through
        await new Promise((resolve) => {
          const tid = setTimeout(() => {
            debugLogger.debug("Gemini Live disconnect timeout, using accumulated text");
            resolve();
          }, DISCONNECT_TIMEOUT_MS);

          const done = () => {
            clearTimeout(tid);
            this.onFinalTranscript = prevOnFinal;
            this.onError = prevOnError;
            resolve();
          };

          this.onFinalTranscript = (text) => {
            prevOnFinal?.(text);
            done();
          };

          this.onError = (err) => {
            prevOnError?.(err);
            done();
          };

          // Signal end of audio turn
          try {
            this.ws.send(JSON.stringify({ clientContent: { turnComplete: true } }));
          } catch {
            done();
          }
        });
      }

      this.ws.close();
    }

    const result = { text: this.getFullTranscript() };
    this.cleanup();
    this.isDisconnecting = false;
    return result;
  }

  cleanup() {
    clearTimeout(this.connectionTimeout);
    this.connectionTimeout = null;

    if (this.ws) {
      try {
        this.ws.close();
      } catch {}
      this.ws = null;
    }

    this.isConnected = false;
    this.isConnecting = false;
    this.closeResolve = null;
  }
}

module.exports = GeminiLiveStreaming;
