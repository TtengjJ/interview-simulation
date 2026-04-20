package interview.guide.modules.voiceinterview.service;

import com.alibaba.dashscope.audio.omni.OmniRealtimeCallback;
import com.alibaba.dashscope.audio.omni.OmniRealtimeConfig;
import com.alibaba.dashscope.audio.omni.OmniRealtimeConversation;
import com.alibaba.dashscope.audio.omni.OmniRealtimeModality;
import com.alibaba.dashscope.audio.omni.OmniRealtimeParam;
import com.alibaba.dashscope.audio.omni.OmniRealtimeTranscriptionParam;
import com.alibaba.dashscope.exception.NoApiKeyException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import interview.guide.modules.voiceinterview.config.VoiceInterviewProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Qwen3 Realtime ASR Service
 *
 * 阿里云通义千问实时语音识别服务（ASR）
 * <p>
 * 基于 qwen3-asr-flash-realtime 模型，通过 WebSocket 协议实现低延迟语音转文本。
 * 支持多会话并发管理、服务端 VAD（语音活动检测）和流式转录结果推送。
 * </p>
 *
 * 核心特性：
 * <ul>
 *   <li><b>多会话管理</b>：使用 ConcurrentHashMap 存储多个并发的 ASR 会话</li>
 *   <li><b>服务端 VAD</b>：自动检测静音（400ms）并触发句子边界识别</li>
 *   <li><b>回调机制</b>：通过 Consumer 接口实时推送最终结果和部分结果</li>
 *   <li><b>自动重连</b>：WebSocket 断开后支持 restartTranscription 重新建立连接</li>
 *   <li><b>资源清理</b>：会话终止时自动关闭连接并释放内存</li>
 * </ul>
 *
 * 工作流程：
 * <ol>
 *   <li>调用 startTranscription() 创建 WebSocket 连接并配置参数</li>
 *   <li>通过 sendAudio() 持续发送 PCM 音频数据（Base64 编码）</li>
 *   <li>服务端 VAD 检测到静音后自动触发识别</li>
 *   <li>在 onEvent 回调中接收 transcription.completed 事件并推送结果</li>
 *   <li>调用 stopTranscription() 结束会话并关闭连接</li>
 * </ol>
 *
 * @see OmniRealtimeConversation DashScope SDK 实时对话客户端
 * @see OmniRealtimeCallback WebSocket 事件回调接口
 */
@Slf4j
@Service
public class QwenAsrService {

    // Runtime configuration values (loaded from VoiceInterviewProperties; setters kept for tests)
    private String url;

    private String model;

    private String apiKey;

    private String language;

    private String format;

    private Integer sampleRate;

    private Boolean enableTurnDetection;

    private String turnDetectionType;

    private Float turnDetectionThreshold;

    private Integer turnDetectionSilenceDurationMs;

    public QwenAsrService(VoiceInterviewProperties voiceInterviewProperties) {
        VoiceInterviewProperties.AsrConfig asr = voiceInterviewProperties.getQwen().getAsr();
        this.url = asr.getUrl();
        this.model = asr.getModel();
        this.apiKey = asr.getApiKey();
        this.language = asr.getLanguage();
        this.format = asr.getFormat();
        this.sampleRate = asr.getSampleRate();
        this.enableTurnDetection = asr.isEnableTurnDetection();
        this.turnDetectionType = asr.getTurnDetectionType();
        this.turnDetectionThreshold = asr.getTurnDetectionThreshold();
        this.turnDetectionSilenceDurationMs = asr.getTurnDetectionSilenceDurationMs();
    }

    /**
     * 活跃的 ASR 会话映射表
     * <p>
     * Key: 会话 ID（用户提供的唯一标识符，通常是 interview sessionId）<br>
     * Value: AsrSession 对象，包含 OmniRealtimeConversation 实例和回调函数
     * </p>
     * <p>
     * <b>线程安全</b>：使用 ConcurrentHashMap 支持多线程并发读写
     * </p>
     */
    private final Map<String, AsrSession> sessions = new ConcurrentHashMap<>();

    /** 防止同一 interview sessionId 上并发 stop/start；并在重连时与 {@link #sessionLocks} 配合 */
    private final ConcurrentHashMap<String, Object> sessionLocks = new ConcurrentHashMap<>();

    /**
     * 获取指定会话的锁对象（用于同步控制）
     * <p>
     * 确保同一个 sessionId 的 start/stop/restart 操作串行执行，避免竞态条件
     * </p>
     */
    private Object lockForSession(String sessionId) {
        return sessionLocks.computeIfAbsent(sessionId, k -> new Object());
    }

    /**
     * Initialize the ASR service.
     * This method is automatically called by Spring after the service is constructed
     * and all configuration values have been loaded from VoiceInterviewProperties.
     *
     * @throws IllegalStateException if apiKey is not configured
     */
    @PostConstruct
    public void init() {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalStateException("API key must be configured before initializing QwenAsrService");
        }
        log.info("QwenAsrService initialized with model: {}, url: {}", model, url);
    }

    /**
     * 启动新的语音识别会话
     * <p>
     * 该方法会创建到 DashScope ASR 服务的 WebSocket 连接，并设置回调处理器。
     * 会话使用服务端 VAD（Voice Activity Detection）自动检测句子边界。
     * </p>
     * <p>
     * <b>注意</b>：此方法为简化版本，仅支持最终结果回调，不支持部分结果（实时字幕）
     * </p>
     *
     * @param sessionId 会话唯一标识符
     * @param onFinal 当句子/片段识别完成时触发的回调（completed 事件）
     * @param onError 发生错误时触发的回调
     * @throws IllegalStateException 如果会话已存在或服务未初始化
     */
    public void startTranscription(String sessionId, Consumer<String> onFinal, Consumer<Throwable> onError) {
        startTranscription(sessionId, onFinal, null, onError);
    }

    /**
     * 启动语音识别会话（支持部分结果回调）
     * <p>
     * 与 {@link #startTranscription(String, Consumer, Consumer)} 类似，但额外支持
     * 实时部分结果推送（用于前端显示动态字幕）。
     * </p>
     *
     * @param onPartial 部分转录结果回调（可为 null，用于实时字幕更新）
     */
    public void startTranscription(
            String sessionId,
            Consumer<String> onFinal,
            Consumer<String> onPartial,
            Consumer<Throwable> onError) {
        // 同步锁定当前会话（防止并发冲突）
        synchronized (lockForSession(sessionId)) {
            startTranscriptionLocked(sessionId, onFinal, onPartial, onError);
        }
    }

    /**
     * 停止旧连接并重新建立（用于 ASR WebSocket 被服务端关闭后恢复识别）
     * <p>
     * <b>典型场景</b>：解决「第三轮起无声」问题 —— 当服务端主动关闭 WebSocket 时，
     * 需要重新建立连接以继续接收音频识别结果。
     * </p>
     * <p>
     * 操作流程：
     * <ol>
     *   <li>同步锁定当前会话（防止并发冲突）</li>
     *   <li>调用 stopTranscription() 停止并清理旧连接</li>
     *   <li>等待 200ms（确保旧连接完全关闭）</li>
     *   <li>调用 startTranscriptionLocked() 重新建立连接</li>
     *   <li>轮询验证新连接是否就绪（最多 1 秒，每次间隔 100ms）</li>
     * </ol>
     * </p>
     */
    public void restartTranscription(
            String sessionId,
            Consumer<String> onFinal,
            Consumer<String> onPartial,
            Consumer<Throwable> onError) {
        synchronized (lockForSession(sessionId)) {
            log.info("[Session: {}] Restarting DashScope ASR (stop + start)", sessionId);
            stopTranscription(sessionId);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            startTranscriptionLocked(sessionId, onFinal, onPartial, onError);

            // Verify reconnection succeeded
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    Thread.sleep(100);
                    AsrSession newSession = sessions.get(sessionId);
                    if (newSession != null && newSession.getConversation() != null) {
                        log.info("[Session: {}] ASR reconnection verified successfully", sessionId);
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("[Session: {}] ASR reconnection verification interrupted", sessionId);
                    return;
                }
            }

            log.warn("[Session: {}] ASR reconnection may not be fully ready after 1 second", sessionId);
        }
    }

    private void startTranscriptionLocked(
            String sessionId,
            Consumer<String> onFinal,
            Consumer<String> onPartial,
            Consumer<Throwable> onError) {
        if (sessions.containsKey(sessionId)) {
            throw new IllegalStateException("Session already exists: " + sessionId);
        }

        try {
            // 步骤 1：构建 WebSocket 连接参数
            OmniRealtimeParam param = OmniRealtimeParam.builder()
                    .model(model)      // 模型名称：qwen3-asr-flash-realtime
                    .url(url)          // WebSocket 服务端点
                    .apikey(apiKey)    // API Key 认证
                    .build();

            // 用于存储 OmniRealtimeConversation 对象
            final AtomicReference<OmniRealtimeConversation> conversationRef = new AtomicReference<>();

            // 步骤 2：创建 WebSocket 事件回调处理器
            OmniRealtimeCallback callback = new OmniRealtimeCallback() {
                @Override
                public void onOpen() {
                    log.debug("[Session: {}] WebSocket connection established", sessionId);
                }

                @Override
                public void onEvent(JsonObject message) {
                    // 处理服务端推送的事件（识别结果、错误等）
                    handleServerEvent(sessionId, message, onFinal, onPartial, onError);
                }

                @Override
                public void onClose(int code, String reason) {
                    OmniRealtimeConversation closed = conversationRef.get();
                    log.warn("[Session: {}] DashScope ASR WebSocket closed - code: {}, reason: {}",
                            sessionId, code, reason);
                    // ① 关键修复：仅移除与本次连接对应的会话，避免重连后旧 onClose 误删新连接
                    // 这是「第三轮起无声」问题的根本原因
                    sessions.compute(sessionId, (id, existing) -> {
                        if (existing != null && closed != null && existing.getConversation() == closed) {
                            return null;  // 只有当前连接匹配时才删除
                        }
                        return existing;  // 否则保留新建立的连接
                    });
                }
            };

            // 步骤 3：创建 ASR 对话客户端实例
            OmniRealtimeConversation conversation = new OmniRealtimeConversation(param, callback);
            conversationRef.set(conversation);

            // 步骤 4：在连接前将会话存入 Map（确保 hasActiveSession() 立即返回 true）
            sessions.put(sessionId, new AsrSession(conversation, onFinal, onPartial, onError));

            // 步骤 5：异步建立 WebSocket 连接（非阻塞）
            Thread connectionThread = new Thread(() -> {
                try {
                    conversation.connect();  // 阻塞直到连接成功或失败

                    // 步骤 6：配置转录参数（语言、采样率、音频格式）
                    OmniRealtimeTranscriptionParam transcriptionParam = new OmniRealtimeTranscriptionParam();
                    transcriptionParam.setLanguage(language);           // 语言：zh（中文）
                    transcriptionParam.setInputSampleRate(sampleRate);  // 采样率：16000 Hz
                    transcriptionParam.setInputAudioFormat(format);     // 格式：pcm

                    // 步骤 7：配置会话参数（VAD、模态等）
                    OmniRealtimeConfig config = OmniRealtimeConfig.builder()
                            .modalities(Collections.singletonList(OmniRealtimeModality.TEXT))  // 仅输出文本
                            .enableTurnDetection(enableTurnDetection)                           // 启用 VAD
                            .turnDetectionType(turnDetectionType)                               // VAD 类型：server_vad
                            .turnDetectionThreshold(turnDetectionThreshold)                     // VAD 阈值
                            .turnDetectionSilenceDurationMs(turnDetectionSilenceDurationMs)     // 静音时长：400ms
                            .transcriptionConfig(transcriptionParam)
                            .build();

                    // 步骤 8：更新会话配置
                    conversation.updateSession(config);

                    log.info("[Session: {}] Transcription session started successfully", sessionId);

                } catch (Exception e) {
                    log.error("[Session: {}] Failed to establish connection", sessionId, e);
                    // 连接失败时清理会话
                    sessions.compute(sessionId, (id, existing) -> {
                        if (existing != null && existing.getConversation() == conversation) {
                            return null;
                        }
                        return existing;
                    });
                    onError.accept(e);
                }
            }, "ASR-Connection-" + sessionId);
            connectionThread.setDaemon(true);  // 设置为守护线程，随主进程退出
            connectionThread.start();

        } catch (Exception e) {
            String errorMsg = "Failed to create transcription session: " + sessionId;
            log.error(errorMsg, e);
            sessions.remove(sessionId);
            onError.accept(new IllegalStateException(errorMsg, e));
            throw new IllegalStateException(errorMsg, e);
        }
    }

    /**
     * 向 ASR 服务发送音频数据进行转录
     * <p>
     * 音频数据应为 16kHz 采样率的 PCM 格式。数据在发送前会被 Base64 编码。
     * </p>
     * <p>
     * 启用服务端 VAD 后，服务会自动检测语音片段并在检测到静音时触发识别。
     * </p>
     *
     * @param sessionId 会话标识符
     * @param audioData 原始 PCM 音频字节数组
     * @throws IllegalStateException 如果会话不存在
     */
    public void sendAudio(String sessionId, byte[] audioData) {
        AsrSession session = sessions.get(sessionId);
        if (session == null) {
            throw new IllegalStateException("No active session found: " + sessionId);
        }

        try {
            // 将音频数据转换为 Base64 字符串
            String audioBase64 = Base64.getEncoder().encodeToString(audioData);

            // 发送到 ASR 服务
            session.getConversation().appendAudio(audioBase64);

            log.trace("[Session: {}] Sent {} bytes of audio data", sessionId, audioData.length);

        } catch (Exception e) {
            log.error("[Session: {}] appendAudio failed (upstream may reconnect)", sessionId, e);
            // 抛出异常以便 WebSocket 层执行 restartTranscription；
            // 不在此重复调用 onError，避免用户先看到红条再恢复
            throw new IllegalStateException("ASR append failed: " + sessionId, e);
        }
    }

    /**
     * 停止转录并关闭会话
     * <p>
     * 该方法通知 ASR 服务完成任何待处理的转录，等待最终结果，然后关闭 WebSocket 连接。
     * </p>
     * <p>
     * <b>线程安全</b>：使用 synchronized 锁确保同一会话的 stop 操作串行执行
     * </p>
     *
     * @param sessionId 会话标识符
     */
    public void stopTranscription(String sessionId) {
        synchronized (lockForSession(sessionId)) {
            AsrSession session = sessions.remove(sessionId);
            // Clean up the session lock to prevent memory leak
            sessionLocks.remove(sessionId);
            if (session == null) {
                log.warn("[Session: {}] Attempted to stop non-existent session", sessionId);
                return;
            }

            try {
                session.getConversation().endSession();
                log.info("[Session: {}] Transcription session stopped", sessionId);
            } catch (InterruptedException e) {
                log.error("[Session: {}] Thread interrupted while ending session", sessionId, e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("[Session: {}] Error while ending session (may already be closed): {}", sessionId, e.getMessage());
            }

            try {
                session.getConversation().close();
            } catch (Exception e) {
                log.debug("[Session: {}] Connection already closed: {}", sessionId, e.getMessage());
            }
        }
    }

    /**
     * Check if a session with the given ID is currently active.
     *
     * @param sessionId Session identifier
     * @return true if session exists and is active, false otherwise
     */
    public boolean hasActiveSession(String sessionId) {
        return sessions.containsKey(sessionId);
    }

    /**
     * Destroy the service and cleanup all active sessions.
     *
     * This method is called automatically when the Spring container shuts down.
     * It stops all active sessions and releases resources.
     */
    @PreDestroy
    public void destroy() {
        log.info("Destroying QwenAsrService with {} active sessions", sessions.size());

        // Stop all active sessions
        sessions.keySet().forEach(sessionId -> {
            try {
                stopTranscription(sessionId);
            } catch (Exception e) {
                log.error("[Session: {}] Error during cleanup", sessionId, e);
            }
        });

        sessions.clear();
        log.info("QwenAsrService destroyed successfully");
    }

    /**
     * 处理 DashScope ASR 服务端推送的事件
     * <p>
     * 支持的事件类型：
     * <ul>
     *   <li><b>session.created</b>：会话在服务端创建成功</li>
     *   <li><b>session.updated</b>：会话配置更新完成</li>
     *   <li><b>conversation.item.input_audio_transcription.completed</b>：最终转录结果（完整句子）</li>
     *   <li><b>conversation.item.input_audio_transcription.text / .delta</b>：部分转录结果（实时字幕）</li>
     *   <li><b>error</b>：发生错误，记录异常并通知调用方</li>
     *   <li><b>session.finished</b>：会话已结束</li>
     *   <li><b>conversation.item.input_audio_transcription.failed</b>：单次识别失败</li>
     * </ul>
     * </p>
     *
     * @param sessionId 会话标识符
     * @param message JSON 格式的事件消息
     * @param onFinal 最终片段文本回调
     * @param onPartial 流式部分文本回调（可选）
     * @param onError 错误回调
     */
    private void handleServerEvent(
            String sessionId,
            JsonObject message,
            Consumer<String> onFinal,
            Consumer<String> onPartial,
            Consumer<Throwable> onError) {
        try {
            String eventType = message.get("type").getAsString();

            log.trace("[Session: {}] Received event: {}", sessionId, eventType);

            switch (eventType) {
                case "session.created":
                    log.debug("[Session: {}] Session created on server", sessionId);
                    break;

                case "session.updated":
                    log.debug("[Session: {}] Session configuration updated", sessionId);
                    break;

                case "conversation.item.input_audio_transcription.completed":
                    // 最终转录结果：VAD 检测到静音后触发的完整句子识别
                    JsonObject transcriptObj = message.getAsJsonObject();
                    String transcript = transcriptObj.get("transcript").getAsString();
                    String language = transcriptObj.has("language") ?
                            transcriptObj.get("language").getAsString() : "unknown";
                    String emotion = transcriptObj.has("emotion") ?
                            transcriptObj.get("emotion").getAsString() : "neutral";

                    log.debug("[Session: {}] Transcription completed - language: {}, emotion: {}, text: {}",
                            sessionId, language, emotion, transcript);

                    // 触发最终结果回调（用于 LLM 对话触发）
                    onFinal.accept(transcript);
                    break;

                case "conversation.item.input_audio_transcription.text":
                case "conversation.item.input_audio_transcription.delta":
                    // 部分转录结果：用于实时字幕显示
                    dispatchPartialTranscript(sessionId, message, onPartial);
                    break;

                case "error":
                    // Error event
                    JsonObject errorObj = message.getAsJsonObject("error");
                    String errorType = errorObj.has("type") ? errorObj.get("type").getAsString() : "unknown";
                    String errorCode = errorObj.has("code") ? errorObj.get("code").getAsString() : "unknown";
                    String errorMessage = errorObj.has("message") ? errorObj.get("message").getAsString() : "Unknown error";

                    String fullErrorMessage = String.format("ASR Error [%s/%s]: %s", errorType, errorCode, errorMessage);
                    log.error("[Session: {}] {}", sessionId, fullErrorMessage);

                    onError.accept(new IllegalStateException(fullErrorMessage));
                    break;

                case "session.finished":
                    log.debug("[Session: {}] Session finished on server", sessionId);
                    break;

                case "conversation.item.input_audio_transcription.failed":
                    log.error("[Session: {}] ASR transcription failed (single utterance): {}", sessionId, message);
                    break;

                default:
                    if (eventType != null && eventType.contains("transcription")) {
                        log.debug("[Session: {}] Unhandled transcription-related event: {}", sessionId, message);
                    } else {
                        log.trace("[Session: {}] Unhandled event type: {}", sessionId, eventType);
                    }
            }

        } catch (Exception e) {
            log.error("[Session: {}] Error processing server event", sessionId, e);
            onError.accept(e);
        }
    }

    /**
     * 转发部分/流式 ASR 文本用于实时 UI 显示（VAD 本身不意味着可见的 STT）
     * <p>
     * 从 JSON 事件中提取文本并调用 onPartial 回调，供前端实时更新字幕
     * </p>
     */
    private void dispatchPartialTranscript(
            String sessionId, JsonObject message, Consumer<String> onPartial) {
        if (onPartial == null) {
            log.trace("[Session: {}] Partial transcription received (no consumer)", sessionId);
            return;
        }
        String text = extractTranscriptPayload(message);
        if (text != null && !text.isBlank()) {
            onPartial.accept(text);
        } else {
            log.trace("[Session: {}] Partial ASR event without extractable text: {}", sessionId, message);
        }
    }

    /**
     * 从 ASR JSON 事件中提取可显示的文本
     * <p>
     * 对于 {@code conversation.item.input_audio_transcription.text} 事件，官方预览格式为：
     * {@code text}（已确认的前缀）+ {@code stash}（草稿后缀）；两者可能为空。
     * </p>
     * <p>
     * 提取优先级：
     * <ol>
     *   <li>transcript 字段（最终结果）</li>
     *   <li>text + stash 组合（部分结果）</li>
     *   <li>delta 字段（增量文本）</li>
     *   <li>item.transcript 字段（备用）</li>
     * </ol>
     * </p>
     */
    static String extractTranscriptPayload(JsonObject message) {
        if (message.has("transcript") && !message.get("transcript").isJsonNull()) {
            JsonElement el = message.get("transcript");
            if (el.isJsonPrimitive()) {
                return el.getAsString();
            }
        }
        // Real-time partial: text + stash (see Alibaba qwen-asr-realtime server events doc)
        if (message.has("text") || message.has("stash")) {
            String prefix = "";
            String suffix = "";
            if (message.has("text") && !message.get("text").isJsonNull() && message.get("text").isJsonPrimitive()) {
                prefix = message.get("text").getAsString();
            }
            if (message.has("stash") && !message.get("stash").isJsonNull() && message.get("stash").isJsonPrimitive()) {
                suffix = message.get("stash").getAsString();
            }
            String combined = prefix + suffix;
            if (!combined.isBlank()) {
                return combined;
            }
        }
        if (message.has("delta")) {
            JsonElement d = message.get("delta");
            if (d.isJsonPrimitive()) {
                return d.getAsString();
            }
            if (d.isJsonObject()) {
                JsonObject o = d.getAsJsonObject();
                if (o.has("text") && !o.get("text").isJsonNull()) {
                    return o.get("text").getAsString();
                }
                if (o.has("transcript") && !o.get("transcript").isJsonNull()) {
                    return o.get("transcript").getAsString();
                }
            }
        }
        if (message.has("item") && message.get("item").isJsonObject()) {
            JsonObject item = message.getAsJsonObject("item");
            if (item.has("transcript") && !item.get("transcript").isJsonNull()) {
                return item.get("transcript").getAsString();
            }
        }
        return null;
    }

    /**
     * Internal class to hold session data.
     */
    private static class AsrSession {
        private final OmniRealtimeConversation conversation;
        private final Consumer<String> onFinal;
        private final Consumer<String> onPartial;
        private final Consumer<Throwable> onError;

        AsrSession(
                OmniRealtimeConversation conversation,
                Consumer<String> onFinal,
                Consumer<String> onPartial,
                Consumer<Throwable> onError) {
            this.conversation = conversation;
            this.onFinal = onFinal;
            this.onPartial = onPartial;
            this.onError = onError;
        }

        public OmniRealtimeConversation getConversation() {
            return conversation;
        }

        public Consumer<Throwable> getOnError() {
            return onError;
        }
    }

    // Setter methods for configuration (used by Spring @Value injection or tests)

    public void setUrl(String url) {
        this.url = url;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setSampleRate(Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    public void setEnableTurnDetection(Boolean enableTurnDetection) {
        this.enableTurnDetection = enableTurnDetection;
    }

    public void setTurnDetectionType(String turnDetectionType) {
        this.turnDetectionType = turnDetectionType;
    }

    public void setTurnDetectionThreshold(Float turnDetectionThreshold) {
        this.turnDetectionThreshold = turnDetectionThreshold;
    }

    public void setTurnDetectionSilenceDurationMs(Integer turnDetectionSilenceDurationMs) {
        this.turnDetectionSilenceDurationMs = turnDetectionSilenceDurationMs;
    }
}
