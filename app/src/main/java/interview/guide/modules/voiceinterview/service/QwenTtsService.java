package interview.guide.modules.voiceinterview.service;

import com.alibaba.dashscope.audio.qwen_tts_realtime.QwenTtsRealtime;
import com.alibaba.dashscope.audio.qwen_tts_realtime.QwenTtsRealtimeAudioFormat;
import com.alibaba.dashscope.audio.qwen_tts_realtime.QwenTtsRealtimeCallback;
import com.alibaba.dashscope.audio.qwen_tts_realtime.QwenTtsRealtimeConfig;
import com.alibaba.dashscope.audio.qwen_tts_realtime.QwenTtsRealtimeParam;
import com.google.gson.JsonObject;
import interview.guide.modules.voiceinterview.config.VoiceInterviewProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Qwen TTS Realtime Service (WebSocket-based)
 *
 * 阿里云通义千问实时语音合成服务
 * <p>
 * 基于 WebSocket 协议实现低延迟文本转语音（TTS），支持流式音频块接收。
 * </p>
 *
 * 核心特性：
 * <ul>
 *   <li><b>WebSocket 实时连接</b>：建立长连接，避免每次请求重复握手开销</li>
 *   <li><b>同步阻塞接口</b>：通过 CountDownLatch 等待合成完成，超时保护 30 秒</li>
 *   <li><b>音频块收集</b>：自动累积 response.audio.delta 事件中的 Base64 音频片段</li>
 *   <li><b>参数可配置</b>：音色、语速、音量、采样率等均通过 VoiceInterviewProperties 配置</li>
 * </ul>
 *
 * 工作流程：
 * <ol>
 *   <li>创建 WebSocket 连接并注册回调</li>
 *   <li>发送 session.update 配置会话参数（音色、格式等）</li>
 *   <li>调用 appendText() + commit() 触发合成</li>
 *   <li>在 onEvent 回调中接收 audio.delta 音频块并累积</li>
 *   <li>收到 response.done 后释放 CountDownLatch，返回完整 PCM 数据</li>
 * </ol>
 *
 * @see QwenTtsRealtime DashScope SDK 实时 TTS 客户端
 * @see QwenTtsRealtimeCallback WebSocket 事件回调接口
 */
@Slf4j
@Service
public class QwenTtsService {

    // Runtime configuration values (loaded from VoiceInterviewProperties; setters kept for tests)
    private String model;

    private String apiKey;

    private String voice;

    private String format;

    private Integer sampleRate;

    private String mode;

    private String languageType;

    private Float speechRate;

    private Integer volume;

    public QwenTtsService(VoiceInterviewProperties voiceInterviewProperties) {
        VoiceInterviewProperties.QwenTtsConfig tts = voiceInterviewProperties.getQwen().getTts();
        this.model = tts.getModel();
        this.apiKey = tts.getApiKey();
        this.voice = tts.getVoice();
        this.format = tts.getFormat();
        this.sampleRate = tts.getSampleRate();
        this.mode = tts.getMode();
        this.languageType = tts.getLanguageType();
        this.speechRate = tts.getSpeechRate();
        this.volume = tts.getVolume();
    }

    /**
     * Initialize the TTS service.
     * This method is automatically called by Spring after the service is constructed
     * and all configuration values have been loaded from VoiceInterviewProperties.
     *
     * @throws IllegalStateException if apiKey is not configured
     */
    @PostConstruct
    public void init() {
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalStateException("API key must be configured before initializing QwenTtsService");
        }
        log.info("QwenTtsService initialized with model: {}, voice: {}, sampleRate: {}Hz",
                 model, voice, sampleRate);
    }

    /**
     * 将文本合成为 PCM 音频数据
     * <p>
     * 该方法通过 DashScope WebSocket API 同步执行 TTS 合成：
     * <ol>
     *   <li>建立 WebSocket 连接到阿里云服务</li>
     *   <li>发送文本并触发合成（commit 模式）</li>
     *   <li>在回调中收集音频块（Base64 解码）</li>
     *   <li>通过 CountDownLatch 等待合成完成（最多 30 秒）</li>
     *   <li>返回完整的 PCM 音频字节数组</li>
     * </ol>
     * </p>
     *
     * @param text 待合成的文本（null、空或纯空格文本返回空数组）
     * @return PCM 音频数据（24kHz 采样率），合成失败返回空数组
     */
    public byte[] synthesize(String text) {
        // Handle null, empty, or whitespace-only text
        if (text == null || text.trim().isEmpty()) {
            log.debug("Empty or null text provided, returning empty audio array");
            return new byte[0];
        }

        log.debug("Starting TTS synthesis for text: {} characters", text.length());

        // ① 初始化同步等待锁（CountDownLatch）
        // 用于阻塞当前线程，直到 TTS 合成完成或超时
        CountDownLatch synthesisLatch = new CountDownLatch(1);

        // ② 音频数据容器（线程安全）
        // 用于累积从 WebSocket 回调中收到的音频块
        ByteArrayContainer audioContainer = new ByteArrayContainer();

        // ③ 错误引用容器
        // 用于在回调线程和主线程之间传递异常信息
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        // ④ 响应 ID 容器（用于追踪和日志）
        AtomicReference<String> responseIdRef = new AtomicReference<>();

        try {
            // 步骤 1：构建 WebSocket 连接参数
            QwenTtsRealtimeParam param = QwenTtsRealtimeParam.builder()
                    .model(model)      // 模型名称：qwen-tts-realtime
                    .apikey(apiKey)    // API Key 认证
                    .build();

            // 步骤 2：创建 WebSocket 事件回调处理器
            QwenTtsRealtimeCallback callback = new QwenTtsRealtimeCallback() {
                @Override
                public void onOpen() {
                    log.debug("TTS WebSocket connection established");
                }

                @Override
                public void onEvent(JsonObject message) {
                    // 处理服务端推送的事件（音频块、完成信号、错误等）
                    handleServerEvent(message, audioContainer, synthesisLatch, errorRef, responseIdRef);
                }

                @Override
                public void onClose(int code, String reason) {
                    log.debug("TTS WebSocket closed - code: {}, reason: {}", code, reason);
                    // 连接关闭时释放锁，防止主线程永久阻塞
                    synthesisLatch.countDown();
                }
            };

            // 步骤 3：创建 TTS 客户端实例
            QwenTtsRealtime qwenTtsRealtime = new QwenTtsRealtime(param, callback);

            try {
                // 步骤 4：建立 WebSocket 连接（阻塞操作）
                qwenTtsRealtime.connect();

                // 步骤 5：配置会话参数（音色、音频格式、语速等）
                QwenTtsRealtimeConfig config = QwenTtsRealtimeConfig.builder()
                        .voice(voice)                          // 音色：Cherry/Serena/Ethan 等
                        .responseFormat(getAudioFormat())      // 音频格式：PCM_24000HZ_MONO_16BIT
                        .mode(mode)                            // 模式："commit"（用户手动触发）
                        .languageType(languageType)            // 语言类型：zh（中文）
                        .speechRate(speechRate)                // 语速：1.0 为标准速度
                        .volume(volume)                        // 音量：0-100
                        .build();

                // 步骤 6：更新会话配置
                qwenTtsRealtime.updateSession(config);

                log.info("[TTS] Session configured with voice: {}, triggering synthesis for text (length: {})",
                         voice, text.length());

                // 步骤 7：发送文本并触发合成
                qwenTtsRealtime.appendText(text);  // 追加文本到缓冲区
                qwenTtsRealtime.commit();          // 提交并触发合成

                log.info("[TTS] Text sent to TTS service, waiting for audio response...");

                // 步骤 8：等待合成完成（最多 30 秒）
                boolean completed = synthesisLatch.await(30, TimeUnit.SECONDS);

                if (!completed) {
                    log.error("TTS synthesis timeout after 30 seconds");
                    return new byte[0];
                }

                // Check if error occurred
                Throwable error = errorRef.get();
                if (error != null) {
                    log.error("TTS synthesis failed", error);
                    return new byte[0];
                }

                // Return collected audio data
                byte[] audioData = audioContainer.toByteArray();
                log.info("[TTS] Synthesis completed successfully - {} bytes of audio data, responseId: {}",
                         audioData.length, responseIdRef.get());

                return audioData;

            } finally {
                // Ensure connection is closed
                try {
                    qwenTtsRealtime.close();
                } catch (Exception e) {
                    log.error("Error closing TTS connection", e);
                }
            }

        } catch (InterruptedException e) {
            log.error("TTS synthesis interrupted", e);
            Thread.currentThread().interrupt();
            return new byte[0];
        } catch (Exception e) {
            log.error("Failed to synthesize text", e);
            return new byte[0];
        }
    }

    /**
     * Get audio format for Qwen TTS Realtime.
     * Currently supports 24kHz PCM format.
     *
     * @return QwenTtsRealtimeAudioFormat enum value
     */
    private QwenTtsRealtimeAudioFormat getAudioFormat() {
        // Qwen TTS Realtime uses 24kHz by default
        return QwenTtsRealtimeAudioFormat.PCM_24000HZ_MONO_16BIT;
    }

    /**
     * Destroy the service and cleanup resources.
     *
     * This method is called automatically when the Spring container shuts down.
     * Currently, no persistent resources need cleanup as each synthesis creates
     * its own temporary connection.
     */
    @PreDestroy
    public void destroy() {
        log.info("QwenTtsService destroyed successfully");
    }

    /**
     * 处理 DashScope TTS 服务端推送的事件
     * <p>
     * 支持的事件类型：
     * <ul>
     *   <li><b>session.created</b>：会话创建成功</li>
     *   <li><b>session.updated</b>：会话配置更新完成</li>
     *   <li><b>response.audio.delta</b>：收到音频块（Base64 编码），需解码并累积</li>
     *   <li><b>response.done</b>：合成完成，释放 CountDownLatch</li>
     *   <li><b>error</b>：发生错误，记录异常并释放锁</li>
     * </ul>
     * </p>
     *
     * @param message JSON 格式的事件消息
     * @param audioContainer 音频块累积容器
     * @param synthesisLatch 同步等待锁
     * @param errorRef 异常引用容器
     * @param responseIdRef 响应 ID 引用容器
     */
    private void handleServerEvent(JsonObject message, ByteArrayContainer audioContainer,
                                    CountDownLatch synthesisLatch, AtomicReference<Throwable> errorRef,
                                    AtomicReference<String> responseIdRef) {
        try {
            String eventType = message.get("type").getAsString();

            if (log.isTraceEnabled()) {
                log.trace("Received TTS event: {}, full message: {}", eventType, message);
            } else {
                log.debug("Received TTS event: {}", eventType);
            }

            switch (eventType) {
                case "session.created":
                    String sessionId = message.has("session") && message.get("session").isJsonObject()
                            ? message.get("session").getAsJsonObject().get("id").getAsString()
                            : "unknown";
                    log.debug("TTS session created: {}", sessionId);
                    break;

                case "session.updated":
                    log.debug("TTS session configuration updated");
                    break;

                case "response.audio.delta":
                    // 收到音频块事件：delta 字段为 Base64 编码的 PCM 数据
                    if (message.has("delta")) {
                        String audioBase64 = message.get("delta").getAsString();
                        if (audioBase64 != null && !audioBase64.isEmpty()) {
                            // Base64 解码为原始 PCM 字节数组
                            byte[] audioChunk = Base64.getDecoder().decode(audioBase64);
                            audioContainer.append(audioChunk);  // 累积到容器
                            log.trace("Received audio chunk - {} bytes", audioChunk.length);
                        }
                    }
                    break;

                case "response.done":
                    // 合成完成事件：这是 Qwen TTS API 的最终信号
                    String responseId = responseIdRef.get();
                    log.debug("TTS response completed - responseId: {}", responseId);
                    // 释放 CountDownLatch，唤醒主线程
                    synthesisLatch.countDown();
                    break;

                case "error":
                    // Error event
                    if (message.has("error")) {
                        var errorElement = message.get("error");
                        String errorType = "unknown";
                        String errorCode = "unknown";
                        String errorMessage = "Unknown error";

                        if (errorElement.isJsonObject()) {
                            JsonObject errorObj = errorElement.getAsJsonObject();
                            errorType = errorObj.has("type") ? errorObj.get("type").getAsString() : "unknown";
                            errorCode = errorObj.has("code") ? errorObj.get("code").getAsString() : "unknown";
                            errorMessage = errorObj.has("message") ? errorObj.get("message").getAsString() : "Unknown error";
                        } else {
                            errorMessage = errorElement.toString();
                        }

                        String fullErrorMessage = String.format("TTS Error [%s/%s]: %s", errorType, errorCode, errorMessage);
                        log.error("{}", fullErrorMessage);

                        errorRef.set(new IllegalStateException(fullErrorMessage));
                        synthesisLatch.countDown();
                    }
                    break;

                default:
                    log.trace("Unhandled TTS event type: {}", eventType);
            }

        } catch (Exception e) {
            log.error("Error processing TTS server event", e);
            errorRef.set(e);
            synthesisLatch.countDown();
        }
    }

    /**
     * 内部类：高效收集音频块的容器
     * <p>
     * 使用 ByteArrayOutputStream 实现摊销 O(1) 的追加性能，
     * 避免手动数组扩容导致的 O(n²) 复制开销。
     * </p>
     * <p>
     * <b>线程安全</b>：所有方法使用 synchronized 修饰，支持多线程并发写入
     * </p>
     */
    private static class ByteArrayContainer {
        private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();

        public synchronized void append(byte[] chunk) {
            baos.write(chunk, 0, chunk.length);
        }

        public synchronized byte[] toByteArray() {
            return baos.toByteArray();
        }
    }

    // Setter methods for configuration (used by Spring @Value injection or tests)

    public void setModel(String model) {
        this.model = model;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public void setVoice(String voice) {
        this.voice = voice;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setSampleRate(Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setLanguageType(String languageType) {
        this.languageType = languageType;
    }

    public void setSpeechRate(Float speechRate) {
        this.speechRate = speechRate;
    }

    public void setVolume(Integer volume) {
        this.volume = volume;
    }
}
