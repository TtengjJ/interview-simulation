package interview.guide.modules.voiceinterview.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import interview.guide.modules.voiceinterview.dto.WebSocketControlMessage;
import interview.guide.modules.voiceinterview.dto.WebSocketSubtitleMessage;
import interview.guide.modules.voiceinterview.model.VoiceInterviewMessageEntity;
import interview.guide.modules.voiceinterview.model.VoiceInterviewSessionEntity;
import interview.guide.modules.voiceinterview.config.VoiceInterviewProperties;
import interview.guide.modules.voiceinterview.service.QwenAsrService;
import interview.guide.modules.voiceinterview.service.QwenTtsService;
import interview.guide.modules.voiceinterview.service.DashscopeLlmService;
import interview.guide.modules.voiceinterview.service.VoiceInterviewService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WebSocket Handler for Voice Interview
 * 语音面试 WebSocket 处理器
 * <p>
 * Handles real-time bidirectional audio streaming for voice interviews.
 * Processing pipeline: User Audio → STT → LLM → TTS → AI Audio
 * </p>
 * 
 * <h2>核心架构</h2>
 * <ul>
 *   <li><b>音频流处理</b>：接收前端 Base64 编码的 PCM 音频，通过 DashScope ASR 实时转文字</li>
 *   <li><b>文本合并策略</b>：VAD 切分后的多段 STT 结果先缓存，用户手动提交后统一触发 LLM</li>
 *   <li><b>LLM 响应</b>：支持流式/非流式两种模式，流式模式下按句子级并发调用 TTS</li>
 *   <li><b>TTS 合成</b>：将 LLM 生成的文本转为 WAV 音频返回前端播放</li>
 *   <li><b>回声抑制</b>：AI 说话期间及冷却期（800ms）内丢弃麦克风输入，防止回声触发 STT</li>
 *   <li><b>会话管理</b>：暂停超时检测（4:30 警告，5:00 自动暂停）、断连自动结束会话</li>
 * </ul>
 * 
 * <h2>线程模型</h2>
 * <ul>
 *   <li><b>utteranceMergeScheduler</b>：2 个调度线程，负责 STT 片段合并的延迟调度</li>
 *   <li><b>voicePipelineExecutor</b>：虚拟线程池，执行 LLM/TTS/JDBC 等阻塞操作，避免占满调度器</li>
 *   <li><b>WebSocket 主线程</b>：处理连接建立、消息接收、断开清理</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class VoiceInterviewWebSocketHandler extends TextWebSocketHandler implements DisposableBean {

    private final ObjectMapper objectMapper;
    /** 阿里云 DashScope ASR 服务：实时语音识别（STT） */
    private final QwenAsrService sttService;
    /** 阿里云 DashScope TTS 服务：文本转语音 */
    private final QwenTtsService ttsService;
    /** DashScope LLM 服务：大语言模型对话生成 */
    private final DashscopeLlmService llmService;
    /** 语音面试业务服务：会话管理、消息持久化、状态控制 */
    private final VoiceInterviewService interviewService;
    /** 语音面试配置属性：开场问题、流式开关、超时时间等 */
    private final VoiceInterviewProperties voiceInterviewProperties;
    /** Micrometer 指标注册表（可选注入，用于监控埋点） */
    private final ObjectProvider<MeterRegistry> meterRegistryProvider;

    /**
     * 合并多段 STT 定稿后再触发 LLM 的延迟调度
     * <p>
     * 用途：当 VAD 检测到用户停顿并输出 final 片段时，不立即触发 LLM，而是等待一段时间（如 1.5s）
     * 收集更多片段，减少频繁调用 LLM 的成本。
     * </p>
     */
    private final ScheduledExecutorService utteranceMergeScheduler = createUtteranceMergeScheduler();

    /**
     * LLM / TTS / JDBC 等阻塞工作全部跑在虚拟线程上，避免占满 utteranceMergeScheduler 的 2 个调度线程。
     * <p>
     * Java 21 虚拟线程特性：每个任务独立虚拟线程，阻塞时自动让出平台线程，高并发场景下性能优异。
     * </p>
     */
    private final ExecutorService voicePipelineExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /** 活跃 WebSocket 会话映射：sessionId → WebSocketSession */
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    /** 会话状态映射：sessionId → SessionState（包含累积文本、处理锁、AI 说话标记等） */
    private final Map<String, SessionState> sessionStates = new ConcurrentHashMap<>();
    /** 开场问题音频缓存：text → WAV 音频字节数组，避免重复 TTS 调用 */
    private final Map<String, byte[]> openingAudioCache = new ConcurrentHashMap<>();

    // Activity tracking for pause timeout
    // 活动跟踪（用于暂停超时）：记录每个会话最后活动时间，定时任务检测超时
    private final Map<String, Long> lastActivityTime = new ConcurrentHashMap<>();
    /** 暂停警告阈值：4 分 30 秒无活动时发送警告 */
    private static final long WARNING_TIME_MS = (long) (4.5 * 60 * 1000);  // 4:30
    /** 暂停超时阈值：5 分钟无活动自动暂停会话 */
    private static final long PAUSE_TIMEOUT_MS = 5 * 60 * 1000;            // 5:00
    /** WebSocket 发送超时限制（毫秒），超过则抛出异常 */
    private static final int WS_SEND_TIME_LIMIT_MS = 10_000;
    /** WebSocket 发送缓冲区限制（字节），防止内存溢出 */
    private static final int WS_SEND_BUFFER_LIMIT_BYTES = 512 * 1024;
    /** AI 音频播放结束后的冷却期，防止扬声器尾音被麦克风拾取触发 STT */
    private static final long AI_SPEAK_COOLDOWN_MS = 800;
    private static final String DEFAULT_OPENING_QUESTION_ALGORITHM =
        "你好，我是本场面试官。第一个问题：请你口述一道算法题，不写代码，只讲\u300C问题建模、数据结构选型、步骤、复杂度、边界处理\u300D。";
    private static final String DEFAULT_OPENING_QUESTION_BACKEND =
        "你好，我是本场面试官。第一个问题：请用 1 分钟介绍一个你深度参与的项目，按三点回答：业务目标、你负责的核心模块、核心技术栈。说完我会立刻追问一个关键技术决策。";

    private static ScheduledExecutorService createUtteranceMergeScheduler() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2, r -> {
            Thread t = new Thread(r, "voice-utterance-merge");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    @PostConstruct
    void warmupOpeningAudioCache() {
        // 异步预热开场问题音频缓存，避免首次请求时 TTS 延迟影响用户体验
        voicePipelineExecutor.execute(() -> {
            try {
                VoiceInterviewProperties.OpeningConfig opening = voiceInterviewProperties.getOpening();
                if (opening == null) {
                    return;
                }
                // 收集所有可能的开场问题模板（技能专属 + 算法通用 + 后端通用）
                LinkedHashSet<String> allTemplates = new LinkedHashSet<>();
                if (opening.getSkillQuestions() != null) {
                    allTemplates.addAll(opening.getSkillQuestions().values());
                }
                allTemplates.add(opening.getAlgorithmQuestion());
                allTemplates.add(opening.getBackendQuestion());
                // 预合成每个模板的 WAV 音频并缓存
                for (String template : allTemplates) {
                    preloadOpeningAudio(template);
                }
                log.info("Opening audio cache warmed: {} entries", openingAudioCache.size());
            } catch (Exception e) {
                log.warn("Opening audio cache warmup skipped: {}", e.getMessage());
            }
        });
    }

    private void preloadOpeningAudio(String text) {
        if (text == null || text.isBlank()) {
            return;
        }
        byte[] wavAudio = synthesizeToWav(text);
        if (wavAudio.length > 0) {
            openingAudioCache.put(text, wavAudio);
        }
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        // 从 URI 路径提取 sessionId（格式：/ws/voice-interview/{sessionId}）
        String sessionId = extractSessionId(session);

        // Increase message size limits for audio streaming
        // 1 second of PCM audio @ 16kHz, 16-bit = ~32KB raw, ~42KB base64
        // Set limit to 256KB to allow some buffer and multiple messages
        // 调整消息大小限制：1 秒 16kHz/16bit PCM 音频约 32KB，Base64 编码后约 42KB，设置 256KB 留有余量
        session.setTextMessageSizeLimit(256 * 1024); // 256KB
        session.setBinaryMessageSizeLimit(256 * 1024); // 256KB

        // 装饰 WebSocketSession，增加并发发送保护和缓冲区限制，防止消息堆积导致 OOM
        WebSocketSession safeSession = new ConcurrentWebSocketSessionDecorator(
            session, WS_SEND_TIME_LIMIT_MS, WS_SEND_BUFFER_LIMIT_BYTES
        );

        // 注册会话到全局映射表
        sessions.put(sessionId, safeSession);
        sessionStates.put(sessionId, new SessionState());
        lastActivityTime.put(sessionId, System.currentTimeMillis());
        log.info("WebSocket connection established for session: {}", sessionId);

        try {
            // 启动 DashScope ASR 实时转录，注册 partial/final/error 回调
            startDashScopeStt(sessionId, safeSession);

            // 发送欢迎消息
            sendMessage(safeSession, createWelcomeMessage());
            // 自动开场：面试官先说开场语并直接提出第一个问题（仅首次连接、无历史消息时触发）
            triggerOpeningQuestionIfNeeded(sessionId, safeSession);
        } catch (Exception e) {
            log.error("Error establishing WebSocket connection for session {}", sessionId, e);
            sendError(safeSession, "初始化语音识别失败: " + e.getMessage());
        }
    }

    /**
     * Create welcome message
     */
    private String createWelcomeMessage() {
        return toJson(Map.of(
            "type", "control",
            "action", "welcome",
            "message", "连接成功，准备开始语音面试",
            "timestamp", System.currentTimeMillis()
        ));
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Error serializing JSON", e);
            return "{}";
        }
    }

    /**
     * Send message to WebSocket session
     */
    private void sendMessage(WebSocketSession session, String message) {
        try {
            if (session.isOpen()) {
                session.sendMessage(new TextMessage(message));
                log.debug("Message sent to session: {}", message.substring(0, Math.min(100, message.length())));
            } else {
                log.warn("Session is closed, cannot send message");
            }
        } catch (Exception e) {
            log.error("Error sending message to session", e);
        }
    }

    private void triggerOpeningQuestionIfNeeded(String sessionId, WebSocketSession session) {
        // 在虚拟线程中执行，避免阻塞 WebSocket 主线程
        voicePipelineExecutor.execute(() -> {
            try {
                if (session == null || !session.isOpen()) {
                    return;
                }

                // 检查是否有历史对话（重连/恢复场景不重复开场）
                List<String> history = getHistory(sessionId);
                if (history != null && !history.isEmpty()) {
                    // 已有历史对话（如重连/恢复），不重复开场
                    return;
                }

                // 获取会话实体，确定技能类型（算法/后端/其他）
                VoiceInterviewSessionEntity sessionEntity = getSessionEntity(sessionId);
                if (sessionEntity == null) {
                    log.warn("Session entity not found when sending opening question: {}", sessionId);
                    return;
                }

                // 根据技能类型构建开场问题
                String aiReply = buildOpeningQuestion(sessionEntity);
                if (aiReply == null || aiReply.isBlank()) {
                    return;
                }

                if (!session.isOpen()) {
                    return;
                }

                // 文本先行，保证前端立即可见（低延迟体验）
                sendTextMessage(session, aiReply);
                // 保存消息到数据库
                saveMessage(sessionId, null, aiReply);

                // 语音随后下发（从缓存或实时合成）
                byte[] wavAudio = getOpeningWavAudio(aiReply);
                if (wavAudio.length > 0 && session.isOpen()) {
                    sendAudio(session, wavAudio, aiReply);
                }

                log.info("Opening question sent for session {}", sessionId);
            } catch (Exception e) {
                log.error("Failed to send opening question for session {}", sessionId, e);
            }
        });
    }

    private byte[] getOpeningWavAudio(String text) {
        byte[] cached = openingAudioCache.get(text);
        if (cached != null && cached.length > 0) {
            return cached;
        }
        byte[] wav = synthesizeToWav(text);
        if (wav.length > 0) {
            openingAudioCache.put(text, wav);
        }
        return wav;
    }

    private byte[] synthesizeToWav(String text) {
        byte[] pcm = ttsService.synthesize(text);
        if (pcm == null || pcm.length == 0) {
            return new byte[0];
        }
        return convertPcmToWav(pcm);
    }

    private String buildOpeningQuestion(VoiceInterviewSessionEntity sessionEntity) {
        String skillId = sessionEntity.getSkillId() != null ? sessionEntity.getSkillId() : "";
        VoiceInterviewProperties.OpeningConfig opening = voiceInterviewProperties.getOpening();
        Map<String, String> skillQuestions = opening != null ? opening.getSkillQuestions() : null;
        if (skillQuestions != null) {
            String bySkill = skillQuestions.get(skillId);
            if (bySkill != null && !bySkill.isBlank()) {
                return bySkill;
            }
        }
        List<String> algorithmSkills = opening != null && opening.getAlgorithmSkills() != null
            ? opening.getAlgorithmSkills()
            : List.of();

        if (algorithmSkills.contains(skillId)) {
            String configured = opening != null ? opening.getAlgorithmQuestion() : null;
            return configured != null && !configured.isBlank()
                ? configured
                : DEFAULT_OPENING_QUESTION_ALGORITHM;
        }
        String configured = opening != null ? opening.getBackendQuestion() : null;
        return configured != null && !configured.isBlank()
            ? configured
            : DEFAULT_OPENING_QUESTION_BACKEND;
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String sessionId = extractSessionId(session);

        // 记录接收消息的大小，便于排查大消息问题
        int messageSize = message.getPayload().length();
        int messageSizeKB = messageSize / 1024;
        log.info("[WebSocket] Received message: sessionId={}, size={}KB ({} bytes), limit={}KB",
                sessionId, messageSizeKB, messageSize, session.getTextMessageSizeLimit() / 1024);

        if (messageSizeKB > 200) {
            log.warn("[WebSocket] Large message detected: {}KB", messageSizeKB);
        }

        try {
            // 解析 JSON 消息
            JsonNode msg = objectMapper.readTree(message.getPayload());
            String type = msg.get("type").asText();

            // Update last activity time for pause timeout detection
            // 更新最后活动时间（用于暂停超时检测）：任何消息都视为用户活跃
            lastActivityTime.put(sessionId, System.currentTimeMillis());

            switch (type) {
                case "audio":
                    // 处理音频消息：Base64 解码后发送给 ASR
                    String audioData = msg.has("data") ? msg.get("data").asText() : null;
                    if (audioData != null && !audioData.isEmpty()) {
                        handleUserAudio(sessionId, audioData);
                    } else {
                        log.warn("Received audio message without data");
                    }
                    break;
                case "control":
                    // 处理控制消息：submit（提交文本）、end_interview（结束面试）、start_phase（开始阶段）
                    try {
                        handleControl(sessionId, objectMapper.treeToValue(msg, WebSocketControlMessage.class));
                    } catch (Exception e) {
                        log.error("Error handling control message for session {}", sessionId, e);
                        sendError(session, "控制消息处理失败: " + e.getMessage());
                    }
                    break;
                default:
                    log.warn("Unknown message type: {} for session {}", type, sessionId);
            }

        } catch (Exception e) {
            log.error("Error handling message for session {}", sessionId, e);
            sendError(session, "消息处理失败: " + e.getMessage());
        }
    }

    @Override
    public void destroy() {
        // 优雅关闭线程池：先中断所有任务，再等待终止
        voicePipelineExecutor.shutdownNow();
        utteranceMergeScheduler.shutdownNow();
        try {
            if (!voicePipelineExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("voicePipelineExecutor did not terminate within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        String sessionId = extractSessionId(session);
        try {
            // 从全局映射表中移除会话
            sessions.remove(sessionId);
            SessionState removedState = sessionStates.remove(sessionId);
            if (removedState != null) {
                // 中断正在执行的 LLM+TTS 管线线程，避免资源浪费
                Thread t = removedState.getProcessingThread();
                if (t != null) {
                    t.interrupt();
                }
            }
            lastActivityTime.remove(sessionId);

            // Stop STT transcription
            // 停止 ASR 转录，释放 DashScope WebSocket 连接
            sttService.stopTranscription(sessionId);
            log.info("WebSocket connection closed for session: {}, status: {}", sessionId, status);

            // WebSocket 异常断开时自动结束会话，防止状态永远停留在 IN_PROGRESS
            try {
                interviewService.endSessionIfInProgress(sessionId);
            } catch (Exception endEx) {
                log.warn("Failed to auto-end session {} after disconnect: {}", sessionId, endEx.getMessage());
            }
        } catch (Exception e) {
            log.error("Error cleaning up session {} after close", sessionId, e);
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("WebSocket transport error for session {}", extractSessionId(session), exception);
    }

    /**
     * 判断 ASR 异常是否可恢复（断线重连场景）
     * <p>
     * 可恢复错误：
     * <ul>
     *   <li>"No active session"：ASR 会话未激活（可能已超时断开）</li>
     *   <li>"ASR append failed"：音频推送失败（网络波动）</li>
     * </ul>
     * </p>
     */
    private static boolean shouldRecoverAsrConnection(IllegalStateException ex) {
        String m = ex.getMessage();
        if (m == null) {
            return false;
        }
        return m.contains("No active session") || m.contains("ASR append failed");
    }

    private void startDashScopeStt(String sessionId, WebSocketSession session) {
        sttService.startTranscription(
                sessionId,
                text -> handleSttResult(sessionId, text, true),
                text -> handleSttResult(sessionId, text, false),
                error -> {
                    log.error("STT error for session {}", sessionId, error);
                    sendError(session, "语音识别失败: " + error.getMessage());
                }
        );
    }

    /**
     * DashScope ASR 断线后重连（回调与首次 start 一致）
     */
    private void restartDashScopeStt(String sessionId) {
        WebSocketSession session = sessions.get(sessionId);
        if (session == null || !session.isOpen()) {
            return;
        }
        sttService.restartTranscription(
                sessionId,
                text -> handleSttResult(sessionId, text, true),
                text -> handleSttResult(sessionId, text, false),
                error -> {
                    log.error("STT error for session {}", sessionId, error);
                    sendError(session, "语音识别失败: " + error.getMessage());
                }
        );
    }

    /**
     * 处理用户音频消息
     * <p>
     * 核心流程：
     * <ol>
     *   <li>Base64 解码为 PCM 字节数组</li>
     *   <li>检查 AI 是否在说话或处于冷却期，是则丢弃（回声抑制）</li>
     *   <li>发送给 DashScope ASR 进行实时转录</li>
     *   <li>如果发送失败且是可恢复错误，尝试重启 ASR 并重试最多 15 次</li>
     * </ol>
     * </p>
     *
     * @param sessionId 会话 ID
     * @param base64Audio Base64 编码的 PCM 音频数据（前端采集的麦克风输入）
     */
    private void handleUserAudio(String sessionId, String base64Audio) {
        WebSocketSession session = sessions.get(sessionId);
        if (session == null) {
            log.warn("Session not found: {}", sessionId);
            return;
        }

        // AI 正在说话或处于回声冷却期时，丢弃麦克风输入，防止回声触发 LLM
        SessionState state = sessionStates.get(sessionId);
        if (state != null && state.isAiSpeakingOrCooldown()) {
            return;
        }

        try {
            byte[] audioData = Base64.getDecoder().decode(base64Audio);
            log.debug("Received audio data for session {}, size: {} bytes", sessionId, audioData.length);

            try {
                // 正常推送音频到 ASR
                sttService.sendAudio(sessionId, audioData);
            } catch (IllegalStateException ex) {
                // ASR 连接断开，尝试重连
                if (shouldRecoverAsrConnection(ex)) {
                    log.warn("[Session: {}] ASR send failed ({}), restarting DashScope and retrying chunk",
                            sessionId, ex.getMessage() != null ? ex.getMessage() : "unknown");
                    restartDashScopeStt(sessionId);
                    // 重试最多 15 次，每次等待 80ms，总等待时间约 1.2s
                    boolean sent = false;
                    for (int i = 0; i < 15; i++) {
                        try {
                            Thread.sleep(80);
                            sttService.sendAudio(sessionId, audioData);
                            sent = true;
                            break;
                        } catch (IllegalStateException retry) {
                            if (!shouldRecoverAsrConnection(retry)) {
                                throw retry;
                            }
                        }
                    }
                    if (!sent) {
                        log.error("[Session: {}] ASR still down after restart", sessionId);
                        sendError(session, "语音识别连接中断，请刷新页面后重试");
                    }
                } else {
                    throw ex;
                }
            }

        } catch (Exception e) {
            log.error("Error handling user audio for session {}", sessionId, e);
            String errorMessage = getErrorMessage(e);
            sendError(session, errorMessage);
        }
    }

    /**
     * 处理 STT 识别结果（来自 DashScope ASR 回调）
     * <p>
     * 两种模式：
     * <ul>
     *   <li><b>partial（流式片段）</b>：VAD 检测到语音但未停顿，实时更新字幕，不触发 LLM</li>
     *   <li><b>final（定稿片段）</b>：VAD 检测到停顿，将文本追加到 mergeBuffer，等待用户提交或超时</li>
     * </ul>
     * </p>
     *
     * @param sessionId 会话 ID
     * @param recognizedText 识别出的文本
     * @param isFinalSegment 是否为定稿片段（true=用户已停顿，false=仍在说话）
     */
    private void handleSttResult(String sessionId, String recognizedText, boolean isFinalSegment) {
        WebSocketSession session = sessions.get(sessionId);
        SessionState state = sessionStates.get(sessionId);

        if (session == null || state == null) {
            log.warn("Session or state not found: {}", sessionId);
            return;
        }

        // Partial 片段：仅更新字幕，记录活动时间
        if (!isFinalSegment) {
            state.markSttActivity();
            sendSubtitle(session, recognizedText, false);
            return;
        }

        // Final 片段：累积到 mergeBuffer，等待合并后统一触发 LLM
        log.debug("STT final segment for session {}: {}", sessionId, recognizedText);
        incrementCounter("app.voice.interview.asr.final_segments", "status", "received");

        // 合并多次 VAD 切段，等用户手动提交后统一触发 LLM
        state.appendFinalSttSegment(recognizedText);
        // 实时更新字幕显示累积文本
        sendSubtitle(session, state.getMergeBufferPreview(), false);
    }

    /**
     * 手动提交：获取 mergeBuffer 中累积的用户文本并触发 LLM 管线
     * <p>
     * 调用时机：
     * <ul>
     *   <li>用户点击"提交"按钮</li>
     *   <li>前端通过 control/submit 消息触发</li>
     * </ul>
     * </p>
     * <p>
     * 并发控制：使用 CAS 确保同一会话只有一个 LLM 请求在处理，避免重复触发
     * </p>
     */
    private void flushMergedUtteranceToLlm(String sessionId) {
        WebSocketSession session = sessions.get(sessionId);
        SessionState state = sessionStates.get(sessionId);
        if (session == null || state == null || !session.isOpen()) {
            return;
        }
        // CAS 原子操作：如果 processing=false 则设为 true，否则延迟 400ms 重试
        if (!state.isProcessing().compareAndSet(false, true)) {
            utteranceMergeScheduler.schedule(
                    () -> flushMergedUtteranceToLlm(sessionId),
                    400,
                    TimeUnit.MILLISECONDS);
            return;
        }
        long mergeStartAt = state.getMergeStartedAt();
        // 取出并清空 mergeBuffer
        String userText = state.takeMergeBufferAndClear();
        if (userText == null || userText.trim().isEmpty()) {
            state.isProcessing().set(false);
            return;
        }
        // 记录合并等待时长指标
        long mergeWaitMs = Math.max(0, System.currentTimeMillis() - mergeStartAt);
        recordTimerMillis("app.voice.interview.asr.merge_wait", mergeWaitMs, "status", "success");
        state.setAccumulatedText(userText);
        log.info("Merged user utterance for session {}, triggering LLM (length {})", sessionId, userText.length());

        // 提交到虚拟线程执行阻塞的 LLM+TTS 管线，立即释放调度器线程
        voicePipelineExecutor.execute(() -> {
            state.setProcessingThread(Thread.currentThread());
            try {
                triggerLlmResponse(sessionId, session, state);
            } finally {
                state.isProcessing().set(false);
                state.setProcessingThread(null);
            }
        });
    }



    /**
     * 触发 LLM 响应（核心方法）
     * <p>
     * 完整处理流程：
     * <ol>
     *   <li>从数据库加载会话实体和对话历史</li>
     *   <li>调用 LLM 生成回复（支持流式/非流式两种模式）</li>
     *   <li>流式模式下，按句子级并发调用 TTS，实现边生成边播放</li>
     *   <li>非流式模式下，等待 LLM 完整输出后一次性调用 TTS</li>
     *   <li>将用户文本和 AI 回复保存到数据库</li>
     *   <li>发送 WAV 音频到前端播放</li>
     *   <li>设置 AI 说话标记和冷却期，防止回声</li>
     * </ol>
     * </p>
     * <p>
     * <b>流式模式优化</b>：
     * <ul>
     *   <li>LLM 每输出一个完整句子就启动 TTS，TTS 与 LLM 并行执行</li>
     *   <li>使用 Semaphore 限制并发 TTS 数量，避免资源耗尽</li>
     *   <li>支持分块推送（chunked）或合并推送两种音频传输模式</li>
     * </ul>
     * </p>
     *
     * @param sessionId 会话 ID
     * @param session WebSocket 会话
     * @param state 会话状态（包含用户文本、处理锁等）
     */
    private void triggerLlmResponse(String sessionId, WebSocketSession session, SessionState state) {
        long turnStartNanos = System.nanoTime();
        // 标记 AI 开始说话，阻止麦克风输入（回声抑制）
        state.aiSpeaking.set(true);
        try {
            if (!session.isOpen()) {
                log.warn("WebSocket session is closed, skipping LLM response for session {}", sessionId);
                return;
            }

            String userText = state.getAccumulatedText();
            if (userText == null || userText.trim().isEmpty()) {
                log.warn("Empty user text, skipping LLM response");
                return;
            }

            log.info("Getting LLM response for session {}, text: {}", sessionId, userText);

            // 从数据库获取会话实体（用于构建 Prompt 上下文）
            VoiceInterviewSessionEntity sessionEntity = getSessionEntity(sessionId);
            if (sessionEntity == null) {
                log.error("Session entity not found for session {}, cannot generate LLM response", sessionId);
                sendError(session, "会话不存在，请重新开始面试");
                return;
            }

            // 加载历史对话记录（提供上下文给 LLM）
            List<String> conversationHistory = getHistory(sessionId);

            long llmStartNanos = System.nanoTime();
            AtomicLong firstTokenAtNanos = new AtomicLong(0);
            boolean streamEnabled = voiceInterviewProperties.isLlmStreamingEnabled();
            String aiReply;

            if (streamEnabled) {
                // ==================== 流式模式 ====================
                // 句子级并发 TTS：LLM 流式输出期间每检测到一个完整句子就启动 TTS
                Semaphore ttsSemaphore = new Semaphore(
                    Math.max(1, voiceInterviewProperties.getMaxConcurrentTtsPerSession()));
                List<CompletableFuture<byte[]>> ttsFutures = new ArrayList<>();

                // 调用 LLM 流式接口，注册 partial/sentence 回调
                aiReply = llmService.chatStreamSentences(
                    userText,
                    // Partial 回调：每收到一段文本就发送到前端显示（低延迟字幕）
                    partialText -> {
                        if (partialText == null || partialText.isBlank() || !session.isOpen()) {
                            return;
                        }
                        // 记录首 token 延迟指标（TTFT）
                        if (firstTokenAtNanos.compareAndSet(0L, System.nanoTime())) {
                            recordTimerSinceNanos(
                                "app.voice.interview.llm.first_token_latency",
                                llmStartNanos,
                                "status", "success"
                            );
                        }
                        sendTextMessage(session, partialText);
                    },
                    // Sentence 回调：每检测到一个完整句子就启动 TTS 合成
                    sentence -> {
                        if (sentence == null || sentence.isBlank()) {
                            return;
                        }
                        // 信号量控制并发 TTS 数量
                        ttsSemaphore.acquireUninterruptibly();
                        CompletableFuture<byte[]> future = CompletableFuture.supplyAsync(() -> {
                            try {
                                return ttsService.synthesize(sentence);
                            } finally {
                                ttsSemaphore.release();
                            }
                        }, voicePipelineExecutor);
                        ttsFutures.add(future);
                    },
                    sessionEntity,
                    conversationHistory
                );

                // 记录 LLM 总耗时和调用次数指标
                recordTimerSinceNanos("app.voice.interview.llm.duration", llmStartNanos, "status", "success");
                incrementCounter("app.voice.interview.llm.calls", "status", "success", "streaming", "true");
                log.info("LLM response for session {}: '{}'", sessionId, aiReply);

                if (!session.isOpen()) {
                    log.warn("WebSocket closed during LLM processing, discarding response for session {}", sessionId);
                    return;
                }

                // 发送最终的用户文本字幕（标记为 final）
                sendSubtitle(session, userText, true);
                // 发送完整的 AI 回复文本
                sendTextMessage(session, aiReply);
                // 保存对话到数据库
                saveMessage(sessionId, userText, aiReply);

                // 按顺序收集所有 TTS 结果并发送音频
                if (!ttsFutures.isEmpty()) {
                    long ttsStartNanos = System.nanoTime();
                    boolean chunkedEnabled = voiceInterviewProperties.isChunkedAudioEnabled();

                    if (chunkedEnabled) {
                        // ==================== 分块推送模式 ====================
                        // 每句 TTS 完成后立即推送，前端逐句播放
                        for (int i = 0; i < ttsFutures.size(); i++) {
                            try {
                                byte[] pcm = ttsFutures.get(i).get();
                                if (pcm != null && pcm.length > 0 && session.isOpen()) {
                                    byte[] wavAudio = convertPcmToWav(pcm);
                                    sendAudioChunk(session, wavAudio, i, i == ttsFutures.size() - 1);
                                }
                            } catch (Exception e) {
                                log.warn("[Session: {}] TTS chunk {} failed: {}", sessionId, i, e.getMessage());
                            }
                        }
                        recordTimerSinceNanos("app.voice.interview.tts.duration", ttsStartNanos, "status", "success");
                    } else {
                        // ==================== 合并模式 ====================
                        // 收集所有 PCM 后合并为一个完整音频，一次性发送
                        List<byte[]> pcmChunks = new ArrayList<>();
                        int totalSize = 0;
                        for (CompletableFuture<byte[]> f : ttsFutures) {
                            try {
                                byte[] pcm = f.get();
                                if (pcm != null && pcm.length > 0) {
                                    pcmChunks.add(pcm);
                                    totalSize += pcm.length;
                                }
                            } catch (Exception e) {
                                log.warn("[Session: {}] TTS future failed for one sentence: {}", sessionId, e.getMessage());
                            }
                        }
                        recordTimerSinceNanos("app.voice.interview.tts.duration", ttsStartNanos, "status", "success");

                        if (!session.isOpen()) {
                            log.warn("WebSocket closed during TTS processing, discarding audio for session {}", sessionId);
                            return;
                        }

                        if (totalSize > 0) {
                            // 合并所有 PCM 片段
                            byte[] mergedPcm = new byte[totalSize];
                            int offset = 0;
                            for (byte[] chunk : pcmChunks) {
                                System.arraycopy(chunk, 0, mergedPcm, offset, chunk.length);
                                offset += chunk.length;
                            }
                            byte[] wavAudio = convertPcmToWav(mergedPcm);
                            log.info("[Session: {}] Sending merged audio - {} sentences, WAV size: {} bytes",
                                sessionId, pcmChunks.size(), wavAudio.length);
                            sendAudio(session, wavAudio, aiReply);
                        } else {
                            log.error("[Session: {}] All TTS calls returned empty audio", sessionId);
                            incrementCounter("app.voice.interview.tts.empty_audio", "status", "empty");
                        }
                    }
                }
            } else {
                // ==================== 非流式模式 ====================
                // 等待 LLM 完整输出后一次性调用 TTS
                aiReply = llmService.chat(userText, sessionEntity, conversationHistory);
                recordTimerSinceNanos("app.voice.interview.llm.duration", llmStartNanos, "status", "success");
                incrementCounter("app.voice.interview.llm.calls", "status", "success", "streaming", "false");
                log.info("LLM response for session {}: '{}'", sessionId, aiReply);

                if (!session.isOpen()) {
                    log.warn("WebSocket closed during LLM processing, discarding response for session {}", sessionId);
                    return;
                }

                sendSubtitle(session, userText, true);
                sendTextMessage(session, aiReply);
                saveMessage(sessionId, userText, aiReply);

                long ttsStartNanos = System.nanoTime();
                log.info("[Session: {}] Starting TTS synthesis for text (length: {})",
                    sessionId, aiReply.length());
                byte[] aiAudio = ttsService.synthesize(aiReply);
                recordTimerSinceNanos("app.voice.interview.tts.duration", ttsStartNanos, "status", "success");

                if (!session.isOpen()) {
                    return;
                }

                if (aiAudio == null || aiAudio.length == 0) {
                    log.error("[Session: {}] TTS returned empty audio", sessionId);
                    incrementCounter("app.voice.interview.tts.empty_audio", "status", "empty");
                } else {
                    byte[] wavAudio = convertPcmToWav(aiAudio);
                    sendAudio(session, wavAudio, aiReply);
                }
            }

            // 清空累积文本，准备下一轮对话
            state.setAccumulatedText("");
            // AI 说话结束，设置冷却期后解除封锁
            state.aiSpeaking.set(false);
            state.aiSpeakEndAt.set(System.currentTimeMillis() + AI_SPEAK_COOLDOWN_MS);
            // 记录本轮对话总耗时和完成指标
            recordTimerSinceNanos("app.voice.interview.turn.duration", turnStartNanos, "status", "success");
            incrementCounter("app.voice.interview.turn.completed", "status", "success");

        } catch (Exception e) {
            log.error("Error triggering LLM response for session {}", sessionId, e);
            state.aiSpeaking.set(false);
            recordTimerSinceNanos("app.voice.interview.turn.duration", turnStartNanos, "status", "failure");
            incrementCounter("app.voice.interview.turn.completed", "status", "failure");
            incrementCounter("app.voice.interview.errors", "stage", "turn");
            if (session.isOpen()) {
                sendError(session, "AI响应失败: " + e.getMessage());
            }
        }
    }

    /**
     * Convert exception to user-friendly error message
     */
    private String getErrorMessage(Exception e) {
        Throwable cause = e.getCause();

        // Check for specific Aliyun errors
        if (cause != null) {
            String message = cause.getMessage();
            if (message != null) {
                if (message.contains("403") || message.contains("ACCESS_DENIED")) {
                    return "阿里云语音服务认证失败：AccessKey 无效或已过期。请在 .env 文件中配置正确的 ALIYUN_ACCESS_KEY";
                }
                if (message.contains("timeout") || message.contains("channel inactive")) {
                    return "阿里云语音服务连接超时。请检查网络连接或稍后重试";
                }
            }
        }

        // Default error message
        return "语音处理失败：" + e.getMessage();
    }

    /**
     * Handle control message (end_interview, start_phase)
     */
    private void handleControl(String sessionId, WebSocketControlMessage control) {
        log.info("Control message for session {}: action={}, phase={}",
                sessionId, control.getAction(), control.getPhase());

        switch (control.getAction()) {
            case "submit":
                // 文本提交：前端可能通过 data.text 传入用户输入的文本（非语音识别）
                if (control.getData() != null) {
                    Object textObj = control.getData().get("text");
                    if (textObj instanceof String text && !text.isBlank()) {
                        SessionState state = sessionStates.get(sessionId);
                        if (state != null) {
                            state.appendFinalSttSegment(text);
                        }
                    }
                }
                flushMergedUtteranceToLlm(sessionId);
                break;
            case "end_interview":
                interviewService.endSession(sessionId);
                break;
            case "start_phase":
                interviewService.startPhase(sessionId, control.getPhase());
                break;
        }
    }

/**
 * 发送字幕消息的方法
 * @param session WebSocket会话对象，用于与客户端通信
 * @param text 要发送的字幕文本内容
 * @param isFinal 是否为最终字幕消息的标志
 */
    private void sendSubtitle(WebSocketSession session, String text, boolean isFinal) {
    // 创建WebSocket字幕消息对象，使用Builder模式构建
        WebSocketSubtitleMessage subtitle = WebSocketSubtitleMessage.builder()
                .type("subtitle")  // 设置消息类型为字幕
                .text(text)       // 设置字幕文本内容
                .isFinal(isFinal) // 设置是否为最终消息的标志
                .build();         // 构建消息对象
    // 将消息对象转换为JSON格式并发送
        sendMessage(session, toJson(subtitle));
    }

    private void sendAudio(WebSocketSession session, byte[] audio, String text) {
        if (!session.isOpen()) {
            return;
        }
        String base64Audio = Base64.getEncoder().encodeToString(audio);
        log.info("Sending audio to frontend - WAV size: {} bytes, Base64 length: {}",
                audio.length, base64Audio.length());
        sendMessage(session, toJson(Map.of(
                "type", "audio",
                "data", base64Audio,
                "text", text
        )));
    }

    private void sendTextMessage(WebSocketSession session, String text) {
        sendMessage(session, toJson(Map.of("type", "text", "content", text)));
    }

    private void sendError(WebSocketSession session, String error) {
        sendMessage(session, toJson(Map.of("type", "error", "message", error)));
    }

    private void sendAudioChunk(WebSocketSession session, byte[] wavAudio, int index, boolean isLast) {
        if (!session.isOpen()) {
            return;
        }
        String base64Audio = Base64.getEncoder().encodeToString(wavAudio);
        sendMessage(session, toJson(Map.of(
                "type", "audio_chunk",
                "data", base64Audio,
                "index", index,
                "isLast", isLast
        )));
        log.debug("[Session] Sent audio chunk index={}, isLast={}, size={} bytes", index, isLast, wavAudio.length);
    }

/**
 * 记录从指定时间点开始的时间差，并将其作为计时指标注册到MeterRegistry中
 *
 * @param metricName 指标名称，用于标识这个计时指标
 * @param startNanos 开始时间点，以纳秒为单位
 * @param tags 可变参数，用于标识这个计时指标的标签
 */
    private void recordTimerSinceNanos(String metricName, long startNanos, String... tags) {
    // 获取MeterRegistry实例
        MeterRegistry registry = getRegistry();
    // 如果registry为null，则直接返回，不执行后续操作
        if (registry == null) {
            return;
        }
    // 计算经过的时间（纳秒），确保结果不为负数
        long elapsed = Math.max(0, System.nanoTime() - startNanos);
    // 使用指定的指标名称和标签创建或获取计时器，并记录时间差
        registry.timer(metricName, tags).record(elapsed, TimeUnit.NANOSECONDS);
    }

    private void recordTimerMillis(String metricName, long millis, String... tags) {
        MeterRegistry registry = getRegistry();
        if (registry == null) {
            return;
        }
        registry.timer(metricName, tags).record(Math.max(0, millis), TimeUnit.MILLISECONDS);
    }

    private void incrementCounter(String metricName, String... tags) {
        MeterRegistry registry = getRegistry();
        if (registry == null) {
            return;
        }
        registry.counter(metricName, tags).increment();
    }

    private MeterRegistry getRegistry() {
        return meterRegistryProvider.getIfAvailable();
    }

    /**
     * Scheduled task to check for pause warnings and timeouts
     * Runs every 30 seconds
     * 定时任务：检查暂停警告和超时
     */
    @Scheduled(fixedRate = 30000)
    public void checkPauseTimeout() {
        long now = System.currentTimeMillis();

        lastActivityTime.forEach((sessionId, lastTime) -> {
            long elapsed = now - lastTime;

            // Send warning at 4:30
            if (elapsed > WARNING_TIME_MS && elapsed < PAUSE_TIMEOUT_MS) {
                sendPauseWarning(sessionId);
            }
            // Timeout at 5:00
            else if (elapsed >= PAUSE_TIMEOUT_MS) {
                log.warn("Session {} inactive for {} minutes, pausing",
                    sessionId, PAUSE_TIMEOUT_MS / 60000);
                handlePauseTimeout(sessionId);
            }
        });
    }

    /**
     * Send pause warning notification
     * 发送暂停警告通知
     */
    private void sendPauseWarning(String sessionId) {
        WebSocketSession session = sessions.get(sessionId);
        if (session != null && session.isOpen()) {
            sendMessage(session, toJson(Map.of(
                "type", "control",
                "action", "pause_timeout_warning",
                "message", "会话将在30秒后暂停，请继续说话或点击继续",
                "timestamp", System.currentTimeMillis()
            )));
        }
    }

    /**
     * Handle pause timeout - save state and disconnect
     * 处理暂停超时 - 保存状态并断开连接
     */
    private void handlePauseTimeout(String sessionId) {
        WebSocketSession session = sessions.get(sessionId);

        try {
            if (session != null && session.isOpen()) {
                sendMessage(session, toJson(Map.of(
                    "type", "control",
                    "action", "pause_timeout",
                    "message", "会话因超时已暂停,可在历史记录中恢复",
                    "timestamp", System.currentTimeMillis()
                )));
            }

            // 2. Save session state to database
            interviewService.pauseSession(sessionId, "timeout");

            // 3. Close WebSocket connection
            if (session != null && session.isOpen()) {
                session.close(CloseStatus.GOING_AWAY);
            }

            // 4. Cleanup - Stop ASR session to prevent resource leak
            sttService.stopTranscription(sessionId);
            sessions.remove(sessionId);
            sessionStates.remove(sessionId);
            lastActivityTime.remove(sessionId);

            log.info("Session {} paused due to timeout", sessionId);

        } catch (Exception e) {
            log.error("Error handling pause timeout for session {}", sessionId, e);
        }
    }

    /**
     * Extract session ID from WebSocket URI path
     * Path format: /ws/voice-interview/{sessionId}
     */
    private String extractSessionId(WebSocketSession session) {
        String path = session.getUri().getPath();
        return path.substring(path.lastIndexOf('/') + 1);
    }

    /**
     * Get chat history for session
     * Load conversation history from database
     */
    private List<String> getHistory(String sessionId) {
        try {
            List<VoiceInterviewMessageEntity> messages = interviewService.getConversationHistory(sessionId);
            List<String> history = new ArrayList<>();

            for (VoiceInterviewMessageEntity msg : messages) {
                if (msg.getUserRecognizedText() != null && !msg.getUserRecognizedText().isEmpty()) {
                    history.add("用户：" + msg.getUserRecognizedText());
                }
                if (msg.getAiGeneratedText() != null && !msg.getAiGeneratedText().isEmpty()) {
                    history.add("AI：" + msg.getAiGeneratedText());
                }
            }

            log.debug("Loaded {} messages from history for session {}", history.size(), sessionId);
            return history;
        } catch (Exception e) {
            log.error("Error loading conversation history for session {}", sessionId, e);
            return new ArrayList<>();
        }
    }

    /**
     * Get session entity from database
     */
    private VoiceInterviewSessionEntity getSessionEntity(String sessionId) {
        try {
            Long sessionIdLong = Long.parseLong(sessionId);
            return interviewService.getSession(sessionIdLong);
        } catch (NumberFormatException e) {
            log.error("Invalid session ID format: {}", sessionId);
            return null;
        }
    }

    /**
     * Save message to database
     */
    private void saveMessage(String sessionId, String userText, String aiText) {
        try {
            interviewService.saveMessage(sessionId, userText, aiText);
            log.debug("Message saved to database for session: {}", sessionId);
        } catch (Exception e) {
            log.error("Error saving message for session {}", sessionId, e);
        }
    }

    /**
     * Convert PCM audio to WAV format
     * Adds 44-byte WAV header to PCM data for browser playback
     *
     * @param pcmData Raw PCM audio data (24kHz, 16-bit, mono)
     * @return WAV formatted audio data
     */
    private byte[] convertPcmToWav(byte[] pcmData) {
        // Use 24000Hz for Qwen TTS Realtime API
        int sampleRate = 24000;
        int bitsPerSample = 16;
        int numChannels = 1;
        int byteRate = sampleRate * numChannels * bitsPerSample / 8;
        int blockAlign = numChannels * bitsPerSample / 8;
        int dataSize = pcmData.length;
        int fileSize = dataSize + 36;

        byte[] wavData = new byte[dataSize + 44];

        // Write WAV header directly to avoid stream allocation overhead
        int pos = 0;

        // RIFF header
        wavData[pos++] = 'R'; wavData[pos++] = 'I'; wavData[pos++] = 'F'; wavData[pos++] = 'F';
        writeIntLE(wavData, pos, fileSize); pos += 4;
        wavData[pos++] = 'W'; wavData[pos++] = 'A'; wavData[pos++] = 'V'; wavData[pos++] = 'E';

        // fmt chunk
        wavData[pos++] = 'f'; wavData[pos++] = 'm'; wavData[pos++] = 't'; wavData[pos++] = ' ';
        writeIntLE(wavData, pos, 16); pos += 4; // Chunk size
        writeShortLE(wavData, pos, (short) 1); pos += 2; // Audio format (1 = PCM)
        writeShortLE(wavData, pos, (short) numChannels); pos += 2;
        writeIntLE(wavData, pos, sampleRate); pos += 4;
        writeIntLE(wavData, pos, byteRate); pos += 4;
        writeShortLE(wavData, pos, (short) blockAlign); pos += 2;
        writeShortLE(wavData, pos, (short) bitsPerSample); pos += 2;

        // data chunk
        wavData[pos++] = 'd'; wavData[pos++] = 'a'; wavData[pos++] = 't'; wavData[pos++] = 'a';
        writeIntLE(wavData, pos, dataSize); pos += 4;

        // Copy PCM data
        System.arraycopy(pcmData, 0, wavData, 44, pcmData.length);

        return wavData;
    }

    /**
     * Write 32-bit integer in little-endian format
     */
    private static void writeIntLE(byte[] buf, int pos, int value) {
        buf[pos] = (byte) (value & 0xFF);
        buf[pos + 1] = (byte) ((value >> 8) & 0xFF);
        buf[pos + 2] = (byte) ((value >> 16) & 0xFF);
        buf[pos + 3] = (byte) ((value >> 24) & 0xFF);
    }

    /**
     * Write 16-bit short in little-endian format
     */
    private static void writeShortLE(byte[] buf, int pos, short value) {
        buf[pos] = (byte) (value & 0xFF);
        buf[pos + 1] = (byte) ((value >> 8) & 0xFF);
    }

    /**
     * 会话状态内部类
     * <p>
     * 管理单个 WebSocket 会话的运行时状态，包括：
     * <ul>
     *   <li>用户文本累积（accumulatedText）</li>
     *   <li>处理锁（processing）：防止并发 LLM 请求</li>
     *   <li>AI 说话标记（aiSpeaking）：回声抑制</li>
     *   <li>STT 片段合并缓冲区（mergeBuffer）</li>
     *   <li>虚拟线程引用（processingThread）：断连时可中断</li>
     * </ul>
     * </p>
     * <p>
     * <b>线程安全</b>：所有字段使用 AtomicXXX 或 volatile，支持多线程并发访问
     * </p>
     */
    private static class SessionState {
        /** 累积的用户文本（LLM 输入），每次 flush 后清空 */
        private final AtomicReference<String> accumulatedText = new AtomicReference<>("");
        /** 处理锁：true=正在执行 LLM+TTS 管线，false=空闲 */
        private final AtomicBoolean processing = new AtomicBoolean(false);
        /** AI 正在播放 TTS 音频，期间丢弃麦克风回声 */
        private final AtomicBoolean aiSpeaking = new AtomicBoolean(false);
        /** AI 音频播放结束后，额外等待这段时间再接受用户音频（ms），防止回声尾音 */
        private final AtomicLong aiSpeakEndAt = new AtomicLong(0);
        /** 多段 STT completed 拼接，防抖后再送 LLM */
        private final AtomicReference<String> mergeBuffer = new AtomicReference<>("");
        /** mergeBuffer 开始计时点，用于”最长等待补充”判定 */
        private final AtomicLong mergeStartedAt = new AtomicLong(0);
        /** 最近一次 STT 活动时间（partial/final） */
        private final AtomicLong lastSttActivityAt = new AtomicLong(System.currentTimeMillis());
        /** 当前正在执行 LLM+TTS 管线的虚拟线程，断连时可中断 */
        private volatile Thread processingThread = null;

        /**
         * 追加一个 STT final 片段到 mergeBuffer
         * <p>
         * 如果 buffer 为空，记录开始时间；否则直接拼接。
         * 同时更新 lastSttActivityAt，用于超时检测。
         * </p>
         */
        void appendFinalSttSegment(String segment) {
            String s = segment == null ? "" : segment.trim();
            if (s.isEmpty()) {
                return;
            }
            mergeBuffer.updateAndGet(prev -> {
                if (prev == null || prev.isEmpty()) {
                    mergeStartedAt.set(System.currentTimeMillis());
                    return s;
                }
                return prev + s;
            });
            markSttActivity();
        }

        /** 获取 mergeBuffer 预览（用于实时更新字幕） */
        String getMergeBufferPreview() {
            String s = mergeBuffer.get();
            return s == null ? "" : s;
        }

        /**
         * 取出并清空 mergeBuffer
         * <p>
         * 原子操作：返回当前内容并将 buffer 重置为空字符串。
         * 同时重置 mergeStartedAt 为 0。
         * </p>
         */
        String takeMergeBufferAndClear() {
            mergeStartedAt.set(0);
            return mergeBuffer.getAndSet("");
        }

        /** 标记 STT 活动时间（用于暂停超时检测） */
        void markSttActivity() {
            lastSttActivityAt.set(System.currentTimeMillis());
        }

        /** 获取 mergeBuffer 开始时间，如果未设置则返回当前时间 */
        long getMergeStartedAt() {
            long value = mergeStartedAt.get();
            return value > 0 ? value : System.currentTimeMillis();
        }

        String getAccumulatedText() {
            return accumulatedText.get();
        }

        void setAccumulatedText(String text) {
            accumulatedText.set(text);
        }

        AtomicBoolean isProcessing() {
            return processing;
        }

        void setProcessingThread(Thread t) {
            this.processingThread = t;
        }

        public Thread getProcessingThread() {
            return processingThread;
        }

        /**
         * 判断是否应该丢弃麦克风输入
         * <p>
         * 两种情况：
         * <ol>
         *   <li>AI 正在说话（aiSpeaking=true）</li>
         *   <li>AI 播放结束后的冷却期（默认 800ms），防止扬声器尾音被录入</li>
         * </ol>
         * </p>
         */
        boolean isAiSpeakingOrCooldown() {
            if (aiSpeaking.get()) {
                return true;
            }
            // AI 播放结束后的冷却期（默认 800ms），防止扬声器尾音被录入
            return System.currentTimeMillis() < aiSpeakEndAt.get();
        }
    }
}
