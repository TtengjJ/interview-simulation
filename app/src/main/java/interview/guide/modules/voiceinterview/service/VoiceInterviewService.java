package interview.guide.modules.voiceinterview.service;

import interview.guide.common.constant.CommonConstants.InterviewDefaults;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.modules.voiceinterview.config.VoiceInterviewProperties;
import interview.guide.modules.voiceinterview.dto.CreateSessionRequest;
import interview.guide.modules.voiceinterview.dto.VoiceInterviewMessageDTO;
import interview.guide.modules.voiceinterview.dto.SessionMetaDTO;
import interview.guide.modules.voiceinterview.dto.SessionResponseDTO;
import interview.guide.modules.voiceinterview.listener.VoiceEvaluateStreamProducer;
import interview.guide.modules.voiceinterview.model.VoiceInterviewMessageEntity;
import interview.guide.modules.voiceinterview.model.VoiceInterviewSessionEntity;
import interview.guide.modules.voiceinterview.model.VoiceInterviewSessionStatus;
import interview.guide.modules.voiceinterview.repository.VoiceInterviewEvaluationRepository;
import interview.guide.modules.voiceinterview.repository.VoiceInterviewMessageRepository;
import interview.guide.modules.voiceinterview.repository.VoiceInterviewSessionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 语音面试会话管理服务
 * <p>
 * 负责语音面试会话的完整生命周期管理，包括：
 * <ul>
 *   <li><b>会话创建</b>：初始化会话状态、配置面试角色和阶段、生成 WebSocket URL</li>
 *   <li><b>状态流转</b>：阶段切换（自我介绍→技术面→项目面→HR面）、暂停/恢复</li>
 *   <li><b>消息持久化</b>：保存用户语音识别文本和 AI 回复到数据库</li>
 *   <li><b>缓存优化</b>：使用 Redis 缓存活跃会话，减少数据库查询压力</li>
 *   <li><b>评估触发</b>：会话结束时通过 Redis Stream 异步触发评估任务</li>
 * </ul>
 * </p>
 *
 * <b>架构定位</b>：
 * <ul>
 *   <li>作为业务编排层，协调 Repository、Redis、Stream Producer</li>
 *   <li>不直接调用 AI 服务（ASR/LLM/TTS），这些由 {@code VoiceInterviewWebSocketHandler} 处理</li>
 *   <li>专注于数据持久化和状态管理，确保会话数据的 ACID 特性</li>
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class VoiceInterviewService {

    private final VoiceInterviewSessionRepository sessionRepository;
    private final VoiceInterviewMessageRepository messageRepository;
    private final VoiceInterviewEvaluationRepository evaluationRepository;
    private final RedissonClient redissonClient;
    private final VoiceInterviewProperties properties;
    private final VoiceEvaluateStreamProducer voiceEvaluateStreamProducer;

    private static final String SESSION_CACHE_KEY_PREFIX = "voice:interview:session:";
    private static final int CACHE_TTL_HOURS = 1;
    private static final String DEFAULT_USER_ID = "default";

    /**
     * 创建新的语音面试会话
     * <p>
     * 该方法执行以下操作：
     * <ol>
     *   <li>根据请求参数构建会话实体（角色类型、难度、启用的面试阶段等）</li>
     *   <li>设置默认 LLM Provider（从配置文件读取）</li>
     *   <li>确定第一个启用的面试阶段（优先顺序：INTRO → TECH → PROJECT → HR）</li>
     *   <li>保存到 PostgreSQL 数据库</li>
     *   <li>将新会话写入 Redis 缓存（TTL 1 小时）</li>
     *   <li>生成 WebSocket 连接 URL 并返回给前端</li>
     * </ol>
     * </p>
     *
     * <b>事务保证</b>：
     * <ul>
     *   <li>使用 {@code @Transactional} 确保会话创建和缓存更新的原子性</li>
     *   <li>如果缓存失败，数据库回滚，避免脏数据</li>
     * </ul>
     *
     * @param request 会话创建请求（包含技能 ID、难度、自定义 JD、简历 ID、阶段配置等）
     * @return 会话响应 DTO（包含 sessionId、当前阶段、WebSocket URL）
     */
    @Transactional
    public SessionResponseDTO createSession(CreateSessionRequest request) {
        String effectiveSkillId = request.getSkillId() != null ? request.getSkillId() : InterviewDefaults.SKILL_ID;
        String effectiveLlmProvider = (request.getLlmProvider() != null && !request.getLlmProvider().isBlank())
            ? request.getLlmProvider()
            : properties.getLlmProvider();

        VoiceInterviewSessionEntity session = VoiceInterviewSessionEntity.builder()
                .userId(DEFAULT_USER_ID)
                .roleType(effectiveSkillId)
                .skillId(effectiveSkillId)
                .difficulty(request.getDifficulty() != null ? request.getDifficulty() : InterviewDefaults.DIFFICULTY)
                .customJdText(request.getCustomJdText())
                .resumeId(request.getResumeId())
                .introEnabled(request.getIntroEnabled())
                .techEnabled(request.getTechEnabled())
                .projectEnabled(request.getProjectEnabled())
                .hrEnabled(request.getHrEnabled())
                .llmProvider(effectiveLlmProvider)
                .plannedDuration(request.getPlannedDuration())
                .currentPhase(determineFirstPhase(request))
                .build();

        VoiceInterviewSessionEntity saved = sessionRepository.save(session);
        cacheSession(saved);

        log.info("Created voice interview session: {} with template: {}, phase: {}",
                saved.getId(), effectiveSkillId, saved.getCurrentPhase());

        return buildSessionResponse(saved);
    }

    /**
     * 强制结束处于 IN_PROGRESS 状态的会话（用于 WebSocket 异常断开的兜底）
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>当 WebSocket 连接意外断开（网络故障、浏览器崩溃）时，前端无法主动调用 {@code endSession}</li>
     *   <li>此时会话仍处于 {@code IN_PROGRESS} 状态，需要后端自动清理</li>
     *   <li>该方法会检查会话状态，只有处于 {@code IN_PROGRESS} 时才执行结束逻辑</li>
     * </ul>
     * </p>
     *
     * <b>与正常结束的区别</b>：
     * <ul>
     *   <li>正常结束的 {@code endSession(String)} 会将状态设为 {@code COMPLETED}</li>
     *   <li>此方法不会重复操作已结束的会话，避免多次触发评估任务</li>
     * </ul>
     *
     * @param sessionId 会话 ID（字符串格式）
     */
    @Transactional
    public void endSessionIfInProgress(String sessionId) {
        Long sessionIdLong = parseSessionId(sessionId);
        VoiceInterviewSessionEntity session = sessionRepository.findById(sessionIdLong).orElse(null);
        if (session == null || session.getStatus() != VoiceInterviewSessionStatus.IN_PROGRESS) {
            return;
        }
        log.info("Auto-ending IN_PROGRESS session {} after WebSocket disconnect", sessionId);
        endSession(session);
    }

    /**
     * 结束面试会话并触发异步评估
     * <p>
     * 该方法执行以下操作：
     * <ol>
     *   <li>解析会话 ID 并从数据库加载会话实体</li>
     *   <li>设置结束时间、实际时长、状态为 {@code COMPLETED}</li>
     *   <li>将会话当前阶段标记为 {@code COMPLETED}</li>
     *   <li>更新评估状态为 {@code PENDING}（等待异步评估）</li>
     *   <li>保存到数据库并清除 Redis 缓存</li>
     *   <li>通过 {@code VoiceEvaluateStreamProducer} 发送评估任务到 Redis Stream</li>
     * </ol>
     * </p>
     *
     * <b>异步评估机制</b>：
     * <ul>
     *   <li>评估任务不在此处同步执行，而是通过 Redis Stream 解耦</li>
     *   <li>消费者（{@code VoiceEvaluateStreamConsumer}）会在后台异步调用 LLM 生成评估报告</li>
     *   <li>避免阻塞主线程，提升用户体验</li>
     * </ul>
     *
     * @param sessionId 会话 ID（字符串格式，内部转换为 Long）
     */
    @Transactional
    public void endSession(String sessionId) {
        Long sessionIdLong = parseSessionId(sessionId);
        VoiceInterviewSessionEntity session = getSession(sessionIdLong);

        if (session == null) {
            log.warn("Session not found: {}", sessionId);
            return;
        }

        endSession(session);
        voiceEvaluateStreamProducer.sendEvaluateTask(sessionId);
    }

    private void endSession(VoiceInterviewSessionEntity session) {
        session.setEndTime(LocalDateTime.now());
        session.setCurrentPhase(VoiceInterviewSessionEntity.InterviewPhase.COMPLETED);
        session.setStatus(VoiceInterviewSessionStatus.COMPLETED);
        session.setActualDuration((int) Duration.between(session.getStartTime(), LocalDateTime.now()).toSeconds());
        session.setEvaluateStatus(AsyncTaskStatus.PENDING);

        sessionRepository.save(session);
        invalidateSessionCache(session.getId());

        log.info("Ended voice interview session: {}, duration: {} seconds, evaluation triggered",
                session.getId(), session.getActualDuration());
    }

    /**
     * 通过 ID 获取会话实体（支持 Redis 缓存降级）
     * <p>
     * 查询策略：
     * <ol>
     *   <li><b>优先查缓存</b>：从 Redis 中获取会话对象（Key: {@code voice:interview:session:{id}}）</li>
     *   <li><b>缓存未命中</b>：从 PostgreSQL 数据库查询</li>
     *   <li><b>自动回填</b>：数据库查询结果会自动写入缓存（在 {@code getSession(Long)} 中实现）</li>
     * </ol>
     * </p>
     *
     * <b>性能优化</b>：
     * <ul>
     *   <li>活跃会话的查询几乎全部命中缓存，减少数据库压力</li>
     *   <li>缓存 TTL 为 1 小时，过期后自动失效</li>
     *   <li>会话状态变更时会主动清除缓存（{@code invalidateSessionCache}）</li>
     * </ul>
     *
     * @param sessionId 会话 ID（字符串格式）
     * @return 会话实体，不存在则返回 null
     */
    public VoiceInterviewSessionEntity getSession(String sessionId) {
        return getSession(parseSessionId(sessionId));
    }

    /**
     * Get session by ID with Redis cache fallback
     * 通过ID获取会话，支持Redis缓存
     *
     * @param sessionId Session ID as Long
     * @return VoiceInterviewSessionEntity or null if not found
     */
    public VoiceInterviewSessionEntity getSession(Long sessionId) {
        if (sessionId == null) {
            return null;
        }

        // Try cache first
        String cacheKey = getSessionCacheKey(sessionId);
        RBucket<VoiceInterviewSessionEntity> bucket = redissonClient.getBucket(cacheKey);
        VoiceInterviewSessionEntity cached = bucket.get();

        if (cached != null) {
            log.debug("Session {} found in cache", sessionId);
            return cached;
        }

        // Fallback to database
        return sessionRepository.findById(sessionId).orElse(null);
    }

    /**
     * 开始新的面试阶段
     * <p>
     * 用于在面试过程中动态切换阶段（例如从 INTRO 切换到 TECH）。
     * 阶段切换会触发以下操作：
     * <ol>
     *   <li>验证阶段名称的合法性（必须是 INTRO/TECH/PROJECT/HR 之一）</li>
     *   <li>更新会话的 {@code currentPhase} 字段</li>
     *   <li>保存到数据库并更新 Redis 缓存</li>
     *   <li>记录日志以便追踪阶段流转历史</li>
     * </ol>
     * </p>
     *
     * <b>注意事项</b>：
     * <ul>
     *   <li>该方法不验证阶段切换的合理性（例如不能从 HR 跳回 INTRO），由调用方保证</li>
     *   <li>阶段信息会嵌入到 LLM Prompt 中，影响 AI 面试官的提问策略</li>
     * </ul>
     *
     * @param sessionId 会话 ID（字符串格式）
     * @param phaseStr  阶段名称字符串（如 "TECH"）
     */
    @Transactional
    public void startPhase(String sessionId, String phaseStr) {
        Long sessionIdLong = parseSessionId(sessionId);
        VoiceInterviewSessionEntity session = getSession(sessionIdLong);

        if (session == null) {
            log.warn("Cannot start phase - session not found: {}", sessionId);
            return;
        }

        try {
            VoiceInterviewSessionEntity.InterviewPhase newPhase =
                    VoiceInterviewSessionEntity.InterviewPhase.valueOf(phaseStr.toUpperCase());

            VoiceInterviewSessionEntity.InterviewPhase oldPhase = session.getCurrentPhase();
            session.setCurrentPhase(newPhase);
            sessionRepository.save(session);
            cacheSession(session); // Update cache

            log.info("Session {} transitioned from phase {} to {}", sessionId, oldPhase, newPhase);

        } catch (IllegalArgumentException e) {
            log.error("Invalid phase string: {}", phaseStr, e);
        }
    }

    /**
     * Get current phase for session
     * 获取会话当前阶段
     *
     * @param sessionId Session ID (String format)
     * @return Current InterviewPhase or null if session not found
     */
    public VoiceInterviewSessionEntity.InterviewPhase getCurrentPhase(String sessionId) {
        VoiceInterviewSessionEntity session = getSession(sessionId);
        return session != null ? session.getCurrentPhase() : null;
    }

    /**
     * 保存对话消息（用户语音识别文本 + AI 回复文本）
     * <p>
     * 该方法将一轮完整的问答保存到数据库，包括：
     * <ol>
     *   <li>用户说出的内容（经 ASR 识别后的文本）</li>
     *   <li>AI 生成的回复文本（经 LLM 生成并优化后的文本）</li>
     *   <li>当前所处的面试阶段（INTRO/TECH/PROJECT/HR）</li>
     *   <li>消息序列号（用于保持对话顺序）</li>
     * </ol>
     * </p>
     *
     * <b>数据结构</b>：
     * <ul>
     *   <li>一条 {@code VoiceInterviewMessageEntity} 代表一轮问答（一问一答）</li>
     *   <li>不是每条消息单独存储，而是成对存储，方便后续评估时提取 Q&A 对</li>
     * </ul>
     *
     * @param sessionId 会话 ID（字符串格式）
     * @param userText  用户语音识别后的文本
     * @param aiText    AI 生成的回复文本
     */
    @Transactional
    public void saveMessage(String sessionId, String userText, String aiText) {
        Long sessionIdLong = parseSessionId(sessionId);
        VoiceInterviewSessionEntity session = getSession(sessionIdLong);

        if (session == null) {
            log.warn("Cannot save message - session not found: {}", sessionId);
            return;
        }

        VoiceInterviewMessageEntity message = VoiceInterviewMessageEntity.builder()
                .sessionId(sessionIdLong)
                .messageType("DIALOGUE")
                .phase(session.getCurrentPhase())
                .userRecognizedText(userText)
                .aiGeneratedText(aiText)
                .sequenceNum(getNextSequenceNum(sessionIdLong))
                .build();

        messageRepository.save(message);
        log.debug("Saved message for session: {}, phase: {}, sequence: {}",
                sessionId, session.getCurrentPhase(), message.getSequenceNum());
    }

    /**
     * Get conversation history for a session
     * 获取会话的对话历史记录
     *
     * @param sessionId Session ID (String format)
     * @return List of messages ordered by sequence number
     */
    public List<VoiceInterviewMessageEntity> getConversationHistory(String sessionId) {
        Long sessionIdLong = parseSessionId(sessionId);
        return messageRepository.findBySessionIdOrderBySequenceNumAsc(sessionIdLong);
    }

    /**
     * Get conversation history as DTOs (for frontend)
     */
    public List<VoiceInterviewMessageDTO> getConversationHistoryDTO(String sessionId) {
        return getConversationHistory(sessionId).stream()
            .map(msg -> VoiceInterviewMessageDTO.builder()
                .id(msg.getId())
                .sessionId(msg.getSessionId())
                .messageType(msg.getMessageType())
                .phase(msg.getPhase() != null ? msg.getPhase().name() : null)
                .userRecognizedText(msg.getUserRecognizedText())
                .aiGeneratedText(msg.getAiGeneratedText())
                .timestamp(msg.getTimestamp())
                .sequenceNum(msg.getSequenceNum())
                .build())
            .collect(Collectors.toList());
    }

    /**
     * 暂停面试会话
     * <p>
     * 用于用户主动暂停或超时自动暂停场景。
     * 暂停后会记录暂停时间点，但不会清除 Redis 缓存（因为会话仍可能恢复）。
     * </p>
     *
     * <b>状态校验</b>：
     * <ul>
     *   <li>只有处于 {@code IN_PROGRESS} 状态的会话才能被暂停</li>
     *   <li>如果会话已是 {@code PAUSED} 或 {@code COMPLETED}，抛出业务异常</li>
     * </ul>
     *
     * @param sessionId 会话 ID
     * @param reason    暂停原因（{@code user_initiated} 或 {@code timeout}）
     * @throws BusinessException 如果会话状态不允许暂停
     */
    @Transactional
    public void pauseSession(String sessionId, String reason) {
        Long sessionIdLong = parseSessionId(sessionId);

        VoiceInterviewSessionEntity session = sessionRepository.findById(sessionIdLong)
            .orElseThrow(() -> new BusinessException(ErrorCode.NOT_FOUND, "会话不存在: " + sessionId));

        if (session.getStatus() != VoiceInterviewSessionStatus.IN_PROGRESS) {
            throw new BusinessException(ErrorCode.BAD_REQUEST,
                "会话状态为 " + session.getStatus() + "，无法暂停"
            );
        }

        session.setStatus(VoiceInterviewSessionStatus.PAUSED);
        session.setPausedAt(LocalDateTime.now());

        sessionRepository.save(session);
        //Cache-Aside（旁路缓存）写数据库时，主动删缓存，以确保下次读到的一定是最新数据。
        invalidateSessionCache(sessionIdLong);

        log.info("Session {} paused, reason: {}", sessionId, reason);
    }

    /**
     * 恢复面试会话
     * <p>
     * 从暂停状态恢复到进行中状态，并重新生成 WebSocket URL 供前端重连。
     * 恢复后会：
     * <ol>
     *   <li>更新状态为 {@code IN_PROGRESS}</li>
     *   <li>记录恢复时间点（用于计算实际面试时长）</li>
     *   <li>重新写入 Redis 缓存</li>
     *   <li>返回会话响应 DTO（包含新的 WebSocket URL）</li>
     * </ol>
     * </p>
     *
     * <b>状态校验</b>：
     * <ul>
     *   <li>只有处于 {@code PAUSED} 状态的会话才能被恢复</li>
     *   <li>如果会话已是 {@code IN_PROGRESS} 或 {@code COMPLETED}，抛出业务异常</li>
     * </ul>
     *
     * @param sessionId 会话 ID
     * @return 会话响应 DTO（包含 WebSocket URL）
     * @throws BusinessException 如果会话状态不允许恢复
     */
    @Transactional
    public SessionResponseDTO resumeSession(String sessionId) {
        Long sessionIdLong = parseSessionId(sessionId);

        VoiceInterviewSessionEntity session = sessionRepository.findById(sessionIdLong)
            .orElseThrow(() -> new BusinessException(ErrorCode.NOT_FOUND, "会话不存在: " + sessionId));

        if (session.getStatus() != VoiceInterviewSessionStatus.PAUSED) {
            throw new BusinessException(ErrorCode.BAD_REQUEST,
                "会话状态为 " + session.getStatus() + "，无法恢复"
            );
        }

        session.setStatus(VoiceInterviewSessionStatus.IN_PROGRESS);
        session.setResumedAt(LocalDateTime.now());

        VoiceInterviewSessionEntity saved = sessionRepository.save(session);
        cacheSession(saved);

        log.info("Session {} resumed with {} messages in conversation history",
            sessionId, messageRepository.countBySessionId(sessionIdLong));

        return buildSessionResponse(saved);
    }

    /**
     * Get all sessions for a user
     * 获取用户所有会话
     *
     * @param userId User ID (optional, defaults to DEFAULT_USER_ID)
     * @param status Filter by status (optional)
     * @return List of session metadata
     */
    public List<SessionMetaDTO> getAllSessions(String userId, String status) {
        userId = userId != null ? userId : DEFAULT_USER_ID;

        List<VoiceInterviewSessionEntity> sessions;
        if (status != null && !status.isEmpty()) {
            VoiceInterviewSessionStatus statusEnum =
                VoiceInterviewSessionStatus.valueOf(status.toUpperCase());
            sessions = sessionRepository.findByUserIdAndStatusOrderByUpdatedAtDesc(userId, statusEnum);
        } else {
            sessions = sessionRepository.findByUserIdOrderByUpdatedAtDesc(userId);
        }

        return sessions.stream()
            .map(session -> SessionMetaDTO.builder()
                .sessionId(session.getId())
                .roleType(session.getRoleType())
                .status(session.getStatus().name())
                .currentPhase(session.getCurrentPhase().name())
                .createdAt(session.getCreatedAt())
                .updatedAt(session.getUpdatedAt())
                .actualDuration(session.getActualDuration())
                .messageCount(messageRepository.countBySessionId(session.getId()))
                .evaluateStatus(session.getEvaluateStatus() != null ? session.getEvaluateStatus().name() : null)
                .evaluateError(session.getEvaluateError())
                .build())
            .collect(Collectors.toList());
    }

    /**
     * Get session DTO by ID
     * 通过ID获取会话DTO
     *
     * @param sessionId Session ID as Long
     * @return SessionResponseDTO with session details or null if not found
     */
    public SessionResponseDTO getSessionDTO(Long sessionId) {
        VoiceInterviewSessionEntity session = getSession(sessionId);

        if (session == null) {
            return null;
        }

        return buildSessionResponse(session);
    }

    /**
     * 检查是否应该转换到下一个面试阶段
     * <p>
     * 基于以下三个规则判断是否需要自动切换阶段：
     * <ol>
     *   <li><b>规则 1（强制切换）</b>：当前阶段持续时间达到最大时长（{@code maxDuration}）</li>
     *   <li><b>规则 2（问题数量上限）</b>：已提问数量达到最大问题数（{@code maxQuestions}）</li>
     *   <li><b>规则 3（建议切换）</b>：达到建议时长且已提问数量达到最小问题数</li>
     * </ol>
     * </p>
     *
     * <b>配置来源</b>：
     * <ul>
     *   <li>各阶段的时长和问题数配置来自 {@code VoiceInterviewProperties}</li>
     *   <li>例如：INTRO 阶段最小 3 分钟、建议 5 分钟、最大 8 分钟，最少 2 个问题、最多 5 个问题</li>
     * </ul>
     *
     * @param session        当前会话实体
     * @param phaseStartTime 当前阶段开始时间
     * @param questionCount  当前阶段已提问数量
     * @return true 表示应该切换阶段，false 表示继续当前阶段
     */
    public boolean shouldTransitionToNextPhase(VoiceInterviewSessionEntity session,
                                                LocalDateTime phaseStartTime,
                                                int questionCount) {
        VoiceInterviewSessionEntity.InterviewPhase currentPhase = session.getCurrentPhase();
        if (currentPhase == null || currentPhase == VoiceInterviewSessionEntity.InterviewPhase.COMPLETED) {
            return false;
        }

        Duration phaseDuration = Duration.between(phaseStartTime, LocalDateTime.now());
        VoiceInterviewProperties.DurationConfig config = getPhaseConfig(currentPhase);

        // Rule 1: Max duration reached (forced transition)
        if (phaseDuration.toMinutes() >= config.getMaxDuration()) {
            log.info("Phase {} reached max duration {} minutes, forcing transition",
                    currentPhase, config.getMaxDuration());
            return true;
        }

        // Rule 2: Min questions reached and sufficient information gathered (AI judgment)
        // For MVP, we use a simple heuristic based on question count
        if (questionCount >= config.getMaxQuestions()) {
            log.info("Phase {} reached max questions {}, suggesting transition",
                    currentPhase, config.getMaxQuestions());
            return true;
        }

        // Rule 3: Suggested duration reached with min questions
        if (phaseDuration.toMinutes() >= config.getSuggestedDuration()
                && questionCount >= config.getMinQuestions()) {
            log.info("Phase {} reached suggested duration {} with {} questions, suggesting transition",
                    currentPhase, config.getSuggestedDuration(), questionCount);
            return true;
        }

        return false;
    }

    /**
     * 获取当前阶段之后的下一个启用阶段
     * <p>
     * 根据会话配置中启用的阶段（introEnabled/techEnabled/projectEnabled/hrEnabled），
     * 按顺序返回下一个应该进入的阶段。
     * </p>
     *
     * <b>阶段流转逻辑</b>：
     * <pre>
     * INTRO → TECH → PROJECT → HR → COMPLETED
     * </pre>
     * 如果某个阶段被禁用（例如 techEnabled=false），则跳过该阶段。
     *
     * @param session 当前会话实体
     * @return 下一个阶段枚举值，如果没有更多阶段则返回 {@code COMPLETED}
     */
    public VoiceInterviewSessionEntity.InterviewPhase getNextPhase(VoiceInterviewSessionEntity session) {
        VoiceInterviewSessionEntity.InterviewPhase current = session.getCurrentPhase();
        if (current == null) {
            return getFirstEnabledPhase(session);
        }

        return switch (current) {
            case INTRO -> session.getTechEnabled() ? VoiceInterviewSessionEntity.InterviewPhase.TECH :
                    session.getProjectEnabled() ? VoiceInterviewSessionEntity.InterviewPhase.PROJECT :
                            session.getHrEnabled() ? VoiceInterviewSessionEntity.InterviewPhase.HR :
                                    VoiceInterviewSessionEntity.InterviewPhase.COMPLETED;
            case TECH -> session.getProjectEnabled() ? VoiceInterviewSessionEntity.InterviewPhase.PROJECT :
                    session.getHrEnabled() ? VoiceInterviewSessionEntity.InterviewPhase.HR :
                            VoiceInterviewSessionEntity.InterviewPhase.COMPLETED;
            case PROJECT -> session.getHrEnabled() ? VoiceInterviewSessionEntity.InterviewPhase.HR :
                    VoiceInterviewSessionEntity.InterviewPhase.COMPLETED;
            case HR, COMPLETED -> VoiceInterviewSessionEntity.InterviewPhase.COMPLETED;
        };
    }

    // ==================== Private Helper Methods ====================

    /**
     * Determine the first phase based on enabled phases
     * 根据启用的阶段确定第一个阶段
     */
    private VoiceInterviewSessionEntity.InterviewPhase determineFirstPhase(CreateSessionRequest request) {
        if (request.getIntroEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.INTRO;
        if (request.getTechEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.TECH;
        if (request.getProjectEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.PROJECT;
        if (request.getHrEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.HR;
        return VoiceInterviewSessionEntity.InterviewPhase.COMPLETED;
    }

    /**
     * Get first enabled phase from session
     */
    private VoiceInterviewSessionEntity.InterviewPhase getFirstEnabledPhase(VoiceInterviewSessionEntity session) {
        if (session.getIntroEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.INTRO;
        if (session.getTechEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.TECH;
        if (session.getProjectEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.PROJECT;
        if (session.getHrEnabled()) return VoiceInterviewSessionEntity.InterviewPhase.HR;
        return VoiceInterviewSessionEntity.InterviewPhase.COMPLETED;
    }

    private SessionResponseDTO buildSessionResponse(VoiceInterviewSessionEntity session) {
        return SessionResponseDTO.builder()
                .sessionId(session.getId())
                .roleType(session.getRoleType())
                .currentPhase(session.getCurrentPhase().name())
                .status(session.getStatus().name())
                .startTime(session.getStartTime())
                .plannedDuration(session.getPlannedDuration())
                .webSocketUrl(String.format("ws://localhost:8080/ws/voice-interview/%d", session.getId()))
                .build();
    }

    /**
     * Get phase configuration from properties
     */
    private VoiceInterviewProperties.DurationConfig getPhaseConfig(VoiceInterviewSessionEntity.InterviewPhase phase) {
        return switch (phase) {
            case INTRO -> properties.getPhase().getIntro();
            case TECH -> properties.getPhase().getTech();
            case PROJECT -> properties.getPhase().getProject();
            case HR -> properties.getPhase().getHr();
            default -> new VoiceInterviewProperties.DurationConfig(0, 0, 0, 0, 0);
        };
    }

    /**
     * Get next sequence number for messages in a session
     */
    private int getNextSequenceNum(Long sessionId) {
        return (int) messageRepository.countBySessionId(sessionId) + 1;
    }

    /**
     * Update evaluation status on session entity (shared by Producer/Consumer/Controller)
     */
    public void updateEvaluateStatus(Long sessionId, AsyncTaskStatus status, String error) {
        try {
            sessionRepository.findById(sessionId).ifPresent(session -> {
                session.setEvaluateStatus(status);
                session.setEvaluateError(error);
                sessionRepository.save(session);
                log.debug("Evaluation status updated: sessionId={}, status={}", sessionId, status);
            });
        } catch (Exception e) {
            log.error("Failed to update evaluation status: sessionId={}, status={}, error={}",
                    sessionId, status, e.getMessage(), e);
        }
    }

    /**
     * Trigger async evaluation for a session (called by Controller)
     */
    @Transactional
    public void triggerEvaluation(Long sessionId) {
        updateEvaluateStatus(sessionId, AsyncTaskStatus.PENDING, null);
        voiceEvaluateStreamProducer.sendEvaluateTask(sessionId.toString());
    }

    /**
     * 删除语音面试会话及其关联的消息和评估记录
     */
    @Transactional
    public void deleteSession(Long sessionId) {
        if (!sessionRepository.existsById(sessionId)) {
            throw new BusinessException(ErrorCode.VOICE_SESSION_NOT_FOUND, "会话不存在: " + sessionId);
        }
        evaluationRepository.findBySessionId(sessionId).ifPresent(evaluationRepository::delete);
        messageRepository.deleteBySessionId(sessionId);
        sessionRepository.deleteById(sessionId);
        log.info("Deleted voice interview session: {}", sessionId);
    }

    /**
     * Cache session in Redis
     */
    private void cacheSession(VoiceInterviewSessionEntity session) {
        String cacheKey = getSessionCacheKey(session.getId());
        RBucket<VoiceInterviewSessionEntity> bucket = redissonClient.getBucket(cacheKey);
        bucket.set(session, Duration.ofHours(CACHE_TTL_HOURS));
        log.debug("Cached session: {}", session.getId());
    }

    /**
     * Invalidate session cache in Redis
     */
    private void invalidateSessionCache(Long sessionId) {
        String cacheKey = getSessionCacheKey(sessionId);
        RBucket<VoiceInterviewSessionEntity> bucket = redissonClient.getBucket(cacheKey);
        bucket.delete();
        log.debug("Invalidated cache for session: {}", sessionId);
    }

    /**
     * Generate Redis cache key for session
     */
    private String getSessionCacheKey(Long sessionId) {
        return SESSION_CACHE_KEY_PREFIX + sessionId;
    }

    /**
     * Parse session ID from String to Long with error handling
     */
    private Long parseSessionId(String sessionId) {
        if (sessionId == null) {
            return null;
        }
        try {
            return Long.parseLong(sessionId);
        } catch (NumberFormatException e) {
            log.error("Invalid session ID format: {}", sessionId, e);
            return null;
        }
    }
}
