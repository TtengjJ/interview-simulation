package interview.guide.modules.interview.listener;

import interview.guide.common.async.AbstractStreamConsumer;
import interview.guide.common.ai.LlmProviderRegistry;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.interview.model.InterviewAnswerEntity;
import interview.guide.modules.interview.model.InterviewQuestionDTO;
import interview.guide.modules.interview.model.InterviewReportDTO;
import interview.guide.modules.interview.model.InterviewSessionEntity;
import interview.guide.modules.interview.repository.InterviewSessionRepository;
import interview.guide.modules.interview.service.AnswerEvaluationService;
import interview.guide.modules.interview.service.InterviewPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 面试评估 Stream 消费者
 * <p>
 * 负责从 Redis Stream 消费评估任务，调用 LLM 生成结构化报告并持久化。
 * </p>
 *
 * <b>核心职责</b>：
 * <ul>
 *   <li><b>消息解析</b>：从 Stream 中提取 Session ID</li>
 *   <li><b>数据组装</b>：将用户回答回填到题目列表中，构建完整的 Q&A 上下文</li>
 *   <li><b>LLM 评估</b>：调用 {@code AnswerEvaluationService} 执行批量打分与反馈生成</li>
 *   <li><b>状态管理</b>：实时更新数据库中的评估进度（PROCESSING -> COMPLETED/FAILED）</li>
 *   <li><b>失败重试</b>：处理异常时的自动重试入队逻辑</li>
 * </ul>
 */
@Slf4j
@Component
public class EvaluateStreamConsumer extends AbstractStreamConsumer<EvaluateStreamConsumer.EvaluatePayload> {

    private final InterviewSessionRepository sessionRepository;
    private final AnswerEvaluationService evaluationService;
    private final InterviewPersistenceService persistenceService;
    private final ObjectMapper objectMapper;
    private final LlmProviderRegistry llmProviderRegistry;

    public EvaluateStreamConsumer(
        RedisService redisService,
        InterviewSessionRepository sessionRepository,
        AnswerEvaluationService evaluationService,
        InterviewPersistenceService persistenceService,
        ObjectMapper objectMapper,
        LlmProviderRegistry llmProviderRegistry
    ) {
        super(redisService);
        this.sessionRepository = sessionRepository;
        this.evaluationService = evaluationService;
        this.persistenceService = persistenceService;
        this.objectMapper = objectMapper;
        this.llmProviderRegistry = llmProviderRegistry;
    }

    record EvaluatePayload(String sessionId) {}

    @Override
    protected String taskDisplayName() {
        return "评估";
    }

    @Override
    protected String streamKey() {
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_STREAM_KEY;
    }

    @Override
    protected String groupName() {
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_GROUP_NAME;
    }

    @Override
    protected String consumerPrefix() {
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_CONSUMER_PREFIX;
    }

    @Override
    protected String threadName() {
        return "evaluate-consumer";
    }

    /**
     * 解析 Stream 消息体
     * <p>
     * 校验消息格式，如果缺少必要的 Session ID 则返回 null，父类会自动 ACK 丢弃该脏数据。
     * </p>
     */
    @Override
    protected EvaluatePayload parsePayload(StreamMessageId messageId, Map<String, String> data) {
        String sessionId = data.get(AsyncTaskStreamConstants.FIELD_SESSION_ID);
        if (sessionId == null) {
            log.warn("消息格式错误，跳过: messageId={}", messageId);
            return null;
        }
        return new EvaluatePayload(sessionId);
    }

    @Override
    protected String payloadIdentifier(EvaluatePayload payload) {
        return "sessionId=" + payload.sessionId();
    }

    /**
     * 标记任务开始处理
     * <p>
     * 在正式执行耗时的 LLM 调用前，先将状态更新为 PROCESSING，
     * 方便前端感知任务已进入队列并开始执行。
     * </p>
     */
    @Override
    protected void markProcessing(EvaluatePayload payload) {
        updateEvaluateStatus(payload.sessionId(), AsyncTaskStatus.PROCESSING, null);
    }

    /**
     * 执行核心业务逻辑：生成评估报告
     * <p>
     * <b>处理流程</b>：
     * <ol>
     *   <li><b>加载会话</b>：从 DB 获取包含简历信息的完整会话实体</li>
     *   <li><b>反序列化题目</b>：将 JSON 格式的题目列表还原为 Java 对象</li>
     *   <li><b>合并答案</b>：遍历 {@code InterviewAnswerEntity}，将用户的真实回答填入对应的题目对象中</li>
     *   <li><b>LLM 评估</b>：调用 {@code evaluationService.evaluateInterview} 进行多维度打分</li>
     *   <li><b>持久化结果</b>：将生成的 {@code InterviewReportDTO} 存入数据库</li>
     * </ol>
     * </p>
     *
     * <b>防御性编程</b>：
     * 如果会话已被删除（例如用户在评估期间手动清理了数据），则直接返回并 ACK 消息，避免空指针异常。
     */
    @Override
    protected void processBusiness(EvaluatePayload payload) {
        String sessionId = payload.sessionId();
        Optional<InterviewSessionEntity> sessionOpt = sessionRepository.findBySessionIdWithResume(sessionId);
        if (sessionOpt.isEmpty()) {
            log.warn("会话已被删除，跳过评估任务: sessionId={}", sessionId);
            return;
        }

        InterviewSessionEntity session = sessionOpt.get();
        List<InterviewQuestionDTO> questions = objectMapper.readValue(
            session.getQuestionsJson(),
            new TypeReference<>() {}
        );

        // 将用户的回答回填到题目列表中，构建完整的 Q&A 对供 LLM 评估
        List<InterviewAnswerEntity> answers = persistenceService.findAnswersBySessionId(sessionId);
        for (InterviewAnswerEntity answer : answers) {
            int index = answer.getQuestionIndex();
            if (index >= 0 && index < questions.size()) {
                InterviewQuestionDTO question = questions.get(index);
                questions.set(index, question.withAnswer(answer.getUserAnswer()));
            }
        }

        // 获取 LLM 客户端并执行评估
        String provider = session.getLlmProvider();
        ChatClient chatClient = llmProviderRegistry.getChatClientOrDefault(provider);

        String resumeText = session.getResume() != null ? session.getResume().getResumeText() : "";
        InterviewReportDTO report = evaluationService.evaluateInterview(chatClient, sessionId, resumeText, questions);
        persistenceService.saveReport(sessionId, report);
    }

    @Override
    protected void markCompleted(EvaluatePayload payload) {
        updateEvaluateStatus(payload.sessionId(), AsyncTaskStatus.COMPLETED, null);
    }

    @Override
    protected void markFailed(EvaluatePayload payload, String error) {
        updateEvaluateStatus(payload.sessionId(), AsyncTaskStatus.FAILED, error);
    }

    /**
     * 消息重试入队逻辑
     * <p>
     * 当评估过程中发生非致命异常时，将消息重新写入 Stream 尾部。
     * 每次重试都会增加 {@code retryCount}，父类会限制最大重试次数（通常为 3 次）。
     * </p>
     */
    @Override
    protected void retryMessage(EvaluatePayload payload, int retryCount) {
        String sessionId = payload.sessionId();
        try {
            Map<String, String> message = Map.of(
                AsyncTaskStreamConstants.FIELD_SESSION_ID, sessionId,
                AsyncTaskStreamConstants.FIELD_RETRY_COUNT, String.valueOf(retryCount)
            );

            redisService().streamAdd(
                AsyncTaskStreamConstants.INTERVIEW_EVALUATE_STREAM_KEY,
                message,
                AsyncTaskStreamConstants.STREAM_MAX_LEN
            );
            log.info("评估任务已重新入队: sessionId={}, retryCount={}", sessionId, retryCount);

        } catch (Exception e) {
            log.error("重试入队失败: sessionId={}, error={}", sessionId, e.getMessage(), e);
            updateEvaluateStatus(sessionId, AsyncTaskStatus.FAILED, truncateError("重试入队失败: " + e.getMessage()));
        }
    }

    /**
     * 交卷到当前时刻的毫秒数。没有交卷时间时返回 null。
     */
    private Long elapsedSinceSubmit(InterviewSessionEntity session) {
        LocalDateTime submittedAt = session.getEvaluateSubmittedAt();
        if (submittedAt == null) {
            return null;
        }
        return Math.max(0, Duration.between(submittedAt, LocalDateTime.now()).toMillis());
    }

    /**
     * 更新评估状态
     */
    private void updateEvaluateStatus(String sessionId, AsyncTaskStatus status, String error) {
        try {
            sessionRepository.findBySessionId(sessionId).ifPresent(session -> {
                session.setEvaluateStatus(status);
                session.setEvaluateError(error);
                if (status == AsyncTaskStatus.COMPLETED) {
                    Long durationMs = elapsedSinceSubmit(session);
                    if (durationMs != null) {
                        session.setEvaluateDurationMs(durationMs);
                        log.info("评估耗时: sessionId={}, evaluateDurationMs={}", sessionId, durationMs);
                    }
                }
                sessionRepository.save(session);
                log.debug("评估状态已更新: sessionId={}, status={}, evaluateDurationMs={}",
                    sessionId, status, session.getEvaluateDurationMs());
            });
        } catch (Exception e) {
            log.error("更新评估状态失败: sessionId={}, status={}, error={}", sessionId, status, e.getMessage(), e);
        }
    }

}
