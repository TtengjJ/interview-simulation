package interview.guide.modules.voiceinterview.listener;

import interview.guide.common.async.AbstractStreamConsumer;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.voiceinterview.service.VoiceInterviewEvaluationService;
import interview.guide.modules.voiceinterview.service.VoiceInterviewService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 语音面试评估 Stream 消费者
 * <p>
 * 负责从专用的 Redis Stream 消费语音面试评估任务，调用 {@code VoiceInterviewEvaluationService} 生成报告。
 * </p>
 *
 * <b>与文字面试消费者的区别</b>：
 * <ul>
 *   <li><b>极简逻辑</b>：不需要手动组装 Q&A，因为 {@code VoiceInterviewEvaluationService} 内部已经处理了消息到记录的转换</li>
 *   <li><b>类型转换</b>：Redis Stream 中的 ID 是 String，而业务层需要 Long，因此多处存在 {@code Long.parseLong}</li>
 *   <li><b>状态更新委托</b>：所有状态变更都通过 {@code VoiceInterviewService.updateEvaluateStatus} 统一处理</li>
 * </ul>
 */
@Slf4j
@Component
public class VoiceEvaluateStreamConsumer extends AbstractStreamConsumer<VoiceEvaluateStreamConsumer.VoiceEvaluatePayload> {

    private final VoiceInterviewService voiceInterviewService;
    private final VoiceInterviewEvaluationService evaluationService;

    public VoiceEvaluateStreamConsumer(RedisService redisService,
                                       VoiceInterviewService voiceInterviewService,
                                       VoiceInterviewEvaluationService evaluationService) {
        super(redisService);
        this.voiceInterviewService = voiceInterviewService;
        this.evaluationService = evaluationService;
    }

    record VoiceEvaluatePayload(String sessionId) {}

    @Override
    protected String taskDisplayName() {
        return "语音面试评估";
    }

    @Override
    protected String streamKey() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_STREAM_KEY;
    }

    @Override
    protected String groupName() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_GROUP_NAME;
    }

    @Override
    protected String consumerPrefix() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_CONSUMER_PREFIX;
    }

    @Override
    protected String threadName() {
        return "voice-evaluate-consumer";
    }

    /**
     * 解析消息负载
     * <p>
     * 从 Map 中提取 {@code FIELD_VOICE_SESSION_ID}。如果提取失败，返回 null 让父类丢弃该脏数据。
     * </p>
     */
    @Override
    protected VoiceEvaluatePayload parsePayload(StreamMessageId messageId, Map<String, String> data) {
        String sessionId = data.get(AsyncTaskStreamConstants.FIELD_VOICE_SESSION_ID);
        if (sessionId == null) {
            log.warn("消息格式错误，跳过: messageId={}", messageId);
            return null;
        }
        return new VoiceEvaluatePayload(sessionId);
    }

    @Override
    protected String payloadIdentifier(VoiceEvaluatePayload payload) {
        return "voiceSessionId=" + payload.sessionId();
    }

    /**
     * 标记任务开始处理
     * <p>
     * 将数据库中的评估状态更新为 PROCESSING。
     * </p>
     */
    @Override
    protected void markProcessing(VoiceEvaluatePayload payload) {
        voiceInterviewService.updateEvaluateStatus(
                Long.parseLong(payload.sessionId()), AsyncTaskStatus.PROCESSING, null);
    }

    /**
     * 执行核心业务：生成语音面试评估
     * <p>
     * 直接委托给 {@code VoiceInterviewEvaluationService.generateEvaluation}。
     * 该方法内部会自动处理：加载会话、提取对话记录、调用 LLM 评估、保存 JSON 结果。
     * </p>
     */
    @Override
    protected void processBusiness(VoiceEvaluatePayload payload) {
        evaluationService.generateEvaluation(Long.parseLong(payload.sessionId()));
        log.info("语音面试评估完成: sessionId={}", payload.sessionId());
    }

    @Override
    protected void markCompleted(VoiceEvaluatePayload payload) {
        voiceInterviewService.updateEvaluateStatus(
                Long.parseLong(payload.sessionId()), AsyncTaskStatus.COMPLETED, null);
    }

    @Override
    protected void markFailed(VoiceEvaluatePayload payload, String error) {
        voiceInterviewService.updateEvaluateStatus(
                Long.parseLong(payload.sessionId()), AsyncTaskStatus.FAILED, error);
    }

    /**
     * 重试入队逻辑
     * <p>
     * 当评估失败时，将任务重新写入 Stream 尾部。注意这里使用的是语音专用的常量。
     * </p>
     */
    @Override
    protected void retryMessage(VoiceEvaluatePayload payload, int retryCount) {
        String sessionId = payload.sessionId();
        try {
            Map<String, String> message = Map.of(
                AsyncTaskStreamConstants.FIELD_VOICE_SESSION_ID, sessionId,
                AsyncTaskStreamConstants.FIELD_RETRY_COUNT, String.valueOf(retryCount)
            );

            redisService().streamAdd(
                AsyncTaskStreamConstants.VOICE_EVALUATE_STREAM_KEY,
                message,
                AsyncTaskStreamConstants.STREAM_MAX_LEN
            );
            log.info("语音面试评估任务已重新入队: sessionId={}, retryCount={}", sessionId, retryCount);
        } catch (Exception e) {
            log.error("重试入队失败: sessionId={}, error={}", sessionId, e.getMessage(), e);
            voiceInterviewService.updateEvaluateStatus(
                    Long.parseLong(sessionId), AsyncTaskStatus.FAILED, truncateError("重试入队失败: " + e.getMessage()));
        }
    }
}
