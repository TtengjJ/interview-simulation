package interview.guide.modules.voiceinterview.listener;

import interview.guide.common.async.AbstractStreamProducer;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.voiceinterview.service.VoiceInterviewService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 语音面试评估任务生产者
 * <p>
 * 负责在语音面试结束后，将评估任务异步发送到专用的 Redis Stream。
 * </p>
 *
 * <b>关键设计</b>：
 * <ul>
 *   <li><b>@Lazy 注入</b>：由于 {@code VoiceInterviewService} 可能间接依赖本类，使用懒加载避免循环依赖</li>
 *   <li><b>独立 Stream</b>：使用 {@code VOICE_EVALUATE_STREAM_KEY} 与文字面试评估隔离，互不干扰</li>
 * </ul>
 */
@Slf4j
@Component
public class VoiceEvaluateStreamProducer extends AbstractStreamProducer<String> {

    private final VoiceInterviewService voiceInterviewService;

    public VoiceEvaluateStreamProducer(RedisService redisService,
                                       @Lazy VoiceInterviewService voiceInterviewService) {
        super(redisService);
        this.voiceInterviewService = voiceInterviewService;
    }

    /**
     * 发送评估任务到 Redis Stream
     * <p>
     * 调用父类模板方法，底层通过 Lua 脚本原子性地将消息写入 Stream。
     * </p>
     *
     * @param sessionId 语音面试会话 ID（String 类型）
     */
    public void sendEvaluateTask(String sessionId) {
        sendTask(sessionId);
    }

    @Override
    protected String taskDisplayName() {
        return "语音面试评估";
    }

    @Override
    protected String streamKey() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_STREAM_KEY;
    }

    /**
     * 构建消息体
     * <p>
     * 注意：这里使用的是 {@code FIELD_VOICE_SESSION_ID}，与文字面试的 {@code FIELD_SESSION_ID} 区分开。
     * </p>
     */
    @Override
    protected Map<String, String> buildMessage(String sessionId) {
        return Map.of(
            AsyncTaskStreamConstants.FIELD_VOICE_SESSION_ID, sessionId,
            AsyncTaskStreamConstants.FIELD_RETRY_COUNT, "0"
        );
    }

    @Override
    protected String payloadIdentifier(String sessionId) {
        return "voiceSessionId=" + sessionId;
    }

    /**
     * 发送失败回调
     * <p>
     * 如果 Redis 写入失败，立即更新数据库状态为 FAILED，防止前端无限等待。
     * 注意：由于 Session ID 在消息中是 String 类型，此处需要手动转换为 Long。
     * </p>
     */
    @Override
    protected void onSendFailed(String sessionId, String error) {
        voiceInterviewService.updateEvaluateStatus(
                Long.parseLong(sessionId), AsyncTaskStatus.FAILED, truncateError(error));
    }
}
