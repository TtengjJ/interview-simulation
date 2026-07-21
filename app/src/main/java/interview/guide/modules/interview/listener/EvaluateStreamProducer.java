package interview.guide.modules.interview.listener;

import interview.guide.common.async.AbstractStreamProducer;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.interview.repository.InterviewSessionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 面试评估任务生产者
 * <p>
 * 负责在文字面试结束后，将评估任务异步发送到 Redis Stream。
 * 通过解耦“面试结束”与“报告生成”两个动作，避免长耗时操作阻塞用户请求。
 * </p>
 *
 * <b>核心职责</b>：
 * <ul>
 *   <li>封装评估任务消息（包含 Session ID 和重试计数）</li>
 *   <li>处理发送失败时的状态回写（标记为 FAILED）</li>
 * </ul>
 */
@Slf4j
@Component
public class EvaluateStreamProducer extends AbstractStreamProducer<String> {

    private final InterviewSessionRepository sessionRepository;

    public EvaluateStreamProducer(RedisService redisService, InterviewSessionRepository sessionRepository) {
        super(redisService);
        this.sessionRepository = sessionRepository;
    }

    /**
     * 发送评估任务到 Redis Stream
     * <p>
     * 调用父类模板方法 {@code sendTask}，底层会执行 Lua 脚本确保原子性写入。
     * </p>
     *
     * @param sessionId 面试会话 ID
     */
    public void sendEvaluateTask(String sessionId) {
        sendTask(sessionId);
    }

    @Override
    protected String taskDisplayName() {
        return "评估";
    }

    @Override
    protected String streamKey() {
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_STREAM_KEY;
    }

    /**
     * 构建发送给消费者的消息体
     * <p>
     * 消息内容极简，仅包含 Session ID 和初始重试次数。
     * 这种设计减少了 Redis 的内存占用和网络传输开销。
     * </p>
     */
    @Override
    protected Map<String, String> buildMessage(String sessionId) {
        return Map.of(
            AsyncTaskStreamConstants.FIELD_SESSION_ID, sessionId,
            AsyncTaskStreamConstants.FIELD_RETRY_COUNT, "0"
        );
    }

    @Override
    protected String payloadIdentifier(String sessionId) {
        return "sessionId=" + sessionId;
    }

    /**
     * 发送失败时的回调处理
     * <p>
     * 如果 Redis 写入失败（如 Redis 宕机），立即更新数据库中的评估状态为 FAILED，
     * 防止前端一直轮询等待一个永远不会开始的任务。
     * </p>
     */
    @Override
    protected void onSendFailed(String sessionId, String error) {
        updateEvaluateStatus(sessionId, AsyncTaskStatus.FAILED, truncateError(error));
    }

    /**
     * 更新数据库中的评估状态
     * <p>
     * 使用 {@code findBySessionId} 确保只更新存在的会话，并截断过长的错误信息以防数据库字段溢出。
     * </p>
     */
    private void updateEvaluateStatus(String sessionId, AsyncTaskStatus status, String error) {
        sessionRepository.findBySessionId(sessionId).ifPresent(session -> {
            session.setEvaluateStatus(status);
            if (error != null) {
                session.setEvaluateError(error.length() > 500 ? error.substring(0, 500) : error);
            }
            sessionRepository.save(session);
        });
    }
}
