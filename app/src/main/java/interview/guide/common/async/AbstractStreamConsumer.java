package interview.guide.common.async;

import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.infrastructure.redis.RedisService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis Stream 消费者模板基类。
 * <p>
 * 将消费循环、ACK、重试与生命周期管理收敛到统一模板，子类仅关注业务处理逻辑。
 */
@Slf4j
public abstract class AbstractStreamConsumer<T> {

    private final RedisService redisService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executorService;
    private String consumerName;

    protected AbstractStreamConsumer(RedisService redisService) {
        this.redisService = redisService;
    }

    @PostConstruct
    public void init() {
        //生成唯一消费者名称
        this.consumerName = consumerPrefix() + UUID.randomUUID().toString().substring(0, 8);

        //创建消费者组
        try {
            redisService.createStreamGroup(streamKey(), groupName());
            log.info("Redis Stream 消费者组已创建或已存在: {}", groupName());
        } catch (Exception e) {
            log.warn("创建消费者组时发生异常（可能已存在）: {}", e.getMessage());
        }

        //Redis Stream 的 Consumer Group 机制保证：同一条消息只会被组内一个消费者处理
        //单线程简化了并发控制，无需考虑线程安全问题
        this.executorService = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            //队列满时，阻塞线程
            new LinkedBlockingQueue<>(),
            r -> {
                //创建消费者线程
                Thread t = new Thread(r, threadName());
                //设置为守护线程
                t.setDaemon(true);
                return t;
            },
                //队列满时，拒绝
            new ThreadPoolExecutor.AbortPolicy()
        );

        running.set(true);
        executorService.submit(this::consumeLoop);
        log.info("{}消费者已启动: consumerName={}", taskDisplayName(), consumerName);
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (executorService != null) {
            executorService.shutdown();
        }
        log.info("{}消费者已关闭: consumerName={}", taskDisplayName(), consumerName);
    }

    //启动消费循环
    private void consumeLoop() {
        while (running.get()) {
            try {
                redisService.streamConsumeMessages(
                    streamKey(),
                    groupName(),
                    consumerName,
                    AsyncTaskStreamConstants.BATCH_SIZE,
                    AsyncTaskStreamConstants.POLL_INTERVAL_MS,
                    this::processMessage
                );
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    log.info("消费者线程被中断");
                    break;
                }
                log.error("消费消息时发生错误: {}", e.getMessage(), e);
            }
        }
    }

    //核心业务编排逻辑，消息处理
    private void processMessage(StreamMessageId messageId, Map<String, String> data) {
        // Step 1: 解析消息载荷
        T payload = parsePayload(messageId, data);
        if (payload == null) {
            ackMessage(messageId);  // ← 无法解析，直接 ACK 丢弃
            return;
        }

        // Step 2: 读取重试次数
        int retryCount = parseRetryCount(data);
        log.info("开始处理{}任务: {}, messageId={}, retryCount={}",
                taskDisplayName(), payloadIdentifier(payload), messageId, retryCount);

        try {
            // Step 3: 标记为处理中
            markProcessing(payload);

            // Step 4: 执行业务逻辑（子类实现）
            processBusiness(payload);

            // Step 5: 标记为完成
            markCompleted(payload);

            // Step 6: ACK 确认消息
            ackMessage(messageId);
            log.info("{}任务完成: {}", taskDisplayName(), payloadIdentifier(payload));

        } catch (Exception e) {
            log.error("{}任务失败: {}, error={}", taskDisplayName(), payloadIdentifier(payload), e.getMessage(), e);

            // Step 7: 失败处理 - 重试或标记失败
            if (retryCount < AsyncTaskStreamConstants.MAX_RETRY_COUNT) {
                // 7a: 未达到最大重试次数 → 重新入队
                retryMessage(payload, retryCount + 1);
            } else {
                // 7b: 超过最大重试次数 → 标记为失败
                markFailed(payload, truncateError(
                        taskDisplayName() + "失败(已重试" + retryCount + "次): " + e.getMessage()
                ));
            }

            // Step 8: 无论重试还是失败，都要 ACK 原消息
            ackMessage(messageId);
        }
    }

    protected int parseRetryCount(Map<String, String> data) {
        try {
            return Integer.parseInt(data.getOrDefault(AsyncTaskStreamConstants.FIELD_RETRY_COUNT, "0"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    protected String truncateError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 500 ? error.substring(0, 500) : error;
    }

    private void ackMessage(StreamMessageId messageId) {
        try {
            redisService.streamAck(streamKey(), groupName(), messageId);
        } catch (Exception e) {
            log.error("确认消息失败: messageId={}, error={}", messageId, e.getMessage(), e);
        }
    }

    protected RedisService redisService() {
        return redisService;
    }

    protected abstract String taskDisplayName();

    protected abstract String streamKey();

    protected abstract String groupName();

    protected abstract String consumerPrefix();

    protected abstract String threadName();

    protected abstract T parsePayload(StreamMessageId messageId, Map<String, String> data);

    protected abstract String payloadIdentifier(T payload);

    protected abstract void markProcessing(T payload);

    protected abstract void processBusiness(T payload);

    protected abstract void markCompleted(T payload);

    protected abstract void markFailed(T payload, String error);

    protected abstract void retryMessage(T payload, int retryCount);
}
