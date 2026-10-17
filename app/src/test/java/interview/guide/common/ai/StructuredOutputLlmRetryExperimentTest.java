package interview.guide.common.ai;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.OpenAiApi;
import org.springframework.ai.retry.RetryUtils;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 真实 LLM：结构化解析失败后，依赖 StructuredOutputInvoker 重试（含 repair prompt）
 * 能否拿到合法 QuestionListDTO JSON。
 *
 * <p>首轮 system 故意诱导 Markdown/解释性输出，提高首次 BeanOutputConverter 失败概率；
 * 次轮由 Invoker 追加严格 JSON 修复提示，调用同一大模型。
 *
 * <p>运行前需存在仓库根目录 {@code .env.dev} 且配置有效 {@code AI_BAILIAN_API_KEY}。
 */
@DisplayName("真实 LLM 结构化输出重试实验（出题）")
class StructuredOutputLlmRetryExperimentTest {

    private static final Logger log = LoggerFactory.getLogger(StructuredOutputLlmRetryExperimentTest.class);
    private static final int N = 5;
    private static final String LOG_CONTEXT = "direction_question_llm";
    private static final String BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode";

    /**
     * 诱导首轮非纯 JSON（模拟线上常见失败形态），便于观察重试挽救。
     */
    private static final String INDUCE_FIRST_FAIL = """

        # 输出约束（必须遵守）
        1) 先用中文写出完整出题思路（不少于 120 字），不要一上来就给 JSON。
        2) 最终结果必须包在 markdown 的 ```json 代码块中。
        3) 代码块前后各再写一句总结，禁止只输出裸 JSON 对象。
        """;

    private static Map<String, String> envDev;
    private static String apiKey;
    private static String model;

    record QuestionListDTO(List<QuestionDTO> questions) {}

    record QuestionDTO(
        String question,
        String type,
        String category,
        String topicSummary,
        List<String> followUps
    ) {}

    private StructuredOutputInvoker invoker;
    private MeterRegistry meterRegistry;
    private BeanOutputConverter<QuestionListDTO> outputConverter;
    private ChatClient chatClient;

    @BeforeAll
    static void loadEnv() {
        envDev = readEnvFile(findEnvDev());
        apiKey = firstNonBlank(
            envDev.get("AI_BAILIAN_API_KEY"),
            System.getenv("AI_BAILIAN_API_KEY")
        );
        model = firstNonBlank(
            envDev.get("AI_MODEL"),
            System.getenv("AI_MODEL"),
            "qwen-plus"
        );
    }

    static boolean hasRealApiKey() {
        if (envDev == null) {
            loadEnv();
        }
        return apiKey != null
            && !apiKey.isBlank()
            && !apiKey.startsWith("your_")
            && !"test-api-key".equals(apiKey);
    }

    @BeforeEach
    void setUp() {
        assumeTrue(hasRealApiKey(), "跳过：未配置有效 AI_BAILIAN_API_KEY（.env.dev）");

        StructuredOutputProperties properties = new StructuredOutputProperties();
        // 生产默认 2（首轮 + 1 次重试）；此处用 3 = 首轮失败后最多再试 2 次
        properties.setStructuredMaxAttempts(3);
        properties.setStructuredIncludeLastError(true);
        properties.setStructuredRetryUseRepairPrompt(true);
        properties.setStructuredRetryAppendStrictJsonInstruction(true);
        properties.setStructuredMetricsEnabled(true);

        meterRegistry = new SimpleMeterRegistry();
        invoker = new StructuredOutputInvoker(properties, meterRegistry);
        outputConverter = new BeanOutputConverter<>(QuestionListDTO.class);
        chatClient = buildPlainChatClient(apiKey, model);
        log.info("LLM 实验客户端就绪: model={}, baseUrl={}", model, BASE_URL);

        // 鉴权失败时不应伪装成「结构化解析失败」去刷满 5 轮
        assumeTrue(probeLlmAuth(chatClient),
            "跳过：DashScope 返回 401，请更新仓库根目录 .env.dev 中的 AI_BAILIAN_API_KEY 后重跑");
    }

    @Test
    @EnabledIf("hasRealApiKey")
    @DisplayName("跑 5 次真实 LLM：统计首轮失败后的重试挽救率")
    void runFiveRealLlmRoundsAndComputeSalvageRate() {
        int firstAttemptFailures = 0;
        int salvageSuccesses = 0;
        int firstAttemptSuccesses = 0;
        int finalFailures = 0;

        String systemPrompt = """
            你是 Java 后端面试出题助手，请按要求生成面试题。
            """ + INDUCE_FIRST_FAIL + "\n\n" + outputConverter.getFormat();

        String userPrompt = """
            请出 2 道中级 Java 后端面试题，覆盖 Spring 与并发。
            每题给出 type、category、topicSummary，以及 0~1 个追问 followUps。
            """;

        for (int i = 1; i <= N; i++) {
            double failBefore = counterValue("app.ai.structured_output.attempts", "failure");
            double successAttemptsBefore = counterValue("app.ai.structured_output.attempts", "success");

            try {
                QuestionListDTO result = invoker.invoke(
                    chatClient,
                    systemPrompt,
                    userPrompt,
                    outputConverter,
                    ErrorCode.INTERVIEW_QUESTION_GENERATION_FAILED,
                    "方向题生成失败：",
                    LOG_CONTEXT,
                    log
                );

                double failAfter = counterValue("app.ai.structured_output.attempts", "failure");
                double successAttemptsAfter = counterValue("app.ai.structured_output.attempts", "success");
                boolean hadFirstFail = failAfter > failBefore;
                boolean hadLaterSuccess = successAttemptsAfter > successAttemptsBefore;

                assertThat(result.questions()).isNotEmpty();

                if (hadFirstFail && hadLaterSuccess) {
                    firstAttemptFailures++;
                    salvageSuccesses++;
                    log.info("[LLM实验] round={}/{} 首轮失败后重试成功: questions={}",
                        i, N, result.questions().size());
                } else {
                    firstAttemptSuccesses++;
                    log.info("[LLM实验] round={}/{} 首轮即成功: questions={}",
                        i, N, result.questions().size());
                }
            } catch (BusinessException e) {
                double failAfter = counterValue("app.ai.structured_output.attempts", "failure");
                if (failAfter > failBefore) {
                    firstAttemptFailures++;
                }
                finalFailures++;
                log.error("[LLM实验] round={}/{} 重试后仍失败: {}", i, N, e.getMessage());
            }
        }

        double salvageRate = firstAttemptFailures == 0
            ? Double.NaN
            : (double) salvageSuccesses / firstAttemptFailures;

        log.info("""
            ========== 真实 LLM 结构化重试实验结果 ==========
            模型: {}
            N={}
            maxAttempts=3（失败后最多再试 2 次）
            首轮即成功={}
            首轮失败次数 F={}
            挽救成功次数 R={}
            最终仍失败={}
            挽救率 R/F = {}
            --- Micrometer ---
            attempts(failure)={}
            attempts(success)={}
            invocations(success)={}
            invocations(failure)={}
            ================================================
            """,
            model,
            N,
            firstAttemptSuccesses,
            firstAttemptFailures,
            salvageSuccesses,
            finalFailures,
            Double.isNaN(salvageRate) ? "N/A（无首轮失败样本）" : String.format("%.1f%%", salvageRate * 100),
            counterValue("app.ai.structured_output.attempts", "failure"),
            counterValue("app.ai.structured_output.attempts", "success"),
            counterValue("app.ai.structured_output.invocations", "success"),
            counterValue("app.ai.structured_output.invocations", "failure")
        );

        // 实验报告型断言：有首轮失败样本时，重试应至少挽救 1 次；否则仅保证未全部失败
        if (firstAttemptFailures > 0) {
            assertThat(salvageSuccesses)
                .as("首轮失败后，repair 重试应至少挽救 1 次（真实 LLM）")
                .isPositive();
        } else {
            log.warn("5 轮均首轮即成功，未形成「失败后重试」样本；诱导首轮失败的 prompt 可能被模型忽略");
            assertThat(firstAttemptSuccesses).isEqualTo(N);
        }
    }

    /** 发一条极短请求，区分 401 鉴权失败与正常模型响应。 */
    private static boolean probeLlmAuth(ChatClient client) {
        try {
            String reply = client.prompt()
                .user("只回复一个字：好")
                .call()
                .content();
            log.info("LLM 鉴权探测成功: replyPreview={}",
                reply == null ? "null" : reply.substring(0, Math.min(40, reply.length())));
            return true;
        } catch (Exception e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            log.error("LLM 鉴权探测失败: {}", msg);
            return !msg.contains("401");
        }
    }

    private static ChatClient buildPlainChatClient(String key, String modelName) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10_000);
        requestFactory.setReadTimeout(300_000);

        OpenAiApi openAiApi = OpenAiApi.builder()
            .baseUrl(BASE_URL)
            .apiKey(key)
            .restClientBuilder(RestClient.builder().requestFactory(requestFactory))
            .build();

        OpenAiChatOptions options = OpenAiChatOptions.builder()
            .model(modelName)
            .temperature(0.7)
            .build();

        OpenAiChatModel chatModel = new OpenAiChatModel(
            openAiApi,
            options,
            ToolCallingManager.builder().build(),
            RetryUtils.DEFAULT_RETRY_TEMPLATE,
            ObservationRegistry.NOOP
        );
        return ChatClient.builder(chatModel).build();
    }

    private double counterValue(String name, String status) {
        var counter = meterRegistry.find(name)
            .tag("status", status)
            .tag("context", LOG_CONTEXT)
            .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static Path findEnvDev() {
        Path cwd = Path.of("").toAbsolutePath();
        Path[] candidates = {
            cwd.resolve(".env.dev"),
            cwd.resolve("../.env.dev"),
            cwd.getParent() != null ? cwd.getParent().resolve(".env.dev") : null
        };
        for (Path p : candidates) {
            if (p != null && Files.isRegularFile(p)) {
                return p;
            }
        }
        // Gradle :app:test 的 user.dir 通常是 app/
        Path fromUserDir = Path.of(System.getProperty("user.dir")).resolve("../.env.dev").normalize();
        if (Files.isRegularFile(fromUserDir)) {
            return fromUserDir;
        }
        return cwd.resolve(".env.dev");
    }

    private static Map<String, String> readEnvFile(Path path) {
        Map<String, String> map = new HashMap<>();
        if (!Files.isRegularFile(path)) {
            return map;
        }
        try {
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                int eq = trimmed.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String k = trimmed.substring(0, eq).trim();
                String v = trimmed.substring(eq + 1).trim();
                if ((v.startsWith("\"") && v.endsWith("\"")) || (v.startsWith("'") && v.endsWith("'"))) {
                    v = v.substring(1, v.length() - 1);
                }
                map.put(k, v);
            }
        } catch (Exception e) {
            log.warn("读取 {} 失败: {}", path, e.getMessage());
        }
        return map;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return null;
    }
}
