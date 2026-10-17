package interview.guide.common.evaluation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import interview.guide.common.ai.StructuredOutputInvoker;
import interview.guide.common.ai.StructuredOutputProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.OpenAiApi;
import org.springframework.ai.retry.RetryUtils;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import io.micrometer.observation.ObservationRegistry;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 对已有文字面试会话走 UnifiedEvaluationService（分批评估 + 二次汇总），统计端到端耗时并回写报告。
 * 运行中的 IDE 进程未加载有效 API Key，交卷链路会在数秒内落到 0 分兜底，因此这里直接用 .env.dev 调同一评估引擎。
 */
@DisplayName("统一评估引擎耗时实验")
class InterviewEvaluationLatencyExperimentTest {

    private static final Logger log = LoggerFactory.getLogger(InterviewEvaluationLatencyExperimentTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String JDBC_URL =
        "jdbc:postgresql://localhost:5432/interview_guide";

    private static final List<long[]> SESSIONS = List.of(
        new long[] {24},
        new long[] {25},
        new long[] {26},
        new long[] {27},
        new long[] {30}
    );

    private static String apiKey;
    private static String chatModel;
    private static String dbPassword;

    @BeforeAll
    static void loadEnv() {
        Map<String, String> env = readEnvFile(findEnvDev());
        apiKey = firstNonBlank(env.get("AI_BAILIAN_API_KEY"), System.getenv("AI_BAILIAN_API_KEY"));
        chatModel = firstNonBlank(env.get("AI_MODEL"), System.getenv("AI_MODEL"), "qwen-plus");
        dbPassword = firstNonBlank(env.get("POSTGRES_PASSWORD"), "123456");
    }

    static boolean hasRealApiKey() {
        if (apiKey == null) {
            loadEnv();
        }
        return apiKey != null && !apiKey.isBlank() && !apiKey.startsWith("your_")
            && !"test-api-key".equals(apiKey);
    }

    @Test
    @EnabledIf("hasRealApiKey")
    @DisplayName("5 条文字面试：分批评估 + 二次汇总耗时")
    void evaluateFiveSessions() throws Exception {
        assumeTrue(hasRealApiKey(), "跳过：未配置有效 AI_BAILIAN_API_KEY");

        StructuredOutputProperties properties = new StructuredOutputProperties();
        properties.setStructuredMaxAttempts(2);
        properties.setStructuredIncludeLastError(true);
        properties.setStructuredRetryUseRepairPrompt(true);
        properties.setStructuredRetryAppendStrictJsonInstruction(true);
        properties.setStructuredMetricsEnabled(false);
        StructuredOutputInvoker invoker = new StructuredOutputInvoker(properties, new SimpleMeterRegistry());
        InterviewEvaluationProperties evaluationProperties = new InterviewEvaluationProperties();
        UnifiedEvaluationService evaluationService = new UnifiedEvaluationService(
            invoker, new DefaultResourceLoader(), evaluationProperties);
        ChatClient chatClient = buildPlainChatClient(apiKey, chatModel);

        List<Double> seconds = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(JDBC_URL, "postgres", dbPassword)) {
            for (long[] idHolder : SESSIONS) {
                long id = idHolder[0];
                SessionData session = loadSession(connection, id);
                long start = System.nanoTime();
                EvaluationReport report = evaluationService.evaluate(
                    chatClient, session.sessionId, session.records, "", null);
                double sec = (System.nanoTime() - start) / 1_000_000_000.0;
                seconds.add(sec);
                saveReport(connection, id, report);
                log.info("评估完成 id={} sessionId={} skill={} questions={} score={} elapsedSec={}",
                    id, session.sessionId, session.skillId, session.records.size(),
                    report.overallScore(), String.format("%.1f", sec));
            }
        }

        seconds.sort(Double::compareTo);
        double sum = seconds.stream().mapToDouble(Double::doubleValue).sum();
        log.info("""
            ========== 统一评估耗时 ==========
            模型: {}  batchSize=8  样本={}
            各次秒数: {}
            min={}s  avg={}s  max={}s
            ==================================
            """,
            chatModel,
            seconds.size(),
            seconds.stream().map(v -> String.format("%.1f", v)).toList(),
            String.format("%.1f", seconds.get(0)),
            String.format("%.1f", sum / seconds.size()),
            String.format("%.1f", seconds.get(seconds.size() - 1))
        );
    }

    private static SessionData loadSession(Connection connection, long id) throws Exception {
        String sessionId;
        String skillId;
        String questionsJson;
        try (PreparedStatement ps = connection.prepareStatement(
            "SELECT session_id, skill_id, questions_json FROM interview_sessions WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("会话不存在: " + id);
                }
                sessionId = rs.getString(1);
                skillId = rs.getString(2);
                questionsJson = rs.getString(3);
            }
        }
        Map<Integer, String> answers = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(
            "SELECT question_index, user_answer FROM interview_answers WHERE session_id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    answers.put(rs.getInt(1), rs.getString(2));
                }
            }
        }
        JsonNode questions = MAPPER.readTree(questionsJson);
        List<QaRecord> records = new ArrayList<>();
        for (JsonNode question : questions) {
            int index = question.path("questionIndex").asInt();
            records.add(new QaRecord(
                index,
                question.path("question").asText(),
                question.path("category").asText(""),
                answers.get(index)
            ));
        }
        return new SessionData(sessionId, skillId, records);
    }

    private static void saveReport(Connection connection, long id, EvaluationReport report) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement("""
            UPDATE interview_sessions
            SET overall_score = ?, overall_feedback = ?, strengths_json = ?, improvements_json = ?,
                reference_answers_json = ?, status = 'EVALUATED', evaluate_status = 'COMPLETED',
                evaluate_error = NULL, completed_at = NOW()
            WHERE id = ?
            """)) {
            ps.setInt(1, report.overallScore());
            ps.setString(2, report.overallFeedback());
            ps.setString(3, MAPPER.writeValueAsString(report.strengths()));
            ps.setString(4, MAPPER.writeValueAsString(report.improvements()));
            ps.setString(5, MAPPER.writeValueAsString(report.referenceAnswers()));
            ps.setLong(6, id);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = connection.prepareStatement("""
            UPDATE interview_answers
            SET score = ?, feedback = ?, reference_answer = ?, key_points_json = ?
            WHERE session_id = ? AND question_index = ?
            """)) {
            Map<Integer, EvaluationReport.ReferenceAnswer> refs = new HashMap<>();
            for (EvaluationReport.ReferenceAnswer ref : report.referenceAnswers()) {
                refs.put(ref.questionIndex(), ref);
            }
            for (EvaluationReport.QuestionEvaluation detail : report.questionDetails()) {
                EvaluationReport.ReferenceAnswer ref = refs.get(detail.questionIndex());
                ps.setInt(1, detail.score());
                ps.setString(2, detail.feedback());
                ps.setString(3, ref == null ? null : ref.referenceAnswer());
                ps.setString(4, ref == null || ref.keyPoints() == null
                    ? null : MAPPER.writeValueAsString(ref.keyPoints()));
                ps.setLong(5, id);
                ps.setInt(6, detail.questionIndex());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static ChatClient buildPlainChatClient(String key, String modelName) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10_000);
        requestFactory.setReadTimeout(300_000);
        OpenAiApi openAiApi = OpenAiApi.builder()
            .baseUrl("https://dashscope.aliyuncs.com/compatible-mode")
            .apiKey(key)
            .restClientBuilder(RestClient.builder().requestFactory(requestFactory))
            .build();
        OpenAiChatModel chatModel = new OpenAiChatModel(
            openAiApi,
            OpenAiChatOptions.builder().model(modelName).temperature(0.2).build(),
            ToolCallingManager.builder().build(),
            RetryUtils.DEFAULT_RETRY_TEMPLATE,
            ObservationRegistry.NOOP
        );
        return ChatClient.builder(chatModel).build();
    }

    private record SessionData(String sessionId, String skillId, List<QaRecord> records) {}

    private static Path findEnvDev() {
        Path fromUserDir = Path.of(System.getProperty("user.dir")).resolve("../.env.dev").normalize();
        if (Files.isRegularFile(fromUserDir)) {
            return fromUserDir;
        }
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isRegularFile(cwd.resolve(".env.dev"))) {
            return cwd.resolve(".env.dev");
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
                if (trimmed.isEmpty() || trimmed.startsWith("#") || !trimmed.contains("=")) {
                    continue;
                }
                int eq = trimmed.indexOf('=');
                String key = trimmed.substring(0, eq).trim();
                String value = trimmed.substring(eq + 1).trim();
                if ((value.startsWith("\"") && value.endsWith("\""))
                    || (value.startsWith("'") && value.endsWith("'"))) {
                    value = value.substring(1, value.length() - 1);
                }
                map.put(key, value);
            }
        } catch (Exception e) {
            log.warn("读取 env 失败: {}", e.getMessage());
        }
        return map;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
