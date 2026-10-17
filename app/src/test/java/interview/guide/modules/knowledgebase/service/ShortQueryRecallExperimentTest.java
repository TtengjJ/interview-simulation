package interview.guide.modules.knowledgebase.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 短问召回对照实验。
 *
 * <p>生产库 vector_store 仅有 2 个 chunk，无法做标注评测。
 * 正确片段故意不包含短问原词，另有一篇干扰片段包含该词但主题无关，
 * 用来观察改写和放宽 topK/阈值能否把正确片段召回。
 *
 * <p>基线 A：原问题，topK=8，minScore=0.28，不改写。
 * <p>当前 B：Query Rewrite 后先搜改写句再搜原句，topK=20，minScore=0.18
 * （与 KnowledgeBaseQueryService 短问参数及候选顺序一致）。
 */
@DisplayName("短问召回对照实验")
class ShortQueryRecallExperimentTest {

    private static final Logger log = LoggerFactory.getLogger(ShortQueryRecallExperimentTest.class);
    private static final String BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1";
    private static final String EMBEDDING_MODEL = "text-embedding-v3";
    private static final int EMBEDDING_DIM = 1024;
    private static final int BASELINE_TOP_K = 8;
    private static final double BASELINE_MIN_SCORE = 0.28;
    private static final int CURRENT_TOP_K = 20;
    private static final double CURRENT_MIN_SCORE = 0.18;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(15))
        .build();

    private static String apiKey;
    private static String chatModel;

    record Chunk(String id, String text) {}

    record QueryCase(String question, String goldChunkId) {}

    record Hit(String chunkId, double score) {}

    @BeforeAll
    static void loadEnv() {
        Map<String, String> env = readEnvFile(findEnvDev());
        apiKey = firstNonBlank(env.get("AI_BAILIAN_API_KEY"), System.getenv("AI_BAILIAN_API_KEY"));
        chatModel = firstNonBlank(env.get("AI_MODEL"), System.getenv("AI_MODEL"), "qwen-plus");
    }

    static boolean hasRealApiKey() {
        if (apiKey == null) {
            loadEnv();
        }
        return apiKey != null
            && !apiKey.isBlank()
            && !apiKey.startsWith("your_")
            && !"test-api-key".equals(apiKey);
    }

    @Test
    @EnabledIf("hasRealApiKey")
    @DisplayName("30 条短问：基线 topK=8/0.28 对比改写+topK=20/0.18")
    void compareShortQueryRecall() throws Exception {
        assumeTrue(hasRealApiKey(), "跳过：未配置有效 AI_BAILIAN_API_KEY");

        List<Chunk> chunks = corpus();
        List<QueryCase> cases = cases();
        assertThat(cases).hasSize(30);
        assertThat(cases).allSatisfy(c ->
            assertThat(c.question().replaceAll("\\s+", "").length()).isLessThanOrEqualTo(4));

        Map<String, float[]> chunkVectors = embedAll(chunks.stream().map(Chunk::text).toList());
        Map<String, float[]> byId = new LinkedHashMap<>();
        for (int i = 0; i < chunks.size(); i++) {
            byId.put(chunks.get(i).id(), chunkVectors.get(chunks.get(i).text()));
        }

        int baselineHits = 0;
        int currentHits = 0;
        int gained = 0;
        int lost = 0;

        for (QueryCase item : cases) {
            List<Hit> baseline = search(item.question(), byId, BASELINE_TOP_K, BASELINE_MIN_SCORE);
            boolean baselineOk = containsGold(baseline, item.goldChunkId());

            String rewritten = rewrite(item.question());
            List<Hit> current = search(rewritten, byId, CURRENT_TOP_K, CURRENT_MIN_SCORE);
            if (current.isEmpty()) {
                current = search(item.question(), byId, CURRENT_TOP_K, CURRENT_MIN_SCORE);
            }
            boolean currentOk = containsGold(current, item.goldChunkId());

            if (baselineOk) {
                baselineHits++;
            }
            if (currentOk) {
                currentHits++;
            }
            if (!baselineOk && currentOk) {
                gained++;
            }
            if (baselineOk && !currentOk) {
                lost++;
            }
            log.info("短问='{}' gold={} rewrite='{}' baseline={} current={}",
                item.question(), item.goldChunkId(), rewritten, baselineOk, currentOk);
        }

        int n = cases.size();
        double recallA = (double) baselineHits / n;
        double recallB = (double) currentHits / n;
        double absolutePoints = (recallB - recallA) * 100;
        String relative = recallA == 0
            ? "N/A（基线为 0）"
            : String.format("%.1f%%", (recallB - recallA) / recallA * 100);

        log.info("""
            ========== 短问召回对照 ==========
            语料 chunks={} 短问 N={}
            基线 A: 原问题 topK={} minScore={} 命中={} Recall={}
            当前 B: rewrite + topK={} minScore={} 命中={} Recall={}
            绝对提升={} 个百分点
            相对提升={}
            B 找回 A 未中={}  A 中了 B 丢失={}
            ==================================
            """,
            chunks.size(), n,
            BASELINE_TOP_K, BASELINE_MIN_SCORE, baselineHits, String.format("%.1f%%", recallA * 100),
            CURRENT_TOP_K, CURRENT_MIN_SCORE, currentHits, String.format("%.1f%%", recallB * 100),
            String.format("%.1f", absolutePoints),
            relative,
            gained, lost
        );

        assertThat(baselineHits + currentHits).isPositive();
    }

    private static boolean containsGold(List<Hit> hits, String gold) {
        return hits.stream().anyMatch(h -> h.chunkId().equals(gold));
    }

    private static List<Hit> search(String query, Map<String, float[]> chunks, int topK, double minScore)
        throws Exception {
        if (query == null || query.isBlank()) {
            return List.of();
        }
        float[] q = embedAll(List.of(query)).get(query);
        List<Hit> ranked = new ArrayList<>();
        for (Map.Entry<String, float[]> entry : chunks.entrySet()) {
            double score = cosine(q, entry.getValue());
            if (score >= minScore) {
                ranked.add(new Hit(entry.getKey(), score));
            }
        }
        ranked.sort(Comparator.comparingDouble(Hit::score).reversed());
        if (ranked.size() > topK) {
            return ranked.subList(0, topK);
        }
        return ranked;
    }

    private static String rewrite(String question) throws Exception {
        String prompt = """
            你是一个检索查询改写助手。把用户原始问题改写成更适合知识库检索的单句查询。
            要求：保留核心意图；对过短问题补充必要语义；只输出单行纯文本，不要 Markdown，不要解释。
            用户原始问题：
            %s
            """.formatted(question);
        String body = MAPPER.writeValueAsString(Map.of(
            "model", chatModel,
            "temperature", 0.2,
            "messages", List.of(Map.of("role", "user", "content", prompt))
        ));
        JsonNode root = post("/chat/completions", body);
        String text = root.path("choices").path(0).path("message").path("content").asText("").trim();
        if (text.isBlank()) {
            return question;
        }
        int newline = text.indexOf('\n');
        return newline > 0 ? text.substring(0, newline).trim() : text;
    }

    private static Map<String, float[]> embedAll(List<String> texts) throws Exception {
        Map<String, float[]> result = new HashMap<>();
        for (int start = 0; start < texts.size(); start += 10) {
            List<String> batch = texts.subList(start, Math.min(start + 10, texts.size()));
            String body = MAPPER.writeValueAsString(Map.of(
                "model", EMBEDDING_MODEL,
                "input", batch,
                "dimensions", EMBEDDING_DIM
            ));
            JsonNode data = post("/embeddings", body).path("data");
            for (JsonNode item : data) {
                int index = item.path("index").asInt();
                JsonNode vectorNode = item.path("embedding");
                float[] vector = new float[vectorNode.size()];
                for (int i = 0; i < vectorNode.size(); i++) {
                    vector[i] = (float) vectorNode.get(i).asDouble();
                }
                result.put(batch.get(index), vector);
            }
        }
        return result;
    }

    private static JsonNode post(String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + path))
            .timeout(Duration.ofSeconds(60))
            .header("Authorization", "Bearer " + apiKey)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("DashScope " + path + " HTTP " + response.statusCode());
        }
        return MAPPER.readTree(response.body());
    }

    private static double cosine(float[] a, float[] b) {
        double dot = 0;
        double na = 0;
        double nb = 0;
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) {
            return 0;
        }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    private static List<Chunk> corpus() {
        List<Chunk> chunks = new ArrayList<>();
        chunks.addAll(List.of(
            new Chunk("spring-tx", "Spring 在方法上声明数据一致性边界。同类内部直接调用不会经过代理，因此不会按 REQUIRED 加入已有工作单元，抛出运行时异常时才会撤销已执行的写入。"),
            new Chunk("mysql-index", "InnoDB 二级索引只保存索引列和主键。若查询列不在二级索引中，需要再用主键访问聚簇索引取整行。查询列全部位于索引内时可以避免这次额外访问。"),
            new Chunk("mysql-lock", "两个数据会话各自持有对方下一步需要的记录锁并互相等待。范围锁会锁住索引上的空隙，从而阻止其他会话插入新行，避免同一查询条件两次读到不同行集。"),
            new Chunk("hashmap", "JDK 哈希表在元素数量超过容量乘以 0.75 后把桶数组扩大一倍。单个桶的链太长时，会把链转成平衡二叉搜索树，降低查找退化成线性扫描的概率。"),
            new Chunk("jvm-gc", "虚拟机把新创建的对象放在年轻分代。回收暂停所有应用线程的阶段称为 Stop The World。G1 把堆切成 Region，在并发标记后挑选垃圾比例高的 Region 拷贝存活对象。"),
            new Chunk("thread-pool", "任务先进入有界缓冲，缓冲满了再增加工作线程。线程数也达到上限后，新任务会按 Abort 或 CallerRuns 被丢掉或退回调用方执行。"),
            new Chunk("redis-cache", "查询一个库里根本没有的键时，请求会直接落到数据库。某个热点键刚好到期时，瞬时流量会打穿数据库。大量键在同一秒到期也会让数据库承受整库流量。概率型位图可以先挡掉不可能存在的键。"),
            new Chunk("aop", "面向横切逻辑时，有接口的类走 JDK 生成的调用转发对象，没有接口则生成子类。环绕逻辑可以决定要不要调用原来的业务方法。"),
            new Chunk("classloader", "加载请求先交给上层加载器，上层能加载就不再由当前加载器定义同名核心类。SPI 场景下当前加载器需要反向让下层完成实现类定义。"),
            new Chunk("cas", "处理器提供比较并替换指令，只有内存里的旧值仍等于期望值才写入新值。若值从甲变成乙再变回甲，仅比较数值会误判没人改过，需要额外的版本号。"),
            new Chunk("spring-cycle", "两个单例通过 setter 互相需要对方。容器会把尚未完成属性填充的对象提前放进缓存，让另一方先拿到引用，构造器参数里的互相需要则无法提前暴露。")
        ));
        chunks.addAll(List.of(
            new Chunk("trap-tx", "银行柜面办理事务时要核对客户身份，事务办理窗口每天上午九点开放。"),
            new Chunk("trap-index", "这本书最后有索引，按拼音列出人名和页码，方便查阅历史人物。"),
            new Chunk("trap-lock", "交通高峰经常死锁，路口的车谁也不让，间隙很小很难并线，幻读路牌会看错方向。"),
            new Chunk("trap-map", "密码学里的哈希可以把文件变成摘要。机房扩容要加机器。红黑是一种棋类开局。"),
            new Chunk("trap-gc", "城市垃圾分类叫 GC 试点。新生儿要打疫苗。STW 也是某车站的电报代码。"),
            new Chunk("trap-pool", "线程池是小区物业名词的误写。拒绝别人的邀请要委婉。食堂排队要用队列。"),
            new Chunk("trap-cache", "子弹可以穿透木板。绝缘体被击穿会导电。山上发生雪崩需要救援。布隆是游戏里的一名英雄。"),
            new Chunk("trap-aop", "AOP 是某协会缩写。切面是几何课的截面。代理是商务上代替他人签约。"),
            new Chunk("trap-loader", "双亲是父母的合称。委派是领导把工作交给下属。"),
            new Chunk("trap-cas", "CAS 是化学文摘登记号。ABA 是一种心理疗法的缩写。"),
            new Chunk("trap-cycle", "循环是操场跑圈。三级是考试成绩的等级。")
        ));
        return chunks;
    }

    private static List<QueryCase> cases() {
        return List.of(
            new QueryCase("事务", "spring-tx"),
            new QueryCase("传播", "spring-tx"),
            new QueryCase("回滚", "spring-tx"),
            new QueryCase("索引", "mysql-index"),
            new QueryCase("回表", "mysql-index"),
            new QueryCase("覆盖", "mysql-index"),
            new QueryCase("死锁", "mysql-lock"),
            new QueryCase("间隙", "mysql-lock"),
            new QueryCase("幻读", "mysql-lock"),
            new QueryCase("哈希", "hashmap"),
            new QueryCase("扩容", "hashmap"),
            new QueryCase("红黑", "hashmap"),
            new QueryCase("GC", "jvm-gc"),
            new QueryCase("新生", "jvm-gc"),
            new QueryCase("STW", "jvm-gc"),
            new QueryCase("线程池", "thread-pool"),
            new QueryCase("拒绝", "thread-pool"),
            new QueryCase("队列", "thread-pool"),
            new QueryCase("穿透", "redis-cache"),
            new QueryCase("击穿", "redis-cache"),
            new QueryCase("雪崩", "redis-cache"),
            new QueryCase("布隆", "redis-cache"),
            new QueryCase("AOP", "aop"),
            new QueryCase("切面", "aop"),
            new QueryCase("代理", "aop"),
            new QueryCase("双亲", "classloader"),
            new QueryCase("委派", "classloader"),
            new QueryCase("CAS", "cas"),
            new QueryCase("ABA", "cas"),
            new QueryCase("循环", "spring-cycle")
        );
    }

    private static Path findEnvDev() {
        Path fromUserDir = Path.of(System.getProperty("user.dir")).resolve("../.env.dev").normalize();
        if (Files.isRegularFile(fromUserDir)) {
            return fromUserDir;
        }
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isRegularFile(cwd.resolve(".env.dev"))) {
            return cwd.resolve(".env.dev");
        }
        if (cwd.getParent() != null && Files.isRegularFile(cwd.getParent().resolve(".env.dev"))) {
            return cwd.getParent().resolve(".env.dev");
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
