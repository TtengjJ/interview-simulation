package interview.guide.modules.interview.skill;

import interview.guide.common.ai.LlmProviderRegistry;
import interview.guide.common.ai.StructuredOutputInvoker;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 面试方向 Skill 管理服务
 * <p>
 * 负责管理不同技术栈（如 Java、Python、算法等）的面试配置与参考资料。
 * 核心功能包括：
 * <ul>
 *   <li><b>Skill 加载</b>：从 classpath:skills/{id}/SKILL.md 解析预设面试官人设</li>
 *   <li><b>分类映射</b>：建立考核维度（Category）与参考文件（Reference）的关联索引</li>
 *   <li><b>JD 解析</b>：利用 LLM 从职位描述中提取面试重点并匹配对应的参考知识</li>
 *   <li><b>Prompt 注入</b>：在面试和评估阶段，动态将相关的 Markdown 参考资料注入到 LLM Prompt 中</li>
 * </ul>
 * </p>
 *
 * <b>架构定位</b>：
 * 与 SkillsTool 互补。SkillsTool 负责在运行时按需加载 SKILL.md 的人设部分，
 * 而本类负责在后端预处理分类配置（skill.meta.yml），并在生成 Prompt 时批量注入 references。
 */
@Slf4j
@Service
public class InterviewSkillService {

    public static final String CUSTOM_SKILL_ID = "custom";

    private static final int MIN_JD_LENGTH = 50;

    private static final Pattern FRONT_MATTER_PATTERN = Pattern.compile("(?s)^---\\s*\\n(.*?)\\n---\\s*\\n?(.*)$");
    private static final Pattern SKILL_ID_PATTERN = Pattern.compile(".*/skills/([^/]+)/SKILL\\.md$");
    private static final String SKILL_META_FILE = "skill.meta.yml";
    private static final String JD_PARSE_SYSTEM_PROMPT_PATH = "classpath:prompts/jd-parse-system.st";

    private static final int MAX_REFERENCE_SECTION_CHARS = 12000;
    private static final int MAX_EVALUATION_REFERENCE_SECTION_CHARS = 6000;
    private static final int MAX_SINGLE_REFERENCE_CHARS = 3000;

    private final LlmProviderRegistry llmProviderRegistry;
    private final StructuredOutputInvoker structuredOutputInvoker;
    private final BeanOutputConverter<CategoryListDTO> jdOutputConverter;
    private final PromptTemplate jdSystemPromptTemplate;
    private final ResourceLoader resourceLoader;

    /** 预设 Skill 注册表，启动时从 classpath:skills/{skillId}/SKILL.md 加载，使用 TreeMap 保证有序性 */
    private final Map<String, InterviewSkillProperties.SkillDefinition> presetRegistry = new TreeMap<>();

    /** 参考内容缓存（classpath 资源不可变，加载一次后复用，避免重复 IO） */
    private final Map<String, String> referenceCache = new ConcurrentHashMap<>();

    /** 全局 category key → (ref文件名, 是否shared) 映射，启动时构建，之后只读，用于快速查找参考文件路径 */
    private final Map<String, RefMapping> categoryRefIndex = new HashMap<>();

    /** JD 解析用的参考文件清单 Markdown 表格，启动时生成一次，作为 LLM 解析 JD 时的上下文提示 */
    private String cachedReferenceFileList;

    record RefMapping(String ref, boolean shared, String sourceSkillId) {}

    public InterviewSkillService(LlmProviderRegistry llmProviderRegistry,
                                 StructuredOutputInvoker structuredOutputInvoker,
                                 ResourceLoader resourceLoader) throws IOException {
        this.llmProviderRegistry = llmProviderRegistry;
        this.structuredOutputInvoker = structuredOutputInvoker;
        this.resourceLoader = resourceLoader;
        this.jdOutputConverter = new BeanOutputConverter<>(CategoryListDTO.class) {};
        this.jdSystemPromptTemplate = new PromptTemplate(loadClasspathPrompt(JD_PARSE_SYSTEM_PROMPT_PATH));
    }

    @PostConstruct
    void loadPresetSkills() throws IOException {
        // 扫描 classpath:skills/ 目录下所有的 SKILL.md 文件
        var resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath:skills/*/SKILL.md");
        Yaml yaml = new Yaml();

        for (Resource resource : resources) {
            String skillId = extractSkillId(resource);
            if (skillId == null || "_shared".equals(skillId)) {
                continue;
            }

            // 解析 Skill 定义（包含 front matter 中的 name/description 以及 body 中的 persona）
            InterviewSkillProperties.SkillDefinition def = parseSkillDefinition(skillId, resource, yaml);
            if (def.getName() == null || def.getName().isBlank()) {
                log.warn("跳过无效 Skill（缺少 name）: {}", skillId);
                continue;
            }

            presetRegistry.put(skillId, def);
            log.info("加载预设 Skill: {} ({})", skillId, def.getName());
        }

        log.info("共加载 {} 个预设 Skill", presetRegistry.size());

        // 构建分类到参考文件的索引，方便后续根据分类名快速找到对应的知识点文件
        buildCategoryRefIndex();
        // 预生成参考文件列表的 Markdown 表格，供 JD 解析时使用
        cachedReferenceFileList = buildReferenceFileList();
    }

    /**
     * 构建分类到参考文件的映射索引
     * <p>
     * 遍历所有预设 Skill 的分类配置，提取出每个分类对应的参考文件名（ref）。
     * 这个索引是后续“知识点注入”的核心依据。
     * </p>
     */
    private void buildCategoryRefIndex() {
        categoryRefIndex.clear();
        for (var entry : presetRegistry.entrySet()) {
            InterviewSkillProperties.SkillDefinition def = entry.getValue();
            if (def.getCategories() == null) continue;
            for (InterviewSkillProperties.CategoryDef cat : def.getCategories()) {
                if (cat.getRef() != null && !cat.getRef().isBlank() && cat.getKey() != null) {
                    categoryRefIndex.putIfAbsent(cat.getKey(),
                        new RefMapping(cat.getRef(), Boolean.TRUE.equals(cat.getShared()), entry.getKey()));
                }
            }
        }
        log.info("构建 category→reference 映射: {} 个条目", categoryRefIndex.size());
    }

    public List<SkillDTO> getAllSkills() {
        return presetRegistry.entrySet().stream()
            .map(e -> toSkillDTO(e.getKey(), e.getValue()))
            .toList();
    }

    public SkillDTO getSkill(String skillId) {
        InterviewSkillProperties.SkillDefinition preset = presetRegistry.get(skillId);
        if (preset != null) {
            return toSkillDTO(skillId, preset);
        }
        throw new BusinessException(ErrorCode.BAD_REQUEST, "未找到面试主题: " + skillId);
    }

    /**
     * 从 JD 解析结果构建自定义 SkillDTO。
     * 遍历 customCategories，尝试在 categoryRefIndex 中匹配参考文件。
     */
    public SkillDTO buildCustomSkill(List<CategoryDTO> customCategories, String jdText) {
        List<SkillCategoryDTO> categories = customCategories.stream()
            .map(cat -> {
                RefMapping refMapping = categoryRefIndex.get(cat.key());
                if (refMapping != null) {
                    if (!refMapping.ref().equals(cat.ref())
                        || refMapping.shared() != Boolean.TRUE.equals(cat.shared())) {
                        log.info("JD 分类 reference 已按本地映射纠正: key={}, modelRef={}, modelShared={}, mappedRef={}, mappedShared={}",
                            cat.key(), cat.ref(), cat.shared(), refMapping.ref(), refMapping.shared());
                    }
                    return new SkillCategoryDTO(cat.key(), cat.label(), cat.priority(),
                        refMapping.ref(), refMapping.shared());
                }
                return new SkillCategoryDTO(cat.key(), cat.label(), cat.priority(),
                    cat.ref(), Boolean.TRUE.equals(cat.shared()));
            })
            .toList();

        long matchedCount = categories.stream().filter(c -> c.ref() != null && !c.ref().isBlank()).count();
        log.info("构建自定义 Skill: {} 个分类, {} 个匹配到参考文件", categories.size(), matchedCount);

        return new SkillDTO(CUSTOM_SKILL_ID, "自定义面试（JD 解析）",
            "基于职位描述提取的面试方向", categories,
            false, jdText, null, null);
    }

    /**
     * 从 JD 文本中解析出面试考核维度
     * <p>
     * 调用 LLM 分析职位描述，提取出关键技术点和能力要求，并将其映射到系统已有的分类体系中。
     * </p>
     *
     * <b>处理流程</b>：
     * <ol>
     *   <li>校验 JD 长度，防止过短文本导致解析失败</li>
     *   <li>渲染 System Prompt，注入当前可用的参考文件列表作为上下文</li>
     *   <li>调用 LLM 进行结构化输出，返回 {@code CategoryListDTO}</li>
     *   <li>统计匹配到的参考文件数量，用于日志监控</li>
     * </ol>
     */
    public List<CategoryDTO> parseJd(String jdText) {
        if (jdText == null || jdText.length() < MIN_JD_LENGTH) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "JD 内容太少（至少 " + MIN_JD_LENGTH + " 字），请补充后重试");
        }

        log.info("开始解析 JD，长度: {}", jdText.length());

        ChatClient chatClient = llmProviderRegistry.getDefaultChatClient();
        String systemPrompt = jdSystemPromptTemplate.render(Map.of(
            "referenceFileList", cachedReferenceFileList
        )) + "\n\n" + jdOutputConverter.getFormat();
        String userPrompt = "职位描述：\n" + jdText;

        try {
            CategoryListDTO result = structuredOutputInvoker.invoke(
                chatClient, systemPrompt, userPrompt, jdOutputConverter,
                ErrorCode.AI_SERVICE_ERROR, "JD 解析失败：", "JD 解析", log
            );

            if (result == null || result.categories() == null || result.categories().isEmpty()) {
                throw new BusinessException(ErrorCode.AI_SERVICE_ERROR, "JD 解析结果为空，请重试");
            }

            long refMatched = result.categories().stream().filter(c -> c.ref() != null && !c.ref().isBlank()).count();
            log.info("JD 解析完成: {} 个方向, {} 个匹配到参考文件", result.categories().size(), refMatched);
            return result.categories();
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("JD 解析失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.AI_SERVICE_ERROR, "JD 解析失败，请重试或选择预设主题");
        }
    }

    private String buildReferenceFileList() {
        // 收集所有去重的参考文件（文件名 → Markdown 表格行）
        Map<String, String> refDescriptions = new LinkedHashMap<>();
        for (var entry : presetRegistry.entrySet()) {
            String skillName = entry.getValue().getDisplayName() != null
                ? entry.getValue().getDisplayName() : entry.getValue().getName();
            if (entry.getValue().getCategories() == null) continue;
            for (InterviewSkillProperties.CategoryDef cat : entry.getValue().getCategories()) {
                if (cat.getRef() != null && !cat.getRef().isBlank()) {
                    refDescriptions.putIfAbsent(cat.getRef(),
                        "| " + cat.getRef()
                            + " | " + (Boolean.TRUE.equals(cat.getShared()) ? "shared" : "skill-local")
                            + " | " + skillName
                            + " | " + cat.getLabel() + " |\n");
                }
            }
        }

        if (refDescriptions.isEmpty()) {
            return "（无可用参考文件）";
        }

        StringBuilder sb = new StringBuilder("| 文件名 | 范围 | 来源 Skill | 覆盖内容 |\n");
        sb.append("|--------|------|-------------|----------|\n");
        for (String row : refDescriptions.values()) {
            sb.append(row);
        }
        return sb.toString();
    }

    public Map<String, Integer> calculateAllocation(String skillId, int totalQuestions) {
        return calculateAllocation(getSkill(skillId).categories(), totalQuestions);
    }

    /**
     * 计算题目在不同考核维度间的分配方案
     * <p>
     * 采用“保底 + 轮转”的策略，确保重要考点不被遗漏，同时合理分配题量。
     * </p>
     *
     * <b>分配逻辑</b>：
     * <ol>
     *   <li><b>Phase 1</b>：给标记为 {@code ALWAYS_ONE} 的分类各分配 1 题（如：自我介绍）</li>
     *   <li><b>Phase 2</b>：给所有剩余分类（CORE 优先）各分配 1 题，保证覆盖面</li>
     *   <li><b>Phase 3</b>：如果还有剩余名额，按 CORE → Normal 的顺序循环追加题目</li>
     * </ol>
     */
    public Map<String, Integer> calculateAllocation(List<SkillCategoryDTO> categories, int totalQuestions) {
        List<SkillCategoryDTO> alwaysOneCats = new ArrayList<>();
        List<SkillCategoryDTO> coreCats = new ArrayList<>();
        List<SkillCategoryDTO> normalCats = new ArrayList<>();

        for (SkillCategoryDTO cat : categories) {
            switch (cat.priority()) {
                case "ALWAYS_ONE" -> alwaysOneCats.add(cat);
                case "CORE" -> coreCats.add(cat);
                default -> normalCats.add(cat);
            }
        }

        Map<String, Integer> allocation = new LinkedHashMap<>();
        int remaining = totalQuestions;

        // Phase 1: ALWAYS_ONE 保底各 1 题
        for (SkillCategoryDTO cat : alwaysOneCats) {
            if (remaining > 0) {
                allocation.put(cat.key(), 1);
                remaining--;
            }
        }

        // Phase 2: 先给所有类目各 1 题（CORE 优先），保证覆盖率
        for (SkillCategoryDTO cat : coreCats) {
            if (remaining > 0) {
                allocation.put(cat.key(), 1);
                remaining--;
            }
        }
        for (SkillCategoryDTO cat : normalCats) {
            if (remaining > 0) {
                allocation.put(cat.key(), 1);
                remaining--;
            }
        }

        // Phase 3: 剩余名额按 CORE 优先轮转分配
        while (remaining > 0) {
            for (SkillCategoryDTO cat : coreCats) {
                if (remaining <= 0) break;
                allocation.merge(cat.key(), 1, Integer::sum);
                remaining--;
            }
            for (SkillCategoryDTO cat : normalCats) {
                if (remaining <= 0) break;
                allocation.merge(cat.key(), 1, Integer::sum);
                remaining--;
            }
            if (coreCats.isEmpty() && normalCats.isEmpty()) break;
        }

        // 确保所有类目都出现在 allocation 中（即使题量为 0）
        for (SkillCategoryDTO cat : coreCats) {
            allocation.putIfAbsent(cat.key(), 0);
        }
        for (SkillCategoryDTO cat : normalCats) {
            allocation.putIfAbsent(cat.key(), 0);
        }

        log.debug("题目分配: total={}, allocation={}", totalQuestions, allocation);
        return allocation;
    }

/**
 * 根据技能类别分配情况构建描述字符串
 *
 * @param allocation 技能类别分配映射表，键为类别键，值为题目数量
 * @param categories 技能类别DTO列表，包含类别标签和优先级等信息
 * @return 返回格式化的分配描述字符串，每行显示一个类别的分配情况
 */
    public String buildAllocationDescription(Map<String, Integer> allocation, List<SkillCategoryDTO> categories) {
    // 使用StringBuilder来高效构建字符串
        StringBuilder sb = new StringBuilder();
    // 遍历所有技能类别
        for (SkillCategoryDTO cat : categories) {
        // 获取当前类别的分配数量，如果不存在则默认为0
            int count = allocation.getOrDefault(cat.key(), 0);
        // 如果分配数量大于0，则添加到描述字符串中
            if (count > 0) {
            // 按照指定格式添加类别信息：类别标签 | 题目数量 | 优先级 |
                sb.append("| ").append(cat.label()).append(" | ").append(count).append(" 题 | ").append(cat.priority()).append(" |\n");
            }
        }
    // 返回构建好的描述字符串
        return sb.toString();
    }

/**
 * 构建参考部分的方法
 * @param skill 技能数据传输对象，包含技能相关信息
 * @param allocation 一个映射表，存储类别与对应的分配数量
 * @return 返回构建好的参考部分字符串
 */
    public String buildReferenceSection(SkillDTO skill, Map<String, Integer> allocation) {
    // 调用内部方法构建参考部分，传入技能对象、过滤条件和最大字符限制
        return buildReferenceSectionInternal(
            skill,  // 技能数据传输对象
            category -> allocation.getOrDefault(category.key(), 0) > 0,  // Lambda表达式，用于过滤分配数量大于0的类别
            MAX_REFERENCE_SECTION_CHARS  // 参考部分的最大字符数限制
        );
    }

    /**
     * 评估阶段参考基线：不限制题量分配，覆盖该 skill 下所有配置了 reference 的分类。
     */
    public String buildEvaluationReferenceSection(String skillId) {
        SkillDTO skill = getSkill(skillId);
        return buildReferenceSectionInternal(
            skill,
            category -> true,
            MAX_EVALUATION_REFERENCE_SECTION_CHARS
        );
    }

    /**
     * 安全版本的评估参考基线：skillId 为空或加载失败时返回空字符串，不抛异常。
     */
    public String buildEvaluationReferenceSectionSafe(String skillId) {
        if (skillId == null || skillId.isBlank()) {
            return "";
        }
        try {
            return buildEvaluationReferenceSection(skillId);
        } catch (Exception e) {
            log.warn("加载评估参考基线失败，降级为无参考: skillId={}, error={}", skillId, e.getMessage());
            return "";
        }
    }

    /**
     * 内部方法：根据过滤器构建参考内容片段
     * <p>
     * 该方法负责从文件系统读取 Markdown 格式的参考资料，并按分类组装成 Prompt 的一部分。
     * </p>
     *
     * <b>关键逻辑</b>：
     * <ul>
     *   <li><b>Custom Skill 路径修正</b>：如果是自定义 Skill，需要根据 categoryKey 反查原始 Skill ID 来定位文件</li>
     *   <li><b>字符数限制</b>：防止注入的内容过长导致 LLM Context Window 溢出，超过阈值会自动截断</li>
     *   <li><b>缓存命中</b>：通过 {@code referenceCache} 避免重复读取磁盘文件</li>
     * </ul>
     */
    private String buildReferenceSectionInternal(SkillDTO skill,  // 技能数据传输对象，包含技能的基本信息和分类信息
                                                 Predicate<SkillCategoryDTO> categoryFilter,  // 分类过滤器，用于筛选需要处理的技能分类
                                                 int maxChars) {  // 最大允许的字符数，用于控制输出长度
        StringBuilder sb = new StringBuilder();  // 使用 StringBuilder 高效构建字符串结果

    // 遍历技能的所有分类
        for (SkillCategoryDTO category : skill.categories()) {
        // 使用过滤器测试当前分类，不符合则跳过
            if (!categoryFilter.test(category)) {
                continue;
            }
        // 如果分类没有引用内容或引用内容为空，则跳过
            if (category.ref() == null || category.ref().isBlank()) {
                continue;
            }

            // custom 模式下非 shared 的 ref 需要查原始 skillId 拼路径
            String effectiveSkillId = skill.id();  // 默认使用当前技能ID
        // 如果是自定义技能且分类不是共享的，需要查找原始技能ID
            if (CUSTOM_SKILL_ID.equals(skill.id()) && !category.shared() && category.ref() != null) {
            // 从分类引用索引中查找映射关系
                RefMapping mapping = categoryRefIndex.get(category.key());
                if (mapping != null) {
                    effectiveSkillId = mapping.sourceSkillId();  // 使用原始技能ID
                }
            }
        // 加载引用内容
            String referenceContent = loadReferenceContent(effectiveSkillId, category.ref(), category.shared());
            if (referenceContent.isBlank()) {
                continue;  // 如果加载的内容为空，则跳过
            }

        // 如果结果不为空，添加分隔符
            if (!sb.isEmpty()) {
                sb.append("\n\n");
            }
        // 添加分类标题和内容
            sb.append("### ").append(category.label()).append(" (").append(category.key()).append(")\n");
            sb.append(referenceContent);

        // 检查是否超过最大字符数限制
            if (sb.length() >= maxChars) {
                sb.setLength(maxChars);  // 截断到最大长度
                sb.append("\n...（references 已截断）");  // 添加截断标记
                break;  // 超出限制后停止处理
            }
        }

    // 如果结果为空，返回默认提示信息
        return sb.isEmpty() ? "未配置 references。" : sb.toString();
    }

    private String loadClasspathPrompt(String path) throws IOException {
        Resource resource = resourceLoader.getResource(path);
        return resource.getContentAsString(StandardCharsets.UTF_8);
    }

/**
 * 解析技能定义的方法，从资源文件中读取并解析技能的 front matter 和内容
 * @param skillId 技能的唯一标识符
 * @param resource 包含技能定义的资源文件
 * @param yaml YAML解析器实例，用于解析front matter
 * @return 解析后的技能定义对象
 * @throws BusinessException 当文件格式错误或读取失败时抛出
 */
    private InterviewSkillProperties.SkillDefinition parseSkillDefinition(String skillId, Resource resource, Yaml yaml) {
        try {
        // 读取资源文件内容为字符串，使用UTF-8编码
            String markdown = resource.getContentAsString(StandardCharsets.UTF_8);
        // 使用正则表达式匹配front matter模式
            Matcher matcher = FRONT_MATTER_PATTERN.matcher(markdown);
        // 检查是否匹配到front matter
            if (!matcher.matches()) {
            // 如果没有匹配到，抛出业务异常，提示文件格式错误
                throw new BusinessException(ErrorCode.BAD_REQUEST,
                    "Skill 文件格式错误（缺少 front matter）: " + resource.getDescription());
            }

        // 获取front matter部分内容
            String frontMatter = matcher.group(1);
        // 获取并处理body部分内容，去除首尾空白
            String body = matcher.group(2) != null ? matcher.group(2).trim() : "";

        // 将front matter内容解析为SkillFrontMatterDefinition对象
            InterviewSkillProperties.SkillFrontMatterDefinition frontMatterDef =
                yaml.loadAs(frontMatter, InterviewSkillProperties.SkillFrontMatterDefinition.class);

        // 创建技能定义对象
            InterviewSkillProperties.SkillDefinition definition = new InterviewSkillProperties.SkillDefinition();
        // 如果front matter定义不为空，设置名称和描述
            if (frontMatterDef != null) {
                definition.setName(frontMatterDef.getName());
                definition.setDescription(frontMatterDef.getDescription());
            }
        // 如果body部分不为空，设置persona
            if (!body.isBlank()) {
                definition.setPersona(body);
            }

        // 加载技能的元定义信息
            InterviewSkillProperties.SkillMetaDefinition metaDef = loadSkillMetaDefinition(skillId, yaml);
        // 如果元定义不为空，设置显示名称、显示信息和分类
            if (metaDef != null) {
                definition.setDisplayName(metaDef.getDisplayName());
                definition.setDisplay(metaDef.getDisplay());
                definition.setCategories(metaDef.getCategories());
            }

        // 确保分类不为null，如果为null则设置为空列表
            if (definition.getCategories() == null) {
                definition.setCategories(List.of());
            }
        // 返回完整的技能定义
            return definition;
        } catch (IOException e) {
        // 如果发生IO异常，抛出业务异常，提示读取文件失败
            throw new BusinessException(ErrorCode.INTERNAL_ERROR,
                "读取 Skill 文件失败: " + resource.getDescription());
        }
    }

    private InterviewSkillProperties.SkillMetaDefinition loadSkillMetaDefinition(String skillId, Yaml yaml) {
        String location = "classpath:skills/" + skillId + "/" + SKILL_META_FILE;
        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            log.warn("skill meta 文件不存在，使用默认配置: skillId={}, location={}", skillId, location);
            return null;
        }

        try {
            String content = resource.getContentAsString(StandardCharsets.UTF_8);
            InterviewSkillProperties.SkillMetaDefinition meta = yaml.loadAs(content, InterviewSkillProperties.SkillMetaDefinition.class);
            return meta != null ? meta : new InterviewSkillProperties.SkillMetaDefinition();
        } catch (IOException e) {
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "读取 skill meta 文件失败: " + location);
        }
    }

    private String extractSkillId(Resource resource) {
        try {
            String normalized = resource.getURL().toString().replace('\\', '/');
            Matcher matcher = SKILL_ID_PATTERN.matcher(normalized);
            if (matcher.matches()) {
                return matcher.group(1);
            }
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    private String loadReferenceContent(String skillId, String referenceFile, boolean shared) {
        if (!isSafeReferencePath(referenceFile)) {
            log.warn("忽略不安全的 reference 路径: skillId={}, ref={}", skillId, referenceFile);
            return "";
        }

        List<String> candidateLocations = resolveReferenceLocations(skillId, referenceFile, shared);
        for (String location : candidateLocations) {
            String content = referenceCache.computeIfAbsent(location, this::readReferenceContent);
            if (!content.isBlank()) {
                return content;
            }
        }

        log.warn("未找到 reference: skillId={}, ref={}, shared={}, locations={}",
            skillId, referenceFile, shared, candidateLocations);
        return "";
    }

    private List<String> resolveReferenceLocations(String skillId, String referenceFile, boolean shared) {
        LinkedHashSet<String> locations = new LinkedHashSet<>();
        if (shared) {
            locations.add(buildSharedReferenceLocation(referenceFile));
        }

        addSkillReferenceLocations(locations, skillId, referenceFile);

        if (!shared) {
            locations.add(buildSharedReferenceLocation(referenceFile));
        }

        if (CUSTOM_SKILL_ID.equals(skillId) || shared) {
            for (String presetSkillId : presetRegistry.keySet()) {
                addSkillReferenceLocations(locations, presetSkillId, referenceFile);
            }
        }

        return List.copyOf(locations);
    }

    private void addSkillReferenceLocations(LinkedHashSet<String> locations, String skillId, String referenceFile) {
        if (skillId == null || skillId.isBlank() || CUSTOM_SKILL_ID.equals(skillId)) {
            return;
        }
        locations.add("classpath:skills/" + skillId + "/references/" + referenceFile);
        locations.add("classpath:skills/" + skillId + "/" + referenceFile);
    }

    private String buildSharedReferenceLocation(String referenceFile) {
        return "classpath:skills/_shared/references/" + referenceFile;
    }

    private String readReferenceContent(String location) {
        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            return "";
        }

        try {
            String content = resource.getContentAsString(StandardCharsets.UTF_8).trim();
            if (content.length() > MAX_SINGLE_REFERENCE_CHARS) {
                return content.substring(0, MAX_SINGLE_REFERENCE_CHARS) + "\n...（单文件内容已截断）";
            }
            return content;
        } catch (IOException e) {
            log.warn("读取 reference 失败: location={}", location, e);
            return "";
        }
    }

    private boolean isSafeReferencePath(String referenceFile) {
        return !referenceFile.contains("..")
            && !referenceFile.startsWith("/")
            && !referenceFile.startsWith("\\")
            && referenceFile.matches("[a-zA-Z0-9._/-]+");
    }

    private SkillDTO toSkillDTO(String id, InterviewSkillProperties.SkillDefinition def) {
        String skillDisplayName = (def.getDisplayName() != null && !def.getDisplayName().isBlank())
            ? def.getDisplayName()
            : def.getName();

        InterviewSkillProperties.DisplayDef disp = def.getDisplay();
        DisplayDTO displayDTO = disp != null
            ? new DisplayDTO(disp.getIcon(), disp.getGradient(), disp.getIconBg(), disp.getIconColor())
            : null;

        List<SkillCategoryDTO> categories = def.getCategories() == null
            ? List.of()
            : def.getCategories().stream()
                .map(c -> new SkillCategoryDTO(
                    c.getKey(),
                    c.getLabel(),
                    c.getPriority(),
                    c.getRef(),
                    Boolean.TRUE.equals(c.getShared())
                ))
                .toList();

        return new SkillDTO(
            id,
            skillDisplayName,
            def.getDescription(),
            categories,
            true,
            null,
            def.getPersona(),
            displayDTO
        );
    }

    public record SkillDTO(String id, String name, String description,
                           List<SkillCategoryDTO> categories,
                           boolean isPreset, String sourceJd, String persona, DisplayDTO display) {}

    public record DisplayDTO(String icon, String gradient, String iconBg, String iconColor) {}

    /**
     * 预设 Skill 分类（可携带 references 绑定信息）
     */
    public record SkillCategoryDTO(String key, String label, String priority, String ref, boolean shared) {}

    /**
     * JD 解析返回分类（可携带 LLM 匹配的 ref/shared 信息，后端会按本地 categoryRefIndex 纠正）
     */
    public record CategoryDTO(String key, String label, String priority,
                               String ref, Boolean shared) {}

    private record CategoryListDTO(List<CategoryDTO> categories) {}
}
