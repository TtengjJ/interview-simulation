package interview.guide.modules.voiceinterview.service;

import interview.guide.common.ai.LlmProviderRegistry;
import interview.guide.modules.resume.model.ResumeEntity;
import interview.guide.modules.resume.repository.ResumeRepository;
import interview.guide.modules.voiceinterview.config.VoiceInterviewProperties;
import interview.guide.modules.voiceinterview.model.VoiceInterviewSessionEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * DashScope LLM 服务（通义千问大语言模型）
 * <p>
 * 基于 Spring AI ChatClient 封装的 LLM 对话服务，支持同步调用和流式输出。
 * 专为语音面试场景优化，提供句子级别的流式分割和语音友好型文本处理。
 * </p>
 *
 * 核心特性：
 * <ul>
 *   <li><b>多 Provider 支持</b>：通过 LlmProviderRegistry 动态选择 LLM 提供商</li>
 *   <li><b>简历上下文注入</b>：自动加载候选人简历并融入 System Prompt</li>
 *   <li><b>流式句子分割</b>：检测终止标点（。！？；!?;.）并按句子推送 TTS</li>
 *   <li><b>实时文本节流</b>：控制字幕推送频率（最小间隔 + 最小字符增量）</li>
 *   <li><b>语音优化</b>：去除 Markdown 标记、限制长度、智能截断</li>
 *   <li><b>错误映射</b>：将技术异常转换为用户友好的中文提示</li>
 * </ul>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DashscopeLlmService {

    /** 终止标点符号集合（用于句子边界检测） */
    private static final String TERMINAL_PUNCTUATION = "。！？；!?;.";

    private final LlmProviderRegistry llmProviderRegistry;       // LLM 提供商注册表
    private final VoiceInterviewPromptService promptService;     // Prompt 模板服务
    private final ResumeRepository resumeRepository;             // 简历数据仓库
    private final VoiceInterviewProperties voiceInterviewProperties;  // 配置属性

    /**
     * 同步调用 LLM 生成回复
     * <p>
     * 适用于非流式场景，一次性等待完整响应后返回。
     * </p>
     *
     * @param userInput 用户输入文本
     * @param session 面试会话实体
     * @param conversationHistory 历史对话记录
     * @return 优化后的 AI 回复文本
     */
    public String chat(String userInput, VoiceInterviewSessionEntity session, List<String> conversationHistory) {
        try {
            // 步骤 1：构建 Prompt 上下文（包含 System Prompt 和用户输入）
            PromptContext promptContext = buildPromptContext(userInput, session, conversationHistory);

            // 步骤 2：获取配置的 LLM Provider
            String provider = session.getLlmProvider();
            log.info("[VoiceInterview] Session {} using LLM provider: {}", session.getId(), provider);

            // 步骤 3：从注册表获取 ChatClient
            ChatClient chatClient = llmProviderRegistry.getVoiceChatClient(provider);

            // 步骤 4：调用 LLM（阻塞等待完整响应）
            ChatClient.CallResponseSpec response = chatClient.prompt()
                .system(promptContext.systemPrompt())  // 设置系统提示词
                .user(promptContext.userPrompt())      // 设置用户输入
                .call();                               // 同步调用

            // 步骤 5：提取原始响应文本
            String content = response.chatResponse().getResult().getOutput().getText();

            // 步骤 6：语音优化（去格式、截断等）
            String optimized = optimizeForVoice(content);

            log.info("LLM response generated for session {}: {}", session.getId(),
                     optimized.substring(0, Math.min(100, optimized.length())));

            return optimized;

        } catch (Exception e) {
            log.error("LLM chat error for session {}: {}", session.getId(), e.getMessage(), e);
            return mapLlmErrorToUserMessage(e);
        }
    }

    /**
     * 流式调用 LLM（仅推送 Token，不分割句子）
     * <p>
     * 兼容旧接口，内部委托给 {@link #chatStreamSentences}
     * </p>
     */
    public String chatStream(String userInput, Consumer<String> onToken, VoiceInterviewSessionEntity session, List<String> conversationHistory) {
        return chatStreamSentences(userInput, onToken, null, session, conversationHistory);
    }

    /**
     * 流式调用 LLM，按句子级别推送文本
     * <p>
     * 该方法在接收 LLM 流式输出的同时，执行两项关键任务：
     * <ol>
     *   <li><b>句子分割</b>：检测到终止标点时，回调 onSentence 触发 TTS 合成</li>
     *   <li><b>实时字幕</b>：按固定频率推送部分文本给 onToken（用于前端显示）</li>
     * </ol>
     * </p>
     * <p>
     * <b>节流机制</b>：
     * <ul>
     *   <li>时间阈值：至少间隔 emitIntervalMs（默认 80ms）</li>
     *   <li>字符阈值：至少新增 minCharsDelta 个字符（默认 4 个）</li>
     * </ul>
     * </p>
     *
     * @param userInput 用户输入文本
     * @param onToken 实时文本回调（用于字幕显示，可为 null）
     * @param onSentence 句子完成回调（用于触发 TTS，可为 null）
     * @param session 面试会话实体
     * @param conversationHistory 历史对话记录
     * @return 完整优化后的文本
     */
    public String chatStreamSentences(String userInput,
                                       Consumer<String> onToken,
                                       Consumer<String> onSentence,
                                       VoiceInterviewSessionEntity session,
                                       List<String> conversationHistory) {
        try {
            // 步骤 1：构建 Prompt 上下文
            PromptContext promptContext = buildPromptContext(userInput, session, conversationHistory);
            String provider = session.getLlmProvider();
            log.info("[VoiceInterview] Session {} using LLM provider (sentence stream): {}", session.getId(), provider);

            // 步骤 2：获取 ChatClient
            ChatClient chatClient = llmProviderRegistry.getVoiceChatClient(provider);

            // 步骤 3：初始化状态追踪变量
            StringBuilder raw = new StringBuilder();                    // 累积原始响应文本
            AtomicLong lastEmitNanos = new AtomicLong(System.nanoTime());  // 上次推送时间戳
            AtomicInteger lastEmitLength = new AtomicInteger(0);           // 上次推送时的文本长度
            AtomicInteger lastSentenceEnd = new AtomicInteger(0);          // 上次句子结束位置

            // 步骤 4：读取节流配置（防止频繁推送导致前端卡顿）
            int emitIntervalMs = Math.max(80, voiceInterviewProperties.getAiStreamPushIntervalMs());
            int minCharsDelta = Math.max(4, voiceInterviewProperties.getAiStreamMinCharsDelta());

            // 步骤 5：发起流式调用
            chatClient.prompt()
                .system(promptContext.systemPrompt())
                .user(promptContext.userPrompt())
                .stream()    // 启用流式模式
                .content()   // 只关注文本内容
                .doOnNext(token -> {
                    if (token == null || token.isEmpty()) {
                        return;  // 忽略空 Token
                    }
                    raw.append(token);  // 累积到缓冲区

                    // ① 句子边界检测：如果当前 Token 包含终止标点，触发 onSentence 回调
                    if (onSentence != null && hasTerminalSince(token)) {
                        String normalized = normalizeRealtimeText(raw.toString());
                        int currentEnd = normalized.length();
                        if (currentEnd > lastSentenceEnd.get()) {
                            // 提取自上次句子结束后的新内容
                            String sentence = normalized.substring(lastSentenceEnd.get()).trim();
                            if (!sentence.isEmpty()) {
                                onSentence.accept(sentence);  // 触发 TTS 合成
                            }
                            lastSentenceEnd.set(currentEnd);  // 更新句子结束位置
                        }
                    }

                    // ② 实时文本推送（用于前端字幕显示）
                    if (onToken == null) {
                        return;
                    }
                    long now = System.nanoTime();
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(now - lastEmitNanos.get());
                    int currentLength = raw.length();

                    // 节流判断：时间间隔 >= 阈值 AND 新增字符数 >= 阈值
                    boolean shouldEmit = elapsedMs >= emitIntervalMs && currentLength - lastEmitLength.get() >= minCharsDelta;
                    if (!shouldEmit) {
                        return;
                    }

                    String normalized = normalizeRealtimeText(raw.toString());
                    if (normalized.isBlank()) {
                        return;
                    }
                    onToken.accept(normalized);  // 推送实时文本
                    lastEmitNanos.set(now);      // 更新时间戳
                    lastEmitLength.set(normalized.length());  // 更新长度
                })
                .blockLast();  // 阻塞等待流式输出完成

            // 步骤 6：发送最后一段文本（可能不以终止标点结尾）
            if (onSentence != null) {
                String normalized = normalizeRealtimeText(raw.toString());
                if (normalized.length() > lastSentenceEnd.get()) {
                    String remaining = normalized.substring(lastSentenceEnd.get()).trim();
                    if (!remaining.isEmpty()) {
                        onSentence.accept(remaining);  // 推送剩余文本
                    }
                }
            }

            // 步骤 7：对完整响应进行语音优化
            String optimized = optimizeForVoice(raw.toString());
            if (onToken != null && !optimized.isBlank()) {
                onToken.accept(optimized);  // 推送最终优化结果
            }

            log.info("LLM sentence stream response for session {}: {}", session.getId(),
                optimized.substring(0, Math.min(100, optimized.length())));
            return optimized;
        } catch (Exception e) {
            log.error("LLM sentence stream error for session {}: {}", session.getId(), e.getMessage(), e);
            return mapLlmErrorToUserMessage(e);
        }
    }

    /**
     * 构建 Prompt 上下文（System Prompt + 用户输入）
     * <p>
     * 该方法负责：
     * <ol>
     *   <li>从数据库加载候选人简历文本（如果关联了 resumeId）</li>
     *   <li>调用 PromptService 生成包含技能要求和简历上下文的 System Prompt</li>
     *   <li>拼接历史对话记录和当前用户输入</li>
     * </ol>
     * </p>
     */
    private PromptContext buildPromptContext(String userInput, VoiceInterviewSessionEntity session, List<String> conversationHistory) {
        String resumeText = null;
        // 尝试加载简历文本（如果会话关联了简历）
        if (session.getResumeId() != null) {
            ResumeEntity resume = resumeRepository.findById(session.getResumeId()).orElse(null);
            if (resume != null) {
                resumeText = resume.getResumeText();
            }
        }

        // 生成 System Prompt（包含技能要求 + 简历上下文）
        String systemPrompt = promptService.generateSystemPromptWithContext(session.getSkillId(), resumeText);

        // 构建用户 Prompt（历史对话 + 当前输入）
        StringBuilder promptBuilder = new StringBuilder();
        if (conversationHistory != null && !conversationHistory.isEmpty()) {
            promptBuilder.append("【之前的对话】\n");
            for (String message : conversationHistory) {
                promptBuilder.append(message).append("\n");
            }
            promptBuilder.append("\n【当前对话】\n");
        }
        promptBuilder.append("用户：").append(userInput);
        return new PromptContext(systemPrompt, promptBuilder.toString());
    }

    /**
     * 将 LLM 异常映射为用户友好的中文提示
     * <p>
     * 根据异常消息中的关键词识别错误类型，并返回对应的提示信息。
     * </p>
     */
    private String mapLlmErrorToUserMessage(Exception e) {
        String errorMessage = e.getMessage();
        if (errorMessage != null) {
            // 认证失败（403 / ACCESS_DENIED / Authentication）
            if (errorMessage.contains("403") || errorMessage.contains("ACCESS_DENIED") ||
                errorMessage.contains("Authentication")) {
                return "AI 服务认证失败，请检查 API Key 配置";
            }
            // 超时错误
            else if (errorMessage.contains("timeout") || errorMessage.contains("Timeout")) {
                return "AI 服务响应超时，请稍后重试";
            }
            // 频率限制（429 / rate limit / quota）
            else if (errorMessage.contains("429") || errorMessage.contains("rate limit") ||
                       errorMessage.contains("quota")) {
                return "AI 服务调用频率超限，请稍后重试";
            }
            // 网络连接失败
            else if (errorMessage.contains("connection") || errorMessage.contains("network")) {
                return "AI 服务网络连接失败，请检查网络";
            }
        }
        // 默认错误提示
        return "抱歉，AI 服务暂时不可用，请稍后重试";
    }

    /**
     * 语音优化：清理文本格式并限制长度
     * <p>
     * 优化策略：
     * <ol>
     *   <li>去除 Markdown 标记（**、```、`、列表符号）</li>
     *   <li>合并多余空白字符</li>
     *   <li>如果超过最大长度，优先在终止标点处截断</li>
     *   <li>无法找到合适截断点时，添加省略号</li>
     * </ol>
     * </p>
     */
    private String optimizeForVoice(String content) {
        String normalized = normalizeRealtimeText(content);
        if (normalized.isBlank()) {
            return "请继续。";  // 空响应兜底
        }

        // 检查是否超过最大长度限制
        int maxChars = Math.max(80, voiceInterviewProperties.getAiQuestionMaxChars());
        if (normalized.length() <= maxChars) {
            return normalized;  // 未超限，直接返回
        }

        // 截断到最大长度
        String truncated = normalized.substring(0, maxChars);

        // 寻找最后一个终止标点（从后往前搜索）
        int lastTerminal = -1;
        for (int i = truncated.length() - 1; i >= 0; i--) {
            if (TERMINAL_PUNCTUATION.indexOf(truncated.charAt(i)) >= 0) {
                lastTerminal = i;
                break;
            }
        }

        // 如果在后半段找到终止标点，则在此处截断（保证句子完整性）
        if (lastTerminal >= maxChars / 2) {
            return truncated.substring(0, lastTerminal + 1);
        }

        // 否则添加省略号表示截断
        return truncated + "…";
    }

    /**
     * 规范化实时文本：去除 Markdown 标记和多余空白
     * <p>
     * 清理规则：
     * <ul>
     *   <li>去除粗体标记：**text** → text</li>
     *   <li>去除代码块标记：```code``` → code</li>
     *   <li>去除行内代码标记：`code` → code</li>
     *   <li>去除列表符号：- item / * item / + item → item</li>
     *   <li>合并连续空白字符为单个空格</li>
     * </ul>
     * </p>
     */
    private String normalizeRealtimeText(String content) {
        if (content == null || content.isBlank()) {
            return "";
        }
        return content
            .replace("**", "")                                    // 去除粗体标记
            .replace("```", "")                                   // 去除代码块标记
            .replace("`", "")                                     // 去除行内代码标记
            .replaceAll("(?m)^\\s*[-*+]\\s*", "")                 // 去除列表符号
            .replaceAll("\\s+", " ")                              // 合并空白字符
            .trim();
    }

    /**
     * 检查 Token 中是否包含终止标点
     * <p>
     * 用于判断当前 Token 是否标志着一个句子的结束
     * </p>
     */
    private boolean hasTerminalSince(String token) {
        for (int i = 0; i < token.length(); i++) {
            if (TERMINAL_PUNCTUATION.indexOf(token.charAt(i)) >= 0) {
                return true;  // 找到终止标点
            }
        }
        return false;
    }

    /**
     * Prompt 上下文记录（不可变数据载体）
     * <p>
     * 包含 System Prompt（角色设定 + 技能要求 + 简历上下文）
     * 和 User Prompt（历史对话 + 当前输入）
     * </p>
     */
    private record PromptContext(String systemPrompt, String userPrompt) {}
}
