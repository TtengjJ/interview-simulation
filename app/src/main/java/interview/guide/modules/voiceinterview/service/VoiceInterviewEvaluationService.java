package interview.guide.modules.voiceinterview.service;

import interview.guide.common.ai.LlmProviderRegistry;
import interview.guide.common.evaluation.EvaluationReport;
import interview.guide.common.evaluation.QaRecord;
import interview.guide.common.evaluation.UnifiedEvaluationService;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.modules.interview.skill.InterviewSkillService;
import interview.guide.modules.voiceinterview.dto.VoiceEvaluationDetailDTO;
import interview.guide.modules.voiceinterview.dto.VoiceEvaluationDetailDTO.AnswerDetail;
import interview.guide.modules.voiceinterview.model.VoiceInterviewEvaluationEntity;
import interview.guide.modules.voiceinterview.model.VoiceInterviewMessageEntity;
import interview.guide.modules.voiceinterview.model.VoiceInterviewSessionEntity;
import interview.guide.modules.voiceinterview.repository.VoiceInterviewEvaluationRepository;
import interview.guide.modules.voiceinterview.repository.VoiceInterviewMessageRepository;
import interview.guide.modules.voiceinterview.repository.VoiceInterviewSessionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 语音面试评估服务
 * 复用 UnifiedEvaluationService 的分批评估 + 结构化输出 + 降级兜底
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class VoiceInterviewEvaluationService {

    private final UnifiedEvaluationService unifiedEvaluationService;
    private final LlmProviderRegistry llmProviderRegistry;
    private final VoiceInterviewEvaluationRepository evaluationRepository;
    private final VoiceInterviewMessageRepository messageRepository;
    private final VoiceInterviewSessionRepository sessionRepository;
    private final ObjectMapper objectMapper;
    private final InterviewSkillService skillService;

    /**
     * 生成语音面试评估报告（由异步消费者调用）
     * <p>
     * 该方法执行以下操作：
     * <ol>
     *   <li>从数据库加载会话实体和所有对话消息</li>
     *   <li>如果无对话记录，生成空评估报告并返回</li>
     *   <li>将对话消息转换为 {@code QaRecord} 列表（问题-回答对）</li>
     *   <li>根据会话配置的 LLM Provider 获取 ChatClient</li>
     *   <li>调用 {@code UnifiedEvaluationService} 执行批量评估</li>
     *   <li>将评估结果保存到数据库（JSON 格式存储）</li>
     * </ol>
     * </p>
     *
     * <b>事务管理策略</b>：
     * <ul>
     *   <li><b>LLM 调用在事务外</b>：避免长时间占用数据库连接（LLM 响应可能需要 10-30 秒）</li>
     *   <li><b>DB 写入在事务内</b>：通过 {@code saveEvaluationTransactional} 方法保证原子性</li>
     *   <li><b>异常处理</b>：捕获所有异常并记录日志，抛出业务异常供上层重试</li>
     * </ul>
     *
     * <b>性能优化</b>：
     * <ul>
     *   <li>分批评估：每次最多处理 8 个问题，避免单次 Prompt 过长导致超时</li>
     *   <li>结构化输出：直接生成 JSON，减少后处理开销</li>
     *   <li>降级兜底：如果结构化输出失败，使用传统文本解析作为备选方案</li>
     * </ul>
     *
     * @param sessionId 会话 ID
     * @throws BusinessException 如果评估过程中发生不可恢复的错误
     */
    public void generateEvaluation(Long sessionId) {
        try {
            log.info("开始生成语音面试评估: sessionId={}", sessionId);

            VoiceInterviewSessionEntity session = getSession(sessionId);
            List<VoiceInterviewMessageEntity> messages = messageRepository
                .findBySessionIdOrderBySequenceNumAsc(sessionId);

            // 如果没有有效对话记录，生成空评估报告作为兜底
            if (messages.isEmpty()) {
                log.warn("语音面试会话无对话记录，生成空评估结果: sessionId={}", sessionId);
                saveEmptyEvaluationTransactional(sessionId, session);
                return;
            }

            // 将数据库消息实体转换为评估服务所需的 Q&A 记录列表
            List<QaRecord> qaRecords = buildQaRecords(messages);

            String provider = session.getLlmProvider();
            ChatClient chatClient = llmProviderRegistry.getChatClientOrDefault(provider);

            String sessionIdStr = String.valueOf(sessionId);
            String referenceContext = skillService.buildEvaluationReferenceSectionSafe(session.getSkillId());

            // 调用统一评估服务执行 LLM 批量评估（此步骤耗时较长，在事务外执行）
            EvaluationReport report = unifiedEvaluationService.evaluate(
                chatClient, sessionIdStr, qaRecords, null, referenceContext);

            // 将评估结果序列化并持久化到数据库（在独立事务中执行）
            saveEvaluationTransactional(sessionId, session, report);

        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("生成语音面试评估失败: sessionId={}", sessionId, e);
            throw new BusinessException(ErrorCode.VOICE_EVALUATION_FAILED,
                "生成评估失败: " + e.getMessage());
        }
    }

    public VoiceEvaluationDetailDTO getEvaluation(Long sessionId) {
        VoiceInterviewEvaluationEntity evaluation = evaluationRepository.findBySessionId(sessionId)
            .orElseThrow(() -> new BusinessException(ErrorCode.VOICE_EVALUATION_NOT_FOUND,
                "评估结果不存在: " + sessionId));

        return buildDetailDTO(evaluation);
    }

    private List<QaRecord> buildQaRecords(List<VoiceInterviewMessageEntity> messages) {
        List<QaRecord> records = new ArrayList<>();
        int index = 0;

        for (VoiceInterviewMessageEntity msg : messages) {
            String aiText = msg.getAiGeneratedText();
            String userText = msg.getUserRecognizedText();

            if ((aiText != null && !aiText.isBlank()) || (userText != null && !userText.isBlank())) {
                records.add(new QaRecord(
                    index,
                    aiText != null ? aiText.trim() : "",
                    inferCategory(aiText),
                    userText != null && !userText.isBlank() ? userText.trim() : null
                ));
                index++;
            }
        }

        return records;
    }

    private String inferCategory(String aiText) {
        if (aiText == null) return "综合";
        if (aiText.contains("项目") || aiText.contains("实习") || aiText.contains("工作经历")) return "项目深挖";
        if (aiText.contains("自我介绍") || aiText.contains("介绍一下自己")) return "自我介绍";
        if (aiText.contains("职业规划") || aiText.contains("为什么") || aiText.contains("优缺点")) return "HR问题";
        return "技术问题";
    }

    @Transactional
    public void saveEvaluationTransactional(Long sessionId, VoiceInterviewSessionEntity session,
                                 EvaluationReport report) {
        try {
            List<EvaluationReport.QuestionEvaluation> questionItems = report.questionDetails();
            List<EvaluationReport.ReferenceAnswer> refAnswerItems = report.referenceAnswers();

            VoiceInterviewEvaluationEntity entity = VoiceInterviewEvaluationEntity.builder()
                .sessionId(sessionId)
                .overallScore(report.overallScore())
                .overallFeedback(report.overallFeedback())
                .questionEvaluationsJson(objectMapper.writeValueAsString(questionItems))
                .strengthsJson(objectMapper.writeValueAsString(report.strengths()))
                .improvementsJson(objectMapper.writeValueAsString(report.improvements()))
                .referenceAnswersJson(objectMapper.writeValueAsString(refAnswerItems))
                .interviewerRole(session.getRoleType())
                .interviewDate(session.getStartTime())
                .build();

            evaluationRepository.save(entity);
            log.info("评估结果已保存: sessionId={}, score={}", sessionId, entity.getOverallScore());
        } catch (Exception e) {
            log.error("保存评估结果失败: sessionId={}", sessionId, e);
            throw new BusinessException(ErrorCode.VOICE_EVALUATION_FAILED,
                "保存评估失败: " + e.getMessage());
        }
    }

    @Transactional
    public void saveEmptyEvaluationTransactional(Long sessionId, VoiceInterviewSessionEntity session) {
        try {
            VoiceInterviewEvaluationEntity entity = evaluationRepository.findBySessionId(sessionId)
                .orElseGet(() -> VoiceInterviewEvaluationEntity.builder().sessionId(sessionId).build());

            entity.setOverallScore(0);
            entity.setOverallFeedback("本次语音面试未形成有效对话记录，暂无可评估内容。");
            entity.setQuestionEvaluationsJson("[]");
            entity.setStrengthsJson("[]");
            entity.setImprovementsJson("[\"请先完成至少一轮有效问答后再生成评估。\"]");
            entity.setReferenceAnswersJson("[]");
            entity.setInterviewerRole(session.getRoleType());
            entity.setInterviewDate(session.getStartTime());

            evaluationRepository.save(entity);
            log.info("空评估结果已保存: sessionId={}", sessionId);
        } catch (Exception e) {
            log.error("保存空评估结果失败: sessionId={}", sessionId, e);
            throw new BusinessException(ErrorCode.VOICE_EVALUATION_FAILED,
                "保存空评估失败: " + e.getMessage());
        }
    }

    /**
     * 构建评估详情 DTO（用于前端展示）
     * <p>
     * 将数据库中存储的 JSON 字符串反序列化为前端所需的 DTO 对象。
     * </p>
     *
     * <b>数据处理流程</b>：
     * <ol>
     *   <li>从数据库加载 {@code VoiceInterviewEvaluationEntity}</li>
     *   <li>反序列化 JSON 字段为 Java 对象（逐题评估、优势、改进建议、参考答案）</li>
     *   <li>将参考答案按问题索引建立 Map，方便快速查找</li>
     *   <li>组装 {@code AnswerDetail} 列表（包含问题、回答、评分、反馈、参考答案）</li>
     *   <li>构建最终的 {@code VoiceEvaluationDetailDTO} 并返回</li>
     * </ol>
     * </p>
     *
     * <b>异常处理</b>：
     * <ul>
     *   <li>如果 JSON 反序列化失败，记录日志并抛出业务异常</li>
     *   <li>常见原因：数据库中的 JSON 格式损坏或版本不兼容</li>
     * </ul>
     *
     * @param entity 数据库中的评估实体
     * @return 评估详情 DTO
     * @throws BusinessException 如果反序列化失败
     */
    private VoiceEvaluationDetailDTO buildDetailDTO(VoiceInterviewEvaluationEntity entity) {
        try {
            // 反序列化逐题评估结果
            List<EvaluationReport.QuestionEvaluation> questionItems = objectMapper.readValue(
                entity.getQuestionEvaluationsJson(),
                new TypeReference<List<EvaluationReport.QuestionEvaluation>>() {}
            );

            // 反序列化优势和改进建议列表
            List<String> strengths = objectMapper.readValue(
                entity.getStrengthsJson(),
                new TypeReference<List<String>>() {}
            );

            List<String> improvements = objectMapper.readValue(
                entity.getImprovementsJson(),
                new TypeReference<List<String>>() {}
            );

            // 反序列化参考答案列表
            List<EvaluationReport.ReferenceAnswer> refAnswers = objectMapper.readValue(
                entity.getReferenceAnswersJson(),
                new TypeReference<List<EvaluationReport.ReferenceAnswer>>() {}
            );

            // 将参考答案转换为 Map，以问题索引为 Key，便于后续快速匹配
            Map<Integer, EvaluationReport.ReferenceAnswer> refMap = refAnswers.stream()
                .collect(Collectors.toMap(
                    EvaluationReport.ReferenceAnswer::questionIndex, r -> r, (a, b) -> a));

            // 组装每道题的详细评估信息，并将对应的参考答案合并进去
            List<AnswerDetail> answers = new ArrayList<>();
            for (EvaluationReport.QuestionEvaluation q : questionItems) {
                EvaluationReport.ReferenceAnswer ref = refMap.get(q.questionIndex());
                answers.add(AnswerDetail.builder()
                    .questionIndex(q.questionIndex())
                    .question(q.question())
                    .category(q.category())
                    .userAnswer(q.userAnswer())
                    .score(q.score())
                    .feedback(q.feedback())
                    .referenceAnswer(ref != null ? ref.referenceAnswer() : null)
                    .keyPoints(ref != null ? ref.keyPoints() : null)
                    .build());
            }

            return VoiceEvaluationDetailDTO.builder()
                .sessionId(entity.getSessionId())
                .totalQuestions(answers.size())
                .overallScore(entity.getOverallScore())
                .overallFeedback(entity.getOverallFeedback())
                .strengths(strengths)
                .improvements(improvements)
                .answers(answers)
                .build();

        } catch (Exception e) {
            log.error("构建评估详情失败: sessionId={}", entity.getSessionId(), e);
            throw new BusinessException(ErrorCode.VOICE_EVALUATION_FAILED,
                "构建评估结果失败: " + e.getMessage());
        }
    }

    private VoiceInterviewSessionEntity getSession(Long sessionId) {
        return sessionRepository.findById(sessionId)
            .orElseThrow(() -> new BusinessException(ErrorCode.VOICE_SESSION_NOT_FOUND,
                "语音面试会话不存在: " + sessionId));
    }
}
