package interview.guide.common.ai;

import java.util.List;
import java.util.stream.Collectors;

import io.micrometer.common.KeyValue;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationFilter;
import org.springframework.ai.chat.observation.ChatModelObservationContext;
import org.springframework.ai.content.Content;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * 将 Spring AI ChatModel 的 prompt/completion 写入 OTel 高基数属性，
 * 供 Langfuse 在 Generation 上展示 input/output。
 *
 * <p>{@code spring.ai.chat.observations.log-prompt/completion} 仅写入 Observation
 * Context；本 Filter 负责映射为：
 * <ul>
 *   <li>{@code gen_ai.prompt} / {@code gen_ai.completion}（OTel GenAI 约定）</li>
 *   <li>{@code langfuse.observation.input} / {@code langfuse.observation.output}</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(prefix = "app.langfuse", name = "enabled", havingValue = "true")
public class ChatModelCompletionContentObservationFilter implements ObservationFilter {

  @Override
  public Observation.Context map(Observation.Context context) {
    if (!(context instanceof ChatModelObservationContext chatContext)) {
      return context;
    }

    String prompt = String.join("\n", processPrompts(chatContext));
    String completion = String.join("\n", processCompletion(chatContext));

    chatContext.addHighCardinalityKeyValue(KeyValue.of("gen_ai.prompt", prompt));
    chatContext.addHighCardinalityKeyValue(KeyValue.of("gen_ai.completion", completion));
    // Langfuse 原生字段，UI 上 Input/Output 更稳定
    chatContext.addHighCardinalityKeyValue(KeyValue.of("langfuse.observation.input", prompt));
    chatContext.addHighCardinalityKeyValue(KeyValue.of("langfuse.observation.output", completion));

    return chatContext;
  }

  private List<String> processPrompts(ChatModelObservationContext context) {
    if (context.getRequest() == null
        || CollectionUtils.isEmpty(context.getRequest().getInstructions())) {
      return List.of();
    }
    return context.getRequest().getInstructions().stream()
        .map(Content::getText)
        .filter(StringUtils::hasText)
        .collect(Collectors.toList());
  }

  private List<String> processCompletion(ChatModelObservationContext context) {
    if (context.getResponse() == null
        || CollectionUtils.isEmpty(context.getResponse().getResults())) {
      return List.of();
    }
    return context.getResponse().getResults().stream()
        .filter(generation -> generation.getOutput() != null
            && StringUtils.hasText(generation.getOutput().getText()))
        .map(generation -> generation.getOutput().getText())
        .collect(Collectors.toList());
  }
}
