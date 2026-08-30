package interview.guide.common.config;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.micrometer.tracing.opentelemetry.autoconfigure.otlp.OtlpHttpSpanExporterBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 为 Langfuse OTLP 端点注入 Basic Auth 与 ingestion 版本头。
 */
@Configuration
@EnableConfigurationProperties(LangfuseProperties.class)
@ConditionalOnProperty(prefix = "app.langfuse", name = "enabled", havingValue = "true")
@Slf4j
public class LangfuseOtlpConfiguration {

  @Bean
  OtlpHttpSpanExporterBuilderCustomizer langfuseAuthCustomizer(LangfuseProperties properties) {
    return builder -> {
      if (!properties.hasCredentials()) {
        log.warn("Langfuse enabled but publicKey/secretKey missing; OTLP auth header not set");
        return;
      }
      String credential = properties.getPublicKey() + ":" + properties.getSecretKey();
      String encoded = Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
      builder.setHeaders(() -> Map.of(
          "Authorization", "Basic " + encoded,
          "x-langfuse-ingestion-version", "4"
      ));
      log.info("Langfuse OTLP auth configured: endpoint={}", properties.getOtlpEndpoint());
    };
  }
}
