package interview.guide.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Langfuse 可观测性配置（经 OpenTelemetry OTLP 上报）。
 */
@ConfigurationProperties(prefix = "app.langfuse")
public class LangfuseProperties {

  /** 是否启用 Langfuse OTLP 上报 */
  private boolean enabled = false;

  /** Langfuse Public Key（pk-lf-...） */
  private String publicKey = "";

  /** Langfuse Secret Key（sk-lf-...） */
  private String secretKey = "";

  /**
   * OTLP traces 完整 endpoint（需含 /v1/traces）。
   * 例：https://cloud.langfuse.com/api/public/otel/v1/traces
   */
  private String otlpEndpoint = "https://cloud.langfuse.com/api/public/otel/v1/traces";

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public void setPublicKey(String publicKey) {
    this.publicKey = publicKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getOtlpEndpoint() {
    return otlpEndpoint;
  }

  public void setOtlpEndpoint(String otlpEndpoint) {
    this.otlpEndpoint = otlpEndpoint;
  }

  public boolean hasCredentials() {
    return publicKey != null && !publicKey.isBlank()
        && secretKey != null && !secretKey.isBlank();
  }
}
