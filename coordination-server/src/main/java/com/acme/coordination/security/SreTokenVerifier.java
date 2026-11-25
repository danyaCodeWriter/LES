package com.acme.coordination.security;

import com.acme.coordination.api.error.ParticipantException;
import com.acme.coordination.config.CoordinationProperties;
import com.acme.coordination.service.CoordinatorReadVerificationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;

/**
 * Статический {@code coordination.sre.stream-listen-token}: SSE workers/jobs/keys без токена лидера coordinator.
 */
@Component
public class SreTokenVerifier {

  private final CoordinationProperties props;

  public SreTokenVerifier(CoordinationProperties props) {
    this.props = props;
  }

  public boolean matches(String secret) {
    String t = props.sre().streamListenToken();
    return t != null && !t.isBlank() && secret != null && secret.equals(t);
  }

  /** 401 если токен не передан / SRE выключен; 403 если значение не совпало. */
  public void assertSre(String secret) {
    String configured = props.sre().streamListenToken();
    if (configured == null || configured.isBlank()) {
      throw new ParticipantException(
          HttpStatus.UNAUTHORIZED,
          "SRE_TOKEN_REQUIRED",
          "coordination.sre.stream-listen-token is not configured; cannot authorize this operation.");
    }
    if (secret == null || secret.isBlank()) {
      throw new ParticipantException(
          HttpStatus.UNAUTHORIZED,
          "SRE_TOKEN_REQUIRED",
          "Send Authorization: Bearer <coordination.sre.stream-listen-token> or X-Coordinator-Token.");
    }
    if (!configured.equals(secret)) {
      throw new ParticipantException(
          HttpStatus.FORBIDDEN, "SRE_TOKEN_FORBIDDEN", "SRE token does not match coordination.sre.stream-listen-token.");
    }
  }

  public void assertSreFromRequest(ServerHttpRequest request) {
    assertSre(CoordinatorReadVerificationService.extractCoordReadSecret(request));
  }

  public void assertSreFromGrpc(String accessCredential) {
    assertSre(CoordinatorReadVerificationService.grpcReadSecret(accessCredential));
  }
}
