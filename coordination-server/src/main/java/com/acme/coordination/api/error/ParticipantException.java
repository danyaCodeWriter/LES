package com.acme.coordination.api.error;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;

/** Job acquire: missing/invalid participant proof (HTTP 400/403). */
public final class ParticipantException extends RuntimeException {

  private final String code;
  private final HttpStatus status;

  public ParticipantException(HttpStatus status, String code, String message) {
    super(message);
    this.status = status;
    this.code = code;
  }

  public HttpStatus getStatus() {
    return status;
  }

  public String getCode() {
    return code;
  }

  public Map<String, Object> responseBody() {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("error", code);
    m.put("message", getMessage());
    return m;
  }
}
