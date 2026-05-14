package com.acme.coordination.api.error;

import java.util.LinkedHashMap;
import java.util.Map;

/** Конфликт при создании ресурса (HTTP 409): координатор / job / worker уже существует. */
public final class ResourceConflictException extends RuntimeException {

  private final String code;
  private final Map<String, Object> details;

  public ResourceConflictException(String code, String message, Map<String, Object> details) {
    super(message);
    this.code = code;
    this.details = details == null ? Map.of() : Map.copyOf(details);
  }

  public String getCode() {
    return code;
  }

  public Map<String, Object> getDetails() {
    return details;
  }

  public Map<String, Object> responseBody() {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("error", code);
    m.put("message", getMessage());
    m.putAll(details);
    return m;
  }
}
