package com.acme.coordination.sdk.session;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

final class JsonLeaseResponses {

  private JsonLeaseResponses() {}

  static String requireToken(JsonNode body, String what) {
    if (body == null || body.isNull()) {
      throw new LeaseNotAcquiredException(what + ": empty response");
    }
    JsonNode t = body.get("token");
    if (t == null || !t.isTextual() || t.asText().isBlank()) {
      throw new LeaseNotAcquiredException(what + ": missing token in " + body);
    }
    return t.asText();
  }

  static boolean acquiredFlag(JsonNode body) {
    if (body == null || body.isNull()) {
      return false;
    }
    if (body.has("acquired")) {
      return body.get("acquired").asBoolean(false);
    }
    if (body.has("created")) {
      return body.get("created").asBoolean(false);
    }
    // Some success bodies only return token
    return body.has("token") && body.get("token").isTextual();
  }

  static long fenceEpoch(JsonNode body) {
    if (body == null || !body.has("fenceEpoch")) {
      return 0L;
    }
    return body.get("fenceEpoch").asLong(0L);
  }

  static void fireOnce(AtomicBoolean fired, AtomicReference<Consumer<LeaseLostEvent>> hook, LeaseLostEvent ev) {
    if (!fired.compareAndSet(false, true)) {
      return;
    }
    Consumer<LeaseLostEvent> c = hook.get();
    if (c != null) {
      try {
        c.accept(ev);
      } catch (RuntimeException ignored) {
        // caller callbacks must not break the renewer
      }
    }
  }
}
