package com.acme.coordination.sdk;

import com.acme.coordination.ring.ConsistentHashRing;
import com.acme.coordination.ring.RingAssignment;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Parses {@code POST /v1/rings/assign} JSON into {@link RingAssignment} for {@link RingAssignment#workerFor(Object)}. */
public final class RingAssignments {
  private RingAssignments() {}

  /**
   * Expects top-level {@code ranges} object: workerId → array of {@code { "start": number, "end": number }} (same shape
   * as coordination-server). Ignores {@code points} if present.
   */
  public static RingAssignment fromAssignResponse(JsonNode root) {
    if (root == null || !root.isObject()) {
      throw new IllegalArgumentException("root must be a JSON object");
    }
    JsonNode rangesNode = root.get("ranges");
    if (rangesNode == null || !rangesNode.isObject()) {
      throw new IllegalArgumentException("Expected property 'ranges' (object) on ring assign response");
    }
    Map<String, List<ConsistentHashRing.Range>> map = new LinkedHashMap<>();
    rangesNode.fields().forEachRemaining(entry -> {
      String workerId = entry.getKey();
      JsonNode arr = entry.getValue();
      if (!arr.isArray()) {
        throw new IllegalArgumentException("ranges['" + workerId + "'] must be an array");
      }
      List<ConsistentHashRing.Range> list = new ArrayList<>();
      for (JsonNode el : arr) {
        if (!el.isObject()) {
          throw new IllegalArgumentException("each range must be an object with start/end");
        }
        list.add(new ConsistentHashRing.Range(el.get("start").asLong(), el.get("end").asLong()));
      }
      map.put(workerId, list);
    });
    return RingAssignment.of(map);
  }
}
