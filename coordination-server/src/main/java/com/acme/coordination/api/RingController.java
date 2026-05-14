package com.acme.coordination.api;

import com.acme.coordination.api.dto.ComputeRangesRequest;
import com.acme.coordination.hash.ConsistentHashService;
import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/rings")
public class RingController {

  private final ConsistentHashService ring;

  public RingController(ConsistentHashService ring) {
    this.ring = ring;
  }

  @PostMapping("/assign")
  public Object assign(@Valid @RequestBody ComputeRangesRequest req) {
    int vn = req.virtualNodes() == null || req.virtualNodes() < 1 ? 0 : req.virtualNodes();
    var res = ring.computeRanges(req.workerIds(), vn);
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("ranges", res.ranges());
    if (Boolean.TRUE.equals(req.withPos())) {
      body.put("points", res.points());
    }
    return body;
  }
}

