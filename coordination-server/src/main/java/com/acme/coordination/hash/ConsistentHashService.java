package com.acme.coordination.hash;

import com.acme.coordination.ring.ConsistentHashRing;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * Spring bean delegating to {@link ConsistentHashRing} (shared implementation lives in {@code coordination-hash}).
 */
@Service
public class ConsistentHashService {

  private final ConsistentHashRing ring = new ConsistentHashRing();

  public ConsistentHashRing.ComputeResult computeRanges(List<String> workerIds, int virtualNodes) {
    return ring.computeRanges(workerIds, virtualNodes);
  }

  public String lookupWorker(List<String> workerIds, int virtualNodes, String key) {
    return ring.lookupWorker(workerIds, virtualNodes, key);
  }
}
