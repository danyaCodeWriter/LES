package com.acme.coordination.api;

import com.acme.coordination.model.CoordinatorRecord;
import com.acme.coordination.model.JobRecord;
import com.acme.coordination.model.WorkerRecord;
import com.acme.coordination.service.CoordinatorService;
import com.acme.coordination.service.JobLockService;
import com.acme.coordination.service.WorkerRegistryService;
import java.util.List;

/** Strip lease secrets from read responses (caller is already authorized as coordinator). */
public final class ReadResponseSanitizer {

  private ReadResponseSanitizer() {}

  public static CoordinatorService.CoordinatorState strip(CoordinatorService.CoordinatorState st) {
    CoordinatorRecord r = st.record();
    CoordinatorRecord redacted =
        new CoordinatorRecord(r.coordinatorId(), r.instanceId(), null, r.metadata(), r.issuedAt(), r.fenceEpoch());
    return new CoordinatorService.CoordinatorState(redacted, st.leaseId(), st.modRevision());
  }

  public static WorkerRegistryService.WorkerState strip(WorkerRegistryService.WorkerState st) {
    WorkerRecord r = st.record();
    WorkerRecord redacted = new WorkerRecord(r.workerId(), r.instanceId(), null, r.metadata(), r.issuedAt());
    return new WorkerRegistryService.WorkerState(redacted, st.leaseId(), st.modRevision());
  }

  public static List<WorkerRegistryService.WorkerState> stripWorkers(List<WorkerRegistryService.WorkerState> list) {
    return list.stream().map(ReadResponseSanitizer::strip).toList();
  }

  public static JobLockService.JobState strip(JobLockService.JobState st) {
    JobRecord r = st.record();
    JobRecord redacted = new JobRecord(r.jobId(), r.holder(), null, r.metadata(), r.issuedAt());
    return new JobLockService.JobState(redacted, st.leaseId(), st.modRevision());
  }
}
