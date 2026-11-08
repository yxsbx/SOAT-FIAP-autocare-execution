package br.com.autocarehub.execution;

import java.time.Instant;
import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("execution_jobs")
@CompoundIndex(name = "branch_queue", def = "{'branchId': 1, 'createdAt': 1, '_id': 1}")
record ExecutionJob(@Id String orderId, String branchId, Status status, Instant createdAt, List<StatusEvent> outbox) {

    enum Status {
        QUEUED,
        IN_DIAGNOSIS,
        REPAIR_RELEASED,
        IN_REPAIR,
        FINISHED
    }

    record StatusEvent(String eventId, String orderId, String branchId, Status status) {}
}
