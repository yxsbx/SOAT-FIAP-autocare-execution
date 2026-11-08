package br.com.autocarehub.execution;

import br.com.autocarehub.telemetry.MessageTrace;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
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
        FINISHED,
        RELEASE_FAILED
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record StatusEvent(
            String eventId,
            String orderId,
            String branchId,
            Status status,
            String budgetId,
            @JsonIgnore String traceContext) {
        @PersistenceCreator
        StatusEvent {}

        StatusEvent(String eventId, String orderId, String branchId, Status status) {
            this(eventId, orderId, branchId, status, null);
        }

        StatusEvent(String eventId, String orderId, String branchId, Status status, String budgetId) {
            this(eventId, orderId, branchId, status, budgetId, MessageTrace.capture());
        }
    }
}
