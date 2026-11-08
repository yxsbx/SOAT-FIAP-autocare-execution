package br.com.autocarehub.execution;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import br.com.autocarehub.execution.ExecutionJob.Status;
import br.com.autocarehub.execution.ExecutionJob.StatusEvent;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

@Service
class ExecutionQueue {
    static final String COMMANDS = "autocare.execution.command.v1";
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionQueue.class);
    private final MongoTemplate mongo;
    private final ObjectMapper json;

    ExecutionQueue(MongoTemplate mongo, ObjectMapper json) {
        this.mongo = mongo;
        this.json = json;
    }

    @RabbitListener(queues = COMMANDS)
    public void consume(String payload) {
        var command = json.readValue(payload, Command.class);
        switch (command.action()) {
            case OPEN -> open(command.branchId(), command.orderId());
            case RELEASE_REPAIR -> releaseRepair(command);
        }
    }

    private void releaseRepair(Command command) {
        try {
            advance(command.branchId(), command.orderId(), Status.REPAIR_RELEASED);
        } catch (ResponseStatusException failure) {
            if (failure.getStatusCode() != HttpStatus.NOT_FOUND || command.budgetId() == null) {
                throw failure;
            }
            recordMissingJob(command);
        }
    }

    private void recordMissingJob(Command command) {
        String orderId = command.orderId().toString();
        String branchId = command.branchId().toString();
        var event = new StatusEvent(
                UUID.randomUUID().toString(),
                orderId,
                branchId,
                Status.RELEASE_FAILED,
                command.budgetId().toString());
        // A tombstone and its event share one atomic write, so a late OPEN cannot reopen the repair.
        var job = Objects.requireNonNull(mongo.findAndModify(
                query(where("_id").is(orderId)),
                new Update()
                        .setOnInsert("branchId", branchId)
                        .setOnInsert("status", Status.RELEASE_FAILED)
                        .setOnInsert("createdAt", Instant.now())
                        .setOnInsert("outbox", List.of(event)),
                FindAndModifyOptions.options().upsert(true).returnNew(true),
                ExecutionJob.class));
        if (job.status() != Status.RELEASE_FAILED || !branchId.equals(job.branchId())) {
            advance(command.branchId(), command.orderId(), Status.REPAIR_RELEASED);
        }
    }

    ExecutionJob open(UUID branchId, UUID orderId) {
        var job = mongo.findAndModify(
                query(where("_id").is(orderId.toString())),
                new Update()
                        .setOnInsert("branchId", branchId.toString())
                        .setOnInsert("status", Status.QUEUED)
                        .setOnInsert("createdAt", Instant.now())
                        .setOnInsert("outbox", List.of()),
                FindAndModifyOptions.options().upsert(true).returnNew(true),
                ExecutionJob.class);
        if (!branchId.toString().equals(Objects.requireNonNull(job).branchId())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Order is assigned to another branch");
        }
        return job;
    }

    ExecutionJob advance(UUID branchId, UUID orderId, Status next) {
        Status previous =
                switch (next) {
                    case IN_DIAGNOSIS -> Status.QUEUED;
                    case REPAIR_RELEASED -> Status.IN_DIAGNOSIS;
                    case IN_REPAIR -> Status.REPAIR_RELEASED;
                    case FINISHED -> Status.IN_REPAIR;
                    case QUEUED, RELEASE_FAILED ->
                        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot reset execution");
                };
        var key = where("_id").is(orderId.toString()).and("branchId").is(branchId.toString());
        var event = new StatusEvent(UUID.randomUUID().toString(), orderId.toString(), branchId.toString(), next);
        var changed = mongo.findAndModify(
                query(key.and("status").is(previous)),
                new Update().set("status", next).push("outbox", event),
                FindAndModifyOptions.options().returnNew(true),
                ExecutionJob.class);
        if (changed != null) {
            LOG.info("execution_stage_changed orderId={} branchId={} status={}", orderId, branchId, next);
            return changed;
        }
        var current = mongo.findOne(
                query(where("_id").is(orderId.toString()).and("branchId").is(branchId.toString())), ExecutionJob.class);
        if (current == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        if (current.status() == Status.RELEASE_FAILED && next != Status.REPAIR_RELEASED) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Repair was closed after a release failure");
        }
        if (current.status().ordinal() < next.ordinal()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Previous execution step is required");
        }
        return current;
    }

    List<ExecutionJob> active(UUID branchId) {
        return mongo.find(
                query(where("branchId")
                                .is(branchId.toString())
                                .and("status")
                                .nin(Status.FINISHED, Status.RELEASE_FAILED))
                        .with(Sort.by("createdAt", "_id"))
                        .limit(100),
                ExecutionJob.class);
    }

    enum Action {
        OPEN,
        RELEASE_REPAIR
    }

    record Command(UUID branchId, UUID orderId, Action action, UUID budgetId) {
        Command(UUID branchId, UUID orderId, Action action) {
            this(branchId, orderId, action, null);
        }

        Command {
            Objects.requireNonNull(branchId);
            Objects.requireNonNull(orderId);
            Objects.requireNonNull(action);
        }
    }
}
