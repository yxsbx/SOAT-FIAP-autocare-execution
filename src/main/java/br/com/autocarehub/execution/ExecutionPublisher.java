package br.com.autocarehub.execution;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import br.com.autocarehub.telemetry.MessageTrace;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
class ExecutionPublisher {
    static final String RESULTS = "autocare.os.execution-updated.v1";
    private final MongoTemplate mongo;
    private final RabbitTemplate rabbit;
    private final ObjectMapper json;

    ExecutionPublisher(MongoTemplate mongo, RabbitTemplate rabbit, ObjectMapper json) {
        this.mongo = mongo;
        this.rabbit = rabbit;
        this.json = json;
    }

    @Scheduled(fixedDelayString = "${execution.outbox.delay-ms:1000}")
    public void publish() throws Exception {
        var job = mongo.findOne(
                query(where("outbox.0").exists(true)).with(Sort.by("createdAt", "_id")), ExecutionJob.class);
        if (job == null) {
            return;
        }
        var event = job.outbox().getFirst();
        var confirmation = new CorrelationData(UUID.randomUUID().toString());
        try (var scope = MessageTrace.restore(event.traceContext())) {
            rabbit.convertAndSend(
                    "",
                    RESULTS,
                    json.writeValueAsString(event),
                    message -> {
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        return message;
                    },
                    confirmation);
        }
        if (!confirmation.getFuture().get(5, TimeUnit.SECONDS).ack() || confirmation.getReturned() != null) {
            throw new IllegalStateException("RabbitMQ did not route the execution status");
        }
        mongo.updateFirst(
                query(where("_id").is(job.orderId())),
                new Update()
                        .pull(
                                "outbox",
                                query(where("eventId").is(event.eventId())).getQueryObject()),
                ExecutionJob.class);
    }
}
