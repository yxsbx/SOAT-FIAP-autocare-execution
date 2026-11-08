package br.com.autocarehub.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import br.com.autocarehub.execution.ExecutionJob.Status;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import tools.jackson.databind.json.JsonMapper;

class ExecutionFlowTest {
    private final JsonMapper json = JsonMapper.builder().build();
    private final MongoTemplate mongo = mock(MongoTemplate.class);
    private final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    private final UUID branchId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();
    private final ExecutionQueue queue = new ExecutionQueue(mongo, json);

    @Test
    void opensOrdersByCommandAndKeepsTheExistingBranch() {
        var job = job(Status.QUEUED);
        when(mongo.findAndModify(
                        any(Query.class), any(Update.class), any(FindAndModifyOptions.class), eq(ExecutionJob.class)))
                .thenReturn(job);
        queue.consume(
                json.writeValueAsString(new ExecutionQueue.Command(branchId, orderId, ExecutionQueue.Action.OPEN)));
        assertThat(queue.open(branchId, orderId)).isSameAs(job);
        assertThatThrownBy(() -> queue.open(UUID.randomUUID(), orderId)).hasMessageContaining("409");
    }

    @Test
    void authorizesAndAdvancesThroughDiagnosisAndRepair() {
        when(mongo.findAndModify(
                        any(Query.class), any(Update.class), any(FindAndModifyOptions.class), eq(ExecutionJob.class)))
                .thenReturn(
                        job(Status.IN_DIAGNOSIS),
                        job(Status.REPAIR_RELEASED),
                        job(Status.IN_REPAIR),
                        job(Status.FINISHED));
        var controller = new ExecutionController(queue, "internal");
        assertThatThrownBy(() -> controller.list(branchId, "wrong")).hasMessageContaining("401");
        assertThatThrownBy(() -> new ExecutionController(queue, "").list(branchId, ""))
                .hasMessageContaining("401");
        when(mongo.find(any(Query.class), eq(ExecutionJob.class))).thenReturn(List.of(job(Status.QUEUED)));
        assertThat(controller.list(branchId, "internal"))
                .extracting(ExecutionController.JobView::status)
                .containsExactly(Status.QUEUED);
        assertThat(controller
                        .advance(
                                branchId,
                                orderId,
                                "internal",
                                new ExecutionController.UpdateStatus(Status.IN_DIAGNOSIS))
                        .status())
                .isEqualTo(Status.IN_DIAGNOSIS);
        assertThatThrownBy(() -> controller.advance(
                        branchId, orderId, "internal", new ExecutionController.UpdateStatus(Status.REPAIR_RELEASED)))
                .hasMessageContaining("400");
        queue.consume(json.writeValueAsString(
                new ExecutionQueue.Command(branchId, orderId, ExecutionQueue.Action.RELEASE_REPAIR)));
        assertThat(controller
                        .advance(branchId, orderId, "internal", new ExecutionController.UpdateStatus(Status.IN_REPAIR))
                        .status())
                .isEqualTo(Status.IN_REPAIR);
        assertThat(controller
                        .advance(branchId, orderId, "internal", new ExecutionController.UpdateStatus(Status.FINISHED))
                        .status())
                .isEqualTo(Status.FINISHED);
    }

    @Test
    void rejectsMissingAndOutOfOrderOperationsAndAcceptsReplays() {
        assertThatThrownBy(() -> queue.advance(branchId, orderId, Status.QUEUED))
                .hasMessageContaining("400");
        assertThatThrownBy(() -> queue.advance(branchId, orderId, Status.IN_DIAGNOSIS))
                .hasMessageContaining("404");
        when(mongo.findOne(any(Query.class), eq(ExecutionJob.class))).thenReturn(job(Status.QUEUED));
        assertThatThrownBy(() -> queue.advance(branchId, orderId, Status.IN_REPAIR))
                .hasMessageContaining("409");
        var finished = job(Status.FINISHED);
        when(mongo.findOne(any(Query.class), eq(ExecutionJob.class))).thenReturn(finished);
        assertThat(queue.advance(branchId, orderId, Status.IN_DIAGNOSIS)).isSameAs(finished);
    }

    @Test
    void removesOutboxEventOnlyAfterConfirmation() throws Exception {
        var publisher = new ExecutionPublisher(mongo, rabbit, json);
        publisher.publish();
        verifyNoInteractions(rabbit);

        var event = new ExecutionJob.StatusEvent(
                UUID.randomUUID().toString(), orderId.toString(), branchId.toString(), Status.FINISHED);
        var job = new ExecutionJob(
                orderId.toString(), branchId.toString(), Status.FINISHED, Instant.now(), List.of(event));
        when(mongo.findOne(any(Query.class), eq(ExecutionJob.class))).thenReturn(job);
        confirm(event, false);
        assertThatThrownBy(publisher::publish).isInstanceOf(IllegalStateException.class);
        verify(mongo, never()).updateFirst(any(Query.class), any(Update.class), eq(ExecutionJob.class));

        confirm(event, true);
        publisher.publish();
        verify(mongo).updateFirst(any(Query.class), any(Update.class), eq(ExecutionJob.class));
        assertThat(new ExecutionConfiguration().executionCommandQueues().getDeclarablesByType(Queue.class))
                .extracting(Queue::getName)
                .containsExactly(ExecutionQueue.COMMANDS, ExecutionQueue.COMMANDS + ".dead");
    }

    private void confirm(ExecutionJob.StatusEvent event, boolean ack) {
        doAnswer(call -> {
                    var message = json.readValue((String) call.getArgument(2), ExecutionJob.StatusEvent.class);
                    assertThat(message.eventId()).isEqualTo(event.eventId());
                    assertThat(message.status()).isEqualTo(event.status());
                    MessagePostProcessor processor = call.getArgument(3);
                    assertThat(processor
                                    .postProcessMessage(new Message(new byte[0]))
                                    .getMessageProperties()
                                    .getDeliveryMode())
                            .isEqualTo(MessageDeliveryMode.PERSISTENT);
                    CorrelationData confirmation = call.getArgument(4);
                    confirmation.getFuture().complete(new CorrelationData.Confirm(ack, null));
                    return null;
                })
                .when(rabbit)
                .convertAndSend(
                        eq(""),
                        eq(ExecutionPublisher.RESULTS),
                        any(String.class),
                        any(MessagePostProcessor.class),
                        any(CorrelationData.class));
    }

    private ExecutionJob job(Status status) {
        return new ExecutionJob(orderId.toString(), branchId.toString(), status, Instant.now(), List.of());
    }
}
