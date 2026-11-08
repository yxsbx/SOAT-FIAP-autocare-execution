package br.com.autocarehub.execution;

import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class ExecutionConfiguration {
    @Bean
    Declarables executionCommandQueues() {
        String queue = ExecutionQueue.COMMANDS;
        return new Declarables(
                QueueBuilder.durable(queue)
                        .deadLetterExchange("")
                        .deadLetterRoutingKey(queue + ".dead")
                        .build(),
                QueueBuilder.durable(queue + ".dead").build());
    }
}
