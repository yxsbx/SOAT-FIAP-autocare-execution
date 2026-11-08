package br.com.autocarehub.execution;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ExecutionApplication {
    public static void main(String[] args) {
        SpringApplication.run(ExecutionApplication.class, args);
    }
}
