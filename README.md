# AutoCare Hub — Execution Service

Execution mantém a fila por filial e controla diagnóstico, liberação, reparo e conclusão em MongoDB. Recebe comandos da OS em `autocare.execution.command.v1` e devolve mudanças de etapa em `autocare.os.execution-updated.v1`. A atualização da etapa e o registro do evento são atômicos no documento MongoDB.

As rotas internas abaixo exigem o header `X-Internal-Service-Token`, configurado por `EXECUTION_INTERNAL_TOKEN`.

- `GET /internal/v1/branches/{branchId}/execution` lista a fila ativa.
- `PATCH /internal/v1/branches/{branchId}/execution/{orderId}` recebe `status` com `IN_DIAGNOSIS`, `IN_REPAIR` ou `FINISHED`. A liberação para reparo vem da OS após aprovação e pagamento.

```sh
mvn -B spotless:check verify
```

O ensaio local da integração registrou **85,00% de cobertura de linhas** na revisão `17f5f0a`. O relatório JaCoCo local fica em `target/site/jacoco/index.html`. Em uma execução do CI para o SHA da entrega, consulte o artefato `execution-coverage` na página da execução. [Workflow de CI](https://github.com/yxsbx/SOAT-FIAP-autocare-execution/actions/workflows/ci.yml)

Configure `EXECUTION_MONGODB_URI`, `RABBITMQ_HOST`, `RABBITMQ_USER` e `RABBITMQ_PASSWORD` para iniciar o serviço. Use o BDD na API para validar o conjunto localmente. Os manifestos de implantação estão em `k8s/`.

O contrato HTTP está em [OpenAPI](docs/api/openapi.yaml).

[Arquitetura e procedimentos da Fase 4](https://github.com/yxsbx/SOAT-FIAP-autocare-docs)
