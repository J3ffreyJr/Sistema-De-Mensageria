# Sistema de Mensageria

Protótipo Java 25 com RabbitMQ e Maven.

## Requisitos

- JDK 25 (`JAVA_HOME` deve apontar para a instalação ativa).
- Maven 3.9 ou superior.
- Docker Compose v2 para o broker local (alternativamente, uma instalação local do RabbitMQ).

## Executar o RabbitMQ para desenvolvimento

```bash
docker compose up -d
```

O Compose cria o utilizador de desenvolvimento `dev` com a palavra-passe
`dev-password`, expõe AMQP em `localhost:5672` e a consola de gestão em
<http://localhost:15672>. Os dados ficam no volume nomeado `rabbitmq-data`.
Para parar o broker sem remover os dados, use `docker compose stop`; para
remover também o volume, use `docker compose down -v`.

Como alternativa, instale RabbitMQ localmente e forneça as credenciais através
das variáveis abaixo.

## Configuração

Sem variáveis, a configuração usa os valores locais compatíveis com o broker
RabbitMQ padrão (`localhost`, porta `5672`, vhost `/`, utilizador `guest`,
palavra-passe `guest`, TLS desativado). Em ambientes reais, forneça credenciais
por secrets; não coloque palavras-passe no código ou no controlo de versões.

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `RABBITMQ_HOST` | `localhost` | Host do broker |
| `RABBITMQ_PORT` | `5672` | Porta AMQP |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `RABBITMQ_USERNAME` | `guest` | Utilizador |
| `RABBITMQ_PASSWORD` | `guest` | Palavra-passe |
| `RABBITMQ_TLS_ENABLED` | `false` | `true` ou `false` |

Também são aceites propriedades Java equivalentes (`rabbitmq.host`,
`rabbitmq.port`, `rabbitmq.vhost`, `rabbitmq.username`, `rabbitmq.password` e
`rabbitmq.tlsEnabled`) quando não existe a variável de ambiente correspondente.
Palavras-passe são redigidas no `toString()` da configuração.

## Compilar e testar

```bash
mvn clean test
```

Os testes são unitários e não iniciam Docker nem dependem de um broker ativo.
O Compose é apenas uma conveniência para desenvolvimento e testes manuais.

Para executar o produtor ou um consumidor depois de iniciar o broker, compile
com Maven e execute a classe Java correspondente usando o classpath gerado.
