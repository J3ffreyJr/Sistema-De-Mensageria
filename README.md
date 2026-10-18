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

O Compose lê as credenciais de um ficheiro `.env` ignorado pelo Git. Prepare e
inicie o broker assim (PowerShell):

```powershell
Copy-Item .env.example .env
docker compose up -d
$env:RABBITMQ_HOST = "localhost"
$env:RABBITMQ_PORT = "5672"
$env:RABBITMQ_VHOST = "/"
$env:RABBITMQ_USERNAME = "dev"
$env:RABBITMQ_PASSWORD = "dev-password"
$env:RABBITMQ_TLS_ENABLED = "false"
```

O exemplo cria o utilizador `dev` com a palavra-passe `dev-password`, expõe
AMQP em `localhost:5672` e a consola de gestão em <http://localhost:15672>.
Os dados ficam no volume nomeado `rabbitmq-data`. Para parar o broker sem
remover os dados, use `docker compose stop`; para remover também o volume,
use `docker compose down -v`. Em Bash, use `cp .env.example .env`, os mesmos
comandos `docker compose` e `export` para as seis variáveis.

Como alternativa, instale RabbitMQ localmente e forneça as credenciais através
das variáveis abaixo.

## Configuração

Host, porta, vhost e TLS têm defaults locais (`localhost`, `5672`, `/` e
`false`). `RABBITMQ_USERNAME` e `RABBITMQ_PASSWORD` são obrigatórias e falham
com uma mensagem clara quando não são fornecidas. Em ambientes reais, forneça
credenciais por secrets; não coloque palavras-passe no código ou no controlo
de versões.

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `RABBITMQ_HOST` | `localhost` | Host do broker |
| `RABBITMQ_PORT` | `5672` | Porta AMQP |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `RABBITMQ_USERNAME` | obrigatório | Utilizador |
| `RABBITMQ_PASSWORD` | obrigatório | Palavra-passe |
| `RABBITMQ_TLS_ENABLED` | `false` | `true` ou `false` |

Também são aceites propriedades Java equivalentes (`rabbitmq.host`,
`rabbitmq.port`, `rabbitmq.vhost`, `rabbitmq.username`, `rabbitmq.password` e
`rabbitmq.tlsEnabled`) quando não existe a variável de ambiente correspondente;
as variáveis de ambiente têm precedência. Por exemplo, para uma execução sem
variáveis de ambiente:

```powershell
java -Drabbitmq.username=dev -Drabbitmq.password=dev-password `
  -cp target\classes <classe-principal>
```

Palavras-passe são redigidas no `toString()` da configuração.

## Compilar e testar

```bash
mvn clean test
```

Este comando executa os testes unitários e os testes de integração são
ignorados; não inicia Docker nem depende de um broker ativo.

Para a validação end-to-end opt-in, primeiro inicie o broker:

```powershell
Copy-Item .env.example .env
docker compose up -d
mvn verify -Pintegration
```

O perfil `integration` ativa testes com filas efémeras (e mensagens com IDs
únicos), confirma publicações obrigatórias, valida roteamento, acknowledgements
manuais, recuperação da ligação, idempotência e dead-letter. Uma execução
bem-sucedida termina com `BUILD SUCCESS`; sem o perfil, todos estes testes são
ignorados de forma limpa. Para parar o broker mantendo os dados use
`docker compose stop`; para remover também o volume e começar de um broker
efémero use `docker compose down -v`. O broker local persistente (volume
`rabbitmq-data`) é diferente do modo efémero usado por CI, que deve usar um
volume novo ou uma instância RabbitMQ isolada.

Para executar o produtor ou um consumidor depois de iniciar o broker, compile
com Maven e execute a classe Java correspondente usando o classpath gerado.
