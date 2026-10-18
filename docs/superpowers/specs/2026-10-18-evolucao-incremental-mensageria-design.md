# Evolução incremental do sistema de mensageria

## Contexto e objectivo

O projecto actual é um protótipo Java/RabbitMQ com um produtor de consola e dois consumidores. A primeira etapa da evolução deve tornar o núcleo de mensageria confiável e configurável antes de introduzir uma API web ou uma interface de chat.

O objectivo desta fase é garantir entrega `at-least-once`, recuperação de falhas, processamento idempotente, configuração por ambiente e uma base testável que possa ser reutilizada por um backend web futuro.

### Decisões aprovadas

- Java 25 é o requisito oficial.
- RabbitMQ deve funcionar tanto por Docker Compose como por instalação local.
- O desenvolvimento deve usar volume persistente; os testes de integração devem poder usar um broker efémero.
- A semântica de entrega é `at-least-once`.
- Duplicados devem ser controlados por `messageId`.
- O motion design fica para a fase web, com suporte a `prefers-reduced-motion`.

## Arquitectura proposta

O núcleo será dividido em quatro responsabilidades:

1. **Configuração**: lê host, porta, vhost, credenciais, TLS e nomes de topologia a partir de propriedades ou variáveis de ambiente.
2. **RabbitConnectionManager**: gere a conexão, recuperação, recriação de canais e re-subscription com backoff.
3. **Publisher**: valida e serializa o DTO, publica mensagens persistentes com publisher confirms e trata mensagens não roteadas.
4. **Consumer/Processor**: limita o prefetch, processa mensagens, confirma apenas após sucesso e encaminha falhas segundo a política definida.

Fluxo principal:

```text
Publisher -> direct exchange -> durable queue -> Consumer -> Processor -> ack
```

As declarações da exchange, filas, bindings e dead-letter topology devem ser idempotentes. O núcleo não deve depender de uma interface web; a futura API poderá reutilizar o publisher e os processadores através de interfaces explícitas.

## Contrato da mensagem

O payload deixa de ser um `Map<String, Object>` e passa a ser um DTO versionado:

```json
{
  "schemaVersion": 1,
  "messageId": "PROD-...",
  "content": "Olá",
  "createdAt": "2026-10-18T17:20:00Z",
  "producer": "producer-cli",
  "routingKey": "consumidor1"
}
```

`schemaVersion`, `messageId`, `content` e `createdAt` são obrigatórios. Timestamps usam `Instant` em ISO-8601. As propriedades AMQP devem declarar `application/json`, UTF-8, `messageId` e entrega persistente.

O consumidor deve rejeitar payloads inválidos sem permitir retry infinito. O `messageId` é a chave de idempotência e deve acompanhar logs, confirmações, reentregas e dead-letter events.

## Entrega, retry e dead-letter

O publisher deve:

- activar publisher confirms;
- publicar com `mandatory = true`;
- tratar retornos de mensagens;
- usar propriedades persistentes e metadados completos;
- aplicar retry limitado para falhas de publicação transitórias;
- distinguir confirmação, retorno e falha.

O consumidor deve:

- usar `basicQos(1)` inicialmente;
- usar ack manual;
- confirmar somente depois do processamento bem-sucedido;
- fazer `nack/requeue` para falhas transitórias dentro do limite;
- encaminhar payloads inválidos e mensagens que excederam tentativas para dead-letter;
- evitar efeitos duplicados através de uma verificação idempotente por `messageId`.

O retry inicial será implementado com TTL e filas de retry, sem adicionar uma biblioteca de mensageria. A topologia terá uma dead-letter exchange e filas de dead-letter associadas aos fluxos principais.

## Configuração e segurança

Não devem existir credenciais ou endpoints obrigatórios hard-coded no código. A configuração deve suportar:

```text
RABBITMQ_HOST
RABBITMQ_PORT
RABBITMQ_VHOST
RABBITMQ_USERNAME
RABBITMQ_PASSWORD
RABBITMQ_TLS_ENABLED
```

O utilizador `guest` poderá ser usado apenas numa configuração local documentada. Docker Compose deve criar uma configuração de desenvolvimento explícita, enquanto ambientes reais fornecem credenciais através de secrets.

## Conexão e encerramento

O `RabbitConnectionManager` deve detectar falhas, registar a alteração de estado e tentar reconectar com backoff limitado. Ao reconectar, deve recriar canais, declarar a topologia e reinstalar os consumidores.

O processo deve possuir shutdown controlado. O encerramento deve parar novos consumos, permitir a conclusão do trabalho em curso quando possível, fechar canais e fechar a conexão. Falhas de shutdown devem ser registadas sem esconder a falha original.

## Logging e observabilidade

O logging deve deixar de abrir e fechar manualmente um ficheiro por evento. Deve usar a fachada SLF4J já presente no projecto, com backend configurado para:

- níveis configuráveis;
- rotação de ficheiros;
- logs de consola e ficheiro;
- `messageId` e correlation ID;
- eventos de conexão, publicação, confirmação, retry, ack, nack e dead-letter;
- redacção ou omissão do conteúdo completo quando este for sensível.

`iniciarNovoLog()` não deve apagar silenciosamente o histórico numa execução normal.

## Testes

### Unitários

Devem cobrir configuração, DTO, validação, serialização, UTF-8, routing e classificação de erros.

### Integração

Com RabbitMQ real, devem verificar:

- destino apenas para consumidor 1;
- destino apenas para consumidor 2;
- destino para ambos;
- persistência e publisher confirms;
- ack após processamento;
- retry e dead-letter;
- reentrega depois de interrupção.

### Idempotência e resiliência

Devem existir cenários para mensagem duplicada pelo mesmo `messageId`, reinício do broker e reinício do consumidor durante o processamento. O resultado esperado é ausência de perda silenciosa e ausência de efeitos duplicados.

## Compatibilidade e operação

O projecto deve incluir:

- `docker-compose.yml` para RabbitMQ;
- documentação de Java 25, Maven, Docker e execução local;
- instruções para executar testes unitários e de integração;
- `.gitignore` para `target`, logs e configurações locais;
- remoção ou regeneração automática de `dependency-list.txt`, evitando divergência do `pom.xml`.

## Fora do âmbito da Fase 1

- frontend web;
- WebSocket;
- autenticação de utilizadores;
- persistência de conversas numa base de dados;
- presença online;
- anexos;
- animações de interface.

Estas capacidades serão desenhadas numa fase posterior, depois de o núcleo de mensageria passar os testes de confiabilidade.

## Critérios de aceitação

1. O projecto compila e testa consistentemente com Java 25.
2. Nenhuma credencial de broker necessária fica hard-coded.
3. Uma mensagem confirmada pelo publisher não é considerada processada antes de ack do consumidor.
4. Falhas durante o processamento provocam reentrega ou dead-letter segundo a política definida.
5. O mesmo `messageId` não produz efeitos duplicados.
6. O sistema recupera de uma interrupção temporária do RabbitMQ sem intervenção manual do operador.
7. Os testes verificam os três destinos de routing e os principais cenários de falha.
8. A configuração e execução local estão documentadas.
