# Task 1 — Relatório de implementação

## Estado

Concluído. O build Maven foi mantido em Java 25 e as versões aprovadas no
`pom.xml` foram preservadas.

## Alterações

- Adicionado `RabbitMQProperties` como record imutável, com defaults locais,
  overrides por variáveis de ambiente e propriedades Java, validação de porta,
  credenciais, vhost e TLS.
- `RabbitMQConfig.getConnectionFactory()` mantém a API existente e passa a
  carregar configuração externa; foi adicionada uma sobrecarga que recebe
  `RabbitMQProperties`. TLS é ativado quando configurado.
- A palavra-passe é redigida no `toString()` e não é incluída em logs.
- Os testes de configuração cobrem defaults, overrides, porta inválida,
  palavra-passe vazia, parsing TLS e redacção de segredo.
- Adicionado `docker-compose.yml` apenas para desenvolvimento, usando imagem
  RabbitMQ management, credenciais explícitas de desenvolvimento, volume
  persistente nomeado e health check.
- Adicionados `.gitignore` atualizado e `README.md` com requisitos Java 25,
  Maven, Docker Compose, RabbitMQ local, variáveis de ambiente e comandos de
  teste. O Compose não é usado pelos testes unitários.
- Removido `dependency-list.txt`, que anunciava versões divergentes das
  dependências do `pom.xml`.

## TDD e validação

1. Os testes de configuração foram escritos antes da implementação.
2. A execução focada com JDK 25 falhou como esperado porque
   `RabbitMQProperties` ainda não existia.
3. Após a implementação, `mvn -q -Dtest=RabbitMQConfigTest test` passou.
4. A validação final `mvn clean test` com `JAVA_HOME` em
   `C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot` passou:
   11 testes, 0 falhas, 0 erros.
5. A revisão local incluiu `git diff --check`, sem problemas de whitespace.

## Limitações e preocupações

- Os defaults `guest/guest` existem apenas para compatibilidade com a execução
  local documentada; ambientes reais devem fornecer secrets.
- O Compose contém credenciais de desenvolvimento intencionalmente explícitas,
  não credenciais de produção.
- O aviso de operações unchecked em código legado de `Consumidor2` permanece
  fora do escopo desta tarefa.

## Relatório de correção — 2026-10-18

### Alterações realizadas

- Removidos os defaults `guest/guest` de `RabbitMQProperties`. Username e
  password agora são obrigatórios e produzem uma `IllegalArgumentException`
  explícita indicando as variáveis `RABBITMQ_USERNAME`/`RABBITMQ_PASSWORD` ou
  as propriedades Java equivalentes.
- Mantida a precedência de ambiente sobre propriedades Java, sem expor
  credenciais no código de produção ou no `toString()`.
- Adicionado `.env.example` (com valores exclusivamente de desenvolvimento)
  e atualizado o Compose para ler as credenciais por interpolação externa.
  `.env` continua ignorado pelo Git.
- Atualizado o README com os comandos PowerShell/Bash para copiar `.env`,
  iniciar o Compose e exportar a mesma configuração para a aplicação.
- Adicionados testes de overrides por propriedades Java, precedência das
  variáveis de ambiente e captura/restauração das propriedades do sistema em
  cada teste, garantindo limpeza mesmo quando a suite é executada em conjunto.

### Testes e comandos

JDK usado: `C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot`

- `mvn -q -Dtest=RabbitMQConfigTest test`
  - Resultado: sucesso; 11 testes, 0 falhas, 0 erros.
- `mvn clean test`
  - Resultado: `BUILD SUCCESS`; 14 testes, 0 falhas, 0 erros.
- `git diff --check`
  - Resultado: sucesso, sem erros de whitespace.

### Preocupações

- O Compose continua a usar `dev/dev-password` apenas como credencial local
  de desenvolvimento, agora fornecida por `.env.example`/`.env`; não é um
  default de Java nem uma credencial de produção.
- O aviso de operações unchecked em `Consumidor2` permanece preexistente e
  fora do escopo.

