Wallet & Wager Engine

Serviço Go para operações de apostas por HTTP e SQS. PostgreSQL coordena as
instâncias; carteira, ledger, idempotência, inbox e outbox compartilham uma
transação.

## Pré-requisitos

- Go **1.26.4** e Docker com Compose v2.
- Tilt é opcional para desenvolvimento, sem Kubernetes ou extensões remotas.
- Portas locais: API 8080; réplicas 8082/8083; Keycloak 8081; PostgreSQL 5433;
  LocalStack 4566; Prometheus opcional 9090.
- O primeiro build baixa módulos públicos e imagens. Depois, os caches podem ser
  reutilizados. Nenhum recurso AWS real é provisionado.

## Inicialização

```sh
docker compose up --build -d
docker compose logs -f app-1
curl --fail http://localhost:8080/health/ready
```

O Compose provisiona realm/clientes Keycloak, quatro filas FIFO e redrive, aplica
Goose e inicia a API. Uma falha na migration impede a subida da aplicação.
PostgreSQL usa volume persistente. `docker compose down` preserva esse volume;
não execute `down -v` se precisar dos dados.

```sh
# Três processos, conexões e memórias independentes
docker compose --profile replicas up --build -d
# Métricas e painel básico do Prometheus
docker compose --profile observability up -d prometheus
# Desenvolvimento com rebuild e live update
tilt up
```

O ambiente é exclusivamente de desenvolvimento: credenciais de exemplo,
Keycloak `start-dev`, HTTP no IdP e LocalStack. Leia os limites de segurança em
[ARCHITECTURE.md](ARCHITECTURE.md) antes de qualquer uso fora de localhost.

## Executar o Go no host

```sh
docker compose up -d postgres keycloak localstack localstack-init
set -a
. ./.env.example
set +a
go run ./cmd/migrate up
go run ./cmd/api
```

`.env.example` contém nomes e valores locais. O processo lê variáveis de ambiente,
não carrega `.env` implicitamente. Não commite credenciais reais.

Configurações principais: `DATABASE_URL`, `HTTP_ADDR`, `INSTANCE_ID`,
`OIDC_ISSUER`, `OIDC_FETCH_ISSUER`, `OIDC_AUDIENCE`,
`OIDC_ALLOW_INSECURE_HTTP`, `AWS_REGION`, `SQS_ENDPOINT`,
`SQS_INPUT_QUEUE`, `SQS_OUTPUT_QUEUE`, `SQS_INPUT_DLQ`,
`SQS_TRUSTED_PROVIDERS`, `WORKER_CONCURRENCY`, `PROCESS_TIMEOUT`,
`SQS_VISIBILITY_TIMEOUT`, `SHUTDOWN_TIMEOUT`, `CLAIM_LEASE`,
`POLL_INTERVAL`, `REFERENCE_TTL`, `REFERENCE_MAX_ATTEMPTS`.

O SDK usa sua cadeia de credenciais padrão. No Compose, `test/test` é somente para
o emulador. Não forneça credenciais de nuvem para executar estes exemplos.

## Autenticação e exemplos

Clientes locais provisionados:

| Cliente | Secret de desenvolvimento | Permissão |
|---|---|---|
| `wallet-internal-client` | `wallet-internal-local-only` | wallet read/write/reconcile |
| `provider-a-client` | `provider-a-local-only` | wager read/write, provider-a |
| `provider-b-client` | `provider-b-local-only` | wager read/write, provider-b |

Obtenha um token; os comandos de exemplo usam `jq` apenas para extrair respostas:

```sh
INTERNAL_TOKEN=$(curl --fail --silent \
  http://localhost:8081/realms/jungle/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=wallet-internal-client \
  -d client_secret=wallet-internal-local-only | jq -r .access_token)
PROVIDER_TOKEN=$(curl --fail --silent \
  http://localhost:8081/realms/jungle/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=provider-a-client \
  -d client_secret=provider-a-local-only | jq -r .access_token)

# Jogador novo a cada execução: o par (playerId, currency) é único, então
# repetir o exemplo com um ID fixo devolve 409 na segunda vez, corretamente.
PLAYER_ID=$(uuidgen | tr 'A-Z' 'a-z')

WALLET_ID=$(curl --fail --silent http://localhost:8080/wallets \
  -H "Authorization: Bearer $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER_ID\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" | jq -r .id)

curl --silent http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-demo-1' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"bet-demo-1\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-demo\",\"gameId\":\"game-demo\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

curl --silent -X POST "http://localhost:8080/wallets/$WALLET_ID/reconciliation" \
  -H "Authorization: Bearer $INTERNAL_TOKEN"
```

Repita a BET com a mesma chave/conteúdo: retorna replay, sem novo débito, com o
mesmo `transactionId` e `idempotentReplay: true`. Um token de `provider-b` lendo
uma transação de `provider-a` recebe `403`; sem token, `401`.

### Rotas

- `POST /wallets`, `GET /wallets/{id}`.
- `GET /wallets/{id}/ledger?limit=50&cursor=...` (limite 1–100).
- `POST /wallets/{id}/reconciliation` (read-only).
- `POST /wagering/transactions` com `Idempotency-Key` obrigatório.
- `GET /wagering/transactions/{id}`.
- `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`.
- Públicas: `GET /health/live`, `/health/ready`, `/metrics`.
- As rotas de negócio também aceitam prefixo `/v1`.

201: criação/processamento novo; 200: leitura ou replay; 202: referência pendente;
400: entrada inválida; 401: token inválido; 403: permissão/provedor incompatível;
404: recurso inexistente no escopo autorizado; 409: identidade em conflito;
422: rejeição financeira confirmada; 503: indisponibilidade transitória.
JSON inválido, campos desconhecidos, conteúdo adicional, corpo excessivo e tipo
de conteúdo incorreto são recusados. Erros não expõem mensagens SQL ou tokens.

## SQS e eventos

Filas provisionadas: `wager-transactions.fifo`, `wager-transactions-dlq.fifo`,
`wager-events.fifo`, `wager-events-dlq.fifo`.

A entrada usa o envelope do enunciado: `messageId`, `type` igual a
`WagerTransactionRequested`, `occurredAt` RFC3339 e `data` com os campos da
operação mais `idempotencyKey`. Use `MessageGroupId=walletId` e
`MessageDeduplicationId=messageId`. Somente produtores internos confiáveis podem
publicar: provedores externos usam HTTP autenticado.

A saída usa envelope tipado versionado com `eventId`, `eventType`, `aggregateId`,
`correlationId`, `causationId` opcional, `occurredAt`, `version` e `data`.
Os eventos obrigatórios são `WagerTransactionProcessed`,
`WagerTransactionRejected`, `WagerTransactionPendingReference` e
`WalletBalanceChanged`. A identidade do evento permanece estável em retries.

Consumidores de eventos devem deduplicar por `eventId`. Projeções de snapshots
de saldo devem impedir regressão por `walletVersion`; não descartar operações
financeiras distintas apenas por chegarem fora de ordem. Não há exactly-once.

Long polling: 20s; concorrência limitada a 1–10 por processo; processamento 30s;
visibility 60s; redrive após cinco recebimentos. Mensagens inválidas não são
deletadas: seguem retry e DLQ. Rejeições financeiras confirmadas recebem ACK.
Outbox nunca descarta eventos por esgotamento de tentativas.

## Migrations e geração

```sh
go run ./cmd/migrate up
go run ./cmd/migrate status
# Reverte UMA migration. Use apenas banco descartável; pode remover dados.
go run ./cmd/migrate down
# Ferramentas opcionais fixadas, não necessárias para compilar código gerado
make tools
make generate
```

Goose 3.24.3; SQLC 1.30.0; queries geradas ficam versionadas. A migration exige
PostgreSQL 18+ e `uuidv7()` nativo. A imagem padrão usa tag/digest fixados no
Compose. IDs internos são gerados no banco; IDs externos são preservados.

## Testes

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Integração e conformance rodam em paralelo: cada teste cria o seu próprio schema,
então são independentes por construção. Recovery **não** roda em paralelo, de
propósito — aqueles fixtures trabalham com prazos de milissegundos e matam
processos reais; em paralelo falharam duas execuções em três, e uma prova de
durabilidade instável não prova nada. Os pools de teste têm limite explícito:
`pgxpool` abriria uma conexão por núcleo do host e uma dúzia de testes paralelos
esgotaria o servidor antes de provar qualquer coisa.

Integração exige containers reais. Use banco de desenvolvimento descartável:
os testes criam schemas/filas isolados e removem os objetos que criaram. Nunca
aponte `TEST_DATABASE_URL` para produção.

```sh
docker compose up --build -d
export TEST_DATABASE_URL='postgres://jungle:jungle-local@localhost:5433/jungle?sslmode=disable'
export TEST_SQS_ENDPOINT='http://localhost:4566'
export SYSTEM_TEST=1
go test -tags=integration -race -count=1 ./...
```

### Roteiro de validação

Cada garantia obrigatória do enunciado tem um comando. A pilha precisa estar de
pé com as três réplicas, porque várias delas só valem com processos separados:
conexões e memória independentes são parte do que está sendo provado.

```sh
docker compose --profile replicas up --build -d
export TEST_DATABASE_URL='postgres://jungle:jungle-local@localhost:5433/jungle?sslmode=disable'
export TEST_SQS_ENDPOINT='http://localhost:4566'
export SYSTEM_TEST=1
```

| Garantia | Comando |
|---|---|
| Concorrência, incluindo o caso obrigatório `100.00` contra duas apostas de `80.00`, as 50 duplicatas em paralelo e carteiras distintas avançando juntas | `make concurrency` |
| Recuperação: crash entre commit e ACK, entre publicação e marcação, reentrega, disputa entre publishers, reversão antes da referência, reinício | `make recovery` |
| Autenticação real contra o Keycloak, isolamento entre provedores em consultas e replays | `make auth` |
| Tudo acima mais migrations, constraints, imutabilidade do ledger, inbox/outbox e composição Fx | `make integration` |
| Operações adversariais geradas e aplicadas em paralelo, com auditoria ao final | `make conformance` |
| Invariantes financeiras conferidas direto no PostgreSQL, fora do processo de teste | `make audit` |

`make audit` é o veredito que não depende do código de teste: ele lê o banco e
verifica saldo contra o ledger, unicidade, não negatividade e imutabilidade. Um
teste verde com auditor vermelho seria o teste mentindo.

Consulte [VALIDATION.md](VALIDATION.md) para os comandos efetivamente executados,
resultados e pendências. Testes unitários de adapters usam doubles pontuais;
eles não substituem PostgreSQL, Keycloak e SQS nas suítes de integração.

## Teste de carga

O laboratório vive em `tests/load` sob a tag `load` e em **módulo Go próprio**,
para que a árvore de dependências do `vegeta` não entre no grafo do serviço. Ele
não entra em `go test ./...` nem em `-tags=integration`: é medição, não gate.

```sh
# a pilha precisa estar de pé com as três réplicas
make replicas
export TEST_DATABASE_URL='postgres://jungle:jungle-local@localhost:5433/jungle?sslmode=disable'
LOAD_RATE=800 LOAD_DURATION=15s LOAD_WALLETS=64 make load
```

| Variável | Padrão | Significado |
|---|---|---|
| `LOAD_RATE` | `200` | requisições por segundo oferecidas (modelo aberto) |
| `LOAD_DURATION` | `20s` | duração da rajada |
| `LOAD_WALLETS` | `64` | wallets distintas no perfil `fanout` |
| `LOAD_TARGETS` | três réplicas locais | URLs base, separadas por vírgula |

O gerador é `github.com/tsenart/vegeta/v12/lib` usado como biblioteca, em
**modelo aberto com taxa constante**. Geradores de modelo fechado (`hey`, `bombardier`) esperam cada resposta antes de enviar a
próxima, então se auto-regulam e escondem justamente o crescimento de fila que
caracteriza saturação. Um `Targeter` em Go também permite corpo dinâmico —
cada requisição carrega `externalTransactionId` e `Idempotency-Key` únicos e
escopados à execução, senão o teste mediria o caminho de replay, que é barato.

Dois perfis, porque o sistema tem dois tetos distintos:

- **`TestDrainOneWalletUnderLoad`** — uma wallet financiada para exatamente K
  apostas recebe muito mais que K apostas concorrentes nas três réplicas. Mede
  contenção, não vazão: o lock por wallet serializa por definição. A asserção é
  lida do PostgreSQL, não das respostas, para que uma resposta perdida não
  possa disfarçar débito duplicado.
- **`TestFanoutThroughput`** — wallets distintas não disputam a mesma linha.
  Este é o número de vazão que vale citar.

Todo perfil termina reconciliando cada wallet tocada e exigindo
`difference = 0.00`, além de esperar a outbox drenar. Número de RPS sem esse
veredito não é publicável: `422 INSUFFICIENT_FUNDS` é o motor funcionando, não
erro, e a taxa de sucesso padrão de qualquer gerador mentiria sobre isso.

Resultados medidos e seus limites estão em [VALIDATION.md](VALIDATION.md).

## Conformance sob concorrência

O maior risco de uma suíte escrita à mão é cobrir só os casos que alguém pensou.
`tests/model` gera a carga: um fluxo aleatório mas **reprodutível** de operações
adversariais, aplicado por muitos workers ao mesmo tempo.

```sh
make conformance
MODEL_OPS=6000 MODEL_WORKERS=48 MODEL_WALLETS=32 make conformance
MODEL_SEED=1789929189210695000 make conformance   # repete uma falha exata
```

O gerador insiste nas combinações que ninguém escolhe à mão: reversão de
reversão, rollback que chega antes da referência, a mesma identidade com payload
alterado, duas reversões disputando uma referência. A semente é impressa em toda
execução, não só na falha — um resultado que não se reproduz não serve.

O oráculo não é uma segunda implementação do ledger. São as propriedades que
valem sob qualquer intercalação:

- **Acordo de identidade.** Quantos workers tentarem a mesma identidade, todos
  ouvem falar da mesma operação. Saldo e versão só precisam coincidir em estado
  terminal, onde são snapshot congelado; em `PENDING_REFERENCE` o motor responde
  com a carteira viva, porque ainda não há resultado para congelar.
- **Efeito único.** Uma identidade produz no máximo uma transação e no máximo um
  lançamento, independente de quantas tentativas correram juntas.
- **Auditoria.** No fim roda `scripts/audit.sql`, o mesmo veredito do caos.

Medido: 6000 operações, 48 workers, 32 carteiras, ~1730 ops/s, zero violação.
O teto continua sendo o número de carteiras distintas, não os núcleos — é o mesmo
limite arquitetural que a carga mostrou.

## Dead letter queues e redrive

As duas filas têm redrive nativo com `maxReceiveCount=5`.

Com FIFO isso não é higiene, é estrutural: a entrada usa
`MessageGroupId = walletId`, e em FIFO **uma mensagem envenenada bloqueia o grupo
inteiro**. Sem DLQ, um único payload malformado congelaria todas as operações
daquele jogador por tempo indefinido. O redrive é o que desbloqueia o grupo.

O consumidor nunca publica na DLQ por conta própria — deixa o redrive agir. Um
`SendMessage` para a DLQ seguido de `DeleteMessage` abriria outra janela de perda
entre as duas chamadas.

Note a assimetria deliberada com a outbox: a entrada **desiste** no quinto
recebimento porque a mensagem continua na DLQ para inspeção; a outbox **nunca**
desiste, porque descartar um evento já commitado é perda financeira silenciosa.
Por isso o alerta da outbox é por idade, não por tentativas.

### Runbook: mensagem na DLQ

Disparado por `jungle_dlq_messages{queue="input"} > 0` ou `{queue="output"} > 0`.
A de entrada diz que o nosso consumidor recusou algo; a de saída diz que quem
consome os nossos eventos recusou, e sem ela ninguém aqui perceberia que a
fronteira de integração travou.

**1. Olhe sem consumir.** Visibilidade zero devolve a mensagem imediatamente,
então a inspeção não a esconde de quem for investigar em seguida.

```sh
aws --endpoint-url=$SQS_ENDPOINT sqs receive-message \
  --queue-url $DLQ_URL --max-number-of-messages 10 --visibility-timeout 0 \
  --message-system-attribute-names ApproximateReceiveCount
```

**2. Classifique antes de agir.** Três causas, três desfechos diferentes:

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| Envelope inválido, `kind` desconhecido, moeda errada | produtor quebrado | corrigir o produtor; **não** reprocessar |
| Mesmo `messageId`, hash diferente | mensagem alterada em trânsito | investigar o produtor; nunca reprocessar |
| Envelope válido, falha de infraestrutura nas 5 tentativas | banco ou broker indisponível na janela | reprocessar é seguro |

**3. Confirme o que já aconteceu.** Antes de reprocessar, veja se a operação já
teve efeito — as tentativas anteriores podem ter commitado:

```sql
SELECT status, failure_code, completed_at FROM wager_transaction
WHERE provider_id = :provider AND external_transaction_id = :external;
SELECT completed_at FROM inbox WHERE message_id = :message_id;
```

Se já existe registro terminal, o reprocessamento vai retornar replay e não
moverá dinheiro. Isso é o comportamento desejado, não um problema.

**4. Reprocesse só o caso de infraestrutura.** O redrive nativo preserva corpo e
atributos, então a identidade de negócio e a deduplicação por inbox continuam
valendo:

```sh
aws --endpoint-url=$SQS_ENDPOINT sqs start-message-move-task \
  --source-arn $DLQ_ARN --destination-arn $SOURCE_QUEUE_ARN
```

**Nunca reenvie reconstruindo a mensagem à mão.** Um `messageId` novo cria uma
inbox nova e contorna a deduplicação — é assim que se produz débito duplicado.

**5. Feche com a auditoria.** `./scripts/audit.sh redrive` confirma que o
reprocessamento não deixou nada inconsistente.

Não existe replay automático: uma mensagem na DLQ chegou lá porque cinco
tentativas falharam, e reprocessar às cegas repete a quinta.

## Caos e modos de falha

Carga sozinha diz quão rápido; caos sozinho diz se caiu. Só os dois juntos, com
o ledger auditado entre as falhas, dizem se o serviço pode ficar na frente de
dinheiro.

```sh
make profile                      # porteiro: mede host e VM do Docker, dá o veredito
make audit                        # auditor global de invariantes, read-only
make chaos SCENARIO=kill-app      # injeta um modo de falha e audita
make soak                         # carga contínua + todos os cenários + relatório
./scripts/chaos.sh --list         # cenários disponíveis e o que cada um espera
DRY_RUN=1 ./scripts/soak.sh       # imprime o plano sem tocar em nada
```

### Porteiro de recursos

`scripts/profile.sh` compara o host com a fatia que o Docker recebeu e recusa a
execução sem folga. Numa máquina de desenvolvimento a VM do Docker costuma ter
uma fração da CPU e da memória do host, e containers de outros projetos dividem
essa fatia: começar um soak sem folga mede o OOM killer, não o serviço. O
veredito sai como `go`, `caution` ou `stop` (exit 0/1/2).

### O auditor

São 16 checagens `FAIL` e 4 `WARN`. Vale saber quais carregam peso: várias
duplicam uma constraint ou trigger e só disparam se uma migration derrubou a
garantia — são o detector de guarda removida. As que cobrem algo que o schema
**não** impõe são `conservation`, `ledger_chain`, `result_snapshot`,
`reversal_mismatch`, `result_currency`, `duplicate_event` e `event_not_emitted`.
A mais importante é `reversal_mismatch`: a regra de que uma reversão tem o valor
e a identidade da referência vive só em Go, e um ROLLBACK que credita mais do que
a referência passa por conservação, cardinalidade, aritmética e direção.

`scripts/audit.sql` é SQL puro e read-only, e roda **com a aplicação fora do
ar** — que é justamente quando o ledger mais precisa ser conferido. Ele devolve
uma linha por achado, `FAIL` para dinheiro ou garantia durável quebrada e `WARN`
para limiar operacional que o sistema ainda pode recuperar sozinho.

Verifica conservação por carteira (`saldo = créditos - débitos`, somado em
`NUMERIC` porque totais históricos estouram `int64`), cardinalidade e aritmética
do ledger, direção coerente com o `kind` e com o `kind` da referência, snapshot
de replay, reversão dupla, unicidade das identidades de negócio, estado terminal
completo, emissão de evento e integridade da inbox.

### Cenários

Todos usam apenas primitivas do Docker e `psql`. Pumba e Toxiproxy não são
exigidos: tornariam a suíte não reproduzível offline, e morte de processo,
processo travado, partição de rede, aperto de memória e conexão cortada já
cobrem os modos de falha que este serviço precisa sobreviver.

| Cenário | Mecanismo | O que precisa se manter |
|---|---|---|
| `kill-app` | `SIGKILL` numa réplica | pares absorvem o tráfego; nada perdido nem duplicado |
| `rolling-restart` | `SIGTERM` em sequência | drenagem graciosa; readiness cai antes do socket |
| `pause-db` | `SIGSTOP` no PostgreSQL | escrita falha fechada em 503; nada commita pela metade |
| `kill-db` | `SIGKILL` + restart | nenhuma transação rasgada sobrevive; pool reconecta |
| `drop-db-conns` | `pg_terminate_backend` | transação em voo faz rollback inteiro |
| `partition-sqs` | rede desconectada | consumer para de dar ACK; outbox acumula sem perder |
| `pause-sqs` | `SIGSTOP` no broker | publish expira, repete com backoff, mesmo `event_id` |
| `flap-sqs` | conecta/desconecta em ciclo | tempestade de retry limitada por backoff |
| `partition-idp` | Keycloak fora da rede | token já emitido continua válido; liveness não depende do IdP |
| `squeeze-app` | limite de memória cortado | OOM killer, restart, claims liberados por lease |

Exaustão de disco fica de fora de propósito: encher o disco da VM é lento e pode
travar o daemon, então permanece procedimento manual em volume dedicado.

Os scripts rodam em **bash 3.2**, que é o que o macOS embarca e para onde
`/usr/bin/env bash` aponta numa instalação limpa. Nada de arrays associativos
nem `mapfile`; expansões de array vazio são sempre protegidas por contagem,
porque em 3.2 elas abortam sob `set -u`.

Todo cenário arma o seu restore **antes** da ação destrutiva e nunca o desarma:
os restores são idempotentes, então executá-los duas vezes é inofensivo enquanto
executá-los zero vezes deixa a máquina quebrada. `INT` e `TERM` restauram **e
saem** (130/143) em vez de retomar o script, senão um `Ctrl-C` dentro de um laço
injetaria a falha seguinte com a rede de segurança já gasta.

Três detalhes que só aparecem executando, e que os scripts tratam:

- **Reconectar à rede exige `--alias`.** O Compose dá a cada container um alias
  DNS curto igual ao nome do serviço, e as apps resolvem `localstack` e
  `keycloak` por ele. Um `docker network connect` sem `--alias` devolve só o nome
  do container: a stack fica silenciosamente quebrada até um recreate completo.
- **`docker update --memory 0` é no-op.** O daemon ignora campos de recurso
  zerados, então um container que subiu sem limite não tem como perdê-lo por
  update. Se o limite original era `0`, o restore é `--force-recreate`; o script
  confere o resultado e aborta com o comando de recuperação se não bateu.
- **Matar o PID do gerador orfana o `go test`.** `( ... ) &` guarda o PID do
  subshell; o `go test` sobrevive reparentado e continua escrevendo no ledger que
  a auditoria seguinte vai ler. O `soak.sh` usa grupo de processos
  (`set -m` + `kill -- -PID`).

### Perfil de carga sob caos

`TestSteadyLoadUnderChaos` existe separado dos perfis de medição porque estes
exigem zero `5xx` e 1% de orçamento de erro — corretos para medir, errados sob
falha induzida. O perfil de caos cobra o contrato visível ao cliente: nenhuma
identidade da execução pode existir duas vezes, tudo que o serviço confirmou tem
de estar no PostgreSQL, e nada pode ficar fora de estado terminal. A consistência
interna do banco fica com o auditor, sem duplicar a asserção.

## Observabilidade

Logs JSON carregam correlação e IDs disponíveis, nunca tokens, credenciais,
receipt handles ou payload financeiro completo. Métricas Prometheus cobrem
HTTP, resultados, duplicatas, retries, conflitos, duração de aquisição de lock,
backlog/idade da outbox, referências pendentes, DLQ e reconciliação divergente.
IDs não são labels. Readiness verifica drain, PostgreSQL e SQS.

O domínio financeiro não usa ponto flutuante. Métricas de duração/contagem usam a
representação numérica exigida pelo Prometheus; dinheiro nunca entra nelas.

### Latência por operação

Latência de BET é requisito não funcional, então tem série própria:

```promql
histogram_quantile(0.99, sum by (le) (rate(
  jungle_wager_duration_seconds_bucket{kind="BET",outcome="PROCESSED"}[5m])))
```

`jungle_wager_duration_seconds{transport,kind,outcome}` mede o caso de uso, fora
da decodificação e da autorização, no mesmo ponto nos dois transportes — então
HTTP e SQS são comparáveis. O `outcome` separa `REPLAY` (que pula o trabalho
financeiro) e `REJECTED` (que não escreve lançamento) de `PROCESSED`: misturá-los
puxa o percentil do trabalho real para baixo.

O conjunto de labels é **declarado, não inferido**. `provider_id` fica de fora:
multiplicaria séries sem responder à pergunta.

Os buckets vão de 0,5ms a 5s porque o motor tem dois regimes — carteiras
distintas liquidam perto de 2ms, uma carteira em disputa leva centenas de ms. Os
buckets padrão do Prometheus começam em 5ms e colocariam quase tudo num balde só.

### Conjunto de métricas

Quatro perguntas, e cada uma tem resposta:

| Pergunta | Séries |
|---|---|
| **SLA** — quanto demora uma BET? | `jungle_wager_duration_seconds{transport,kind,outcome}` |
| **SLA por rota** — qual endpoint está lento? | `jungle_http_request_seconds{method,route,status_class}` |
| **SLA por statement** — qual query está lenta? | `jungle_database_query_seconds{statement}`, `jungle_database_lock_query_seconds{query}` |
| **Correção** — o que está sendo recusado e por quê? | `jungle_wager_results_total{transport,kind,status,failure_code}`, `jungle_duplicates_total`, `jungle_conflicts_total`, `jungle_reconciliations_total{consistent}` |
| **Durabilidade** — os eventos estão saindo? | `jungle_outbox_published_total{event_type,outcome}`, `jungle_outbox_pending`, `jungle_outbox_oldest_seconds`, `jungle_dlq_messages{queue}`, `jungle_sqs_messages_total{queue,outcome}`, `jungle_reference_attempts_total{outcome}` |
| **Saturação** — o que vai quebrar a seguir? | `jungle_db_pool_connections` / `_max_connections` / `_empty_acquires_total` / `_acquire_seconds_total`, `jungle_retries_total{worker,reason}` |

`empty_acquires_total` subindo com `connections` no teto precede os timeouts:
é o que distingue pool esgotado de banco lento, que backlog e latência não
separam.

`failure_code` separa as rejeições: jogador sem saldo é normal, moeda divergente
é integração quebrada, referência ausente é ordem de fila. Conjunto fechado,
cardinalidade limitada.

`jungle_build_info{version,go_version,instance}` correlaciona regressão com deploy.

Dinheiro nunca entra em métrica, e nenhum ID — wallet, transaction, message ou
provider — é label.

`jungle_http_request_seconds` fecha uma assimetria que existia: o contador sempre
carregou `route`, o histograma não, então dava para saber quantas requisições cada
rota recebeu e não quanto tempo levavam. `status_class` em vez de `status` evita
que um 404 rápido seja promediado na latência do trabalho que de fato roda.

`jungle_database_query_seconds{statement}` cronometra **toda** query, não só as
que travam. O nome sai do `-- name:` do sqlc quando existe e, caso contrário, é
derivado como `verbo:tabela`. Isso é seguro porque a tabela precisa ser uma que
este schema possui: um statement desconhecido vira `other:other` em vez de gerar
uma série a partir do texto do SQL.

Controle de transação tem categoria própria, e `tx:commit` é a que interessa: os
triggers de integridade são `DEFERRABLE INITIALLY DEFERRED`, então rodam no
commit. A duração dele é o custo de **validar** a escrita, não só o fsync — um
guard caro aparece ali e em nenhum outro lugar. A consulta de reconciliação, que soma o
ledger inteiro em `NUMERIC` e é plausivelmente a mais cara do sistema, não tinha
medição alguma até aqui.

`jungle_database_lock_query_seconds{query}` isola a espera de lock por statement,
com o nome extraído do comentário `-- name:` que o sqlc grava no código gerado.
`latência_BET − GetWalletForUpdate` é o custo real do caminho financeiro, separado
da fila.
