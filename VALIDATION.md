# Evidência de validação

O que foi executado e medido. Código presente sem execução não aparece aqui.

Ambiente: Go 1.26.4 no host; PostgreSQL 18.1, Keycloak 26.0.7 e LocalStack em
contêiner, imagens fixadas por tag/digest; três réplicas da aplicação. Cada
suíte de integração cria schema e filas próprios e remove o que criou.

## Suítes

```sh
go build ./...   go vet ./...   gofmt -l   # sem divergências
go test ./...                              # passa sem banco e sem variáveis
go test -race ./...                        # idem

export TEST_DATABASE_URL='postgres://jungle:jungle-local@localhost:5433/jungle?sslmode=disable'
export TEST_SQS_ENDPOINT='http://localhost:4566'
export SYSTEM_TEST=1
go test -tags=integration -race -count=1 ./...
```

Doze pacotes verdes contra a pilha subida do zero (`down -v` seguido de `up
--build`), incluindo `internal/modules` com `fx.ValidateApp`.

Coberto por essa execução:

- migrations `up`/`down`/`up`; guards de imutabilidade do ledger; direção do
  lançamento coerente com o `kind`; recusa de lançamento anexado a `LOSS` ou a
  rejeição já commitada;
- idempotência por chave, hash e external ID, com replay devolvendo o resultado
  original mesmo após movimento posterior na carteira;
- três processos independentes: 50 duplicatas da mesma BET com um único débito,
  e a disputa `100 − 80 − 80`;
- carteiras distintas progredindo em paralelo sob barreira, sem `sleep` como prova;
- isolamento de provedor contra Keycloak real, incluindo token expirado emitido
  pelo próprio IdP;
- redelivery SQS, replay cruzado HTTP/SQS, mensagem malformada chegando à DLQ
  por redrive nativo;
- crash entre commit e ACK e entre publicação e marcação, com `event_id` estável
  e sem débito duplicado;
- fencing de claim de outbox e de referência, retomada após lease expirada;
- `PENDING_REFERENCE` resolvido por outra instância após `SIGKILL`;
- drenagem ordenada: intake para antes do drain, pool fecha por último, sem
  vazamento de goroutine.

## Comportamento verificado por HTTP e SQS

| Cenário | Resposta |
|---|---|
| `BET` acima do saldo | `422` `INSUFFICIENT_FUNDS` |
| `BET` em moeda diferente da carteira | `422` `CURRENCY_MISMATCH` |
| `LOSS` com valor ≠ `0.00` | `422` `INVALID_AMOUNT_FOR_KIND` |
| `"1e2"` como amount | `400` `INVALID_AMOUNT`, sem arredondar |
| mesma chave com conteúdo diferente | `409` |
| replay de chave e conteúdo iguais | `200`, `idempotentReplay: true`, sem novo débito |
| `ROLLBACK` sem referência existente | `202` `PENDING_REFERENCE`, retentativa agendada |
| operação entrando por SQS | processada, saldo correto, reconciliação `consistent` |
| leitura de transação de outro provedor | `403` |
| requisição sem token | `401` |

Saldo permanece intacto após todas as rejeições.

## Carga

Topologia: um laptop com banco, IdP, broker e as três réplicas no mesmo host.
**Os números medem esta topologia, não capacidade de produção.** Gerador:
`vegeta` como biblioteca, modelo aberto com taxa constante, distribuído entre
as três réplicas.

`fanout` — carteiras distintas, o número de vazão:

| Taxa oferecida | Atingida | p50 | p95 | p99 | Resultado |
|---:|---:|---:|---:|---:|---|
| 400/s | 400,1/s | 2 ms | 5 ms | 11 ms | 6000/6000 `PROCESSED`, zero erro |
| 800/s | 800,1/s | 3 ms | 53 ms | 98 ms | 12000/12000 `PROCESSED`, zero erro |

`drain` — uma carteira, o custo da contenção:

| Taxa oferecida | p50 | p95 | p99 | Resultado |
|---:|---:|---:|---:|---|
| 400/s | 2 ms | 70 ms | 293 ms | 1500 processadas, 4500 rejeitadas, saldo `0.00` |
| 800/s | 269 ms | 761 ms | 883 ms | 3000 processadas, 9000 rejeitadas, saldo `0.00` |

A mesma taxa que custa 3 ms de p50 em 64 carteiras custa 269 ms numa só: é o
`SELECT ... FOR UPDATE` serializando a linha. O teto de paralelismo do sistema é
o número de carteiras distintas. Execuções repetidas variam na cauda (p99 entre
21 ms e 98 ms em 800/s); cada linha é uma execução única, não média.

Publisher da outbox: **982 eventos/s**, medidos drenando um backlog de 263.483
em 4m28s ao fim do soak.

## Soak: 58 minutos a 400 rps com falhas injetadas

Doze modos de falha injetados sob carga contínua contra as três réplicas, com o
auditor executando entre cada um.

| Cenário | Veredito | Cenário | Veredito |
|---|---|---|---|
| `kill-app` | invariantes mantidas | `partition-idp` | invariantes mantidas |
| `pause-db` | invariantes mantidas | `squeeze-app` | invariantes mantidas |
| `drop-db-conns` | invariantes mantidas | `lease-steal` | invariantes mantidas |
| `partition-sqs` | invariantes mantidas | `compound` | invariantes mantidas |
| `pause-sqs` | invariantes mantidas | `kill-db` | invariantes mantidas |
| `flap-sqs` | invariantes mantidas | `rolling-restart` | invariantes mantidas |

**1.366.823 operações confirmadas, 1.366.823 persistidas, zero duplicadas, zero
inacabadas.** Contrato de drenagem do perfil de carga: passou. Auditoria final:
limpa. Relatório completo, sem edição de vereditos, em
[`docs/evidence/soak-clean.md`](docs/evidence/soak-clean.md).

Ordenação conferida no banco depois da execução:

| Ordenação | Concordância |
|---|---|
| publicação contra banco, entre transações da mesma carteira | 2.733.714 de 2.733.714 |
| ledger `created_at` contra `id` | 1.366.857 de 1.366.857 |

Restam quatro eventos em dois pares: os dois eventos de um mesmo commit
(`WagerTransactionProcessed` e `WalletBalanceChanged` da mesma operação), cuja
ordem relativa inverte porque o `uuidv7()` é alocado antes do `clock_timestamp()`
do INSERT. Ambos descrevem o mesmo fato já commitado; o enunciado não impõe ordem
entre eles.

## Auditor de invariantes

Executado read-only contra a base, fora do processo de teste. Recomputa cada
saldo a partir do ledger e verifica unicidade, não negatividade, imutabilidade,
cadeia `balance_before`/`balance_after`, coerência de reversões e emissão de
eventos. Resultado nas execuções acima: **zero `FAIL`**.

Resultado limpo sozinho não distingue sistema correto de auditor cego. A
detecção foi provada:

- somar uma unidade mínima ao saldo sem lançamento correspondente, dentro de
  transação revertida, disparou `conservation` e `result_snapshot`;
- `ledger_chain` acusou cadeia rompida (`before=100` após `after=90`) num
  cenário em que `conservation` **passa** (`100 − 10 + 5 = 95`): é a assinatura
  de lost update que a soma não vê e a cadeia vê;
- `reversal_mismatch` acusou `ROLLBACK` de 700 sobre referência de 500 e `REFUND`
  apontado para outra carteira, deixando a reversão honesta passar;
- duas injeções foram recusadas pelo próprio banco (`terminal wager is
  immutable`, `wallet ledger is append-only`), confirmando que essas checagens
  são detectores de guarda removida, não a primeira linha de defesa.

Custo: as duas checagens caras liam `payload->'data'->>'transactionId'` por
linha da outbox. Substituídas por igualdade de contagem por
`(wallet_id, event_type)` — exata porque `aggregate_id` é sempre a carteira,
`event_type` é função 1:1 do status terminal, e status terminal é imutável por
trigger. Contra 3,2 milhões de eventos: **129s para 5,8s**.

`WagerTransactionPendingReference` fica fora dessa igualdade, porque a linha do
wager não guarda que passou por esse estado. Sem efeito financeiro.

## Conformance gerada

`tests/model` gera um fluxo adversarial reprodutível e o aplica com muitos
workers, depois audita. Medido: 6000 operações, 48 workers, 32 carteiras,
~1730 ops/s, 5499 identidades distintas persistidas, nenhuma duplicada, nenhuma
com lançamento em dobro, auditor limpo. A semente é impressa em toda execução.

O gerador expôs uma fronteira de especificação: replay de operação em
`PENDING_REFERENCE` devolve o saldo vivo, não um snapshot — correto, porque a
operação ainda não produziu resultado para congelar. A propriedade do teste
estava errada, não o produto.

## Observabilidade medida

Latência de BET, com 250 rps sobre 24 carteiras:

```
jungle_wager_duration_seconds_count{kind="BET",outcome="PROCESSED",transport="http"} 833
  le=0.0005 →   0      le=0.005 → 756
  le=0.001  →   1      le=0.01  → 802
  le=0.0025 → 668      le=0.025 → 818
```

A massa cai entre 1 ms e 2,5 ms com quatro cortes acima, então p50 e p99 são
calculáveis em vez de interpolados dentro de um balde único. Os buckets foram
escolhidos a partir da distribuição medida; os defaults do Prometheus, cujo menor
corte é 5 ms, colocavam 86% das observações no primeiro balde.

Resultados, publicação e saturação de pool:

```
jungle_wager_results_total{failure_code="",kind="BET",status="PROCESSED"} 133
jungle_wager_results_total{failure_code="INSUFFICIENT_FUNDS",kind="BET",status="REJECTED"} 400
jungle_outbox_published_total{event_type="WagerTransactionProcessed",outcome="published"} 192
jungle_db_pool_connections 20
jungle_db_pool_max_connections 20
jungle_db_pool_empty_acquires_total 62
```

Latência por rota e por statement:

```
jungle_http_request_seconds_count{method="POST",route="/wagering/transactions",status_class="2xx"} 533
jungle_database_query_seconds_count{statement="AllocateID"}        2157
jungle_database_query_seconds_count{statement="update:outbox"}     1146
jungle_database_query_seconds_count{statement="GetWalletForUpdate"} 533
```

16 statements distintos, todos com rótulo limitado: o nome vem do `-- name:` do
sqlc quando existe, senão é derivado como `verbo:tabela` contra uma allowlist —
o que não pertence ao schema colapsa em `other:other` em vez de virar série nova
a partir do texto do SQL.

`tx:commit` medido em 538 commits: 483 abaixo de 0,5 ms, 535 abaixo de 2,5 ms.
Como os guards de integridade são `DEFERRABLE INITIALLY DEFERRED`, eles executam
no commit; a validação diferida não é o gargalo.

Nenhuma série carrega identidade como label.

## Defeitos encontrados e corrigidos

Sob carga, invisíveis na suíte funcional:

- **Publisher estrangulado em ~12 eventos/s.** `Concurrency` servia a três
  papéis e era limitado a `1..10` pelo teto de `MaxNumberOfMessages` do
  `ReceiveMessage`. Separado em `OUTBOX_PUBLISH_BATCH`.
- **Publisher dormia com backlog conhecido.** Após publicar um lote cheio ainda
  esperava `POLL_INTERVAL`, travando a vazão em `lote/intervalo`. Agora drena
  lotes consecutivos até um lote voltar incompleto.
- **Orçamento de conexões estourado.** `MaxConns` fixo em 20 por processo; três
  réplicas mais a suíte de sistema esgotavam `max_connections=100` e escrita
  virava `503`. Agora configurável, ocioso liberado em 2 min, Compose com 200.
- **Corrida em `findIdentity`.** As duas identidades de negócio eram consultadas
  em dois `SELECT` sequenciais, cada um com seu snapshot `READ COMMITTED`; um
  commit entre eles transformava replay legítimo em `409`. Agora é um único
  `SELECT`, um único snapshot, cada braço preso ao seu índice único.

Três defeitos de relógio, todos da mesma classe — carimbo escrito por um
processo e lido contra outro:

- **`wallet_ledger_entry.created_at`** vinha de `time.Now()` na réplica. A página
  do ledger ordena por ele; três réplicas não compartilham relógio. Medidas 8
  divergências em 1.589.285 lançamentos entre `created_at` e `id`; a cadeia
  `balance_before`/`balance_after` mostrou que `created_at` estava certo e `id`
  errado. Passou a `clock_timestamp()` no INSERT, lido de volta com `RETURNING`.
- **`outbox.occurred_at`** ordenava a publicação, e o broker é FIFO por carteira.
  Mesma correção. Sem sintoma local, porque as três réplicas são contêineres na
  mesma VM e compartilham um relógio; sob Kubernetes seriam três nós.
- **`RetryOutbox`** recebia um prazo calculado com `time.Now()`, comparado contra
  `clock_timestamp()` na consulta de claim — o backoff saía errado pelo tamanho
  do desacordo. Passou a enviar a duração. Varridas todas as colunas de tempo do
  esquema: nenhuma outra compara relógios diferentes.

Encontrados por revisão independente e por mutação:

- **Teto de lote contraditório.** A validação aceitava `OUTBOX_PUBLISH_BATCH` até
  500 enquanto `ClaimOutbox` rejeita acima de 100: qualquer valor em 101..500
  subia saudável, passava no readiness e publicava zero eventos para sempre.
  Agora há um único `application.MaxOutboxBatch`.
- **`continue` tratava lote de falhas como progresso.** Com o broker fora, o
  publisher percorria o backlog inteiro a toda velocidade, sem freio, justamente
  no modo de falha em que `POLL_INTERVAL` importa. Agora exige progresso publicado.
- **Testes que passavam contra mutações.** O de paginação passava com a paginação
  desligada; o de goroutines passava com um `Stop` que não esperava os loops; o
  de busy-loop passava com o publisher morto. Reescritos e reverificados por
  mutação, incluindo empate real de `created_at` para exercitar o desempate por `id`.
- **Barreira de float com quatro escapes:** conversão nua `float64(m.minor)`,
  `type Rate float64`, `map[string]float64` e aninhamento além de um nível.
  Fechados; a detecção virou descida recursiva e conversões legítimas exigem
  waiver `//money:allow-float` por linha.
- **Supply chain.** O `vegeta` arrastava sqlite e gonum para o grafo do serviço.
  O harness virou módulo Go próprio.

No próprio ferramental de teste, e mais graves que os anteriores porque mentiam
sobre o produto:

- **Falha de ambiente reportada como violação financeira.** `/dev/shm` do
  PostgreSQL em 64 MB derrubava o auditor sob varredura paralela, e o soak
  registrava isso como dinheiro quebrado. Corrigido com exit 3 para inconclusivo
  contra exit 1 para violação real, e `shm_size: 512m`.
- **`grep | head` sob `pipefail`** morria de SIGPIPE com status 141, que o soak
  lia como "abortado por sinal" e então **descartava uma violação real**.
  Trocado por `grep -m`.

## Limite do laboratório, não do motor

O primeiro soak de uma hora terminou em `[fail]` com o perfil de carga acusando
`878812 of 976610 events unpublished`. Não era vazão do publisher: as três
réplicas estavam a 0,18%, 0,52% e 0,69% de CPU enquanto o LocalStack marcava
acima de 95% e 5,5 GiB, e a reclamação do outbox custava 15 ms por lote de cem,
o que daria 6.600 eventos/s por réplica.

A fila de saída tinha 2.111.642 mensagens e a de entrada, zero: a fila de saída
é o fluxo destinado ao provedor, que não existe na demo, então nada a consome e
o emulador retinha cada evento já enviado. Numa fila nova ele fazia 1.255
envios/s; com dois milhões retidos, cerca de 250/s. Minutos depois morreu por
OOM (`exit=247`), e as três réplicas continuaram servindo.

O soak passou a purgar a fila de saída uma vez por minuto — que é também a taxa
que a AWS permite. Descartar é o descarte certo: o contrato do motor é que um
evento commitado chegue ao broker, e `outbox.published_at` é o que a asserção de
drenagem lê. Na execução seguinte o emulador ficou entre 520 e 706 MiB durante a
hora inteira e o contrato passou.

Duas alavancas de produção ficam registradas e não adotadas. `SendMessageBatch`
multiplica por dez a cota do SQS FIFO (300 msg/s para 3.000), mas medido no
emulador é **duas vezes mais lento** que o envio unitário — adotá-lo pelo número
local seria otimizar o dublê de teste. E as filas são criadas em FIFO padrão, sem
`DeduplicationScope=messageGroup` nem `FifoThroughputLimit=perMessageGroupId`;
ligá-los é seguro neste desenho, porque toda mensagem já leva
`MessageGroupId=walletId` e um `MessageDeduplicationId` globalmente único.

## Limite conhecido do auditor

As checagens `FAIL` fazem varredura completa. Com a outbox inflada por execuções
sucessivas de carga (2,6 milhões de eventos), duas delas levavam 60s e 69s antes
da otimização por contagem. Os ramos `WARN` foram limitados na origem, porque num
`UNION ALL` todo ramo materializa antes do `ORDER BY`.

Três saídas, nenhuma aplicada: limpar o dado acumulado, criar índice funcional
sobre a extração JSON (custa manutenção em toda inserção da outbox, que está no
caminho financeiro), ou gravar `wager_transaction_id` em coluna própria
(migration que toca o caminho de escrita).

## Limites declarados

- Entrega é at-least-once com efeito financeiro effectively-once; não há
  exactly-once.
- LocalStack Community não é prova de enforcement IAM de produção.
- Os números de carga valem para a topologia de um laptop descrita acima e não
  são alegação de capacidade de produção.
- A expiração do TTL de referência para `REJECTED` é exercitada por
  `tests/recovery` com TTL comprimido, não esperada no relógio de produção.
- Não há perfilamento contínuo nem teste de resistência além do soak de uma hora.
- OpenTelemetry, partidas dobradas e dashboards elaborados estão fora do escopo
  entregue.
