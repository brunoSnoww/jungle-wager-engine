# Decisões de arquitetura

## Caminho de uma operação

As duas portas de entrada convergem para o mesmo caso de uso, e tudo o que uma
operação altera cabe numa transação só. É daí que vem a garantia central: não
existe evento publicado para um saldo que não persistiu, porque o evento nasce
dentro do mesmo commit que o saldo.

```
   provedor --- HTTP ---+                    +--- SQS FIFO --- provedor
     Idempotency-Key    |                    |    envelope messageId
                        v                    v
          +---------------------------------------------+
          |   ProcessWager  (application)               |
          |   mesma porta, mesmas garantias             |
          +----------------------+----------------------+
                                 |
   +=============================v==============================+
   |  UMA transação PostgreSQL, READ COMMITTED                  |
   |                                                            |
   |   SELECT ... FOR UPDATE wallet                             |
   |        ponto de serialização, por carteira: carteiras      |
   |        distintas avançam em paralelo, sem lock global      |
   |                                                            |
   |     wager_transaction    identidade externa e estado       |
   |     wallet               saldo e versão                    |
   |     wallet_ledger_entry  append-only, imutável por trigger |
   |     inbox                (consumer, messageId)  [só SQS]   |
   |     outbox               evento com published_at NULL      |
   |                                                            |
   +=============================+==============================+
                                 |  COMMIT
                                 v
                 nada sai para o broker antes daqui
```

A publicação é outro processo, e disputada. Cada réplica reclama registros da
outbox com lease e token de cercamento, de modo que um publisher morto tem a
reivindicação expirada em vez de reter as linhas para sempre.

```
    app-1            app-2            app-3
      |                |                |     três réplicas,
      +----------------+----------------+     um único backlog
                       |
                       |  claim com lease + fencing token
                       v
                 +-----------+
                 |  outbox   |  ORDER BY next_attempt_at,
                 +-----+-----+           occurred_at, event_id
                       |
                       |  SendMessage, MessageGroupId = walletId
                       v
              SQS FIFO de saída  ---->  DLQ após 5 recebimentos
```

Uma reversão que chega antes da transação que referencia não bloqueia a entrada:
ela é persistida como `PENDING_REFERENCE` e um terceiro worker a retoma com
backoff exponencial, sobrevivendo a reinício, até resolver ou esgotar o prazo.

## Fronteiras e composição

`domain` importa apenas a biblioteca padrão. `application` declara comandos,
resultados, normalização, hashes e portas pequenas. HTTP e SQS usam a mesma
porta `ProcessWager`. O adapter PostgreSQL implementa sua unidade transacional,
delegando as regras de saldo/reversão/transição ao domínio. Nenhum repositório
faz commit independente dentro desse processamento.

`modules` é a raiz Fx: configuração, observabilidade, persistência,
autenticação, aplicação, mensageria, workers e HTTP. Dependências são injetadas
por construtores. Não há registrador global, `init()` de composição ou estado
financeiro compartilhado em memória entre instâncias.

## Money e entidades

Money encapsula `int64` de unidades mínimas e moeda. BRL, USD e EUR são o conjunto
ISO 4217 suportado, todos com duas casas; não se promete suporte universal a
moedas de outras escalas. `25`, `25.0` e `25.00` normalizam para `25.00`.
Não se aceita negativo externo, exponentes, NaN/Infinity, sinal `+`, espaços,
escala excedente ou overflow. Diferenças internas podem ser negativas. Soma,
subtração, negação e comparação validam moeda e limites; o valor zero de Go é
inválido. O JSON de dinheiro usa strings, nunca números float.

Wallet e WagerTransaction têm campos encapsulados, snapshots copiados e criação
separada da reidratação. Carteira começa na versão 1 e muda de versão somente
com saldo. LOSS exige zero, não gera ledger nem incremento da versão.

Uma abertura positiva cria OPENING interno processado, ledger CREDIT de zero
até o saldo inicial e dois eventos no mesmo commit. Abertura zero cria apenas a
carteira. O par jogador/moeda é único. OPENING externo é recusado.

IDs internos de carteira, operação, lançamento e evento são UUIDv7 gerados no
PostgreSQL. Quando o construtor precisa do ID antes do INSERT, SQLC executa
`SELECT uuidv7()` dentro da mesma transação. O ID do envelope é exatamente o
ID da outbox. Identidades externas não são regeneradas. UUIDv7 não determina
ordem de commit: timestamps explícitos e cursor `(created_at,id)` permanecem.

O `created_at` do lançamento é carimbado por `clock_timestamp()` no INSERT e
lido de volta, nunca enviado pela aplicação. A página do ledger ordena por ele,
e as réplicas não compartilham relógio: três processos carimbando de três
relógios ordenam dois lançamentos contra a ordem em que o lock da carteira foi
de fato concedido. Medido nesta máquina, host contra PostgreSQL divergiam
16,4 ms, com 114 µs entre os lançamentos vizinhos — folga negativa de duas
ordens de grandeza. `clock_timestamp()` é um relógio só para toda a frota e
avança dentro da transação, então lançamentos do mesmo commit ainda separam.
Ele não é monótono (NTP pode saltá-lo), mas um relógio ocasionalmente ajustado
é melhor que três em desacordo permanente, e o desempate por `id` cobre empates.
Consequência do defeito era ordem de leitura errada, não corrupção: a cadeia
`balance_before`/`balance_after` e a reconciliação independem de ordem.

O `occurred_at` da outbox segue a mesma regra, e pelo mesmo motivo: o publisher
reclama por `ORDER BY next_attempt_at, occurred_at, event_id` e o broker é FIFO
por carteira, então relógio de aplicação ali deixa duas réplicas enviarem os
eventos de uma carteira numa ordem que o banco nunca cometeu. A coluna não é o
mesmo carimbo que está dentro do payload: o payload diz quando o evento
aconteceu, o consumidor o lê e o dobra no hash de inbox, e por isso ele
permanece o valor de domínio, estável entre reentregas. A coluna decide ordem
de envio e vem de `clock_timestamp()`.

## Transação financeira e concorrência

READ COMMITTED com `SELECT ... FOR UPDATE` da wallet é o ponto de serialização.
A ordem é wallet antes de wagers, também nos retries de referência. Nunca se
adquire um segundo lock de wallet para validar uma referência incompatível.
Carteiras diferentes avançam independentemente.

O fluxo reserva inbox quando aplicável, verifica identidades já confirmadas,
bloqueia wallet para operação nova, insere PENDING, decide no domínio, persiste
resultado, altera saldo/ledger quando necessário, insere eventos e completa
inbox, tudo em um único `pgx.Tx`. Depois do commit vêm resposta/ACK.

`ON CONFLICT DO NOTHING RETURNING` seguido de outro comando SELECT resolve a
corrida de idempotência com novo snapshot READ COMMITTED. Não se captura uma
violação de unicidade para continuar usando uma transação abortada. Uma
rejeição financeira é resultado durável, não erro que provoque rollback da
auditoria. Falha transitória antes do commit reverte todos os efeitos.

PENDING nunca é confirmado sozinho; há guard deferred no banco. O único estado
intermediário confirmado é PENDING_REFERENCE, retomável por outra instância.
Constraints impõem saldo não negativo, unicidades, vínculo carteira/jogador,
origem interna/externa, equação do ledger e referências relacionais. Triggers
proíbem UPDATE/DELETE do ledger, mutação de operação terminal e alteração do
snapshot da outbox. Administrador do banco continua sendo autoridade confiável.

## Idempotência

Há duas unicidades por provider: chave recebida e ID externo. Mesma chave com
hash equivalente retorna o resultado persistido, inclusive saldo/versão
originais; hash diferente conflita. Mesmo ID externo com chave diferente também
conflita. Consultas e replays HTTP usam o provider autenticado.

Hash financeiro SHA-256: JSON com chaves ordenadas; campos provider, ID externo,
jogador, wallet, rodada, jogo, kind, money e referência externa. UUIDs são
canônicos; kind maiúsculo; dinheiro com duas casas; ausência de referência vira
string vazia. Chave de idempotência e metadados de transporte são excluídos.

Inbox usa `(consumer_name,message_id)` do envelope, não ID gerado pelo SQS.
Seu hash é separado: tipo, ocorrência UTC e data canônico, **incluindo a chave**.
Mudar a chave de um envelope já recebido não pode ser ocultado por ACK de
duplicata. Inbox e resultado financeiro são confirmados juntos.

## Reversões e pendências

BET debita; WIN credita; LOSS não movimenta; REFUND restitui uma BET inteira;
ROLLBACK inverte BET, WIN ou REFUND integralmente. Referência deve concordar em
provider, jogador, wallet, moeda e rodada. WIN pode referenciar BET como contexto.

Uma referência admite no máximo uma reversão financeira direta bem-sucedida
entre REFUND e ROLLBACK. Rollback de refund referencia o refund e não reabre a
BET original para novo crédito. Reversão debitória sem saldo usa
`INSUFFICIENT_FUNDS_FOR_REVERSAL`, distinto de `INSUFFICIENT_FUNDS` da BET.

Referência ausente ou ainda pendente gera PENDING_REFERENCE. Referência terminal
sem sucesso ou incompatível rejeita definitivamente. Worker faz claims curtas
com SKIP LOCKED, lease e token novo a cada aquisição; valida token/estado sob lock
antes de processar. Defaults: até 12 tentativas, TTL 24h e backoff limitado a 1h.
Esgotamento enquanto a dependência permanece ausente gera REJECTED com
REFERENCE_NOT_FOUND e evento. Estados terminais não reabrem.

## Outbox, SQS e recuperação

O claim da outbox é transação curta; rede acontece fora dela. Claim token serve
como fencing: worker antigo não marca ou reagenda claim readquirida, mesmo com
igual instance ID. Itens de um batch são publicados concorrentemente, com
timeout menor que lease; não ficam esperando sequencialmente até a lease vencer.

Publicar e marcar no banco não são atomicamente coordenáveis. Crash entre ambos
causa republicação com o mesmo eventId. FIFO deduplica por janela, mas inbox e
consumidores idempotentes fornecem proteção persistente. Entrega é at-least-once,
com efeito financeiro effectively-once; nenhuma promessa de exactly-once.

Múltiplos publishers podem enviar eventos de uma wallet fora da ordem dos
commits. FIFO preserva ordem de chegada à fila, não reconstitui causalidade SQL.
Snapshots usam walletVersion para evitar regressão. Eventos de transações
distintas não podem ser descartados por terem versão menor.

As filas são criadas em FIFO padrão, cuja cota é 300 msg/s por fila sem lote e
3.000 com lote de dez. Duas alavancas ficam disponíveis e não aplicadas, porque
nenhuma das duas pode ser medida aqui. `SendMessageBatch` multiplica a cota por
dez na AWS; no emulador local ele é duas vezes mais lento que o envio unitário,
então o número local recomendaria o contrário do certo. E
`DeduplicationScope=messageGroup` com `FifoThroughputLimit=perMessageGroupId`
liga o modo de alta vazão, que sobe a cota para milhares por segundo e por
região; é seguro neste desenho porque toda mensagem já carrega
`MessageGroupId=walletId` e um `MessageDeduplicationId` globalmente único, de
modo que estreitar o escopo de dedup não muda o que é deduplicado. Ambas são
decisões de provisionamento, não de código de aplicação.

Timeout/erro transitório mantém mensagem para retry; permanentemente inválida
atinge DLQ via redrive. O consumidor não faz publish manual na DLQ seguido de
delete, evitando outra janela de perda. Outbox não tem descarte por tentativas:
backoff limitado mais métrica de atraso. Reprocessar DLQ requer investigação e
preservação das identidades; não há replay automático cego de rejeições.

## Autenticação e broker

Keycloak externo em client_credentials. Verificador exige RS256, assinatura,
issuer, audience, exp e scopes; provider_id só vale após validação. JWKS/discovery
vêm exclusivamente do issuer configurado, com cache/refresh limitado; nenhum URL
do token é acessado. `OIDC_FETCH_ISSUER` permite rota interna Docker sem relaxar
issuer assinado. HTTP não pode escolher provider diferente do principal.

Operações de carteira exigem cliente interno sem provider_id e scope específico.
Provedores só leem suas próprias transações. Health checks são públicos.
`/metrics` deve ficar restrito à rede de observabilidade fora do ambiente local.

A fila de entrada é privada e compartilhada entre produtores **internos
confiáveis**. O corpo não autentica provider: uma allowlist de providers é defesa
adicional, não substitui IAM. Fixtures de policies separam produtor e runtime;
provedores externos recebem somente credenciais HTTP. LocalStack Community não
constitui prova de enforcement IAM de produção. Credenciais `test` e secrets do
realm são deliberadamente locais; não existe configuração pronta para produção.

## Lifecycle

OnStart verifica pool, discovery/JWKS e filas; faz bind HTTP e inicia goroutines
sem bloquear o hook. Falha fatal do servidor pede shutdown ao Fx. OnStop marca
readiness negativa, para receives/claims, drena HTTP, aguarda jobs em voo e só
então fecha o pool. Contexto de trabalho não é cancelado no início do drain.
Ao exceder prazo, cancela I/O; mensagens voltam por visibility e leases expiram.
Defaults: processamento 30s, shutdown 45s, visibility/lease 60s. A configuração
rejeita combinações de prazos inseguras.

## Tetos de capacidade

O sistema tem dois tetos distintos e eles não se medem com o mesmo teste.

O caminho financeiro serializa por wallet via `SELECT ... FOR UPDATE`. O teto de
paralelismo é, portanto, o número de wallets distintas em disputa, não o número
de réplicas nem de conexões. Carga sobre uma única wallet mede o custo da fila
de lock; carga sobre wallets independentes mede a vazão real. Medir só o
primeiro caso e chamar de throughput seria descrever a invariante como defeito.

O publisher da outbox é um teto separado e mais baixo que a ingestão HTTP.
`OUTBOX_PUBLISH_BATCH` é deliberadamente independente de `WORKER_CONCURRENCY`:
este último é limitado a dez pelo `MaxNumberOfMessages` do `ReceiveMessage`, e
deixar esse limite de entrada governar a saída estrangula a publicação. Um lote
cheio indica backlog e é seguido de outra aquisição imediata, sem esperar o
intervalo de polling; um lote incompleto volta ao ritmo normal. O lote inteiro é
publicado em uma única onda concorrente, então a lease precisa cobrir a onda.

O orçamento de conexões é

    max_replicas * DB_MAX_CONNS + migrations + operadores <= max_connections

e o multiplicador é o **teto do autoscaler**, não a contagem em regime. Um
rolling update ainda soma `maxSurge` por cima. Orçar pelas réplicas habituais é
como um evento de scale-out transforma toda escrita em `503` — e um autoscaler
por CPU escala justamente quando o gargalo é lock e conexão, agravando o
incidente que deveria aliviar. Conexões ociosas são liberadas em dois minutos
para que uma rajada não retenha o pool depois de terminada.

## Obrigações do orquestrador

Três garantias o processo não consegue impor sozinho; são contrato de deploy.

O **prazo de morte** precisa ficar acima de `SHUTDOWN_TIMEOUT`. No Compose é
`stop_grace_period` (55s aqui). No Kubernetes é `terminationGracePeriodSeconds`,
cujo default de 30s fica **abaixo** dos 45s de drenagem e mataria o processo no
meio dela, sem executar a liberação de claims.

A **remoção do endpoint** é concorrente ao `SIGTERM`: tráfego ainda chega depois
do sinal. Em Kubernetes isso pede um `preStop` que apenas espera, para que a
readiness negativa propague antes do socket fechar.

A **ordem das migrations** aqui vem de `depends_on: service_completed_successfully`,
que é garantia mais forte que a do Kubernetes por padrão. Um `Job` mais um
`Deployment` não têm ordenação sem hook de Helm, initContainer ou sync wave, e
pods podem subir contra um banco não migrado.

Sobre paralelismo não há obrigação: desde o Go 1.25 o runtime deriva
`GOMAXPROCS` do limite de CPU do cgroup, então nenhuma biblioteca é necessária.
O processo registra `gomaxprocs` e `visible_cpus` no startup porque a diferença
entre os dois é onde o throttling de CFS se esconde.

Números medidos e a topologia a que pertencem estão em
[VALIDATION.md](VALIDATION.md).

## Reconciliação e operação

Uma única consulta SQL usa snapshot MVCC consistente e soma NUMERIC exata para
evitar overflow de totais históricos. O saldo líquido é convertido com limite
int64; diferença é saldo armazenado menos reconstruído. Reconciliação nunca
altera dados; divergências aparecem na resposta, logs e métrica.

Logs estruturados não carregam payload completo, segredos ou receipt handles.
Métricas têm labels limitadas, sem identidades. Tempo de query FOR UPDATE inclui
espera de lock e execução, não é apresentado como medição isolada de lock wait.
OpenTelemetry, double-entry e testes de carga permanecem opcionais; não foram
substituídos por afirmações de performance sem medição.

## Validação e limites de entrega

Veja [VALIDATION.md](VALIDATION.md) para evidência executada. Código presente não
é sinônimo de cenário validado: os gates obrigatórios — Compose em checkout
limpo, schema, auth real contra Keycloak, três processos e janelas de crash —
foram executados com `-race` contra infraestrutura real, e é essa execução, não
o código, que sustenta a entrega.
