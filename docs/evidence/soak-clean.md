<!-- Evidência de uma execução real, copiada sem edição de vereditos.
Reproduza com `make soak`; o script escreve em .chaos-reports/, que não é
versionado porque é diretório de trabalho e específico da máquina. Caminhos
absolutos foram tornados relativos ao repositório; nada mais foi alterado. -->

# Soak report 20260920T224428

## Environment
```
host    : Apple M5, 10 cores (4P+6E), 24GB, load1=4.01
docker  : 4 vCPU, 7936MiB total, 1269MiB used, 6667MiB free
tenancy : 10 containers running, 6 ours, 4 foreign
budget  : start the sweep at 400 rps; the generator runs on the host and will not cap you
verdict : caution
          - 4 container(s) from other projects share this VM; results will not be reproducible
          - Docker holds 4 of 10 host cores; raising it is the single highest-leverage change
```

## Plan
- rate: 400 rps across three replicas
- wallets: 32
- scenarios: kill-app pause-db drop-db-conns partition-sqs pause-sqs flap-sqs partition-idp squeeze-app lease-steal compound kill-db rolling-restart

## Timeline
### kill-app (22:45:00)
[22:45:01] scenario 'kill-app' for 20s
[22:45:01] expectation: SIGKILL one replica, restart it. Expects: peers absorb traffic, no lost or duplicated money, inbox and outbox resume.
jungle-gaming-app-2-1
[22:45:01] killed jungle-gaming-app-2-1; peers on 8080 and 8083 must keep serving
[22:45:23] fault window closed; letting the system settle before the verdict
[ok] chaos-kill-app: every financial invariant holds (0 warning(s)) -- .chaos-reports/audit-20260920T224533-chaos-kill-app.txt
- verdict: invariants held
### pause-db (22:45:58)
[22:45:58] scenario 'pause-db' for 20s
[22:45:59] expectation: SIGSTOP PostgreSQL. Expects: writes fail closed as 503, nothing partially commits, the pool recovers on unpause.
jungle-gaming-postgres-1
[22:45:59] paused jungle-gaming-postgres-1; a stalled dependency is not the same as a dead one
[22:46:21] fault window closed; letting the system settle before the verdict
[ok] chaos-pause-db: every financial invariant holds (0 warning(s)) -- .chaos-reports/audit-20260920T224632-chaos-pause-db.txt
- verdict: invariants held
### drop-db-conns (22:46:58)
[22:46:58] scenario 'drop-db-conns' for 20s
[22:46:58] expectation: pg_terminate_backend every application connection. Expects: in-flight transactions roll back whole, the pool refills.
[22:46:58] terminated 60 application backend(s) mid-flight
[22:47:18] fault window closed; letting the system settle before the verdict
[ok] chaos-drop-db-conns: every financial invariant holds (0 warning(s)) -- .chaos-reports/audit-20260920T224728-chaos-drop-db-conns.txt
- verdict: invariants held
### partition-sqs (22:47:55)
[22:47:55] scenario 'partition-sqs' for 20s
[22:47:55] expectation: Detach LocalStack from the network. Expects: the consumer stops acknowledging, the outbox backlogs, no event is lost.
[22:47:56] detached localstack from jungle-gaming_default
[22:48:16] fault window closed; letting the system settle before the verdict
[ok] chaos-partition-sqs: every financial invariant holds (0 warning(s)) -- .chaos-reports/audit-20260920T224826-chaos-partition-sqs.txt
- verdict: invariants held
### pause-sqs (22:48:56)
[22:48:56] scenario 'pause-sqs' for 20s
[22:48:56] expectation: SIGSTOP LocalStack. Expects: publishes time out, retry with backoff, and republish under the same event_id.
jungle-gaming-localstack-1
[22:48:56] paused jungle-gaming-localstack-1; a stalled dependency is not the same as a dead one
[22:49:16] fault window closed; letting the system settle before the verdict
[ok] chaos-pause-sqs: every financial invariant holds (0 warning(s)) -- .chaos-reports/audit-20260920T224926-chaos-pause-sqs.txt
- verdict: invariants held
### flap-sqs (22:49:57)
[22:49:57] scenario 'flap-sqs' for 20s
[22:49:57] expectation: Detach and reattach the broker repeatedly. Expects: retry storms stay bounded by backoff, no duplicate financial effect.
[22:49:57] flap 1/2: broker detached
[22:50:06] flap 2/2: broker detached
[22:50:14] fault window closed; letting the system settle before the verdict
[warn] chaos-flap-sqs: 19 operational warning(s)
          WARN|stale_unpublished_event|event=01a0c1a6-125b-74e0-b603-9084ef3fe304 type=WagerTransactionProcessed attempts=1 age=00:02:23.356098 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-1263-73fa-a24c-9c5018049e49 type=WalletBalanceChanged attempts=1 age=00:02:23.348144 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-1295-76c1-abd5-72378d6b030d type=WalletBalanceChanged attempts=1 age=00:02:23.297975 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-129e-7de0-a7bb-edf7dc46cb4e type=WalletBalanceChanged attempts=1 age=00:02:23.288539 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-12ea-70c2-82ac-152b45dd7386 type=WalletBalanceChanged attempts=1 age=00:02:23.213332 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-14ce-7d2f-8cb5-5a3f44690a5f type=WalletBalanceChanged attempts=1 age=00:02:22.728593 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-14d1-70d6-ab02-3a1a378af6eb type=WagerTransactionProcessed attempts=1 age=00:02:22.726377 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-14d8-7bbc-9984-07a0b0b1f4e2 type=WagerTransactionProcessed attempts=1 age=00:02:22.718677 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-14db-7707-ad45-b0bd08b0e159 type=WalletBalanceChanged attempts=1 age=00:02:22.715972 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-14dd-7d77-a2dd-aabe6b2ab6d0 type=WalletBalanceChanged attempts=1 age=00:02:22.713563 error=PUBLISH_FAILED
[ok] chaos-flap-sqs: every financial invariant holds (19 warning(s)) -- .chaos-reports/audit-20260920T225024-chaos-flap-sqs.txt
- verdict: invariants held
### partition-idp (22:50:58)
[22:50:58] scenario 'partition-idp' for 20s
[22:50:58] expectation: Detach Keycloak. Expects: already-issued tokens keep working, liveness does not depend on the IdP.
[22:50:59] detached keycloak from jungle-gaming_default
[22:51:19] fault window closed; letting the system settle before the verdict
[warn] chaos-partition-idp: 19 operational warning(s)
          WARN|stale_unpublished_event|event=01a0c1a6-8e07-742d-aab9-b43f8edcfdae type=WalletBalanceChanged attempts=1 age=00:02:56.95838 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e21-701c-b107-56d33227c8ee type=WalletBalanceChanged attempts=1 age=00:02:56.932622 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e27-7dfa-8e6a-0d67387148b9 type=WagerTransactionProcessed attempts=1 age=00:02:56.925764 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e2d-7417-b012-66cdd6f78ba8 type=WagerTransactionProcessed attempts=1 age=00:02:56.920375 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e32-7af1-a96a-b3f380ba23ba type=WalletBalanceChanged attempts=1 age=00:02:56.914959 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e34-73ac-b371-ec8bd54cd8d3 type=WagerTransactionProcessed attempts=1 age=00:02:56.913398 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e37-7035-9f72-85e151ab0fae type=WalletBalanceChanged attempts=1 age=00:02:56.910617 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e39-7c6c-b73a-94193d721058 type=WagerTransactionProcessed attempts=1 age=00:02:56.907835 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-8e82-7c55-af05-c9ada8e8b4ca type=WagerTransactionProcessed attempts=1 age=00:02:56.83483 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a6-c609-7a28-8a3d-2f3db4cc58f7 type=WalletBalanceChanged attempts=1 age=00:02:42.619921 error=PUBLISH_FAILED
[ok] chaos-partition-idp: every financial invariant holds (19 warning(s)) -- .chaos-reports/audit-20260920T225129-chaos-partition-idp.txt
- verdict: invariants held
### squeeze-app (22:52:01)
[22:52:01] scenario 'squeeze-app' for 20s
[22:52:01] expectation: Clamp one replica below its working set to force the OOM killer. Expects: restart recovers, claims released by lease, limit restored.
[22:52:02] clamped jungle-gaming-app-1-1 to 21MiB against a 43MiB working set
[22:52:41] fault window closed; letting the system settle before the verdict
[warn] chaos-squeeze-app: 11 operational warning(s)
          WARN|stale_unpublished_event|event=01a0c1a7-bc9c-744f-9adc-f5f542683aed type=WalletBalanceChanged attempts=1 age=00:03:00.565265 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-0362-75b5-bbe3-54c749661977 type=WagerTransactionProcessed attempts=1 age=00:02:42.447173 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-0cec-73c9-8c40-75185da08186 type=WalletBalanceChanged attempts=1 age=00:02:40.005316 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-0cf6-7911-b85a-7eb89eaade6f type=WagerTransactionProcessed attempts=1 age=00:02:39.994956 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-0d3e-7f86-a3c4-28400b379aae type=WagerTransactionProcessed attempts=1 age=00:02:39.922554 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-0d41-781d-af1a-6bea2fad0ff1 type=WagerTransactionProcessed attempts=1 age=00:02:39.920026 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-0d5a-79e0-bb8a-91c217f564e8 type=WagerTransactionProcessed attempts=1 age=00:02:39.894905 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-2c07-7e9b-9f98-939652fa81af type=WalletBalanceChanged attempts=1 age=00:02:32.041602 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-2c1d-7ebf-9641-bf3ee15580d1 type=WalletBalanceChanged attempts=1 age=00:02:32.019584 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1a8-4392-7be8-832e-8e37f0f7fe00 type=WalletBalanceChanged attempts=0 age=00:02:26.014762 error=-
[ok] chaos-squeeze-app: every financial invariant holds (11 warning(s)) -- .chaos-reports/audit-20260920T225251-chaos-squeeze-app.txt
- verdict: invariants held
### lease-steal (22:53:31)
[22:53:31] scenario 'lease-steal' for 20s
[22:53:31] expectation: Freeze one replica past its lease, let peers steal its claims, then thaw it. Expects: the stale owner's fencing token is refused, so no claim completes twice.
jungle-gaming-app-1-1
[22:53:31] froze jungle-gaming-app-1-1 for 75s, past its OCI runtime exec failed: exec failed: unable to start container process: exec: "printenv": executable file not found in $PATH
60s lease; peers should steal its claims
[22:54:46] thawed jungle-gaming-app-1-1; its in-flight claims are now fenced
[22:55:33] fault window closed; letting the system settle before the verdict
[warn] chaos-lease-steal: 20 operational warning(s)
          WARN|stale_unpublished_event|event=01a0c1aa-ee32-7ab7-a2fc-645cedb5c254 type=WagerTransactionProcessed attempts=0 age=00:02:23.760043 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee32-7d23-9bce-ea384f471268 type=WalletBalanceChanged attempts=0 age=00:02:23.759883 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee3a-7578-bcc1-7358e74b3b43 type=WagerTransactionProcessed attempts=0 age=00:02:23.752357 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee3a-77bf-ba17-d6f352803a90 type=WalletBalanceChanged attempts=0 age=00:02:23.752231 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee3e-7f5b-8ff4-ad3598f57ada type=WagerTransactionProcessed attempts=0 age=00:02:23.74754 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee3f-74da-bd8d-4e9e1edf709a type=WalletBalanceChanged attempts=0 age=00:02:23.747414 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee41-7339-9748-aa2440b42706 type=WagerTransactionProcessed attempts=0 age=00:02:23.745504 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee41-7552-8712-ce74dee1e613 type=WalletBalanceChanged attempts=0 age=00:02:23.745392 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee43-7c87-86b4-d4758c5d85bf type=WagerTransactionProcessed attempts=0 age=00:02:23.742926 error=-
          WARN|stale_unpublished_event|event=01a0c1aa-ee43-7eba-9ef3-f12b8d80eba3 type=WalletBalanceChanged attempts=0 age=00:02:23.742797 error=-
[ok] chaos-lease-steal: every financial invariant holds (20 warning(s)) -- .chaos-reports/audit-20260920T225543-chaos-lease-steal.txt
- verdict: invariants held
### compound (22:56:24)
[22:56:24] scenario 'compound' for 20s
[22:56:24] expectation: Kill a replica and freeze the broker at the same instant. Expects: two subsystems failing together still converge, with no lost or duplicated money.
jungle-gaming-app-2-1
jungle-gaming-localstack-1
[22:56:24] killed jungle-gaming-app-2-1 and froze the broker together for 20s
[22:56:47] fault window closed; letting the system settle before the verdict
[warn] chaos-compound: 20 operational warning(s)
          WARN|stale_unpublished_event|event=01a0c1ab-3938-7651-80b0-82dae6cc6e20 type=WalletBalanceChanged attempts=1 age=00:03:18.479829 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1ab-b8af-7dfa-964f-1432e07e8b40 type=WalletBalanceChanged attempts=0 age=00:02:45.848329 error=-
          WARN|stale_unpublished_event|event=01a0c1ab-b8b0-7978-b618-a6f8d0fc2f92 type=WalletBalanceChanged attempts=0 age=00:02:45.847332 error=-
          WARN|stale_unpublished_event|event=01a0c1ab-b8c0-76ea-8121-1d3eba299a33 type=WagerTransactionProcessed attempts=0 age=00:02:45.831792 error=-
          WARN|stale_unpublished_event|event=01a0c1ab-b8c1-7023-91d5-95b256595882 type=WalletBalanceChanged attempts=0 age=00:02:45.831223 error=-
          WARN|stale_unpublished_event|event=01a0c1ab-b92e-7d7f-8aa2-bd67fccb8cef type=WalletBalanceChanged attempts=0 age=00:02:45.721405 error=-
          WARN|stale_unpublished_event|event=01a0c1ab-bd1f-7fad-8f57-ce894e1e8ed9 type=WagerTransactionProcessed attempts=1 age=00:02:44.711929 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1ab-bd4d-7b98-9eb9-3f752c990629 type=WagerTransactionProcessed attempts=1 age=00:02:44.666518 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1ac-4488-788a-95e9-d435dcf78d59 type=WalletBalanceChanged attempts=0 age=00:02:10.047288 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-448d-79dc-be97-300e8e048162 type=WagerTransactionProcessed attempts=0 age=00:02:10.042603 error=-
[ok] chaos-compound: every financial invariant holds (20 warning(s)) -- .chaos-reports/audit-20260920T225657-chaos-compound.txt
- verdict: invariants held
### kill-db (22:57:28)
[22:57:28] scenario 'kill-db' for 20s
[22:57:28] expectation: SIGKILL PostgreSQL and restart it. Expects: no torn transaction survives, pgxpool reconnects without restarting the apps.
jungle-gaming-postgres-1
[22:57:49] waiting for PostgreSQL to accept connections again
[22:57:53] fault window closed; letting the system settle before the verdict
[warn] chaos-kill-db: 20 operational warning(s)
          WARN|stale_unpublished_event|event=01a0c1ac-f7c5-722d-a607-eb50300c89b8 type=WagerTransactionProcessed attempts=0 age=00:02:30.313869 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7c5-749d-bc9b-28e7546e48ea type=WalletBalanceChanged attempts=0 age=00:02:30.313674 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7c8-75e3-99c9-b81ece516628 type=WalletBalanceChanged attempts=0 age=00:02:30.310614 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7cc-730a-9254-7450b2a4f791 type=WagerTransactionProcessed attempts=0 age=00:02:30.306782 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7cc-7627-bf6d-f6d42f7bf2b7 type=WalletBalanceChanged attempts=0 age=00:02:30.306607 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7ce-7902-bab9-cc41aed8f515 type=WagerTransactionProcessed attempts=0 age=00:02:30.304416 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7ce-7be9-83e6-2aad9ac155e3 type=WalletBalanceChanged attempts=0 age=00:02:30.304224 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7d0-7e6e-8008-0f96f568f5c5 type=WalletBalanceChanged attempts=0 age=00:02:30.302116 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7d4-79f3-bac2-db5bd06caa8d type=WagerTransactionProcessed attempts=0 age=00:02:30.298343 error=-
          WARN|stale_unpublished_event|event=01a0c1ac-f7d4-7d1e-b46a-c1df2015189b type=WalletBalanceChanged attempts=0 age=00:02:30.298171 error=-
[ok] chaos-kill-db: every financial invariant holds (20 warning(s)) -- .chaos-reports/audit-20260920T225803-chaos-kill-db.txt
- verdict: invariants held
### rolling-restart (22:58:36)
[22:58:36] scenario 'rolling-restart' for 20s
[22:58:36] expectation: SIGTERM each replica in turn. Expects: graceful drain, in-flight work completes, readiness flips before the socket closes.
[22:58:36] draining app-1
[22:58:38] draining app-2
[22:58:42] draining app-3
[22:58:44] fault window closed; letting the system settle before the verdict
[warn] chaos-rolling-restart: 20 operational warning(s)
          WARN|stale_unpublished_event|event=01a0c1ac-a93d-7c9e-8dc1-b2093ad4189c type=WalletBalanceChanged attempts=1 age=00:03:41.048129 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1ac-a948-7e98-81a6-2405bb9098f8 type=WagerTransactionProcessed attempts=1 age=00:03:41.026927 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1ac-a955-7cd7-9997-c6d7501db644 type=WalletBalanceChanged attempts=1 age=00:03:41.01016 error=PUBLISH_FAILED
          WARN|stale_unpublished_event|event=01a0c1ad-e8fc-7915-b5d6-a6fa64e1eb8c type=WalletBalanceChanged attempts=0 age=00:02:19.193833 error=-
          WARN|stale_unpublished_event|event=01a0c1ad-e901-7f56-b9b8-d02ee2408e0b type=WalletBalanceChanged attempts=0 age=00:02:19.188426 error=-
          WARN|stale_unpublished_event|event=01a0c1ad-e903-7da0-b686-af9069c64c59 type=WagerTransactionProcessed attempts=0 age=00:02:19.186542 error=-
          WARN|stale_unpublished_event|event=01a0c1ad-e903-7fd6-a80d-108bf310810f type=WalletBalanceChanged attempts=0 age=00:02:19.186403 error=-
          WARN|stale_unpublished_event|event=01a0c1ad-e909-7c06-94a5-8d7275ea0778 type=WagerTransactionProcessed attempts=0 age=00:02:19.180606 error=-
          WARN|stale_unpublished_event|event=01a0c1ad-e909-7ec3-af23-b695a1b9871e type=WalletBalanceChanged attempts=0 age=00:02:19.18043 error=-
          WARN|stale_unpublished_event|event=01a0c1ad-e90b-7de5-9e5e-1e1265c8315b type=WagerTransactionProcessed attempts=0 age=00:02:19.178512 error=-
[ok] chaos-rolling-restart: every financial invariant holds (20 warning(s)) -- .chaos-reports/audit-20260920T225854-chaos-rolling-restart.txt
- verdict: invariants held

## Load profile
```
    chaos_test.go:80: [chaos] t+3140s map[201:4000]
    chaos_test.go:80: [chaos] t+3150s map[201:4000]
    chaos_test.go:80: [chaos] t+3160s map[201:4000]
    chaos_test.go:80: [chaos] t+3170s map[201:4000]
    chaos_test.go:80: [chaos] t+3180s map[201:4000]
    chaos_test.go:80: [chaos] t+3190s map[201:4000]
    chaos_test.go:80: [chaos] t+3200s map[201:4000]
    chaos_test.go:80: [chaos] t+3210s map[201:4000]
    chaos_test.go:80: [chaos] t+3220s map[201:4000]
    chaos_test.go:80: [chaos] t+3230s map[201:4000]
    chaos_test.go:80: [chaos] t+3240s map[201:4000]
    chaos_test.go:80: [chaos] t+3250s map[201:4000]
    chaos_test.go:80: [chaos] t+3260s map[201:4000]
    chaos_test.go:80: [chaos] t+3270s map[201:4000]
    chaos_test.go:80: [chaos] t+3280s map[201:4000]
    chaos_test.go:80: [chaos] t+3290s map[201:4000]
    chaos_test.go:80: [chaos] t+3300s map[201:4000]
    chaos_test.go:80: [chaos] t+3310s map[201:4000]
    chaos_test.go:80: [chaos] t+3320s map[201:4000]
    chaos_test.go:80: [chaos] t+3330s map[201:4000]
    chaos_test.go:80: [chaos] t+3340s map[201:4000]
    chaos_test.go:80: [chaos] t+3350s map[201:4000]
    chaos_test.go:80: [chaos] t+3360s map[201:4000]
    chaos_test.go:80: [chaos] t+3370s map[201:4000]
    chaos_test.go:80: [chaos] t+3380s map[201:4000]
    chaos_test.go:80: [chaos] t+3390s map[201:4000]
    chaos_test.go:80: [chaos] t+3400s map[201:4000]
    chaos_test.go:80: [chaos] t+3410s map[201:4000]
    chaos_test.go:80: [chaos] t+3420s map[201:4000]
    chaos_test.go:80: [chaos] t+3430s map[201:4000]
    chaos_test.go:80: [chaos] t+3440s map[201:4000]
    chaos_test.go:80: [chaos] t+3450s map[201:4000]
    chaos_test.go:80: [chaos] t+3460s map[201:4000]
    chaos_test.go:80: [chaos] t+3470s map[201:4000]
    chaos_test.go:80: [chaos] t+3480s map[201:1]
    chaos_test.go:119: [outbox] drained backlog=263483 in 4m28.333s (982 events/s)
    chaos_test.go:121: [chaos] contract held: 1366823 acknowledged, 1366823 persisted, zero duplicated, zero unfinished
    chaos_test.go:122: [chaos] run scripts/audit.sh for the database-internal invariants
--- PASS: TestSteadyLoadUnderChaos (3753.57s)
ok  	jungle/tests/load	3754.311s
```

## Final audit
```
[ok] final: every financial invariant holds (0 warning(s)) -- .chaos-reports/audit-20260920T234708-final.txt
```

## Verdict
- load profile exit: 0
- final audit exit: 0
- every scenario left the ledger consistent
- scenarios injected: 12 of 12
- the output queue was purged once a minute: the demo has no provider consuming it, and an emulator holding every event it was ever sent slows down until the run measures the emulator instead of the engine. Delivery is judged by outbox.published_at, not by queue depth.
