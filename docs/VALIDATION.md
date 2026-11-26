# 테스트와 실측 결과

현재 진척은 [CONTEXT](CONTEXT.md), 실행·중단·재개는 [RUNBOOK](RUNBOOK.md), 데이터 의미는 [ARCHITECTURE](ARCHITECTURE.md)와 [API_CONTRACTS](API_CONTRACTS.md)가 기준이다. 이 문서는 최신 결과·측정 조건·한계·재현 근거만 기록한다. 아래 수치는 실측과 조건부 환산을 구분한다.

## 현재 검증 결과

- 2026-11-27 공동 예산 브랜치에서 오프라인 단위 회귀 **579 passed**, 격리 Docker PostgreSQL 16의 통합 운영 검증 **30 passed**. 시간 한도, 쿠팡 리포트 몫과 본문 403의 공동 차단, HUB 동시 예약, 기존 기록·함수 권한 보존, 독립 경로의 HTTP 전 예약을 확인했다. [회귀 실행 방법](#회귀-실행-방법)

2026-11-26 PostgreSQL 저장 모듈 분리 후 오프라인 단위 회귀 **579 passed**(12.51초), 격리 WSL의 Python 3.12·PostgreSQL 16.15 통합 회귀 **252 passed**(71.36초)다. 기존 상품 저장·이력, 재개·발행 및 쿠팡 완료 발행본 내보내기를 검증했다. 통합 실행과 migration의 오프라인 계획도 확인했다. 최신 JUnit은 `artifacts/validation/unit.xml`, `integration.xml`이다. [회귀 실행 방법](#회귀-실행-방법)

## 오너클랜 5000개 표본 E2E

**약 5,000개 라이브 수집의 완료 시간과 지속 처리량은 미확인이다.** 최초 진단은 개별 상품이 보완 후에도 빠지면 이후 전송을 막는 시험용 조건으로 중단됐다. 이 조건을 제거했고 실제 worker의 요청 `[100,1,100]`에서 정상 199개 저장, 미반환 1개 pending 보존, 다음 묶음 진행을 격리 PostgreSQL로 확인했다. 점수와 발행은 상세 미완료 상태에서 차단된다.

- 11월 22일 라이브 진단은 직전 KST 하루의 이력 앞 12페이지에서 12,000개 신호와 고유 상품 4,712개를 골랐다. 세 묶음에서 299개를 저장했으며 누락 1개는 보완에서도 HTTP 200과 빈 배열로 돌아왔다. 미요청 4,412개를 실제 미반환으로 세지 않는다. 당시 최종 실행은 HTTP 17회, 준비 진단을 포함하면 46회였다.
- 수정 후 라이브 시험은 요청으로 중단됐다. 상세 요청 20회와 정상 응답 합계 1,194개가 기록됐다. 마지막 요청의 완료 여부, 고유 상품 수, 저장 완료량은 미확인이다.

재현은 `scripts.probes.ownerclan_live_e2e`의 무접속 plan부터 확인한다. 과거 진단과 보완 조건은 `artifacts/experiments/ownerclan-live-e2e/`, 수정 이후 부분 실행은 `artifacts/experiments/ownerclan-workflow-e2e/measurement.json`, worker 회귀는 `tests/integration/test_ownerclan_probe_continuation.py`에 있다.

### 5000개 저장 스냅샷 재생

기존 상품 5,000개와 연결 행을 별도 schema에 재생했다. PostgreSQL 16, CPU 2코어·RAM 2GiB에서 원본 행 복원은 **6.034초**, initial 점수·발행은 **7.619초**, 유효 1,366개를 발행했다. 전체 상품 ID·순위·payload 차이는 0이었다. 90일 coverage·공급사 identity 근거 없이 신뢰도 NULL을 보존했다. 실제 API 시간과 대규모 발행·동시 부하는 측정 범위 밖이다.

재현은 `scripts.benchmarks.ownerclan_replay --dump <저장소밖검증백업>`, 집계는 `artifacts/experiments/ownerclan-live-e2e/snapshot-replay.json`을 참고한다.

### 하루 10만 변경의 조건부 해석

관측된 고유 변경 104,251개는 계획 가정의 근거다. **기존 약 3시간 환산은 철회했다.** 정상 응답 일부와 소규모 저장 스냅샷이 분모였고, 라이브 전체 표본의 지속 처리, 재시도, 공급사 영향·만료, 전체 유효 상품 발행이 빠져 있다. 운영 완료 시간은 [1,500만 규모 검증 계획](#1500만-규모와-운영-승인-전-남은-조건)에서 재측정한다.

## 쿠팡 베스트 실데이터 검증

2026-11-26 격리 PostgreSQL 16.16에서 같은 상품의 가장 높은 원본 순위만 남기도록 수정한 뒤, 쿠팡 HTTP 3회로 완료 발행본을 만들었다. 응답/발행 행은 주방용품 1013에서 94/89, 생활용품 1014에서 96/73, 스포츠/레저 1017에서 81/68이었다. 세 카테고리의 발행본 조회를 확인했다. 운영 DB는 사용하지 않았다. 중복을 확인한 초기 호출 3회를 포함한 전체 근거는 `artifacts/validation/coupang-best-live.json`에 있다.

## 회귀 실행 방법

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q --junitxml=artifacts/validation/unit.xml
python -m pytest tests/integration -q --junitxml=artifacts/validation/integration.xml
git diff --check
```

기본 pytest는 외부 API·DB 없는 단위 테스트만 수집한다. 통합 테스트는 명시적 `COLLECTOR_TEST_POSTGRES_DSN`이 필요하며 host=`127.0.0.1`, dbname=`collector_stage1_test`만 허용한다. fixture는 UUID schema만 생성·삭제하고 `.env`를 읽지 않는다. API probe는 pytest에 포함하지 않으며 `--dry-run`을 오프라인 검증으로 간주하지 않는다. 캐시는 `artifacts/validation/pytest-cache/`다.

### 격리 PostgreSQL 검증

```powershell
docker run --name chozy-collector-test --publish 127.0.0.1:55439:5432 --env POSTGRES_DB=collector_stage1_test --env POSTGRES_PASSWORD=collector_test_only --detach postgres:16
docker exec chozy-collector-test pg_isready -U postgres -d collector_stage1_test
$env:COLLECTOR_TEST_POSTGRES_DSN='host=127.0.0.1 port=55439 dbname=collector_stage1_test user=postgres password=collector_test_only'
python -m pytest tests/integration -q
```

위 값은 폐기용 자격값이다. 기존 이름·포트 충돌 시 기존 자원을 재사용하지 않는다. 준비 완료 후 실행하고 종료 시 이번 전용 컨테이너와 연결된 volume만 식별해 제거한다. 운영 DB를 검증 대상으로 사용하지 않는다.

### 오너클랜 실패·재개 및 발행 안전성 격리 검증

실제 HTTP 전송을 금지한 fixture와 별도 PostgreSQL에서 다음을 검증했다.

| 상황 | 보존/차단 계약 |
| --- | --- |
| null·필드 누락·잘못된 root·빈 배열·미반환 | 완료 버전 유지, 실패/next_retry_at 기록, lease 해제. 삭제·무변경으로 간주하지 않음 |
| 일부 반환·일부 DB 실패·보완 timeout | commit된 정상 상품 보존, 나머지만 재개 |
| client retry 소진 | 새 묶음 중단, 요청 버전과 backoff 보존 |
| commit 응답 유실·청크 중단·새 요청·stale lease | 중복/역전 방지, 새 소유자·새 버전을 이전 응답으로 완료하지 않음 |
| 미완료 상세·미인증 revision | 새 점수/발행 거부, 이전 정상 발행과 다른 소싱처 유지 |
| 통합 Runner initial→새 job incremental | 상세 반영→완료 증표→점수→원자 발행→SQL 조회, 실패/재개·중복 실행 검증 |

발견·수정한 결함은 운영 migration 없는 직접 `scoring_cli` 경로가 pending 상세를 무시하던 문제다. `scoring_boundary`와 `publish()`의 미완료 검사로 보강했다. 이는 운영 입력 보호 migration을 대체하지 않는다. 근거: `tests/integration/test_ownerclan_failure_safety.py`, `test_operations_ownerclan_e2e.py`, 기존 scoring/detail 회귀와 `artifacts/validation/ownerclan-safety.json`.

## 오너클랜 API 지연 실측

### 묶음 크기 비교 실측

같은 상품 100개를 대상으로 worker 1개, 전체 상품 필드, 요청 시작 간격 3초, connect/read 5/45초, 자동 재시도·누락 보완 없이 비교했다. 2026-11-22 기준 **50·100개 묶음은 약 6초에 전부 반환했고, 10개와 단건에서도 빈 응답이 발생했다.** 단건은 별도 Session·JWT에서 24번째 요청의 GraphQL 호출 제한으로 중단됐다. HTTP 200이었으며 Retry-After와 reset 헤더는 없었다.

| 묶음 크기 | 조회 시도 | 경과 시간 | 반환/대상 |
| --- | ---: | ---: | ---: |
| 100개 | 1 | 5.709초 | 100/100 |
| 50개 | 2 | 5.784초 | 100/100 |
| 20개 | 5 | 25.764초 | 100/100 |
| 10개 | 10 | 31.487초 | 90/100 |
| 단건 재측정 | 24 | 79.521초 | 22/100 |

10개·단건의 시간은 전체 100개 확보 시간이 아니다. 묶음 축소의 개선 효과와 단건 완주, 시간대·캐시·순서 효과는 미확인이다. 전체 비교를 재현할 경우 같은 key 집합과 전송 조건으로 순서를 교대한다. 한 순회 최소 118회에 인증·재시도 예산을 더하고, 계정 사용량과 제공처 오류 중단 조건을 먼저 확인한다.

재현은 `scripts.probes.ownerclan_batch_comparison`의 무접속 plan을 사용한다. `--execute`는 실제 호출, `--single`은 별도 단건 측정이다. 고정 key·필드 해시와 최초 단건 집계 오류 및 재측정 기록은 `artifacts/experiments/ownerclan-batch-comparison/{measurement,single-measurement,analysis}.json`에 있다.

### 오너클랜 소량 지연 재확인

2026-11-22 단건 대조 3회에서 전송부터 헤더까지 **0.297~1.250초**를 기록했고 모두 정상 반환됐다. 같은 상품의 전날 대조에서는 item·새 Session bulk가 정상, 재사용 Session의 bulk가 HTTP 200과 빈 배열이었다. 순서·캐시·시간·연결 조건이 섞여 Session이 원인인지 미확인이다. 별도 지연 구간은 HTTP 19회에서 1,097개 확보·3개 미확보, 701.536초였다. timeout과 network backoff가 58.2%였지만 정상 응답도 평균 19.746초였다. 지연은 JSON 처리와 DB 저장 전 read 대기에서 관측됐다.

반복 장애 조사 시 실제 시각, 상품 key, query hash, 정상/빈 응답을 함께 대조한다. 근거는 `artifacts/experiments/`의 `ownerclan-latency-recheck/measurement.json`, `ownerclan-query-comparison/{session-measurement,session-analysis}.json`, `ownerclan-detail-latency/analysis.json`과 묶음·병렬 비교 기록이다.

### 오너클랜 호출 정책과 하루 1만 상품 조사

과거 상세 1만 개 표본은 최초 100회에 9,772개, 보완 포함 **9,997개**를 확보했다. 기록된 처리 시간 합계는 453.005초이며 연속 실행의 완료 시간이 아니다. `allItems(first:1)`의 과거 180 RPM 1시간 실측은 10,571시도·10,520성공·51오류였고, 200 RPM 시험에서는 885번째에 제한이 관측됐다. 다른 시점과 쿼리의 결과라 현재 묶음 worker의 계정 한도·리셋이나 처리 시간에 적용할 수 없다.

호출 수 산정은 [Collector 배치당 호출량](API_USAGE.md#collector-배치당-호출량), 운영 속도와 오류별 재개는 [RUNBOOK](RUNBOOK.md#오너클랜-호출-속도와-오류-후-재개), 원시 집계는 [근거 색인](#근거-색인과-복구-위치)을 참고한다.

## 도매매 Private 동기화 재호출

**2026-11-25 02:17 KST 기준, 로컬 IP에서 Private 변경 검색 승인이 필요하다.** `_search`를 최근 24시간·`size=1`·`channel.supply=true`·`no` 내림차순으로 한 번 호출했다. HTTP 200 안에 `result=false`, `401/UNAUTHORIZED`, `승인되지 않은 IP 입니다`가 반환됐다. 같은 환경과 Private 키로 `getItemViewES` v4.0(`market=supply`, 상품번호 `12473769`) 상세 조회는 성공했다. 따라서 상세 조회 성공으로 변경 검색 권한을 판정할 수 없다.

승인 후 성공 응답·시간대·페이지 종료·완전성·호출 한도를 확인한다. 근거는 `artifacts/validation/domeggook-private-sync.json`과 [동기화 원시 기록](#근거-색인과-복구-위치)이다.

## 운영 DB 복원과 통합 실행 준비

2026-11-21 논리 백업을 격리 `collector_operations_copy`에 복원해 점수·운영 migration 네 개의 적용과 재실행 no-op을 확인했다. 상품 5,240·옵션 13,307·관측 5,358·이벤트 294,806·상세 요청 205,268·검색 순위 18,194행의 해시와 sequence가 일치했다. 500-keyset 백필은 전체 5,240개를 대조했고 기존 행 업데이트, orphan, 잘못된 이벤트 연결은 0건이었다. 필수 컬럼·인덱스 검사 통과로 stage1 재적용은 불필요했다.

실제 Runner의 initial→새 job incremental, 실패·재개, 이전 홈 발행본 유지와 중복 실행을 가짜 공급처로 검증했다. 복원 시 pending 184,385개인 사본은 새 점수와 완료 증표가 차단됐다. 작은 검증 DB의 결과이며 운영 잠금, 대규모 인덱스 생성시간, 운영 Spring·외부 API는 별도 검증 대상이다.

### 최신 운영 migration 사전 점검

2026-11-22 읽기 전용 감사 시 DB 용량은 약 391MB였다. 당시 ledger에는 `stage2_storage_contract`와 `drop_payload_based_change_history_20260904`만 있었고, 앱 계정 `collector`는 superuser였다. 이후 로컬 Docker 적용과 Spring 실예약 확인은 [현재 상태](CONTEXT.md#구현과-운영-상태)에 기록한다.

복구점은 `C:/dev/Chozy/backups/migration-preflight-20261122/`의 custom dump·data/config/.env ZIP·비밀번호 제외 역할 정의·SHA manifest다. 격리 DB 복원 후 기존 사본과 스키마·인덱스·전체 행 해시가 일치했다. `--no-owner --no-acl`을 사용했으므로 역할/ACL·로그인 비밀번호·PITR을 별도 검증한다. 감사 시점의 상태는 적용 직전에 다시 확인한다.

근거는 `artifacts/validation/{migration-preflight,migration-backup-restore}.json`, 검토 SQL은 `migration-preflight-plan.sql`, 실행 순서는 [운영 전환](DEPLOYMENT.md#운영-전환-준비와-적용-순서)에 있다.

### DB 권한 분리 검증

2026-11-22 외부 `migration-preflight-20261122/collector.dump`를 새 PostgreSQL16·CPU2/RAM2GiB·localhost55450/collector_operations_copy에 `--no-owner --no-acl`로 복원했다. 운영에는 접속/변경하지 않고 사본에 기존 네 migration과 새 `runtime_budget_guard`, 최소 권한 계획을 적용했다. 구조/정책은 [DATABASE](DATABASE.md#실행-계정과-권한-분리), 배포/복구는 [운영 전환](DEPLOYMENT.md#권한-분리-사본-검증과-배포)가 기준이다.

관리자와 다른 실제 비소유자 LOGIN 두 개에 각각 Collector/Spring NOLOGIN 역할만 부여해 검증했다. 복원 사본과 별도 작은 fixture에서 각24건(총48건)의 금지 작업을 확인했다: DDL/테이블 삭제/TRUNCATE/트리거 해제/role 생성/sequence setval/ledger 변경/예산 직접 수정, Spring의 원본 상품·발행 행 직접 접근, Collector의 완료 추천 행 수정·인증 근거 변경은 권한 오류로 거부됐다. 실제 Runner로 수집→상세→initial 발행→미반환 시 이전 완료본 유지→재개→새 job incremental을 통과했고 Spring 함수 조회가 같은 완료 배치와 일치했다. source HTTP는 fixture이며 외부API0회다.

두 역할의 동시 예약8건에서 한도5건만 reserved·나머지3건 exhausted:day였고 실제 usage5를 보존했다. 차단 함수는 blocked=true만 설정하고 직접 차단 해제/사용량 삭제는 거부했다. 임시 동명 테이블로 예산/조회 함수를 가리는 시도도 통과하지 못했다. 복원 사본의 미완료 상세184,385개는 전후 동일하고 runtime 점수 실행은 `Detail refresh incomplete`로 차단됐다.

근거는 `artifacts/validation/database-access.json`, 새 역할용 SQL은 `database-access-plan.sql`, 재현은 `python -m scripts.rehearse_access`의 무접속 plan이다. 이 시험의 범위는 사본의 DB 권한과 fixture 프로세스다. 실제 연동·권한 전환 상태는 [CONTEXT](CONTEXT.md#구현과-운영-상태)에 있으며, tenant별 권한과 PITR은 별도 검증 대상이다.

## 보존된 성능 검증

별도 PostgreSQL 16·CPU 2/RAM 2GiB·동일 seed/분포/청크100의 합성 측정이며 외부 API 비용은 없다.

| 상품 10,000개 | 최적화 전 | 최적화 후 |
| --- | ---: | ---: |
| 최초 점수 계산 | 181.599초 | 114.562초 |
| 1% 혼합 변경 | 54.836초 | 34.026초 |
| 1% 순수 가격 변경 | 36.528초 | 5.336초 |
| 순수 가격 재계산 상품 수 | 1,850 | 100 |
| 90일 만료 | 90.376초 | 64.380초 |

순위/집합/업무 payload 동일(증분 미변경 상품 asOf/observedAt 보존 차이 제외), DB 증가 +5.0%, Python RSS 표본 +5.8%. 단회 결과이며 모든 자원 지표가 개선된 것은 아니다. 근거는 `collection-pipeline-benchmark/`, `scoring-optimization-benchmark/`이고 before에는 보존된 `baseline_scoring.py`가 필요하다. provenance의 과거 경로는 `artifacts/validation/purpose-names.json`과 대조한다.

### 발행 쓰기 비용의 반복 비교

**채택한 변경은 발행 INSERT에서 충돌 무시를 제거하고, 실행 행 잠금으로 동시 호출을 직렬화한 것이다.** 50만 유효 상품의 합성 데이터에서 INSERT+commit 중앙값이 **9.266초 → 6.764초(-27.0%)**였다. 적용 후 실제 `publish()`+commit은 3회에 **6.600/6.729/6.856초**였고 전체 상품 ID·순위·payload 차이는 0이었다. 재호출 no-op, 동시 호출, 미완료 배치 차단, 이전 발행본 보존, commit 응답 유실 재개는 집중 회귀 63개로 확인했다. [발행 계약](ARCHITECTURE.md#증분과-발행)

비교는 전용 PostgreSQL 16·CPU 2코어·RAM 2GiB·work_mem 8MB에서 동일 합성 payload, 발행 FK와 고유 인덱스 2개, 실행 직전 CHECKPOINT를 유지해 대안별 3회 교대했다. 50만 FK 발행 조건의 기록은 WAL 431.5~443.8MB, heap/TOAST 292.7MB·인덱스 44.1MB였다. 실제 분포·원본 trigger·Spring 동시 조회는 제외했다. 쓰기 비용 비교에서 인덱스·FK별 독립 기여율은 확정하지 못했고, planner/work_mem 변경과 순위 CTE 대안은 채택하지 않았다.

이전 50만 단회 기록은 work_mem 8MB에서 **23.867초**, 64MB에서 **16.741초**다. 측정 조건이 달라 위 개선율의 기준으로 사용하지 않는다. EXPLAIN 밀리초와 벽시계 초는 별도로 읽는다.

재현 도구는 `scripts.benchmarks.publication_costs`와 `publication_conflicts`이며 기본값은 무접속 plan이다. 대안 비교는 비용 fixture를 만든 뒤 `--execute`, 적용 경로 검증은 `--verify-publish`를 추가한다. 근거는 `artifacts/experiments/publication-costs/{results,conflicts}.json`, 과거 탐색은 `publication-profile/profile.json`과 `operations-readiness/publication-{capacity,comparison,comparison-sql}.json`에 있다.

### 1500만 규모와 운영 승인 전 남은 조건

목표 서버 CPU/RAM/디스크·일일 허용시간·Spring 조회 p95·발행 보관 기간은 미확정이다. 현재 fixture의 4GiB/25분 guard를 풀어 1,500만을 실행하지 않는다.

1. 실제 payload 폭p50/p95/p99·제공처/공급사 편중·옵션/90일 이력 fan-out·인증 표본/유효율을 집계한다. NULL·동점·미래 as_of·부적격도 포함한다.
2. 같은 서버/스토리지 조건에서 100만→300만→500만→1,500만으로 확대하며 매 단계 실측으로 다음 시간/공간 중단 gate를 정한다. cold/warm 구분·조건별 최소3회·대안 ABBA. 테스트/백업/다른 benchmark를 동시에 돌리지 않는다.
3. initial, 무변경 전체 발행, 가격만 변경, 공급사 편중/인증/소속/품절 변화, 90일 만료, 대량 합집합과 중단/재개를 측정한다. DB 처리와 별도 API 시간을 합쳐 일일 창을 확인한다.
4. INSERT/완료UPDATE/commit, 실제 INSERT BUFFERS/WAL·checkpoint·temp I/O·락/청크 지연·peakRAM·복제 지연·autovacuum·동시 Spring p95를 기록한다. 전체 집합/동점 순위/payload 해시·미완료 차단·이전 완료본·commit 유실 재개를 확인한다.
5. 용량 예산은 `상시 상품/옵션/90일 이벤트·이력/점수 + 보관 배치 수×실측 배치 + 동시 신규 배치 + peak WAL/복제 지연 + temp + 백업/복원 공간 + 합의된 안전 여유`. 각 heap/TOAST/인덱스를 직접 측정하며 보관 정책 없이는 용량 승인도 보류한다. 선형 환산만으로 운영 가능 판정을 내리지 않는다.

성능 재현 도구는 `scripts.benchmarks.pipeline/scoring/verify_scoring/operations/publication/publication_profile/publication_costs/publication_conflicts`다. 기본 plan과 guard를 먼저 확인한다. pipeline 계열은 localhost55449·label `chozy.collector-benchmark=true`·C: 여유30GiB·RAM4GiB·DockerFS10GiB·volume4GiB·1500초를 검사한다. 기존 50,000 확대는 예상1200초 gate에서 거부됐다. 전용 서버 예시는 아래이며 재실행은 새 서버/DB, 과거 집계를 외부 보존한 후 수행한다.

```powershell
docker run --name chozy-publication-profile --label chozy.collector-benchmark=true --cpus 2 --memory 2g --publish 127.0.0.1:55449:5432 --env POSTGRES_DB=collector_stage1_test --env POSTGRES_PASSWORD=collector_benchmark_only --detach postgres:16 -c shared_buffers=256MB -c work_mem=8MB -c max_wal_size=512MB -c shared_preload_libraries=pg_stat_statements -c track_io_timing=on
python -m scripts.benchmarks.publication_profile
```

실행은 승인된 범위에서 `--execute`를 추가한다. publication_profile은 새 schema만 사용하며, operations/publication은 각각의 전용 container명과 1,000개 fixture를 전제로 한다. 원래 재현 조건은 집계/provenance와 대조한다.

## 근거 색인과 복구 위치

구형 쿠팡 키워드 검색의 8월 산출물 5개는 작업공간의 `backups/retired-coupang-keyword-search-20261126/`에 보관했다.

아래 경로는 별도 표기가 없으면 `artifacts/experiments/` 상대경로다. 대량 원본은 저장소 밖, 일반 예시는 공급처별 최대 5개, 임시 파일은 실험별 scratch에만 둔다. JWT/비밀번호를 결과 로그에 출력하지 않는다.

| 조사 | 보존 근거 |
| --- | --- |
| 일일 변경량 | `daily-change-2026-11-20/owner-history-followup/analysis.json`: 2026-11-19고유 변경 상품 104,251개·중복 제거 사건 153,614개. 전체 분모 미확인으로 변경률 산출 불가 |
| 과거 상세 1만 | `owner-items-batch-20261120-130311/analysis.json`, 원본은 외부 retained-originals |
| API/query/누락/Session | `ownerclan-query-comparison/`의 plan, small/batch/missing/session 측정·analysis·code-history-audit |
| 공식 호출 정책 | `ownerclan-api-policy/sources.json`, rate/detail-measurement, analysis. PDF 원본 [Seller API 매뉴얼](references/OwnerclanAPI_Seller_Manual.pdf) |
| 지연/병렬 비교 | `ownerclan-detail-latency/analysis.json`, `ownerclan-batch-size-comparison/measurement.json`, 과거 E2E `ownerclan-e2e/` |
| migration/정합성 | `artifacts/validation/migration-rehearsal.json`, replay, operations-copy-integrity, operations-backup, migration-preflight, migration-backup-restore |
| 동기화/과거 제한 | `domeggook-private-rate/analysis.json`, `domeggook-private-sync/20261119-223214.json`. 오래된 일일 환산의 단위 불일치는 근거에서 제외 |

외부 복구점은 `C:/dev/Chozy/backups/` 아래의 `collector-cleanup-20261120-133841/retained-originals/`(대량 원본), `collector-layout-cleanup-20261120-222336/`(artifacts/sources-before.zip·status/patch), `collector-purpose-names-20261120-224447/`(이름 변경 전), `docs-consolidation-20261122/documents-before.zip`(이번 문서 정리 전)이다. CRC/hash와 layout-cleanup.json·purpose-names.json을 대조하고 별도 디렉터리에 복원해 필요한 파일만 비교한다. 현재 source/data/log/lock을 일괄 덮어쓰지 않는다.
