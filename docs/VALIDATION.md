# 테스트와 실측 결과

현재 진척은 [CONTEXT](CONTEXT.md), 실행·중단·재개는 [RUNBOOK](RUNBOOK.md), 데이터 의미는 [ARCHITECTURE](ARCHITECTURE.md)와 [API_CONTRACTS](API_CONTRACTS.md)가 기준이다. 이 문서는 최신 결과·측정 조건·한계·재현 근거만 기록한다. 아래 수치는 실측과 조건부 환산을 구분한다.

## 오너클랜 5000개 표본 E2E

**정정:** 아래 과거 실행에는 시험용 `SampleClient`가 보완 후 미반환에서 전송을 차단하는 조건이 개입했다. 실제 `refresh_details`는 그 상품만 pending/retry로 남기고 다음 묶음을 계속 처리한다. 따라서 “실제 수집기가299개에서 중단돼 E2E에 실패했다”는 기존 보고는 철회한다. API의 미반환 관측 자체와 저장/차단 수치는 당시 진단 결과로만 유효하다. 약5,000개 전체 수집 지속·완료시간은 검증되지 않았다.

해당 조건을 제거하고 계측은 관찰만 하도록 수정했다. 실제 worker와 수정 controller를 연결한 격리 PostgreSQL 회귀에서 요청 크기 `[100,1,100]`, 정상199개 저장·미완료1개 보존을 확인했다. 코드: `tests/integration/test_ownerclan_probe_continuation.py`. 원본 기획서 PDF p.8의 매일 추천 갱신, p.28의 일일 배치와 대조했으며, 한 상품의 미반환 때문에 나머지 수집을 멈추라는 요구는 확인되지 않았다. 기존 Collector 설계 §23은 미반환 보완/미완료 보존, §오류정책은 소싱처 누락 시 이전 발행본 유지를 규정한다. 원본 서비스 기획이 개별 실패의 부분 발행까지 확정한 것으로 단정하지 않는다. 이번 정정은 API0회·운영DB 변경0회이며 업무 worker/점수 차단 정책은 변경하지 않았다.

아래는 2026-11-22 KST의 **과거 진단 실행 수치**다. 시험 controller의 추가 중단 조건이 개입했으므로 업무 수집기의 약5,000개 완주 실패나 지속 처리량을 증명하지 않는다. 운영 DB/기존 checkpoint/lease/설정은 변경하지 않았다.

전용 PostgreSQL16, CPU2/RAM2GiB/shared_buffers256MB/work_mem8MB, localhost55449/collector_stage1_test에서 업무 코드를 호출했지만 그 위의 진단 controller가 동작을 바꿨다. 직전 KST 하루(11월21일) 변경이력의 앞12페이지에서 고유 key4,712개를 선정했다. 전체 하루 순회가 아니며 정렬 순서·중복률의 대표성이 없다. 과거 마지막 진단은 누락1회 보완을 허용한 뒤에도 미반환이면 다음 전송을 차단했다. **이 추가 조건은 당시 잘못된 시험 설정이며 현재 제거됐다.**

| 실제 API→격리 DB 구간 | 실측/판정 |
| --- | --- |
| 변경이력 표본 조회 | 12,000신호·12페이지·고유4,712key, 39.633초. hasNextPage=true이므로 하루 피드 미완료 |
| 원본 이벤트/상세 요청 저장 | 이벤트11,996건·표본4,712개 요청, 55.101초. 수량 신호4건은 사건으로 만들지 않음 |
| 상세100개 1차 | 100/100, HTTP구간10.218초 |
| 상세100개 2차 | 100/100, HTTP구간3.469초 |
| 상세100개 3차 | **99/100**, HTTP200·GraphQL오류0, 15.735초 |
| 누락 1개 보완 | **0/1, 빈 배열**, HTTP200·GraphQL오류0, 0.141초. WFSV627 미완료 유지 |
| 상세+부분저장/실패 기록 | 38.004초, 정상299개 commit. 표본미완료4,413개=누락1+미요청4,412 |
| 점수/발행 | pending 때문에 **Detail refresh incomplete**로 차단. 점수0·발행0, 강제완료/삭제 없음 |
| 주 실행 구간 | 132.757초(이력→차단까지, schema/auth 준비 제외). 5,000개 완료시간이 아님 |

최종 실행은 인증1+이력12+상세4=HTTP17회다. 앞선 표본 선택 실행13회와 첫 누락에서 즉시 중단한 진단16회(279개 저장·누락21)를 포함해 **이번 작업 전체는 인증3+이력36+상세7=HTTP46회**다. 최초 실행은 정확히5,000개 조건 때문에 상세 전 중단했고, 이후 요청에 맞춰 약5,000개 허용 범위로 수정했다. 이 추가 준비 시간/호출을 숨기거나 처리량 분모에 합치지 않는다.

full ITEM_FIELDS·POST JSON·worker1·시작간격3초·5/45초·HTTP자동retry/redirect0, 최종 실행의 누락만1초 뒤1회 보완을 적용했다. timeout/403/429/GraphQL제한은 모두0이다. worker failureCount101 중100개는 진단 중단 후 다음 claim이 전송 전에 거부돼 기록된 것이며, 실제 API누락은1개다. 모든 요청은 미완료로 보존했고 claim/실패 기록을 실제 반환 누락과 구분했다. local app대기/다른Python수집없음/운영공동예산테이블없음을 확인했으나 외부계정사용·제공처잔여예산은 미확인이다.

### 5000개 저장 스냅샷 재생

실제 API 실패 후 **추가 API0회**로 외부 검증 백업의 기존 상품5,000개와 연결된 이벤트11,995·관측5,118·옵션12,787행을 별도 schema에 재생했다. 원래 값/관측시각/ID를 보존했고 live 표본의 미완료 요청을 삭제하거나 완료 처리하지 않았다. 이 별도 snapshot scope에만 완료 증표를 만들었다.

| 오프라인 재생 | 실측 |
| --- | ---: |
| 기존 행 COPY/trigger/commit | 6.034초 |
| 실제 initial 점수5,000개+발행 | 7.619초 |
| 유효1,366개 정렬/발행 SQL | 0.041737초(마지막commit 제외, 위시간에 포함) |
| read_supply_home 10개 | 0.002918초 |
| 전체 상품ID/순위/payload 차이 | 0 |

coverage/공급사identity 근거0, 신뢰도5,000개 모두NULL을 보존했다. 실제90일 인증·공급사peer 계산·운영동시부하·Spring 프로세스·1,500만 전체정렬은 미검증이다. COPY는 기존DB행 재생이며 API 정규화·상품별 상세저장 throughput과 동일하지 않다. **이 재생 시간을 실패한 live 실행에 합쳐 전체 E2E 성공으로 표시하지 않는다.**

### 하루 10만 변경의 조건부 해석

아래는 당시 정상200개 prefix의 산술 기록일 뿐이며, 잘못된 시험 중단으로 약5,000개를 시도하지 못했으므로 **약3시간이라는 운영 용량 판단은 철회**한다. 실제 worker의 지속 처리와 retry/만료/전체 발행을 포함한 재측정이 필요하다.

문서의 약10만은 과거 고유 변경104,251개 관측에 기반한 계획 가정이다. 점수대상은 `변경 ∪ 공급사영향 ∪ 만료`, 정렬/발행은 전체 유효카탈로그다. 다음은 작은 표본을 이용한 민감도 계산이며 실측/SLA가 아니다.

| 조건부 구간 | 산식/환산 |
| --- | --- |
| 이력조회+이벤트/요청저장 | 최종 `(39.633+55.101)/12,000×신호수`. 하루15만~25만신호 시 **약20~33분**(앞선 실행은21~35분). 표본중복률을 하루 전체에 적용한 값은 아님 |
| 상세+저장, 모든후속요청 완전반환 가정 | 정상앞200개 구간18.309161초(다음claim 포함)÷200×10만 = **약2시간33분**. 앞선 진단 환산은2시간38분. API만의 시간은 이미 포함돼 중복가산하지 않음 |
| 점수 소규모 근거없는snapshot 가정 | 7.619/5,000×10만 = **약2.54분**. 실제90일/공급사peer·영향확대/만료·대규모인덱스 비용을 보장하지 않음 |
| 정상반환 가정 소계 | 두 실행의 조건부 환산 범위 **약2.9~3.3시간 + 전체카탈로그 발행의 미검증 시간**. 위점수에 소규모발행이 포함되므로 실제대규모발행으로 교체해야 하며 그대로 이중가산하지 않음 |

최종 실행에서도 빠진1개가 보완 후 미확보였다. **실제10만개 완료시간/상한은 산출불가**다. 이전17.8시간 지연환산과 이번 정상prefix환산은 서로 다른 시점/분모이며, 이번 시험으로 하루3시간 완료를 약속하거나 이전지연이 해소됐다고 판단하지 않는다. retry/DBbackoff·계정일/월예산·실제coverage/peer·전체발행을 검증해야 한다.

수정 도구는 `scripts.probes.ownerclan_live_e2e`이며 누락 보완/다음 묶음 진행을 실제 worker에 맡긴다. `--bounded-recovery`는 호환 옵션이고 같은 동작이다. 수정 후 결과 경로는 `artifacts/experiments/ownerclan-workflow-e2e/`로 과거 진단 결과와 구분하며 아직 실제 재실행은 없다. 오프라인재생은 `scripts.benchmarks.ownerclan_replay --dump <저장소밖검증백업>`, 기본plan은 모두 무호출이다. 과거 근거는 `artifacts/experiments/ownerclan-live-e2e/bounded-recovery/{measurement,analysis}.json`과 상위 폴더의 `{measurement,selection-attempt,snapshot-replay,analysis}.json`이다. 기존 측정/해시는 덮어쓰지 않고 해석만 정정한다.

## 현재 검증 결과

최신 단위 회귀는 Python3.12.12 **602 passed**(15.97초), 별도 PostgreSQL16·127.0.0.1:55432/collector_stage1_test의 전체 통합 회귀는 **250 passed**(321.42초)다. PR 준비에서 전체 회귀를 재실행했으며 외부API0회·운영DB 변경0회다. 최신 JUnit은 `artifacts/validation/unit.xml`, `integration.xml`에 둔다. 앞선 권한/발행 집중 회귀63개와 worker/controller 지속 처리 회귀1개의 근거는 `publication-access.xml`, `ownerclan-continuation.xml`에 보존한다. 승인된 실제 API 조사는 아래 묶음 비교 실측에 별도로 기록한다. 실제 Spring 프로세스·운영 전체 E2E·1,500만 성능·일일 SLA는 통과한 것으로 보지 않는다.

새 도구의 무접속 plan, operations plan, Python 문법·문서 로컬 링크/앵커·코드 fence·공백 검사와 `git diff --check`도 통과했다. 이번 전용 컨테이너와 연결된 anonymous volume은 식별 후 제거했다. 측정 집계와 외부 원본 백업은 보존하고 문서 정리용 scratch는 제거했다. 기존 운영 컨테이너·volume·수집 상태는 유지했다.

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q --junitxml=artifacts/validation/unit.xml
python -m pytest tests/integration -q --junitxml=artifacts/validation/integration.xml
git diff --check
```

기본 pytest는 외부 API·DB 없는 단위 테스트만 수집한다. 통합 테스트는 명시적 `COLLECTOR_TEST_POSTGRES_DSN`이 필요하며 host=`127.0.0.1`, dbname=`collector_stage1_test`만 허용한다. fixture는 UUID schema만 생성·삭제하고 `.env`를 읽지 않는다. API probe는 pytest에 포함하지 않으며 `--dry-run`을 오프라인 검증으로 간주하지 않는다. 캐시는 `artifacts/validation/pytest-cache/`다.

### 9월 작업 브랜치 회귀 결과

2026-11-24에 Python 3.12.12로 9월 커밋을 각각 별도 폴더에 풀어 대상 테스트만 다시 실행했다. 당시 동작은 모의 응답과 임시 상태 파일로 확인했다. 현재 `main`의 오프라인 단위 테스트도 **602개 통과**했다. 이번 재검증에는 외부 API와 운영 DB를 사용하지 않았다.

| 범위 | 결과 |
| --- | --- |
| 도매꾹 일일 수집 예산 (`3a9e244`, 9월 4일) | **2개 통과**. 남은 요청 예산이 0이면 목록·상세 API 요청 없이 목록의 다음 페이지와 상세의 다음 인덱스를 각각 저장했다. |
| 쿠팡 키워드 재개 (`fa1d229`, 9월 5일) | **2개 통과**. 모의 dry-run에서 완료 키워드를 건너뛰고 기존 checkpoint를 보존했다. 완료된 키워드만 남은 일반 실행에서는 추가 요청 없이 checkpoint를 지웠다. |
| PostgreSQL 저장 흐름 (9월 8일) | 당시 [PR #55](https://github.com/Chozy-dev/Chozy-DataCollector/pull/55)에 **167개 통과**로 기록됐다. |

### 격리 PostgreSQL 검증

```powershell
docker run --name chozy-collector-test --publish 127.0.0.1:55432:5432 --env POSTGRES_DB=collector_stage1_test --env POSTGRES_PASSWORD=collector_test_only --detach postgres:16
docker exec chozy-collector-test pg_isready -U postgres -d collector_stage1_test
$env:COLLECTOR_TEST_POSTGRES_DSN='host=127.0.0.1 port=55432 dbname=collector_stage1_test user=postgres password=collector_test_only'
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

### 묶음 크기 영향 비교 계획

수정한 controller의 후속 약5,000개 시험은 사용자 요청으로 중단했다. `artifacts/experiments/ownerclan-workflow-e2e/measurement.json`의 상태는 `interrupted_by_user`다. 기록된 상세 요청20회·정상 반환 합계1,194개는 중단 시점의 부분 관측이며 저장 완료/고유 확보량이나 지속 처리량으로 간주하지 않는다. 진행 중이던 마지막 요청의 완료 여부도 미확정이다. 자동 재개나 추가 API 호출은 하지 않았다.

사용자 요청으로 아래 비교의 첫 실행을 수행했다. [실측](#묶음-크기-비교-실측) 이후 현재는 GraphQL 제한으로 중단했으며, 단건 완주/순서 교대 반복은 남아 있다. 단건에서도 timeout/빈 배열 이력이 있으므로 크기만을 원인으로 전제하지 않는다. 아래는 실험 설계의 기준이며 자동 재개 승인이 아니다.

- 동일한 고정100개 key 집합을 1/10/20/50/100개씩 나눠 각 방식에서 같은100개를 대상으로 비교한다. full 필드·API/전송 방식·JWT·Session 정책·worker1·요청 시작 간격·timeout을 고정하고 **묶음 크기만** 바꾼다. 필드 수/호출 간격 비교는 별도 실험으로 분리한다.
- 한 순회의 최소 조회 수는100+10+5+2+1=118회(인증·보완·재시도 별도)다. 실제 호출 전 계정 공동 사용·허용 예산·시간/자원 한도·오류 중단 조건을 정하고, 반복/순서 교대의 추가 예산도 따로 산정한다. 기존 중단 실행의 남은 예산을 자동 전용하지 않는다.
- 요청/반환 key·누락/중복·응답 bytes·헤더/본문 시간·전체100개 시도/확보 시간·HTTP/GraphQL 오류·보완 결과를 기록한다. 누락은 보완/미완료로 보존하며, 계측 controller가 다른 묶음 진행을 임의로 막지 않는다. 제공처 제한/네트워크 오류/전체 예산 초과는 누락과 구분해 기록한다.
- 캐시/시간대/순서 영향을 줄여 반복하고, **정상 응답 속도와 상품 완전성을 함께** 비교한다. 작은 묶음에서 반복적으로 빠르고 완전해지는지 확인한 뒤에만 운영 상한 변경·5,000개 지속 처리 재측정을 검토한다. 이번 문서 기록으로 실행이나 운영 설정 변경을 시작하지 않는다.

### 묶음 크기 비교 실측

2026-11-22 17:48:59~17:56:13 KST, 기존 고정100개 key와 full ITEM_FIELDS를 사용했다. worker1·POST JSON·요청 시작간격3초·connect/read5/45초·자동 retry/redirect/필드 fallback/누락 보완0이다. 로컬 app은 signal.pause 대기, 호스트 Python 수집기는 관측되지 않았으며 외부 계정 사용량·제공처 잔여 한도는 미확인이다. 운영 DB/checkpoint/수집 설정은 변경하지 않았다.

| 요청 묶음 | 조회 시도 | 방식별 시도 시간(대기 포함) | 정상 확보/100 | 결과 |
| --- | ---: | ---: | ---: | --- |
| 100개 | 1회 | 5.709초 | 100 | 완료 |
| 50개 | 2회 | 5.784초 | 100 | 완료 |
| 20개 | 5회 | 25.764초 | 100 | 완료 |
| 10개 | 10회 | 31.487초 | 90 | HTTP200 빈 배열1회로10개 누락 |
| 1개, 별도 재측정 | 24/100회 | 79.521초까지 | 22 | 정상22회·빈 배열1회·GraphQL 제한1회, 나머지76회 미요청 |

표의10개/1개 시간은 **100개 확보 완료시간이 아니다.** 100/50/20개는 같은 JWT/Session의 내림차순 단회 관측이다. 최초 단건은28개 응답(정상27·빈 배열1)을 대조한 뒤 완료되지 않은 다음 호출 기록의 `http_seconds`를 집계하다 로컬 KeyError로 중단됐다. 이 부분을 단건 전체 비교에서 제외했다. 원래 예외는 집계 오류에 가려져 최초 유발 원인을 확정하지 못했다. 시간 미확정 기록을 허용하고 원래 예외를 보존하도록 수정했으며, 일시적인 atomic replace 실패에도 제한 재시도를 추가해 오프라인 회귀를 통과했다.

단건만 별도 인증/JWT/Session으로 새100개 연속 측정을 시작했으나24번째 조회에서 **HTTP200·GraphQL 제한 오류1개**가 관측돼 즉시 중단했다. Retry-After/rate-limit/reset 헤더는 없었고 제한 뒤 추가 확인/재시도는0회다. 두 실행의 UTC 관측 최소 시작 간격은 각각3.000174/3.000227초였다. 이 로컬 간격은 계정 전체 요청량이나 제공처 승인 한도의 증명이 아니다.

전체 기록은 인증2+조회71=73개 시작 기록이며, 그중 초기 실행의 마지막 조회1개는 전송 완료 여부가 확인되지 않은 기록이다. 응답/전송 종료가 기록된 것은 **인증2+조회70=72회**다. 최초 실행에서 정한 상한은 조회118+인증1, 도구 오류 후 단건 보완 측정 상한은 별도 조회100+인증1이었으며 실제 사용은 위 기록과 구분한다. 최초 단건 부분 관측을 재측정에 합치거나 이전 응답으로 누락을 채우지 않았다.

이번 관측에서는50/100개가 약6초에 같은100개를 확보했지만 0.075초 차이로 둘의 우열을 판단하지 않는다. 10개·단건에서도 빈 응답이 있어 **묶음을 줄이면 누락이 해결된다는 가설은 지지되지 않았다.** 단건 재측정은 JWT/Session/시간대가 달라 통제된 단일 실행 비교가 아니며, 캐시·순서 교대 반복·단건 완주·대량 지속 처리·계정 한도 원인·reset은 미검증이다. 운영 묶음/간격을 변경하거나 일일 E2E 시간을 외삽하지 않는다.

도구는 `scripts.probes.ownerclan_batch_comparison`(기본 무접속 plan, `--execute`만 실제 호출, `--single`은 별도 단건 측정)이다. 기존 결과가 있으면 우발적 재실행을 거부한다. 근거는 `artifacts/experiments/ownerclan-batch-comparison/{measurement,single-measurement,analysis}.json`이며 원시 시작 기록은 수정하지 않고 분석에서 부분/미확정 기록을 구분했다. 고정 key/필드 hash는 각각 `369e724b342dbead0eed2d5c3ce56239da83268916f12b0866be4eabb3921208`/`911507659041f07d733a455a69bb109ffa739157e6ac8a6261687ba214491564`다. 향후 실제 호출은 제공처/공동 계정 한도와 reset 확인 및 새 실행 범위 검토 후 진행한다.

### 오너클랜 소량 지연 재확인

2026-11-22 03:41 KST, 인증 1+조회 3=HTTP 4회, 총 10.328초(인증·시작 간격 포함). full ITEM_FIELDS·POST JSON·worker 1·3초 시작 간격·5/45초·retry/redirect 0회였다.

| 상품/API | Session | 전송→헤더 | 반환 |
| --- | --- | ---: | --- |
| WFM25VZ / bulk1 | 새 A | 0.578초 | 1/1, soldout·가격 정상 |
| WFSQRWB / bulk1 | 새 B | 0.297초 | 1/1, available·가격 정상 |
| WFSQRWB / item | B 재사용 | 1.250초 | 1/1, available·가격 정상 |

오류/누락/timeout 0회였다. 본문 읽기 계측 0은 시계 분해능 미만이며 실제 0초가 아니다. 헤더 시간은 연결/TLS/망/서버를 포함한다. `http_seconds`에는 adapter 페이싱이 포함돼 순수 전송 시간과 다르다. 이 표본은 지연 미재현이며 지속 처리·100개 묶음·일일 SLA 회복 증명은 아니다. `artifacts/experiments/ownerclan-latency-recheck/measurement.json`에 보존했다.

### 오너클랜 API·Session 교차 실측

2026-11-21 19:14:59~19:15:48 KST, 인증 1+조회 3=HTTP 4회, 49.203초 뒤 불완전 반환으로 중단했다. 같은 WFSQRWB에서 새 A/item 32.422초 정상, 새 B/bulk1 5.156초 정상, A 재사용/bulk1 8.609초에 정확한 `{"data":{"itemsByKeys":[]}}`+LF(28 bytes), HTTP 200·GraphQL 오류 없음이었다. 같은 JWT·full fields·bulk query SHA-256 `79c5c9a5c97902eb1d9f21060c9402f70b2366ac44d4926ccc99f6e9c137ede3`였다. 정상 대조·B 재사용 item은 중단 규칙상 미측정이다.

순서·캐시·시간·연결이 섞여 Session 재사용 인과는 미확정이다. 실제 TCP 재사용과 서버 trace도 없다. 당시 첫 UTC 시작 간격은 2.995055초로 엄밀한 3초를 증명하지 못했고, 이후 **진단 도구의 pacing clock만** 고해상도 perf_counter로 수정·가상시계 회귀 검증했다. 원래 실측은 변경하지 않았다. `ownerclan-query-comparison/session-measurement.json`, `session-analysis.json`에 원문 envelope·hash·시계 한계를 둔다.

### 과거 실패와 원인 판단

| 관측 | 결과/해석 |
| --- | --- |
| 같은 100개를 20개씩 비교 | 세 번째 호출 HTTP 200에 0/20. 인증 1+조회 3, 59.594초에 40/100 확보·누락 20·미요청 40. 5/10/50/100 방식 미실행 |
| 문제 key 원인 조사 | 정상 대조 bulk1과 WFSQRWB item 성공 뒤 동일 key bulk1이 45초 timeout. 과거 별도 28-byte 응답 원문은 복구 불가여서 이번 정확한 빈 배열과 동일하다고 확정하지 않음 |
| 100/50/20 교차 비교 | 100개 timeout, 50개 방식 일부 확보, 20개 방식에서 다시 timeout. 묶음 축소의 일관된 개선 미입증 |
| worker 1 대 2 소량 | 100개 3묶음·조회 3+인증 2, 175.248초. timeout 2묶음·나머지 99/100. 병렬 배수/처리량 산출 불가 |
| 공식 한도 조사 실호출 | GraphQL 5+인증 2. 낮은 실효 RPM에서도 allItems·50개 상세 timeout, 101개는 오류 없이 1행. 현재 hard cap/분·시간·일 한도 미확정 |
| 과거 1만 상세 | 최초 100회에 9,772개, 보완 포함 9,997/10,000. 처리 합 453.005초이며 연속 wall-clock/정확히 1만 완료시간이 아님 |
| 지연 구간 HTTP 19회 | 701.536초에 1,097개 확보, 3개 미확보. 성공 HTTP 14회 276.441초, timeout 5회 225.327초, network backoff 183초, 보완 대기 3초, 미분리 13.768초. 마지막 commit 미포함 |

timeout+network backoff는 위 지연 구간의 58.2%지만 성공 HTTP도 평균 19.746초였다. 90초 backoff는 기존 timeout 보호 정책이며 403/429의 증거가 아니다. 지연은 JSON/정규화/DB 이전 read 대기에서 관측됐다. 서버 resolver/cache/routing/숨은 제한·연결/망 중 원인은 분리하지 못했다. 제공처에 UTC·key·동일 query hash·정상/빈 envelope 자료로 trace 확인이 필요하며 메시지를 대신 발송하지 않았다.

과거 성공과 현재 builder의 full fields/hash·POST JSON 의도는 대조했지만 당시 실행 바이너리·소켓 상태까지 증명하지는 못했다. full fields는 HTML content·이미지 바이너리를 포함하지 않는다. worker는 시작 간격을 긴 응답 뒤 추가로 매번 더하지 않으며, 누락 보완 1회와 timeout retry는 별개다. 설정·Session 정책·timeout·묶음은 이 소량 관측만으로 바꾸지 않는다.

### 오너클랜 호출 정책과 하루 1만 상품 조사

공식 문구의 범위는 [API_CONTRACTS](API_CONTRACTS.md#공식-호출-제한-안내의-범위), 오류별 대기·재개는 [RUNBOOK](RUNBOOK.md#오너클랜-호출-속도와-오류-후-재개)에 둔다. 계정 전체 사용량·제공처 잔여 예산·reset은 여전히 미확인이다. 로컬 RPM guard는 제공처 승인 한도가 아니다.

과거 allItems(first:1)의 60/120/180 RPM 단기 성공, 180 RPM 1시간 10,571시도/10,520성공/51오류와 200 RPM 시험의 885번째 제한 기록은 당시 쿼리/계정 조건이다. 현재 묶음 worker의 처리량·시간/일 쿼터에 적용하지 않는다. 회복 확인용 30 RPM 이하→정상 시 권고 60 RPM→별도 지속 검증 후보 120 RPM을 이미 배포한 설정으로 읽지 않는다. 기존 interval=.4초는 설정상 150 RPM이지 승인/안정 한도가 아니다.

호출 수는 `ceil(이벤트 수/1000)+ceil(고유 상세 대상/100)+보완/실패 재시도+인증/발견`이다. 고정 1만 key의 최소 상세는 100회, 10만 key는 1,000회다. 실제 지연·페이싱·예산·저장은 별도다. 일일 전체 모델은 `변경 저장 + (변경 ∪ 공급사 영향 ∪ 만료) 계산 + 전체 유효 집합 발행 + 재시도/동시 부하`이며 10만 변경=10만 계산=10만 정렬로 동일시하지 않는다.

기존 10만 상세 환산 75.5분은 과거 소량 회복 시나리오, 17.8시간은 위 지연 구간의 조건부 환산이다. 26.7시간(단순 1.5배)이나 36~39시간 전체 표는 검증된 상한이 아니므로 현재 예측에서 제외했다. 두 timeout 후 성공을 매 묶음 가정하면 API/retry만 약 56.45시간이나 이것도 정책 민감도 예시일 뿐 상한이 아니다. 모든 실패가 재시도 뒤 성공한다는 보장과 당일 예산이 없으면 유한 완료시간을 보장할 수 없다.

## 도매매 Private 동기화 재호출

2026-11-21 17:02 KST, `_search` 최근 24시간·size=1·channel.supply=true·no 내림차순으로 1회, 0.190초에 HTTP 200이나 `result=false`, `401/UNAUTHORIZED`, `승인되지 않은 IP 입니다`였다. retry·페이지 순회·DB 저장은 없었다. 승인·성공 응답·시간대/종료/완전성·한도는 미검증이다. 근거는 `artifacts/validation/domeggook-private-sync.json`. 과거 getItemViewES의 HTTP 429(약180~190 RPM)를 Private 한도에 대입하지 않는다.

## 운영 DB 복원과 통합 실행 준비

2026-11-21 운영 논리 백업을 격리 `collector_operations_copy`에 복원하고 기존 네 migration을 순차 적용·재실행 no-op 확인했다. 운영 DB는 변경하지 않았다.

| 사본 migration | 시간 | WAL bytes |
| --- | ---: | ---: |
| stage7_supply_scoring | 0.153초 | 448,792 |
| stage8_operations | 0.106초 | 284,480 |
| stage7_history_policy_v2 | 0.022초 | 4,912 |
| stage10_scoring_performance | 0.026초 | 10,048 |

상품 5,240·옵션 13,307·관측 이력 5,358·이벤트 294,806·상세 요청 205,268·검색 순위 18,194개 전체 행 해시와 sequence를 보존했다. 500-keyset 전체 백필 5,240개 완료·업데이트 0, 대조 차이/orphan history/잘못된 event 연결 0이었다. 필수 stage2 컬럼/인덱스 validator 통과로 ledger 없는 stage1 재적용은 불필요했다. CPU 2/RAM 2GiB·작은 DB 단회 결과이며 운영 잠금/1,500만 index 생성시간 보장이 아니다.

fixture E2E는 실제 Runner/adapter/history/detail/store/score/publish/read_supply_home을 사용했다. 공급처만 in-memory였으며 initial→새 job incremental, 실패/재개·이전 홈 유지·완료 job 중복 실행을 검증했다. 실제 Spring·외부 API를 포함한 운영 전체 E2E와 구분한다. 사본에도 pending 184,385개가 남아 점수/완료 증표가 차단됐고 coverage/identity/receipt/발행 배치 0이었다.

### 최신 운영 migration 사전 점검

2026-11-22 읽기 전용 감사: PostgreSQL 16.15, DB 약391MB, ledger는 `stage2_storage_contract`와 `drop_payload_based_change_history_20260904`뿐이며 네 migration은 계속 운영 미적용이다. 관측 시 다른 세션·잠금 0, app 대기, ownerclan pending 184,385개였다. C: 여유 약165.6GB·Docker FS 약1.018TB·현재 WAL 약134.2MB는 현재 작은 DB의 값이다.

새 복구점 `C:/dev/Chozy/backups/migration-preflight-20261122/`: custom dump 25,853,686 bytes(4.445초), data/config/.env ZIP, 비밀번호 제외 role 정의·SHA manifest. ZIP/현재 상태 차이 0. 새 격리 DB에 19.256초 복원 후 schema/인덱스 검사와 기존 사본의 전체 행 해시가 일치해 기존 migration/백필 검증을 재사용했다. `--no-owner --no-acl` 복원이므로 role/ACL·로그인 비밀번호·PITR·서버 장애 복구까지 증명하지 않는다. 이전 복구점은 `C:/dev/Chozy/backups/operations-readiness-20261121/`다.

유일한 앱 로그인 `collector`는 superuser/createdb/createrole/replication/bypassrls이고 별도 Spring/runtime 최소 권한 역할이 없다. `read_supply_home`, `reserve_api_call`은 SECURITY INVOKER라 EXECUTE만으로 충분하지 않으며 예산 테이블 직접 UPDATE는 우회 위험이 있다. 운영 권한은 변경하지 않았다. 최소 권한·실제 연결 역할·전환 창·적용 직전 상태 재확인·최종 승인이 남는다. 쓰기 대상은 기존 네 migration뿐이며 수집·점수·자동화 활성화와 별개다. 적용/복구 절차는 [RUNBOOK](RUNBOOK.md#운영-전환-준비와-적용-순서).

감사/복원 근거는 `artifacts/validation/migration-preflight.json`, `migration-backup-restore.json`, 검토 SQL은 `migration-preflight-plan.sql`(SHA-256 `eea6fa6f300164651ca35a9331554c986e60146092d2061a861fa1b096ee5332`)이다. 감사는 point-in-time이며 writer fence가 아니다.

### DB 권한 분리 검증

2026-11-22 외부 `migration-preflight-20261122/collector.dump`를 새 PostgreSQL16·CPU2/RAM2GiB·localhost55450/collector_operations_copy에 `--no-owner --no-acl`로 복원했다. 운영에는 접속/변경하지 않고 사본에 기존 네 migration과 새 `runtime_budget_guard`, 최소 권한 계획을 적용했다. 구조/정책은 [DATABASE](DATABASE.md#실행-계정과-권한-분리), 배포/복구는 [RUNBOOK](RUNBOOK.md#권한-분리-사본-검증과-배포)가 기준이다.

관리자와 다른 실제 비소유자 LOGIN 두 개에 각각 Collector/Spring NOLOGIN 역할만 부여해 검증했다. 복원 사본과 별도 작은 fixture에서 각24건(총48건)의 금지 작업을 확인했다: DDL/테이블 삭제/TRUNCATE/트리거 해제/role 생성/sequence setval/ledger 변경/예산 직접 수정, Spring의 원본 상품·발행 행 직접 접근, Collector의 완료 추천 행 수정·인증 근거 변경은 권한 오류로 거부됐다. 실제 Runner로 수집→상세→initial 발행→미반환 시 이전 완료본 유지→재개→새 job incremental을 통과했고 Spring 함수 조회가 같은 완료 배치와 일치했다. source HTTP는 fixture이며 외부API0회다.

두 역할의 동시 예약8건에서 한도5건만 reserved·나머지3건 exhausted:day였고 실제 usage5를 보존했다. 차단 함수는 blocked=true만 설정하고 직접 차단 해제/사용량 삭제는 거부했다. 임시 동명 테이블로 예산/조회 함수를 가리는 시도도 통과하지 못했다. 복원 사본의 미완료 상세184,385개는 전후 동일하고 runtime 점수 실행은 `Detail refresh incomplete`로 차단됐다.

근거는 `artifacts/validation/database-access.json`, 새 역할용 SQL은 `database-access-plan.sql`, 재현은 `python -m scripts.rehearse_access`의 기본 무접속 plan이다. 이 결과는 사본의 DB 권한·fixture 프로세스 검증이며 실제 Spring 배포/로그인·공동 계정 운영·tenant별 권한·쿠팡/네이버 권한·PITR은 미검증이다. 운영의 기존 네 migration·helper·권한/DSN 전환은 모두 미적용이다.

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

### 추가 성능 측정과 채택 판단

기존 `operations-readiness/publication-capacity.json`의 **seconds_including_commit**: 50만 유효 상품 23.866635초(work_mem 8MB), 16.741411초(64MB). **16,723.9초의 보존 근거는 없다.** EXPLAIN ms와 벽시계 초를 혼동하지 않는다. 10만은 각각1.290/2.168초, WAL 103.6~105.8MB; 50만 WAL 538.5~556.4MB. LIKE fixture는 인덱스/CHECK는 유지하지만 FK/입력 trigger·실제 분포는 재현하지 않았다. SELECT EXPLAIN은 external merge와 temp written 약42,200 blocks(346MB)를 보였다.

정렬에서 payload를 뺀 MATERIALIZED rank CTE+join 대안의 50만 ABBA: current7.947/17.326초, candidate15.967/14.329초, 전체 순위 차이0. 안정적 이득이 없어 대안 SQL·work_mem 확대를 채택하지 않았다. `publication-comparison.json`, `publication-comparison-sql.json`에 근거를 둔다.

### 발행 비용 분리와 migration 사전 점검

2026-11-22 `publication_profile.py`의 별도 합성 payload 부분집합·전부 유효 fixture. 원본 상품 FK/입력 trigger는 생략했지만 발행 FK/두 unique 인덱스를 유지했다. work_mem8MB, CPU2/RAM2GiB/shared_buffers256MB에서 실제 publish+commit을 측정했다.

| 행 수 | 기본 → seqscan 비활성 → seqscan 비활성 → 기본 |
| --- | --- |
| 100,000 | 2.025 → 2.719 → 3.494 → 3.544초 |
| 500,000 | 16.355 → 11.292 → 22.169 → 11.632초 |

8회 모두 전체 product_id/순위/payload 차이0. **기본도 인덱스 계획**이어서 접근 방식 개선을 분리한 실험이 아니고 cache/checkpoint 상태도 통제하지 않았다. planner 설정은 채택하지 않았다. 50만 INSERT10.961~21.820초·완료 UPDATE.272~.286초·commit.003~.501초. 별도 실제 INSERT EXPLAIN14.410초 중 입력 subquery1.297초, temp0이었다. heap-only4.379초는 인덱스·충돌 검사·FK가 함께 빠진 값이라 각각의 기여율로 해석하지 않는다. EXPLAIN 노드 시간은 누적값이라 합산하지 않는다.

해당 50만 배치 heap/TOAST314.5MB·인덱스56.0MB·WAL448.1~463.4MB. 작은 합성값을 전체 DB/1,500만 용량으로 환산하지 않는다. 후속 비용 분리는 아래에 기록한다. 인덱스/충돌 처리를 제거하려면 중복 실행·실패·이전 완료본 보존 계약부터 증명해야 한다. 근거는 `artifacts/experiments/publication-profile/profile.json`.

### 발행 쓰기 비용의 반복 비교

2026-11-22 새 전용 PostgreSQL16·CPU2/RAM2GiB/shared_buffers256MB/work_mem8MB/max_wal_size512MB/checkpoint_timeout1h에서 동일 합성 payload·고정 순위 분포로 10만/50만×6조건×3회=36회를 비교했다. 최종 실행에는 다른 테스트/복원/benchmark를 함께 돌리지 않았다. 앞선 탐색 실행은 일부에 테스트/복원이 겹쳐 최종 비교에서 제외했다. 원본 상품 FK/입력 trigger·실제 상품 분포·동시 Spring 부하는 생략했으며, 발행 FK는 해당 조건에 명시적으로 유지했다. 순서를 교대/회전했으나 cold cache를 재현한 것은 아니다.

| 50만 INSERT+commit 조건 | 중앙값(최소~최대), 초 | WAL, MB |
| --- | ---: | ---: |
| heap만 | 2.490 (2.275~2.671) | 304.9 |
| 동일 키의 비고유 인덱스2개 | 12.273 (4.514~12.632) | 403.5~416.8 |
| 고유 인덱스2개 | 6.806 (5.576~13.710) | 407.5~417.9 |
| 고유 인덱스+ON CONFLICT | 7.123 (6.405~13.464) | 431.6~439.6 |
| 위 조건+발행 FK | 13.197 (9.547~19.614) | 431.5~443.8 |
| 위 조건+직전 명시적 CHECKPOINT | 10.947 (9.396~11.478) | 433.8~434.2 |

각 값은 독립 실행이며 **행 간 차이를 합산해 기여율로 해석하지 않는다.** 비고유/고유 인덱스의 시간 역전과 큰 편차 때문에 고유 검사 자체의 비용을 이 표의 차이로 확정할 수 없다. FK 조건의 실제 EXPLAIN에서는 입력 subquery1.181~1.247초, FK trigger2.271~2.338초/50만회, root I/O read.100~.120초/write.427~.559초, temp written0을 관측했다. 노드 시간은 누적값이며 합산하지 않는다. 직전 CHECKPOINT 자체는.068~.320초로 표 시간 밖이며 운영 checkpoint 중첩·WAL CPU 비용의 독립적인 인과 실험은 아니다.

36회 모두 전체 ID 집합·동점 순위·payload 차이0이다. 동일 source에 변경하지 않은 업무 `publish()`를 호출한 별도 단회는 10만1.731초/50만8.566초였고 완료 후 재호출에서 중복 행0이었다. 이전 측정과 payload·cache 조건이 달라 코드 최적화 효과로 보고하지 않는다. FK 조건의 50만 heap/TOAST292.7MB·인덱스44.1MB, 전용 volume 약1.268GB를 확인했으며 실제 1,500만 전체 DB 용량으로 환산하지 않는다. 원문 EXPLAIN BUFFERS/WAL/trigger·bgwriter 전후값·commit·크기는 `artifacts/experiments/publication-costs/results.json`에 보존한다.

재현: 새 전용 `chozy-publication-costs` 서버를 기존 profile 예시와 같은 자원/포트/label로 만들고 `-c checkpoint_timeout=1h`를 추가한 뒤 `python -m scripts.benchmarks.publication_costs --execute`를 실행한다. 기본값은 무접속 plan이며 기존 4GiB/25분 guard를 유지한다. 최종 운영 서버/분포/SLA/보관 기간 확정과 100만→300만→500만→1,500만 실측은 계속 남는다.

동일 source·고유 인덱스2개·발행 FK를 유지하고 각 실행 직전 CHECKPOINT를 수행한 후 `ON CONFLICT`와 plain INSERT를 ABBA+BA 순서로 각3회 비교했다. EXPLAIN INSERT+commit 중앙값은 **9.266→6.764초(약27.0% 감소)**, 범위는 각각8.759~16.461초/6.542~9.505초였으며 전체 ID/순위/payload 차이0이었다. 이는 합성 INSERT 구간의 비교이며 일일 수집/점수 전체의 개선율이 아니다. WAL 비활성화·인덱스/FK 제거·planner 설정 변경은 채택하지 않았다.

발행 행 INSERT의 충돌 무시만 제거하고 scoring_runs 행 잠금·완료본 재호출 no-op·영속화된 미완료 행의 무결성 검토 차단을 보강해 채택했다. 새 실제 `publish()`+commit 50만 건3회는 **6.600/6.729/6.856초**, 매번 재호출 후에도50만 행·순위/payload 차이0이었다. 이전 업무 경로의8.566초는 단회이므로 전체 함수 개선율로 확정하지 않는다. 동시 직접 호출·미완료 배치의 잘못된 payload 혼합 차단·기존 rollback/다른 소싱처 보존·commit 응답 유실 재개는 집중 회귀63개에서 통과했다. 의미/차단 정책은 [ARCHITECTURE](ARCHITECTURE.md#증분과-발행)를 따른다.

추가 근거는 `artifacts/experiments/publication-costs/conflicts.json`, 재현은 위 비용 fixture 완료 후 `python -m scripts.benchmarks.publication_conflicts --execute`, 채택 함수 검증은 같은 명령에 `--verify-publish`를 추가한다. 두 도구 모두 기본값은 무접속 plan이다.

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
