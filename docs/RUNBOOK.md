# 수집 실행과 복구

통합 운영 진입점은 `python -m product_data_collector.operations`다. 기본은 오프라인 계획이며 `--execute`와 명시적인 DSN 없이는 DB·API를 실행하지 않는다. 운영 준비 상태는 [CONTEXT](CONTEXT.md)를 확인한다.

## 설정

프로젝트 루트 `.env.example`을 `.env`로 복사해 로컬 값을 설정한다. 실제 값과 검토한 운영 YAML은 Git에 넣지 않는다. 로컬 설정 이름은 `*.local.yaml` 또는 `operations.reviewed.yaml`을 사용한다.

| 영역 | 환경변수 |
| --- | --- |
| PostgreSQL 기존 수집기 | `POSTGRES_ENABLED/HOST/PORT/DB/USER/PASSWORD` |
| 오너클랜 | `OWNERCLAN_USERNAME/PASSWORD/ENV` |
| 도매매 | `DOMEGGOOK_OpenAPI_KEY`, Private 승인 후 `DOMEGGOOK_PRIVATE_API_KEY` (통합 동기화) |
| 쿠팡 | `COUPANG_ACCESS_KEY/SECRET_KEY` |
| 네이버 검색광고 | `NAVER_AD_API_KEY/SECRET_KEY/CUSTOMER_ID` |
| 네이버 API HUB | `NAVER_HUB_KEY_ID/KEY` |
| 통합 운영·migration | `--dsn-env`로 이름을 지정한 DSN 환경변수 |

네이버·통합 실행기·migration·scoring CLI에는 환경변수를 실행 프로세스에 명시적으로 공급한다. 통합 실행기의 기존 공급처 로더는 `.env`를 읽을 수 있다. 독립 도매매 `sync_cli`는 기존 `DOMEGGOOK_PrivateAPI_KEY`도 받는다. 설정 예제는 `product_data_collector/operations/example.yaml`, `coupang_API/config/best.example.yaml`, `naver_API/season.example.yaml`이다. 빈 카테고리·예산·REVIEW_REQUIRED 값은 검토해 채운다.

```powershell
python -m product_data_collector.operations plan --config product_data_collector/operations/example.yaml
```

예제의 schedules는 비어 있고 automatic_enabled=false다. 실제 KST 시각·주기·계정·호출 예산은 별도로 정한다.

## 로컬 DB와 schema 준비

아래는 `.env`의 로컬 DB를 실제로 시작·초기화하는 명령이다. `check_postgres.py`는 단순 ping과 함께 빈 DB의 공통 schema를 초기화한다.

```powershell
docker compose up -d postgres
python scripts/check_postgres.py
python -m product_data_collector.persistence.migrations --plan
```

기존 DB는 [DATABASE의 순서](DATABASE.md#migration과-백필)로 필요한 migration을 검토·적용한다. 일반 수집 startup은 자동 migration하지 않는다. 예를 들어 검토한 DSN이 REVIEWED_COLLECTOR_DSN에 공급되어 있을 때:

```powershell
python -m product_data_collector.persistence.migrations --apply stage2_storage_contract --dsn-env REVIEWED_COLLECTOR_DSN
python -m product_data_collector.persistence.backfill --dsn-env REVIEWED_COLLECTOR_DSN --name current-v2 --batch-size 500 --max-batches 1
python -m product_data_collector.persistence.backfill --dsn-env REVIEWED_COLLECTOR_DSN --verify --after-id 0 --batch-size 500
```

백필은 같은 name으로 반복하고 대조는 출력 next_id로 이어간다. 실패 시 신규 테이블을 삭제해 되돌리지 않는다. 구형 저장 경로 복귀가 필요하면 새 이벤트 worker를 중단하고 검토된 `COLLECTOR_STORAGE_VERSION=1` 경로를 사용한다. 이 설정만으로 새 기능의 전체 rollback이 되는 것은 아니다.

## 운영 전환 준비와 적용 순서

현재 상태·미적용 migration·최신 복구점은 [CONTEXT](CONTEXT.md)와 [검증](VALIDATION.md#최신-운영-migration-사전-점검)에 있다. 사본 migration/전체 백필·fixture E2E는 통과했고 운영 적용은 별도다.

1. **대상/복구점:** 명시적 DSN·DB버전·schema/search_path·ledger·필수 컬럼/인덱스를 대조한다. stage1 ledger 부재만으로 재적용하지 않는다. DB와 공급처 data/state/log/config의 일관된 백업을 확보하고 새 격리 DB 복원 가능성을 확인한다.
2. **적용 전 검토/최종 승인:** 기존 사본 근거를 재사용하되 적용 시점 변경분을 확인한다. Collector/Spring role/ACL·최소 권한, 같은 계정/소싱처 writer 정지·배출, 장기 트랜잭션·DDL잠금, DB/인덱스/WAL/temp/백업 공간, 위험·복구 절차·전환 창을 보고한 뒤 최종 승인을 받는다. 논리 복원은 권한/PITR·운영 lock·1,500만 용량 검증을 대체하지 않는다. 승인 전 운영 migration은 실행하지 않는다.
3. **migration:** stage2 호환성을 확인하고 아래 네 개를 순차 적용한다. 선행 migration은 자동 적용하지 않으며 쿠팡/네이버 migration은 해당 기능 전환 시 별도 검토한다.

   ```powershell
   python -m product_data_collector.persistence.migrations --apply stage7_supply_scoring --dsn-env REVIEWED_COLLECTOR_DSN
   python -m product_data_collector.persistence.migrations --apply stage8_operations --dsn-env REVIEWED_COLLECTOR_DSN
   python -m product_data_collector.persistence.migrations --apply stage7_history_policy_v2 --dsn-env REVIEWED_COLLECTOR_DSN
   python -m product_data_collector.persistence.migrations --apply stage10_scoring_performance --dsn-env REVIEWED_COLLECTOR_DSN
   ```

   단계별 transaction/ledger, lock_timeout5초다. 실패 단계만 rollback되며 앞서 commit한 단계는 유지한다. 잠금 실패 시 timeout을 늘리거나 수집을 무작정 재시작하지 않고 원인·ledger/schema를 재확인한다. migration은 기존 전체 점수/발행을 실행하지 않는다.
4. **설정/예산/배포:** reviewed YAML의 source/account·default/auth별 초/분/일/월 예산·min_interval/chunk를 실제 승인량과 공동 사용에 맞춘다. 자동 실행false/schedules=[] 유지, 첫 점수는 initial. 별도 operations process에 DSN/자격증명·reviewed YAML을 주입한다. 이미지에서 제외되는 기존 data/state/log를 source YAML과 같은 경로에 mount하고 비daily 기존 DSN도 일치시킨다. 빈 디렉터리로 checkpoint를 대체하지 않는다.
5. **수동 수집:** API 회복·공동 예산/감시/중단 조건·실제 재개 승인을 갖춘 뒤 기존 ownerclan daily checkpoint/pending을 잇는다. 기존 카탈로그에 bootstrap을 다시 시작하지 않는다. pending 수량을 읽기 전용 재확인하고 lease/버전/retry를 보존한다. wrapper와 통합 실행기를 병행하지 않는다. 도매매 Private는 승인/성공 계약 검증 전 제외한다.
6. **계산/조회:** `collect_history→refresh_details→certify_collection→scoring_boundary→run_scoring→publish`다. 상세/입력revision 완료 증표가 없으면 중단하며 증표를 직접 삽입하거나 강제 완료하지 않는다. 전체 유효 집합 정렬/원자 발행은 run_scoring 내부이며 수동 SQL 발행을 추가하지 않는다. NULL/결측 사유·전체 batch/count/rank·read_supply_home·Spring 실제 역할/공동 예약/알림을 확인한다. 90일 coverage/공급사 인증은 별도 실제 근거가 필요하다.
7. **증분/자동화:** initial 완료 후 **새 job부터** incremental로 전환해 실제 변경/공급사 영향/만료·중단/재개를 검증한다. 미완료 job의 hash/as-of/scope는 바꾸지 않는다. 일일 시간/용량·복구·Spring 연동 검증 및 별도 승인 뒤만 schedules/automatic_enabled/감시/보관 정책을 활성화한다.
8. **복구:** 아래 [중단과 복구](#중단과-복구)를 따른다. 이전 완료본을 보존하고 table/trigger/ledger 삭제를 rollback으로 사용하지 않는다. wrapper 복귀도 새 schema/trigger 호환성을 사본에서 확인한다. DB복원은 전환 후 신규 데이터 보존/대조를 포함한 별도 작업이며 백업을 운영에 무조건 덮어쓰지 않는다.

### 사본 검증과 선택형 Docker 실행

```powershell
python -m scripts.migration_preflight
python -m scripts.migration_preflight --execute --backup-dir C:/dev/Chozy/backups/<새-복구점-이름>
python -m scripts.rehearse_migrations --dsn-env OPERATIONS_COPY_DSN
```

preflight 기본은 무접속 계획, execute는 Compose 운영 DB 읽기 전용 감사와 **저장소 밖 새 경로**에 custom dump·data/config/.env ZIP·비밀번호 제외role 정의·SHA를 남긴다. migration/수집/권한 변경은 없다. point-in-time 관측으로 writer fence를 대신하지 않는다. 계정 전환 상태는 [CONTEXT](CONTEXT.md), 예산 함수와 역할의 권한은 [DATABASE](DATABASE.md#실행-계정과-권한-분리)를 참고한다.

복원은 새 격리 DB에 `pg_restore --exit-on-error --no-owner --no-acl`, 운영 덮어쓰기 금지다. 이 복원은 role/ACL·PITR을 검증하지 않는다. rehearse의 execute는 별도서버 `127.0.0.1:<비표준포트>/collector_operations_copy`만 허용하고 전체 행 해시 비교를 하므로 시간/temp 비용이 든다. 이미 통과한 사본 검증을 운영 모니터링으로 반복하지 않는다. 필요한 사본 백필은 `backfill --dsn-env OPERATIONS_COPY_DSN --name operations-rehearsal --batch-size 500 --max-batches 20`, 대조는 같은 옵션의 `--verify`로 이어간다.

```powershell
docker compose --profile operations build operations
docker compose --profile operations run --rm --no-deps operations
# 승인된 DSN과 reviewed YAML을 제공한 뒤 실행
docker compose --profile operations run --rm --no-deps operations status --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
```

operations profile은 app과 독립적이고 기본plan·restart/scheduler/DB자동migration 없음이다. 저장소는 read-only, 기존 ownerclan data는 같은 경로에 writable mount하며 없는 경로는 자동 생성하지 않는다. 다른 공급처는 기존state/log/인증도 연결한다. 컨테이너의127.0.0.1은 호스트DB가 아니므로 Compose DB는 host=postgres/port5432를 사용하고 DSN을 로그에 출력하지 않는다.

### 권한 분리 사본 검증과 배포

역할/허용 범위는 [DATABASE](DATABASE.md#실행-계정과-권한-분리), 최신 사본 검증은 [VALIDATION](VALIDATION.md#db-권한-분리-검증)을 따른다. 새 도구의 기본값은 무접속 plan이다.

```powershell
python -m scripts.rehearse_access
# 새 전용 서버의 빈 collector_operations_copy에 승인된 백업을 먼저 복원한다.
# 전용 컨테이너: chozy-access-rehearsal, label chozy.collector-access=true
# localhost:55450만 허용하며 운영 DB를 대상으로 실행할 수 없다.
python -m scripts.rehearse_access --execute --dsn-env ACCESS_REHEARSAL_DSN
```

execute는 사본에 기존 네 migration과 `runtime_budget_guard`, 새 시험 역할/폐기용 로그인 계정을 만들고 실제 Runner 회귀를 수행한다. SQL 계획은 `artifacts/validation/database-access-plan.sql`, 결과는 `database-access.json`이다. 계획은 새 역할용이며 기존 역할이 있으면 생성 단계에서 중단한다. 부분 적용을 수동으로 이어가지 않고 전체 transaction을 rollback한다. 시험 후 해당 전용 서버/volume만 제거한다.

운영 전환은 **최종 승인 후** 다음 순서로 수행한다. 이 문서 갱신은 승인이나 운영 적용이 아니다.

1. 기존 적용 절차의 직전 백업·writer/잠금·공간·ledger gate를 확인하고 기존 네 migration을 순서대로 적용한다. 이어 관리자 DSN으로 `--apply runtime_budget_guard`를 실행한다. 이 helper는 예산 데이터를 변경하지 않는다.
2. 사본에서 검토한 권한 계획을 실제 schema/새 역할 이름/관리자에 맞춰 검토 후 한 transaction으로 적용한다. 관리자와 함수 소유자는 runtime 로그인의 상속 대상에서 제외한다. PUBLIC 회수의 다른 앱 영향과 관리자별 default privileges도 확인한다.
3. 별도 안전한 경로로 실제 로그인 계정/비밀번호를 발급하고 runtime 역할만 부여한다. runtime/Spring DSN으로 상태·예약·완료본 조회를 확인하고 수집 설정의 관리자 DSN을 교체한다. 비밀번호는 SQL 산출물/로그에 저장하지 않는다. 새 코드 배포 전에 차단 helper가 존재해야 한다. 누락 시 BudgetSession은 예약/HTTP 전에 `runtime_budget_guard_missing`으로 중단한다. `operations policy`와 차단 해제는 관리자 DSN에서만 실행한다.
4. 실제 Spring 연결·공동 예산·경고 확인과 수동 initial/incremental을 수행한다. 자동화 활성화는 별도 승인 사항이다. 쿠팡/네이버는 해당 객체의 권한 검토 전 실행 대상에서 제외한다.

문제가 생기면 writer를 멈추고 이전 완료본을 유지한다. 새 runtime 로그인/DSN 사용을 중단하고 승인된 복구 절차를 따른다. 임시 대응으로 runtime에 superuser나 예산 직접 UPDATE를 부여하지 않는다. helper/권한을 적용한 DB에서 구버전 BudgetSession은 403/429 차단 시 직접 UPDATE가 거부되므로 구버전 코드만 단독 복귀시키지 않는다. 복구에 필요한 역할 속성·ACL·함수 정의/소유자/설정은 적용 직전 별도로 백업한다.

## 통합 실행

검토한 설정·선행 schema·자격증명이 준비된 환경에서 사용한다. 아래 날짜는 CLI 형식 예시이므로 실제 슬롯으로 바꾼다.

```powershell
python -m product_data_collector.operations policy --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
python -m product_data_collector.operations run --task ownerclan-bootstrap --at 2026-11-20T00:00:00+09:00 --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
python -m product_data_collector.operations run --task ownerclan --at 2026-11-20T09:00:00+09:00 --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
```

일일 task는 `domeggook`, `ownerclan`, `coupang`, `naver`, `comparison`이다. 상품 source에는 `*-bootstrap`, `*-discovery`, `*-recovery`도 있다. source별 수집·상세 완료 뒤 점수와 발행을 진행한다. 초기 점수는 scoring_mode=initial, 완료 후 새 작업부터 incremental을 사용한다. 같은 미완료 job의 설정을 바꾸어 재개하지 않는다.

도매매 bootstrap 완료는 목록 순회 완료다. startedAt부터 적재 종료 이후까지 별도 sync catch-up을 수행해야 한다. Private 미승인 기간은 미확인 구간으로 남긴다. 장기 적재 중에도 피드 보존 기간을 넘기지 않아야 한다.

자동 실행은 검토한 schedules와 automatic_enabled=true를 설정한 뒤 시작한다.

```powershell
python -m product_data_collector.operations schedule --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
```

### 공동 호출 예산

`(account, api)`별 second/minute/day/month 상한과 `min_interval`을 설정한다. [외부 API 호출 한도](API_CONTRACTS.md#외부-api-호출-한도)를 확인한 뒤 실제 계정 정책을 적용한다. HTTP 시도 전 별도 트랜잭션에서 `reserve_api_call`을 commit한다. 실패·timeout·commit 응답 불명은 사용량에 남고 재시도에는 다시 예약한다. HTTP 403/429는 차단 상태를 남기고 중단한다.

같은 자격증명으로 호출하는 Spring과 Collector 경로에는 같은 DB와 `(account, api)`를 설정한다. `require_platform_budget=true`는 미연동 상태를 거부한다. 모든 공유 호출 경로가 예약을 사용하는지 확인한 뒤 `platform_integrated=true`를 설정한다. 개발 검증과 운영 적용 상태는 [CONTEXT](CONTEXT.md#구현과-운영-상태)에 있다.

기존 독립 CLI와 개발 wrapper에는 공동 예산이 자동 연결되지 않는다. 같은 계정을 병행 운영하려면 transport 연동을 먼저 확인한다.

## 중단과 복구

오너클랜의 null·빈 배열·미반환·재시도 소진은 정상 완료나 삭제 신호가 아니다. 상세 요청의 requested/completed 버전·retry·next_retry_at을 보존하고, 실패 직후 같은 key를 강제로 반복하거나 완료 버전을 수동으로 올리지 않는다. 정상 상품은 이미 commit됐을 수 있으므로 전체 결과를 롤백하거나 초기화하지 않는다. 통합 실행은 상세 미완료이면 점수·발행 단계로 넘어가지 않는다. 직접 점수 CLI도 운영 migration 유무와 관계없이 미완료 상세가 있으면 새 실행/발행을 거부하도록 보강했다. 이전 정상 발행본은 유지한다. [격리 검증과 확인된 결함](VALIDATION.md#오너클랜-실패재개-및-발행-안전성-격리-검증)을 따른다. 이 검사는 운영 migration·입력 보호 배포를 대신하지 않는다.

| 상태·상황 | 조치 |
| --- | --- |
| paused | 예산·시간·범위 제한과 남은 상세 요청을 확인하고 같은 scope로 재개 |
| failed | error·cursor·요청 버전을 확인하고 원인 해결 후 재개 |
| running인데 프로세스 없음 | 완료로 수정하지 않고 재실행하여 DB session lock과 checkpoint로 소유권 재확인 |
| 입력 revision·설정 scope 변경 | 기존 결과를 섞지 않고 새 job/run으로 교체 |
| 인증·403/429·영구 상세 오류 | 원인을 해결한 뒤 차단 또는 해당 요청 next_retry_at을 운영자가 재설정 |

```powershell
python -m product_data_collector.operations status --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
python -m product_data_collector.operations retry --job-id '<기존-id>' --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
python -m product_data_collector.operations supersede --job-id '<기존-id>' --replacement-id '<새-id>' --config operations.reviewed.yaml --dsn-env REVIEWED_COLLECTOR_DSN --execute
```

retry는 같은 scope·기준시각을 유지한다. supersede는 기존 실패 기록을 남기고 새 작업을 만든다. 새 작업은 수집부터 다시 검증한다. SIGINT/SIGTERM 후 진행 중 요청·청크가 정리될 시간을 주고 강제 종료된 경우 마지막 commit/checkpoint부터 재개한다.

오너클랜 daily는 `incremental-state.json`의 pendingHistory, bootstrap/backfill은 별도 상태 파일을 사용한다. 도매매 새 경로는 지정한 state-dir의 고정 범위·searchAfter·예산을 보존한다. 기존 Open 경로는 discovery/detail/recent 상태 파일을 사용한다. 쿠팡 best·네이버·scoring은 DB manifest/ledger에서 재개한다. 상태와 원본 이력을 삭제해 실패를 숨기지 않는다.

오너클랜 daily/refresh와 통합 실행기는 최대 100개 묶음 상세 worker를 사용한다. --limit는 HTTP 호출 수가 아니라 처리할 상품 수다. 누락 key만 한 번 보완하며, 계속 누락되면 pending과 원인을 보존한다. 처리 중 중단되면 완료 상품은 유지하고 미완료 상품은 기존 lease 만료·next_retry_at 이후 같은 모드로 재개한다. 이번 전환에 새 migration은 없다.

기본 status는 최근 작업·경고·발행·예산·DB 통계를 반환한다. status_full=true는 상세 대기·queue·결측·coverage 집계를 추가하므로 큰 테이블의 상시 poll에는 사용하지 않는다. ops_alerts는 outbox이며 실제 알림 전달과 scheduler 외부 감시는 별도 연동 대상이다.

## 공급처별 명령

### 오너클랜 호출 속도와 오류 후 재개

제공처 안내는 [API_CONTRACTS](API_CONTRACTS.md#공식-호출-제한-안내의-범위), 시도·확보 수는 [VALIDATION](VALIDATION.md#오너클랜-호출-정책과-하루-1만-상품-조사)에 있다. 아래 수치는 내부 운영 권고로, 배포 전 계정 상태와 함께 검토한다.

- 회복 확인은 계정 합산30RPM 이하, 정상 확인 후 권고60RPM(worker1·최대100개). 120RPM은 공동사용/정상응답을 확인한 뒤 별도 지속 검증할 후보일 뿐이다. 응답이 느리면 시작 간격을 응답 뒤 추가로 매번 더하지 않는다.
- HTTP시도/실패/보완/retry/인증과 Spring/외부호스트 사용을 함께 센다. JWT 재발급이 예산을 초기화하지 않는다. 최근 단건 성공으로 대량 회복·50개 축소·병렬 증가 효과를 확정하지 않는다.
- `itemHistories(first:1000, 고정구간/after)→고유key→100개상세`의 event/commit/checkpoint·누락 대조를 유지한다. 고정key의 최신상세만 필요하면 직접묶음과 비교하고, 최초key발견은 allItems(최대1000)를 사용한다. 둘을 같은 처리량으로 환산하지 않는다.

| 오류 | 중단/재개 |
| --- | --- |
| 429/GraphQL quota | 계정 전체 새 요청중단. Retry-After 숫자/HTTP-date·명시reset이 있으면 `max(90초, 해당대기)+0~10초jitter` 후 운영자 단일확인. 긴 값을90/300초로 자르지 않음 |
| 제한 대기정보 없음 | 90→180→360초 뒤 각1회만 확인. 계속제한이면 자동재개 중지·계정문의. 성공해도 이전속도 절반 이하/최대30~60RPM |
| 403 | 고정대기초 없음. 권한/승인/IP/WAF 원인을 해결하기 전 반복하지 않음. 제공처가 일시quota/reset을 명시한 경우만 그 시각 이후확인 |
| 시간/일 예산초과 | 명시reset까지 대기. reset미상은 문의/운영확인, 정각/KST자정 초기화 추정금지 |
| timeout/502/503/504 | 429와구분. Retry-After존중, 없으면90초이상 뒤1회확인. 근접확인에서 재발하면중지하고15분이상 후 재확인. 공식복구시간 실측값은 아님 |

공통 worker/BudgetSession은 제한시중단하지만 이 표의 자동차단해제/단계별회복은 구현돼 있지 않다. 운영자가 원인확인 후 blocked/next_retry_at을 재설정한다. legacy client의 Retry-After는 숫자/설정상한만 지원하므로 긴/날짜형은 별도 준수한다. 표준근거: [Retry-After/403](https://www.rfc-editor.org/rfc/rfc9110.html), [429](https://www.rfc-editor.org/rfc/rfc6585.html#section-4).

조사 도구는 기본 무호출계획, execute만 실제호출이다. 실행 전 local수집/계정사용·이번예산·중단조건을확인하고 기존결과의 우발재실행을 피한다. 진단별 과거 승인횟수는 새실행예산으로 전용하지 않는다. 첫timeout/제한/불완전반환 중단의 진단과 업무worker의 제한보완을 구분한다.

```powershell
python -m scripts.probes.ownerclan_live_e2e --bounded-recovery
python -m scripts.probes.ownerclan_session_probe
```

live_e2e는 실제 변경 피드 앞부분의 약 5,000개 key 표본을 대상으로 event→상세→점수→발행→SQL 조회를 연결한다. 별도 label의 PostgreSQL, localhost55449·collector_stage1_test만 사용한다. 전체 하루 피드·운영 checkpoint·Spring 완료로 간주하지 않으며 90일 coverage나 공급사 인증을 만들지 않는다. 계측 controller는 미반환으로 나머지 수집을 차단하지 않는다. 실제 worker가 누락1회 보완, 미완료/재시도 보존과 다음 묶음 진행을 담당한다. 네트워크/제공처 오류 및 호출/시간 상한은 별도 중단 조건이다. 전체 시도 후 pending이 남으면 새 점수/발행 차단을 확인한다. `--bounded-recovery`는 호환 옵션이며 같은 동작이다. 인증·조회·시간 상한과 계정 미확인 항목은 결과 JSON에 기록한다. 재실행에는 새 전용 서버와 기존 결과 보존이 필요하다.

개별 동작 확인·수동 복구용이다. **오너클랜과 기존 도매매의 `--dry-run`은 실제 API를 호출한다.** 네트워크 없는 검증은 pytest를 사용한다.

| 대상 | 명령 |
| --- | --- |
| 오너클랜 | `python -m ownerclan_API --mode bootstrap/daily/refresh/discovery/recovery/backfill` 중 모드 하나 선택 |
| 도매매 새 경로 | `python -m domeggook_API.workflows.sync_cli --help` |
| 도매매 기존 Open 경로 | `python -m domeggook_API --mode full` 또는 `--mode daily` |
| 쿠팡 best 계획 | `python -m coupang_API --config coupang.best.local.yaml --date 2026-11-20 --dsn-env COUPANG_BEST_DSN` |
| 네이버 계획 | `python -m naver_API --config naver.season.local.yaml --date 2026-11-20 --dsn-env NAVER_SEASON_DSN` |

쿠팡·네이버 계획에 실제 호출·저장을 선택하려면 --execute를 추가한다. 도매매 Private sync는 승인 후 확인한 provider-timezone과 초기 start를 지정하며 미완료 재개 시 범위를 바꾸지 않는다. 오너클랜 legacy-categories는 명시적으로 선택하는 기존 경로다.

도매매 동기화의 현재 진입점은 `domeggook_API.workflows.sync_cli`다. 기존 `domeggook_API.workflows.stage4_cli` 명령과 `stage4` import는 같은 구현으로 연결하는 호환용 파일로 남겼다. 새 설정·명령에는 기능 이름을 사용한다. 기존 설정의 `data/state/stage4`는 재개 데이터 경로이므로 이름만 바꾸어 새 작업을 시작하지 않는다. Compose command와 현재 실행 중인 수집 컨테이너의 시작 명령은 변경하지 않았다.

## 개발 도구와 배포

| 경로 | 용도 |
| --- | --- |
| `scripts/run_collectors.py`, `run_daily_collector.py` | 오너클랜·도매꾹 개발·표본 실행 |
| `scripts/check_postgres.py` | 로컬 DB 확인·공통 초기화 |
| `scripts/export_product_lookup.py` | 오너클랜·도매매 상품과 최신 완료 쿠팡 베스트 발행본을 CSV·JSONL로 내보내기 |
| `scripts/probes/` | 실제 API·변경 피드·묶음 상세 조사. 실행 전 각 --help와 호출 범위를 확인 |
| `scripts/benchmarks/` | 전용 PostgreSQL 합성 부하·전후 비교. [검증 절차](VALIDATION.md) 참조 |

응답 형식 확인용 상품 예시는 artifacts/samples/에 제공처별 최대 5개만 보관한다. 고정 카테고리 조사 도구의 기본도 5개 카테고리×상품 1개다. 과거 대량 조사를 재현할 때는 외부 백업을 작업 디렉터리에 복원하고, 이력·집계 도구에는 --input-dir, 1만 상품 묶음 실측 도구에는 --source로 원본을 지정한다. 소량 예시를 전체 피드·성능 검증의 입력으로 사용하지 않는다.

`python -m scripts.export_product_lookup --platform coupang`은 PostgreSQL에서 기준일이 가장 최근인 완료 발행본만 읽는다. 여러 카테고리에 나온 상품은 가장 높은 순위의 행을 남긴다. `--platform`을 생략하면 공급처 상품도 함께 내보낸다. 베스트 migration 적용 전이나 완료 발행본이 없을 때 쿠팡 행은 0개다.

Compose 기본 기동은 PostgreSQL만 실행한다. `data-collector`는 `collectors` profile에 있으며, 명시적으로 켜도 Python 대기 프로세스만 실행한다. 컨테이너 기동은 API 호출·DB 초기화·수집을 실행하지 않는다. 통합 운영 이미지는 별도 Dockerfile을 사용한다.

```powershell
docker compose up -d
```

기존 개발 wrapper로 실제 수집할 때만 다음 명령을 명시적으로 실행한다. 오너클랜 이후 도매매를 수집하며 기존 상태에서 재개한다. 실행 전에 계정 예산과 다른 수집 실행 여부를 확인한다.

```powershell
docker compose run --rm data-collector sh -c "pip install --no-cache-dir -r requirements.txt && python scripts/run_collectors.py"
```

DB 초기화가 필요한 경우에는 위의 로컬 DB 준비 절차를 별도로 따른다. 수집 명령에 DB 초기화나 migration을 자동으로 묶지 않는다.

```powershell
docker build -f Dockerfile.operations -t chozy-collector-operations .
docker run --rm chozy-collector-operations
```

기본 이미지는 오프라인 plan을 실행한다. 실제 배포는 검토한 설정·환경변수·DB 권한·보관 정책·프로세스 감시와 종료 유예를 준비해 수행한다. 소량 예시·집계·검증 결과는 artifacts/, 실제 collector 상태는 기존 data/ 경로에 둔다. 실험 중 사용한 대량 원본은 종료 후 저장소 밖으로 분리한다.

### 로컬 자료 정리

배치는 [README](../README.md#폴더와-문서)를 따른다. 조사 스크립트는 `artifacts/experiments/`에 결과를 생성한다. PostgreSQL 합성 smoke 측정은 `artifacts/experiments/scoring-smoke/`, pytest 캐시는 `artifacts/validation/pytest-cache/`를 사용한다. 최신 JUnit과 migration 계획은 같은 파일명을 갱신한다.

실험 종료 후 scratch·이전 검증 결과·집계에 포함된 개별 결과를 제거한다. 삭제 전에 고유 근거·재현용 baseline을 실험 폴더에 남기고, 결론·조건·한계는 해당 기준 문서에 반영한다. 원본 보존이 필요하면 저장소 밖에 백업하고 [VALIDATION](VALIDATION.md)에 복구 위치를 기록한다.

실제 수집의 `data/`와 기존 `logs/`는 정리에서 제외한다. 특히 로그 폴더의 `collector.lock`·`fixed-category-collector.lock`과 오너클랜 설정의 log_dir는 동시 실행 방지에도 사용된다. 이 경로를 옮기려면 수집 중단·재개 상태·기존 컨테이너 설정을 포함한 별도 전환이 필요하다.

### 기존 수집 컨테이너의 시작 명령 전환

이전 컨테이너에는 설치 → DB 확인 → 실제 수집 command가 남을 수 있다. Docker Desktop의 단순 시작·재시작은 저장된 command를 재사용하므로 Compose 파일만 수정해서는 해결되지 않는다. 실행 중 수집은 승인받아 중단하고, DB를 유지한 채 수집 컨테이너만 새 대기 명령으로 재생성한다.

```powershell
docker compose --profile collectors up -d --no-deps --force-recreate data-collector
docker inspect product-data-collector-app --format '{{json .Config.Cmd}}'
docker compose --profile collectors ps
```

현재 command는 `["python","-c","import signal; signal.pause()"]`다. 대기 컨테이너를 restart해도 수집하지 않는다. 소스 수정 후에는 다음 명시적 수집 실행에서 새 코드를 읽으며, 코드를 로딩하려고 대기 컨테이너를 재시작할 필요는 없다.
