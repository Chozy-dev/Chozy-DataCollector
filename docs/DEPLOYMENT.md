# Collector 운영 전환

수집 실행과 재개는 [RUNBOOK](RUNBOOK.md)을 따른다.

## 운영 공동 호출 예산 적용

1. 같은 공급사 자격증명의 Spring·수집기·독립 실행을 일시 정지한다. 같은 공동 PostgreSQL에 연결되는지 확인하고 기존 정책·사용량을 백업한다. 정책 테이블과 기존 함수의 DDL 변경 권한이 있는 관리자 DSN으로 `runtime_budget_guard` 다음에 `python -m product_data_collector.persistence.migrations --apply shared_api_budget --dsn-env REVIEWED_ADMIN_DSN`을 적용한다. 기존 예약 함수의 이름·사용량·차단 상태·권한을 보존한다.
2. 검토한 운영 YAML의 `budgets`에 아래 매핑을 등록하고 관리자 DSN으로 `python -m product_data_collector.operations policy --config operations.reviewed.yaml --dsn-env REVIEWED_ADMIN_DSN --execute`를 실행한다. 호출 한도는 실제 계정 기준으로 입력한다. 새 `hour` 구간 추가 시 그 시각 이전 호출의 시간별 합계는 자동 복원되지 않으므로, 새 구간부터 전환하거나 기존 사용량을 별도로 확인한다.

   | 계정 별칭 | API | `shared_api` | 한도 |
   | --- | --- | --- | --- |
   | 쿠팡 공통 | `default` | 생략 | 검색·베스트 합산 상한 및 최소 간격 |
   | 쿠팡 공통 | `best` | `default` | 공동 정책의 모든 기간보다 낮은 수집 상한 |
   | 네이버 공통 | `hub` | 생략 | HUB 키 전체의 초당 상한 |
   | 네이버 공통 | `trend`, `shopping` 각각 | `hub` | API별 월 상한. 필요하면 내부 일 상한도 설정 |
   | 네이버 공통 | `ad` | 생략 | 검색광고 내부 상한 및 간격 |

3. Spring 시장 분석의 예산 접속·계정 별칭, 수집기 통합 DSN·`sources.*.account`, 독립 실행의 [공동 예산 환경 변수](RUNBOOK.md#공동-호출-예산)를 동일 정책에 맞춘다. Spring 역할에는 `api_budget_policy` 조회와 예산 함수 실행 권한이 필요하다. 공동 호출 경로를 모두 확인한 뒤 관련 정책에 `platform_integrated: true`를 설정한다.
4. 격리 환경에서 쿠팡 검색 1회와 베스트 1회가 `default`에 합산되고, HUB 트렌드·쇼핑인사이트가 API별 월 상한과 공통 `hub`를 함께 차감하는지 확인한다. 정책 설치 전에 운영 새 코드를 활성화하면 필요한 매핑이 없는 경로가 공급사 HTTP 전에 중단된다.

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
8. **복구:** 아래 [중단과 복구](RUNBOOK.md#중단과-복구)를 따른다. 이전 완료본을 보존하고 table/trigger/ledger 삭제를 rollback으로 사용하지 않는다. wrapper 복귀도 새 schema/trigger 호환성을 사본에서 확인한다. DB복원은 전환 후 신규 데이터 보존/대조를 포함한 별도 작업이며 백업을 운영에 무조건 덮어쓰지 않는다.

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

execute는 사본에 기존 네 migration, `runtime_budget_guard`, `shared_api_budget`을 적용한다. 새 시험 역할과 폐기용 로그인 계정으로 Runner 회귀를 수행한다. SQL 계획은 `artifacts/validation/database-access-plan.sql`, 결과는 `database-access.json`이다. 계획은 새 역할용이며 기존 역할이 있으면 생성 단계에서 중단한다. 부분 적용을 수동으로 이어가지 않고 전체 transaction을 rollback한다. 시험 후 해당 전용 서버/volume만 제거한다.

운영 전환은 **최종 승인 후** 다음 순서로 수행한다. 이 문서 갱신은 승인이나 운영 적용이 아니다.

1. 기존 적용 절차의 직전 백업·writer/잠금·공간·ledger gate를 확인하고 기존 네 migration을 순서대로 적용한다. 이어 관리자 DSN으로 `--apply runtime_budget_guard`를 실행한다. 이 helper는 예산 데이터를 변경하지 않는다.
2. 사본에서 검토한 권한 계획을 실제 schema/새 역할 이름/관리자에 맞춰 검토 후 한 transaction으로 적용한다. 관리자와 함수 소유자는 runtime 로그인의 상속 대상에서 제외한다. PUBLIC 회수의 다른 앱 영향과 관리자별 default privileges도 확인한다.
3. 별도 안전한 경로로 실제 로그인 계정/비밀번호를 발급하고 runtime 역할만 부여한다. runtime/Spring DSN으로 상태·예약·완료본 조회를 확인하고 수집 설정의 관리자 DSN을 교체한다. 비밀번호는 SQL 산출물/로그에 저장하지 않는다. 새 코드 배포 전에 차단 helper가 존재해야 한다. 누락 시 BudgetSession은 예약/HTTP 전에 `runtime_budget_guard_missing`으로 중단한다. `operations policy`와 차단 해제는 관리자 DSN에서만 실행한다.
4. 실제 Spring 연결·공동 예산·경고 확인과 수동 initial/incremental을 수행한다. 자동화 활성화는 별도 승인 사항이다. 쿠팡/네이버는 해당 객체의 권한 검토 전 실행 대상에서 제외한다.

문제가 생기면 writer를 멈추고 이전 완료본을 유지한다. 새 runtime 로그인/DSN 사용을 중단하고 승인된 복구 절차를 따른다. 임시 대응으로 runtime에 superuser나 예산 직접 UPDATE를 부여하지 않는다. helper/권한을 적용한 DB에서 구버전 BudgetSession은 403/429 차단 시 직접 UPDATE가 거부되므로 구버전 코드만 단독 복귀시키지 않는다. 복구에 필요한 역할 속성·ACL·함수 정의/소유자/설정은 적용 직전 별도로 백업한다.
