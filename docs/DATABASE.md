# PostgreSQL 저장과 조회 계약

PostgreSQL 16을 사용한다. DDL의 실제 기준은 `product_data_collector/persistence/`의 schema 모듈과 [migrations.py](../product_data_collector/persistence/migrations.py)다. 아래는 테이블의 역할과 변경 시 유지할 계약이다.

## 상품과 이력

| 테이블 | 키와 역할 |
| --- | --- |
| `products` | `(platform, external_product_id)` 자연키. 표시 정보와 최신 가격·재고·배송·판매 가능 상태 |
| `product_options` | `(product_id, option_identity)` 고유. 현재 옵션·가격 의미·수량·상태·membership |
| `product_history` | 상품별 최초·실제 변경 관측. observed_at, changed_fields, prices/inventory/shipping JSONB |
| `product_events` | dedupe_key 고유. 원본 사건·관측의 출처, scope, 시각, 전후값·확실성·옵션 연결 근거 |
| `product_discovery_targets` | 발견 상품과 맥락, 버전별 상세 요청·완료·lease·실패·재시도 |
| `product_search_ranks` | 검색·카테고리·정렬·페이지 등 조회 맥락별 순위 |
| `product_raw_samples` | 디버깅용 원본 샘플. 플랫폼별 최대 100개 상품 |
| `storage_backfill_progress` | 이름별 high_id·마지막 처리 ID·건수·완료 상태 |
| `schema_migrations` | 명시적으로 적용한 migration 식별자와 시각 |

### 최신 상품

[postgres_storage.py](../product_data_collector/persistence/postgres_storage.py)는 기존 저장 진입점과 배치 트랜잭션을 담당한다. 연결·초기 스키마 검증은 [postgres_connection.py](../product_data_collector/persistence/postgres_connection.py)에 있다. [snapshot_payload.py](../product_data_collector/persistence/snapshot_payload.py)는 상품 응답을 저장 행으로 변환하며, 최신값은 [domain/current_state.py](../product_data_collector/domain/current_state.py)에서 투영한다. 발견 대상·검색 순위 행과 SQL은 [discovery_storage.py](../product_data_collector/persistence/discovery_storage.py), 공통 시간·텍스트·JSON 변환은 [storage_values.py](../product_data_collector/persistence/storage_values.py)에 있다.

- 가격은 `current_supply_price`, `supply_price_status`, `supply_price_basis`로 값과 근거를 함께 저장한다. `source_contract`와 미해결 가격을 구분한다.
- 재고는 `stock_quantity`, `stock_is_infinite`, 상태와 근거를 함께 저장한다. 9999 sentinel과 정확한 실재고를 구분한다.
- 판매 가능 여부, 빠른배송, 평균 발송일, 최소 주문량·주문 단위, 배송 조건, 현재 원문과 `current_observed_at`을 보존한다.
- 제품 표시 정보·판매자 정보와 플랫폼 전용 `source_specific`을 둔다. 공급처 필드는 [API_CONTRACTS](API_CONTRACTS.md)에 정리한다.

동일 자연키의 쓰기는 advisory lock과 관측 시각으로 직렬화한다. 늦은 응답이 최신 상품·옵션을 덮어쓰지 않는다. 같은 상태를 다시 관측하면 last_collected_at 등 최신 관측만 갱신하고 이력을 중복 생성하지 않는다. 상품명·URL·이미지만 바뀐 경우에도 핵심 이력을 추가하지 않는다.

### 옵션과 이벤트

옵션에는 원본 key 또는 검증되지 않은 identity를 구분하고 `price_meaning=final/unknown`, `price_confidence=source_contract/unresolved`를 남긴다. 완전한 옵션 응답이라는 근거가 있을 때만 누락 membership을 반영한다. 일부 응답을 전체 옵션 삭제로 해석하지 않는다.

원본 이벤트는 occurred_at과 실제 수집 observed_at을 분리한다. observation 이벤트의 occurred_at은 NULL이다. 원문 전후값과 검증값, value_status, identity_status, raw_event를 보존한다. 옵션 연결이 미확정이면 option_reference를 남기고 unresolved로 저장한다. 원본 event ID가 없을 때 fingerprint는 동일 내용의 별개 발생을 구분하지 못할 수 있다.

상품을 아직 저장하지 않았어도 이벤트를 자연키로 보존하고 나중에 product_id를 연결할 수 있다. source 사건과 나중에 비교한 관측을 합산하지 않는다. 단순 `stockChanged/optionStockChanged` 수량 신호는 원본 사건 수에 넣지 않고 상세 재조회로 처리한다.

### 상세 재조회

[detail_refresh.py](../product_data_collector/persistence/detail_refresh.py)의 요청·claim·완료·실패 인터페이스를 사용한다. requested_version과 completed_version을 분리하고 processing_version·lease_token·lease_until을 보존한다. API 호출 직전 claim하며 상품 저장과 해당 버전 완료를 같은 트랜잭션에서 확인한다.

실패는 완료 버전을 올리지 않는다. 처리 중 새 요청이 생기면 새 버전은 남는다. 이벤트 페이지 저장 성공과 상세 완료를 별도로 판정한다. 최신값·옵션·원본/관측 이력 조회는 persistence의 current_state/product_options/product_events 모듈을 사용한다.

## 일자별 시장 스냅샷

| 영역 | 테이블 | 계약 |
| --- | --- | --- |
| 쿠팡 | `coupang_best_batches/categories/items` | 날짜별 scope, 카테고리별 진행·예산 예약, 상품·순위·전일 변화 |
| 네이버 | `naver_season_batches/themes/requests/items` | 날짜·테마·요청 part별 checkpoint, 검색수·트렌드·상승률 |
| 네이버 호출 | `naver_call_reservations` | 실제 호출일·월의 API별 예약량 |

`coupang_best_published`, `naver_season_published`는 completed 배치만 노출한다. 날짜 전체의 범위와 데이터 검증이 끝나기 전에는 발행하지 않는다. 재개 시 원래 날짜·카테고리·테마 scope를 유지한다.

## 점수와 운영

| 테이블 | 역할 |
| --- | --- |
| `scoring_coverage`, `scoring_supplier_identity` | 기간·지표·공급사 인증 근거. 기본 미인증 |
| `scoring_supplier_samples` | 공급사·기준시각별 결정적 표본과 집계 |
| `scoring_queue`, `scoring_supplier_jobs` | 변경·만료·공급사 전파와 generation/cursor |
| `product_supply_scores` | 상품별 최신 점수·결측 사유·입력 근거·산식·next_due |
| `scoring_runs` | 고정 as-of·버전·처리 범위·청크 재개 상태 |
| `supply_home_batches/items` | 발행 단위 metadata와 불변 순위 payload |
| `ops_jobs`, `ops_alerts` | 작업 ledger·단계·scope·시도·경고 outbox |
| `ops_collection_receipts`, `ops_score_inputs` | 소싱처 수집 완료 revision과 계산 입력 보호 |
| `api_budget_policy/usage` | 계정·API별 공동 한도·차단·기간별 예약량 |

점수 이력·경계·NULL·공급사 표본·발행 정책은 [ARCHITECTURE](ARCHITECTURE.md)가 기준이다. 원본 90일 anchor, 진행 중 품절, 인증 근거를 만료 처리 목적으로 삭제하지 않는다. 추천·작업·예약의 자동 보관 정리는 아직 구현하지 않았다.

## Platform 조회

| 기능 | 진입점 |
| --- | --- |
| 홈 공급 추천 | SQL `read_supply_home('DOMEME' 또는 'OWNERCLAN', size)` |
| 쿠팡 베스트 | `coupang_API.persistence.best.read_published(connection, category_id)` |
| 네이버 시즌 | `naver_API.queries.read_published(connection, theme_code)` |
| 네이버와 쿠팡 비교 | `naver_API.queries.read_compare(connection, theme_code)` |

조회는 한 응답에서 같은 완료 배치를 선택한다. 완료본 부재와 선택 카테고리·테마의 빈 목록을 구분한다. 최신 카테고리가 없다고 과거 날짜 결과를 섞지 않는다. 추천은 발행된 가격·이름·URL·이미지·점수·asOf·observedAt·formulaVersion·missingReasons를 사용하며 현재 products와 재결합해 과거 payload를 바꾸지 않는다.

Spring은 HTTP envelope·직렬화·기본 선택·사용자 로직을 담당한다. 운영 역할에는 필요한 조회와 예약 함수 권한을 주고 입력 트리거 비활성화·DDL·TRUNCATE·sequence 수정 권한을 주지 않는다.

### 실행 계정과 권한 분리

권한 계획의 기준은 [access.py](../product_data_collector/persistence/access.py)다. 기존 객체 소유자/migration 관리자와 새 `collector_runtime`, `spring_runtime`, `collector_function_owner`를 분리한다. 세 역할은 NOLOGIN이며 실제 로그인 계정에 필요한 runtime 역할만 부여한다. runtime은 객체를 소유하거나 관리자/함수 소유자 역할을 상속하지 않는다. 기존 관리자 계정으로 계속 실행하면 이 제한은 적용되지 않는다.

| 역할 | 허용 범위 |
| --- | --- |
| Collector | 공급 상품·옵션·이벤트·상세 요청·점수·작업 읽기/저장, 관측 이력 추가. 삭제는 raw sample/점수 큐/공급사 작업에 한정. 인증 근거와 migration ledger는 읽기만 허용 |
| Spring | `read_supply_home`으로 완료 추천 조회, 작업/경고/공동 예산 상태 조회, 경고 acknowledged_at 갱신, 예산 예약/차단 함수 호출 |
| 함수 소유자 | 공동 예산 테이블 읽기/예약 갱신과 발행 결과 조회. 로그인·schema CREATE·상품 입력 변경 불가 |

공동 예산을 runtime의 직접 UPDATE로 열지 않는다. `reserve_api_call`과 차단만 가능한 `block_api_account`, 완료본 조회 `read_supply_home`을 제한된 소유자의 SECURITY DEFINER로 실행하고 `search_path=pg_catalog,<검토된 schema>,pg_temp`로 고정한다. PUBLIC 함수 실행과 관련 테이블/sequence 권한, schema CREATE를 회수한다. 향후 함수의 PUBLIC EXECUTE 기본값도 계획 실행 관리자에 대해 회수하므로 다른 migration 소유자로 실행하거나 함수를 교체하면 ACL/소유자/search_path를 재검토한다. 근거: [PostgreSQL 16의 SECURITY DEFINER 지침](https://www.postgresql.org/docs/16/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY).

네 기존 migration의 식별자/DDL은 유지하며, 새 `runtime_budget_guard`는 차단 함수를 추가하는 별도 명시적 migration이다. 권한 계획은 이 함수와 기존 네 migration이 있는 사본에서 생성한다. 신규 배포의 BudgetSession은 이 함수를 사용하므로 helper 적용 후 새 코드를 배포한다. 기존 schema의 수집 startup은 구조/인덱스 검사만 수행하고 DDL 복구는 관리자 migration으로 남긴다.

이번 범위는 공급 수집·점수·통합 운영이다. 쿠팡/네이버 조회와 저장 권한, tenant/account별 격리, 실제 Spring 프로세스 로그인 설정은 별도 전환 항목이다. 예산을 늘리거나 차단을 푸는 policy 변경은 관리자 작업이다. 재현·운영 전환은 [RUNBOOK](RUNBOOK.md#권한-분리-사본-검증과-배포), 검증 결과는 [VALIDATION](VALIDATION.md#db-권한-분리-검증)에 둔다.

## Migration과 백필

일반 수집은 기존 DB를 자동 변환하지 않는다. 빈 DB의 공통 초기화와 이후 기능별 migration을 구분한다. 기존 DB에서 SchemaCompatibilityError가 나면 컨테이너 재생성 대신 SQL 계획과 schema를 대조한다.

| 적용 순서 | 기존 migration 식별자 | 목적 | 구현 모듈 |
| --- | --- | --- | --- |
| 1 | `stage1_additive_columns` | 구형 공통 스키마의 추가 컬럼 | [migrations.py](../product_data_collector/persistence/migrations.py) |
| 2 | `stage2_storage_contract` | 최신값·옵션·이벤트·상세 요청 | [product_storage_schema.py](../product_data_collector/persistence/product_storage_schema.py) |
| 3 | `stage5_coupang_best` | 쿠팡 일일 베스트 | [coupang_best_schema.py](../product_data_collector/persistence/coupang_best_schema.py) |
| 4 | `stage6_naver_season` | 네이버 시즌·비교 | [naver_season_schema.py](../product_data_collector/persistence/naver_season_schema.py) |
| 5 | `stage7_supply_scoring` | 공급 점수·큐·추천 | [supply_scoring_schema.py](../product_data_collector/persistence/supply_scoring_schema.py) |
| 6 | `stage8_operations` | 통합 작업·공동 예산·입력 경계 | [operations_schema.py](../product_data_collector/persistence/operations_schema.py) |
| 7 | `stage7_history_policy_v2` | 90일 정책·관측 가격 준비 상태 보완 | [scoring_policy_schema.py](../product_data_collector/persistence/scoring_policy_schema.py) |
| 8 | `stage10_scoring_performance` | 점수 SQL·영향 전파 최적화 | [scoring_invalidation_schema.py](../product_data_collector/persistence/scoring_invalidation_schema.py) |
| 9 | `runtime_budget_guard` | runtime용 차단 함수 추가(권한 적용은 별도) | [access.py](../product_data_collector/persistence/access.py) |

필요한 기능의 선행 migration부터 적용한다. 구현 파일은 기능 이름으로 바꿨으며, 위 식별자와 적용 순서·DDL은 그대로 유지한다. 식별자는 `schema_migrations`에 저장된 값이므로 파일 이름에 맞춰 바꾸지 않는다. 기존 SQL 트리거 이름과 기본 백필 작업 이름도 저장 상태와 연결되므로 유지한다. 적용에는 명시적인 DSN이 필요하며 `.env`를 자동으로 읽지 않는다. 운영 사본의 계획·백업·lock 비용을 검토한 뒤 실행한다.

`stage7_supply_scoring`은 기존 상품을 참조하는 점수·큐·발행 테이블과 trigger를 추가하며, migration 자체가 기존 상품 전체의 점수를 채우거나 추천을 발행하지는 않는다. 기존 상품의 첫 전체 점수는 `initial` 실행이 필요하다. `stage8_operations`는 이 점수 스키마를 전제로 입력 보호·완료 revision과 공동 예산을 추가한다. 오너클랜만 전환할 때 쿠팡/네이버 migration은 별도 기능이며, 실제 전환 순서와 재개 계약은 [RUNBOOK](RUNBOOK.md#운영-전환-준비와-적용-순서)을 따른다.

2026-11-21 복원 사본에서 `stage2_storage_contract`의 필수 컬럼·인덱스 및 공통 schema 검증을 통과했다. 따라서 stage1 ledger 부재만으로 stage1을 재적용하지 않았다. 누락된 위 5~8 순서의 네 migration은 사본에만 적용·재실행 검증했고 운영 ledger는 그대로다. 기존 데이터 전체 보존·백필 대조 결과는 [VALIDATION](VALIDATION.md#운영-db-복원과-통합-실행-준비)에 둔다. `CREATE INDEX`는 concurrent 방식이 아니므로 운영 writer와 충돌할 수 있으며, 사본의 짧은 실행시간을 운영 lock 보장으로 사용하지 않는다.

**운영에는 네 migration 모두 아직 미적용**이다. 사본 검증 완료와 운영 적용 승인을 구분한다. 적용 직전 백업·복원 가능성, role/ACL·최소 권한, 잠금과 데이터/인덱스/WAL/임시 파일의 디스크 여유를 검토하고 최종 승인을 받는다. 전체 추천 배치·인덱스·보관 누적을 포함한 1,500만 상품 저장 용량은 미검증이며, 근거·남은 검증은 [VALIDATION](VALIDATION.md#1500만-규모와-운영-승인-전-남은-조건), 적용 gate는 [RUNBOOK](RUNBOOK.md#운영-전환-준비와-적용-순서)에 둔다.

구형 데이터는 [backfill.py](../product_data_collector/persistence/backfill.py)의 제한된 keyset batch로 옮긴다. 같은 이름으로 반복하여 재개하고, `--verify`의 next_id로 다음 범위를 대조한다. `--integrity-counts`는 명시적인 전체 스캔이다. 과거 history로 원본 사건을 생성하지 않는다. 실제 명령과 복구는 [RUNBOOK](RUNBOOK.md)에 둔다.
