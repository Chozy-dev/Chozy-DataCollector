# 공급처 API와 정규화 계약

아래는 현재 코드와 보존된 실측의 해석 기준이다. 호출 예산은 운영 설정과 제공처 승인 범위로 정하며, 소규모 성공을 전체 계정의 처리량 보장으로 해석하지 않는다. 수치 요약은 [VALIDATION](VALIDATION.md)에 둔다.

## 오너클랜

Seller GraphQL API를 사용한다. 쿼리는 [api/queries.py](../ownerclan_API/api/queries.py), 정규화는 [services/normalization.py](../ownerclan_API/services/normalization.py), 일일 이력·상세 worker는 [workflows/history_sync.py](../ownerclan_API/workflows/history_sync.py)에 있다.

| 원본 | 저장 의미 |
| --- | --- |
| `key`, `productId` | 공통 external_product_id |
| 상품명·images·URL | 상품 표시 정보. URL 부재 시 selfcode 기반 주소 |
| `price`, `fixedPrice`, 옵션 가격 | 기본 가격과 옵션 최종가격을 구분하여 정규화 |
| 옵션 `status`, `quantity` | 판매 상태와 수량을 함께 판정 |
| `shippingFee`와 조건식 | 단일 숫자로 해석 가능할 때만 금액, 원문 조건은 보존 |
| `metadata.vendorKey` | 공급사 후보 식별자. 값의 의미와 카탈로그 인증은 별도 |
| `guaranteedShippingPeriod` | 발송 점수 입력. 1일 미만·누락은 결측 |
| `openmarketSellable`, 반품·인증·고시·attributes | source_specific 보조 정보 |

`itemHistories`는 고정 날짜 구간과 cursor로 끝까지 읽는다. `hasNextPage=false`만 종료 근거로 삼는다. 빈 non-terminal·반복 cursor·잘못된 node/종류/시각은 실패다. 구간은 7일 이하로 분할하고 인접 경계를 겹친다. daily는 pendingHistory와 마지막 성공 watermark를 보존한다. 보존 기간 거부는 gap으로 남기며 무변경으로 바꾸지 않는다.

원본 종류·전후값·밀리초 시각·원문을 보존한다. 안정 event ID가 없어 같은 내용의 별개 사건이 fingerprint로 합쳐질 수 있다. cursor는 사건 ID가 아니다. 이벤트에 optionKey가 없을 수 있어 옵션 title/reference를 unresolved로 남기며 현재 옵션과 자동 연결하지 않는다. 수량 변경 신호는 현재 상세 재조회를 요청한다.

**현재 묶음 상세 구현:** `itemsByKeys`는 실측한 최대 100개를 POST JSON으로 요청한다. connect timeout은 `min(5, request.timeout_seconds)`, read timeout은 `max(45, request.timeout_seconds)`초다. 요청/반환 key를 대조하고 미반환만 1초 뒤 한 번 보완한다. 이후 누락은 기존 DB backoff로 남긴다. 중복·예상 밖 key와 잘못된 응답은 완료로 처리하지 않는다. 부분 성공은 상품별로 commit하고 각 lease·버전·실패를 따로 관리한다. 전체 필드가 제공처에서 거부되면 최소 필드로 한 번 전환하며, HTTP 403/429·GraphQL 호출 제한은 새 요청을 중단한다. 상세는 현재 응답 관측값이며 과거 사건 시점으로 소급하지 않는다.

### 공식 호출 제한 안내의 범위

2026-11-21 확인한 [공식 상품 API 안내](https://www.ownerclan.com/V2/service/api-center-item.php)는 `allItems` 1회 최대 1,000개, `itemsByKeys` 최대 5,000개를 안내한다. API 센터가 연결하는 [공식 매뉴얼 ZIP](https://cdn.ownerclan.com/API%20%EB%A7%A4%EB%89%B4%EC%96%BC.zip)의 Seller PDF는 **2020-10-21 / v0.11.0**이다. PDF p.73에는 **단일 `item` 조회 설명 안에서 초당 1,000건**이라는 오래된 문구가 있으나, 이를 현재 Seller GraphQL 전체의 승인 한도로 사용하지 않는다. p.3에는 `allItems`의 `search`, `priceAsc/priceDesc`에 강화된 Quota가 적용된다고만 적혀 있다.

확인한 공개 문서에서는 **현재 전체 계정에 적용되는 분·시간·일 단위 숫자, 계정/IP/token별 합산 기준, 쿼리별 비용, reset 시각, 403/429 차단 지속시간을 확정하지 못했다.** 미기재를 무제한으로 해석하지 않는다. RPM을 시간·일로 곱한 값은 설정상 산술 용량이지 제공처 쿼터가 아니다. JWT 재발급이 예산을 초기화한다고 가정하지 않는다.

5,000개 안내와 달리 11월 20일 production은 1,000개 요청에 최대 100개 오류를 반환했다. 11월 21일에는 101개 요청이 HTTP 200/오류 없음/**1행 반환**이어서 같은 명시적 거절을 재현하지 못했다. 현재 서버의 정확한 hard cap을 새로 확정한 결과가 아니며, 업무 요청 상한은 기존 100개를 유지한다. HTTP 상태와 요청 개수만으로 완료를 판단하지 않는다. 최신 실측은 [VALIDATION](VALIDATION.md#오너클랜-호출-정책과-하루-1만-상품-조사), 재개 절차는 [RUNBOOK](RUNBOOK.md#오너클랜-호출-속도와-오류-후-재개)를 따른다.

## 도매꾹과 도매매

Open API 목록·일반 상세, 도매매 채널 상세, Private 변경 검색을 구분한다. 파서는 [services/parsing.py](../domeggook_API/services/parsing.py), Private client는 [api/private_sync.py](../domeggook_API/api/private_sync.py), 별도 수집 경로는 [workflows/product_sync.py](../domeggook_API/workflows/product_sync.py)다.

| 원본 | 저장 의미 |
| --- | --- |
| `basis.no`, `basis.title`, `basis.status` | 상품 ID·이름·상태 |
| `price.dome`, `price.supply` | 도매꾹·도매매 공급가를 별도 보존 |
| `price.labeledPrice.low/recommend` | 최소·권장 소매가 |
| `price.resale.minimum/minumum`, `Recommand` | 원본 alias를 보존한 재판매 가격 |
| `selectOpt.data`의 `domPrice`, `supPrice` | 옵션 추가금. 해당 시장 기본가와 가격 계약을 확인해 최종가 계산 |
| 옵션 `hid`, `sup`, `qty` | 노출·판매 여부와 수량을 함께 판정. 수량만으로 판매 가능 판정 금지 |
| `qty.inventory` | 재고 수량과 9999 sentinel 구분 |
| `seller.score.avg/cnt`, `good`, `type` | 만족도·후기·우수·판매자 유형 |
| `seller.vacation`, 발송 관련 필드 | 휴가·평균 발송·빠른배송을 구분 |
| `deli.dome`, `deli.supply` | 시장별 배송비·유형·조건표를 각각 보존 |

도매매의 옵션 공급가는 검증된 고정단가와 supPrice 계약을 사용한다. 불확실한 조합·옵션 선택이 필요한 대표가는 NULL로 남긴다. 도매꾹 옵션 가격을 도매매 최종 공급가로 대신 쓰지 않는다. 옵션 재정렬·삭제·재생성 전후의 identity는 별도 검증 대상이다.

일반 상세의 판매자 만족도가 ES 상세에는 없을 수 있어 ES 하나로 모든 점수 입력을 대체할 수 없다. 배송 조건표·수량별 비례·착불·무료를 숫자 하나로 축약하지 않는다. 도매매 배송이 없으면 도매꾹 배송으로 임의 채우지 않는다.

### Private 변경 검색

승인 대기 상태다. 문서 기반 구현은 Bearer 인증, 최대 size 100, dateModify 범위와 opaque searchAfter 배열을 사용한다. **성공 실응답·시간대·종료 규칙·지연·보존 기간·삭제/채널 이탈 포착·실제 제한은 미확정**이다. getAllSupplyChk를 같은 기능의 대체로 간주하지 않는다.

2026-11-21 재호출도 HTTP 200 안에 `result=false`, 오류 `401/UNAUTHORIZED`, `승인되지 않은 IP 입니다`를 반환했다. HTTP 성공만으로 권한이나 데이터 조회 성공을 판단하지 않는다. 최신 조건과 근거는 [VALIDATION](VALIDATION.md#도매매-private-동기화-재호출)을 따른다. 과거 `getItemViewES` 묶음 조회의 호출 제한 조사는 이 검색 API의 승인·한도를 입증하지 않는다.

dateModify는 raw에 남기되 사건 발생시각으로 만들지 않는다. 결과는 내부 `modification_signal`, occurred_at=NULL, ambiguous identity로 저장한다. 원본 전후값·실제 사건 종류를 추정하지 않는다. `channel.supply=true`는 현재 채널에서 이탈한 상품을 놓칠 수 있어 승인 후 복구 정책을 검토한다.

기존 Open API의 full/detail-first daily도 보존되어 있다. 새 Private 경로와 기존 경로의 실행 차이는 [RUNBOOK](RUNBOOK.md)에 정리한다.

## 쿠팡

쿠팡 수집 CLI는 `bestcategories`로 설정된 카테고리의 베스트 상품을 수집한다. `products/search`는 Spring 정밀분석에서 시장가 표본을 조회한다. 베스트 파서는 [services/best.py](../coupang_API/services/best.py), 일일 수집은 [workflows/best.py](../coupang_API/workflows/best.py)다.

| 원본·요청 | 저장 의미 |
| --- | --- |
| 요청 categoryId, 기준일 | 응답 상품의 카테고리·일별 맥락 |
| `productId`, `productName`, `productUrl`, `productImage` | 상품 표시 정보 |
| `productPrice` | 베스트 응답의 노출 가격 |
| `rank`, `isRocket` | 응답 순위와 로켓배송 |
| 로컬 전일 스냅샷 | previousRank·rankChange·rankStatus 계산 |

HTTP 200이어도 `rCode/rMessage` 제한을 검사한다. limit=100 요청의 81개 반환을 관측했으므로 요청 limit만으로 응답 완전성을 확정하지 않는다. 동일 상품 ID가 서로 다른 순위로 반복되면 가장 높은 원본 순위의 행을 남기고 순위는 재부여하지 않는다. 순위 값 자체가 중복된 응답은 실패 처리한다. 홈 카테고리 이름은 요청 ID에 매핑한 설정값을 사용한다. 같은 날짜의 카테고리 범위는 고정하며 모든 대상의 저장·검증이 끝나야 발행한다. 전일 완료본이 없으면 순위 변화를 0으로 만들지 않는다.

2026-09-02에는 검색 제한 후 best도 `rCode=403`을 반환했다는 과거 기록이 있다. 계정 예산은 두 경로를 함께 계산하며, 이 관측을 현재 계정의 고정 호출 한도로 해석하지 않는다. 당시 보고서의 수치와 원본 복구 위치는 [VALIDATION](VALIDATION.md)을 따른다.

## 네이버

검색광고 RelKwdStat과 API HUB 트렌드는 별도 인증을 사용한다. [client.py](../naver_API/client.py), [services.py](../naver_API/services.py), [collector.py](../naver_API/collector.py)가 담당한다.

- 월마다 검토한 서로 다른 event 코드 3개와 테마명을 설정한다. 요청 월·기준일·실제 observed_at은 서로 다른 값이다.
- 검색수는 정수 또는 `"< 10"` 문자열이다. 공백을 정규화하고 소량값을 0이나 임의 정수로 바꾸지 않는다. PC/mobile 유효값으로 상위 10개를 선정한다.
- 트렌드는 5그룹씩 2회 요청하며 기준일 D−56~D−1의 일간 지수에서 직전 28일 대비 최근 28일 평균 상승률을 계산한다.
- ratio는 요청 그룹·기간의 상대지수다. 절대 검색량으로 취급하거나 다른 요청의 스케일을 섞지 않는다. 누락·중복·이전 평균 0은 상태와 NULL을 남긴다.
- 쿠팡 비교는 동일 기준일의 완료된 수집 카테고리만 대상으로 한다. 비교본 부재는 `coupang_unavailable`이며 미일치 0건과 구분한다.

인증 변수와 재개 명령은 [RUNBOOK](RUNBOOK.md), PostgreSQL 조회 함수는 [DATABASE](DATABASE.md)를 따른다.
