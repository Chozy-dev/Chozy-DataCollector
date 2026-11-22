# Product Data Collector

Chozy 상품 수집·변경 이력·공급 점수·시장 스냅샷을 관리하는 Python 프로젝트다. PostgreSQL에 저장한 완료 결과를 Spring Platform이 조회한다.

**LLM에게 프로젝트를 설명할 때는 [docs/CONTEXT.md](docs/CONTEXT.md) 하나만 먼저 전달한다.** 현재 구현, 미완료 기능, 다음 작업, 코드 위치가 들어 있다. 이후 작업에 필요한 코드만 추가하면 된다.

## 시작

Python 3.12와 PostgreSQL 16을 사용한다. 저장소 루트에서 실행한다.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
python -m pytest -q
```

`.env`에 로컬 자격증명을 설정한다. 통합 실행기·migration은 `.env`를 자동으로 읽지 않으며, 실제 실행에는 별도로 지정한 DSN 환경변수가 필요하다.

외부 API·DB 연결 없이 통합 실행 계획 확인:

```powershell
python -m product_data_collector.operations plan --config product_data_collector/operations/example.yaml
```

실제 수집·DB 초기화·migration·복구는 [운영 절차](docs/RUNBOOK.md), 격리 DB 테스트는 [검증](docs/VALIDATION.md)을 따른다.

`docker compose up -d`는 PostgreSQL만 시작한다. 수집 컨테이너를 켜도 실제 API 수집은 시작하지 않으며, 수집은 운영 절차의 명시적 명령으로 실행한다.

## 폴더와 문서

```text
product_data_collector/   공통 도메인·PostgreSQL 저장·점수·통합 운영
ownerclan_API/           오너클랜 수집
domeggook_API/           도매꾹·도매매 수집
coupang_API/             쿠팡 베스트·기존 검색 수집
naver_API/               네이버 시즌 키워드·트렌드
scripts/                개발 실행·DB 확인·내보내기
  probes/               실제 API 조사 도구
  benchmarks/           격리 환경의 합성 성능 측정
tests/
  unit/                 영역별 오프라인 테스트
  integration/          격리 PostgreSQL 테스트
docs/                   현재 기준 문서와 참고 자료
  references/           전달받은 기획서·설계서·PDF·API 명세 원본
artifacts/              실행·조사·검증 산출물, Git 제외
  experiments/          조사별 집계·실측 근거·성능 비교 baseline
    collection-pipeline-benchmark/  수집·저장·점수 파이프라인 성능
    scoring-optimization-benchmark/ 점수 최적화 전후 비교
  samples/              응답 구조 확인용 예시, 제공처별 최대 5개
  validation/           최신 JUnit·migration 계획·검증 캐시
```

공급처의 `data/`와 기존 `logs/`는 실행 중 사용하는 저장 결과·재개 상태·로그·잠금 경로이며 Git에서 제외한다. 정리 대상으로 일괄 삭제하지 않는다. `scripts/`와 테스트 fixture 소스는 Git으로 관리한다.

`docs/` 최상위에는 현재 기준 문서 6개를 둔다. 외부 참고 자료 원본은 `docs/references/`에서 Git으로 관리한다. 참고 자료의 과거 설계·수치는 현재 구현 상태로 간주하지 않으며, 현재 상태는 CONTEXT를 따른다.

| 문서 | 내용 |
| --- | --- |
| [CONTEXT](docs/CONTEXT.md) | 현재 구현·미완료 기능·다음 작업·코드 위치 |
| [ARCHITECTURE](docs/ARCHITECTURE.md) | 코드 책임·데이터 흐름·점수 산식·정합성 |
| [API_CONTRACTS](docs/API_CONTRACTS.md) | 필드 매핑·응답 제약·미확정 API 계약 |
| [DATABASE](docs/DATABASE.md) | 테이블·조회·migration·백필 |
| [RUNBOOK](docs/RUNBOOK.md) | 설정·실행·중단·재개·배포 |
| [VALIDATION](docs/VALIDATION.md) | 검증 명령·최신 결과·실측 조건과 한계 |

저장소 작업 규칙은 [AGENTS.md](AGENTS.md)를 따른다. 처음에는 README → CONTEXT → 해당 작업의 기준 문서 순서로 읽는다.

파일과 폴더는 기능 이름을 사용한다. schema 모듈은 상품 저장·쿠팡 베스트·네이버 시즌·공급 점수·통합 운영·점수 무효화로 구분하며 [DATABASE](docs/DATABASE.md#migration과-백필)에서 실제 파일을 찾는다. 도매매 전체 적재·변경 동기화는 `workflows/product_sync.py`, CLI는 `workflows/sync_cli.py`다. DB에 기록된 migration 식별자와 기존 재개 경로는 유지한다.

## 로컬 자료 관리

상품 예시는 필드 형식·옵션·판매 상태를 확인하기 위한 자료다. 정상·품절·판매 종료·옵션 등 서로 다른 사례를 제공처별 최대 5개만 남긴다. 단위 테스트는 코드 안의 작은 fixture를 사용하므로 대량 실상품 표본을 보관할 필요가 없다.

실험은 `artifacts/experiments/<조사명>/`에 모으고 임시 변환본은 그 안의 `scratch/`에서 작업 후 제거한다. 종료 후 결론과 한계는 VALIDATION에 반영하고 집계·고유 근거만 남긴다. 집계 JSON에 포함된 개별 결과, 이전 JUnit, PDF 추출본·렌더링은 누적 보관하지 않는다. 재현에 필요한 원본과 baseline은 보존하며, 과거 기록의 별도 복구가 필요하면 VALIDATION의 외부 백업 위치를 확인한다.

기능·정책·진척은 해당 기준 문서를 갱신한다. 단계별 완료 보고서나 중복 README, `tmp/`·`archive/` 보관 폴더를 추가하지 않는다.
