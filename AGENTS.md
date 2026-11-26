# 저장소 작업 안내

## 먼저 읽을 문서

`README.md`와 `docs/CONTEXT.md`를 읽고 작업 범위에 해당하는 문서만 추가로 읽는다. 현재 진척·다음 작업은 CONTEXT가 기준이며, `artifacts/`의 과거 기록을 현재 구현 상태로 간주하지 않는다.

외부 API의 호출 한도와 원본 근거는 [API_CONTRACTS](docs/API_CONTRACTS.md#외부-api-호출-한도)에서 확인한다. 최종 실측의 조건·한계와 근거 위치는 [VALIDATION](docs/VALIDATION.md)에 둔다.

## 구조와 변경 범위

- 공급처 패키지와 `python -m ...` 진입점은 유지한다.
- 공통 저장·도메인·운영 코드는 `product_data_collector/`에 둔다.
- 파일·폴더·설명은 기능과 책임으로 이름 짓고 `stageN` 같은 개발 단계 번호를 새 이름에 사용하지 않는다. 저장된 식별자와 기존 진입점의 호환 이름은 예외로 유지한다.
- 테스트는 `tests/unit/<영역>/`, PostgreSQL 통합 테스트는 `tests/integration/`에 둔다.
- 개발 실행 도구는 `scripts/`, 실제 API 조사 도구는 `scripts/probes/`, 합성 성능 측정은 `scripts/benchmarks/`에 둔다.
- 원시 응답·JUnit·로그·측정 결과는 `artifacts/`에 저장한다. 이 폴더는 Git에서 제외한다.
- 공유할 기획서·설계서·API 명세 등 참고 자료 원본은 `docs/references/`에서 Git으로 관리한다. 개인 첨부·비공개 자료는 저장소 밖에 보관한다. 참고 자료를 현재 구현 상태로 간주하지 않는다.
- 임시 분석·렌더링은 해당 `artifacts/experiments/<조사명>/scratch/`에서만 수행하고 종료 후 제거한다. `tmp/`나 단계별 보관 폴더를 추가하지 않는다. 검증 결과는 `artifacts/validation/`의 같은 이름으로 갱신한다.
- 일반 디버깅 상품 예시는 `artifacts/samples/`에 제공처별 최대 5개만 남긴다. 단위 테스트는 작은 fixture를 사용한다. 대량 원본은 실험 중 임시 사용하고, 종료 후 집계·소량 예시만 남긴다. 원본 보존이 필요하면 저장소 밖 백업으로 분리한다.
- 운영 DB migration 식별자와 checkpoint 파일 이름은 저장된 상태와 연결되므로 임의로 바꾸지 않는다.
- 실행 스크립트 경로·Compose command를 바꾸면 기존 컨테이너에 남은 명령도 확인한다. 단순 restart는 변경된 Compose command를 적용하지 않으므로 필요한 수집 컨테이너만 재생성한다.

## 검증

```text
python -m pytest -q
python -m pytest tests/integration -q
git diff --check
```

기본 pytest는 오프라인 단위 테스트만 수집한다. 통합 테스트는 명시적인 `COLLECTOR_TEST_POSTGRES_DSN`이 필요하며 `127.0.0.1`의 `collector_stage1_test`만 허용한다. 테스트 DB fixture는 무작위 schema만 생성·삭제한다. 세부 실행은 `docs/VALIDATION.md`를 따른다.

공급처 CLI의 `--dry-run` 중에는 실제 API를 호출하는 경로가 있다. 회귀 검증에는 pytest와 문서화된 오프라인 plan을 사용한다. 운영 DB·API를 검증용으로 임의 사용하지 않는다.

## 문서 유지

현재 진척·다음 작업은 CONTEXT, 구조·정책은 ARCHITECTURE/API_CONTRACTS/DATABASE, 실행은 RUNBOOK, 최신 검증 결과는 VALIDATION에 반영한다. 같은 내용을 여러 문서에 복사하지 말고 연결한다. 새로운 STAGE 보고서·날짜별 완료 보고서·중복 README는 만들지 않는다. 결과는 조건·핵심 수치·한계만 간단히 갱신한다.

## 작업 결과 전달

작업 결과는 채팅창에 상세 보고하지 않고 저장소 최상위 `temp.md`에 한국어 핵심 위주로 작성한다. 작성할 때마다 기존 내용을 전부 지우고 최신 작업 결과만 파일 전체에 덮어쓴다. 이전 결과에 이어 쓰거나 작업 이력을 누적하지 않는다. 채팅에는 완료 여부와 파일 위치만 간단히 알린다. `temp.md`는 `.gitignore`로 제외하고 커밋하지 않는다. 기존 기준 문서의 역할은 유지한다.
