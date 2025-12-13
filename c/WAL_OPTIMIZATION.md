# FlintDB C WAL 최적화 정리

이 문서는 `wal.c`에서 수행한 WAL(Write-Ahead Logging) 쓰기 경로 최적화의 **핵심 아이디어/구조 변경/튜닝 포인트**를 정리합니다.

## 목표

- WAL 로깅 처리량(throughput) 개선
- 트랜잭션 UPDATE/DELETE에서 page image(페이지 스냅샷) 로깅 시 발생하는 **불필요한 memcpy/할당/seek** 비용 제거
- 파일 디스크립터의 seek position을 건드리지 않는 **offset 기반 기록**으로 동작을 단순화

## 기존 병목(요약)

### 1) 배치 flush의 `lseek + write`
- 배치 버퍼를 디스크에 밀어 넣을 때 `lseek(fd, pos)` 후 `write(fd, buf, n)` 패턴은 시스템 콜 2회 + 커널 상태 변경이 발생
- 잦은 flush에서 누적 비용이 커짐

### 2) 큰 레코드의 “배치버퍼 복사”
- 큰 payload(페이지 이미지)를 배치 버퍼로 `memcpy` 해놓고 나중에 flush 하는 방식은
  - payload 크기만큼 추가 memcpy 비용 발생
  - 배치 버퍼 용량을 빠르게 소진 → flush 빈도 증가

### 3) dirty page 캐시의 다중 할당 + 누수 리스크
- UPDATE 시 dirty page를 저장하기 위해
  - `dirty_page` 구조체
  - 별도 `data_copy` 배열
  - 별도 `struct buffer` 객체
  를 각각 할당하던 구조
- `buffer_wrap`으로 만든 버퍼의 `free`가 no-op인 경우(빌려쓴 버퍼 개념), `data_copy`가 해제되지 않는 형태로 누수 가능성이 존재
- 동일 offset을 한 트랜잭션 내에서 여러 번 UPDATE/DELETE 했을 때 기존 엔트리 처리도 불명확

## 개선 사항

## 1) 배치 flush/header flush를 `pwrite` 기반으로 변경

- 배치 flush: `wal_flush_batch()`에서 `pwrite(fd, batch, size, current_position)` 형태로 변경
- 헤더 flush: `wal_flush_header()`에서도 동일하게 `pwrite(fd, header, HEADER_SIZE, 0)` 사용

효과:
- flush 시 `lseek` 제거 → syscall/상태변경 비용 감소
- offset 기반 I/O로 쓰기 흐름이 명확해짐

## 2) 큰 레코드는 배치를 우회하는 “direct-write” 경로 추가

- `record_size >= direct_write_threshold` 인 경우:
  - 먼저 배치 버퍼를 flush(동기화 없이)
  - 레코드를 배치 버퍼에 복사하지 않고
  - 헤더(28바이트) + (압축 크기 4바이트) + payload를 바로 파일에 기록

핵심 포인트:
- payload를 배치 버퍼로 옮기는 memcpy가 사라짐
- 큰 레코드 때문에 배치가 자주 깨지는 현상을 완화

### 설정(환경변수)
- `FLINTDB_WAL_DIRECT_WRITE_THRESHOLD`
  - 기본값: 64KB
  - 배치 버퍼(`wal_buffer_size`)보다 크면 자동으로 배치 버퍼 크기로 클램프

## 3) direct-write에서 `lseek`까지 제거 (완전 offset-based)

- POSIX(macOS/Linux): `wal_pwritev_all()`을 통해 `iovec` 조각들을 순차 `pwrite`로 기록
- Windows:
  - `EMULATE_PREAD_PWRITE_WIN32` 사용 시 `pwrite()`
  - 아니면 `pwrite_win32(_get_osfhandle(fd), ...)`
  를 사용하도록 `wal_pwrite_all()` 구현을 정리

효과:
- direct-write 경로에서 fd seek position이 절대 변하지 않음
- multi-thread/다른 코드와 섞일 때도 안전한 offset 기반 기록 유지

## 4) dirty page 캐시 구조 개선 (1회 할당 + 1회 복사)

UPDATE 트랜잭션에서 dirty page 저장을 아래처럼 변경:

- `struct dirty_page`를 flexible array 멤버로 구성
  - `dirty_page + inline data[]`를 한 번에 할당
  - 그 data에 payload를 1회 memcpy
  - `struct buffer buf`는 dirty_page 내부에 내장(별도 heap 할당 제거)

그리고 WAL 로깅도 입력 버퍼를 직접 쓰지 않고, **dirty page에 저장된 안정적인 복사본**을 사용:
- `wal_log(..., page->data, data_size, ...)`

효과:
- UPDATE 당 heap 할당 횟수 감소(3회 → 1회)
- payload memcpy 횟수 감소(상황에 따라 2회 경로가 생기던 것을 1회로 고정)
- 큰 레코드 direct-write와 결합 시 “추가 memcpy 없이” 기록 가능

### overwrite 처리
- 같은 offset이 트랜잭션 내에서 여러 번 UPDATE/DELETE 되는 경우:
  - 기존 dirty entry를 먼저 해제 후 새 엔트리로 교체
  - 메모리 사용량이 누적되지 않도록 보장

### free 정책
- `dirty_page`는 단일 블록 할당이므로 최종 정리는 `FREE(page)` 한 번
- dirty page의 `buf.free`는 no-op으로 설정하여, read 경로에서 호출자가 `free`를 부르더라도 안전

## 튜닝 가이드

### 추천 시작값
- `wal_buffer_size`: 4MB 이상 (기본값 유지 권장)
- `FLINTDB_WAL_DIRECT_WRITE_THRESHOLD`: 64KB ~ 256KB 범위에서 워크로드에 맞춰 조정
  - 페이지 이미지가 크고 자주 발생하면 threshold를 낮춰 direct-write 빈도를 늘리는 것이 유리
  - 작은 레코드가 대부분이면 threshold를 높여 배치 효율을 우선

### 체크 포인트
- `wal_page_data=1`에서 UPDATE/DELETE가 많을수록 direct-write + dirty page 단일할당 개선이 효과적
- `wal_page_data=0`(metadata only) 모드에서는 WAL I/O 자체가 줄어들어 병목 위치가 달라질 수 있음

## 관련 코드

- 핵심 변경 파일: [c/src/wal.c](../c/src/wal.c)
- Windows I/O 레이어: [c/src/runtime_win32.c](../c/src/runtime_win32.c), [c/src/runtime_win32.h](../c/src/runtime_win32.h)

## 주의사항(현재 구현 기준)

- `wal_log`는 에러 처리/부분 기록에 대한 강한 보장을 목표로 하지는 않고, 처리량 중심(best-effort) 설계입니다.
- 레코드 checksum은 현재 placeholder 값이며, 무결성 검증을 강화하려면 checksum 계산/검증과 recovery 로직이 필요합니다.
