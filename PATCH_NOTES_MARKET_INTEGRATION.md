# Market Automation Dashboard Integration

## 포함 내용

- `app/market_api_patch.py`
  - `/api/market2/health`
  - `/api/market2/reports`
  - `/api/market2/rebate-status`
  - `/api/market2/competition`
  - `/api/market2/timeline`
  - `/api/market2/summary`
- `app/main.py`
  - market2 router import/include 연결
- `app/templates/index.html`
  - 기존 ⑩ 시장정보 탭 상단에 `시장정보 자동화 대시보드` 자동 삽입
  - 단가 리포트, 리베이트 현황, 경쟁 격차, 정책 타임라인 렌더링
- `market_automation.db`, `app/market_automation.db`
  - 업로드된 `market_automation.zip`의 DB 반영

## 배포 후 확인 URL

- `/api/market2/health`
- `/api/market2/reports?limit=3`
- `/api/market2/rebate-status?limit=3`
- `/api/market2/competition`
- `/api/market2/timeline?limit=3`

정상 기준: 각 API가 `{ "items": [...] }` 형태로 응답하고, 대시보드 ⑩ 시장정보 탭 상단에 `시장정보 자동화 대시보드`가 표시되어야 합니다.
