# v4-retail-monthly-deploy-safe

## 변경 사항
- ⑪ 월별추이 탭 유지 및 안정화
- ⑫ 소매 매장실적 탭 유지 및 안정화
- `/api/store-sales`, `/api/monthly-trend`, `/api/health`, `/api/version` 제공
- 탭 전환 id 기반 유지
- 대용량 엑셀 자동 적재 기본 OFF: `AUTOLOAD_EXCEL=0`
- Railway 시작 실패 가능성 축소

## 배포 확인
- 운영 화면 상단 버전 배지: `v4-retail-monthly-deploy-safe`
- API 확인: `/api/health`
