# KT Dashboard v4 배포 방법 (Windows)

## 핵심 원인
운영 URL이 ⑩ 시장정보까지만 보이면 최신 코드가 Railway에 반영되지 않은 상태입니다. GitHub에는 ⑪ 월별추이/⑫ 소매 매장실적이 있어도 Railway 최신 배포가 실패하면 이전 성공 배포가 계속 서비스됩니다.

## 적용 순서
1. 이 ZIP을 풀고 `kt-dashboard-v4-deploy-safe` 폴더 안의 파일을 GitHub 로컬 저장소 루트에 덮어씁니다.
2. PowerShell에서 저장소 폴더로 이동합니다.
   ```powershell
   cd C:\path\to\kt-dashboard
   ```
3. 변경 파일 확인:
   ```powershell
   git status
   ```
4. Python 문법 확인:
   ```powershell
   python -m py_compile app/main.py
   ```
5. 커밋/푸시:
   ```powershell
   git add .
   git commit -m "fix: add monthly trend and retail store dashboard deploy-safe"
   git push origin main
   ```
6. Railway에서 최신 deployment가 `Success`인지 확인합니다.
7. 배포 후 아래 주소가 열려야 합니다.
   ```text
   https://kt-dashboard-production.up.railway.app/api/health
   ```
   정상 예: `status: ok`, `version: v4-retail-monthly-deploy-safe`

## 중요
- v4는 Railway 시작 실패를 줄이기 위해 대용량 엑셀 자동 적재를 기본 OFF로 둡니다.
- 배포 후 화면 상단의 `판매`, `소매` 버튼으로 엑셀을 업로드하세요.
- 반드시 파일을 붙여넣기로 만들지 말고, ZIP 파일을 풀어서 실제 파일을 덮어씌우세요. Python은 들여쓰기와 줄바꿈이 깨지면 Railway 배포가 실패합니다.
