from fastapi import APIRouter

router = APIRouter(prefix="/api/ktoa2", tags=["ktoa2"])

@router.get("/health")
def health():
    return {"ok": True, "message": "ktoa2 patch loaded"}