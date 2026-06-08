from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from producer import send_sync_event

app = FastAPI(title="BI-AI Link Data Lake API", version="1.0.0")

class DataSyncPayload(BaseModel):
    source: str
    target: str
    entity_id: str
    data: dict

@app.post("/sync")
async def trigger_sync(payload: DataSyncPayload):
    try:
        # Push to Kafka topic for synchronization
        send_sync_event(payload.model_dump())
        return {"status": "success", "message": "Sync event queued successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "healthy"}
