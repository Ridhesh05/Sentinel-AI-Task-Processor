# check api is running 
from fastapi import FastAPI
app = FastAPI(title="Sentinel AI API")
@app.get("/health")
def health():
    return {"status": "ok"}