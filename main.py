from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello from Reflexity Backend!"}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/api/test")
async def test():
    return {"data": "API is working!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)