from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from agent import EcommerceAgent
import uvicorn

app = FastAPI()
agent = EcommerceAgent()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class Question(BaseModel):
    text: str
    stream: bool = False  # For future streaming support

@app.post("/ask")
async def ask_question(question: Question):
    response = agent.query(question.text)
    if "error" in response:
        raise HTTPException(
            status_code=400,
            detail={
                "error": response["error"],
                "suggestion": response.get("suggestion", "")
            }
        )
    return response

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)