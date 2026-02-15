from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from functools import lru_cache
import re
import time

from gradio_client import Client

#App
app = FastAPI(title="Steam Semantic Search API")


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


#HF Client
HF_SPACE = "Om-2003/steam-game-search-ui"
_client: Optional[Client] = None


def get_client() -> Client:
    """
    Lazy-load HF client. Prevents server crash at import time.
    Retries multiple times because HF DNS / sleep is common.
    """
    global _client
    if _client is not None:
        return _client

    last_err = None
    for _ in range(4):  # retries
        try:
            _client = Client(HF_SPACE)
            return _client
        except Exception as e:
            last_err = e
            time.sleep(1.5)

    raise RuntimeError("Failed to connect to Hugging Face Space") from last_err


#Models
class SearchRequest(BaseModel):
    query: str
    top_k: Optional[int] = 10


class GameResult(BaseModel):
    name: Optional[str] = None
    steam_url: Optional[str] = None
    similarity_score: Optional[float] = None
    avg_sentiment_score: Optional[float] = None
    primary_genre: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    header_image: Optional[str] = None
    background: Optional[str] = None
    screenshots: Optional[List[str]] = None
    short_description: Optional[str] = None
    description: Optional[str] = None
    match_reason: Optional[List[str]] = None


#Utils
def tokenize(text: str) -> List[str]:
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text.lower())
    return [w for w in text.split() if len(w) > 2]


def safe_float(v) -> Optional[float]:
    """
    HF sometimes returns:
      - numbers
      - None
      - "Missing"
      - "N/A"
    This prevents FastAPI ResponseValidationError.
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"missing", "na", "n/a", ""}:
            return None
        try:
            return float(v)
        except:
            return None
    return None


def safe_list(v) -> Optional[List[str]]:
    if v is None:
        return None
    if isinstance(v, list):
        return [str(x) for x in v if x]
    return None


def normalize_hf_output(raw: Any) -> List[Dict]:
    """
    Gradio sometimes returns:
      - list[dict]
      - dict with "data"
    """
    if raw is None:
        return []

    if isinstance(raw, list):
        return raw

    if isinstance(raw, dict) and "data" in raw:
        # sometimes raw["data"] = [results]
        d = raw.get("data")
        if isinstance(d, list) and len(d) > 0 and isinstance(d[0], list):
            return d[0]
        if isinstance(d, list):
            return d

    return []


#HF Search (Cached)
@lru_cache(maxsize=500)
def hf_search_cached(query: str, top_k: int) -> List[Dict]:
    """
    Cached call. Retries because HF spaces sleep.
    """
    client = get_client()

    last_err = None
    for attempt in range(4):
        try:
            raw = client.predict(
                query,
                top_k,
                api_name="/search_games"
            )
            return normalize_hf_output(raw)
        except Exception as e:
            last_err = e
            time.sleep(1.2 + attempt * 0.7)

    raise RuntimeError("HF Space failed after retries") from last_err


#API
@app.post("/search", response_model=List[GameResult])
def search_games(req: SearchRequest):
    query = (req.query or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    top_k = req.top_k or 10
    if top_k < 1:
        top_k = 1
    if top_k > 20:
        top_k = 20

    # HF CALL
    try:
        games = hf_search_cached(query, top_k)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Hugging Face backend unavailable (Space sleeping/down). Try again in 30–90 seconds."
        )

    # MATCH REASON
    query_tokens = tokenize(query)

    cleaned: List[Dict] = []
    for g in games:
        if not isinstance(g, dict):
            continue

        text = f"{g.get('name','')} {g.get('short_description','')} {g.get('description','')}".lower()
        reasons = [t for t in query_tokens if t in text][:6]

        cleaned.append({
            "name": g.get("name"),
            "steam_url": g.get("steam_url"),
            "similarity_score": safe_float(g.get("similarity_score")),
            "avg_sentiment_score": safe_float(g.get("avg_sentiment_score")),
            "primary_genre": g.get("primary_genre"),
            "price": safe_float(g.get("price")),
            "currency": g.get("currency"),
            "header_image": g.get("header_image"),
            "background": g.get("background"),
            "screenshots": safe_list(g.get("screenshots")),
            "short_description": g.get("short_description"),
            "description": g.get("description"),
            "match_reason": reasons
        })

    # sort by similarity desc
    cleaned.sort(key=lambda x: x.get("similarity_score") or 0, reverse=True)

    return cleaned[:top_k]


# Required for Vercel
app = app
