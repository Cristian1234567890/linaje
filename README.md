
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8001
python -m http.server 8000
