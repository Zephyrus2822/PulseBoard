# Data Dashboard Backend

FastAPI + PostgreSQL backend for automated dashboard generation.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up PostgreSQL database

3. Update `.env` file with your credentials

4. Run the server:
```bash
uvicorn server.main:app --reload --port 8000
```

## API Endpoints

- POST `/api/upload` - Upload data file
- GET `/api/analyze/{file_id}` - Get analysis status
- GET `/api/dashboard/{file_id}` - Get dashboard configuration
- POST `/api/chat` - Chatbot endpoint
