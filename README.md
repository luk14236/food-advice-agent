# Food Advice App – Serverless API (AWS SAM)

This repo contains a serverless stack that:
- Generates **three world dishes** per request (`/answer`)
- Parses and classifies dishes (`/ask`)
- Runs **N** orchestrated simulations and writes to Postgres (`/simulate`)
- Reports **top dishes** and **vegetarian/vegan user counts** over the latest N rows (`/reports/stats`)

All infra is defined in `template.yaml` (AWS SAM). Code is Python 3.12.

---

## 🧱 Architecture

- **Lambdas**
  - `AnswerBot`: generates dish names (plain text, semicolon-separated)
  - `AskBot`: parses and classifies into JSON (name, possible_ingredients[], diet)
  - `Orchestrator`: runs multiple `AnswerBot` + `AskBot` calls and writes results to Postgres  
    - stores **3 rows per run** and **1 user_id (UUID)** per run
  - `Report (stats)`: reads the latest N rows and returns:
    - **top_3** most frequent dishes
    - **vegetarian_users_count** (users with at least one veg/vegan dish; `strictVeg=true` optionally requires all 3 veg/vegan)
- **API Gateway** with `x-api-key` auth
- **RDS (PostgreSQL)** for persistent storage
- **Secrets Manager** for DB credentials and connection info
- **VPC + SGs** for secure Lambda ↔ RDS connectivity

---

## 📦 Endpoints

Base URL (prod):  
```
https://<api-id>.execute-api.<region>.amazonaws.com/prod
```

> All requests require header: `x-api-key: <your-key>`

### `POST /answer`
Generate 3 dishes (plain text).
```json
{
  "question": "Tell me your three favorite foods."
}
```
Response (string):
```json
"Feijoada; Bibimbap; Sushi"
```

### `POST /ask`
Parse dishes to JSON (3 items).
```json
{
  "answer": "Feijoada; Bibimbap; Sushi"
}
```
Response:
```json
{
  "favorite_foods": [
    { "name": "Feijoada", "possible_ingredients": ["black beans","pork","garlic"], "diet": "normal" },
    { "name": "Bibimbap", "possible_ingredients": ["rice","egg","vegetables"], "diet": "normal" },
    { "name": "Sushi", "possible_ingredients": ["rice","fish","nori"], "diet": "normal" }
  ]
}
```

### `POST /simulate`
Run N iterations and persist results.
```json
{ "runs": 10 }
```
Response:
```json
{ "ok": true, "runs": 10, "inserted_rows": 30 }
```

### `GET /reports/stats?rows=N[&strictVeg=true]`
Look at the latest N rows and return top dishes + veg user count.
```
GET /reports/stats?rows=200
GET /reports/stats?rows=200&strictVeg=true
```
Response:
```json
{
  "rows_input": 200,
  "top_3": [
    { "name": "salad", "count": 12 },
    { "name": "pasta", "count": 10 },
    { "name": "sushi", "count": 8 }
  ],
  "vegetarian_users_count": 7
}
```

---

## 🗄️ Database

`favorite_foods` (simplified)
```sql
CREATE TABLE IF NOT EXISTS favorite_foods (
  favorite_food_id BIGSERIAL PRIMARY KEY,
  user_id UUID,
  name TEXT NOT NULL,
  possible_ingredients TEXT[] NOT NULL,
  diet TEXT NOT NULL CHECK (diet IN ('vegetarian','vegan','normal')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_favorite_foods_user_id ON favorite_foods (user_id);
```

- **Orchestrator** inserts **3 rows per run** with the same `user_id` (one per dish).

---

## 🧪 Quick Tests (curl)

```bash
# /answer
curl -s -X POST "$BASE/answer"   -H "x-api-key: $KEY" -H "Content-Type: application/json"   -d '{"question":"Tell me your three favorite foods."}'

# /ask
curl -s -X POST "$BASE/ask"   -H "x-api-key: $KEY" -H "Content-Type: application/json"   -d '{"answer":"Feijoada; Bibimbap; Sushi"}'

# /simulate
curl -s -X POST "$BASE/simulate"   -H "x-api-key: $KEY" -H "Content-Type: application/json"   -d '{"runs":10}'

# /reports/stats
curl -s "$BASE/reports/stats?rows=200"   -H "x-api-key: $KEY"
```

---

## 🧠 Notes

- Code targets **Python 3.12**.
- Orchestrator tolerates API Gateway events and direct invokes.
- For performance: Orchestrator batches inserts every 10 runs (30 rows).

---

