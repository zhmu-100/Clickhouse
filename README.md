# DB ORM Service

This project is a standalone microservice built in Kotlin that provides a simple ORM layer for PostgreSQL.

## Environment Setup

Create an `.env` file in the project root with these keys:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=yourpassword
```

## Running the Service

1. Ensure PostgreSQL is running.
2. Create and configure the `.env` file.
3. Start the service:
   ```bash
   ./gradlew run
   ```
   The service listens on port **8080**.

## Testing

Clean build with:

```bash
./gradlew clean
```

Run tests with:

```bash
./gradlew test
```

Create report with:

```bash
./gradlew jacocoTestReport
```

## Endpoints (Port: 8080)

- **POST /create**  
  Inserts a new record.  
  **Example JSON:**
  ```json
  {
    "table": "users",
    "data": {
      "name": "John Doe",
      "email": "john.doe@example.com"
    }
  }
  ```

- **POST /read**  
  Executes a raw SQL SELECT query.  
  **Example JSON:**
  ```json
  {
    "query": "SELECT * FROM users;",
    "params": []
  }
  ```

- **PUT /update**  
  Updates records.  
  **Example JSON:**
  ```json
  {
    "table": "users",
    "data": { "email": "new.email@example.com" },
    "condition": "id = ?",
    "conditionParams": ["1"]
  }
  ```

- **DELETE /delete**  
  Deletes records.  
  **Example JSON:**
  ```json
  {
    "table": "users",
    "condition": "id = ?",
    "conditionParams": ["1"]
  }
  ```

