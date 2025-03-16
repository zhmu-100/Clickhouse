
# ClickHouse Service

This project is a standalone microservice built in Kotlin that provides a simple API for data management with ClickHouse.

## Environment Setup

Create an `.env` file in the project root with the following keys:

```
CLICKHOUSE_URL=jdbc:clickhouse://localhost:8123/test_db
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

API_HOST=0.0.0.0
API_PORT=8080
```

## Running the Service

1. Ensure ClickHouse is running.
2. Create and configure the `.env` file as shown above.
3. Start the service using:
   ```bash
   ./gradlew run
   ```
The service listens on the port specified in the `.env` file (default **8080**).

## Testing

Clean build with:

```bash
./gradlew clean
```

Run tests with:

```bash
./gradlew test
```

Generate test coverage report with:

```bash
./gradlew jacocoTestReport
```

## Endpoints (Port: 8080)

- **POST /insert**  
  Performs a bulk insert operation into a specified table.

  **Example JSON:**
  ```json
  {
    "table": "table1",
    "data": [
      {
        "id": 1,
        "name": "Alice",
        "age": 30
      },
      {
        "id": 2,
        "name": "Bob",
        "age": 25
      }
    ]
  }
  ```

- **POST /query**  
  Executes a raw SQL SELECT query.

  **Example JSON:**
  ```json
  {
    "query": "SELECT * FROM table1"
  }
  ```

