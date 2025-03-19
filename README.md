# ClickHouse Service

This project is a standalone microservice built in Kotlin that provides a simple API for data management with
ClickHouse. It supports common CRUD operations—insert, select, update, and delete—using a secure, parameterized API.

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

- **POST /select**  
  Retrieves data from a specified table with optional filtering, ordering, limit, and offset parameters.

  **Example JSON:**
  ```json
  {
    "table": "table1",
    "columns": ["id", "name", "age"],
    "filters": {
      "age": 30
    },
    "orderBy": "id ASC",
    "limit": 10,
    "offset": 0
  }
  ```

- **PUT /update**  
  Updates data in a specified table.  
  **Note:** ClickHouse does not allow updating key columns (e.g., columns that are part of the primary key or ORDER BY
  clause). Make sure to update only non-key columns.

  **Example JSON:**
  ```json
  {
    "table": "table1",
    "data": {
      "age": 35
    },
    "condition": "id = ?",
    "conditionParams": ["1"]
  }
  ```

- **DELETE /delete**  
  Deletes data from a specified table.  
  **Note:** ClickHouse does not allow deleting key columns. Ensure that your condition targets non-key columns.

  **Example JSON:**
  ```json
  {
    "table": "table1",
    "condition": "id = ?",
    "conditionParams": ["2"]
  }
  ```

