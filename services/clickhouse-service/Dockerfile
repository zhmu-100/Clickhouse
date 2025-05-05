FROM openjdk:17-slim

ARG SERVICE_NAME=clickhouse-service
ARG SERVICE_VERSION=latest
ARG JAR_FILE=./clickhouse-service.jar

# Set service-specific environment variables
ENV SERVICE_NAME=${SERVICE_NAME}
ENV SERVICE_VERSION=${SERVICE_VERSION}

# Copy the service JAR
COPY ${JAR_FILE} /app/service.jar

# Clickhouse-service-specific configurations
ENV SERVICE_CONFIG=/app/config/application.conf
COPY ./config/application/${SERVICE_NAME}/application.conf /app/config/

# Expose clickhouse-service's port
EXPOSE 8080

# Clickhouse-service-specific health check that overrides the base one
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q --spider http://localhost:8080/health || exit 1

# Specify how to run the application
ENTRYPOINT ["java", "-Dconfig.file=/app/config/application.conf", "-jar", "/app/service.jar"]
