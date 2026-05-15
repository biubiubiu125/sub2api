# This Dockerfile builds the backend source tree only and does NOT embed the Vue frontend.
# Use the repository-root Dockerfile or deploy/Dockerfile for production images so that:
# 1. frontend is built into backend/internal/web/dist
# 2. Go is built with -tags embed
FROM golang:1.26.3-alpine

WORKDIR /app

ARG ALLOW_BACKEND_ONLY_IMAGE=false

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN if [ "${ALLOW_BACKEND_ONLY_IMAGE}" != "true" ]; then \
      echo "backend/Dockerfile does not embed frontend. Use ../Dockerfile or ../deploy/Dockerfile for production, or pass --build-arg ALLOW_BACKEND_ONLY_IMAGE=true for an explicit backend-only debug image." >&2; \
      exit 1; \
    fi

RUN CGO_ENABLED=0 go build -o main ./cmd/server/

EXPOSE 8080

CMD ["./main"]
