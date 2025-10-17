# Dockerfile
FROM golang:1.25.2
WORKDIR /app
COPY . .
RUN go mod download
CMD ["go", "run", "task.go"]
