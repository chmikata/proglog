FROM golang:1.19.0 as dev
WORKDIR /app

RUN go install github.com/cosmtrek/air@latest

CMD ["air", "-c", ".air.toml"]
