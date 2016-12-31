FROM golang:alpine
COPY docker-hello.go /go/src/docker-hello.go
RUN go build -o /go/src/docker-hello /go/src/docker-hello.go
ENTRYPOINT [ "/go/src/docker-hello" ]

