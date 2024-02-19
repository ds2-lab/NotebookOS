FROM golang:1.21.5 as builder 

WORKDIR /app 
COPY . . 

RUN go mod init example.com/myapp 
RUN go get github.com/colinmarc/hdfs
RUN CGO_ENABLED=0 GOOS=linux go build -o myapp

FROM alpine:3.14

WORKDIR /app
COPY --from=builder /app/myapp /app/

EXPOSE 8080
ENTRYPOINT ["/app/myapp"]