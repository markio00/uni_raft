FROM alpine:3.21.3

RUN mkdir /app && adduser -h /app -D raft
WORKDIR /app
COPY --chown=raft ./bin/bin_op ./binary

ENTRYPOINT ["/app/binary"]
