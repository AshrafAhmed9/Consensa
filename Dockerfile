# Multi-stage build: compile a static consensa binary, then run it from a minimal image.
# CGO is disabled deliberately -- nothing in this repo needs it, and a static binary means
# the runtime stage does not need libc-compatible base image matching.
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/consensa ./cmd/consensa

FROM alpine:3.20
RUN adduser -D -u 10001 consensa
# /data is created here, owned by the non-root user, BEFORE anything mounts a volume over
# it: Docker populates a fresh named volume from the image directory it replaces on first
# use, preserving ownership -- so this is what makes the volume writable by `consensa`
# rather than defaulting to root-owned.
RUN mkdir -p /data && chown consensa:consensa /data
COPY --from=builder /out/consensa /usr/local/bin/consensa
USER consensa
ENTRYPOINT ["consensa"]
