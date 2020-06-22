FROM golang:1.14-alpine as builder

# At run (for build container) must be declared as an argument '--build-arg SSH_PRIVATE_KEY=...data...'
# Add Maintainer Info
LABEL maintainer="Vladimir Fetisov <ra3eg@mail.ru>"

# Install apk and base soft (git, certicates, openssh)
RUN apk update && apk upgrade && apk add --no-cache git ca-certificates tzdata openssh-client && update-ca-certificates

# Copy everything from the current directory to the PWD(Present Working Directory) inside the container
COPY . .

# Change directory to source files
WORKDIR src

# Compile source files
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOPRIVATE=github.com GO111MODULE=on go build -ldflags="-w -s" -o /go/bin/billing

# Build a small image
FROM scratch

# Copy binary files
COPY --from=builder /go/bin/billing /go/bin/billing

# Entry point
ENTRYPOINT ["/go/bin/billing"]

