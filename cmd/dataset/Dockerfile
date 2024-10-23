# Use the official Golang image as the base image
FROM golang:1.22.5

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code into the container
COPY cmd/dataset /app/cmd/dataset

# Build the application
RUN go build -o main ./cmd/dataset

# Specify the command to run the application
CMD ["./main"]
