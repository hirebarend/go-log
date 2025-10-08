go run cmd/main/main.go

go build cmd/main/main.go && ./main

go test ./...

go test . -bench=.
