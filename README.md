```bash
go build cmd/main/main.go && ./main
```

```bash
go test ./...

go test . -bench=. -benchmem -benchtime=20s
```