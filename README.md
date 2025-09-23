```bash
go build cmd/main/main.go && ./main
```

```bash
go test ./...

go test . -bench=. -benchmem -benchtime=20s
```

```bash
go test -bench=. -run=^$ -benchmem \
  -cpuprofile=cpu.out \
  -memprofile=mem.out \
  -blockprofile=block.out -blockprofilerate=1 \
  -mutexprofile=mutex.out -mutexprofilefraction=1

go tool pprof cpu.out
```