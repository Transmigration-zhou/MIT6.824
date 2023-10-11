# MIT6.824

课程链接: https://pdos.csail.mit.edu/6.824/

## Lab 1: MapReduce
```bash
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*q
go run mrsequential.go wc.so pg*.txt
more mr-out-0
```
验证结果:
```bash
bash test-mr.sh
```