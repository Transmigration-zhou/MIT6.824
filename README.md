# MIT6.824

> 课程链接: https://pdos.csail.mit.edu/6.824/
>
> 仅供参考，严禁抄袭



## Lab 1: MapReduce

https://github.com/Transmigration-zhou/MIT6.824/tree/lab_1

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
