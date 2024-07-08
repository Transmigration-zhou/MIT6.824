# MIT6.824

> 课程链接: https://pdos.csail.mit.edu/6.824/
> 
> 论文链接: https://raft.github.io/raft.pdf
>
> 仅供参考，严禁抄袭



## Lab 1: MapReduce

https://github.com/Transmigration-zhou/MIT6.824/tree/lab_1

![image](https://github.com/Transmigration-zhou/MIT6.824/assets/57855015/628db29b-ddff-48b1-9931-c80310c76d08)


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

## Lab 2: Raft

Raft的可视化网站：https://thesecretlivesofdata.com/raft/

![image](https://github.com/Transmigration-zhou/MIT6.824/assets/57855015/5325d4ee-7cda-41e9-9dd7-422fb150fa5b)

![image](https://github.com/Transmigration-zhou/MIT6.824/assets/57855015/e031d099-dd7e-46e0-8421-4e0823fcde34)


### Part 2A: leader election

https://github.com/Transmigration-zhou/MIT6.824/tree/lab_2a

验证结果:
```bash
cd src/raft
# 单个测试
go test -run 2A -race
# 批量测试（测试次数、同一时刻测试的数量、lab名称）
./go-test-many.sh 1000 4 2A 
```

### Part 2B: log

![image](https://github.com/Transmigration-zhou/MIT6.824/assets/57855015/b756d545-605b-4fb6-aa63-f37eeda4da52)


https://github.com/Transmigration-zhou/MIT6.824/tree/lab_2b

验证结果:
```bash
cd src/raft
# 单个测试
go test -run 2B -race
# 批量测试（测试次数、同一时刻测试的数量、lab名称）
./go-test-many.sh 1000 4 2B

for i in {1..1000}; go test -run 2B 
```

### Part 2C: persistence

https://github.com/Transmigration-zhou/MIT6.824/tree/lab_2c

验证结果:
```bash
cd src/raft
# 单个测试
go test -run 2C -race
# 批量测试（测试次数、同一时刻测试的数量、lab名称）
./go-test-many.sh 1000 4 2C

for i in {1..1000}; go test -run 2C 
```

