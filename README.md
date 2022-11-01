# DS Final Project '22
A small-scale implementation and deployment of MapReduce — as described in the original paper by Dean and Ghemawat — locally using the Go language. 

Done as part of the final project requirements for the UE19CS400MC: Distributed Systems (DIS) Course at PES University. 


## How to Run
```
cd ~/6.824
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
```

### To run the master, on a terminal - 
`go run mrmaster.go pg-*.txt`

### To run workers, open as many terminals as required - 
`go run mrworker.go wc.so`

### To see the output - 
`cat mr-out-* | sort | more`
