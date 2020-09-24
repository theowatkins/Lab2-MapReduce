go build -buildmode=plugin wc.go
go run mrsequential.go wc.so pg*.txt
