go build -buildmode=plugin types.go wc.go
go run types.go mrsequential.go wc.so pg*.txt
