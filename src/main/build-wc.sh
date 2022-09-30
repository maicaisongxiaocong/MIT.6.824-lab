rm wc.go
go build -race -buildmode=plugin ../mrapps/wc.go
rm -f mr-*