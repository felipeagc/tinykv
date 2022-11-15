tinykv: $(wildcard *.go)
	go build -o $@ main.go
