tinykv: $(wildcard *.go)
	go build -o $@ .

clean:
	rm tinykv
