tinykv: $(wildcard *.go)
	go build -o $@ .

test:
	go test . -v -count=1

clean:
	rm tinykv
