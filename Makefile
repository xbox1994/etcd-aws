.PHONY:

GO_SOURCES=$(shell find . -name \*.go)
SOURCES=$(GO_SOURCES)
PLATFORM_BINARIES=dist/etcd-aws.Linux.x86_64

IMAGE_VERSION=1.1.0
IMAGE_NAME="ksowh/etcd-aws:${IMAGE_VERSION}"
GITHUB_USER=ksowh
GITHUB_REPOSITORY=etcd-aws

all: $(PLATFORM_BINARIES)

clean:
	-rm -f $(PLATFORM_BINARIES) dist/etcd.Linux.x86_64

dist/cacert.pem:
	[ -d dist ] || mkdir dist
	curl -s -o $@ https://curl.haxx.se/ca/cacert.pem

dist/etcd.Linux.x86_64:
	[ -d dist ] || mkdir dist
	curl -L -s https://github.com/coreos/etcd/releases/download/v3.1.0/etcd-v3.1.0-linux-amd64.tar.gz |\
		tar -C dist -xzf -
	cp dist/etcd-v3.1.0-linux-amd64/etcd dist/etcd3.Linux.x86_64
	cp dist/etcd-v3.1.0-linux-amd64/etcdctl dist/etcdctl3.Linux.x86_64
	rm -rf dist/etcd-v3.1.0-linux-amd64

dist/etcd-aws.Linux.x86_64: $(SOURCES)
	[ -d dist ] || mkdir dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags '-s' \
	  -o $@ ./etcd-aws.go ./lifecycle.go

container: dist/cacert.pem dist/etcd-aws.Linux.x86_64 dist/etcd.Linux.x86_64
	docker build -t $(IMAGE_NAME) .

check:
	go test ./...

lint:
	go fmt ./...
	goimports -w $(GO_SOURCES)

release: lint check container $(PLATFORM_BINARIES)
	@[ ! -z "$(VERSION)" ] || (echo "you must specify the VERSION"; false)
	which ghr >/dev/null || go get github.com/tcnksm/ghr
	ghr -u $(GITHUB_USER) -r $(GITHUB_REPOSITORY) --delete v$(VERSION) dist/
	docker tag $(IMAGE_NAME) $(IMAGE_NAME):$(VERSION)
	docker push $(IMAGE_NAME)
	docker push $(IMAGE_NAME):$(VERSION)
