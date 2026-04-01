PROTO_SRC = ../crates/gkg-server/proto/gkg.proto
MODULE    = gitlab.com/gitlab-org/orbit/knowledge-graph/gkgpb

PROTOC_GEN_GO_VERSION      = v1.36.11
PROTOC_GEN_GO_GRPC_VERSION = v1.6.1

.PHONY: generate check clean install-tools

install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

generate: install-tools
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go_opt=Mgkg.proto=$(MODULE) \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       --go-grpc_opt=Mgkg.proto=$(MODULE) \
	       -I../crates/gkg-server/proto \
	       $(PROTO_SRC)

check: install-tools
	$(eval TMPDIR := $(shell mktemp -d))
	@protoc --go_out=$(TMPDIR) --go_opt=paths=source_relative \
	        --go_opt=Mgkg.proto=$(MODULE) \
	        --go-grpc_out=$(TMPDIR) --go-grpc_opt=paths=source_relative \
	        --go-grpc_opt=Mgkg.proto=$(MODULE) \
	        -I../crates/gkg-server/proto \
	        $(PROTO_SRC)
	@diff -q gkg.pb.go $(TMPDIR)/gkg.pb.go && \
	 diff -q gkg_grpc.pb.go $(TMPDIR)/gkg_grpc.pb.go || \
	 (echo "Go proto stubs are out of date. Run 'make -C gkgpb generate' and commit." && rm -rf $(TMPDIR) && exit 1)
	@rm -rf $(TMPDIR)
	@echo "Go proto stubs are up to date."

clean:
	rm -f gkg.pb.go gkg_grpc.pb.go
