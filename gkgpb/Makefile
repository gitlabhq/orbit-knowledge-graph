PROTO_SRC = ../crates/gkg-server/proto/gkg.proto
MODULE    = gitlab.com/gitlab-org/orbit/knowledge-graph/gkgpb

.PHONY: generate check clean

generate:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go_opt=Mgkg.proto=$(MODULE) \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       --go-grpc_opt=Mgkg.proto=$(MODULE) \
	       -I../crates/gkg-server/proto \
	       $(PROTO_SRC)

check:
	@mkdir -p /tmp/gkgpb-check
	@protoc --go_out=/tmp/gkgpb-check --go_opt=paths=source_relative \
	        --go_opt=Mgkg.proto=$(MODULE) \
	        --go-grpc_out=/tmp/gkgpb-check --go-grpc_opt=paths=source_relative \
	        --go-grpc_opt=Mgkg.proto=$(MODULE) \
	        -I../crates/gkg-server/proto \
	        $(PROTO_SRC)
	@diff -q gkg.pb.go /tmp/gkgpb-check/gkg.pb.go && \
	 diff -q gkg_grpc.pb.go /tmp/gkgpb-check/gkg_grpc.pb.go || \
	 (echo "Go proto stubs are out of date. Run 'make -C gkgpb generate' and commit." && rm -rf /tmp/gkgpb-check && exit 1)
	@rm -rf /tmp/gkgpb-check
	@echo "Go proto stubs are up to date."

clean:
	rm -f gkg.pb.go gkg_grpc.pb.go
