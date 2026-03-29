PROTO_SRC = ../../crates/gkg-server/proto/gkg.proto
OUT_DIR   = gkgpb

.PHONY: generate check clean

generate:
	cp $(PROTO_SRC) $(OUT_DIR)/gkg.proto
	@grep -q 'go_package' $(OUT_DIR)/gkg.proto || \
		sed -i'' -e '/^package gkg.v1;/a\option go_package = "gitlab.com/gitlab-org/orbit/knowledge-graph/proto/go/gkgpb";' $(OUT_DIR)/gkg.proto
	protoc --go_out=$(OUT_DIR) --go_opt=paths=source_relative \
	       --go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative \
	       -I$(OUT_DIR) $(OUT_DIR)/gkg.proto
	go mod tidy

check:
	@cp $(PROTO_SRC) /tmp/gkg-check.proto
	@grep -q 'go_package' /tmp/gkg-check.proto || \
		sed -i'' -e '/^package gkg.v1;/a\option go_package = "gitlab.com/gitlab-org/orbit/knowledge-graph/proto/go/gkgpb";' /tmp/gkg-check.proto
	@mkdir -p /tmp/gkg-proto-check
	@protoc --go_out=/tmp/gkg-proto-check --go_opt=paths=source_relative \
	        --go-grpc_out=/tmp/gkg-proto-check --go-grpc_opt=paths=source_relative \
	        -I/tmp /tmp/gkg-check.proto
	@diff -q $(OUT_DIR)/gkg.pb.go /tmp/gkg-proto-check/gkg.pb.go > /dev/null 2>&1 && \
		diff -q $(OUT_DIR)/gkg_grpc.pb.go /tmp/gkg-proto-check/gkg_grpc.pb.go > /dev/null 2>&1 || \
		(echo "ERROR: Go proto stubs are out of date. Run 'make -C proto/go generate' and commit." && exit 1)
	@echo "Go proto stubs are up to date."
	@rm -rf /tmp/gkg-proto-check /tmp/gkg-check.proto

clean:
	rm -f $(OUT_DIR)/*.pb.go
