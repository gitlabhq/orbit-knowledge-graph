PROTO_SRC = ../../crates/gkg-server/proto/gkg.proto
OUT_DIR   = gkgpb

.PHONY: generate clean

generate: $(OUT_DIR)/gkg.pb.go

$(OUT_DIR)/gkg.pb.go: $(PROTO_SRC)
	cp $(PROTO_SRC) $(OUT_DIR)/gkg.proto
	@# Add go_package option if not present
	@grep -q 'go_package' $(OUT_DIR)/gkg.proto || \
		sed -i'' -e '/^package gkg.v1;/a\
option go_package = "gitlab.com/gitlab-org/orbit/knowledge-graph/proto/go/gkgpb";' $(OUT_DIR)/gkg.proto
	protoc --go_out=$(OUT_DIR) --go_opt=paths=source_relative \
	       --go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative \
	       -I$(OUT_DIR) gkg.proto
	go mod tidy

clean:
	rm -f $(OUT_DIR)/*.pb.go
