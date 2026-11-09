module e1asset

// Pinned 2026-11-09 to the module set of fabric-samples asset-transfer-basic/chaincode-go/go.mod at the
// pinned samples commit 5789681b4f4d24e58fa40f19a69f5496892374b6 (built against Fabric 2.5 there), minus
// that sample's test-only modules. go 1.25.0 is that file's directive; ccenv 2.5.16 builds with go1.26.4
// (environment plan v2, Stage 5c), so the toolchain is new enough and GOTOOLCHAIN never needs a download.
// go.sum and vendor/ do not exist yet: `go mod tidy && go mod vendor` is environment Stage 7b (download).
go 1.25.0

require github.com/hyperledger/fabric-contract-api-go/v2 v2.2.0

require (
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/spec v0.21.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/hyperledger/fabric-chaincode-go/v2 v2.0.0 // indirect
	github.com/hyperledger/fabric-protos-go-apiv2 v0.3.4 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
