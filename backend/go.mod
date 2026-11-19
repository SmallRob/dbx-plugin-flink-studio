module github.com/xihonglei/flink-studio/backend

go 1.22

require github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk v0.1.0

// Vendored official SDK so the sidecar builds fully offline.
replace github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk => ./internal/dbxpluginsdk
