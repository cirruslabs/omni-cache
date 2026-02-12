package openapi

//go:generate sh -c "curl -fsSL https://raw.githubusercontent.com/tuist/tuist/4.144.3/cli/Sources/TuistCache/OpenAPI/cache.yml -o cache.yml"
//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 -config oapi-codegen.yaml cache.yml
