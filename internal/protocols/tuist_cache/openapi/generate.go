package openapi

//go:generate sh -c "curl -fsSL https://raw.githubusercontent.com/tuist/tuist/4.144.3/cli/Sources/TuistCache/OpenAPI/cache.yml -o cache.yml"
//go:generate sh -c "awk 'BEGIN{skip=0} /^security:$/ {print \"security: []\"; skip=1; next} skip==1 && /^servers:/ {skip=0} skip==0 {print}' cache.yml > cache.yml.tmp && mv cache.yml.tmp cache.yml"
//go:generate go run github.com/ogen-go/ogen/cmd/ogen@v1.18.0 --clean --config ogen.yaml --target . --package openapi cache.yml
