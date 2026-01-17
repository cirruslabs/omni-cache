package version

import (
	"fmt"
	"runtime/debug"
	"strings"
)

var (
	Version     = "unknown"
	Commit      = "unknown"
	FullVersion = ""
)

func init() {
	info, ok := debug.ReadBuildInfo()
	if Version == "unknown" && ok {
		buildVersion := strings.TrimPrefix(info.Main.Version, "v")
		if buildVersion != "" && buildVersion != "(devel)" {
			Version = buildVersion
		}
	}

	if Commit == "unknown" && ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				Commit = setting.Value
				break
			}
		}
	}

	FullVersion = fmt.Sprintf("%s-%s", Version, Commit)
}
