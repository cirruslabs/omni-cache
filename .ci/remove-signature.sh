#!/bin/bash

# Set shell options to enable fail-fast behavior.
set -euo pipefail

codesign --remove-signature "$@" || true
