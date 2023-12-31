#!/usr/bin/env bash
export GOLANGCI_LINT_VERSION="v1.54.2"
export GOFUMPT_VERSION="v0.5.0"

prerequisites() {
  if [[ "$(golangci-lint --version 2>&1)" != *"$GOLANGCI_LINT_VERSION"* ]]; then
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@"${GOLANGCI_LINT_VERSION}"
  fi
  if [[ "$(gofumpt --version 2>&1)" != *"$GOFUMPT_VERSION"* ]]; then
      go install mvdan.cc/gofumpt@"${GOFUMPT_VERSION}"
    fi
}

lint() {
  gofumpt -l -w .
  golangci-lint run --timeout=10m
}

prerequisites
"$@"