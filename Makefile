GO ?= go
# -mod=mod, whether from the environment or the go env file, is
# incompatible with workspace mode; force the default and keep every
# other flag the caller set.
export GOFLAGS := -mod=readonly $(filter-out -mod=%,$(GOFLAGS))
STATICCHECK ?= $(GO) run honnef.co/go/tools/cmd/staticcheck@latest
GOVULNCHECK ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@latest
# Nested modules with their own go.mod, so their dependencies stay out of
# the root module. go.work puts them in one workspace so they build
# against the checked-out root instead of the version their go.mod
# requires; ./... from the root still covers only the root module, so
# every target loops over them.
SUBMODULES = front/a2a tools/a2a session

.PHONY: build deps test vet fmt tidy tidy-check lint vuln check clean

build:
	$(GO) build ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GO) build ./...) || exit 1; done

# The root module is the loop and must build from agenttool,
# openresponses and the standard library alone; anything heavier is a
# nested module. GOWORK=off scopes ./... to the root module.
deps:
	@deps=$$(GOWORK=off $(GO) list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... | grep -v '^github.com/ChristopherDavenport/agentturn' | grep -v '^github.com/ChristopherDavenport/agenttool' | grep -v '^github.com/ChristopherDavenport/openresponses' || true); \
	  test -z "$$deps" || { echo "root module depends on: $$deps"; exit 1; }

test:
	$(GO) test -race ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GO) test -race ./...) || exit 1; done

vet:
	$(GO) vet ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GO) vet ./...) || exit 1; done

tidy:
	$(GO) mod tidy
	@for m in $(SUBMODULES); do (cd $$m && $(GO) mod tidy) || exit 1; done

# Fails when go mod tidy would change any go.mod or go.sum, without
# writing, so a stray dependency shows up in make check and not only in
# CI's diff.
tidy-check:
	$(GO) mod tidy -diff
	@for m in $(SUBMODULES); do (cd $$m && $(GO) mod tidy -diff) || exit 1; done

fmt:
	gofmt -l . && test -z "$$(gofmt -l .)"

lint:
	$(STATICCHECK) ./...
	@for m in $(SUBMODULES); do (cd $$m && $(STATICCHECK) ./...) || exit 1; done

vuln:
	$(GOVULNCHECK) ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GOVULNCHECK) ./...) || exit 1; done

# Everything CI runs.
check: fmt tidy-check vet deps lint vuln test

clean:
	rm -rf .cache
