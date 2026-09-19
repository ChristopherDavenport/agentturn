GO ?= go
STATICCHECK ?= $(GO) run honnef.co/go/tools/cmd/staticcheck@latest
GOVULNCHECK ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@latest
# Nested modules with their own go.mod, so their dependencies stay out of
# the root module. Each requires the released root next to a replace that
# builds against the tree, and every module shares one version: see
# release. ./... from the root covers only the root module, so every
# target loops over them.
SUBMODULES = front/a2a tools/a2a session

.PHONY: build deps test vet fmt tidy tidy-check lint vuln check release clean

build:
	$(GO) build ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GO) build ./...) || exit 1; done

# The root module is the loop and must build from agenttool,
# openresponses and the standard library alone; anything heavier is a
# nested module.
deps:
	@deps=$$($(GO) list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... | grep -v '^github.com/ChristopherDavenport/agentturn' | grep -v '^github.com/ChristopherDavenport/agenttool' | grep -v '^github.com/ChristopherDavenport/openresponses' || true); \
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

MODULE := $(shell $(GO) list -m)
NOTES := $(shell mktemp)

# Cut a release. Every module in the repository shares one version and
# one commit: each nested module's requirement on the root, and on any
# sibling module, is set to VERSION next to the replace that keeps it
# building from the tree; the changelog's Unreleased section is dated;
# everything is checked; one commit is made; the root is tagged VERSION
# and each nested module <dir>/VERSION with the changelog section as the
# message; and the branch and tags are pushed. TRAILER, when set, is
# appended to the commit message.
release:
	@test -n "$(VERSION)" || { echo "usage: make release VERSION=vX.Y.Z"; exit 1; }
	@grep -q '^## Unreleased$$' CHANGELOG.md || { echo "CHANGELOG.md has no Unreleased section"; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "working tree is not clean"; exit 1; }
	@for m in $(SUBMODULES); do ( \
	  cd $$m && $(GO) mod edit -require=$(MODULE)@$(VERSION) && \
	  for s in $(SUBMODULES); do \
	    if grep -q "^[[:space:]]*$(MODULE)/$$s " go.mod; then $(GO) mod edit -require=$(MODULE)/$$s@$(VERSION) || exit 1; fi; \
	  done && $(GO) mod tidy ) || exit 1; done
	sed -i 's/^## Unreleased$$/## $(VERSION) - '"$$(date +%F)"'/' CHANGELOG.md
	$(MAKE) tidy
	$(MAKE) check
	git add -A && git commit -q -m "Release $(VERSION)" $(if $(TRAILER),-m "$(TRAILER)")
	@awk -v v="$(VERSION)" '/^## /{p=($$2==v)} p' CHANGELOG.md | sed '1s/.*/$(VERSION)/' > $(NOTES)
	git tag -a $(VERSION) -F $(NOTES)
	@for m in $(SUBMODULES); do git tag -a $$m/$(VERSION) -F $(NOTES) || exit 1; done
	@rm -f $(NOTES)
	git push origin HEAD $(VERSION) $(patsubst %,%/$(VERSION),$(SUBMODULES))

clean:
	rm -rf .cache
