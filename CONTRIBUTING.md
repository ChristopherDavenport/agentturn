# Contributing

Issues and pull requests are welcome.

## Before you start

The design is in `docs/plans/agent-layer.md`. Its Composition section
states the rule that shapes every package: the loop knows a `Model` and
a list of `Tool`, and never learns a sub-agent concept. Anything that
composes agents is a front (serve side) or a tool (consume side) of one
protocol, never a new concept inside the loop.

For anything larger than a bug fix, open an issue first so the shape of
the change can be discussed before you spend time on it.

## Development

Go 1.25 or later is required. The full local check is:

```sh
make check        # gofmt, tidy, vet, deps, replaces, staticcheck, govulncheck, race tests, every module
```

The individual targets are `fmt`, `tidy-check`, `vet`, `deps`, `lint`,
`vuln`, `test` and `tidy`. `lint` and `vuln` run staticcheck and govulncheck through
`go run`, which may download a newer Go toolchain the first time.

The repository has several modules. The root module is the loop,
`front/responses`, `compact` and `tools/agent`, and depends on
`openresponses`, `agenttool` and the standard library only; `make deps`
fails if anything else creeps in. The tool contract and the MCP
adapters are the separate `agenttool` repository. Every adapter here
that needs another dependency is a nested module with its own `go.mod`,
listed under `SUBMODULES` in the Makefile: `front/a2a`, `tools/a2a` and
`session`. A bare
`go test ./...` at the root does not cover them; the Makefile targets
do. Each nested `go.mod` requires the released root next to a `replace`
to the tree: consumers ignore the replace and fetch the version, the
checkout builds against the working tree.

Tests run offline. The model in a test is the `echo` adapter from
`openresponses`, and streams are validated with `streamtest`.

## Pull requests

- Keep the change focused; unrelated cleanups belong in their own PR.
- Add or update tests. Tests are table-driven.
- Run `make check` before pushing. CI runs the same steps on the minimum
  and current Go versions.
- Note user-visible changes under *Unreleased* in `CHANGELOG.md`.

## Releases

Every module in the repository shares one version and is tagged at one
commit. With the changelog's *Unreleased* section written:

```sh
make release VERSION=v0.1.0
```

sets the root requirement in each nested module to the version, dates
the changelog, runs `make check`, commits, tags `v0.1.0` and
`front/a2a/v0.1.0`, `tools/a2a/v0.1.0`, `session/v0.1.0` with the
changelog section as the message, and pushes. The release workflow
publishes a GitHub release per tag, and the Go module proxy picks the
versions up. Before v1.0.0 the API may change between minor versions;
the changelog records every break.


`make release-guard TAG=<tag>` is what stands between a mistake and a
permanent one, and `make release` runs it for every tag it writes. It
refuses a dirty tree, a tag that already exists locally or on origin, a
version that sorts below the current root release or does not move its
module forward, a first-party require that does not name that version, a
root tag that is not this commit, and a module that will not build with
`GOWORK=off`. The root is guarded and tagged first, because a nested
module's guard needs the root tag to exist. Nothing is public until the
push, so a refusal costs a `git reset --hard HEAD~1` and a `git tag -d`.

`make replaces`, part of `check`, refuses a first-party require that
lacks a matching `replace`. There is no `go.work` here, so the replaces
are the only thing building the tree against itself — and losing one
would make the next release resolve that module from the proxy, where
the version being released does not exist yet.

`go mod tidy` can move a requirement that the release just set, so the
requires are read back and asserted before anything is tagged.
