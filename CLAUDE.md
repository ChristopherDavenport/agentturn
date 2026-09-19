# agentturn

A composable agent loop in Go over Open Responses: one loop, one
transcript type (`openresponses.Items`), one tool contract, hooks and
queues. Everything else is a front or a subscriber. The design is in
`docs/plans/agent-layer.md`; read it before writing code. The
Composition section states the rule that shapes every package: the loop
knows a `Model` and a list of `Tool`, and never learns a sub-agent
concept.

## Module

- Module path: `github.com/ChristopherDavenport/agentturn`.
- Go 1.25 is the floor. The root package name is `agentturn`.
- The root module depends on
  `github.com/ChristopherDavenport/openresponses` (pin v0.0.9 or later),
  `github.com/ChristopherDavenport/agenttool` (the tool contract) and
  the standard library. Nothing else.
- Anything with another dependency is a nested module with its own
  `go.mod`: `front/a2a`, `tools/a2a`, and `session`, which is the only
  place `agentsession` is imported.
- The tool contract, its schema generator and the MCP adapters live in
  `../agenttool`; nothing tool-shaped is added here.

## Siblings

Peer repositories, each independently versioned, each depending on
`openresponses`:

- `../open-responses`: the wire package. Copy its conventions.
- `../agenttool`: the tool contract and the MCP adapters. The root
  module here depends on it; it never depends on this module.
- `../agentsession`: the session format and library. Only the `session`
  nested module here imports it.

Decisions shared with `agentsession` are written in its RFC, not shared
as code: the request hash (JCS canonical form, SHA-256) and the `link`
entry for subsessions. The `session` nested module carries its own copy
of the hash tested against the same golden vectors.

## Conventions

Mirror `../open-responses`: a `Makefile` with `build`, `deps`, `test`,
`vet`, `fmt`, `tidy`, `lint`, `vuln` and `check` targets, a `deps`
target that fails if the root module imports anything beyond
`openresponses`, `agenttool` and the standard library, a `SUBMODULES` list for nested
modules, the same CI workflow shape (minimum and stable Go, lint,
tidy-check), a `CHANGELOG.md` in Keep a Changelog form, and annotated
`v*` tags whose message becomes the release notes. `make check` must
pass before any commit.

Tests are table-driven and run offline against the `echo` adapter and
`streamtest` from `openresponses`.
