module github.com/ChristopherDavenport/agentturn/session

go 1.25.0

require (
	github.com/ChristopherDavenport/agentsession v0.0.11-0.20260929155507-2456736d1651
	github.com/ChristopherDavenport/agenttool v0.0.10
	github.com/ChristopherDavenport/agentturn v0.0.10
	github.com/ChristopherDavenport/openresponses v0.0.12
)

// The require names the released root a consumer fetches; the replace
// builds against the tree.
replace github.com/ChristopherDavenport/agentturn => ../
