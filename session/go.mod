module github.com/ChristopherDavenport/agentturn/session

go 1.25.0

require (
	github.com/ChristopherDavenport/agentsession v0.0.19
	github.com/ChristopherDavenport/agenttool v0.0.14
	github.com/ChristopherDavenport/agentturn v0.0.15
	github.com/ChristopherDavenport/openresponses v0.0.12
)

// The require names the released root a consumer fetches; the replace
// builds against the tree.
replace github.com/ChristopherDavenport/agentturn => ../
