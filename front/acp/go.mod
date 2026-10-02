module github.com/ChristopherDavenport/agentturn/front/acp

go 1.27.0

require (
	github.com/ChristopherDavenport/agenttool v0.0.15
	github.com/ChristopherDavenport/agentturn v0.0.16
	github.com/ChristopherDavenport/openresponses v0.0.14
	github.com/ironpark/acp-go v0.1.0
)

// The require names the released root a consumer fetches; the replace
// builds against the tree.
replace github.com/ChristopherDavenport/agentturn => ../..
