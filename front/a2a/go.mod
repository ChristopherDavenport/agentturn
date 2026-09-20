module github.com/ChristopherDavenport/agentturn/front/a2a

go 1.25.0

require (
	github.com/ChristopherDavenport/agenttool v0.0.4
	github.com/ChristopherDavenport/agentturn v0.0.5
	github.com/ChristopherDavenport/openresponses v0.0.9
	github.com/a2aproject/a2a-go v0.3.15
)

require (
	github.com/google/uuid v1.6.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	google.golang.org/grpc v1.84.0 // indirect
)

// The require names the released root a consumer fetches; the replace
// builds against the tree.
replace github.com/ChristopherDavenport/agentturn => ../..
