module github.com/ChristopherDavenport/agentturn/tools/a2a

go 1.25.0

require (
	github.com/ChristopherDavenport/agenttool v0.0.7
	github.com/ChristopherDavenport/agentturn v0.0.7
	github.com/ChristopherDavenport/agentturn/front/a2a v0.0.7
	github.com/ChristopherDavenport/openresponses v0.0.12
	github.com/a2aproject/a2a-go v0.3.15
)

require (
	github.com/google/uuid v1.6.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// The requires name released versions a consumer fetches; the replaces
// build against the tree.
replace (
	github.com/ChristopherDavenport/agentturn => ../..
	github.com/ChristopherDavenport/agentturn/front/a2a => ../../front/a2a
)
