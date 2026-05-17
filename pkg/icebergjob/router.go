package icebergjob

// router.go is the package-level overview for the icebergjob package.
//
// The iceberg-job binary has a single mode: table-commit. The public
// entry point is icebergjob.New(cfg), returning a *TableCommitter
// whose Execute(ctx) drives the iceberg metadata commit.
// See pkg/icebergjob/commit.go for the implementation.
