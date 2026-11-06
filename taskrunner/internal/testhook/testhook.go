// Package testhook lets the runner's test helpers reach options that no
// caller outside the runner may set.
package testhook

// ApplyWritesImmediately makes the *taskrunner.Options it is given run each
// write as a handler makes it, for taskrunnertest.CheckOwnWrites. The
// taskrunner package sets it; it takes any because this package cannot
// import taskrunner.
var ApplyWritesImmediately func(opts any)
