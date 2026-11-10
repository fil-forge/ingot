package taskrunner_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The runner must stay releasable as a library, so it and its test helpers
// import nothing else from Ingot.
func TestImportsNothingFromIngot(t *testing.T) {
	for _, dir := range []string{".", "taskrunnertest", "internal/testhook"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			src, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			f, err := parser.ParseFile(token.NewFileSet(), file, src, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if strings.HasPrefix(path, "github.com/fil-forge/ingot/") && !strings.HasPrefix(path, "github.com/fil-forge/ingot/taskrunner") {
					t.Errorf("%s imports %s", file, path)
				}
			}
		}
	}
}
