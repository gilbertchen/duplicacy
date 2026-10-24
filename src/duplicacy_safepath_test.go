package duplicacy

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSafeJoinPathRejectsTraversal verifies that safeJoinPath rejects an
// entry path that would resolve outside the restore directory, as could
// arrive from a remote snapshot's own stored file list (which, unlike a
// real local directory walk, has no character restrictions).
func TestSafeJoinPathRejectsTraversal(t *testing.T) {
	base := t.TempDir()
	top := filepath.Join(base, "restore-target")
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(top, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}

	if _, err := safeJoinPath(top, "../outside/pwned.txt"); err == nil {
		t.Fatal("expected safeJoinPath to reject a path escaping the restore directory")
	}

	// A normal, well-formed relative entry path should still resolve
	// correctly.
	p, err := safeJoinPath(top, "dir1/file.txt")
	if err != nil {
		t.Fatalf("unexpected error for a normal path: %v", err)
	}
	want := filepath.Join(top, "dir1", "file.txt")
	if p != want {
		t.Fatalf("safeJoinPath(%q, %q) = %q, want %q", top, "dir1/file.txt", p, want)
	}
}
