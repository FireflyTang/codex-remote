package directory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareNormalizeAndCreate(t *testing.T) {
	base := t.TempDir()
	s := Service{Base: base}
	p, err := s.Prepare("new/../workspace", true)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(base, "workspace")
	if p.Path != want || !p.Created {
		t.Fatalf("got %+v want path=%s created", p, want)
	}
	if info, err := os.Stat(want); err != nil || !info.IsDir() {
		t.Fatalf("directory not created: %v", err)
	}
	again, err := s.Prepare(want, false)
	if err != nil || again.Created {
		t.Fatalf("second prepare=%+v err=%v", again, err)
	}
}

func TestPrepareRejectsFileAndMissing(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := Service{Base: base}
	if _, err := s.Prepare(file, false); err == nil {
		t.Fatal("expected file rejection")
	}
	if _, err := s.Prepare("missing", false); err == nil {
		t.Fatal("expected missing rejection")
	}
}
