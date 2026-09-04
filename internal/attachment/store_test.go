package attachment

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylin1993/codex-remote/internal/persistence"
)

func TestUploadDownloadAndResolveSupportedImages(t *testing.T) {
	for _, tc := range []struct {
		mime   string
		encode func(*bytes.Buffer, image.Image) error
	}{
		{"image/png", func(w *bytes.Buffer, img image.Image) error { return png.Encode(w, img) }},
		{"image/jpeg", func(w *bytes.Buffer, img image.Image) error { return jpeg.Encode(w, img, nil) }},
		{"image/gif", func(w *bytes.Buffer, img image.Image) error { return gif.Encode(w, img, nil) }},
	} {
		t.Run(tc.mime, func(t *testing.T) {
			root := t.TempDir()
			metadata, err := persistence.Open(filepath.Join(root, "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer metadata.Close()
			store, err := New(filepath.Join(root, "attachments"), metadata, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			content := encodedImage(t, tc.encode)
			sum := sha256.Sum256(content)
			digest := hex.EncodeToString(sum[:])
			d, err := store.Upload(context.Background(), "owner-a", "../display-only.image", tc.mime, digest, content)
			if err != nil {
				t.Fatal(err)
			}
			if d.Filename != "../display-only.image" || d.MIMEType != tc.mime || d.SizeBytes != uint64(len(content)) || d.SHA256 != digest || d.WidthPixels != 2 || d.HeightPixels != 1 {
				t.Fatalf("descriptor=%+v", d)
			}
			got, downloaded, err := store.Download(context.Background(), "owner-a", d.AttachmentID)
			if err != nil || got != d || !bytes.Equal(downloaded, content) {
				t.Fatalf("download descriptor=%+v contentEqual=%v err=%v", got, bytes.Equal(downloaded, content), err)
			}
			resolved, path, err := store.Resolve(context.Background(), "owner-a", d.AttachmentID)
			if err != nil || resolved != d || filepath.Dir(path) != filepath.Join(root, "attachments", "blobs") || filepath.Base(path) != d.AttachmentID {
				t.Fatalf("resolve descriptor=%+v path=%q err=%v", resolved, path, err)
			}
			if described, err := store.DescribePath(context.Background(), "owner-a", path); err != nil || described != d {
				t.Fatalf("DescribePath descriptor=%+v err=%v", described, err)
			}
			if _, _, err := store.Download(context.Background(), "owner-b", d.AttachmentID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("cross-owner download err=%v", err)
			}
			if _, err := os.Stat(filepath.Join(root, "attachments", "display-only.image")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("filename was interpreted as a storage path: %v", err)
			}
		})
	}
}

func TestUploadValidationAndPersistedHashCheck(t *testing.T) {
	root := t.TempDir()
	metadata, err := persistence.Open(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	store, err := New(filepath.Join(root, "attachments"), metadata, Config{MaxUploadBytes: 64 << 10, SupportedMIMETypes: []string{"image/png"}})
	if err != nil {
		t.Fatal(err)
	}
	content := encodedImage(t, func(w *bytes.Buffer, img image.Image) error { return png.Encode(w, img) })
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])

	for _, tc := range []struct {
		name, filename, mime, digest string
		content                      []byte
		want                         error
	}{
		{"empty filename", "", "image/png", digest, content, ErrContentInvalid},
		{"uppercase mime", "x", "image/PNG", digest, content, ErrMIMEUnsupported},
		{"mime parameters", "x", "image/png; charset=binary", digest, content, ErrMIMEUnsupported},
		{"uppercase hash", "x", "image/png", "A" + digest[1:], content, ErrContentInvalid},
		{"digest mismatch", "x", "image/png", differentDigest(digest), content, ErrHashMismatch},
		{"invalid image", "x", "image/png", hexDigest([]byte("not png")), []byte("not png"), ErrContentInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.Upload(context.Background(), "owner", tc.filename, tc.mime, tc.digest, tc.content)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want=%v", err, tc.want)
			}
		})
	}

	d, err := store.Upload(context.Background(), "owner", "ok.png", "image/png", digest, content)
	if err != nil {
		t.Fatal(err)
	}
	_, path, err := store.Resolve(context.Background(), "owner", d.AttachmentID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append([]byte(nil), content[:len(content)-1]...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Download(context.Background(), "owner", d.AttachmentID); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("corrupt download err=%v", err)
	}
}

func TestUploadHonorsRawByteLimit(t *testing.T) {
	root := t.TempDir()
	metadata, err := persistence.Open(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	content := encodedImage(t, func(w *bytes.Buffer, img image.Image) error { return png.Encode(w, img) })
	store, err := New(filepath.Join(root, "attachments"), metadata, Config{MaxUploadBytes: uint64(len(content) - 1), SupportedMIMETypes: []string{"image/png"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upload(context.Background(), "owner", "x.png", "image/png", hexDigest(content), content); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err=%v", err)
	}
}

func encodedImage(t *testing.T, encode func(*bytes.Buffer, image.Image) error) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 2, 1))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	img.Set(1, 0, color.RGBA{B: 255, A: 255})
	var out bytes.Buffer
	if err := encode(&out, img); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func hexDigest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func differentDigest(digest string) string {
	if digest[0] == '0' {
		return "1" + digest[1:]
	}
	return "0" + digest[1:]
}
