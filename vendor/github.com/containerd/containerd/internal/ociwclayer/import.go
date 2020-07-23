// +build windows

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// This package is a polyfill of the internal/ociwclayer package in hcsshim.
// Ideally, that package will become public, and this can be replaced.

package ociwclayer

import (
	"archive/tar"
	"bufio"
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/backuptar"
	"github.com/Microsoft/hcsshim"
)

const (
	// whiteoutPrefix prefix means file is a whiteout. If this is followed by a
	// filename this means that file has been removed from the base layer.
	// See https://github.com/opencontainers/image-spec/blob/master/layer.md#whiteouts
	whiteoutPrefix = ".wh."
)

var (
	// mutatedFiles is a list of files that are mutated by the import process
	// and must be backed up and restored.
	mutatedFiles = map[string]string{
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD":      "bcd.bak",
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD.LOG":  "bcd.log.bak",
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD.LOG1": "bcd.log1.bak",
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD.LOG2": "bcd.log2.bak",
	}
)

// ImportLayer reads a layer from an OCI layer tar stream and extracts it to the
// specified path. The caller must specify the parent layers, if any, ordered
// from lowest to highest layer.
//
// The caller must ensure that the thread or process has acquired backup and
// restore privileges.
//
// This function returns the total size of the layer's files, in bytes.
// Workalike for github.com/Microsoft/hcsshim/internal/ociwclayer ImportLayer
func ImportLayer(ctx context.Context, r io.Reader, layerPath string, parentLayerPaths []string) (size int64, err error) {
	home, id := filepath.Split(layerPath)
	info := hcsshim.DriverInfo{
		HomeDir: home,
	}

	w, err := hcsshim.NewLayerWriter(info, id, parentLayerPaths)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err2 := w.Close(); err2 != nil {
			// This error should not be discarded as a failure here
			// could result in an invalid layer on disk
			if err == nil {
				err = err2
			}
		}
	}()

	tr := tar.NewReader(r)
	buf := bufio.NewWriter(nil)
	hdr, nextErr := tr.Next()
	// Iterate through the files in the archive.
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		if nextErr == io.EOF {
			// end of tar archive
			break
		}
		if nextErr != nil {
			return 0, nextErr
		}

		// Note: path is used instead of filepath to prevent OS specific handling
		// of the tar path
		base := path.Base(hdr.Name)
		if strings.HasPrefix(base, whiteoutPrefix) {
			dir := path.Dir(hdr.Name)
			originalBase := base[len(whiteoutPrefix):]
			originalPath := path.Join(dir, originalBase)
			if err := w.Remove(filepath.FromSlash(originalPath)); err != nil {
				return 0, err
			}
			hdr, nextErr = tr.Next()
		} else if hdr.Typeflag == tar.TypeLink {
			err := w.AddLink(filepath.FromSlash(hdr.Name), filepath.FromSlash(hdr.Linkname))
			if err != nil {
				return 0, err
			}
			hdr, nextErr = tr.Next()
		} else {
			name, fileSize, fileInfo, err := backuptar.FileInfoFromHeader(hdr)
			if err != nil {
				return 0, err
			}
			if err := w.Add(filepath.FromSlash(name), fileInfo); err != nil {
				return 0, err
			}
			size += fileSize
			hdr, nextErr = tarToBackupStreamWithMutatedFiles(buf, w, tr, hdr, layerPath)
		}
	}

	return
}

// tarToBackupStreamWithMutatedFiles reads data from a tar stream and
// writes it to a backup stream, and also saves any files that will be mutated
// by the import layer process to a backup location.
func tarToBackupStreamWithMutatedFiles(buf *bufio.Writer, w io.Writer, t *tar.Reader, hdr *tar.Header, root string) (nextHdr *tar.Header, err error) {
	var (
		bcdBackup       *os.File
		bcdBackupWriter *winio.BackupFileWriter
	)
	if backupPath, ok := mutatedFiles[hdr.Name]; ok {
		bcdBackup, err = os.Create(filepath.Join(root, backupPath))
		if err != nil {
			return nil, err
		}
		defer func() {
			cerr := bcdBackup.Close()
			if err == nil {
				err = cerr
			}
		}()

		bcdBackupWriter = winio.NewBackupFileWriter(bcdBackup, false)
		defer func() {
			cerr := bcdBackupWriter.Close()
			if err == nil {
				err = cerr
			}
		}()

		buf.Reset(io.MultiWriter(w, bcdBackupWriter))
	} else {
		buf.Reset(w)
	}

	defer func() {
		ferr := buf.Flush()
		if err == nil {
			err = ferr
		}
	}()

	return backuptar.WriteBackupStreamFromTarFile(buf, t, hdr)
}
