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
	"context"
	"io"
	"path/filepath"

	"github.com/Microsoft/go-winio/backuptar"
	"github.com/Microsoft/hcsshim"
)

// ExportLayer writes an OCI layer tar stream from the provided on-disk layer.
// The caller must specify the parent layers, if any, ordered from lowest to
// highest layer.
//
// The layer will be mounted for this process, so the caller should ensure that
// it is not currently mounted.
// Workalike for github.com/Microsoft/hcsshim/internal/ociwclayer ExportLayer
func ExportLayer(ctx context.Context, w io.Writer, path string, parentLayerPaths []string) error {
	// Based on github.com/Microsoft/hcsshim/internal/ociwclayer/export.go
	var driverInfo = hcsshim.DriverInfo{}

	err := hcsshim.ActivateLayer(driverInfo, path)
	if err != nil {
		return err
	}
	defer hcsshim.DeactivateLayer(driverInfo, path)

	// Prepare and unprepare the layer to ensure that it has been initialized.
	err = hcsshim.PrepareLayer(driverInfo, path, parentLayerPaths)
	if err != nil {
		return err
	}
	err = hcsshim.UnprepareLayer(driverInfo, path)
	if err != nil {
		return err
	}

	r, err := hcsshim.NewLayerReader(driverInfo, path, parentLayerPaths)
	if err != nil {
		return err
	}

	err = writeTarFromLayer(ctx, r, w)
	cerr := r.Close()
	if err != nil {
		return err
	}
	return cerr
}

// Forked from github.com/Microsoft/hcsshim/internal/ociwclayer/export.go
// ctx added so we can abort early.
func writeTarFromLayer(ctx context.Context, r hcsshim.LayerReader, w io.Writer) error {
	t := tar.NewWriter(w)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		name, size, fileInfo, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if fileInfo == nil {
			// Write a whiteout file.
			hdr := &tar.Header{
				Name: filepath.ToSlash(filepath.Join(filepath.Dir(name), whiteoutPrefix+filepath.Base(name))),
			}
			err := t.WriteHeader(hdr)
			if err != nil {
				return err
			}
		} else {
			err = backuptar.WriteTarFileFromBackupStream(t, r, name, size, fileInfo)
			if err != nil {
				return err
			}
		}
	}
	return t.Close()
}
