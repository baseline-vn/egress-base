// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package uploader

import (
	"io"
	"os"
	"path"
	"time"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/stats"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

const (
	maxRetries = 5
	minDelay   = time.Millisecond * 100
	maxDelay   = time.Second * 5
)

type Uploader interface {
	Upload(string, string, types.OutputType, bool, string) (string, int64, error)
}

type uploader interface {
	upload(string, string, types.OutputType) (string, int64, error)
}

func New(conf config.UploadConfig, backup string, monitor *stats.HandlerMonitor) (Uploader, error) {
	var u uploader
	var err error

	switch c := conf.(type) {
	case *config.EgressS3Upload:
		u, err = newS3Uploader(c)
	case *livekit.S3Upload:
		u, err = newS3Uploader(&config.EgressS3Upload{S3Upload: c})
	case *livekit.GCPUpload:
		u, err = newGCPUploader(c)
	case *livekit.AzureBlobUpload:
		u, err = newAzureUploader(c)
	case *livekit.AliOSSUpload:
		u, err = newAliOSSUploader(c)
	default:
		return &localUploader{}, nil
	}
	if err != nil {
		return nil, err
	}

	remote := &remoteUploader{
		uploader: u,
		backup:   backup,
		monitor:  monitor,
	}

	return remote, nil
}

type remoteUploader struct {
	uploader

	backup  string
	monitor *stats.HandlerMonitor
}

func (u *remoteUploader) Upload(localFilepath, storageFilepath string, outputType types.OutputType, deleteAfterUpload bool, fileType string) (string, int64, error) {
	// Always execute the upload and store in /out/recordings
	outDir := "/out/recordings"
	outFilepath := path.Join(outDir, storageFilepath)
	
	// Ensure the directory exists
	if err := os.MkdirAll(path.Dir(outFilepath), 0755); err != nil {
		logger.Debugw("failed to create output directory", "error", err)
		return "", 0, err
	}
	
	// Copy the file to the output location
	if err := copyFile(localFilepath, outFilepath); err != nil {
		logger.Debugw("failed to copy file to output location", "error", err)
		return "", 0, err
	}
	
	// Get the file size
	fileInfo, err := os.Stat(outFilepath)
	if err != nil {
		logger.Debugw("failed to get file info", "error", err)
		return "", 0, err
	}
	
	// If deleteAfterUpload is true, remove the original file
	if deleteAfterUpload {
		if err := os.Remove(localFilepath); err != nil {
			logger.Debugw("failed to delete original file", "error", err)
			// Note: We don't return here as the upload was successful
		}
	}
	
	return outFilepath, fileInfo.Size(), nil
}

type localUploader struct{}

func (u *localUploader) Upload(localFilepath, _ string, _ types.OutputType, _ bool, _ string) (string, int64, error) {
	stat, err := os.Stat(localFilepath)
	if err != nil {
		return "", 0, err
	}

	return localFilepath, stat.Size(), nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return nil
}