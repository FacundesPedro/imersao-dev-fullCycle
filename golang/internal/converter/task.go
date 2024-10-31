package converter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

type VideoConverter struct {
	DB *sql.DB
}

func NewVideoConverter(DB *sql.DB) *VideoConverter {
	return &VideoConverter{
		DB: DB,
	}
}

type VideoTask struct {
	VideoId int    `json:"video_id"`
	Path    string `json:"path"`
}

// const pattern = "*.chunk"

func (vc *VideoConverter) Handle(msgBytes []byte, convertedFileName, mergedFileName string) {
	var thread VideoTask

	err := json.Unmarshal(msgBytes, &thread)
	if err != nil {
		vc.LogError(thread, "Failed to unmarshal task", err)
		return
	}

	if IsProcessed(vc.DB, thread.VideoId) {
		slog.Warn("Video was already processed", slog.Int("video_id", thread.VideoId))
		return
	}

	err = vc.process(&thread, convertedFileName, mergedFileName)
	if err != nil {
		vc.LogError(thread, "Failed to process video with id="+string(thread.VideoId), err)
		return
	}

	err = MarkProcessed(vc.DB, thread.VideoId)
	if err != nil {
		vc.LogError(thread, "Failed to mark video as processed video_id="+string(thread.VideoId), err)
		return
	}

	slog.Info("Video was succesfully processed", slog.Int("video_id", thread.VideoId))
}

func (vc *VideoConverter) LogError(thread VideoTask, message string, err error) {
	errData := map[string]any{
		"video_id": thread.VideoId,
		"message":  message,
		"details":  err.Error(),
		"time":     time.Now(),
	}

	errSerialized, _ := json.Marshal(errData)
	slog.Error("Processing error", slog.String("data", string(errSerialized)))

	// remember to persist error in a database
	RegisterError(vc.DB, errData, err)
}

func (vc *VideoConverter) process(
	thread *VideoTask,
	convertedFileName,
	mergedFileName string) error {
	mergedFile := filepath.Join(thread.Path, mergedFileName)
	convertedFilePathFolder := filepath.Join(thread.Path, "mpeg-dash")

	slog.Info("Merging chunks", slog.String("path", thread.Path))

	err := vc.mergeChunksFromDir(thread.Path, mergedFile)
	if err != nil {
		errMessage := "Failed to merge chunks for VideoId=" + string(thread.VideoId)

		vc.LogError(*thread, errMessage, err)
		// return fmt.Errorf("%s: %v", errMessage, err)
		return err
	}

	err = os.MkdirAll(convertedFilePathFolder, os.ModePerm)
	if err != nil {
		vc.LogError(*thread, "Failed to create directory for mpeg-dash files", err)
		return err
	}

	slog.Info("Converting video", slog.String("path", thread.Path))
	ffmpegCommand := exec.Command("ffmpeg", "-i", mergedFile,
		"-f", "dash", filepath.Join(convertedFilePathFolder, convertedFileName))

	out, err := ffmpegCommand.CombinedOutput()
	if err != nil {
		vc.LogError(*thread, "Failed to convert video to mpeg-dash format [outstream: "+string(out)+"]",
			err)
		return err
	}

	slog.Info("Video Successfully converted to mpeg-dash", slog.Int("video_id", thread.VideoId), slog.String("pathConverted", convertedFilePathFolder))

	slog.Info("Deleting non-converted video", slog.Int("video_id", thread.VideoId), slog.String("path", mergedFile))
	err = os.Remove(mergedFile)
	if err != nil {
		vc.LogError(*thread, "Failed to remove non-converted file for VideoId="+string(thread.VideoId), err)
		return err
	}

	return nil
}

// will return the number from filename, returns -1 in case of error
func (vc *VideoConverter) getNumberFromFile(filename string) int {
	regex := regexp.MustCompile(`\d+`)

	str := regex.FindString(filepath.Base(filename))

	num, err := strconv.Atoi(str)

	if err != nil {
		return -1
	}

	return num
}

func (vc *VideoConverter) mergeChunksFromDir(inDir, outFile string) error {
	pattern := "*.chunk"

	chunks, err := filepath.Glob(filepath.Join(inDir, pattern))

	if err != nil {
		return fmt.Errorf("failed to find any chunk: %v", err)
	}

	sort.Slice(
		chunks, func(i, j int) bool {
			return vc.getNumberFromFile(chunks[i]) < vc.getNumberFromFile(chunks[j])
		})

	out, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("failed to create a file: %v", err)
	}

	// will close, after function execution for not having memory leak
	defer out.Close()

	for _, chunk := range chunks {
		in, err := os.Open(chunk)
		if err != nil {
			return fmt.Errorf("failed to open chunk: %v", err)
		}

		_, err = out.ReadFrom(in)
		if err != nil {
			return fmt.Errorf("failed to write chunk %s: %v", chunk, err)
		}

		in.Close()
	}

	return nil
}
