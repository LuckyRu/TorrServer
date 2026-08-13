//go:build gst

package gstreamer

import (
	"context"
	"errors"
	"fmt"

	"server/log"
	"server/settings"
)

func gstDebugf(format string, args ...any) {
	if !settings.IsDebug() {
		return
	}
	log.TLogln("[GStreamer] debug:", fmt.Sprintf(format, args...))
}

func gstErrorf(format string, args ...any) {
	log.TLogln("[GStreamer] error:", fmt.Sprintf(format, args...))
}

// gstWarnf is for degradation the server keeps working through, so it must survive with
// debug logging off — that is the whole point of reporting it.
func gstWarnf(format string, args ...any) {
	log.TLogln("[GStreamer] warn:", fmt.Sprintf(format, args...))
}

func gstTaskDebugf(task *Task, format string, args ...any) {
	if !settings.IsDebug() {
		return
	}
	gstDebugf("%s %s", gstTaskLogPrefix(task), fmt.Sprintf(format, args...))
}

func gstTaskErrorf(task *Task, format string, args ...any) {
	gstErrorf("%s %s", gstTaskLogPrefix(task), fmt.Sprintf(format, args...))
}

func gstTaskFailure(task *Task, operation string, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		gstTaskDebugf(task, "%s canceled", operation)
		return
	}
	gstTaskErrorf(task, "%s failed: %v", operation, err)
}

func gstSourceFailure(client string, hash string, fileID string, audio int, operation string, err error) {
	if err == nil {
		return
	}
	prefix := fmt.Sprintf("hash=%s file=%s audio=%d client=%s", hash, fileID, audio, orUnknownClient(client))
	if errors.Is(err, context.Canceled) {
		gstDebugf("%s %s canceled", prefix, operation)
		return
	}
	gstErrorf("%s %s failed: %v", prefix, operation, err)
}

func gstTaskLogPrefix(task *Task) string {
	if task == nil {
		return "hash=<nil>"
	}
	return fmt.Sprintf("hash=%s file=%s audio=%d client=%s session=%s",
		task.Hash, task.FileID, task.Audio, orUnknownClient(task.ClientID), task.Token)
}

func orUnknownClient(client string) string {
	if client == "" {
		return "?"
	}
	return client
}
