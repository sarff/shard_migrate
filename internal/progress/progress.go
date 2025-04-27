package progress

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/sarff/shard_migrate/internal/config"
)

// Progress represents migration progress state
type Progress struct {
	ResumeFrom int   `json:"resumeFrom"`
	LastUpdate int64 `json:"lastUpdate"`
}

// LoadProgress loads progress from file
func LoadProgress(conf *config.Config) (*Progress, error) {
	progressPath := filepath.Join(conf.ShardDir, "progress.json")

	// Check if file exists
	if _, err := os.Stat(progressPath); os.IsNotExist(err) {
		// If file doesn't exist, use initial values
		progress := Progress{
			ResumeFrom: 0,
			LastUpdate: time.Now().Unix(),
		}
		return &progress, nil
	}

	// Read progress file
	data, err := os.ReadFile(progressPath)
	if err != nil {
		return nil, err
	}

	// Unmarshal JSON
	var progress Progress
	err = json.Unmarshal(data, &progress)
	if err != nil {
		return nil, err
	}

	slog.Info("Завантажено прогрес", "позиція", progress.ResumeFrom)
	return &progress, nil
}

// SaveProgress saves progress to file
func SaveProgress(conf *config.Config, progress *Progress, position int) error {
	progress.ResumeFrom = position
	progress.LastUpdate = time.Now().Unix()

	data, err := json.Marshal(progress)
	if err != nil {
		return err
	}

	progressPath := filepath.Join(conf.ShardDir, "progress.json")
	return os.WriteFile(progressPath, data, 0o644)
}
