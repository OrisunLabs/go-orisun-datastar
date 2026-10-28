package todo

import (
	"errors"
	"strings"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type CommandMetadata = eventstore.CommandMetadata

func validateTitle(title string) (string, error) {
	title = strings.TrimSpace(title)
	if title == "" || len(title) > 160 {
		return "", errors.New("todo title must be between 1 and 160 characters")
	}
	return title, nil
}
