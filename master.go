package sync

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// PositionHolder the interface to describe MySQL binlog position holder
type PositionHolder interface {
	Load() (*mysql.Position, error)
	Save(pos *mysql.Position) error
}

// FilePositionHolder the default implementation of PositionHolder based on file
type FilePositionHolder struct {
	dataDir string
}

// Save the function to save the binlog position
func (h *FilePositionHolder) Save(pos *mysql.Position) error {
	if len(h.dataDir) == 0 {
		return nil
	}

	filePath := path.Join(h.dataDir, "master.info")
	tempFilePath := filePath + ".tmp"

	posContent := fmt.Sprintf("%s:%v", pos.Name, pos.Pos)

	var err error
	if err = os.WriteFile(tempFilePath, []byte(posContent), 0644); err != nil {
		log.Error("Canal save master info to file failed", log.String("file", tempFilePath), log.Any("err", err))
		return err
	}

	if err = os.Rename(tempFilePath, filePath); err != nil {
		log.Error("Rename temp file failed", log.String("file", tempFilePath), log.Any("err", err))
		return err
	}

	return nil
}

// Load the function to retrieve the MySQL binlog position
func (h *FilePositionHolder) Load() (*mysql.Position, error) {
	var pos mysql.Position

	if err := os.MkdirAll(h.dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	filePath := path.Join(h.dataDir, "master.info")
	_, err := os.Open(filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return nil, nil
	}

	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	toks := strings.Split(string(bytes), ":")
	if len(toks) == 2 {
		pos.Name = toks[0]

		rawPos, err := strconv.Atoi(toks[1])

		if err != nil {
			return nil, errors.Trace(err)
		}
		pos.Pos = uint32(rawPos)
		return &pos, nil
	}
	return nil, errors.New("Cannot parse mysql position")
}

type masterInfo struct {
	sync.RWMutex

	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`

	lastSaveTime time.Time

	holder PositionHolder
}

func (m *masterInfo) loadPos() error {
	m.lastSaveTime = time.Now()

	pos, err := m.holder.Load()

	if err != nil {
		return errors.Trace(err)
	}

	if pos != nil {
		m.Name = pos.Name
		m.Pos = pos.Pos
	}

	return nil
}

func (m *masterInfo) Save(pos mysql.Position) error {
	log.Info("Save position", log.String("pos", pos.String()))

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}
	m.lastSaveTime = n

	err := m.holder.Save(&pos)

	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
}

func (m *masterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}
