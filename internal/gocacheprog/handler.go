package gocacheprog

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/stats"
)

const (
	cacheKeyPrefix = "gocacheprog"
)

const (
	cmdGet   = "get"
	cmdPut   = "put"
	cmdClose = "close"
)

type Config struct {
	CacheClient CacheClient
	CacheDir    string
	Logger      *slog.Logger
	Strict      bool
}

type Handler struct {
	cacheClient CacheClient
	logger      *slog.Logger
	strict      bool

	cacheDir   string
	cleanupDir bool

	mu      sync.Mutex
	entries map[string]*entry

	now func() time.Time
}

type entry struct {
	outputID []byte
	diskPath string
	size     int64
	putTime  time.Time
}

type request struct {
	ID       int64  `json:"ID"`
	Command  string `json:"Command"`
	ActionID []byte `json:",omitempty"`
	OutputID []byte `json:",omitempty"`
	BodySize int64  `json:",omitempty"`
}

type response struct {
	ID  int64  `json:"ID"`
	Err string `json:",omitempty"`

	KnownCommands []string `json:",omitempty"`

	Miss     bool       `json:",omitempty"`
	OutputID []byte     `json:",omitempty"`
	Size     int64      `json:",omitempty"`
	Time     *time.Time `json:",omitempty"`
	DiskPath string     `json:",omitempty"`
}

func NewHandler(cfg Config) (*Handler, error) {
	if cfg.CacheClient == nil {
		return nil, fmt.Errorf("gocacheprog cache client is nil")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	cacheDir := strings.TrimSpace(cfg.CacheDir)
	cleanupDir := false
	if cacheDir == "" {
		baseDir, err := os.UserCacheDir()
		if err != nil {
			return nil, fmt.Errorf("resolve user cache dir: %w", err)
		}
		baseDir = filepath.Join(baseDir, "omni-cache", "gocacheprog")
		if err := os.MkdirAll(baseDir, 0o700); err != nil {
			return nil, fmt.Errorf("create cache base dir: %w", err)
		}

		sessionDir, err := os.MkdirTemp(baseDir, "session-")
		if err != nil {
			return nil, fmt.Errorf("create cache session dir: %w", err)
		}
		cacheDir = sessionDir
		cleanupDir = true
	} else {
		if err := os.MkdirAll(cacheDir, 0o700); err != nil {
			return nil, fmt.Errorf("create cache dir: %w", err)
		}
	}

	absDir, err := filepath.Abs(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("resolve cache dir: %w", err)
	}

	return &Handler{
		cacheClient: cfg.CacheClient,
		logger:      logger,
		strict:      cfg.Strict,
		cacheDir:    absDir,
		cleanupDir:  cleanupDir,
		entries:     make(map[string]*entry),
		now:         time.Now,
	}, nil
}

func (h *Handler) Serve(ctx context.Context, r io.Reader, w io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return fmt.Errorf("stdin reader is nil")
	}
	if w == nil {
		return fmt.Errorf("stdout writer is nil")
	}

	br := bufio.NewReader(r)
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)

	capabilities := response{
		ID:            0,
		KnownCommands: []string{cmdGet, cmdPut, cmdClose},
	}
	if err := enc.Encode(capabilities); err != nil {
		return fmt.Errorf("write capabilities: %w", err)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush capabilities: %w", err)
	}

	for {
		line, eof, err := readRequestLine(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		var req request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			h.logger.ErrorContext(ctx, "failed to decode request", "err", err)
			if err := writeResponse(bw, enc, response{Err: err.Error()}); err != nil {
				return err
			}
			if eof {
				break
			}
			continue
		}

		res, shouldClose, err := h.handleRequest(ctx, br, &req)
		if err != nil {
			return err
		}
		if err := writeResponse(bw, enc, res); err != nil {
			return err
		}
		if shouldClose || eof {
			break
		}
	}

	if h.cleanupDir {
		if err := os.RemoveAll(h.cacheDir); err != nil {
			h.logger.WarnContext(ctx, "failed to remove cache dir", "path", h.cacheDir, "err", err)
		}
	}
	stats.Default().LogSummary()
	return nil
}

func readRequestLine(r *bufio.Reader) (string, bool, error) {
	for {
		line, err := r.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", false, err
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if errors.Is(err, io.EOF) {
				return "", true, io.EOF
			}
			continue
		}
		return trimmed, errors.Is(err, io.EOF), nil
	}
}

func writeResponse(bw *bufio.Writer, enc *json.Encoder, res response) error {
	if err := enc.Encode(res); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush response: %w", err)
	}
	return nil
}

func (h *Handler) handleRequest(ctx context.Context, br *bufio.Reader, req *request) (response, bool, error) {
	switch req.Command {
	case cmdGet:
		if req.BodySize > 0 {
			if _, err := readBody(br, req.BodySize, io.Discard); err != nil {
				return response{}, false, err
			}
		}
		res := h.handleGet(ctx, req)
		return res, false, nil
	case cmdPut:
		res, err := h.handlePut(ctx, br, req)
		return res, false, err
	case cmdClose:
		if req.BodySize > 0 {
			if _, err := readBody(br, req.BodySize, io.Discard); err != nil {
				return response{}, false, err
			}
		}
		return response{ID: req.ID}, true, nil
	default:
		if req.BodySize > 0 {
			if _, err := readBody(br, req.BodySize, io.Discard); err != nil {
				return response{}, false, err
			}
		}
		return response{ID: req.ID, Err: fmt.Sprintf("unknown command %q", req.Command)}, false, nil
	}
}

func (h *Handler) handlePut(ctx context.Context, br *bufio.Reader, req *request) (response, error) {
	res := response{ID: req.ID}

	if len(req.ActionID) == 0 {
		if req.BodySize > 0 {
			if _, err := readBody(br, req.BodySize, io.Discard); err != nil {
				return response{}, err
			}
		}
		res.Err = "missing ActionID"
		return res, nil
	}
	if len(req.OutputID) == 0 {
		if req.BodySize > 0 {
			if _, err := readBody(br, req.BodySize, io.Discard); err != nil {
				return response{}, err
			}
		}
		res.Err = "missing OutputID"
		return res, nil
	}
	if req.BodySize < 0 {
		res.Err = "invalid BodySize"
		return res, nil
	}

	actionKey := hex.EncodeToString(req.ActionID)
	cachePath := h.cacheFilePath(actionKey)
	tmpFile, err := os.CreateTemp(h.cacheDir, actionKey+".tmp-")
	if err != nil {
		if req.BodySize > 0 {
			if _, discardErr := readBody(br, req.BodySize, io.Discard); discardErr != nil {
				return response{}, discardErr
			}
		}
		res.Err = fmt.Sprintf("create temp file: %v", err)
		return res, nil
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	written, err := readBody(br, req.BodySize, tmpFile)
	if err != nil {
		return response{}, err
	}

	if err := tmpFile.Close(); err != nil {
		res.Err = fmt.Sprintf("close temp file: %v", err)
		return res, nil
	}

	if err := os.Remove(cachePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		res.Err = fmt.Sprintf("remove existing cache file: %v", err)
		return res, nil
	}
	if err := os.Rename(tmpFile.Name(), cachePath); err != nil {
		res.Err = fmt.Sprintf("persist cache file: %v", err)
		return res, nil
	}

	putTime := h.now().UTC()

	if err := h.uploadToCache(ctx, actionKey, cachePath, written); err != nil {
		h.logger.ErrorContext(ctx, "cache upload failed", "action", actionKey, "err", err)
		if h.strict {
			res.Err = err.Error()
			return res, nil
		}
	}

	h.rememberEntry(actionKey, cachePath, req.OutputID, written, putTime)
	res.DiskPath = cachePath
	return res, nil
}

func (h *Handler) handleGet(ctx context.Context, req *request) response {
	res := response{ID: req.ID}

	if len(req.ActionID) == 0 {
		res.Err = "missing ActionID"
		return res
	}

	actionKey := hex.EncodeToString(req.ActionID)
	if entry, ok := h.cachedEntry(actionKey); ok {
		if fileInfo, err := os.Stat(entry.diskPath); err == nil && !fileInfo.IsDir() {
			res.OutputID = append([]byte(nil), entry.outputID...)
			res.DiskPath = entry.diskPath
			res.Size = fileInfo.Size()
			if !entry.putTime.IsZero() {
				putTime := entry.putTime
				res.Time = &putTime
			}
			stats.Default().RecordCacheHit()
			return res
		}
		h.mu.Lock()
		delete(h.entries, actionKey)
		h.mu.Unlock()
	}

	storageKey := cacheKey(actionKey)
	cachePath := h.cacheFilePath(actionKey)
	found, err := h.downloadToFile(ctx, storageKey, cachePath)
	if err != nil {
		h.logger.ErrorContext(ctx, "cache download failed", "action", actionKey, "err", err)
		if h.strict {
			res.Err = err.Error()
			return res
		}
		stats.Default().RecordCacheMiss()
		res.Miss = true
		return res
	}
	if !found {
		stats.Default().RecordCacheMiss()
		res.Miss = true
		return res
	}

	outputID, size, putTime, err := computeOutputID(cachePath)
	if err != nil {
		h.logger.ErrorContext(ctx, "failed to compute output id", "action", actionKey, "err", err)
		if h.strict {
			res.Err = err.Error()
			return res
		}
		stats.Default().RecordCacheMiss()
		res.Miss = true
		return res
	}

	h.rememberEntry(actionKey, cachePath, outputID, size, putTime)
	res.OutputID = append([]byte(nil), outputID...)
	res.DiskPath = cachePath
	res.Size = size
	if !putTime.IsZero() {
		res.Time = &putTime
	}
	stats.Default().RecordCacheHit()
	return res
}

func (h *Handler) cacheFilePath(actionKey string) string {
	return filepath.Join(h.cacheDir, actionKey)
}

func cacheKey(actionKey string) string {
	return path.Join(cacheKeyPrefix, actionKey)
}

func (h *Handler) uploadToCache(ctx context.Context, actionKey, filePath string, size int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	return h.cacheClient.Upload(ctx, cacheKey(actionKey), file, size)
}

func (h *Handler) downloadToFile(ctx context.Context, storageKey, filePath string) (bool, error) {
	tmpFile, err := os.CreateTemp(h.cacheDir, filepath.Base(filePath)+".download-")
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	if err := tmpFile.Truncate(0); err != nil {
		return false, err
	}
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return false, err
	}

	found, err := h.cacheClient.Download(ctx, storageKey, tmpFile)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	if err := tmpFile.Close(); err != nil {
		return false, err
	}
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.Rename(tmpFile.Name(), filePath); err != nil {
		return false, err
	}
	return true, nil
}

func (h *Handler) rememberEntry(actionKey, diskPath string, outputID []byte, size int64, putTime time.Time) {
	if outputID == nil {
		return
	}

	entryCopy := &entry{
		outputID: append([]byte(nil), outputID...),
		diskPath: diskPath,
		size:     size,
		putTime:  putTime,
	}

	h.mu.Lock()
	h.entries[actionKey] = entryCopy
	h.mu.Unlock()
}

func (h *Handler) cachedEntry(actionKey string) (*entry, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := h.entries[actionKey]
	if entry == nil {
		return nil, false
	}
	return entry, true
}

func computeOutputID(path string) ([]byte, int64, time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, 0, time.Time{}, err
	}

	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	outputID := hasher.Sum(nil)
	return outputID, size, info.ModTime().UTC(), nil
}

func readBody(r *bufio.Reader, size int64, dst io.Writer) (int64, error) {
	if size == 0 {
		return 0, nil
	}
	if size < 0 {
		return 0, fmt.Errorf("negative body size")
	}
	if dst == nil {
		return 0, fmt.Errorf("nil body destination")
	}

	if err := consumeOpeningQuote(r); err != nil {
		return 0, err
	}

	encodedLen, err := encodedLength(size)
	if err != nil {
		return 0, err
	}

	limited := &io.LimitedReader{R: r, N: encodedLen}
	decoder := base64.NewDecoder(base64.StdEncoding, limited)
	written, err := io.Copy(dst, decoder)
	if err != nil {
		return written, err
	}
	if written != size {
		return written, fmt.Errorf("decoded %d bytes, expected %d", written, size)
	}
	if limited.N != 0 {
		return written, fmt.Errorf("truncated body: %d base64 bytes remaining", limited.N)
	}

	if err := consumeClosingQuote(r); err != nil {
		return written, err
	}

	return written, nil
}

func consumeOpeningQuote(r *bufio.Reader) error {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case '\n', '\r', ' ', '\t':
			continue
		case '"':
			return nil
		default:
			return fmt.Errorf("expected body opening quote, got %q", b)
		}
	}
}

func consumeClosingQuote(r *bufio.Reader) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	if b != '"' {
		return fmt.Errorf("expected body closing quote, got %q", b)
	}

	b, err = r.ReadByte()
	if err != nil {
		return err
	}
	if b == '\r' {
		b, err = r.ReadByte()
		if err != nil {
			return err
		}
	}
	if b != '\n' {
		return fmt.Errorf("expected newline after body, got %q", b)
	}
	return nil
}

func encodedLength(size int64) (int64, error) {
	if size < 0 {
		return 0, fmt.Errorf("negative size")
	}
	if size > math.MaxInt {
		return 0, fmt.Errorf("body too large")
	}
	return int64(base64.StdEncoding.EncodedLen(int(size))), nil
}
