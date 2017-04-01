// Command tcpsink implements TCP server storing incoming stream of json
// messages to snappy-compressed files.
package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/artyom/autoflags"
	"github.com/golang/snappy"
)

func main() {
	args := struct {
		Addr string        `flag:"addr,address to listen"`
		Dir  string        `flag:"dir,directory to write logs to"`
		Size int           `flag:"size,maximum file size in bytes (split on message boundary)"`
		Age  time.Duration `flag:"age,remove logs older than this value"`
	}{
		Addr: "localhost:9000",
		Dir:  "/tmp/logs",
		Size: 16 << 20,
	}
	autoflags.Parse(&args)
	if err := do(args.Addr, args.Dir, args.Size, args.Age); err != nil {
		log.Fatal(err)
	}
}

func do(addr, dir string, size int, maxAge time.Duration) error {
	if size < 1<<20 {
		size = 1 << 20
	}
	logger := log.New(os.Stderr, "", log.LstdFlags)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		logger.Println(<-sigCh)
		ln.Close()
	}()
	s := &srv{dir: dir, max: size}
	defer s.finish()
	if maxAge > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go cleanOld(ctx, dir, maxAge)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			if err := s.handleConn(conn); err != nil {
				logger.Println(err)
			}
		}()
	}
}

type srv struct {
	dir string
	max int
	log *log.Logger

	mu sync.Mutex
	w  io.WriteCloser // snappy.Writer
	n  int            // number of bytes written to w
	f  *os.File       // os.File
	t  time.Time      // time of file creation
}

func (s *srv) handleConn(conn io.ReadCloser) error {
	defer conn.Close()
	dec := json.NewDecoder(conn)
	var msg json.RawMessage
	for dec.More() {
		if err := dec.Decode(&msg); err != nil {
			return err
		}
		if err := s.writeMessage(msg); err != nil {
			return err
		}
	}
	return conn.Close()
}

func (s *srv) writeMessage(msg []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		if err := s.newLog(); err != nil {
			return err
		}
	}
	n, err := s.w.Write(append(msg, '\n'))
	s.n += n
	if s.n > s.max || err != nil {
		_ = s.rotate(err != nil) // TODO: log error?
	}
	return err
}

// newLog creates new log file. This should be called with lock already held
func (s *srv) newLog() error {
	f, err := ioutil.TempFile(s.dir, "current-")
	if err != nil {
		return err
	}
	_ = f.Chmod(0644)
	s.f, s.w, s.t, s.n = f, snappy.NewWriter(f), time.Now().UTC(), 0
	return nil
}

// rotate closes current file and renames it. This should be called with lock
// already held
func (s *srv) rotate(bad bool) error {
	defer func() {
		s.w.Close()
		s.f.Close()
		if bad {
			os.Rename(s.f.Name(), s.f.Name()+".bad")
		}
		s.f, s.w = nil, nil
	}()
	if err := s.w.Close(); err != nil {
		return err
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	name := filepath.Join(s.dir, s.t.Format("20060102_150405")+".log.sz")
	if bad {
		name += ".bad"
	}
	return os.Rename(s.f.Name(), name)
}

func (s *srv) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return
	}
	s.rotate(false)
}

func cleanOld(ctx context.Context, dir string, maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	if min := 10 * time.Minute; maxAge < min {
		maxAge = min
	}
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			fn := func(path string, info os.FileInfo, err error) error {
				if info.IsDir() && path != dir {
					// don't descend into subdirs
					return filepath.SkipDir
				}
				if err != nil || !info.Mode().IsRegular() ||
					!strings.Contains(path, ".log.sz") {
					return nil
				}
				// TODO: filepath.Walk walks tree in
				// lexicographic mode, so we can rely on file
				// names being sorted by time and return
				// filepath.SkipDir on first non-old record seen
				if t.Sub(info.ModTime()) > maxAge {
					_ = os.Remove(path)
				}
				return nil
			}
			_ = filepath.Walk(dir, fn)
		}
	}
}
