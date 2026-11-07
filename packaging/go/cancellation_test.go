package doltlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type compileStartedContext struct {
	context.Context
	started chan struct{}
}

func (ctx compileStartedContext) Err() error {
	err := ctx.Context.Err()
	select {
	case ctx.started <- struct{}{}:
	default:
	}
	return err
}

func TestCancellationBeforeCompileStarts(t *testing.T) {
	slow := "WITH x(v) AS (VALUES " + strings.Repeat("(1), ", 3000000) + "(1)) SELECT count(*) FROM x"
	for _, lockName := range []string{"mu", "runMu"} {
		for _, method := range []string{"prepare", "describe", "exec"} {
			t.Run(lockName+"/"+method, func(t *testing.T) {
				dc, err := (Driver{}).Open("file:" + filepath.Join(t.TempDir(), "cancel.db"))
				if err != nil {
					t.Fatal(err)
				}
				c := dc.(*conn)
				defer c.Close()
				var lock *sync.Mutex
				if lockName == "mu" {
					lock = &c.mu
				} else {
					lock = &c.runMu
				}
				lock.Lock()
				locked := true
				defer func() {
					if locked {
						lock.Unlock()
					}
				}()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				started := make(chan struct{}, 1)
				done := make(chan error, 1)
				go func() {
					ctx := compileStartedContext{ctx, started}
					var err error
					switch method {
					case "prepare":
						var s driver.Stmt
						s, err = c.PrepareContext(ctx, slow)
						if s != nil {
							s.Close()
						}
					case "describe":
						_, err = c.DescribeColumnsContext(ctx, slow)
					case "exec":
						_, err = c.ExecContext(ctx, slow, nil)
					}
					done <- err
				}()
				<-started
				cancel()
				time.Sleep(50 * time.Millisecond)
				start := time.Now()
				lock.Unlock()
				locked = false
				err = <-done
				elapsed := time.Since(start)
				if !errors.Is(err, context.Canceled) {
					t.Errorf("want context.Canceled, got %v", err)
				}
				if elapsed > 500*time.Millisecond {
					t.Errorf("expired request compiled for %v after unlocking", elapsed)
				}
				if _, err := c.ExecContext(context.Background(), "SELECT 1", nil); err != nil {
					t.Fatalf("connection after cancellation: %v", err)
				}
			})
		}
	}
}

func TestCompileCancellationSurvivesParserStartup(t *testing.T) {
	dc, err := (Driver{}).Open("file:" + filepath.Join(t.TempDir(), "startup.db"))
	if err != nil {
		t.Fatal(err)
	}
	c := dc.(*conn)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	stop := c.interruptOnDone(ctx, time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	slow := "WITH x(v) AS (VALUES " + strings.Repeat("(1), ", 3000000) + "(1)) SELECT count(*) FROM x"
	start := time.Now()
	s, err := c.Prepare(slow)
	elapsed := time.Since(start)
	stop()
	if s != nil {
		s.Close()
	}
	var engineErr *Error
	if !errors.As(err, &engineErr) || engineErr.Code&0xff != 9 {
		t.Fatalf("want SQLITE_INTERRUPT, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("compiling after cancellation ran for %v", elapsed)
	}
	if _, err := c.ExecContext(context.Background(), "SELECT 1", nil); err != nil {
		t.Fatalf("connection after stopping interrupt: %v", err)
	}
}

func TestPrepareStopsInterruptBeforeNextCompile(t *testing.T) {
	dc, err := (Driver{}).Open("file:" + filepath.Join(t.TempDir(), "next.db"))
	if err != nil {
		t.Fatal(err)
	}
	c := dc.(*conn)
	defer c.Close()
	entered, release, nextEntered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var selects atomic.Int32
	if err := c.SetAuthorizer(func(action int, _, _, _, _ string) int {
		if action == AuthSelect {
			switch selects.Add(1) {
			case 1:
				close(entered)
				<-release
			case 2:
				close(nextEntered)
			}
		}
		return AuthOK
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		s, err := c.PrepareContext(ctx, "SELECT 1")
		if s != nil {
			s.Close()
		}
		done <- err
	}()
	<-entered
	c.interruptMu.Lock()
	cancel()
	time.Sleep(50 * time.Millisecond)
	close(release)
	nextDone := make(chan error, 1)
	go func() {
		s, err := c.Prepare("SELECT 2")
		if s != nil {
			s.Close()
		}
		nextDone <- err
	}()
	select {
	case <-nextEntered:
		t.Error("next compilation started before the previous interrupt watcher stopped")
	case <-time.After(50 * time.Millisecond):
	}
	c.interruptMu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("canceled compilation: %v", err)
	}
	if err := <-nextDone; err != nil {
		t.Errorf("next compilation: %v", err)
	}
}
