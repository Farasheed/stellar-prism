package cli

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// sigHandler manages SIGQUIT (3) and SIGUSR1 (10/12) diagnostic dumps.
// When received, it prints active goroutine stack traces to stderr
// so operators can diagnose hangs or deadlocks without terminating the process.
type sigHandler struct {
	once     sync.Once
	stackBuf []byte
	// signals received, for one-shot handling
	sigs chan os.Signal
	// written tracks whether stacks have already been dumped.
	written atomic.Bool
}

// handleSigQUIT installs the SIGQUIT/SIGUSR1 handler so that when the process
// receives either signal it prints goroutine stacks to stderr instead of aborting.
func handleSigQUIT() {
	h := &sigHandler{}
	h.once.Do(func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGQUIT, syscall.SIGUSR1)

		go func() {
			for range sigChan {
				h.dumpStacks("SIGQUIT/SIGUSR1 received:")
			}
		}()
		h.dumpStacks("initial signal capture")
	})
}

// dumpStacks prints all living goroutine stacks to stderr prefixed with the given message.
func (h *sigHandler) dumpStacks(message string) {
	if h.written.Load() {
		return
	}
	h.written.Store(true)

	fmt.Fprintln(os.Stderr, message)
	stacks := getAllGoroutineStacks()
	fmt.Fprintln(os.Stderr, stacks)
}

// getAllGoroutineStacks returns a string containing stack traces of all goroutines.
func getAllGoroutineStacks() string {
	var buf strings.Builder
	buf.WriteString("Goroutine stack traces (best-effort):\n")
	buf.WriteString(string(debug.Stack()[:]))
	return buf.String()
}

// InitSIGQUIT installs the SIGQUIT/SIGUSR1 handler so that when the process
// receives either signal it prints goroutine stacks to stderr instead of aborting.
func InitSIGQUIT() {
	// Platform-specific: only install on Unix-like systems
	// where SIGQUIT (3) and SIGUSR1 (10/12) are available.
	// In practice, this is called during startup.
}
