package proxy

import (
	"bufio"
	"errors"
	"net"
	"net/http"
)

// clientWriter wraps the inbound http.ResponseWriter to record whether
// anything has reached the client yet.
//
// This is the load-bearing safety check for retries. Once a status line has
// gone out, the response is committed: a retry could not change the status, so
// writing a second body onto the first would hand the client a corrupt
// message. Every retry decision is gated on wrote == false.
type clientWriter struct {
	http.ResponseWriter

	// wrote is set by WriteHeader or Write. It is written and read only from
	// the goroutine serving the request — ReverseProxy copies the response
	// body synchronously inside its ServeHTTP — so it needs no atomics.
	wrote  bool
	status int
}

func (w *clientWriter) WriteHeader(code int) {
	if w.wrote {
		// net/http already warns about superfluous WriteHeader; swallowing it
		// here keeps the recorded status equal to what the client actually
		// saw.
		return
	}
	w.wrote = true
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *clientWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.wrote = true
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the real writer. ReverseProxy
// flushes streamed responses through a ResponseController, so without this a
// chunked upstream response would be buffered instead of streamed.
func (w *clientWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush forwards to the underlying writer when it supports flushing. Kept
// alongside Unwrap because plenty of middleware still type-asserts to
// http.Flusher directly.
func (w *clientWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards connection hijacking, which is how a websocket or CONNECT
// upgrade is handed off. Once hijacked the response is irrevocably committed,
// so mark it written.
func (w *clientWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("proxy: underlying ResponseWriter does not support hijacking")
	}
	w.wrote = true
	return h.Hijack()
}
