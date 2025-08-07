package sync

import (
	"bytes"
	"fmt"
	log "log/slog"
	"net"
	"net/http"
	"net/http/pprof"
)

// Stat the struct to hold some stats while MySQL sync
type Stat struct {
	Sms []*Manager

	l net.Listener
}

func (s *Stat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	_ = r
	for _, sm := range s.Sms {
		rr, err := sm.canal.Execute("SHOW MASTER STATUS")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("execute sql error %v", err)))
			return
		}

		binName, _ := rr.GetString(0, 0)
		binPos, _ := rr.GetUint(0, 1)

		pos := sm.canal.SyncedPosition()

		buf.WriteString(fmt.Sprintf("sync info of %s\n", sm.c.Label))
		buf.WriteString("-------------------------------------------------------------------------------\n")
		buf.WriteString(fmt.Sprintf("server_current_binlog:(%s, %d)\n", binName, binPos))
		buf.WriteString(fmt.Sprintf("read_binlog:%s\n", pos))

		buf.WriteString(fmt.Sprintf("insert_num:%d\n", sm.InsertNum))
		buf.WriteString(fmt.Sprintf("update_num:%d\n", sm.UpdateNum))
		buf.WriteString(fmt.Sprintf("delete_num:%d\n", sm.DeleteNum))
		buf.WriteString(fmt.Sprintf("sync chan capacity: %d\n", len(sm.syncCh)))
		buf.WriteString("-------------------------------------------------------------------------------\n\n")
	}

	_, _ = w.Write(buf.Bytes())
}

// Run the start function of Stat server
func (s *Stat) Run(addr string) {
	if len(addr) == 0 {
		return
	}
	log.Info("Run status http server", log.String("addr", addr))
	var err error
	s.l, err = net.Listen("tcp", addr)
	if err != nil {
		log.Error("Listen stat addr failed", log.String("addr", addr), log.Any("err", err))
		return
	}

	srv := http.Server{}
	mux := http.NewServeMux()
	mux.Handle("/stat", s)
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	srv.Handler = mux

	_ = srv.Serve(s.l)
}

// Close the close function of Stat
func (s *Stat) Close() {
	if s.l != nil {
		_ = s.l.Close()
	}
}
