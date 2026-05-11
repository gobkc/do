package exporter

import (
	"fmt"
	"log/slog"
	"net/http"
)

func BuildStreamHandler(filename string, exp DocumentExporter, logic func(exp DocumentExporter) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer exp.Close()

		if err := logic(exp); err != nil {
			http.Error(w, fmt.Sprintf("Export logic failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
		w.Header().Set("Content-Transfer-Encoding", "binary")
		w.WriteHeader(http.StatusOK)

		if err := exp.Flush(w); err != nil {
			slog.Error(`Flush to response error`, slog.String(`err`, err.Error()))
		}
	}
}
