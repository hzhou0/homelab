// Package ui is the browser-facing rendering of the same model the API serves. It ships its own
// assets so the page works on a network that can reach nothing but this service.
package ui

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"
)

//go:embed templates.html
var templates embed.FS

//go:embed assets
var assets embed.FS

var pages = template.Must(template.New("ui").Funcs(template.FuncMap{
	"join": func(values []string) string { return strings.Join(values, ", ") },
	// Anything that is neither absent nor suspended has a compute to take down, whatever state
	// compute_ctl reports it in.
	"running": func(status string) bool { return status != "absent" && status != "suspended" },
	// Absent is not zero: a size that could not be read is not a branch holding nothing.
	"size": func(size *uint64) string {
		if size == nil {
			return ""
		}
		return humanBytes(*size)
	},
}).ParseFS(templates, "templates.html"))

func humanBytes(size uint64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	value, exponent := float64(size)/unit, 0
	for value >= unit && exponent < 3 {
		value, exponent = value/unit, exponent+1
	}
	return fmt.Sprintf("%.1f %s", value, [...]string{"KiB", "MiB", "GiB", "TiB"}[exponent])
}

func Render(w io.Writer, name string, data any) error {
	return pages.ExecuteTemplate(w, name, data)
}

func Assets() http.Handler {
	return http.FileServer(http.FS(assets))
}
