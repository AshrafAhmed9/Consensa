// Package metrics provides the deliberately small Prometheus registration surface shared
// by Consensa components. Labels are bounded identifiers rather than unbounded keys so
// observability cannot become a memory leak under client traffic.
package metrics
