// Package sim supplies the deterministic test environment used by Consensa's
// distributed components. Its scheduler owns message delivery and logical time, so a
// seed describes a complete replayable execution rather than merely a set of random
// choices. Production networking intentionally does not live here.
package sim
