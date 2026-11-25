package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/bnb-chain/bsc-mev-sentry/log"
	"github.com/bnb-chain/bsc-mev-sentry/metrics"
	"github.com/bnb-chain/bsc-mev-sentry/node"
	"github.com/bnb-chain/bsc-mev-sentry/service"
)

// Result classifies one sync attempt. It is also the metrics label.
type Result string

const (
	ResultApplied   Result = "applied"   // registry changed; allowlist replaced
	ResultUnchanged Result = "unchanged" // same fingerprint as last time
	ResultEmpty     Result = "empty"     // waiting for the first non-empty snapshot
	ResultError     Result = "error"     // read failed; previous allowlist kept
)

// Syncer periodically reads the registry and pushes the merged allowlist to the
// sentry. After the first non-empty snapshot, every successful read is authoritative.
type Syncer struct {
	reader   Reader
	interval time.Duration
	extra    []node.BuilderConfig
	contract common.Address

	builders *service.BuilderSet
	status   *service.RegistryState

	last  *Snapshot
	retry time.Duration

	ready     chan struct{}
	readyOnce sync.Once
}

// NewSyncer wires a reader to the sentry's allowlist and status holder.
func NewSyncer(cfg *Config, reader Reader, builders *service.BuilderSet, status *service.RegistryState) *Syncer {
	s := &Syncer{
		reader:   reader,
		interval: cfg.pollInterval(),
		extra:    cfg.ExtraBuilders,
		contract: cfg.ContractAddress,
		builders: builders,
		status:   status,
		ready:    make(chan struct{}),
	}
	s.retry = s.minRetry()
	return s
}

// Ready is closed after the first non-empty registry snapshot is installed.
func (s *Syncer) Ready() <-chan struct{} { return s.ready }

// Run syncs immediately, then schedules each next read by the last result
// until ctx is done.
func (s *Syncer) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(s.nextDelay(s.SyncOnce(ctx)))
		}
	}
}

// nextDelay backs off after failures, polls quickly while waiting for the initial list, and
// otherwise waits the configured interval.
func (s *Syncer) nextDelay(r Result) time.Duration {
	if r == ResultError {
		d := s.retry
		s.retry = min(2*s.retry, min(MaxRetryInterval, s.interval))
		return d
	}
	s.retry = s.minRetry()
	if r == ResultEmpty {
		return min(EmptyPollInterval, s.interval)
	}
	return s.interval
}

func (s *Syncer) minRetry() time.Duration { return min(MinRetryInterval, s.interval) }

// SyncOnce reads the registry once and applies it if it changed.
func (s *Syncer) SyncOnce(ctx context.Context) Result {
	snap, err := s.reader.Fetch(ctx)
	if err != nil {
		// Full error (may include the node URL) goes to the log only; the public
		// status RPC gets the class.
		log.Warnw("registry sync failed; keeping current allowlist", "err", err)
		s.status.RecordError(ErrorClass(err))
		return s.record(ResultError)
	}

	if s.last == nil && len(snap.Builders) == 0 {
		log.Warnw("empty builder registry; waiting for a non-empty snapshot", "block", snap.BlockNumber, "contract", s.contract)
		s.status.RecordError("empty_registry")
		return s.record(ResultEmpty)
	}

	result := ResultUnchanged
	if s.last == nil || s.last.Fingerprint != snap.Fingerprint {
		s.applyList(snap)
		result = ResultApplied
	}
	s.last = snap

	s.status.RecordSuccess(snap.BlockNumber, snap.BlockHash, snap.FetchedAt, snap.Fingerprint, s.builders.Len())
	metrics.RegistrySyncedBlock.Set(float64(snap.BlockNumber))
	metrics.RegistryBuilderCount.Set(float64(s.builders.Len()))
	s.readyOnce.Do(func() { close(s.ready) })
	return s.record(result)
}

func (s *Syncer) applyList(snap *Snapshot) {
	effective := Merge(snap.Builders, s.extra)
	s.builders.Replace(effective)
	var previous []Builder
	if s.last != nil {
		previous = s.last.Builders
	}
	added, removed := Diff(previous, snap.Builders)
	log.Infow("registry allowlist applied",
		"block", snap.BlockNumber,
		"fingerprint", snap.Fingerprint.TerminalString(),
		"registry", len(snap.Builders),
		"effective", len(effective),
		"added", summarize(added),
		"removed", summarize(removed))
}

func (s *Syncer) record(r Result) Result {
	metrics.RegistrySyncTotal.WithLabelValues(string(r)).Inc()
	if r == ResultApplied || r == ResultUnchanged {
		metrics.RegistryLastSuccess.SetToCurrentTime()
	}
	return r
}

// summarize keeps change logs readable: the initial load adds every key at once,
// which is not worth printing address by address.
func summarize(addrs []common.Address) any {
	const maxListed = 10
	if len(addrs) <= maxListed {
		return addrs
	}
	return fmt.Sprintf("%d addresses (first %d: %v)", len(addrs), maxListed, addrs[:maxListed])
}

// ErrorClass maps a fetch error to a short, URL-free label safe to expose over RPC.
func ErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNoCode):
		return "no_code_at_address"
	case errors.Is(err, ErrDecode):
		return "decode_failed"
	case errors.Is(err, ErrNotRegistry):
		return "not_a_registry"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, ErrResolveBlock):
		return "resolve_block_failed"
	case errors.Is(err, ErrCall):
		return "eth_call_failed"
	default:
		return "fetch_failed"
	}
}
