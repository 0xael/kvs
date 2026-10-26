// Package membership — detector.go implements a gRPC-based failure detector.
// It periodically health-checks every peer and updates the membership state.
package membership

import (
	"context"
	"log/slog"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Detector periodically probes peers via gRPC health checks and updates
// the membership health state accordingly.
type Detector struct {
	membership *Membership
	interval   time.Duration // time between probe rounds
	timeout    time.Duration // per-probe RPC deadline
	logger     *slog.Logger
}

// NewDetector creates a failure detector that probes all peers in the
// membership every interval using a per-probe timeout.
func NewDetector(m *Membership, interval, timeout time.Duration) *Detector {
	return &Detector{
		membership: m,
		interval:   interval,
		timeout:    timeout,
		logger:     slog.Default().With("component", "detector"),
	}
}

// Run starts the background detection loop. It blocks until ctx is cancelled.
func (d *Detector) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	d.logger.Info("failure detector started", "interval", d.interval, "timeout", d.timeout)

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("failure detector stopped")
			return
		case <-ticker.C:
			d.probeAll(ctx)
		}
	}
}

// probeAll checks every peer's health via the gRPC health service.
func (d *Detector) probeAll(ctx context.Context) {
	for _, peerID := range d.membership.PeerIDs() {
		conn, err := d.membership.GetConn(peerID)
		if err != nil {
			d.logger.Warn("cannot get conn for probe", "peer", peerID, "error", err)
			continue
		}

		probeCtx, cancel := context.WithTimeout(ctx, d.timeout)
		client := healthpb.NewHealthClient(conn)
		resp, err := client.Check(probeCtx, &healthpb.HealthCheckRequest{
			Service: "ghostkv.v1.InternalService",
		})
		cancel()

		wasHealthy := d.membership.IsHealthy(peerID)
		healthy := err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING

		if wasHealthy != healthy {
			d.logger.Info("peer health changed",
				"peer", peerID,
				"was_healthy", wasHealthy,
				"now_healthy", healthy,
			)
		}

		d.membership.SetHealthy(peerID, healthy)
	}
}
