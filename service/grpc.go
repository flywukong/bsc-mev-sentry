package service

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	buildertypes "github.com/ethereum/go-ethereum/core/types/builder"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/bnb-chain/bsc-mev-sentry/log"
	"github.com/bnb-chain/bsc-mev-sentry/metrics"
	mevpb "github.com/bnb-chain/bsc-mev-sentry/proto"
)

// maxGRPCMsgSize bounds one request while admitting every legal BidBlock:
// the RLP body alone may reach params.MaxBlockSize (8MiB, Osaka+), and blob
// sidecars ride on top of it. LB-level connection and rate limits are still
// required.
const maxGRPCMsgSize = 2 * params.MaxBlockSize

// maxGRPCConcurrentStreams bounds pre-interceptor decoding per connection.
const maxGRPCConcurrentStreams = 32

// relayMethodPrefix excludes health and reflection from business throttling.
const relayMethodPrefix = "/mev.v1.BuilderRelay/"

// grpcSendBidBlockMetric keeps handler and interceptor metrics consistent.
const grpcSendBidBlockMetric = "grpc_mev_sendBidBlock"

// grpcMethodLabel maps a gRPC full method name to its metric label.
func grpcMethodLabel(fullMethod string) string {
	if fullMethod == mevpb.BuilderRelay_SendBidBlock_FullMethodName {
		return grpcSendBidBlockMetric
	}
	return fullMethod
}

// BuilderRelayServer receives RLP-encoded BidBlocks over gRPC.
type BuilderRelayServer struct {
	mevpb.UnimplementedBuilderRelayServer
	sentry *MevSentry
}

// SendBidBlock decodes the RLP BidBlock and hands it to the same core logic
// as the JSON-RPC handler (ecrecover, allowlist, routing, forwarding).
func (b *BuilderRelayServer) SendBidBlock(ctx context.Context, req *mevpb.BidBlockRequest) (resp *mevpb.BidBlockResponse, err error) {
	method := grpcSendBidBlockMetric
	start := time.Now()
	defer recordLatency(method, start)
	defer timeoutCancel(&ctx, b.sentry.timeout)()
	// Count the original business code before converting it to gRPC status.
	defer func() {
		if err != nil {
			orig := err
			final := toGRPCStatus(orig)
			if status.Code(final) == codes.Internal {
				log.Errorw("grpc send bid block failed", "err", orig)
			}
			err = final
			metrics.ApiErrorCounter.WithLabelValues(method, errorCodeLabel(orig, final)).Inc()
		}
	}()

	// gRPC has no HTTP Host fallback, so routing must be explicit.
	host := strings.TrimSpace(req.ValidatorHostName)
	if host == "" {
		return nil, buildertypes.NewInvalidBidError("validator_host_name is required")
	}

	var bidBlock buildertypes.BidBlock
	decodeStart := time.Now()
	if err := rlp.DecodeBytes(req.BidBlockRlp, &bidBlock); err != nil {
		log.Errorw("failed to decode bid block rlp", "err", err)
		return nil, buildertypes.NewInvalidBidError("invalid BidBlock rlp")
	}
	decodeElapsed := time.Since(decodeStart)

	args := BidBlockArgsWrapper{
		BidBlockArgs: buildertypes.BidBlockArgs{
			BidBlock:  &bidBlock,
			Signature: req.Signature,
		},
		ValidatorHostName: host,
	}

	bidHash, err := b.sentry.sendBidBlock(ctx, args)
	if err != nil {
		return nil, err // raw business error; the defer above converts + counts
	}

	log.Debugw("[BID BLOCK GRPC]",
		"block", bidBlock.Header.Number,
		"hash", bidHash.TerminalString(),
		"txs", len(bidBlock.Transactions),
		"sidecars", len(bidBlock.Sidecars),
		"payloadKB", len(req.BidBlockRlp)/1024,
		"decodeMs", decodeElapsed.Milliseconds(),
		"totalMs", time.Since(start).Milliseconds())
	return &mevpb.BidBlockResponse{BidHash: bidHash.Bytes()}, nil
}

// toGRPCStatus maps MEV errors and preserves their code in ErrorInfo.
func toGRPCStatus(err error) error {
	if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
		return err // already a grpc status (e.g. from validation above)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}

	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) {
		// Do not expose internal validator errors.
		return status.Error(codes.Internal, "internal error")
	}

	var code codes.Code
	switch rpcErr.ErrorCode() {
	case buildertypes.InvalidBidParamError, buildertypes.InvalidPayBidTxError,
		buildertypes.BidBlockPreSealVerifyError:
		code = codes.InvalidArgument
	case buildertypes.MevNotRunningError:
		code = codes.Unavailable
	case buildertypes.MevBusyError:
		code = codes.ResourceExhausted
	case buildertypes.MevNotInTurnError:
		code = codes.FailedPrecondition
	case buildertypes.BidBlockPermissionRevokedError:
		code = codes.PermissionDenied
	case buildertypes.BidBlockTooLateError:
		code = codes.DeadlineExceeded
	default:
		return status.Error(codes.Internal, "internal error")
	}

	st := status.New(code, err.Error())
	if detailed, derr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: strconv.Itoa(rpcErr.ErrorCode()),
		Domain: "mev.bnbchain.org",
	}); derr == nil {
		st = detailed
	}
	return st.Err()
}

// errorCodeLabel prefers the original MEV code for metrics.
func errorCodeLabel(orig, final error) string {
	var rpcErr rpc.Error
	if errors.As(orig, &rpcErr) {
		return strconv.Itoa(rpcErr.ErrorCode())
	}
	return status.Code(final).String()
}

// recoveryInterceptor converts handler panics to Internal errors.
// It must be the outermost interceptor and never logs request payloads.
func recoveryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorw("grpc handler panic", "method", info.FullMethod, "panic", r,
				"stack", string(debug.Stack()))
			metrics.ApiErrorCounter.WithLabelValues(grpcMethodLabel(info.FullMethod), "panic").Inc()
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(ctx, req)
}

// concurrencyInterceptor bounds BuilderRelay requests with two semaphores:
// grpcSem caps the memory-heavy gRPC path on its own, and sharedSem is the
// process-wide RPCConcurrency budget also held by the JSON path. Non-business
// RPCs bypass both.
//
// It does NOT queue (unlike the gin middleware): a request that cannot take a
// slot is rejected at once. Waiting would park a decoded message per waiter
// with no bound on the number of waiters, and would spend the bid's deadline
// before any work starts — a queued bid is usually past bidMustBefore anyway.
// Failing fast keeps peak memory a function of the two caps alone and lets the
// builder react in time.
func concurrencyInterceptor(grpcSem, sharedSem chan struct{}) grpc.UnaryServerInterceptor {
	reject := func(fullMethod string) error {
		err := status.Error(codes.ResourceExhausted, "concurrency limit reached")
		metrics.ApiErrorCounter.WithLabelValues(grpcMethodLabel(fullMethod), status.Code(err).String()).Inc()
		return err
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {
		if !strings.HasPrefix(info.FullMethod, relayMethodPrefix) {
			return handler(ctx, req)
		}
		// Tighter gRPC-specific cap first, so overload sheds here.
		if grpcSem != nil {
			select {
			case grpcSem <- struct{}{}:
				defer func() { <-grpcSem }()
			default:
				return nil, reject(info.FullMethod)
			}
		}
		if sharedSem != nil {
			select {
			case sharedSem <- struct{}{}:
				defer func() { <-sharedSem }()
			default:
				return nil, reject(info.FullMethod)
			}
		}
		return handler(ctx, req)
	}
}

// GRPCService owns the BuilderRelay server lifecycle.
type GRPCService struct {
	srv    *grpc.Server
	health *health.Server
	addr   string
}

// Addr returns the bound listen address (useful with ":0" in tests).
func (g *GRPCService) Addr() string { return g.addr }

// Shutdown flips health to NOT_SERVING so LBs drain first, then waits up to
// timeout for in-flight RPCs before forcing the server down.
func (g *GRPCService) Shutdown(timeout time.Duration) {
	g.health.Shutdown()
	done := make(chan struct{})
	go func() {
		g.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		g.srv.Stop()
	}
}

// StartGRPCServer starts BuilderRelay beside JSON-RPC. sharedSem is the
// process-wide RPCConcurrency semaphore (also used by the gin middleware);
// the gRPC path additionally takes its own GRPCConcurrency slot.
// sem is shared with Gin; nil disables process-wide throttling.
func StartGRPCServer(addr string, sentry *MevSentry, sharedSem chan struct{}) (*GRPCService, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("grpc listen on %s: %w", addr, err)
	}

	grpcConcurrency := sentry.grpcConcurrency
	if grpcConcurrency <= 0 {
		grpcConcurrency = defaultGRPCConcurrency
	}
	grpcSem := make(chan struct{}, grpcConcurrency)

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxGRPCMsgSize),
		grpc.MaxSendMsgSize(maxGRPCMsgSize),
		grpc.MaxConcurrentStreams(maxGRPCConcurrentStreams),
		// recovery outermost so it also covers the other interceptors.
		grpc.ChainUnaryInterceptor(recoveryInterceptor, concurrencyInterceptor(grpcSem, sharedSem)),
	}
	srv := grpc.NewServer(opts...)
	mevpb.RegisterBuilderRelayServer(srv, &BuilderRelayServer{sentry: sentry})

	// Also support probes configured with the service name.
	hs := health.NewServer()
	hs.SetServingStatus(mevpb.BuilderRelay_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)

	g := &GRPCService{srv: srv, health: hs, addr: lis.Addr().String()}
	go func() {
		log.Infow("grpc builder relay listening", "addr", g.addr)
		if err := srv.Serve(lis); err != nil {
			// Keep JSON-RPC alive but fail gRPC health checks.
			hs.Shutdown()
			log.Errorw("grpc server stopped", "err", err)
		}
	}()
	return g, nil
}
