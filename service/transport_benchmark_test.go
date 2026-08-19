package service

import (
	"context"
	"encoding/json"
	"math/big"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	gincontrib "github.com/gin-gonic/contrib/gzip"
	"github.com/gin-gonic/gin"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	buildertypes "github.com/ethereum/go-ethereum/core/types/builder"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	ginutils "github.com/bnb-chain/bsc-mev-sentry/gin"
	"github.com/bnb-chain/bsc-mev-sentry/node"
	"github.com/ethereum/go-ethereum/core/types/builder/mevpb"
)

var benchmarkEncodedBytes []byte

type benchmarkBidBlockPayload struct {
	name  string
	block *buildertypes.BidBlock
}

const benchmarkTransactionGas = 25_096 // 21k base + 256 non-zero calldata bytes

// addBenchmarkTransactions appends structurally valid, signed legacy
// transaction envelopes. Set either count or targetBytes, leaving the other
// zero. Nonces are unique within each fixture.
func addBenchmarkTransactions(tb testing.TB, block *buildertypes.BidBlock, count, targetBytes int) {
	tb.Helper()
	if (count == 0) == (targetBytes == 0) {
		tb.Fatal("set exactly one transaction target")
	}

	keyBytes := make([]byte, 32)
	keyBytes[len(keyBytes)-1] = 1
	txKey, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		tb.Fatal(err)
	}
	signer := types.NewEIP155Signer(big.NewInt(56))
	to := common.HexToAddress("0x0000000000000000000000000000000000001000")
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i%251 + 1)
	}

	block.Transactions = nil
	var encodedBytes int
	for nonce := uint64(0); ; nonce++ {
		if count > 0 && len(block.Transactions) >= count {
			break
		}
		if targetBytes > 0 && encodedBytes >= targetBytes {
			break
		}
		tx := types.NewTransaction(nonce, to, big.NewInt(1), benchmarkTransactionGas, big.NewInt(1_000_000_000), data)
		signed, err := types.SignTx(tx, signer, txKey)
		if err != nil {
			tb.Fatal(err)
		}
		encoded, err := signed.MarshalBinary()
		if err != nil {
			tb.Fatal(err)
		}
		block.Transactions = append(block.Transactions, encoded)
		encodedBytes += len(encoded)
	}
	block.Header.GasUsed = uint64(len(block.Transactions)) * benchmarkTransactionGas
}

func benchmarkBidBlock(tb testing.TB, txCount, blobCount int) *buildertypes.BidBlock {
	tb.Helper()
	block := sampleBidBlock()
	block.Header.Difficulty = big.NewInt(1)
	addBenchmarkTransactions(tb, block, txCount, 0)
	if blobCount > 0 {
		block.Sidecars = append(block.Sidecars, blobSidecar(blobCount))
	}
	return block
}

// benchmarkBidBlockPayloads covers ordinary and blob-heavy production shapes.
// The final fixture remains close to the measured 1.7 MB RLP case.
func benchmarkBidBlockPayloads(tb testing.TB) []benchmarkBidBlockPayload {
	tb.Helper()

	blobHeavy := sampleBidBlock()
	blobHeavy.Header.Difficulty = big.NewInt(1)
	blobHeavy.Sidecars = append(blobHeavy.Sidecars, blobSidecar(6))
	addBenchmarkTransactions(tb, blobHeavy, 0, 900*1024)

	return []benchmarkBidBlockPayload{
		{name: "200tx", block: benchmarkBidBlock(tb, 200, 0)},
		{name: "150tx_3blob", block: benchmarkBidBlock(tb, 150, 3)},
		{name: "6blob_1.7MB", block: blobHeavy},
	}
}

// BenchmarkBidBlockEncoding compares the work a builder performs before the
// request reaches the transport. JSON-RPC marshals the object graph directly;
// gRPC first RLP-encodes BidBlock and then protobuf-encodes the request.
func BenchmarkBidBlockEncoding(b *testing.B) {
	key, err := crypto.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}

	for _, payload := range benchmarkBidBlockPayloads(b) {
		signature, err := crypto.Sign(payload.block.Hash().Bytes(), key)
		if err != nil {
			b.Fatal(err)
		}
		jsonArgs := BidBlockArgsWrapper{
			BidBlockArgs: buildertypes.BidBlockArgs{
				BidBlock:  payload.block,
				Signature: signature,
			},
			ValidatorHostName: "val-1",
		}
		jsonPayload, err := json.Marshal(jsonArgs)
		if err != nil {
			b.Fatal(err)
		}
		rlpPayload, err := rlp.EncodeToBytes(payload.block)
		if err != nil {
			b.Fatal(err)
		}
		grpcRequest := &mevpb.BidBlockRequest{
			BidBlockRlp:       rlpPayload,
			Signature:         signature,
			ValidatorHostName: "val-1",
		}
		grpcPayload, err := proto.Marshal(grpcRequest)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(payload.name+"/JSON", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(jsonPayload)))
			for b.Loop() {
				benchmarkEncodedBytes, err = json.Marshal(jsonArgs)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(jsonPayload)), "payload_B/op")
		})

		b.Run(payload.name+"/gRPC_RLP+protobuf", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(grpcPayload)))
			for b.Loop() {
				encoded, encodeErr := rlp.EncodeToBytes(payload.block)
				if encodeErr != nil {
					b.Fatal(encodeErr)
				}
				benchmarkEncodedBytes, err = proto.Marshal(&mevpb.BidBlockRequest{
					BidBlockRlp:       encoded,
					Signature:         signature,
					ValidatorHostName: "val-1",
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(grpcPayload)), "payload_B/op")
		})
	}
}

// benchmarkValidator is deliberately stateless so the transport benchmark
// measures sentry ingress rather than downstream Validator latency.
type benchmarkValidator struct{ mockValidator }

func (*benchmarkValidator) SendBidBlock(_ context.Context, _ buildertypes.BidBlockArgs, _ common.Address, bidHash common.Hash) (common.Hash, error) {
	return bidHash, nil
}

// TestLargeBidBlockTransportFixture prevents the large benchmark case from
// silently drifting into an unrealistic payload. It verifies transaction
// decoding and both transport-native roundtrips before the mock forwarding
// boundary. Synthetic blob proofs are intentionally not consensus-verified.
func TestLargeBidBlockTransportFixture(t *testing.T) {
	payloads := benchmarkBidBlockPayloads(t)
	require.Len(t, payloads[0].block.Transactions, 200)
	require.Empty(t, payloads[0].block.Sidecars)
	require.Len(t, payloads[1].block.Transactions, 150)
	require.Len(t, payloads[1].block.Sidecars, 1)
	require.Len(t, payloads[1].block.Sidecars[0].Blobs, 3)
	block := payloads[2].block

	rlpPayload, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rlpPayload), 1_650_000)
	require.LessOrEqual(t, len(rlpPayload), 1_800_000)

	decodedArgs := buildertypes.BidBlockArgs{BidBlock: new(buildertypes.BidBlock)}
	require.NoError(t, rlp.DecodeBytes(rlpPayload, decodedArgs.BidBlock))
	decodedTransactions, err := decodedArgs.DecodeTxs()
	require.NoError(t, err)
	require.Len(t, decodedTransactions, len(block.Transactions))

	builderKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	builder := crypto.PubkeyToAddress(builderKey.PublicKey)
	signature, err := crypto.Sign(block.Hash().Bytes(), builderKey)
	require.NoError(t, err)
	sentry := NewMevSentry(
		&Config{RPCTimeout: Duration(0)},
		map[string]node.Validator{"val-1": &benchmarkValidator{}},
		map[common.Address]node.Builder{builder: nil},
	)

	jsonArgs := BidBlockArgsWrapper{
		BidBlockArgs:      buildertypes.BidBlockArgs{BidBlock: block, Signature: signature},
		ValidatorHostName: "val-1",
	}
	jsonPayload, err := json.Marshal(jsonArgs)
	require.NoError(t, err)
	require.Greater(t, len(jsonPayload), len(rlpPayload)*19/10)
	var jsonRoundTrip BidBlockArgsWrapper
	require.NoError(t, json.Unmarshal(jsonPayload, &jsonRoundTrip))
	jsonHash, err := sentry.SendBidBlock(context.Background(), jsonRoundTrip)
	require.NoError(t, err)

	grpcRequest := &mevpb.BidBlockRequest{
		BidBlockRlp:       rlpPayload,
		Signature:         signature,
		ValidatorHostName: "val-1",
	}
	protobufPayload, err := proto.Marshal(grpcRequest)
	require.NoError(t, err)
	t.Logf("large BidBlock: txs=%d blobs=%d rlp=%dB protobuf=%dB json=%dB",
		len(block.Transactions), len(block.Sidecars[0].Blobs), len(rlpPayload), len(protobufPayload), len(jsonPayload))
	var protobufRoundTrip mevpb.BidBlockRequest
	require.NoError(t, proto.Unmarshal(protobufPayload, &protobufRoundTrip))
	grpcResponse, err := (&BidBlockServer{sentry: sentry}).SendBidBlock(context.Background(), &protobufRoundTrip)
	require.NoError(t, err)

	require.Equal(t, block.Hash(), jsonHash)
	require.Equal(t, block.Hash(), common.BytesToHash(grpcResponse.BidHash))
	require.Less(t, len(protobufPayload), len(jsonPayload)*6/10)
}

type bidBlockBenchmarkHarness struct {
	jsonClient *rpc.Client
	httpServer *httptest.Server
	grpcClient mevpb.BidBlockServiceClient
	grpcConn   *grpc.ClientConn
	grpcServer *GRPCService
}

func newBidBlockBenchmarkHarness(b *testing.B, builder common.Address) *bidBlockBenchmarkHarness {
	b.Helper()

	sharedSem := ginutils.NewConcurrencySem(1_024)
	sentry := NewMevSentry(
		&Config{RPCTimeout: Duration(0), GRPCConcurrency: 32},
		map[string]node.Validator{"val-1": &benchmarkValidator{}},
		map[common.Address]node.Builder{builder: nil},
	)

	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("mev", sentry); err != nil {
		b.Fatal(err)
	}
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(
		ginutils.ConcurrencyLimiterWith(sharedSem),
		ginutils.PanicRecovery(),
		gincontrib.Gzip(gincontrib.DefaultCompression),
	)
	router.POST("/", gin.WrapH(rpcServer))
	httpListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	httpServer := httptest.NewUnstartedServer(router)
	httpServer.Listener = httpListener
	httpServer.Start()
	jsonClient, err := rpc.DialHTTP(httpServer.URL)
	if err != nil {
		httpServer.Close()
		b.Fatal(err)
	}

	grpcServer, err := StartGRPCServer("127.0.0.1:0", sentry, sharedSem)
	if err != nil {
		jsonClient.Close()
		httpServer.Close()
		b.Fatal(err)
	}
	grpcConn, err := grpc.NewClient(grpcServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Shutdown(time.Second)
		jsonClient.Close()
		httpServer.Close()
		b.Fatal(err)
	}

	h := &bidBlockBenchmarkHarness{
		jsonClient: jsonClient,
		httpServer: httpServer,
		grpcClient: mevpb.NewBidBlockServiceClient(grpcConn),
		grpcConn:   grpcConn,
		grpcServer: grpcServer,
	}
	b.Cleanup(func() {
		grpcConn.Close()
		grpcServer.Shutdown(time.Second)
		jsonClient.Close()
		httpServer.Close()
		rpcServer.Stop()
	})
	return h
}

// BenchmarkBidBlockTransport exercises the production-shaped local transports:
// Gin + JSON-RPC over HTTP and grpc-go over HTTP/2, including their middleware,
// client encoding, unmarshalling, signature recovery, routing and a stateless
// Validator call.
// Connections and payload fixtures are created and warmed before timing.
func BenchmarkBidBlockTransport(b *testing.B) {
	key, err := crypto.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	builder := crypto.PubkeyToAddress(key.PublicKey)
	harness := newBidBlockBenchmarkHarness(b, builder)

	for _, payload := range benchmarkBidBlockPayloads(b) {
		signature, err := crypto.Sign(payload.block.Hash().Bytes(), key)
		if err != nil {
			b.Fatal(err)
		}
		jsonArgs := BidBlockArgsWrapper{
			BidBlockArgs: buildertypes.BidBlockArgs{
				BidBlock:  payload.block,
				Signature: signature,
			},
			ValidatorHostName: "val-1",
		}
		jsonPayload, err := json.Marshal(jsonArgs)
		if err != nil {
			b.Fatal(err)
		}
		rlpPayload, err := rlp.EncodeToBytes(payload.block)
		if err != nil {
			b.Fatal(err)
		}
		grpcRequest := &mevpb.BidBlockRequest{
			BidBlockRlp:       rlpPayload,
			Signature:         signature,
			ValidatorHostName: "val-1",
		}
		grpcPayload, err := proto.Marshal(grpcRequest)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(payload.name+"/JSON-RPC", func(b *testing.B) {
			var result common.Hash
			if err := harness.jsonClient.CallContext(context.Background(), &result, "mev_sendBidBlock", jsonArgs); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(jsonPayload)))
			b.ResetTimer()
			for b.Loop() {
				if err := harness.jsonClient.CallContext(context.Background(), &result, "mev_sendBidBlock", jsonArgs); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(jsonPayload)), "payload_B/op")
		})

		b.Run(payload.name+"/gRPC", func(b *testing.B) {
			if _, err := harness.grpcClient.SendBidBlock(context.Background(), grpcRequest); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(grpcPayload)))
			b.ResetTimer()
			for b.Loop() {
				encoded, err := rlp.EncodeToBytes(payload.block)
				if err != nil {
					b.Fatal(err)
				}
				request := &mevpb.BidBlockRequest{
					BidBlockRlp:       encoded,
					Signature:         signature,
					ValidatorHostName: "val-1",
				}
				if _, err := harness.grpcClient.SendBidBlock(context.Background(), request); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(grpcPayload)), "payload_B/op")
		})
	}
}
