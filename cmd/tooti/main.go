// Package main is the root command entrypoint for the tooti node and tooling.
// Binaries and subcommands will be expanded in later phases.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mrostamii/tooti/pkg/apiv1"
	"github.com/mrostamii/tooti/pkg/backend/ollama"
	"github.com/mrostamii/tooti/pkg/config"
	"github.com/mrostamii/tooti/pkg/dataplane"
	"github.com/mrostamii/tooti/pkg/gateway"
	"github.com/mrostamii/tooti/pkg/node"
	"github.com/mrostamii/tooti/pkg/registry"
	"github.com/mrostamii/tooti/pkg/x402client"
	"github.com/mrostamii/tooti/pkg/x402spike"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("tooti")
		fmt.Println("usage: tooti config-check -file ./node.yaml")
		fmt.Println("usage: tooti pay chat -url http://127.0.0.1:8080/v1/chat/completions [-api-key <prepaid>] -model qwen2.5:3b -message \"say hi\"")
		fmt.Println("usage: tooti pay topup -gateway http://127.0.0.1:8080 -amount-usdc 5")
		return
	}

	switch os.Args[1] {
	case "config-check":
		runConfigCheck(os.Args[2:])
	case "node":
		runNode(os.Args[2:])
	case "network":
		runNetwork(os.Args[2:])
	case "gateway":
		runGateway(os.Args[2:])
	case "pay":
		runPay(os.Args[2:])
	default:
		fmt.Printf("unknown command: %s\n", os.Args[1])
		os.Exit(2)
	}
}

func runConfigCheck(args []string) {
	fs := flag.NewFlagSet("config-check", flag.ExitOnError)
	file := fs.String("file", "./node.yaml", "path to node.yaml")
	_ = fs.Parse(args)

	cfg, err := config.Load(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config invalid: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("config valid")
	fmt.Printf("node=%s tcp=%d quic=%d backend=%s models=%d\n",
		cfg.Node.Name,
		cfg.Listen.TCPPort,
		cfg.Listen.QUICPort,
		cfg.Backend.BaseURL,
		len(cfg.Models.Advertised),
	)
}

func runNode(args []string) {
	if len(args) == 0 {
		fmt.Println("usage: tooti node <start|status> -file ./node.yaml")
		os.Exit(2)
	}
	switch args[0] {
	case "start":
		runNodeStart(args[1:])
	case "status":
		runNodeStatus(args[1:])
	default:
		fmt.Printf("unknown node command: %s\n", args[0])
		os.Exit(2)
	}
}

func runNodeStart(args []string) {
	fs := flag.NewFlagSet("node start", flag.ExitOnError)
	file := fs.String("file", "./node.yaml", "path to node.yaml")
	_ = fs.Parse(args)

	cfg, err := config.Load(*file)
	if err != nil {
		log.Fatalf("config invalid: %v", err)
	}
	if err := config.EnsureTCPAddrAvailable(fmt.Sprintf("0.0.0.0:%d", cfg.Listen.TCPPort)); err != nil {
		log.Fatalf("preflight failed for listen.tcp_port: %v", err)
	}
	if err := config.EnsureUDPAddrAvailable(fmt.Sprintf("0.0.0.0:%d", cfg.Listen.QUICPort)); err != nil {
		log.Fatalf("preflight failed for listen.quic_port: %v", err)
	}
	if cfg.Metrics.Enabled {
		if err := config.EnsureTCPAddrAvailable(cfg.Metrics.Listen); err != nil {
			log.Fatalf("preflight failed for metrics.listen: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cli := ollama.New(cfg.Backend.BaseURL)
	if err := cli.HealthCheck(ctx); err != nil {
		log.Fatalf("backend health check failed: %v", err)
	}

	available, err := cli.ListModels(ctx)
	if err != nil {
		log.Fatalf("list backend models failed: %v", err)
	}

	log.Printf("node start: name=%s tcp=%d quic=%d backend=%s", cfg.Node.Name, cfg.Listen.TCPPort, cfg.Listen.QUICPort, cfg.Backend.BaseURL)
	hw := node.DetectHardware()
	log.Printf("hardware: os=%s arch=%s gpu=%s ram_bytes=%d vram_bytes=%d", hw.OS, hw.Arch, hw.GPU, hw.RAMBytes, hw.VRAMBytes)
	log.Printf("backend models: %v", available)
	for _, want := range cfg.Models.Advertised {
		if !slices.Contains(available, want) {
			log.Printf("warning: advertised model %q not found in backend /api/tags", want)
		}
	}
	rt, err := node.Start(ctx, cfg)
	if err != nil {
		log.Fatalf("node runtime failed to start: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			log.Printf("node shutdown warning: %v", err)
		}
	}()

	log.Printf("node is ready (libp2p + dht + backend); press Ctrl+C to stop")

	<-ctx.Done()
	log.Printf("shutdown signal received; exiting")
}

func runNodeStatus(args []string) {
	fs := flag.NewFlagSet("node status", flag.ExitOnError)
	file := fs.String("file", "./node.yaml", "path to node.yaml")
	_ = fs.Parse(args)

	cfg, err := config.Load(*file)
	if err != nil {
		log.Fatalf("config invalid: %v", err)
	}

	cli := ollama.New(cfg.Backend.BaseURL)
	err = cli.HealthCheck(context.Background())
	health := "ok"
	if err != nil {
		health = "error: " + err.Error()
	}

	fmt.Printf("node=%s\n", cfg.Node.Name)
	fmt.Printf("backend=%s\n", cfg.Backend.BaseURL)
	fmt.Printf("health=%s\n", health)
	fmt.Printf("advertised_models=%v\n", cfg.Models.Advertised)
	hw := node.DetectHardware()
	fmt.Printf("os=%s\n", hw.OS)
	fmt.Printf("arch=%s\n", hw.Arch)
	fmt.Printf("gpu=%s\n", hw.GPU)
	fmt.Printf("ram_bytes=%d\n", hw.RAMBytes)
	fmt.Printf("vram_bytes=%d\n", hw.VRAMBytes)
}

func runGateway(args []string) {
	if len(args) == 0 {
		fmt.Println("usage: tooti gateway start [-file ./node.yaml] [-listen 127.0.0.1:8080] [-ollama http://127.0.0.1:11434]")
		os.Exit(2)
	}
	switch args[0] {
	case "start":
		runGatewayStart(args[1:])
	default:
		fmt.Printf("unknown gateway command: %s\n", args[0])
		os.Exit(2)
	}
}

func runGatewayStart(args []string) {
	fs := flag.NewFlagSet("gateway start", flag.ExitOnError)
	file := fs.String("file", "./node.yaml", "path to node.yaml")
	listen := fs.String("listen", "", "gateway listen address override")
	ollamaBase := fs.String("ollama", "", "ollama base URL override (optional)")
	localBackend := fs.Bool("local-backend", false, "enable local Ollama fallback in gateway (default false: remote-only routing)")
	x402Mode := fs.String("x402-mode", "off", "deprecated; chat x402 removed — use prepaid API keys and x402 top-up only")
	x402Enable := fs.Bool("x402-enable", false, "enable x402 payment requirement for /v1/chat/completions")
	x402Facilitator := fs.String("x402-facilitator", "", "x402 facilitator base URL (e.g. https://x402.org/facilitator)")
	x402Network := fs.String("x402-network", "eip155:84532", "x402 network in CAIP-2 format")
	x402Asset := fs.String("x402-asset", "0x036CbD53842c5426634e7929541eC2318f3dCF7e", "x402 payment asset address")
	_ = fs.String("x402-amount", "10000", "x402 payment amount in token atomic units")
	x402PricePer1K := fs.Int64("x402-price-per-1k", 0, "dynamic pricing: token atomic units per 1K estimated tokens (0 disables dynamic pricing)")
	x402MinAmount := fs.Int64("x402-min-amount", 1000, "dynamic pricing: minimum charge per request in token atomic units")
	x402DefaultOutputTokens := fs.Int64("x402-default-output-tokens", 256, "dynamic pricing: fallback output token estimate when request.max_tokens is absent")
	x402PayTo := fs.String("x402-payto", "", "x402 recipient wallet address")
	x402TokenName := fs.String("x402-token-name", "USDC", "x402 token name for EIP-712 domain")
	x402TokenVersion := fs.String("x402-token-version", "2", "x402 token version for EIP-712 domain")
	_ = fs.Parse(args)

	cfg, err := config.Load(*file)
	if err != nil {
		log.Fatalf("config invalid: %v", err)
	}

	resolvedListen := cfg.Gateway.Listen
	if *listen != "" {
		resolvedListen = *listen
	}
	resolvedOllama := cfg.Backend.BaseURL
	if *ollamaBase != "" {
		resolvedOllama = *ollamaBase
	}
	if err := config.EnsureTCPAddrAvailable(resolvedListen); err != nil {
		log.Fatalf("preflight failed for gateway listen: %v", err)
	}
	if err := config.EnsureTCPAddrAvailable(fmt.Sprintf("0.0.0.0:%d", cfg.Listen.TCPPort)); err != nil {
		log.Fatalf("preflight failed for listen.tcp_port: %v", err)
	}
	if err := config.EnsureUDPAddrAvailable(fmt.Sprintf("0.0.0.0:%d", cfg.Listen.QUICPort)); err != nil {
		log.Fatalf("preflight failed for listen.quic_port: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hb := time.Duration(cfg.Heartbeat.IntervalSec) * time.Second
	reg := registry.New(hb)
	rt, err := node.StartObserving(ctx, cfg, reg.ApplyHealthJSON)
	if err != nil {
		log.Fatalf("gateway p2p observe failed: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			log.Printf("gateway p2p shutdown warning: %v", err)
		}
	}()

	pruneEvery := hb
	if pruneEvery < 5*time.Second {
		pruneEvery = 5 * time.Second
	}
	go func() {
		t := time.NewTicker(pruneEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n := reg.PruneStale(); n > 0 {
					log.Printf("network registry: pruned %d stale node(s)", n)
				}
			}
		}
	}()
	modelSyncEvery := hb
	if modelSyncEvery < 10*time.Second {
		modelSyncEvery = 10 * time.Second
	}
	go func() {
		syncOnce := func() {
			queryCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()

			avail, err := rt.ListModelAvailability(queryCtx, cfg.Models.Advertised, 32)
			if err != nil {
				log.Printf("network registry: model sync failed: %v", err)
				return
			}
			knownList := reg.List()
			known := make(map[string]struct{}, len(knownList))
			for _, rec := range knownList {
				known[rec.NodeID] = struct{}{}
			}
			modelMap := buildKnownNodeModelMap(avail, known)
			nowMS := time.Now().UnixMilli()
			for nodeID, models := range modelMap {
				if err := reg.ApplyNodeAnnounceProto(&apiv1.NodeAnnounce{
					NodeId:      nodeID,
					Models:      models,
					TimestampMs: nowMS,
				}); err != nil {
					log.Printf("network registry: model merge failed for %s: %v", nodeID, err)
				}
			}
		}

		syncOnce()
		t := time.NewTicker(modelSyncEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				syncOnce()
			}
		}
	}()

	log.Printf("gateway start: listen=%s ollama=%s p2p=tcp/%d quic/%d (health topic %q)",
		resolvedListen, resolvedOllama, cfg.Listen.TCPPort, cfg.Listen.QUICPort, node.HealthTopicID)
	proxy := gateway.NewOpenAIProxy(resolvedListen, resolvedOllama, reg)
	gwID := strings.TrimSpace(cfg.Gateway.ID)
	if gwID == "" {
		gwID = resolvedListen
	}
	proxy.SetGatewayID(gwID)
	proxy.SetGatewayMode(cfg.Gateway.Mode)
	proxy.SetControlAPIToken(cfg.Gateway.ControlAPIToken)
	proxy.SetAuthMode(cfg.Gateway.AuthMode)
	proxy.SetCORSAllowedOrigins(cfg.Gateway.CORSAllowedOrigins)
	resolvedX402PayTo := strings.TrimSpace(*x402PayTo)
	if resolvedX402PayTo == "" {
		resolvedX402PayTo = strings.TrimSpace(cfg.Node.X402.PayTo)
	}
	modelPricingCfg := cfg.Models.ModelPricing
	if len(modelPricingCfg) == 0 {
		// Backward compatibility for older configs.
		modelPricingCfg = cfg.Gateway.X402.ModelPricing
	}
	prepaidModelPricing := make(map[string]gateway.X402TokenPricingConfig, len(modelPricingCfg))
	for model, mp := range modelPricingCfg {
		prepaidModelPricing[model] = gateway.X402TokenPricingConfig{
			AtomicPer1KTokens:   mp.PricePer1KAtomic,
			MinAmountAtomic:     mp.MinAmountAtomic,
			MaxAmountAtomic:     mp.MaxAmountAtomic,
			DefaultOutputTokens: mp.DefaultOutputTokens,
		}
	}
	prepaidTokenPricing := &gateway.X402TokenPricingConfig{
		AtomicPer1KTokens:   10000, // 0.01 USDC per 1K tokens
		MinAmountAtomic:     1000,  // 0.001 USDC floor per request
		DefaultOutputTokens: 256,
	}
	if *x402PricePer1K > 0 {
		prepaidTokenPricing.AtomicPer1KTokens = *x402PricePer1K
	}
	if *x402MinAmount > 0 {
		prepaidTokenPricing.MinAmountAtomic = *x402MinAmount
	}
	if *x402DefaultOutputTokens > 0 {
		prepaidTokenPricing.DefaultOutputTokens = *x402DefaultOutputTokens
	}
	proxy.SetPrepaidPricing(prepaidTokenPricing, prepaidModelPricing)
	if strings.EqualFold(strings.TrimSpace(cfg.Gateway.Mode), "official") {
		store, err := dataplane.OpenPostgresStore(
			cfg.Gateway.Postgres.DSN,
			cfg.Gateway.Postgres.MaxOpenConns,
			cfg.Gateway.Postgres.MaxIdleConns,
			cfg.Gateway.Postgres.ConnMaxLifetimeSec,
		)
		if err != nil {
			log.Fatalf("official gateway postgres init failed: %v", err)
		}
		defer func() {
			if err := store.Close(); err != nil {
				log.Printf("postgres close warning: %v", err)
			}
		}()
		proxy.SetControlStore(store)
		log.Printf("official gateway control store enabled (postgres)")
		if resolvedX402PayTo == "" {
			log.Printf("x402 prepaid topup unavailable: set -x402-payto or node.x402.pay_to")
		} else {
			proxy.SetX402PrepaidTopupPaywall(&gateway.X402PaywallConfig{
				FacilitatorURL: strings.TrimSpace(*x402Facilitator),
				Requirement: x402spike.PaymentRequirements{
					Scheme:            "exact",
					Network:           strings.TrimSpace(*x402Network),
					Amount:            "1000000",
					Asset:             strings.TrimSpace(*x402Asset),
					PayTo:             resolvedX402PayTo,
					MaxTimeoutSeconds: 60,
					Extra: map[string]any{
						"name":    strings.TrimSpace(*x402TokenName),
						"version": strings.TrimSpace(*x402TokenVersion),
					},
				},
			})
			log.Printf("x402 prepaid topup enabled network=%s asset=%s payto=%s facilitator=%s",
				strings.TrimSpace(*x402Network), strings.TrimSpace(*x402Asset), resolvedX402PayTo, strings.TrimSpace(*x402Facilitator))
		}
	} else {
		log.Printf("community gateway mode (control store disabled)")
	}
	proxy.SetLocalBackendEnabled(*localBackend)
	proxy.SetTimeouts(
		time.Duration(cfg.Timeouts.FirstTokenSec)*time.Second,
		time.Duration(cfg.Timeouts.TotalRequestSec)*time.Second,
	)
	mode := strings.ToLower(strings.TrimSpace(*x402Mode))
	if mode == "" {
		mode = "off"
	}
	if *x402Enable || mode == "managed" {
		log.Printf("warning: per-request chat x402 (-x402-enable / -x402-mode=managed) is removed; billing is prepaid API keys + x402 top-up only")
	}
	if mode != "" && mode != "off" {
		log.Printf("warning: ignoring -x402-mode=%q (chat paywall disabled)", *x402Mode)
	}
	log.Printf("gateway chat billing: prepaid API keys on official gateways; x402 only on /v1/prepaid/topup when configured")
	if *localBackend {
		log.Printf("gateway local backend fallback enabled (mode=hybrid)")
	} else {
		log.Printf("gateway local backend fallback disabled (mode=remote-only)")
	}
	proxy.SetRemoteChatFunc(buildRemoteChatFunc(rt, cfg))
	proxy.SetRemoteStreamChatFunc(buildRemoteStreamChatFunc(rt, cfg))
	proxy.SetPeerLatencyFunc(func(ctx context.Context, nodeID string) (time.Duration, error) {
		targetID, err := peer.Decode(nodeID)
		if err != nil {
			return 0, err
		}
		return rt.PingPeer(ctx, targetID)
	})
	if err := proxy.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("gateway failed: %v", err)
	}
	log.Printf("gateway stopped")
}

func buildRemoteChatFunc(rt *node.Runtime, cfg *config.Config) gateway.RemoteChatFunc {
	return func(ctx context.Context, nodeID string, req *gateway.RemoteChatRequest) (*gateway.RemoteChatResponse, error) {
		if req == nil {
			return nil, fmt.Errorf("remote chat request is nil")
		}
		targetID, err := peer.Decode(nodeID)
		if err != nil {
			return nil, fmt.Errorf("decode target peer id: %w", err)
		}
		requestID := strings.TrimSpace(req.RequestID)
		if requestID == "" {
			requestID = fmt.Sprintf("req-%d", time.Now().UnixNano())
		}
		wireReq := &apiv1.InferenceRequest{
			RequestId: requestID,
			Model:     req.Model,
			Messages:  make([]*apiv1.ChatMessage, 0, len(req.Messages)),
			Params:    map[string]string{},
		}
		for _, m := range req.Messages {
			wireReq.Messages = append(wireReq.Messages, &apiv1.ChatMessage{
				Role:    m.Role,
				Content: m.Content,
			})
		}
		if req.Temperature != nil {
			wireReq.Params["temperature"] = fmt.Sprintf("%g", *req.Temperature)
		}
		if req.MaxTokens != nil && *req.MaxTokens > 0 {
			wireReq.Params["max_tokens"] = fmt.Sprintf("%d", *req.MaxTokens)
		}
		if s := strings.TrimSpace(req.PaymentSignature); s != "" {
			wireReq.Params["payment_signature"] = s
		}
		if s := strings.TrimSpace(req.ResourceURL); s != "" {
			wireReq.Params["x402_resource_url"] = s
		}

		// Fast path: try direct stream first to avoid per-request DHT lookups.
		inferCtx, cancelInfer := context.WithTimeout(ctx, time.Duration(cfg.Timeouts.TotalRequestSec)*time.Second)
		out, err := rt.InferRemote(inferCtx, targetID, wireReq)
		cancelInfer()
		if err != nil {
			var payErr *node.PaymentRequiredError
			if errors.As(err, &payErr) {
				return nil, &gateway.RemotePaymentRequiredError{
					Message:               payErr.Message,
					PaymentRequiredHeader: payErr.PaymentRequiredHeader,
					PaymentResponseHeader: payErr.PaymentResponseHeader,
				}
			}
			// Fallback: resolve peer addrs via DHT, connect, then retry once.
			providers, findErr := rt.FindModelProviders(ctx, req.Model, 32)
			if findErr != nil {
				return nil, fmt.Errorf("remote inference failed (%v); find model providers: %w", err, findErr)
			}
			var target *peer.AddrInfo
			for i := range providers {
				if providers[i].ID == targetID {
					target = &providers[i]
					break
				}
			}
			if target == nil {
				return nil, fmt.Errorf("target provider %s unavailable for model %q", nodeID, req.Model)
			}
			dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
			if err := rt.ConnectPeer(dialCtx, *target); err != nil {
				cancelDial()
				return nil, fmt.Errorf("connect target provider: %w", err)
			}
			cancelDial()
			retryCtx, cancelRetry := context.WithTimeout(ctx, time.Duration(cfg.Timeouts.TotalRequestSec)*time.Second)
			out, err = rt.InferRemote(retryCtx, targetID, wireReq)
			cancelRetry()
			if err != nil {
				var payErr *node.PaymentRequiredError
				if errors.As(err, &payErr) {
					return nil, &gateway.RemotePaymentRequiredError{
						Message:               payErr.Message,
						PaymentRequiredHeader: payErr.PaymentRequiredHeader,
						PaymentResponseHeader: payErr.PaymentResponseHeader,
					}
				}
				return nil, fmt.Errorf("remote inference retry failed: %w", err)
			}
		}
		return &gateway.RemoteChatResponse{
			Model:            req.Model,
			Content:          out.GetContent(),
			CompletionTokens: out.GetTokensUsed(),
		}, nil
	}
}

type readCloserWithCancel struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *readCloserWithCancel) Close() error {
	r.cancel()
	return r.ReadCloser.Close()
}

func buildRemoteStreamChatFunc(rt *node.Runtime, cfg *config.Config) gateway.RemoteStreamChatFunc {
	return func(ctx context.Context, nodeID string, req *gateway.RemoteChatRequest) (io.ReadCloser, error) {
		if req == nil {
			return nil, fmt.Errorf("remote chat request is nil")
		}
		targetID, err := peer.Decode(nodeID)
		if err != nil {
			return nil, fmt.Errorf("decode target peer id: %w", err)
		}
		requestID := strings.TrimSpace(req.RequestID)
		if requestID == "" {
			requestID = fmt.Sprintf("req-%d", time.Now().UnixNano())
		}
		wireReq := &apiv1.InferenceRequest{
			RequestId: requestID,
			Model:     req.Model,
			Messages:  make([]*apiv1.ChatMessage, 0, len(req.Messages)),
			Params:    map[string]string{},
		}
		for _, m := range req.Messages {
			wireReq.Messages = append(wireReq.Messages, &apiv1.ChatMessage{
				Role:    m.Role,
				Content: m.Content,
			})
		}
		if req.Temperature != nil {
			wireReq.Params["temperature"] = fmt.Sprintf("%g", *req.Temperature)
		}
		if req.MaxTokens != nil && *req.MaxTokens > 0 {
			wireReq.Params["max_tokens"] = fmt.Sprintf("%d", *req.MaxTokens)
		}
		if s := strings.TrimSpace(req.PaymentSignature); s != "" {
			wireReq.Params["payment_signature"] = s
		}
		if s := strings.TrimSpace(req.ResourceURL); s != "" {
			wireReq.Params["x402_resource_url"] = s
		}

		streamCtx, cancelStream := context.WithTimeout(ctx, time.Duration(cfg.Timeouts.TotalRequestSec)*time.Second)
		rc, err := rt.InferRemoteStream(streamCtx, targetID, wireReq)
		if err == nil {
			return &readCloserWithCancel{ReadCloser: rc, cancel: cancelStream}, nil
		}
		cancelStream()

		providers, findErr := rt.FindModelProviders(ctx, req.Model, 32)
		if findErr != nil {
			return nil, fmt.Errorf("remote stream failed (%v); find model providers: %w", err, findErr)
		}
		var target *peer.AddrInfo
		for i := range providers {
			if providers[i].ID == targetID {
				target = &providers[i]
				break
			}
		}
		if target == nil {
			return nil, fmt.Errorf("target provider %s unavailable for model %q", nodeID, req.Model)
		}
		dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
		if err := rt.ConnectPeer(dialCtx, *target); err != nil {
			cancelDial()
			return nil, fmt.Errorf("connect target provider: %w", err)
		}
		cancelDial()

		retryCtx, cancelRetry := context.WithTimeout(ctx, time.Duration(cfg.Timeouts.TotalRequestSec)*time.Second)
		retryRC, err := rt.InferRemoteStream(retryCtx, targetID, wireReq)
		if err != nil {
			cancelRetry()
			return nil, fmt.Errorf("remote stream retry failed: %w", err)
		}
		return &readCloserWithCancel{ReadCloser: retryRC, cancel: cancelRetry}, nil
	}
}

func buildKnownNodeModelMap(avail []node.ModelAvailability, known map[string]struct{}) map[string][]string {
	modelSetByNode := make(map[string]map[string]struct{})
	for _, entry := range avail {
		model := strings.TrimSpace(entry.Model)
		if model == "" {
			continue
		}
		for _, provider := range entry.Providers {
			nodeID := provider.ID.String()
			if nodeID == "" {
				continue
			}
			if _, ok := known[nodeID]; !ok {
				continue
			}
			if _, ok := modelSetByNode[nodeID]; !ok {
				modelSetByNode[nodeID] = map[string]struct{}{}
			}
			modelSetByNode[nodeID][model] = struct{}{}
		}
	}

	out := make(map[string][]string, len(modelSetByNode))
	for nodeID, set := range modelSetByNode {
		models := make([]string, 0, len(set))
		for model := range set {
			models = append(models, model)
		}
		sort.Strings(models)
		out[nodeID] = models
	}
	return out
}

func runNetwork(args []string) {
	if len(args) == 0 {
		fmt.Println("usage: tooti network <peers|models> -file ./node.yaml")
		os.Exit(2)
	}
	switch args[0] {
	case "peers":
		runNetworkPeers(args[1:])
	case "models":
		runNetworkModels(args[1:])
	default:
		fmt.Printf("unknown network command: %s\n", args[0])
		os.Exit(2)
	}
}

func loadQueryRuntime(ctx context.Context, file string) (*node.Runtime, *config.Config, error) {
	cfg, err := config.Load(file)
	if err != nil {
		return nil, nil, err
	}
	queryCfg := *cfg
	queryCfg.Listen.TCPPort = 0
	queryCfg.Listen.QUICPort = 0
	queryCfg.Metrics.Enabled = false
	rt, err := node.StartQueryOnly(ctx, &queryCfg)
	if err != nil {
		return nil, nil, err
	}
	return rt, cfg, nil
}

func runNetworkPeers(args []string) {
	fs := flag.NewFlagSet("network peers", flag.ExitOnError)
	file := fs.String("file", "./node.yaml", "path to node.yaml")
	timeout := fs.Duration("timeout", 12*time.Second, "max discovery time")
	limit := fs.Int("limit", 32, "max providers per model lookup")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	rt, cfg, err := loadQueryRuntime(ctx, *file)
	if err != nil {
		log.Fatalf("network peers runtime start failed: %v", err)
	}
	defer func() {
		_ = rt.Close()
	}()
	rt.ConnectBootstrapsOnce(ctx)
	time.Sleep(2 * time.Second)

	peers := rt.ConnectedPeers()
	avail, err := rt.ListModelAvailability(ctx, cfg.Models.Advertised, *limit)
	if err != nil {
		log.Fatalf("network peers model lookup failed: %v", err)
	}
	modelsByPeer := map[string][]string{}
	for _, m := range avail {
		for _, p := range m.Providers {
			modelsByPeer[p.ID.String()] = append(modelsByPeer[p.ID.String()], m.Model)
		}
	}
	fmt.Printf("peers=%d\n", len(peers))
	for _, p := range peers {
		addrStrs := make([]string, 0, len(p.Addrs))
		for _, a := range p.Addrs {
			addrStrs = append(addrStrs, a.String())
		}
		peerModels := modelsByPeer[p.ID.String()]
		if len(peerModels) == 0 {
			fmt.Printf("- id=%s addrs=%s models=[]\n", p.ID, strings.Join(addrStrs, ","))
			continue
		}
		fmt.Printf("- id=%s addrs=%s models=%v\n", p.ID, strings.Join(addrStrs, ","), peerModels)
	}
}

func runNetworkModels(args []string) {
	fs := flag.NewFlagSet("network models", flag.ExitOnError)
	file := fs.String("file", "./node.yaml", "path to node.yaml")
	timeout := fs.Duration("timeout", 15*time.Second, "max discovery time")
	limit := fs.Int("limit", 32, "max providers per model")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	rt, cfg, err := loadQueryRuntime(ctx, *file)
	if err != nil {
		log.Fatalf("network models runtime start failed: %v", err)
	}
	defer func() {
		_ = rt.Close()
	}()
	rt.ConnectBootstrapsOnce(ctx)
	time.Sleep(2 * time.Second)

	avail, err := rt.ListModelAvailability(ctx, cfg.Models.Advertised, *limit)
	if err != nil {
		log.Fatalf("network models query failed: %v", err)
	}
	for _, m := range avail {
		fmt.Printf("- model=%s providers=%d\n", m.Model, m.ProviderCount)
	}
}

func runPay(args []string) {
	if len(args) == 0 {
		fmt.Println("usage: tooti pay chat -url http://127.0.0.1:8080/v1/chat/completions [-api-key <prepaid>] -model qwen2.5:3b -message \"say hi\"")
		fmt.Println("usage: tooti pay topup -gateway http://127.0.0.1:8080 -amount-usdc 5")
		fmt.Println("usage: tooti pay balance -gateway http://127.0.0.1:8080")
		fmt.Println("usage: tooti pay rotate-key -gateway http://127.0.0.1:8080")
		os.Exit(2)
	}
	if amount, ok := parsePayAmountShortcut(args[0]); ok {
		runPayTopup(append([]string{"-amount-usdc", fmt.Sprintf("%.6f", amount)}, args[1:]...))
		return
	}
	switch args[0] {
	case "chat":
		runPayChat(args[1:])
	case "topup":
		runPayTopup(args[1:])
	case "balance":
		runPayBalance(args[1:])
	case "rotate-key":
		runPayRotateKey(args[1:])
	default:
		fmt.Printf("unknown pay command: %s\n", args[0])
		os.Exit(2)
	}
}

func runPayChat(args []string) {
	fs := flag.NewFlagSet("pay chat", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:8080/v1/chat/completions", "chat completions endpoint URL")
	apiKeyFlag := fs.String("api-key", "", "prepaid API key (Bearer); defaults to ~/.tooti/pay-state.json profile for -url host")
	model := fs.String("model", "qwen2.5:3b", "model name")
	message := fs.String("message", "say hi", "user message content")
	stream := fs.Bool("stream", true, "request streaming response")
	maxTokens := fs.Int("max-tokens", 0, "optional max_tokens sent to provider for pricing/output cap")
	privateKey := fs.String("private-key", "", "optional private key override")
	_ = fs.Parse(args)

	apiKey := prepaidAPIKeyForChatURL(*url, *apiKeyFlag)

	body := map[string]any{
		"model":  *model,
		"stream": *stream,
		"messages": []map[string]string{
			{"role": "user", "content": *message},
		},
	}
	if *maxTokens > 0 {
		body["max_tokens"] = *maxTokens
	}
	raw, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("request marshal error: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, *url, bytes.NewReader(raw))
	if err != nil {
		log.Fatalf("request create error: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	var resp *http.Response
	if apiKey != "" {
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("request failed: %v", err)
		}
	} else {
		client, werr := x402client.NewFromEnv()
		if werr != nil {
			log.Fatalf("wallet error: %v (set EVM_PRIVATE_KEY for x402 chat, or use -api-key / pay topup for prepaid)", werr)
		}
		if strings.TrimSpace(*privateKey) != "" {
			client.PrivateKey = strings.TrimSpace(*privateKey)
		}
		resp, err = client.DoWithPayment(req)
		if err != nil {
			log.Fatalf("request failed: %v", err)
		}
	}
	defer resp.Body.Close()

	if settleHeader := resp.Header.Get("PAYMENT-RESPONSE"); strings.TrimSpace(settleHeader) != "" {
		var settle x402spike.SettlementResponse
		if err := x402spike.DecodeBase64JSON(settleHeader, &settle); err == nil {
			settleRaw, _ := json.MarshalIndent(settle, "", "  ")
			log.Printf("payment settlement: %s", settleRaw)
		}
	}
	if reqID := strings.TrimSpace(resp.Header.Get("X-Tooti-Request-ID")); reqID != "" {
		fmt.Fprintf(os.Stderr, "request_id=%s\n", reqID)
	}

	if *stream {
		if resp.StatusCode >= 400 {
			respBody, _ := io.ReadAll(resp.Body)
			log.Fatalf("status=%d body=%s", resp.StatusCode, string(respBody))
		}
		if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
			log.Fatalf("stream copy error: %v", err)
		}
		return
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("read response error: %v", err)
	}
	if resp.StatusCode >= 400 {
		log.Fatalf("status=%d body=%s", resp.StatusCode, string(respBody))
	}
	_, _ = os.Stdout.Write(respBody)
}
