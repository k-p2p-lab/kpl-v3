package controller

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

// Domain source handlers also serve the Agent network transport. Convert only
// at private-process boundaries, so bulk bytes bypass JSON/Base64 on the pipes.
func workerBinarySourceResult(value any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	chunk, ok := value.(workerSourceBytes)
	if !ok || len(chunk.Data) > workerSourceChunk {
		return nil, errors.New("invalid analysis source response")
	}
	return workerproc.Binary{Body: workerSourceBytes{EOF: chunk.EOF}, Data: chunk.Data}, nil
}

func workerBinarySourceHandler(source workerproc.Handler) workerproc.Handler {
	return func(ctx context.Context, method string, body json.RawMessage) (any, error) {
		if len(workerproc.RequestData(ctx)) != 0 {
			return nil, errors.New("source read does not accept a binary request")
		}
		return workerBinarySourceResult(source(ctx, method, body))
	}
}

func decodeAnalysisOutputChunk(ctx context.Context, raw json.RawMessage) (analysisOutputChunk, error) {
	var chunk analysisOutputChunk
	if err := json.Unmarshal(raw, &chunk); err != nil {
		return chunk, err
	}
	if len(chunk.Data) != 0 {
		return chunk, errors.New("analysis output bytes must use the binary payload")
	}
	chunk.Data = workerproc.RequestData(ctx)
	if len(chunk.Data) == 0 || len(chunk.Data) > workerSourceChunk/2 {
		return chunk, errors.New("invalid analysis output binary payload")
	}
	return chunk, nil
}
