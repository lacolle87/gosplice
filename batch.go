package gosplice

import (
	"context"
	"time"
)

type BatchConfig struct {
	Size    int
	MaxWait time.Duration
}

func PipeBatch[T any](p *Pipeline[T], cfg BatchConfig) *Pipeline[[]T] {
	src := p.source
	hooks := p.hooks
	done := false

	if cfg.MaxWait > 0 {
		return pipeBatchWithTimeout(src, hooks, cfg, p.ctx, p.cancel, p.ctxNoop)
	}

	return &Pipeline[[]T]{
		source: &funcSource[[]T]{fn: func() ([]T, bool) {
			if done {
				return nil, false
			}
			batch := make([]T, 0, cfg.Size)
			for len(batch) < cfg.Size {
				v, ok := src.Next()
				if !ok {
					done = true
					break
				}
				hooks.fireElement(v)
				batch = append(batch, v)
			}
			if len(batch) == 0 {
				return nil, false
			}
			hooks.fireBatch(batch)
			return batch, true
		}},
		hooks:   newHooks[[]T](),
		ctx:     p.ctx,
		cancel:  p.cancel,
		ctxNoop: p.ctxNoop,
	}
}

func pipeBatchWithTimeout[T any](src Source[T], hooks *Hooks[T], cfg BatchConfig, ctx context.Context, cancel context.CancelFunc, ctxNoop bool) *Pipeline[[]T] {
	outCh := make(chan []T, 4)

	go func() {
		defer close(outCh)
		batch := make([]T, 0, cfg.Size)
		timer := time.NewTimer(cfg.MaxWait)
		defer timer.Stop()

		loopCtx := ctx
		if loopCtx == nil {
			loopCtx = context.Background()
		}

		itemCh := make(chan T)
		go func() {
			defer close(itemCh)
			for {
				v, ok := src.Next()
				if !ok {
					return
				}
				select {
				case itemCh <- v:
				case <-loopCtx.Done():
					return
				}
			}
		}()

		flush := func() {
			if len(batch) > 0 {
				out := make([]T, len(batch))
				copy(out, batch)
				hooks.fireBatch(out)
				outCh <- out
				batch = batch[:0]
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(cfg.MaxWait)
		}

		for {
			select {
			case <-loopCtx.Done():
				flush()
				return
			case v, ok := <-itemCh:
				if !ok {
					flush()
					return
				}
				hooks.fireElement(v)
				batch = append(batch, v)
				if len(batch) >= cfg.Size {
					flush()
				}
			case <-timer.C:
				flush()
			}
		}
	}()

	p := FromChannel(outCh)
	p.ctx = ctx
	p.cancel = cancel
	p.ctxNoop = ctxNoop
	return p
}
