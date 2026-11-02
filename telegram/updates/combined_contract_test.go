package updates

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// These integration cases require the combined PR runtime, not feature detection.
func startCombinedContract(t *testing.T, m *Manager, api API) (cancel context.CancelFunc, stopped <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, api, 1, AuthOptions{}) }()
	synctest.Wait()
	return cancel, done
}

func requireCombinedStopped(t *testing.T, done <-chan error, want error) {
	t.Helper()
	select {
	case err := <-done:
		require.ErrorIs(t, err, want)
	default:
		t.Fatal("Run did not return the application error without external cancellation")
	}
}

func requireCombinedRunning(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("Run stopped unexpectedly: %v", err)
	default:
	}
}

func combinedEmptyDifference(context.Context, *tg.UpdatesGetDifferenceRequest) (tg.UpdatesDifferenceClass, error) {
	return &tg.UpdatesDifferenceEmpty{}, nil
}

func TestCombinedRecoveredFactErrorPolicy(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "Default"
		if stop {
			name = "Stop"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				storage, hashes := trackingFixture(t)
				failure := errors.New("recovered fact persistence failed")
				var handled atomic.Int32
				api := &trackingAPI{common: combinedEmptyDifference,
					channel: func(context.Context, *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
						return &tg.UpdatesChannelDifference{Final: true, Pts: 20, OtherUpdates: []tg.UpdateClass{
							&tg.UpdatePinnedChannelMessages{ChannelID: 41, Messages: []int{1}, Pts: 15},
						}}, nil
					},
				}
				m := New(Config{Storage: storage, AccessHasher: hashes, StopOnApplyError: stop,
					TrackChannel: func(int64) bool { return true },
					Handler: telegram.UpdateHandlerFunc(func(context.Context, tg.UpdatesClass) error {
						handled.Add(1)
						return failure
					}),
				})
				cancel, done := startCombinedContract(t, m, api)
				defer cancel()
				require.EqualValues(t, 1, handled.Load())
				wantPts := 20
				if stop {
					requireCombinedStopped(t, done, failure)
					wantPts = 10
				} else {
					requireCombinedRunning(t, done)
				}
				pts, found, err := storage.GetChannelPts(context.Background(), 1, 41)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, wantPts, pts)
			})
		})
	}
}

func TestCombinedRecoveredFactDrainsBeforeReenable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		storage, hashes := trackingFixture(t)
		var selected atomic.Bool
		selected.Store(true)
		var calls atomic.Int32
		requested := make(chan int, 4)
		release := make(chan struct{})
		var callbackContext context.Context
		api := &trackingAPI{common: combinedEmptyDifference,
			channel: func(_ context.Context, r *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
				requested <- r.Pts
				if calls.Add(1) == 1 {
					return &tg.UpdatesChannelDifference{Final: true, Pts: 20, OtherUpdates: []tg.UpdateClass{
						&tg.UpdatePinnedChannelMessages{ChannelID: 41, Messages: []int{1}, Pts: 15},
					}}, nil
				}
				return &tg.UpdatesChannelDifferenceEmpty{Final: true, Pts: r.Pts}, nil
			},
		}
		m := New(Config{Storage: storage, AccessHasher: hashes, StopOnApplyError: true,
			TrackChannel: func(int64) bool { return selected.Load() },
			Handler: telegram.UpdateHandlerFunc(func(ctx context.Context, _ tg.UpdatesClass) error {
				callbackContext = ctx
				select {
				case <-release:
					return ctx.Err()
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		})
		cancel, done := startCombinedContract(t, m, api)
		defer cancel()
		require.NotNil(t, callbackContext)
		require.Equal(t, 10, <-requested)
		selected.Store(false)
		require.NoError(t, m.RefreshChannelTracking(context.Background()))
		synctest.Wait()
		require.NoError(t, callbackContext.Err(), "policy cancellation must not cancel admitted recovered persistence")
		selected.Store(true)
		require.NoError(t, m.RefreshChannelTracking(context.Background()))
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load(), "replacement must wait for the admitted recovered fact")
		close(release)
		synctest.Wait()
		requireCombinedRunning(t, done)
		require.EqualValues(t, 2, calls.Load())
		require.Equal(t, 20, <-requested, "replacement must reread the committed response PTS")
		pts, _, err := storage.GetChannelPts(context.Background(), 1, 41)
		require.NoError(t, err)
		require.Equal(t, 20, pts)
	})
}

func TestCombinedResumeReadErrorPolicy(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "Default"
		if stop {
			name = "Stop"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				storage, hashes := trackingFixture(t)
				failure := errors.New("resume checkpoint unavailable")
				var selected atomic.Bool
				var reads, requests, failed atomic.Int32
				wrapped := &trackingStorage{StateStorage: storage,
					read: func(context.Context, int64, int64) (int, bool, error) {
						reads.Add(1)
						return 0, false, failure
					},
				}
				api := &trackingAPI{common: combinedEmptyDifference,
					channel: func(_ context.Context, r *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
						requests.Add(1)
						return &tg.UpdatesChannelDifferenceEmpty{Final: true, Pts: r.Pts}, nil
					},
				}
				m := New(Config{Storage: wrapped, AccessHasher: hashes, StopOnApplyError: stop,
					TrackChannel:             func(int64) bool { return selected.Load() },
					OnLoadChannelStateFailed: func(int64) { failed.Add(1) },
					Handler:                  telegram.UpdateHandlerFunc(func(context.Context, tg.UpdatesClass) error { return nil }),
				})
				cancel, done := startCombinedContract(t, m, api)
				defer cancel()
				selected.Store(true)
				require.NoError(t, m.RefreshChannelTracking(context.Background()))
				synctest.Wait()
				require.EqualValues(t, 1, reads.Load())
				require.Zero(t, requests.Load(), "failed resume must not start a difference from a guessed PTS")
				if stop {
					requireCombinedStopped(t, done, failure)
				} else {
					requireCombinedRunning(t, done)
					require.EqualValues(t, 1, failed.Load())
				}
				pts, _, err := storage.GetChannelPts(context.Background(), 1, 41)
				require.NoError(t, err)
				require.Equal(t, 10, pts)
			})
		})
	}
}

func TestCombinedFatalErrorDoesNotWaitForRetirementNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		storage, hashes := newMemStorage(), newMemAccessHasher()
		ctx := context.Background()
		require.NoError(t, storage.SetState(ctx, 1, State{}))
		// More quiet channels than the completion queue can hold. All complete
		// through their ordinary Run lifecycle; the test never fills that queue.
		for _, id := range []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 100} {
			require.NoError(t, storage.SetChannelPts(ctx, 1, id, 10))
			require.NoError(t, hashes.SetChannelAccessHash(ctx, 1, id, 123))
		}
		var selected atomic.Bool
		selected.Store(true)
		var ordinaryRequests atomic.Int32
		releaseFailure, commonEntered := make(chan struct{}), make(chan struct{})
		failure := errors.New("channel persistence failed")
		api := &trackingAPI{common: combinedEmptyDifference,
			channel: func(ctx context.Context, r *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
				if r.Channel.(*tg.InputChannel).ChannelID != 100 {
					ordinaryRequests.Add(1)
					return &tg.UpdatesChannelDifferenceEmpty{Final: true, Pts: r.Pts}, nil
				}
				select {
				case <-releaseFailure:
					return &tg.UpdatesChannelDifference{Final: true, Pts: 11,
						NewMessages: []tg.MessageClass{&tg.Message{ID: 1, PeerID: &tg.PeerChannel{ChannelID: 100}}}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}
		m := New(Config{Storage: storage, AccessHasher: hashes, StopOnApplyError: true,
			TrackChannel: func(id int64) bool { return id == 100 || selected.Load() },
			Handler: telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
				if _, common := u.(*tg.Updates).Updates[0].(*tg.UpdateConfig); common {
					close(commonEntered)
					<-ctx.Done() // A common operation waiting for account cancellation.
					return ctx.Err()
				}
				return failure
			}),
		})
		cancel, done := startCombinedContract(t, m, api)
		defer cancel()
		require.EqualValues(t, 11, ordinaryRequests.Load())
		require.NoError(t, m.Handle(ctx, &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateConfig{}}}))
		synctest.Wait()
		select {
		case <-commonEntered:
		default:
			t.Fatal("common callback was not admitted")
		}
		selected.Store(false)
		require.NoError(t, m.RefreshChannelTracking(ctx)) // Scheduling does not wait for the busy owner.
		time.Sleep(idleTimeout)
		synctest.Wait() // Quiet workers see deselection and complete while the owner is blocked.
		require.EqualValues(t, 11, ordinaryRequests.Load(), "deselected workers must not poll again")
		close(releaseFailure)
		synctest.Wait()
		requireCombinedStopped(t, done, failure)
		pts, _, err := storage.GetChannelPts(ctx, 1, 100)
		require.NoError(t, err)
		require.Equal(t, 10, pts)
	})
}
