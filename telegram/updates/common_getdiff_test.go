package updates

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"

	"github.com/gotd/log/logzap"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// TestCommonStateGapKeepsBufferedUpdate reproduces a common-state (private chat
// / basic group) pts gap that strands a buffered update.
//
// A live edit at pts 12 arrives while the state is 10, so it is buffered behind
// the gap at 11. A gap-triggered getDifference then replays the same edit in
// OtherUpdates and advances the state to 12. Because the replayed update is
// routed through the sequence box while the state is still 10, it is buffered
// again; the following setState(12) then makes it look outdated, and the box
// drops it instead of dispatching it.
func TestCommonStateGapKeepsBufferedUpdate(t *testing.T) {
	ctx := context.Background()
	const selfID = 123

	msg := &tg.Message{ID: 1, PeerID: &tg.PeerUser{UserID: 555}, FromID: &tg.PeerUser{UserID: 555}, Message: "v1", Date: 1, EditDate: 1}

	// getDifference replays the edit and advances the state to 12.
	api := &oneDiffAPI{diff: &tg.UpdatesDifference{
		OtherUpdates: []tg.UpdateClass{
			&tg.UpdateEditMessage{Message: msg, Pts: 12, PtsCount: 1},
		},
		State: tg.UpdatesState{Pts: 12, Date: 1},
	}}

	var dispatched []int
	handler := telegram.UpdateHandlerFunc(func(_ context.Context, u tg.UpdatesClass) error {
		if up, ok := u.(*tg.Updates); ok {
			for _, x := range up.Updates {
				if e, ok := x.(*tg.UpdateEditMessage); ok {
					dispatched = append(dispatched, e.Pts)
				}
				if d, ok := x.(*tg.UpdateDeleteMessages); ok {
					dispatched = append(dispatched, d.Pts)
				}
			}
		}
		return nil
	})

	storage := newMemStorage()
	require.NoError(t, storage.SetState(ctx, selfID, State{Pts: 10, Date: 1}))
	s := newState(ctx, stateConfig{
		State:            State{Pts: 10, Date: 1},
		RawClient:        api,
		Logger:           logzap.New(zaptest.NewLogger(t)),
		Tracer:           noop.NewTracerProvider().Tracer(""),
		Handler:          handler,
		OnChannelTooLong: func(int64) {},
		OnTooLong:        func() {},
		Storage:          storage,
		Hasher:           newMemAccessHasher(),
		UserHasher:       newMemUserAccessHasher(),
		SelfID:           selfID,
		DiffLimit:        diffLimitUser,
		WorkGroup:        &errgroup.Group{},
	})

	// Mark the sender known so the edit is not diverted to getDifference by the
	// peer-access-hash check.
	require.NoError(t, s.handleUpdates(ctx, &tg.Updates{
		Users: []tg.UserClass{&tg.User{ID: 555, AccessHash: 7777}},
	}, false))

	// Live edit at pts 12 while the state is 10: gap at 11, so it is buffered.
	require.NoError(t, s.handleUpdates(ctx, &tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateEditMessage{Message: msg, Pts: 12, PtsCount: 1},
	}}, false))
	require.Empty(t, dispatched, "the edit is buffered behind the gap, not dispatched yet")

	// A gap-triggered difference replays the edit and advances the state to 12.
	require.NoError(t, s.getDifference(ctx, "test"))

	require.Equal(t, []int{12}, dispatched, "the replayed edit must be delivered once")
	checkpoint, found, err := storage.GetState(ctx, selfID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 12, checkpoint.Pts)
	require.Equal(t, 12, s.pts.State())

	// Recovery must also leave the live sequence able to advance, without
	// delivering the buffered copy of the edit again.
	require.NoError(t, s.handleUpdates(ctx, &tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateDeleteMessages{Messages: []int{2}, Pts: 13, PtsCount: 1},
	}}, false))
	require.Equal(t, []int{12, 13}, dispatched)
	checkpoint, _, err = storage.GetState(ctx, selfID)
	require.NoError(t, err)
	require.Equal(t, 13, checkpoint.Pts)
	require.Equal(t, 13, s.pts.State())
}
