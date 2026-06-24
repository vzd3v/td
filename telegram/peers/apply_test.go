package peers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgmock"
)

func TestManager_applyChats(t *testing.T) {
	ctx := context.Background()
	a := require.New(t)
	_, m := testManager(t)

	chats := []tg.ChatClass{
		&tg.ChatEmpty{ID: 1},
		&tg.Chat{ID: 2},
		&tg.ChatForbidden{ID: 3},
		&tg.Channel{ID: 4, AccessHash: 14},
		&tg.Channel{ID: 4, Min: true, AccessHash: 16},
		&tg.ChannelForbidden{ID: 5, AccessHash: 15},
		&tg.Channel{ID: 6, Min: true, AccessHash: 16},
	}

	// Ensure nil safety.
	a.NoError(m.applyChats(ctx, nil, nil, nil))
	a.NoError(m.applyChats(ctx, chats...))

	_, ok, err := m.storage.Find(ctx, Key{ID: 2, Prefix: chatsPrefix})
	a.NoError(err)
	a.True(ok)

	_, ok, err = m.storage.Find(ctx, Key{ID: 3, Prefix: chatsPrefix})
	a.NoError(err)
	a.True(ok)

	v, ok, err := m.storage.Find(ctx, Key{ID: 4, Prefix: channelPrefix})
	a.NoError(err)
	a.True(ok)
	a.Equal(int64(14), v.AccessHash)

	v, ok, err = m.storage.Find(ctx, Key{ID: 5, Prefix: channelPrefix})
	a.NoError(err)
	a.True(ok)
	a.Equal(int64(15), v.AccessHash)

	// A channel observed ONLY as min must never be persisted: a min access hash
	// is valid solely for inputPeerPhotoFileLocation and later fails normal RPCs
	// such as channels.getFullChannel. (ID 4 above covers the overwrite case
	// where a full hash already exists; this covers first observation.)
	_, ok, err = m.storage.Find(ctx, Key{ID: 6, Prefix: channelPrefix})
	a.NoError(err)
	a.False(ok)
}

func TestManager_EntityObserver(t *testing.T) {
	ctx := context.Background()
	a := require.New(t)
	mock := tgmock.New(t)
	observer := &recordingEntityObserver{}
	m := Options{
		Cache:    &InmemoryCache{},
		Observer: observer,
	}.Build(tg.NewClient(mock))

	users := []tg.UserClass{
		&tg.UserEmpty{ID: 1},
		&tg.User{ID: 2, AccessHash: 12},
		&tg.User{ID: 3, Min: true, AccessHash: 13},
	}
	chats := []tg.ChatClass{
		&tg.ChatEmpty{ID: 4},
		&tg.Chat{ID: 5},
		&tg.ChatForbidden{ID: 6},
		&tg.Channel{ID: 7, AccessHash: 17},
		&tg.Channel{ID: 8, Min: true, AccessHash: 18},
		&tg.ChannelForbidden{ID: 9, AccessHash: 19},
	}

	a.NoError(m.Apply(ctx, users, chats))

	a.Len(observer.users, len(users))
	a.Same(users[0], observer.users[0])
	a.Same(users[1], observer.users[1])
	a.Same(users[2], observer.users[2])

	a.Len(observer.chats, len(chats))
	a.Same(chats[0], observer.chats[0])
	a.Same(chats[1], observer.chats[1])
	a.Same(chats[2], observer.chats[2])
	a.Same(chats[3], observer.chats[3])
	a.Same(chats[4], observer.chats[4])
	a.Same(chats[5], observer.chats[5])
}

type recordingEntityObserver struct {
	users []tg.UserClass
	chats []tg.ChatClass
}

func (r *recordingEntityObserver) ObserveUsers(ctx context.Context, users ...tg.UserClass) error {
	r.users = append(r.users, users...)
	return nil
}

func (r *recordingEntityObserver) ObserveChats(ctx context.Context, chats ...tg.ChatClass) error {
	r.chats = append(r.chats, chats...)
	return nil
}
