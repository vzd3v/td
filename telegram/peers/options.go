package peers

import (
	"github.com/gotd/log"
	"golang.org/x/sync/singleflight"

	"github.com/gotd/td/constant"
	"github.com/gotd/td/tg"
)

// Options is options of Manager
type Options struct {
	Storage  Storage
	Cache    Cache
	Observer EntityObserver
	Logger   log.Logger
}

func (o *Options) setDefaults() {
	if o.Storage == nil {
		o.Storage = &InmemoryStorage{}
	}
	if o.Cache == nil {
		o.Cache = NoopCache{}
	}
	if o.Observer == nil {
		o.Observer = NoopEntityObserver{}
	}
	if o.Logger == nil {
		o.Logger = log.Nop
	}
}

// Build creates new Manager.
func (o Options) Build(api *tg.Client) *Manager {
	o.setDefaults()
	return &Manager{
		api:      api,
		storage:  o.Storage,
		cache:    o.Cache,
		observer: o.Observer,
		me:       new(atomicUser),
		logger:   log.For(o.Logger),
		sg:       singleflight.Group{},
		needUpdate: peerIDSet{
			m: make(map[constant.TDLibPeerID]struct{}),
		},
		needUpdateFull: peerIDSet{
			m: make(map[constant.TDLibPeerID]struct{}),
		},
	}
}
