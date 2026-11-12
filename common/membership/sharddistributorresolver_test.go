// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package membership

import (
	"testing"

	"github.com/cadence-workflow/shard-manager/service/sharddistributor/client/spectatorclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/log"
)

func TestShardDistributorResolver_Lookup(t *testing.T) {
	resolver, _, shardDistributorMock := newShardDistributorResolver(t)

	shardDistributorMock.EXPECT().GetShardOwner(gomock.Any(), "test-shard").
		Return(&spectatorclient.ShardOwner{
			ExecutorID: "test-owner",
			Metadata: map[string]string{
				"hostIP":   "127.0.0.1",
				"tchannel": "7933",
				"grpc":     "7833",
			},
		}, nil)

	host, err := resolver.Lookup("test-shard")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:7933", host.addr)
	assert.Equal(t, "test-owner", host.Identity())
}

func TestShardDistributorResolver_Lookup_NilSpectatorFails(t *testing.T) {
	ring := NewMockSingleProvider(gomock.NewController(t))
	resolver := NewShardDistributorResolver(nil, ring, log.NewNoop())

	_, err := resolver.Lookup("test-shard")
	assert.ErrorContains(t, err, "no shard distributor client")
}

func TestShardDistributorResolver_Start(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().Start().Times(1)
	resolver.Start()
}

func TestShardDistributorResolver_Stop(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().Stop().Times(1)
	resolver.Stop()
}

func TestShardDistributorResolver_Subscribe(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().Subscribe("test-name", gomock.Any()).Times(1)
	resolver.Subscribe("test-name", nil)
}

func TestShardDistributorResolver_UnSubscribe(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().Unsubscribe("test-name").Times(1)
	resolver.Unsubscribe("test-name")
}

func TestShardDistributorResolver_Members(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().Members().Return(nil)
	resolver.Members()
}

func TestShardDistributorResolver_MemberCount(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().MemberCount().Return(0)
	resolver.MemberCount()
}

func TestShardDistributorResolver_Refresh(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().Refresh().Times(1)
	resolver.Refresh()
}

func TestShardDistributorResolver_AddressToHost(t *testing.T) {
	resolver, ring, _ := newShardDistributorResolver(t)
	ring.EXPECT().AddressToHost("test").Return(HostInfo{}, nil)
	resolver.AddressToHost("test")
}

func newShardDistributorResolver(t *testing.T) (*shardDistributorResolver, *MockSingleProvider, *spectatorclient.MockSpectator) {
	ctrl := gomock.NewController(t)
	spectator := spectatorclient.NewMockSpectator(ctrl)
	ring := NewMockSingleProvider(ctrl)

	resolver := NewShardDistributorResolver(spectator, ring, log.NewNoop()).(*shardDistributorResolver)

	return resolver, ring, spectator
}
