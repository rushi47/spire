package awsiid

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	defaultNewClientCallback = newClient
)

type Client interface {
	ec2.DescribeInstancesAPIClient
	iam.GetInstanceProfileAPIClient
	organizations.ListAccountsAPIClient
}

type clientsCache struct {
	mtx            sync.RWMutex
	config         *SessionConfig
	clients        map[string]*cacheEntry
	orgAccountList map[string]string
	newClient      newClientCallback
}

type cacheEntry struct {
	lock   chan struct{}
	client Client
}

type newClientCallback func(ctx context.Context, config *SessionConfig, region string, assumeRoleARN string) (Client, error)

func newClientsCache(newClient newClientCallback) *clientsCache {
	return &clientsCache{
		clients:   make(map[string]*cacheEntry),
		newClient: newClient,
	}
}

func (cc *clientsCache) configure(config SessionConfig) {
	cc.mtx.Lock()
	cc.clients = make(map[string]*cacheEntry)
	cc.orgAccountList = make(map[string]string)
	cc.config = &config
	cc.mtx.Unlock()
}

func (cc *clientsCache) getClient(ctx context.Context, region, accountID string, assumeRole string) (Client, error) {
	// Do an initial check to see if p client for this region already exists
	cacheKey := accountID + "@" + region

	// Grab (or create) the cache for the region
	r := cc.getCachedClient(cacheKey)

	// Obtain the "lock" to the region cache
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r.lock <- struct{}{}:
	}

	// "clear" the lock when the function is complete
	defer func() {
		<-r.lock
	}()

	// If the client is populated, return it.
	if r.client != nil {
		return r.client, nil
	}

	if cc.config == nil {
		return nil, status.Error(codes.FailedPrecondition, "not configured")
	}

	// Override role from sessionconfig if sent as input : used current while getting org client
	var role string
	if assumeRole != "" {
		role = assumeRole
	} else if cc.config.AssumeRole != "" {
		role = cc.config.AssumeRole
	}

	assumeRoleArn := fmt.Sprintf("arn:%s:iam::%s:role/%s", cc.config.Partition, accountID, role)

	client, err := cc.newClient(ctx, cc.config, region, assumeRoleArn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create client: %v", err)
	}

	r.client = client
	return client, nil
}

func (cc *clientsCache) getCachedClient(cacheKey string) *cacheEntry {
	cc.mtx.Lock()
	defer cc.mtx.Unlock()
	r, ok := cc.clients[cacheKey]
	if !ok {
		r = &cacheEntry{
			lock: make(chan struct{}, 1),
		}
		cc.clients[cacheKey] = r
	}
	return r
}

func newClient(ctx context.Context, config *SessionConfig, region string, assumeRoleARN string) (Client, error) {
	conf, err := newAWSConfig(ctx, config.AccessKeyID, config.SecretAccessKey, region, assumeRoleARN)
	if err != nil {
		return nil, err
	}
	return struct {
		iam.GetInstanceProfileAPIClient
		ec2.DescribeInstancesAPIClient
		organizations.ListAccountsAPIClient
	}{
		GetInstanceProfileAPIClient: iam.NewFromConfig(conf),
		DescribeInstancesAPIClient:  ec2.NewFromConfig(conf),
		ListAccountsAPIClient:       organizations.NewFromConfig(conf),
	}, nil
}
