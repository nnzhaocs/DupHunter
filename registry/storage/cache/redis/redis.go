package redis

import (
	"errors"
	"fmt"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/cache"
	//	redis "github.com/garyburd/redigo/redis"
	digest "github.com/opencontainers/go-digest"
	//NANNAN
	//"encoding/json"
	// "net"

	//	"flag"
	//rejson "github.com/secondspass/go-rejson"
	//	"log"
	redis "github.com/gomodule/redigo/redis"
	//	"github.com/go-redis/redis"
	//	redisc "github.com/mna/redisc"
	redisgo "github.com/go-redis/redis"
)

// redisBlobStatService provides an implementation of
// BlobDescriptorCacheProvider based on redis. Blob descriptors are stored in
// two parts. The first provide fast access to repository membership through a
// redis set for each repo. The second is a redis hash keyed by the digest of
// the layer, providing path, length and mediatype information. There is also
// a per-repository redis hash of the blob descriptor, allowing override of
// data. This is currently used to override the mediatype on a per-repository
// basis.
//
// Note that there is no implied relationship between these two caches. The
// layer may exist in one, both or none and the code must be written this way.

// HERE, we store dbNoBlobl and dbNoBFRecipe on a redis standalone
// we store dbNoFile on a redis cluster
var (
	dbNoBlob = 0
)

type redisBlobDescriptorService struct {
	pool *redis.Pool
}

// NewRedisBlobDescriptorCacheProvider returns a new redis-based
// BlobDescriptorCacheProvider using the provided redis connection pool.
func NewRedisBlobDescriptorCacheProvider(pool *redis.Pool) cache.BlobDescriptorCacheProvider {

	return &redisBlobDescriptorService{
		pool: pool,
	}
}

// RepositoryScoped returns the scoped cache.
func (rbds *redisBlobDescriptorService) RepositoryScoped(repo string) (distribution.BlobDescriptorService, error) {
	
	repo = "testrepo"
	
	if _, err := reference.ParseNormalizedNamed(repo); err != nil {
		return nil, err
	}

	return &repositoryScopedRedisBlobDescriptorService{
		repo:     repo,
		upstream: rbds,
	}, nil
}

// Stat retrieves the descriptor data from the redis hash entry.
func (rbds *redisBlobDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if err := dgst.Validate(); err != nil {
		return distribution.Descriptor{}, err
	}

	conn := rbds.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
		return distribution.Descriptor{}, err
	}

	return rbds.stat(ctx, conn, dgst)
}

func (rbds *redisBlobDescriptorService) Clear(ctx context.Context, dgst digest.Digest) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	conn := rbds.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
		return err
	}

	// Not atomic in redis <= 2.3
	reply, err := conn.Do("HDEL", rbds.blobDescriptorHashKey(dgst), "digest", "length", "mediatype")
	if err != nil {
		return err
	}

	if reply == 0 {
		return distribution.ErrBlobUnknown
	}

	return nil
}

// stat provides an internal stat call that takes a connection parameter. This
// allows some internal management of the connection scope.
func (rbds *redisBlobDescriptorService) stat(ctx context.Context, conn redis.Conn, dgst digest.Digest) (distribution.Descriptor, error) {
	reply, err := redis.Values(conn.Do("HMGET", rbds.blobDescriptorHashKey(dgst), "digest", "size", "mediatype"))
	if err != nil {
		return distribution.Descriptor{}, err
	}

	// NOTE(stevvooe): The "size" field used to be "length". We treat a
	// missing "size" field here as an unknown blob, which causes a cache
	// miss, effectively migrating the field.
	if len(reply) < 3 || reply[0] == nil || reply[1] == nil { // don't care if mediatype is nil
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	var desc distribution.Descriptor
	if _, err := redis.Scan(reply, &desc.Digest, &desc.Size, &desc.MediaType); err != nil {
		return distribution.Descriptor{}, err
	}

	return desc, nil
}

// SetDescriptor sets the descriptor data for the given digest using a redis
// hash. A hash is used here since we may store unrelated fields about a layer
// in the future.
func (rbds *redisBlobDescriptorService) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	if err := cache.ValidateDescriptor(desc); err != nil {
		return err
	}

	conn := rbds.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
		return err
	}

	return rbds.setDescriptor(ctx, conn, dgst, desc)
}

func (rbds *redisBlobDescriptorService) setDescriptor(ctx context.Context, conn redis.Conn, dgst digest.Digest, desc distribution.Descriptor) error {
	if _, err := conn.Do("HMSET", rbds.blobDescriptorHashKey(dgst),
		"digest", desc.Digest,
		"size", desc.Size); err != nil {
		return err
	}

	// Only set mediatype if not already set.
	if _, err := conn.Do("HSETNX", rbds.blobDescriptorHashKey(dgst),
		"mediatype", desc.MediaType); err != nil {
		return err
	}

	return nil
}

func (rbds *redisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
	return "blobs::" + dgst.String()
}

type repositoryScopedRedisBlobDescriptorService struct {
	repo     string
	upstream *redisBlobDescriptorService
}

var _ distribution.BlobDescriptorService = &repositoryScopedRedisBlobDescriptorService{}

// Stat ensures that the digest is a member of the specified repository and
// forwards the descriptor request to the global blob store. If the media type
// differs for the repository, we override it.
func (rsrbds *repositoryScopedRedisBlobDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if err := dgst.Validate(); err != nil {
		return distribution.Descriptor{}, err
	}

	conn := rsrbds.upstream.pool.Get()
	//NANNAN
	defer conn.Close()
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
		return distribution.Descriptor{}, err
	}

	// Check membership to repository first
	member, err := redis.Bool(conn.Do("SISMEMBER", rsrbds.repositoryBlobSetKey(rsrbds.repo), dgst))
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if !member {
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	upstream, err := rsrbds.upstream.stat(ctx, conn, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	// We allow a per repository mediatype, let's look it up here.
	mediatype, err := redis.String(conn.Do("HGET", rsrbds.blobDescriptorHashKey(dgst), "mediatype"))
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if mediatype != "" {
		upstream.MediaType = mediatype
	}

	return upstream, nil
}

// Clear removes the descriptor from the cache and forwards to the upstream descriptor store
func (rsrbds *repositoryScopedRedisBlobDescriptorService) Clear(ctx context.Context, dgst digest.Digest) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	conn := rsrbds.upstream.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
		return err
	}

	// Check membership to repository first
	member, err := redis.Bool(conn.Do("SISMEMBER", rsrbds.repositoryBlobSetKey(rsrbds.repo), dgst))
	if err != nil {
		return err
	}

	if !member {
		return distribution.ErrBlobUnknown
	}

	return rsrbds.upstream.Clear(ctx, dgst)
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	if err := cache.ValidateDescriptor(desc); err != nil {
		return err
	}

	if dgst != desc.Digest {
		if dgst.Algorithm() == desc.Digest.Algorithm() {
			return fmt.Errorf("redis cache: digest for descriptors differ but algorthim does not: %q != %q", dgst, desc.Digest)
		}
	}

	conn := rsrbds.upstream.pool.Get()
	defer conn.Close()
	//NANNAN
	if _, err := conn.Do("SELECT", dbNoBlob); err != nil {
		return err
	}

	return rsrbds.setDescriptor(ctx, conn, dgst, desc)
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) setDescriptor(ctx context.Context, conn redis.Conn, dgst digest.Digest, desc distribution.Descriptor) error {
	if _, err := conn.Do("SADD", rsrbds.repositoryBlobSetKey(rsrbds.repo), dgst); err != nil {
		return err
	}

	if err := rsrbds.upstream.setDescriptor(ctx, conn, dgst, desc); err != nil {
		return err
	}

	// Override repository mediatype.
	if _, err := conn.Do("HSET", rsrbds.blobDescriptorHashKey(dgst), "mediatype", desc.MediaType); err != nil {
		return err
	}

	// Also set the values for the primary descriptor, if they differ by
	// algorithm (ie sha256 vs sha512).
	if desc.Digest != "" && dgst != desc.Digest && dgst.Algorithm() != desc.Digest.Algorithm() {
		if err := rsrbds.setDescriptor(ctx, conn, desc.Digest, desc); err != nil {
			return err
		}
	}

	return nil
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
	return "testrepo::" + dgst.String() //"repository::" + rsrbds.repo + "::blobs::" + dgst.String()
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) repositoryBlobSetKey(repo string) string {
	return "testrepo::" // "repository::" + rsrbds.repo + "::blobs"
}

//NANNAN: for deduplication
type redisDedupMetadataService struct {
	pool         *redis.Pool
	hostserverIp string
	cluster      *redisgo.ClusterClient
}

// NewRedisBlobDescriptorCacheProvider returns a new redis-based
// DedupMetadataServiceCacheProvider using the provided redis connection pool.
func NewRedisDedupMetadataServiceCacheProvider(pool *redis.Pool, cluster *redisgo.ClusterClient, host_ip string) cache.DedupMetadataServiceCacheProvider {
	fmt.Printf("NANNAN: hostip: " + host_ip + "\n")
	return &redisDedupMetadataService{
		pool:         pool,
		cluster:      cluster,
		hostserverIp: host_ip,
	}
}

//"files::sha256:7173b809ca12ec5dee4506cd86be934c4596dd234ee82c0662eac04a8c2c71dc"
func (rdms *redisDedupMetadataService) fileDescriptorHashKey(dgst digest.Digest) string {
	return "File::" + dgst.String()
}

var _ distribution.RedisDedupMetadataService = &redisDedupMetadataService{}

func (rdms *redisDedupMetadataService) StatFile(ctx context.Context, dgst digest.Digest) (distribution.FileDescriptor, error) {
	reply, err := rdms.cluster.Get(rdms.fileDescriptorHashKey(dgst)).Result()
	if err == redisgo.Nil {
		//		context.GetLogger(ctx).Debug("NANNAN: key %s doesnot exist", dgst.String())
		return distribution.FileDescriptor{}, err
	} else if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster error for key %s", err)
		return distribution.FileDescriptor{}, err
	} else {
		var desc distribution.FileDescriptor
		if err = desc.UnmarshalBinary([]byte(reply)); err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot UnmarshalBinary for key %s", err)
			return distribution.FileDescriptor{}, err
		} else {
			return desc, nil
		}
	}
}

//use redis as global lock service
func (rdms *redisDedupMetadataService) SetFileDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.FileDescriptor) error {
	set, err := rdms.cluster.SetNX(rdms.fileDescriptorHashKey(dgst), &desc, 0).Result()
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot set value for key %s", err)
		return err
	}
	if set == true {
		return nil
	} else {
		context.GetLogger(ctx).Warnf("NANNAN: key %s already exsist!", dgst.String())
		return errors.New("key already exsits")
	}
}

func (rdms *redisDedupMetadataService) LayerRecipeHashKey(dgst digest.Digest) string {
	return "Layer:Recipe::" + dgst.String()
}

func (rdms *redisDedupMetadataService) SliceRecipeHashKey(dgst digest.Digest, sip string) string {
	return "Slice:Recipe::" + dgst.String() + "::" + sip //rdms.hostserverIp
}

func (rdms *redisDedupMetadataService) StatLayerRecipe(ctx context.Context, dgst digest.Digest) (distribution.LayerRecipeDescriptor, error) {

	reply, err := rdms.cluster.Get(rdms.LayerRecipeHashKey(dgst)).Result()
	if err == redisgo.Nil {
		//		context.GetLogger(ctx).Debug("NANNAN: key %s doesnot exist", dgst.String())
		return distribution.LayerRecipeDescriptor{}, err
	} else if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster error for key %s", err)
		return distribution.LayerRecipeDescriptor{}, err
	} else {
		var desc distribution.LayerRecipeDescriptor
		if err = desc.UnmarshalBinary([]byte(reply)); err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot UnmarshalBinary for key %s", err)
			return distribution.LayerRecipeDescriptor{}, err
		} else {
			return desc, nil
		}
	}
}

func (rdms *redisDedupMetadataService) SetLayerRecipe(ctx context.Context, dgst digest.Digest, desc distribution.LayerRecipeDescriptor) error {

	err := rdms.cluster.SetNX(rdms.LayerRecipeHashKey(dgst), &desc, 0).Err()
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot set value for key %s", err)
		return err
	}
	return nil
}

func (rdms *redisDedupMetadataService) StatSliceRecipe(ctx context.Context, dgst digest.Digest) (distribution.SliceRecipeDescriptor, error) {

	reply, err := rdms.cluster.Get(rdms.SliceRecipeHashKey(dgst, rdms.hostserverIp)).Result()
	if err == redisgo.Nil {
		//		context.GetLogger(ctx).Debug("NANNAN: key %s doesnot exist", dgst.String())
		return distribution.SliceRecipeDescriptor{}, err
	} else if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster error for key %s", err)
		return distribution.SliceRecipeDescriptor{}, err
	} else {
		var desc distribution.SliceRecipeDescriptor
		if err = desc.UnmarshalBinary([]byte(reply)); err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot UnmarshalBinary for key %s", err)
			return distribution.SliceRecipeDescriptor{}, err
		} else {
			return desc, nil
		}
	}
}

func (rdms *redisDedupMetadataService) SetSliceRecipe(ctx context.Context, dgst digest.Digest, desc distribution.SliceRecipeDescriptor, sip string) error {

	err := rdms.cluster.SetNX(rdms.SliceRecipeHashKey(dgst, sip), &desc, 0).Err()
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot set value for key %s", err)
		return err
	}
	return nil
}

//metadataservice for rlmap and ulmap

func (rdms *redisDedupMetadataService) RLMapHashKey(reponame string) string {
	return "RLMap::" + reponame
}

func (rdms *redisDedupMetadataService) ULMapHashKey(usrname string) string {
	return "ULMap::" + usrname
}

func (rdms *redisDedupMetadataService) StatRLMapEntry(ctx context.Context, reponame string) (distribution.RLmapEntry, error) {

	reply, err := rdms.cluster.Get(rdms.RLMapHashKey(reponame)).Result()
	if err == redisgo.Nil {
		//		context.GetLogger(ctx).Debug("NANNAN: key %s doesnot exist", dgst.String())
		return distribution.RLmapEntry{}, err
	} else if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster error for key %s", err)
		return distribution.RLmapEntry{}, err
	} else {
		var desc distribution.RLmapEntry
		if err = desc.UnmarshalBinary([]byte(reply)); err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot UnmarshalBinary for key %s", err)
			return distribution.RLmapEntry{}, err
		} else {
			return desc, nil
		}
	}
}

func (rdms *redisDedupMetadataService) SetRLMapEntry(ctx context.Context, reponame string, desc distribution.RLmapEntry) error {

	//set slicerecipe
	err := rdms.cluster.Set(rdms.RLMapHashKey(reponame), &desc, 0).Err()
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot set value for key %s", err)
		return err
	}
	return nil
}

func (rdms *redisDedupMetadataService) StatULMapEntry(ctx context.Context, usrname string) (distribution.ULmapEntry, error) {

	reply, err := rdms.cluster.Get(rdms.ULMapHashKey(usrname)).Result()
	if err == redisgo.Nil {
		//		context.GetLogger(ctx).Debug("NANNAN: key %s doesnot exist", dgst.String())
		return distribution.ULmapEntry{}, err
	} else if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster error for key %s", err)
		return distribution.ULmapEntry{}, err
	} else {
		var desc distribution.ULmapEntry
		if err = desc.UnmarshalBinary([]byte(reply)); err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot UnmarshalBinary for key %s", err)
			return distribution.ULmapEntry{}, err
		} else {
			return desc, nil
		}
	}
}

func (rdms *redisDedupMetadataService) SetULMapEntry(ctx context.Context, usrname string, desc distribution.ULmapEntry) error {

	//set slicerecipe
	err := rdms.cluster.Set(rdms.ULMapHashKey(usrname), &desc, 0).Err()
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: redis cluster cannot set value for key %s", err)
		return err
	}
	return nil
}
