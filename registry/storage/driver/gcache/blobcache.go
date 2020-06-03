package gcache

import (
	"fmt"
	//"gcache/gcache"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/allegro/bigcache"
	"github.com/peterbourgon/diskv"
)

// preconstruction cache
type BlobCache struct {
	MemCache  *bigcache.BigCache
	DiskCache *diskv.Diskv

	FileLST  Cache
	LayerLST Cache
	SliceLST Cache
	StageLST Cache
}

var DefaultTTL time.Duration
var FileCacheCap, LayerCacheCap, SliceCacheCap int
var FfileCacheCap float32
var Stype string

func (cache *BlobCache) SetCapTTL(fileCacheCap, layerCacheCap, sliceCacheCap, ttl int,
	stype string) error {

	DefaultTTL = time.Duration(ttl) * time.Millisecond
	fmt.Printf("NANNAN: DefaultTTL: %d\n\n", DefaultTTL)
	Stype = stype
	if stype == "MB" {
		FileCacheCap = fileCacheCap * 1024 * 1024
		LayerCacheCap = layerCacheCap * 1024 * 1024
		SliceCacheCap = sliceCacheCap * 1024 * 1024
	} else if stype == "B" {

		FfileCacheCap = float32(fileCacheCap) / 1024 / 1024
		LayerCacheCap = layerCacheCap
		SliceCacheCap = sliceCacheCap
	} else {

		fmt.Printf("NANNAN: what is the type? \n", stype)
	}

	fmt.Printf("NANNAN: FileCacheCap: %d %s, LayerCacheCap: %d %s, SliceCacheCap: %d %s\n\n",
		FileCacheCap, Stype, LayerCacheCap, Stype, SliceCacheCap, Stype)
	return nil
}

func (cache *BlobCache) Init() error {
	var memcap float32
	if Stype == "B" {
		memcap = FfileCacheCap * 1.2
	} else {
		memcap = float32(FileCacheCap) / 1024 / 1024 * 1.2
	}
	//<<<<<<< HEAD
	config := bigcache.Config{
		Shards:           1,
		LifeWindow:       3600 * time.Minute,
		Verbose:          true,
		HardMaxCacheSize: int(memcap),
		OnRemove:         nil,
	}
	MemCache, err := bigcache.NewBigCache(config)
	if err != nil {
		fmt.Printf("NANNAN: cannot create BlobCache: %s \n", err)
		return err
	}
	cache.MemCache = MemCache

	pth := "/var/lib/registry/docker/registry/v2/diskcache/"
	err = os.MkdirAll(pth, 0777)
	if err != nil {
		fmt.Printf("NANNAN: cannot create DiskCache: %s \n", err)
		return err
	}

	flatTransform := func(s string) []string { return []string{} }
	DiskCache := diskv.New(diskv.Options{
		BasePath:     pth,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024 * 64,
	})
	cache.DiskCache = DiskCache

	fmt.Printf("NANNAN: init cache: mem cache capacity: %d MB \n\n",
		int(memcap))

	FileLST := New(FileCacheCap).ARC().EvictedFunc(func(key, value interface{}) {
		//		fmt.Println("NANNAN: evicted key:", key)
		if k, ok := key.(string); ok {
			cache.MemCache.Delete(k)
		}

	}).
		Expiration(DefaultTTL * 3).
		Build()

	cache.FileLST = FileLST

	LayerLST := New(LayerCacheCap).ARC().EvictedFunc(func(key, value interface{}) {
		if k, ok := key.(string); ok {
			cache.DiskCache.Erase(k)
		}
		//		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL * 1).
		Build()

	cache.LayerLST = LayerLST

	SliceLST := New(SliceCacheCap).ARC().EvictedFunc(func(key, value interface{}) {
		if k, ok := key.(string); ok {
			cache.DiskCache.Erase(k)
		}
		//		fmt.Println("NANNAN: evicted key:", key)
	}).
		Expiration(DefaultTTL * 2).
		Build()

	cache.SliceLST = SliceLST

	// stage area *****
	StageLSTCAP := 1 //10gb
	StageLST := New(StageLSTCAP * 1024 * 1024).LRU().
		Expiration(3600 * time.Minute).
		Build()

	cache.StageLST = StageLST

	fmt.Printf("NANNAN: FileCacheCap: %d %s, LayerCacheCap: %d %s, SliceCacheCap: %d %s, StageLSTCAP: %d MB\n\n",
		FileCacheCap, Stype, LayerCacheCap, Stype, SliceCacheCap, Stype, StageLSTCAP)
	return err
}

func LayerHashKey(dgst string) string {
	return "Layer::" + dgst
}

func SliceHashKey(dgst string) string {
	return "Slice::" + dgst
}
func FileHashKey(dgst string) string {
	return "File::" + dgst
}

func (cache *BlobCache) SetLayer(dgst string, bss []byte) bool {
	key := LayerHashKey(dgst)
	size := len(bss)

	if err := cache.LayerLST.Set(key, size); err != nil {
		//		fmt.Printf("NANNAN: BlobCache SetLayer LayerLST cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	if ok := cache.DiskCache.Has(key); ok {
		//		fmt.Printf("NANNAN: BlobCache SetLayer DiskCache set dgst %s size: %v\n", dgst, size)
		return true
	}

	if err := cache.DiskCache.Write(key, bss); err != nil {
		//		fmt.Printf("NANNAN: BlobCache SetLayer DiskCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}
	//	fmt.Printf("NANNAN: BlobCache SetLayer set dgst %s size: %v, LayerLST, cache size: %v\n", dgst, size, cache.LayerLST.Size(false))
	return true
}

func (cache *BlobCache) SetPUTLayer(dgst string, size int64, bpath string) bool {
	key := LayerHashKey(dgst)

	if err := cache.StageLST.Set(key, path.Join("/var/lib/registry/", bpath)); err != nil {
		//		fmt.Printf("NANNAN: BlobCache SetPUTLayer StageLST cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	//	fmt.Printf("NANNAN: BlobCache SetPUTLayer set dgst %s size: %v, StageLST, cache size: %v\n", dgst, size, cache.StageLST.Len(false))

	return true
}

func (cache *BlobCache) RemovePUTLayer(dgst string, move_tocache bool) bool {
	key := LayerHashKey(dgst)

	time.Sleep(5 * time.Second)

	if !move_tocache {
		cache.StageLST.Remove(key)
		return true
	}

	if bpathval, err := cache.StageLST.Get(key); err != nil {
		//		fmt.Printf("NANNAN: BlobCache RemovePUTLayer StageLST cannot get dgst %s: %v\n", dgst, err)
		return false
	} else {
		if bpath, err := bpathval.(string); err != true {
			//			fmt.Printf("NANNAN: BlobCache RemovePUTLayer StageLST cannot get path string for dgst %s: %v\n", dgst, err)
			return false
		} else {
			if bss, err := ioutil.ReadFile(bpath); err != nil {
				//				fmt.Printf("NANNAN: BlobCache RemovePUTLayer ReadFile cannot get dgst %s: %v, read error\n", dgst, err)
				return false
			} else {
				//promote to cache and remove it
				//				fmt.Printf("NANNAN: BlobCache RemovePUTLayer dgst %s, StageLST, cache size: %v \n", dgst, cache.StageLST.Len(false))
				if err := cache.LayerLST.Set(key, len(bss)); err != nil {
					//					fmt.Printf("NANNAN: BlobCache RemovePUTLayer LayerLST cannot set dgst %s: %v\n", dgst, err)
					return false
				}
				cache.StageLST.Remove(key)
				//				return bss, true
			}
		}
	}

	//	fmt.Printf("NANNAN: BlobCache RemovePUTLayer remove dgst %s, StageLST, cache size: %v \n", dgst, cache.StageLST.Len(false))

	return true
}

func (cache *BlobCache) GetLayer(dgst string) ([]byte, bool) {
	key := LayerHashKey(dgst)

	if _, err := cache.LayerLST.Get(key); err != nil {

		//		fmt.Printf("NANNAN: BlobCache GetLayer LayerLST DiskCache cannot get dgst %s: %v check stage area ...\n", dgst, err)
		if bpathval, err := cache.StageLST.Get(key); err != nil {
			//			fmt.Printf("NANNAN: BlobCache GetLayer StageLST cannot get dgst %s: %v\n", dgst, err)
			return nil, false
		} else {
			if bpath, err := bpathval.(string); err != true {
				//				fmt.Printf("NANNAN: BlobCache GetLayer StageLST cannot get path string for dgst %s: %v\n", dgst, err)
				return nil, false
			} else {
				if bss, err := ioutil.ReadFile(bpath); err != nil {
					//					fmt.Printf("NANNAN: BlobCache GetLayer ReadFile cannot get dgst %s: %v, read error\n", dgst, err)
					return nil, false
				} else {
					//promote to cache and remove it
					//					fmt.Printf("NANNAN: BlobCache GetLayer dgst %s, StageLST, cache size: %v \n", dgst, cache.StageLST.Len(false))
					//					if err := cache.LayerLST.Set(key, len(bss)); err != nil {
					////						fmt.Printf("NANNAN: BlobCache GetLayer LayerLST cannot set dgst %s: %v\n", dgst, err)
					//						return nil, false
					//					}
					////					cache.StageLST.Remove(key)
					return bss, true
				}
			}
		}
	}

	if bss, err := cache.DiskCache.Read(key); err == nil {
		//		fmt.Printf("NANNAN: BlobCache GetLayer LayerLST dgst %s, cache size: %v \n", dgst, cache.LayerLST.Size(false))
		return bss, true
	} else {
		//		fmt.Printf("NANNAN: BlobCache GetLayer DiskCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
}

func (cache *BlobCache) SetSlice(dgst string, bss []byte) bool {
	key := SliceHashKey(dgst)
	size := len(bss)

	if err := cache.SliceLST.Set(key, size); err != nil {
		//		fmt.Printf("NANNAN: BlobCache SetSlice SliceLST cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	if ok := cache.DiskCache.Has(key); ok {
		//		fmt.Printf("NANNAN: BlobCache SetSlice DiskCache set dgst %s size: %v\n", dgst, size)
		return true
	}

	if err := cache.DiskCache.Write(key, bss); err != nil {
		//		fmt.Printf("NANNAN: BlobCache SetSlice DiskCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	//	fmt.Printf("NANNAN: BlobCache SetSlice set dgst %s size: %v\n", dgst, size)

	return true
}

func (cache *BlobCache) GetSlice(dgst string) ([]byte, bool) {
	key := SliceHashKey(dgst)
	if _, err := cache.SliceLST.Get(key); err != nil {
		//		fmt.Printf("NANNAN: BlobCache GetSlice SliceLST cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}

	bss, err := cache.DiskCache.Read(key)
	if err != nil {
		//		fmt.Printf("NANNAN: BlobCache GetSlice DiskCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false
	}
	return bss, true
}

func (cache *BlobCache) SetFile(dgst string, bss []byte) bool {
	key := FileHashKey(dgst)
	size := len(bss)

	if err := cache.FileLST.Set(key, size); err != nil {
		//		fmt.Printf("NANNAN: BlobCache SetFile FileLST cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	if err := cache.MemCache.Set(key, bss); err != nil {
		//		fmt.Printf("NANNAN: BlobCache SetFile MemCache cannot set dgst %s: %v\n", dgst, err)
		return false
	}

	//	fmt.Printf("NANNAN: BlobCache SetFile set dgst %s, FileLST, size: %v, cache size: %v\n", dgst, size, cache.FileLST.Size(false))

	return true
}

func (cache *BlobCache) GetFile(dgst string) ([]byte, bool, float64) {
	key := FileHashKey(dgst)
	if _, err := cache.FileLST.Get(key); err != nil {
		//		fmt.Printf("NANNAN: BlobCache GetFile FileLST cannot get dgst %s: %v\n", dgst, err)
		return nil, false, 0.0
	}
	start := time.Now()
	bss, err := cache.MemCache.Get(key)
	duration := time.Since(start).Seconds()
	if err != nil {
		//		fmt.Printf("NANNAN: BlobCache GetFile MemCache cannot get dgst %s: %v\n", dgst, err)
		return nil, false, 0.0
	}

	//	fmt.Printf("NANNAN: BlobCache GetFile get dgst %s, FileLST, cache size: %v\n", dgst, cache.FileLST.Size(false))

	return bss, true, duration
}
