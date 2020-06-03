package storage

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/klauspost/pgzip"

	"os"
	"path"
	"regexp"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set"
	digest "github.com/opencontainers/go-digest"
	"github.com/panjf2000/ants"
	lz4 "github.com/pierrec/lz4"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver   driver.StorageDriver
	statter  distribution.BlobStatter
	reg      *registry
	pathFn   func(dgst digest.Digest) (string, error)
	redirect bool // allows disabling URLFor redirects
}

type registriesAPIResponse struct {
	Registries []string
}

func (bs *blobServer) URLWriter(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	var registries []string
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if bs.driver.Name() == "distributed" {
		registriesstr, _ := bs.driver.URLFor(ctx, "/dev/nil", nil)
		registries = strings.Split(registriesstr, ",")
	} else {
		registries = make([]string, 0)
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(registriesAPIResponse{
		Registries: registries,
	}); err != nil {
		return err
	}
	return nil
}

func (bs *blobServer) ServeHeadBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	path, err := bs.pathFn(desc.Digest)
	if err != nil {
		return err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return err
		}
	}

	br, err := newFileReader(ctx, bs.driver, path, desc.Size)
	if err != nil {
		return err
	}
	defer br.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
	}

	http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
	return nil
}

type TarFile struct {
	Lm sync.Mutex
	Tw *tar.Writer
}

type PgzipFile struct {
	Lm sync.Mutex
	//	Pw *pgzip.Writer
	Compressbufp *bytes.Buffer
}

type Task struct {
	Src  string
	Desc string
	Reg  *registry
	Tf   *TarFile
	//Ctype string
}

func addToTarFile(tf *TarFile, path string, contents []byte) (int, error) {

	hdr := &tar.Header{
		Name: path,
		Mode: 0600,
		Size: int64(len(contents)),
	}

	tf.Lm.Lock()
	if err := tf.Tw.WriteHeader(hdr); err != nil {
		fmt.Printf("NANNAN: cannot write file header to tar file for %s\n", path)
		tf.Lm.Unlock()
		return 0, err
	}

	size, err := tf.Tw.Write(contents)
	if err != nil {
		fmt.Printf("NANNAN: cannot write file contents to tar file for %s\n", path)
		tf.Lm.Unlock()
		return 0, err
	}

	tf.Lm.Unlock()
	return size, nil
}

func (bs *blobServer) pgzipconcatTarFile(compressbufp *bytes.Buffer, pw *PgzipFile) error {
	rdr, err := pgzip.NewReader(compressbufp)
	if err != nil {
		fmt.Printf("NANNAN: pgzipconcatTarFile: cannot create reader: %v \n", err)
		return err
	}
	bss, err := ioutil.ReadAll(rdr)
	if err != nil {
		fmt.Printf("NANNAN: pgzipconcatTarFile: cannot read from reader: %v \n", err)
		return err
	}

	pw.Lm.Lock()
	//w := pgzip.NewWriter(pw.Compressbufp)
	w, _ := pgzip.NewWriterLevel(pw.Compressbufp, bs.reg.compr_level)
	w.Write(bss)
	w.Close()
	pw.Lm.Unlock()

	return nil
}

func (bs *blobServer) lz4concatTarFile(compressbufp *bytes.Buffer, pw *PgzipFile) error {
	zr := lz4.NewReader(compressbufp)
	//	if err != nil {
	//		fmt.Printf("NANNAN: lz4: cannot create reader: %v \n", err)
	//		return err
	//	}
	bss, err := ioutil.ReadAll(zr)
	if err != nil {
		fmt.Printf("NANNAN: lz4: cannot read from reader: %v \n", err)
		return err
	}

	pw.Lm.Lock()
	header := lz4.Header{CompressionLevel: bs.reg.compr_level}
	zw := lz4.NewWriter(pw.Compressbufp)
	zw.Header = header
	zw.Write(bss)
	zw.Close()
	pw.Lm.Unlock()

	return nil
}

func pgzipTarFile(bufp *bytes.Buffer, compressbufp *bytes.Buffer, compr_level int) []byte {
	w, _ := pgzip.NewWriterLevel(compressbufp, compr_level)
	io.Copy(w, bufp)
	w.Close()
	return compressbufp.Bytes()
}

func lz4TarFile(bufp *bytes.Buffer, compressbufp *bytes.Buffer, compr_level int) []byte {
	header := lz4.Header{CompressionLevel: compr_level}
	zw := lz4.NewWriter(compressbufp)
	zw.Header = header
	_, err := io.Copy(zw, bufp)
	if err != nil {
		fmt.Printf("NANNAN: LZ4 cannot compress. error: %v\n", err)
		return nil
	}
	err = zw.Close()
	if err != nil {
		fmt.Printf("NANNAN: LZ4 cannot close. error: %v\n", err)
		return nil
	}
	return compressbufp.Bytes()
}

func packFile(i interface{}) {

	task, ok := i.(*Task)
	if !ok {
		fmt.Println(ok)
		return
	}
	newsrc := task.Src
	desc := task.Desc
	reg := task.Reg
	tf := task.Tf
	//ctype := task.Ctype

	var contents *[]byte

	//	start := time.Now()
	//check if newsrc is in file cache
	bfss, ok, _ := reg.blobcache.GetFile(newsrc)
	if ok {
		fmt.Printf("NANNAN: file cache hit\n")
		contents = &bfss
	} else {
		//		fmt.Printf("NANNAN: mvfile: file cache error for %s\n", newsrc)
		fmt.Printf("NANNAN: file cache miss\n")

		//check src file exists or not
		var _, err = os.Stat(newsrc)
		if os.IsNotExist(err) {
			fmt.Printf("NANNAN: src file %v: error: %v\n", newsrc, err)
			return
		}

		bfss, err := ioutil.ReadFile(newsrc)
		if err != nil {
			fmt.Printf("NANNAN: read file %s generated error: %v\n", desc, err)
			return
		} else {
			contents = &bfss
			//put in cache
			//			fmt.Printf("NANNAN: file cache put: %v B for %s\n", len(bfss), newsrc)
			if len(bfss) > 0 {
				ok = reg.blobcache.SetFile(newsrc, bfss)
				if !ok {
					fmt.Printf("NANNAN: file cache cannot write to digest: %v: \n", newsrc)
				}
			}
		}
	}

	_, err := addToTarFile(tf, desc, *contents)
	if err != nil {
		fmt.Printf("NANNAN: desc file %s generated error: %v\n", desc, err)
		return
	}

	//	DurationFCP := time.Since(start).Seconds()
	//	fmt.Printf("NANNAN: wrote %d bytes to file %s duration: %v\n", size, desc, DurationFCP)
	return
}

func (bs *blobServer) serveManifest(ctx context.Context, _desc distribution.Descriptor, w http.ResponseWriter, r *http.Request) (float64, error) {
	// get from traditional registry, this is a manifest
	path, err := bs.pathFn(_desc.Digest)
	if err != nil {
		return 0.0, err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return 0.0, err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return 0.0, err
		}
	}

	br, err := newFileReader(ctx, bs.driver, path, _desc.Size) //stat.Size())
	if err != nil {
		return 0.0, err
	}
	defer br.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, _desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", _desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", _desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(_desc.Size))
	}

	start := time.Now()
	http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, br)
	DurationNTT := time.Since(start).Seconds()

	return DurationNTT, nil
}

func (bs *blobServer) packAllFiles(ctx context.Context, desc distribution.SliceRecipeDescriptor, bufp *bytes.Buffer, reg *registry, constructtype string) float64 {

	fcntno := 0.0
	fcnt := 0
	for _, sfdescriptor := range desc.Files {
		if sfdescriptor.HostServerIp != bs.reg.hostserverIp {
			context.GetLogger(ctx).Debugf("NANNAN: this is not a locally available file, %v", sfdescriptor.HostServerIp) // not locally available
			continue
		}
		fcnt += 1
	}

	if fcnt > 500 {
		fcnt = 500
	}

	var wg sync.WaitGroup
	antp, _ := ants.NewPoolWithFunc(fcnt, func(i interface{}) {
		packFile(i)
		wg.Done()
	})
	defer antp.Release()

	regx, err := regexp.Compile("[^a-zA-Z0-9/.-]+")
	if err != nil {
		context.GetLogger(ctx).Errorf("NANNAN: %s, ", err)
		return 0.0
	}

	tw := tar.NewWriter(bufp)

	tf := &TarFile{
		Tw: tw,
	}

	start := time.Now()
	for _, sfdescriptor := range desc.Files {

		if sfdescriptor.HostServerIp != bs.reg.hostserverIp {
			context.GetLogger(ctx).Debugf("NANNAN: this is not a locally available file, %v", sfdescriptor.HostServerIp) // not locally available
			continue
		}
		//in case there are same files inside the layer dir
		fcntno = fcntno + 1.0
		random_dir := fmt.Sprintf("%f", fcntno)

		tarfpath := regx.ReplaceAllString(strings.SplitN(sfdescriptor.FilePath, "diff", 2)[1], "") // replace alphanumeric
		destfpath := path.Join(tarfpath + "-" + random_dir)
		wg.Add(1)
		antp.Invoke(&Task{
			Src:  sfdescriptor.FilePath, //strings.TrimPrefix(bfdescriptor.BlobFilePath, "/var/lib/registry"),
			Desc: destfpath,
			Reg:  reg,
			Tf:   tf,
			//Ctype: constructtype,
		})
	}
	wg.Wait()

	if err := tw.Close(); err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: cannot close tar file for %v", desc.Digest.String())
		return 0.0
	}
	DurationCP := time.Since(start).Seconds()
	return DurationCP
}

func (bs *blobServer) TransferBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, _desc distribution.Descriptor,
	cprssrder *bytes.Reader) (float64, error) {

	path_old, err := bs.pathFn(_desc.Digest)
	size := cprssrder.Size()

	if err != nil {
		return 0.0, err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.URLFor(ctx, path_old, map[string]interface{}{"method": r.Method})
		switch err.(type) {
		case nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return 0.0, err

		case driver.ErrUnsupportedMethod:
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return 0.0, err
		}
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, _desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", _desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", _desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(size))
	}

	start := time.Now()
	http.ServeContent(w, r, _desc.Digest.String(), time.Time{}, cprssrder) //packFile)
	DurationNTT := time.Since(start).Seconds()

	return DurationNTT, nil

}

type Restoringbuffer struct {
	sync.Mutex
	cnd  *sync.Cond
	bufp *bytes.Buffer
	wg   *sync.WaitGroup
}

/*
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE
PRECONSTRUCTLAYER
*/

func (bs *blobServer) notifyPeerPreconstructLayer(ctx context.Context, dgst digest.Digest, wg *sync.WaitGroup) bool {

	defer wg.Done()

	desc, err := bs.reg.metadataService.StatLayerRecipe(ctx, dgst)
	if err != nil {
		context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND LAYER RECIPE: %v or Empty layer for dgst", err, dgst)
		return false
	}

	rand.Seed(time.Now().UnixNano())

	choosen := desc.HostServerIps[rand.Intn(len(desc.HostServerIps)-1)]
	regip := choosen
	//for _, regip := range desc.HostServerIps {

	go func(regip string, ctx context.Context, dgst digest.Digest, wg *sync.WaitGroup) {
		tp := "TYPEPRECONSTRUCTLAYER"

		dgststring := dgst.String()
		var regipbuffer bytes.Buffer
		reponame := context.GetRepoName(ctx)
		usrname := context.GetUsrAddr(ctx)

		regipbuffer.WriteString(regip)
		//		regipbuffer.WriteString(":5000")
		regip = regipbuffer.String()
		context.GetLogger(ctx).Debugf("NANNAN: notifyPeerPreconstructLayer for %s, dgst: %s", regip, dgststring)

		//GET /v2/<name>/blobs/<digest>
		var urlbuffer bytes.Buffer
		urlbuffer.WriteString("http://")
		urlbuffer.WriteString(regip)
		urlbuffer.WriteString("/v2/")
		urlbuffer.WriteString(tp + "USRADDR" + usrname + "REPONAME" + reponame)
		urlbuffer.WriteString("/blobs/sha256:")

		newdgststring := strings.SplitN(dgststring, "sha256:", 2)[1]
		urlbuffer.WriteString(newdgststring)

		url := urlbuffer.String()
		url = strings.ToLower(url)
		context.GetLogger(ctx).Debugf("NANNAN: notifyPeerPreconstructLayer URL %s", url)

		//let's skip head request
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: notifyPeerPreconstructLayer GET URL %s, err %s", url, err)
			return //false
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: notifyPeerPreconstructLayer Do GET URL %s, err %s", url, err)
			return //false
		}
		context.GetLogger(ctx).Debugf("NANNAN: notifyPeerPreconstructLayer %s returned status code %d", regip, resp.StatusCode)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return //false //errors.New("notifyPeerPreconstructLayer to other servers, failed")
		}
		//return

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: cannot read from resp.body: %s", err)
			return err

		}

		context.GetLogger(ctx).Debugf("NANNAN: GetLayerFromRegistry succeed! URL %s size: %d", url, len(body))

		bs.reg.blobcache.SetFile(dgst.String(), body)

		/*		buf := bytes.NewBuffer(body)

				if bs.reg.compressmethod == "pgzip" {
					err = bs.pgzipconcatTarFile(buf, pw)

				} else if bs.reg.compressmethod == "lz4" {
					err = bs.lz4concatTarFile(buf, pw)

				} else {
					fmt.Printf("NANNAN: error. what is the compress method?", bs.reg.compressmethod)

				}*/

	}(regip, ctx, dgst, wg)
	//return true
	//}
	return true
}

/*
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE*/

func (bs *blobServer) GetSliceFromRegistry(ctx context.Context, dgst digest.Digest, regip string, pw *PgzipFile,
	wg *sync.WaitGroup, constructtype string) error {

	defer wg.Done()

	if regip != bs.reg.hostserverIp {

		dgststring := dgst.String()

		var regipbuffer bytes.Buffer
		reponame := context.GetRepoName(ctx)
		usrname := context.GetUsrAddr(ctx)

		regipbuffer.WriteString(regip)
		regipbuffer.WriteString(":5000")
		regip = regipbuffer.String()
		context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry start! from server %s, dgst: %s ", regip, dgststring)

		//GET /v2/<name>/blobs/<digest>
		var urlbuffer bytes.Buffer
		urlbuffer.WriteString("http://")
		urlbuffer.WriteString(regip)
		urlbuffer.WriteString("/v2/")

		urlbuffer.WriteString("TYPE" + constructtype + "USRADDR" + usrname + "REPONAME" + reponame)
		urlbuffer.WriteString("/blobs/sha256:")

		dgststring = strings.SplitN(dgststring, "sha256:", 2)[1]
		urlbuffer.WriteString(dgststring)
		url := urlbuffer.String()
		url = strings.ToLower(url)

		context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry create URL %s ", url)

		//let's skip head request
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: ForwardToRegistry GET URL %s, err %s", url, err)
			return err
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: GetSliceFromRegistry Do GET URL %s, err %s", url, err)
			return err
		}
		defer resp.Body.Close()

		context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry %s returned status code %d", regip, resp.StatusCode)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return errors.New("get slices from other servers, failed")
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			context.GetLogger(ctx).Errorf("NANNAN: cannot read from resp.body: %s", err)
			return err
		}

		context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry succeed! URL %s size: %d", url, len(body))

		buf := bytes.NewBuffer(body)
		//	bs.reg.blobcache.SetFile(dgst.String(), body)

		if bs.reg.compressmethod == "pgzip" {
			err = bs.pgzipconcatTarFile(buf, pw)
		} else if bs.reg.compressmethod == "lz4" {
			err = bs.lz4concatTarFile(buf, pw)
		} else {
			fmt.Printf("NANNAN: error. what is the compress method?", bs.reg.compressmethod)
		}

		return err
	}

	//***** construct locally *****
	desc, err := bs.reg.metadataService.StatSliceRecipe(ctx, dgst)
	if err != nil || (err == nil && len(desc.Files) == 0) {
		context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND SLICE RECIPE: %v or Empty slice ", err)
		//send empty
		buf := bytes.NewBuffer([]byte("gotta!"))
		err = bs.lz4concatTarFile(buf, pw)
		return err
	}

	var wgsl sync.WaitGroup
	bss, _, _ := bs.constructSlice(ctx, desc, dgst, bs.reg, constructtype, &wgsl)

	context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry succeed! from local registry %s size: %d", regip, len(bss))

	buf := bytes.NewBuffer(bss)

	if bs.reg.compressmethod == "pgzip" {
		err = bs.pgzipconcatTarFile(buf, pw)
	} else if bs.reg.compressmethod == "lz4" {
		err = bs.lz4concatTarFile(buf, pw)
	} else {
		fmt.Printf("NANNAN: error. what is the compress method?", bs.reg.compressmethod)
	}

	return err
}

func (bs *blobServer) constructSlice(ctx context.Context, desc distribution.SliceRecipeDescriptor,
	dgst digest.Digest, reg *registry, constructtype string, wg *sync.WaitGroup) ([]byte, float64, string) {

	var buf bytes.Buffer
	var comprssbuf bytes.Buffer

	start := time.Now()
	_ = bs.packAllFiles(ctx, desc, &buf, reg, constructtype)
	//DurationCP
	//start = time.Now()
	var bss []byte
	if bs.reg.compressmethod == "pgzip" {
		bss = pgzipTarFile(&buf, &comprssbuf, 2) // bs.reg.compr_level)
	} else if bs.reg.compressmethod == "lz4" {
		bss = lz4TarFile(&buf, &comprssbuf, 8)
	} else {
		fmt.Printf("NANNAN: error. what is the compress method?", bs.reg.compressmethod)
	}

	DurationSCT := time.Since(start).Seconds()

	tp := "SLICECONSTRUCT"
	return bss, DurationSCT, tp
}

func (bs *blobServer) constructLayer(ctx context.Context, desc distribution.LayerRecipeDescriptor,
	dgst digest.Digest, constructtype string, wg *sync.WaitGroup) ([]byte, float64, string) {

	var lwg sync.WaitGroup
	var comprssbuf bytes.Buffer

	pf := &PgzipFile{
		Compressbufp: &comprssbuf,
	}

	rbuf := &Restoringbuffer{
		bufp: &comprssbuf,
		wg:   wg,
	}
	rbuf.cnd = sync.NewCond(rbuf)

	start := time.Now()
	rsbufval, ok := bs.reg.restoringlayermap.LoadOrStore(dgst.String(), rbuf)
	if ok {
		// loaded true
		if rsbuf, ok := rsbufval.(*Restoringbuffer); ok {
			rsbuf.wg.Add(1)

			context.GetLogger(ctx).Debugf("NANNAN: layer construct waiting for digest: %v", dgst.String())
			rsbuf.Lock()
			//			rsbuf.cnd.Wait()
			rsbuf.Unlock()

			DurationWLCT := time.Since(start).Seconds()

			tp := "WAITLAYERCONSTRUCT"
			bss := rsbuf.bufp.Bytes()
			return bss, DurationWLCT, tp
		} else {
			context.GetLogger(ctx).Debugf("NANNAN: bs.reg.restoringslicermap.LoadOrStore wrong digest: %v", dgst.String())
		}
	} else {
		rbuf.wg.Add(1)

		rbuf.Lock()

		constructtypeslice := ""
		if "PRECONSTRUCTLAYER" == constructtype {
			constructtypeslice = "PRECONSTRUCTSLICE"
		} else if "LAYER" == constructtype {
			constructtypeslice = "SLICE"
		}

		start := time.Now()
		if len(desc.HostServerIps) == 1 {
			//***** construct locally *****
			desc, err := bs.reg.metadataService.StatSliceRecipe(ctx, dgst)
			if err != nil || (err == nil && len(desc.Files) == 0) {
				context.GetLogger(ctx).Warnf("NANNAN: COULDN'T FIND SLICE RECIPE: %v or Empty slice ", err)
				//send empty
				rbuf.bufp = bytes.NewBuffer([]byte("gotta!"))
				rbuf.Unlock()
				DurationLCT := time.Since(start).Seconds()
				//	rbuf.cnd.Broadcast()
				tp := "LAYERCONSTRUCT"
				//			bss := rbuf.bufp.Bytes()
				return []byte("gotta!"), DurationLCT, tp
			}

			var wgsl sync.WaitGroup
			bss, _, _ := bs.constructSlice(ctx, desc, dgst, bs.reg, constructtype, &wgsl)
			//			context.GetLogger(ctx).Debugf("NANNAN: GetSliceFromRegistry succeed! from local registry %s size: %d", regip, len(bss))
			rbuf.bufp = bytes.NewBuffer(bss)
			rbuf.Unlock()
			DurationLCT := time.Since(start).Seconds()
			//	rbuf.cnd.Broadcast()
			tp := "LAYERCONSTRUCT"
			//			bss := rbuf.bufp.Bytes()
			return bss, DurationLCT, tp
		}

		for _, hserver := range desc.HostServerIps {
			lwg.Add(1)
			go bs.GetSliceFromRegistry(ctx, dgst, hserver, pf, &lwg, constructtypeslice)
		}
		lwg.Wait()

		DurationLCT := time.Since(start).Seconds()

		rbuf.Unlock()

		//	rbuf.cnd.Broadcast()

		tp := "LAYERCONSTRUCT"
		bss := comprssbuf.Bytes()

		return bss, DurationLCT, tp
	}
	return nil, 0.0, ""
}

func (bs *blobServer) Preconstructlayers(ctx context.Context, reg *registry) error {
	reponame := context.GetRepoName(ctx)
	usrname := context.GetUsrAddr(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: for repo (%s) and usr (%s)", reponame, usrname)

	rlmapentry, err := bs.reg.metadataService.StatRLMapEntry(ctx, reponame)
	if err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: cannot get rlmapentry for repo (%s)", reponame)
		return err
	}
	fmt.Printf("NANNAN: PrecontstructionLayer: rlmapentry => %v", rlmapentry)
	ulmapentry, err := bs.reg.metadataService.StatULMapEntry(ctx, usrname)
	if err != nil {
		context.GetLogger(ctx).Debugf("NANNAN: Preconstructlayers: cannot get ulentry for usr (%s)", usrname)
		//		return err
	}
	fmt.Printf("NANNAN: PrecontstructionLayer: ulmapentry => %v", ulmapentry)

	var rlgstlst []interface{}
	for k := range rlmapentry.Dgstmap {
		rlgstlst = append(rlgstlst, k)
	}
	fmt.Println("NANNAN: PrecontstructionLayer: rlmapentry dgstlst")
	rlset := mapset.NewSetFromSlice(rlgstlst)

	var ulgstlst []interface{}
	for k := range ulmapentry.Dgstmap {
		ulgstlst = append(ulgstlst, k)
	}
	fmt.Println("NANNAN: PrecontstructionLayer: ulmapentry dgstlst")

	ulset := mapset.NewSetFromSlice(ulgstlst)

	diffset := rlset.Difference(ulset)
	sameset := rlset.Intersect(ulset)

	fmt.Printf("NANNAN: PrecontstructionLayer: diffset dgstlst: ", diffset)
	fmt.Printf("NANNAN: PrecontstructionLayer: sameset dgstlst: ", sameset)

	var repulldgsts []interface{}
	it := sameset.Iterator()
	for elem := range it.C {
		id := elem.(digest.Digest)
		if ulmapentry.Dgstmap[id] > bs.reg.repullcntthres {
			repulldgsts = append(repulldgsts, id)
		}
	}

	repullset := mapset.NewSetFromSlice(repulldgsts)

	descdgstset := diffset.Union(repullset)
	context.GetLogger(ctx).Debugf("NANNAN: descdgstlst: %v ", descdgstset)

	if len(descdgstset.ToSlice()) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	it = descdgstset.Iterator()
	for elem := range it.C {
		wg.Add(1)
		id := elem.(digest.Digest)
		go bs.notifyPeerPreconstructLayer(ctx, id, &wg)
	}
	wg.Wait()

	return nil
}

/*
//NANNAN:
	NoCompression       = flate.NoCompression
	BestSpeed           = flate.BestSpeed
	BestCompression     = flate.BestCompression
	DefaultCompression  = flate.DefaultCompression
	ConstantCompression = flate.ConstantCompression
	HuffmanOnly         = flate.HuffmanOnly
//TYPE XXX USRADDR XXX REPONAME XXX
MANIFEST
LAYER
SLICE

//NANNAN:
`compressionLevel` : any value between 1 and LZ4HC_CLEVEL_MAX will work.
LZ4HC_CLEVEL_MAX        12
*/

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {

	_desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	reqtype := context.GetType(ctx)
	reponame := context.GetRepoName(ctx)
	usrname := context.GetUsrAddr(ctx)
	context.GetLogger(ctx).Debugf("NANNAN: ServeBlob: type: %s for repo (%s) and usr (%s) with dgst (%s)", reqtype, reponame, usrname, dgst.String())

	if reqtype == "MANIFEST" {
		context.GetLogger(ctx).Debugf("NANNAN: THIS IS A MANIFEST REQUEST, serve and preconstruct layers")

		go bs.Preconstructlayers(ctx, bs.reg) // **** prefetch layers ******

	}

	cachehit := false

	var bytesreader *bytes.Reader
	var bss []byte
	//var tp string
	//tp = ""
	var size int64 = 0
	//	var Uncompressedsize int64 = 0
	//	DurationML = 0.0
	//	DurationMAC := 0.0
	//	DurationLCT := 0.0
	//	DurationSCT := 0.0
	//	DurationNTT := 0.0
	//	compressratio := 0.0
	DurationMEM := 0.0
	DurationSSD := 0.0
	DurationNTT := 0.0

	ok := false
	if reqtype == "LAYER" || reqtype == "PRECONSTRUCTLAYER" || reqtype == "MANIFEST" {

		// *** check cache ******

		//		start := time.Now()
		bss, ok, DurationMEM = bs.reg.blobcache.GetFile(dgst.String())
		if ok {
			//			DurationMEM = time.Since(start).Seconds()
			cachehit = true
			if reqtype == "LAYER" || reqtype == "MANIFEST" {
				if reqtype == "LAYER" {
					context.GetLogger(ctx).Debug("NANNAN: layer cache hit!")
				}

				bytesreader = bytes.NewReader(bss)
				size = bytesreader.Size()

			} else {
				bytesreader = bytes.NewReader(bss)
				size = bytesreader.Size()
				//bytesreader = bytes.NewReader([]byte("gotta!"))
			}

			goto out

		} else {

			blobPath, err := bs.pathFn(_desc.Digest)

			layerPath := path.Join("/var/lib/registry", blobPath)
			start := time.Now()
			bss, err = ioutil.ReadFile(layerPath)
			DurationSSD = time.Since(start).Seconds()
			if err != nil {
				fmt.Printf("NANNAN: cannot open layer file =>%s\n", layerPath)
				return err
			}

			//rbuf.bufp = bytes.NewBuffer(bss)
			//rbuf.Unlock()

			if reqtype == "LAYER" || reqtype == "MANIFEST" {

				bytesreader = bytes.NewReader(bss)
				size = bytesreader.Size()
			} else {
				bytesreader = bytes.NewReader([]byte("gotta!"))
			}
		}
		goto out
	}

out:
	start := time.Now()
	_, err = bs.TransferBlob(ctx, w, r, _desc, bytesreader)
	DurationNTT = time.Since(start).Seconds()
	if err != nil {
		return err
	}

	context.GetLogger(ctx).Debugf("NANNAN: primary: reqtype: %v, cachehit: %v, mem time: %v, ssd time: %v, "+
		"layer transfer time: %v, total time: %v, layer compressed size: %v",
		reqtype, cachehit, DurationMEM, DurationSSD, DurationNTT, (DurationMEM + DurationSSD + DurationNTT), size)

	//update ulmap
	go func(reqtype string, bs *blobServer) {
		if reqtype == "LAYER" {
			ulmapentry, err := bs.reg.metadataService.StatULMapEntry(ctx, usrname)
			if err == nil {
				// exsist
				if _, ok := ulmapentry.Dgstmap[dgst]; ok {
					//exsist
					ulmapentry.Dgstmap[dgst] += 1
				} else {
					//not exsist
					ulmapentry.Dgstmap[dgst] = 1
				}
			} else {
				//not exisit
				dgstmap := make(map[digest.Digest]int64)
				dgstmap[dgst] = 1
				ulmapentry = distribution.ULmapEntry{
					Dgstmap: dgstmap,
				}
			}
			err1 := bs.reg.metadataService.SetULMapEntry(ctx, usrname, ulmapentry)
			if err1 != nil {
				return //err1
			}

			//update rlmap
			rlmapentry, err := bs.reg.metadataService.StatRLMapEntry(ctx, reponame)
			if err == nil {
				// exsist
				if _, ok := rlmapentry.Dgstmap[dgst]; ok {
					//exsist
					rlmapentry.Dgstmap[dgst] += 1
				} else {
					rlmapentry.Dgstmap[dgst] = 1
				}
			} else {
				//not exisit
				dgstmap := make(map[digest.Digest]int64)
				dgstmap[dgst] = 1
				rlmapentry = distribution.RLmapEntry{
					Dgstmap: dgstmap,
				}
			}
			err1 = bs.reg.metadataService.SetRLMapEntry(ctx, reponame, rlmapentry)
			if err1 != nil {
				return //err1
			}
		}
	}(reqtype, bs)

	go func(ctx context.Context, cachehit bool, bs *blobServer,
		reqtype string,
		dgst digest.Digest) {
		if cachehit {
			return
		} else {
			if reqtype == "LAYER" || reqtype == "PRECONSTRUCTLAYER" || reqtype == "MANIFEST" {
				if reqtype == "LAYER" {
					context.GetLogger(ctx).Debug("NANNAN: layer cache miss!")
				}
				//if size != 0 && size <= 4*1024*1024 {
				bs.reg.blobcache.SetFile(dgst.String(), bss)
				//}

			}
		}
		return
	}(ctx, cachehit, bs,
		reqtype,
		dgst)

	return nil
}
