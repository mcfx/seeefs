package main

import (
	"fmt"
	"os"
	"os/signal"
	_ "io"
	"io/ioutil"
	"sync"
	"encoding/binary"
	"crypto/sha512"
	"strings"
	"sort"
	"strconv"
	"log"
	"time"
	"syscall"
	"flag"

	"./backend"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"golang.org/x/net/context"
)

const MOUNT_POINT = "mnt"
const FS_DATA_FILE = "fs_data"
const CACHE_PATH = "cache/"
const TMP_PATH = "tmp/"
const CACHE_LIMIT uint64 = 1099511627776
const MIN_BLOCK_SIZE uint64 = 67108864
const MAX_BLOCK_SIZE uint64 = 268435456 * 2

const NullId = 0xffffffffffffffff

type Dir struct {
	Name string
	Inode uint64
	Child, Files []uint64
	ChildMap, FilesMap map[string]uint64
}

type StorageInfo struct {
	NodeId, NodePos uint64
	Nodes []uint64
}

type File struct {
	Name string
	Inode, Size uint64
	SHA512 [sha512.Size]byte
	Storage StorageInfo
}

type Node struct {
	Size uint64
	Source string
}

var Dirs []Dir
var Files []File
var Nodes []Node
var Inodes uint64
var SHA512Lookup map[[sha512.Size]byte]uint64
var NodesOpenCnt []uint64
var NodesLastAccess []uint64
var NodesCached, NodesRealCached []bool
var FSMutex sync.Mutex

var CachedNodes []uint64
var CacheListMutex sync.Mutex
var CacheTotalSize uint64

func realCache(id uint64) {
	FSMutex.Lock()
	src := Nodes[id].Source
	sz := Nodes[id].Size
	FSMutex.Unlock()
	backend.CacheFile(src, CACHE_PATH + strconv.FormatUint(uint64(id), 10), sz)
	FSMutex.Lock()
	CacheListMutex.Lock()
	NodesRealCached[id] = true
	FSMutex.Unlock()
	CacheListMutex.Unlock()
}

func cache(id uint64) {
	//fmt.Println("cache", id, Nodes[id])
	if !NodesCached[id] {
		for CacheTotalSize > CACHE_LIMIT {
			var oldest uint64 = NullId
			pos := -1
			for i := 0; i < len(CachedNodes); i++ {
				if NodesOpenCnt[CachedNodes[i]] == 0 && NodesLastAccess[CachedNodes[i]] < oldest {
					oldest = NodesLastAccess[CachedNodes[i]]
					pos = i
				}
			}
			if pos == -1 {
				break
			}
			if pos != len(CachedNodes) - 1 {
				CachedNodes[pos], CachedNodes[len(CachedNodes) - 1] = CachedNodes[len(CachedNodes) - 1], CachedNodes[pos]
			}
			rid := CachedNodes[len(CachedNodes) - 1]
			CachedNodes = CachedNodes[:len(CachedNodes) - 1]
			NodesCached[rid] = false
			NodesRealCached[rid] = false
			os.Remove(CACHE_PATH + strconv.FormatUint(uint64(rid), 10))
			CacheTotalSize -= Nodes[rid].Size
			//fmt.Println("/remove node", rid)
		}
		NodesCached[id] = true
		go realCache(id)
		CachedNodes = append(CachedNodes, id)
		CacheTotalSize += Nodes[id].Size
		//fmt.Println("/cache node", id)
	}
	NodesLastAccess[id] = uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

func preCache(id uint64) {
	FSMutex.Lock()
	CacheListMutex.Lock()
	cache(id)
	FSMutex.Unlock()
	CacheListMutex.Unlock()
}

func getNodeFile(id uint64) *os.File {
	//fmt.Println("open node", id)
	FSMutex.Lock()
	CacheListMutex.Lock()
	cache(id)
	NodesOpenCnt[id]++
	flag := NodesRealCached[id]
	FSMutex.Unlock()
	CacheListMutex.Unlock()
	if !flag {
		for true {
			FSMutex.Lock()
			CacheListMutex.Lock()
			flag = NodesRealCached[id]
			FSMutex.Unlock()
			CacheListMutex.Unlock()
			if flag {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	f, err := os.Open(CACHE_PATH + strconv.FormatUint(uint64(id), 10))
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func closeNodeFile(id uint64, f *os.File) {
	//fmt.Println("close node", id)
	f.Close()
	FSMutex.Lock()
	CacheListMutex.Lock()
	NodesOpenCnt[id]--
	FSMutex.Unlock()
	CacheListMutex.Unlock()
}

func preFetch(id uint64) {
	var nid uint64
	FSMutex.Lock()
	if len(Files[id].Storage.Nodes) > 0 {
		nid = Files[id].Storage.Nodes[0]
	} else {
		nid = Files[id].Storage.NodeId
	}
	FSMutex.Unlock()
	go preCache(nid)
}

type FuseDir struct {
	Id uint64
}

type FuseFile struct {
	Id uint64
}

type FuseFileHandle struct {
	Storage StorageInfo
	Size, Cur uint64
	NodesSize []uint64
	File *os.File
}

func (f *FuseFileHandle) switchFile(id uint64) {
	if f.Cur == id {
		return
	}
	if f.Cur != NullId {
		closeNodeFile(f.Cur, f.File)
	}
	f.File = getNodeFile(id)
	if id + 1 < uint64(len(f.Storage.Nodes)) {
		preCache(f.Storage.Nodes[id + 1])
	}
	f.Cur = id
}

func (f *FuseFileHandle) readBytes(l, r uint64) []byte {
	if l < 0 {
		l = 0
	}
	if r > f.Size {
		r = f.Size
	}
	if l >= r {
		return make([]byte, 0)
	}
	if len(f.Storage.Nodes) == 0 {
		f.switchFile(f.Storage.NodeId)
		//fmt.Println("switch", f.Storage.NodeId)
		f.File.Seek(int64(l + f.Storage.NodePos), 0)
		buf := make([]byte, r - l)
		f.File.Read(buf)
		return buf
	}
	var ul, ur, tl, tr uint64
	res := make([]byte, 0)
	for i := 0; i < len(f.Storage.Nodes); i++ {
		ur = ul + f.NodesSize[i]
		tl = l; tr = r
		if ul > tl { tl = ul }
		if ur < tr { tr = ur }
		if tl < tr {
			f.switchFile(f.Storage.Nodes[i])
			//fmt.Println("switch big", f.Storage.NodeId)
			f.File.Seek(int64(tl - ul), 0)
			buf := make([]byte, tr - tl)
			f.File.Read(buf)
			res = append(res, buf...)
		}
		ul = ur
	}
	//fmt.Println("read big", l, r, len(res))
	return res
}

func addChild(x uint64, name string) uint64 {
	n := uint64(len(Dirs))
	//fmt.Println(n, name)
	Dirs = append(Dirs, Dir{})
	Dirs[x].Child = append(Dirs[x].Child, n)
	Dirs[x].ChildMap[name] = n
	Dirs[n].Name = name
	Inodes++
	Dirs[n].Inode = Inodes
	Dirs[n].Child = make([]uint64, 0)
	Dirs[n].Files = make([]uint64, 0)
	Dirs[n].ChildMap = make(map[string]uint64, 0)
	Dirs[n].FilesMap = make(map[string]uint64, 0)
	return n
}

func addChildFile(x uint64, name string) uint64 {
	n := uint64(len(Files))
	Files = append(Files, File{})
	Dirs[x].Files = append(Dirs[x].Files, n)
	Dirs[x].FilesMap[name] = n
	Files[n].Name = name
	Inodes++
	Files[n].Inode = Inodes
	Files[n].Storage.Nodes = make([]uint64, 0)
	return n
}

func clear() {
	Dirs = make([]Dir, 1)
	Files = make([]File, 0)
	Nodes = make([]Node, 0)
	Dirs[0].Name = "/"
	Dirs[0].Inode = 1
	Dirs[0].Child = make([]uint64, 0)
	Dirs[0].Files = make([]uint64, 0)
	Dirs[0].ChildMap = make(map[string]uint64, 0)
	Dirs[0].FilesMap = make(map[string]uint64, 0)
	Inodes = 1
	NodesOpenCnt = make([]uint64, 0)
	NodesLastAccess = make([]uint64, 0)
	SHA512Lookup = make(map[[sha512.Size]byte]uint64)
}

func appendUvarint(s []byte, x uint64) []byte {
	n := len(s)
	s = append(s, make([]byte, binary.MaxVarintLen64)...)
	n += binary.PutUvarint(s[n:], x)
	return s[:n]
}

func getUvarint(s []byte) (uint64, uint) {
	a, b := binary.Uvarint(s)
	return a, uint(b)
}

func saveBytes() []byte {
	res := make([]byte, 0)
	res = appendUvarint(res, uint64(len(Dirs)))
	for i := 0; i < len(Dirs); i++ {
		ts := []rune(Dirs[i].Name)
		res = appendUvarint(res, uint64(len(ts)))
		for j := 0; j < len(ts); j++ {
			res = appendUvarint(res, uint64(ts[j]))
		}
		res = appendUvarint(res, uint64(Dirs[i].Inode))
		res = appendUvarint(res, uint64(len(Dirs[i].Child)))
		for j := 0; j < len(Dirs[i].Child); j++ {
			res = appendUvarint(res, uint64(Dirs[i].Child[j]))
		}
		res = appendUvarint(res, uint64(len(Dirs[i].Files)))
		for j := 0; j < len(Dirs[i].Files); j++ {
			res = appendUvarint(res, uint64(Dirs[i].Files[j]))
		}
	}
	res = appendUvarint(res, uint64(len(Files)))
	for i := 0; i < len(Files); i++ {
		ts := []rune(Files[i].Name)
		res = appendUvarint(res, uint64(len(ts)))
		for j := 0; j < len(ts); j++ {
			res = appendUvarint(res, uint64(ts[j]))
		}
		res = appendUvarint(res, uint64(Files[i].Inode))
		res = appendUvarint(res, uint64(Files[i].Size))
		res = appendUvarint(res, uint64(Files[i].Storage.NodeId))
		res = appendUvarint(res, uint64(Files[i].Storage.NodePos))
		res = append(res, Files[i].SHA512[:]...)
		//fmt.Println(len(res))
		res = appendUvarint(res, uint64(len(Files[i].Storage.Nodes)))
		for j := 0; j < len(Files[i].Storage.Nodes); j++ {
			res = appendUvarint(res, uint64(Files[i].Storage.Nodes[j]))
		}
	}
	res = appendUvarint(res, uint64(len(Nodes)))
	for i := 0; i < len(Nodes); i++ {
		res = appendUvarint(res, uint64(Nodes[i].Size))
		ts := []rune(Nodes[i].Source)
		res = appendUvarint(res, uint64(len(ts)))
		for j := 0; j < len(ts); j++ {
			res = appendUvarint(res, uint64(ts[j]))
		}
	}
	res = appendUvarint(res, Inodes)
	return res
}

func save() {
	t := saveBytes()
	f, err := os.Create(FS_DATA_FILE)
	if err != nil {
		log.Fatal(err)
		return
	}
	f.Write(t)
	f.Close()
}

func loadBytes(s []byte) {
	FSMutex.Lock()
	CacheListMutex.Lock()
	var a uint64
	var n, b uint
	a, b = getUvarint(s[n:]); n += b
	Dirs = make([]Dir, uint(a))
	for i := 0; i < len(Dirs); i++ {
		a, b = getUvarint(s[n:]); n += b
		ts := make([]rune, uint(a))
		for j := 0; j < len(ts); j++ {
			a, b = getUvarint(s[n:]); n += b
			ts[j] = rune(a)
		}
		Dirs[i].Name = string(ts)
		Dirs[i].Inode, b = getUvarint(s[n:]); n += b
		a, b = getUvarint(s[n:]); n += b
		Dirs[i].Child = make([]uint64, uint(a))
		for j := 0; j < len(Dirs[i].Child); j++ {
			Dirs[i].Child[j], b = getUvarint(s[n:]); n += b
		}
		a, b = getUvarint(s[n:]); n += b
		Dirs[i].Files = make([]uint64, uint(a))
		for j := 0; j < len(Dirs[i].Files); j++ {
			Dirs[i].Files[j], b = getUvarint(s[n:]); n += b
		}
	}
	SHA512Lookup = make(map[[sha512.Size]byte]uint64)
	a, b = getUvarint(s[n:]); n += b
	Files = make([]File, uint(a))
	for i := 0; i < len(Files); i++ {
		a, b = getUvarint(s[n:]); n += b
		ts := make([]rune, uint(a))
		for j := 0; j < len(ts); j++ {
			a, b = getUvarint(s[n:]); n += b
			ts[j] = rune(a)
		}
		Files[i].Name = string(ts)
		Files[i].Inode, b = getUvarint(s[n:]); n += b
		Files[i].Size, b = getUvarint(s[n:]); n += b
		Files[i].Storage.NodeId, b = getUvarint(s[n:]); n += b
		Files[i].Storage.NodePos, b = getUvarint(s[n:]); n += b
		copy(Files[i].SHA512[:], s[n: n + sha512.Size])
		n += sha512.Size
		//fmt.Println(n, len(s))
		a, b = getUvarint(s[n:]); n += b
		Files[i].Storage.Nodes = make([]uint64, uint(a))
		for j := 0; j < len(Files[i].Storage.Nodes); j++ {
			Files[i].Storage.Nodes[j], b = getUvarint(s[n:]); n += b
		}
		SHA512Lookup[Files[i].SHA512] = uint64(i)
	}
	a, b = getUvarint(s[n:]); n += b
	Nodes = make([]Node, uint(a))
	for i := 0; i < len(Nodes); i++ {
		Nodes[i].Size, b = getUvarint(s[n:]); n += b
		a, b = getUvarint(s[n:]); n += b
		ts := make([]rune, uint(a))
		for j := 0; j < len(ts); j++ {
			a, b = getUvarint(s[n:]); n += b
			ts[j] = rune(a)
		}
		Nodes[i].Source = string(ts)
	}
	Inodes, b = getUvarint(s[n:]); n += b
	if int(n) != len(s) {
		log.Fatal("fs data decode error")
	}
	for i := 0; i < len(Dirs); i++ {
		Dirs[i].ChildMap = make(map[string]uint64, 0)
		Dirs[i].FilesMap = make(map[string]uint64, 0)
		for j := 0; j < len(Dirs[i].Child); j++ {
			Dirs[i].ChildMap[Dirs[Dirs[i].Child[j]].Name] = Dirs[i].Child[j]
		}
		for j := 0; j < len(Dirs[i].Files); j++ {
			Dirs[i].FilesMap[Files[Dirs[i].Files[j]].Name] = Dirs[i].Files[j]
		}
	}
	t := make([]uint64, len(Nodes))
	if NodesOpenCnt != nil {
		copy(t, NodesOpenCnt)
	}
	NodesOpenCnt = t
	t = make([]uint64, len(Nodes))
	if NodesLastAccess != nil {
		copy(t, NodesLastAccess)
	}
	NodesLastAccess = t
	t2 := make([]bool, len(Nodes))
	if NodesCached != nil {
		copy(t2, NodesCached)
	}
	NodesCached = t2
	t3 := make([]bool, len(Nodes))
	if NodesRealCached != nil {
		copy(t3, NodesRealCached)
	}
	NodesRealCached = t3
	FSMutex.Unlock()
	CacheListMutex.Unlock()
}

func load() {
	f, err := os.Open(FS_DATA_FILE)
	if err != nil {
		log.Print(err)
		clear()
		return
	}
	t, _ := ioutil.ReadAll(f)
	f.Close()
	loadBytes(t)
}

func getChild(id uint64, name string) uint64 {
	if val, ok := Dirs[id].ChildMap[name]; ok {
		return val
	}
	return NullId
}

func getChildFile(id uint64, name string) uint64 {
	if val, ok := Dirs[id].FilesMap[name]; ok {
		return val
	}
	return NullId
}

func getPath(path string) (uint64, int) {
	//path[-1] should not be /
	s := strings.Split(path, "/")
	var cur uint64 = 0
	for i := 1; i < len(s); i++ {
		t := getChildFile(cur, s[i])
		if t != NullId {
			return NullId, 2
		}
		t = getChild(cur, s[i])
		if t == NullId {
			return NullId, 1
		}
		cur = t
	}
	return cur, 0
}

func getFile(path string) uint64 {
	//path[-1] should not be /
	s := strings.Split(path, "/")
	var cur uint64 = 0
	for i := 1; i < len(s); i++ {
		t := getChildFile(cur, s[i])
		if t != NullId {
			return t
		}
		cur = getChild(cur, s[i])
	}
	return cur
}

func getNewPath(path string) uint64 {
	//path[-1] should not be /
	s := strings.Split(path, "/")
	var cur uint64 = 0
	for i := 1; i < len(s); i++ {
		t := getChild(cur, s[i])
		if t == NullId {
			t = addChild(cur, s[i])
		}
		cur = t
	}
	return cur
}

func isDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func checkExists(id uint64, path string) bool {
	//path[-1] should be /
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
		return true
	}
	for _, f := range files {
		fn := f.Name()
		if getChildFile(id, fn) != NullId {
			return true
		}
		t := getChild(id, fn)
		if t != NullId {
			if isDir(path + fn) {
				if checkExists(t, path + fn + "/") {
					return true
				}
			} else {
				return true
			}
		}
	}
	return false
}

type NewFile struct {
	Id, Size uint64
	Path string
}

type NewFileList []NewFile

func (s NewFileList) Len() int {
	return len(s)
}

func (s NewFileList) Less(i, j int) bool {
	return s[i].Size < s[j].Size
}

func (s NewFileList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func linkExistsFile(id uint64) bool {
	if rid, ok := SHA512Lookup[Files[id].SHA512]; ok {
		Files[id].Storage = Files[rid].Storage
		Files[id].Inode = Files[rid].Inode
		return true
	}
	return false
}

func uploadNode(i uint64) {
	t := backend.MoveFile(TMP_PATH + strconv.FormatUint(i, 10), strconv.FormatUint(i, 10))
	FSMutex.Lock()
	Nodes[i].Source = t
	FSMutex.Unlock()
}

func sha512OfFile(path string, size uint64) (res [sha512.Size]byte, err error) {
	fmt.Println("computing hash in blocks")
	f, err := os.Open(path)
	if err != nil {
		return
	}
	h := sha512.New()
	cur := 0
	tot := int(size / MAX_BLOCK_SIZE) + 1
	buf := make([]byte, MAX_BLOCK_SIZE)
	for true {
		rc, err := f.Read(buf)
		if err != nil {
			break
		}
		h.Write(buf[:rc])
		cur++
		fmt.Printf("progress: %d/%d\n", cur, tot)
	}
	copy(res[:], h.Sum(nil))
	f.Close()
	err = nil
	return
}

func makeBigFile(id, size uint64, path string, skipLink bool) {
	fmt.Println("makeBigFile", id, size, path)
	var err error
	Files[id].SHA512, err = sha512OfFile(path, size)
	if err != nil {
		log.Fatal(err)
	}
	if !skipLink && linkExistsFile(id) {
		return
	}
	SHA512Lookup[Files[id].SHA512] = id
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
		return
	}
	Files[id].Storage.NodeId = 0
	Files[id].Storage.NodePos = 0
	Files[id].Storage.Nodes = make([]uint64, 0)
	bc := int((size + MAX_BLOCK_SIZE - 1) / MAX_BLOCK_SIZE)
	var pos uint64 = 0
	for i := 0; i < bc; i++ {
		fmt.Println("making block", i)
		bs := (size - pos) / uint64(bc - i)
		buf := make([]byte, int(bs))
		f.Read(buf)
		pos += bs
		n := uint64(len(Nodes))
		path := TMP_PATH + strconv.FormatUint(n, 10)
		//fmt.Println(path)
		f, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
			return
		}
		f.Write(buf)
		f.Close()
		FSMutex.Lock()
		Nodes = append(Nodes, Node{bs, ""})
		FSMutex.Unlock()
		go uploadNode(n)
		Files[id].Storage.Nodes = append(Files[id].Storage.Nodes, n)
	}
	if bc == 1 {
		Files[id].Storage.NodeId = Files[id].Storage.Nodes[0]
		Files[id].Storage.Nodes = make([]uint64, 0)
	}
	f.Close()
}

func makeFiles(s []NewFile, force, skipLink bool) []NewFile {
	fmt.Println("makeFiles:", len(s))
	st := NewFileList(s)
	sort.Sort(st)
	s = []NewFile(st)
	buf := make([]byte, 0)
	pending := make([]NewFile, 0)
	for i := 0; i < len(s); i++ {
		fmt.Println("precess:", i)
		f, err := os.Open(s[i].Path)
		if err != nil {
			log.Fatal(err)
			return s
		}
		t, _ := ioutil.ReadAll(f)
		f.Close()
		Files[s[i].Id].SHA512 = sha512.Sum512(t)
		//fmt.Println(Files[s[i].Id].SHA512)
		if skipLink || !linkExistsFile(s[i].Id) {
			buf = append(buf, t...)
			pending = append(pending, s[i])
		}
		if uint64(len(buf)) >= MIN_BLOCK_SIZE || (i == len(s) - 1 && force && len(pending) > 0) {
			fmt.Println("block ok")
			n := uint64(len(Nodes))
			path := TMP_PATH + strconv.FormatUint(n, 10)
			//fmt.Println(path)
			f, err := os.Create(path)
			if err != nil {
				log.Fatal(err)
				return s
			}
			f.Write(buf)
			f.Close()
			var pos uint64 = 0
			for j := 0; j < len(pending); j++ {
				Files[pending[j].Id].Storage.NodeId = n
				Files[pending[j].Id].Storage.NodePos = pos
				pos += pending[j].Size
				SHA512Lookup[Files[pending[j].Id].SHA512] = pending[j].Id
			}
			FSMutex.Lock()
			Nodes = append(Nodes, Node{uint64(len(buf)), ""})
			FSMutex.Unlock()
			go uploadNode(n)
			buf = make([]byte, 0)
			pending = make([]NewFile, 0)
		}
	}
	return pending
}

func getNewFiles(id uint64, path string) []NewFile {
	res := make([]NewFile, 0)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
		return res
	}
	for _, f := range files {
		fn := f.Name()
		if isDir(path + fn) {
			t := getChild(id, fn)
			if t == NullId {
				t = addChild(id, fn)
			}
			res = append(res, getNewFiles(t, path + fn + "/")...)
		} else {
			t := addChildFile(id, fn)
			fs, err := os.Stat(path + fn)
			if err == nil {
				sz := uint64(fs.Size())
				Files[t].Size = sz
				if sz >= MIN_BLOCK_SIZE {
					makeBigFile(t, sz, path + fn, false)
				} else {
					res = append(res, NewFile{t, sz, path + fn})
				}
			} else {
				log.Fatal(err)
				return res
			}
		}
	}
	return makeFiles(res, false, false)
}

func checkUploaded(st int, pr bool) bool {
	FSMutex.Lock()
	if pr { fmt.Println(len(Nodes)) }
	nodeUsed := make([]int, len(Nodes))
	for i := 0; i < len(Files); i++ {
		if len(Files[i].Storage.Nodes) == 0 {
			nodeUsed[Files[i].Storage.NodeId]++
		} else {
			for j := 0; j < len(Files[i].Storage.Nodes); j++ {
				nodeUsed[Files[i].Storage.Nodes[j]]++
			}
		}
	}
	res := true
	for i := st; i < len(Nodes); i++ {
		if Nodes[i].Source == "" {
			if pr { fmt.Println(i, Nodes[i], nodeUsed[i]) }
			if nodeUsed[i] > 0 { res = false }
		}
	}
	FSMutex.Unlock()
	return res
}

func copyPath(src, dst string) {
	n := len([]rune(src))
	if src[n - 1] == '/' {
		src = src[:n - 1]
	}
	n = len([]rune(dst))
	if dst[n - 1] == '/' {
		dst = dst[:n - 1]
	}
	//fmt.Println(src, dst)
	dst_id, erri := getPath(dst)
	if erri == 0 {
		if checkExists(dst_id, src + "/") {
			log.Fatal("error copy path: file already exists")
			return
		}
	} else {
		if erri == 2 {
			log.Fatal("error copy path: file already exists")
			return
		}
		dst_id = getNewPath(dst)
	}
	//fmt.Println(dst_id)
	old_node := len(Nodes)
	fl := getNewFiles(dst_id, src + "/")
	makeFiles(fl, true, false)
	/*for i := old_node; i < len(Nodes); i++ {
		Nodes[i].Source = backend.MoveFile(TMP_PATH + strconv.FormatUint(uint64(i), 10), strconv.FormatUint(uint64(i), 10))
	}*/
	//backend.WaitAll()
	//time.Sleep(1 * time.Second) // to let fileid write back
	for true {
		if checkUploaded(old_node, false) { break }
		time.Sleep(2 * time.Second)
	}
	//fmt.Println(fl)
}

func dfsCheck(a, b string, id uint64) []NewFile {
	res := make([]NewFile, 0)
	files, err := ioutil.ReadDir(b)
	if err != nil {
		log.Fatal(err)
		return res
	}
	for _, f := range files {
		fn := f.Name()
		if isDir(b + "/" + fn) {
			t := getChild(id, fn)
			if t == NullId {
				t = addChild(id, fn)
				res = append(res, getNewFiles(t, b + "/" + fn + "/")...)
			} else {
				res = append(res, dfsCheck(a + "/" + fn, b + "/" + fn, t)...)
			}
		} else {
			t := getChildFile(id, fn)
			fs, _ := os.Stat(b + "/" + fn)
			sz := uint64(fs.Size())
			flag := false
			if t == NullId {
				flag = true
				t := addChildFile(id, fn)
				Files[t].Size = sz
			} else {
				bh, _ := sha512OfFile(b + "/" + fn, sz)
				ah, _ := sha512OfFile(a + "/" + fn, sz)
				for i := 0; i < sha512.Size; i++ {
					if ah[i] != bh[i] {
						flag = true
						break
					}
				}
			}
			if flag {
				fmt.Println("hash differs:", a, b, fn)
				if sz >= MIN_BLOCK_SIZE {
					makeBigFile(t, sz, b + "/" + fn, true)
				} else {
					res = append(res, NewFile{t, sz, b + "/" + fn})
				}
			}
		}
	}
	return res
}

func checkPath(src, dst string) {
	n := len([]rune(src))
	if src[n - 1] == '/' {
		src = src[:n - 1]
	}
	n = len([]rune(dst))
	if dst[n - 1] == '/' {
		dst = dst[:n - 1]
	}
	old_node := len(Nodes)
	dst_id, _ := getPath(dst)
	var s []NewFile
	if isDir(src) {
		s = dfsCheck(MOUNT_POINT + dst, src, dst_id)
	} else {
		id := getFile(dst)
		if Files[id].Size >= MIN_BLOCK_SIZE {
			makeBigFile(id, Files[id].Size, src, true)
		} else {
			s = make([]NewFile, 1)
			s[0].Id = id
			s[0].Size = Files[id].Size
			s[0].Path = src
		}
	}
	fmt.Println(s)
	makeFiles(s, true, true)
	//time.Sleep(1 * time.Second) // to let it start upload
	backend.WaitAll()
	fmt.Println("waitall ok")
	//time.Sleep(1 * time.Second) // to let fileid write back
	for true {
		if checkUploaded(old_node, false) { break }
		time.Sleep(2 * time.Second)
	}
}

type FuseFS struct {}

func (FuseFS) Root() (fusefs.Node, error) {
	return FuseDir{0}, nil
}

func (f FuseDir) Attr(ctx context.Context, a *fuse.Attr) error {
	FSMutex.Lock()
	a.Inode = Dirs[f.Id].Inode
	FSMutex.Unlock()
	a.Mode = os.ModeDir | 0555
	return nil
}

func (f FuseDir) Lookup(ctx context.Context, name string) (fusefs.Node, error) {
	if f.Id == 0 && name == "__refresh__" {
		log.Print("reload")
		load()
		backend.Load()
	}
	FSMutex.Lock()
	if val, ok := Dirs[f.Id].ChildMap[name]; ok {
		FSMutex.Unlock()
		return FuseDir{val}, nil
	}
	if val, ok := Dirs[f.Id].FilesMap[name]; ok {
		FSMutex.Unlock()
		return FuseFile{val}, nil
	}
	FSMutex.Unlock()
	return nil, fuse.ENOENT
}

func (f FuseDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	FSMutex.Lock()
	res := make([]fuse.Dirent, 0)
	for i := 0; i < len(Dirs[f.Id].Child); i++ {
		ti := Dirs[f.Id].Child[i]
		res = append(res, fuse.Dirent{Inode: Dirs[ti].Inode, Name: Dirs[ti].Name, Type: fuse.DT_File})
	}
	for i := 0; i < len(Dirs[f.Id].Files); i++ {
		ti := Dirs[f.Id].Files[i]
		res = append(res, fuse.Dirent{Inode: Files[ti].Inode, Name: Files[ti].Name, Type: fuse.DT_File})
	}
	FSMutex.Unlock()
	return res, nil
}

func (f FuseFile) Attr(ctx context.Context, a *fuse.Attr) error {
	FSMutex.Lock()
	a.Inode = Files[f.Id].Inode
	a.Mode = 0444
	a.Size = Files[f.Id].Size
	FSMutex.Unlock()
	return nil
}

func (f FuseFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fh fusefs.Handle, err error) {
	preFetch(f.Id)
	FSMutex.Lock()
	res := FuseFileHandle{Storage: Files[f.Id].Storage, Size: Files[f.Id].Size, Cur: NullId}
	res.NodesSize = make([]uint64, len(res.Storage.Nodes))
	for i := 0; i < len(res.Storage.Nodes); i++ {
		res.NodesSize[i] = Nodes[res.Storage.Nodes[i]].Size
	}
	FSMutex.Unlock()
	return &res, nil
}

func (f *FuseFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	var l, r uint64
	l = uint64(req.Offset)
	r = l + uint64(req.Size)
	resp.Data = f.readBytes(l, r)
	return nil
}

func (f *FuseFileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if f.Cur != NullId {
		closeNodeFile(f.Cur, f.File)
		f.Cur = NullId
	}
	return nil
}

func mountMain() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	c, err := fuse.Mount(
		MOUNT_POINT,
		fuse.FSName("seed"),
		fuse.Subtype("seedfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("seed"),
		fuse.MaxReadahead(2097152),
		fuse.ReadOnly(),
		fuse.AllowOther(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	go func() {
		<-sigs
		fuse.Unmount(MOUNT_POINT)
		log.Print("unmount ok")
	}()

	err = fusefs.Serve(c, FuseFS{})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

func main() {
	fmt.Sprintf("just to ban the warning")
	load()
	log.Print("load ok")

	flag.Parse()

	if flag.Arg(0) == "mount" {
		backend.Load()
		mountMain()
		return
	}
	if flag.Arg(0) == "copy" {
		backend.Load()
		src := flag.Arg(1)
		dst := flag.Arg(2)
		copyPath(src, dst)
		save()
		backend.Save()
		os.Stat(MOUNT_POINT + "/__refresh__")
		return
	}
	if flag.Arg(0) == "test" {
		checkUploaded(0, true)
		return
	}
	if flag.Arg(0) == "fix" {
		backend.Load()
		src := flag.Arg(1)
		dst := flag.Arg(2)
		checkPath(src, dst)
		save()
		backend.Save()
		os.Stat(MOUNT_POINT + "/__refresh__")
		return
	}
	if flag.Arg(0) == "drive" && flag.Arg(1) == "addtoken" {
		backend.Load()
		backend.AddToken()
		return
	}
	//backend.Load()
	//fmt.Println(Nodes[0].Source)
	//cache(0)
	//Nodes[356].Source = "14kEhsmuZo5R7fpEpGMRUQmjZ-FcThAch|83/99/41e315"
}
