package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"io"
	"io/ioutil"
	"log"
	_ "net/http"
	"os"
	"time"
	"encoding/gob"
	_ "bytes"
	"math/rand"
	"strings"
	"sync"
)

const CONFIG_FILE = "drive_credentials"
const TOKEN_FILE = "drive_tokens"
const DIRMAP_FILE = "drive_dirmap"
const ROOT_FOLDER = "your root folder id"

func getConfig() *oauth2.Config {
	b, err := ioutil.ReadFile(CONFIG_FILE)
	if err != nil {
		return nil
	}
	config, err := google.ConfigFromJSON(b, drive.DriveFileScope)
	if err != nil {
		return nil
	}
	return config
}

func getService(config *oauth2.Config, token *oauth2.Token) *drive.Service {
	client := config.Client(context.Background(), token)
	service, err := drive.New(client)
	if err != nil {
		return nil
	}
	return service
}

func bytesToToken(s []byte) *oauth2.Token {
	tok := &oauth2.Token{}
	err := json.Unmarshal(s, tok)
	if err != nil {
		return nil
	}
	return tok
}

func tokenToBytes(token *oauth2.Token) []byte {
	s, err := json.Marshal(token)
	if err != nil {
		return nil
	}
	return s
}

func createDir(service *drive.Service, name string, parentId string) (string, error) {
	d := &drive.File{
		Name: name,
		MimeType: "application/vnd.google-apps.folder",
		Parents: []string{parentId},
	}

	file, err := service.Files.Create(d).SupportsTeamDrives(true).Do()

	if err != nil {
		return "", err
	}

	return file.Id, err
}

func createFile(service *drive.Service, name string, mimeType string, content io.Reader, parentId string) (string, error) {
	f := &drive.File{
		MimeType: mimeType,
		Name: name,
		Parents: []string{parentId},
	}

	fmt.Println("start create######################")
	file, err := service.Files.Create(f).Media(content).SupportsTeamDrives(true).Do()
	fmt.Println("end create========================")

	if err != nil {
		return "", err
	}

	return file.Id, err
}

func createDirF(service *drive.Service, name string, parentId string) string {
	for true {
		res, err := createDir(service, name, parentId)
		if err != nil {
			fmt.Println(err)
		} else {
			return res
		}
		time.Sleep(1 * time.Second)
	}
	return ""
}

func uploadFileF(service *drive.Service, name string, src string, parentId string) string {
	for true {
		f, err := os.Open(src)
		if err != nil {
			fmt.Println(err)
			continue
		}
		res, err := createFile(service, name, "application/octet-stream", f, parentId)
		defer f.Close()
		if err != nil {
			fmt.Println(err)
		} else {
			return res
		}
		time.Sleep(1 * time.Second)
	}
	return ""
}

func downloadFile(service *drive.Service, id string, writer io.Writer) error {
	//fmt.Println(id)
	req, err := service.Files.Get(id).SupportsTeamDrives(true).Download()
	if err != nil {
		return err
	}
	io.Copy(writer, req.Body)
	return nil
}

func downloadFileF(service *drive.Service, id string, dst string) {
	for true {
		f, err := os.Create(dst)
		if err != nil {
			return
		}
		err = downloadFile(service, id, f)
		f.Close()
		if err != nil {
			fmt.Println(err)
		} else {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	authURL = strings.Replace(authURL, "drive.file", "drive", -1)
	fmt.Printf("Go to the following link in your browser then type the authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

var config *oauth2.Config
var services []*drive.Service
var dirMap map[string]string
var servicesUsed []bool
var serviceMutex, dirMutex sync.Mutex

func Save() {
	f, err := os.Create(DIRMAP_FILE)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	err = enc.Encode(dirMap)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("drive save ok")
}

func Load() {
	rand.Seed(time.Now().UnixNano())
	config = getConfig()
	if config == nil {
		log.Fatal("read config failed")
	}
	f, err := os.Open(DIRMAP_FILE)
	if err != nil {
		log.Print(err)
		dirMap = make(map[string]string)
	} else {
		dec := gob.NewDecoder(f)
		err = dec.Decode(&dirMap)
		f.Close()
	}
	f, err = os.Open(TOKEN_FILE)
	services = make([]*drive.Service, 0)
	if err != nil {
		log.Print(err)
	} else {
		t := make([][]byte, 0)
		dec := gob.NewDecoder(f)
		err = dec.Decode(&t)
		f.Close()
		for i := 0; i < len(t); i++ {
			services = append(services, getService(config, bytesToToken(t[i])))
		}
	}
	servicesUsed = make([]bool, len(services))
	fmt.Println("drive load ok")
}

func AddToken() {
	f, err := os.Open(TOKEN_FILE)
	t := make([][]byte, 0)
	if err != nil {
		log.Print(err)
	} else {
		dec := gob.NewDecoder(f)
		err = dec.Decode(&t)
		f.Close()
	}
	t = append(t, tokenToBytes(getTokenFromWeb(config)))
	f, err = os.Create(TOKEN_FILE)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	err = enc.Encode(t)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("drive addtoken ok")
}

func randstr() string {
	return fmt.Sprintf("%02x", rand.Intn(256))
}

func upload(service *drive.Service, src, sid string) string {
	s1 := randstr()
	s2 := randstr()
	cur := ROOT_FOLDER
	fmt.Println("upload", src, s1, s2)
	dirMutex.Lock()
	if val, ok := dirMap[s1]; !ok {
		cur = createDirF(service, s1, cur)
		dirMap[s1] = cur
	} else {
		cur = val
	}
	if val, ok := dirMap[s1 + "/" + s2]; !ok {
		cur = createDirF(service, s2, cur)
		dirMap[s1 + "/" + s2] = cur
	} else {
		cur = val
	}
	dirMutex.Unlock()
	fo := fmt.Sprintf("%06x", rand.Intn(1 << 24))
	fn := fo + "_" + sid
	id := uploadFileF(service, fn, src, cur)
	fmt.Println("upload ok", src, s1, s2)
	return id + "|" + s1 + "/" + s2 + "/" + fo
}

func download(service *drive.Service, id, dst string) {
	pos := strings.Index(id, "|")
	downloadFileF(service, id[:pos], dst)
}

func CacheFile(src, dst string, sz uint64) {
	var sid int = -1
	for true {
		serviceMutex.Lock()
		ts := rand.Perm(len(services))
		for i := 0; i < len(services); i++ {
			if !servicesUsed[ts[i]] {
				sid = ts[i]
				servicesUsed[ts[i]] = true
				break
			}
		}
		serviceMutex.Unlock()
		if sid != -1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Println("CacheFile:", sid, src, dst)
	if src == "" {
		fmt.Println("Cache null file")
		f, _ := os.Create(dst)
		f.Truncate(int64(sz))
		return
	}
	download(services[sid], src, dst)
	serviceMutex.Lock()
	servicesUsed[sid] = false
	serviceMutex.Unlock()
}

func MoveFile(src, id string) string {
	var sid int = -1
	for true {
		serviceMutex.Lock()
		ts := rand.Perm(len(services))
		for i := 0; i < len(services); i++ {
			if !servicesUsed[ts[i]] {
				sid = ts[i]
				servicesUsed[ts[i]] = true
				break
			}
		}
		serviceMutex.Unlock()
		if sid != -1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Println("uploading using", sid)
	res := upload(services[sid], src, id)
	serviceMutex.Lock()
	servicesUsed[sid] = false
	serviceMutex.Unlock()
	os.Remove(src)
	return res
}

func WaitAll() {
	for true {
		done := true
		serviceMutex.Lock()
		for i := 0; i < len(services); i++ {
			if servicesUsed[i] {
				done = false
				break
			}
		}
		serviceMutex.Unlock()
		if done {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}
