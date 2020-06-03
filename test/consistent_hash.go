package main
import(
	"fmt"
	"github.com/serialx/hashring"
	//"github.com/buraksezer/consistent"
	//"github.com/cespare/xxhash"
)

func addToMap(maptmp map[string][]string){
	maptmp["s_1"] = append(maptmp["s_1"], "key_1") 
}


func main(){
	servers := []string{
				"192.168.210:5000",
				"192.168.211:5000",
				"192.168.212:5000",
			//	"192.168.213:5000",
				"192.168.214:5000",
				"192.168.215:5000",
				"192.168.216:5000",
				"192.168.217:5000",
				"192.168.218:5000",
				"192.168.219:5000",
	}
	serverForwardMap := make(map[string][]string)
	addToMap(serverForwardMap)
	fmt.Println(serverForwardMap["s_1"])
	ring := hashring.New(servers)
	server, _ := ring.GetNodes("my_key", 4)
	fmt.Println("my_key", server,string(len(serverForwardMap)))
}
