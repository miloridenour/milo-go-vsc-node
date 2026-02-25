package datalayer

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
)

type MutableDirectory struct {
	uio.DynamicDirectory
}

type LeafDir struct {
	Dir         uio.Directory
	leaves      map[string]*LeafDir
	leafDeleted bool
}

// Applies leaves to Dir
func (lf *LeafDir) Compact(recursive bool) {
	for key, value := range lf.leaves {
		if recursive {
			value.Compact(recursive)
		}

		node, _ := value.Dir.GetNode()
		lf.Dir.AddChild(context.Background(), key, node)

		//Do expensive link deletion cycle if a leaf was deleted (directory)
		if lf.leafDeleted == true {
			links, _ := lf.Dir.Links(context.Background())
			for _, v := range links {
				if v.Cid.Prefix().Codec == uint64(multicodec.Protobuf) {
					if lf.leaves[v.Name] == nil {
						//Deletion happened! Remove from directory structure
						lf.Dir.RemoveChild(context.Background(), v.Name)
					}
				}
			}
		}
	}
}

// Ipfs data tree directory wrapping with help functions
type DataBin struct {
	DataLayer *DataLayer
	Leaf      LeafDir
}

func (db *DataBin) Has(path string) bool {
	splitPath := strings.Split(path, "/")
	//Get directory regardless of child
	var qPath string
	if len(splitPath) > 1 {
		qPath = strings.Join(splitPath[:len(splitPath)-1], "/")
	} else {
		qPath = ""
	}
	wrkDir, err := db.resolveWrkDir(qPath)

	if err != nil {
		return false
	}
	endPath := splitPath[len(splitPath)-1]

	// fmt.Println("End pathing for search", endPath)

	if wrkDir.leaves[endPath] != nil {
		return true
	}

	_, err = wrkDir.Dir.Find(context.Background(), endPath)

	return err == nil
}

// Later on support recursive directly look through
func (db *DataBin) List(prefix string) (*[]string, error) {

	wrkDir, err := db.resolveWrkDir(prefix)

	if err != nil {
		lsd := make([]string, 0)
		return &lsd, nil
	}

	links, err := wrkDir.Dir.Links(context.Background())

	names := make([]string, 0)
	for _, v := range links {
		names = append(names, v.Name)
	}

	if err != nil {
		return nil, err
	}

	tree := make([]string, 0)
	for _, v := range links {
		prefix := v.Cid.Prefix()
		if prefix.Codec == uint64(multicodec.Protobuf) {
			//If it's protobuf then it's a sub directory
			//TODO: Recursive resolve
			//Signify it's a directory by including "/" at the end
			tree = append(tree, v.Name+"/")
		} else {
			tree = append(tree, v.Name)
		}
	}

	sort.Strings(tree)
	return &tree, nil
}

func (db *DataBin) Set(path string, link cid.Cid) error {
	node, _ := db.DataLayer.DagServ.Get(context.Background(), link)

	var wrkDir uio.Directory
	splitPath := strings.Split(path, "/")

	var leaf *LeafDir
	if len(splitPath) > 1 {
		//Resolve working dir
		for idx, pathElement := range splitPath[:len(splitPath)-1] {
			if idx == 0 {
				leaf = &db.Leaf
			}
			nextLeaf := leaf.leaves[pathElement]
			if nextLeaf == nil {
				lf := &LeafDir{
					Dir:    uio.NewDirectory(db.DataLayer.DagServ),
					leaves: make(map[string]*LeafDir),
				}
				leaf.leaves[pathElement] = lf
				leaf = lf
			} else {
				leaf = nextLeaf
			}
		}
		wrkDir = leaf.Dir
	} else {
		leaf = &db.Leaf
		wrkDir = leaf.Dir
	}

	err := wrkDir.AddChild(context.Background(), splitPath[len(splitPath)-1], node)

	// dag, _ := dagCbor.Decode(nodeDir.RawData(), mh.SHA2_256, -1)
	// json := dag.RawData()

	// fmt.Println("Json", json)
	return err
}

func (db *DataBin) Get(path string) (*cid.Cid, error) {
	fmt.Println("[datalayer] getting cid for path:", path)
	splitPath := strings.Split(path, "/")

	dirPath := ""
	if len(splitPath) > 1 {
		dirPath = strings.Join(splitPath[:len(splitPath)-1], "/")
	}

	fmt.Println("[datalayer] resolving dir path:", dirPath)
	wrkDir, err := db.resolveWrkDir(dirPath)
	if err != nil {
		fmt.Println("[datalayer] error resolving work dir:", err)
		return nil, os.ErrNotExist
	}
	fmt.Println("[datalayer] workdir resolved successfully")

	endPath := splitPath[len(splitPath)-1]
	fmt.Println("[datalayer] looking for end path:", endPath)

	if wrkDir.leaves[endPath] != nil {
		fmt.Println("[datalayer] found in leaves map (is directory, rejecting)")
		return nil, os.ErrNotExist
	}

	// Instead of using Find() which tries to fetch the block,
	// get the link directly from the directory listing
	fmt.Println("[datalayer] getting links in directory...")
	links, err := wrkDir.Dir.Links(context.Background())
	if err != nil {
		fmt.Println("[datalayer] error getting links:", err)
		return nil, err
	}

	fmt.Println("[datalayer] work directory has", len(links), "entries")
	for _, link := range links {
		fmt.Println("[datalayer]   - '", link.Name, "' codec:", link.Cid.Prefix().Codec)
	}

	// Find the matching link by name
	for _, link := range links {
		if link.Name == endPath {
			fmt.Println("[datalayer] found link for", endPath, "with CID:", link.Cid)
			return &link.Cid, nil
		}
	}

	fmt.Println("[datalayer] link not found for path:", endPath)
	return nil, os.ErrNotExist
}

func (db *DataBin) Delete(path string) (bool, error) {
	splitPath := strings.Split(path, "/")
	//Get directory regardless of child
	var qPath string
	if len(splitPath) > 1 {
		qPath = strings.Join(splitPath[:len(splitPath)-1], "/")
	} else {
		qPath = ""
	}

	wrkDir, err := db.resolveWrkDir(qPath)

	if err != nil {
		return false, err
	}
	endPath := splitPath[len(splitPath)-1]

	if wrkDir.leaves[endPath] != nil {
		delete(wrkDir.leaves, endPath)
		wrkDir.Dir.RemoveChild(context.Background(), endPath)
		return true, nil
	} else {
		err := wrkDir.Dir.RemoveChild(context.Background(), endPath)
		if err == os.ErrNotExist {
			return false, nil
		} else {
			return true, nil
		}
	}
}

// Resolves working directory
// Errors out if path does not exist or path is a file
// Must be exact path to directory
func (db *DataBin) resolveWrkDir(path string) (*LeafDir, error) {
	fmt.Println("[datalayer] resolveWrkDir called with path:", path)

	if path == "" {
		fmt.Println("[datalayer] empty path, returning root leaf")
		return &db.Leaf, nil
	}

	splitPaths := strings.Split(path, "/")
	fmt.Println("[datalayer] split into segments:", splitPaths)

	lf := &db.Leaf
	for i, pathElement := range splitPaths {
		if pathElement == "" {
			fmt.Println("[datalayer] skipping empty segment at index", i)
			continue
		}

		fmt.Println("[datalayer] resolving segment", i+1, ":", pathElement)

		// First try the in-memory leaves map
		if lf.leaves[pathElement] != nil {
			fmt.Println("[datalayer] found in leaves map, moving to next level")
			lf = lf.leaves[pathElement]
			continue
		}

		fmt.Println("[datalayer] not in leaves map, trying Dir.Find()...")
		node, err := lf.Dir.Find(context.Background(), pathElement)
		if err != nil {
			fmt.Println("[datalayer] Dir.Find() failed:", err)
			return nil, err
		}

		fmt.Println("[datalayer] Dir.Find() succeeded, checking if it's a directory...")
		// Convert the node to a directory for the next level
		nextDir, err := uio.NewDirectoryFromNode(db.DataLayer.DagServ, node)
		if err != nil {
			fmt.Println("[datalayer] error converting to directory:", err)
			return nil, err
		}

		fmt.Println("[datalayer] successfully created directory for next level")
		// Create a new LeafDir with this directory
		lf = &LeafDir{
			Dir:    nextDir,
			leaves: make(map[string]*LeafDir),
		}
	}

	fmt.Println("[datalayer] resolveWrkDir completed successfully")
	return lf, nil
}

// Must compact to be safe. If single level
func (db *DataBin) Cid() cid.Cid {
	db.Leaf.Compact(true)
	node, _ := db.Leaf.Dir.GetNode()

	return node.Cid()
}

func (db *DataBin) Save() cid.Cid {
	db.Leaf.Compact(true)
	nodeDir, err := db.Leaf.Dir.GetNode()
	if err != nil {
		panic(err)
	}

	go func() {
		// links, _ := db.Leaf.Dir.Links(context.Background())
		var wg sync.WaitGroup
		// for _, link := range links {

		// 	wg.Add(1)
		// 	go func(link *format.Link) {
		// 		fmt.Println("Getting block", link.Cid)
		// 		blk, _ := db.DataLayer.blockServ.GetBlock(context.Background(), link.Cid)
		// 		fmt.Println("Notifying block", link.Cid)
		// 		db.DataLayer.notify(context.Background(), blk)
		// 		fmt.Println("Done block", link.Cid)
		// 		wg.Done()
		// 	}(link)
		// }
		wg.Wait()
		db.DataLayer.blockServ.AddBlock(context.Background(), nodeDir)
		db.DataLayer.bitswap.NotifyNewBlocks(context.Background(), nodeDir)

		db.DataLayer.p2pService.BroadcastCid(nodeDir.Cid())
	}()

	return nodeDir.Cid()
}

func NewDataBin(da *DataLayer) DataBin {
	uio.HAMTShardingSize = 1
	dir := uio.NewDirectory(da.DagServ)

	return DataBin{
		DataLayer: da,
		Leaf: LeafDir{
			Dir:    dir,
			leaves: make(map[string]*LeafDir),
		},
	}
}

func NewDataBinFromCid(da *DataLayer, inputCid cid.Cid) DataBin {
	return DataBin{
		DataLayer: da,
		Leaf:      newLeafFromCid(da, inputCid),
	}
}

func newLeafFromCid(da *DataLayer, inputCid cid.Cid) LeafDir {
	ctx := context.Background()

	node, _ := da.DagServ.Get(ctx, inputCid)
	dir, _ := uio.NewDirectoryFromNode(da.DagServ, node)

	links, _ := dir.Links(ctx)

	leaves := make(map[string]*LeafDir)
	for _, lnk := range links {
		if lnk.Cid.Prefix().Codec == uint64(multicodec.Protobuf) {
			lf := newLeafFromCid(da, lnk.Cid)
			leaves[lnk.Name] = &lf
		}
	}

	return LeafDir{
		Dir:    dir,
		leaves: leaves,
	}
}
