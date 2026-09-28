package torrent

import "time"

type metaInfo struct {
	infoDict     infoDict
	infoHash     [20]byte
	announce     string
	announceList [][]string
	creationDate time.Time
	comment      string
	createdBy    string
	encoding     string
}

type hashBytes = [20]byte
type md5Hex = [32]byte

type infoDict struct {
	pieceLength int64
	pieces      []hashBytes
	private     bool

	singleFileInfo singleFileInfo
	multFileInfo   multFileInfo
	multipleFiles  bool
}

type singleFileInfo struct {
	name   string
	length int64
	md5sum md5Hex
}

type multFileFiles struct {
	length int64
	md5sum md5Hex
	path   []string
}
type multFileInfo struct {
	name  string
	files []multFileFiles
}
