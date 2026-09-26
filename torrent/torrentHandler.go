package torrent

type Torrent struct {
	torrPath string
	destPath string
	torrName string

	localData metaInfo
}

func NewTorrent() *Torrent {
	return &Torrent{}
}
