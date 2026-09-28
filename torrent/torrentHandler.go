package torrent

type Torrent struct {
	torrPath string
	destPath string
	torrName string

	localData metaInfo
	layout    layout
}

func NewTorrent() *Torrent {
	return &Torrent{}
}
