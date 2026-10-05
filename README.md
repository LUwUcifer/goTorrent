# goTorrent

This repository implements BitTorrent specified protocols and algorithms in golang, utilizing only STL packages

## Installation
Install the package using 
```
cd ~
git clone github.com/LUwUcifer/goTorrent
cd ~/goTorrent
go build
./goTor path/to/torrent/file.torrent
```

The download will automatically start at `~/goTorrent/downloads`

### Current Progress
* Functioning parallel leeching
* Pause/Resume
* Quit/Resume
* CLI Download

### Upcoming Updates
* Portability to other Operating Systems than Linux
* Debug Seeding
* Cleanup Code
* Streamline concurrency
* Better Error Handling
* Testcases
