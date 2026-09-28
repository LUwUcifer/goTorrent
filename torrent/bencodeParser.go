package torrent

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"goTor/bencoder"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (tor *Torrent) fileInfoParser(path, dest string) error {
	err := tor.populateTorrentPath(path, dest)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	err = tor.torrentFilePopulator()
	if err != nil {
		slog.Error(err.Error())
		return err
	}
	return nil
}

func (tor *Torrent) populateTorrentPath(path, dest string) error {
	path, dest = filepath.Clean(path), filepath.Clean(dest)
	var _, torrentName string = filepath.Split(path)

	fileExt := filepath.Ext(torrentName)
	if fileExt != ".torrent" {
		slog.Error("Invalid File Extension", "torrent", torrentName)
		return errors.New("file extension not supported")
	}

	torrentName = strings.TrimSuffix(torrentName, fileExt)
	if torrentName == "" {
		torrentName = fmt.Sprintf("Torrent_%d", rand.Int63())
		slog.Debug("Torrent name is empty", "torrent", torrentName)
	}
	slog.Info("torrent path, destination validated", "path", path, "dest", dest)

	tor.torrPath = path
	tor.destPath = dest
	tor.torrName = torrentName

	return nil
}

func (tor *Torrent) torrentFilePopulator() error {
	path := tor.torrPath
	torrentName := tor.torrName

	torrentFileData, err := os.ReadFile(path)
	if err != nil {
		slog.Error("Error reading torrent file", "name", torrentName, "error", err)
		return err
	}

	dec := bencoder.NewDecoderBytes(torrentFileData)
	decoded, err := dec.Decode()
	if err != nil {
		slog.Error("Error decoding torrent file", "name", torrentName, "error", err)
		return err
	}
	if n := dec.Remaining(); n != 0 {
		slog.Warn("trailing data after torrent dictionary", "name", torrentName, "bytes", n)
	}

	metaInfoMap, ok := decoded.(map[string]any)
	if !ok {
		err := errors.New("error decoding torrent file: not a map")
		slog.Error(err.Error(), "name", torrentName)
		return err
	}

	infoBytes, ok := dec.InfoBytes()
	if !ok {
		err := errors.New("error decoding torrent file: no info dictionary")
		slog.Error(err.Error(), "name", torrentName)
		return err
	}
	infoHash := sha1.Sum(infoBytes)

	if err := tor.populateMetaInfo(metaInfoMap, infoHash); err != nil {
		slog.Error("Error populating torrent file", "name", torrentName, "error", err)
		return err
	}
	return nil
}

func (tor *Torrent) populateMetaInfo(raw map[string]any, infoHash hashBytes) error {
	slog.Debug("populating meta info", "keys", len(raw))

	var mi metaInfo
	mi.infoHash = infoHash

	if announce, ok := raw["announce"].(string); ok {
		mi.announce = announce
	} else {
		slog.Warn("announce field missing or not a string")
	}

	if annList, ok := raw["announce-list"].([]any); ok {
		for i, tierRaw := range annList {
			var tierCounter []string
			tier, ok := tierRaw.([]any)
			if !ok {
				slog.Warn("announce-list tier is not a list, skipping", "index", i)
				continue
			}
			for _, urlRaw := range tier {
				if url, ok := urlRaw.(string); ok {
					tierCounter = append(tierCounter, url)
				} else {
					slog.Warn("announce-list url is not a string, skipping", "tier", i)
				}
			}
			if len(tierCounter) == 0 {
				slog.Warn("announce-list tier has no usable urls, skipping", "index", i)
				continue
			}
			mi.announceList = append(mi.announceList, tierCounter)
		}
		slog.Debug("parsed announce-list", "tiers", len(mi.announceList))
	}

	if createdRaw, ok := raw["creation date"].(int64); ok {
		mi.creationDate = time.Unix(createdRaw, 0)
	}

	if comment, ok := raw["comment"].(string); ok {
		mi.comment = comment
	}

	if createdBy, ok := raw["created by"].(string); ok {
		mi.createdBy = createdBy
	}

	if encoding, ok := raw["encoding"].(string); ok {
		mi.encoding = encoding
	}

	infoRaw, ok := raw["info"].(map[string]any)
	if !ok {
		slog.Error("missing or invalid info dictionary")
		return errors.New("missing or invalid info dictionary")
	}

	info, err := parseInfoDict(infoRaw)
	if err != nil {
		slog.Error("failed to parse info dictionary", "error", err)
		return err
	}
	mi.infoDict = info

	lay, err := buildLayout(info)
	if err != nil {
		slog.Error("invalid torrent layout", "error", err)
		return err
	}

	tor.localData = mi
	tor.layout = lay
	slog.Info("meta info populated",
		"announce", mi.announce,
		"infoHash", fmt.Sprintf("%x", mi.infoHash[:]),
		"multipleFiles", info.multipleFiles,
		"files", len(lay.files),
		"totalLength", lay.totalLength,
		"pieces", lay.numPieces,
	)
	return nil
}

func parseInfoDict(raw map[string]any) (infoDict, error) {
	slog.Debug("parsing info dictionary", "keys", len(raw))

	var info infoDict

	pieceLength, ok := raw["piece length"].(int64)
	if !ok {
		slog.Error("missing or invalid piece length")
		return info, errors.New("missing or invalid piece length")
	}
	info.pieceLength = pieceLength

	piecesStr, ok := raw["pieces"].(string)
	if !ok {
		slog.Error("missing or invalid pieces")
		return info, errors.New("missing or invalid pieces")
	}
	pieceBytes := []byte(piecesStr)
	if len(pieceBytes)%20 != 0 {
		slog.Error("pieces length is not a multiple of 20", "length", len(pieceBytes))
		return info, errors.New("pieces length is not a multiple of 20")
	}
	info.pieces = make([]hashBytes, 0, len(pieceBytes)/20)
	for i := 0; i < len(pieceBytes); i += 20 {
		var h hashBytes
		copy(h[:], pieceBytes[i:i+20])
		info.pieces = append(info.pieces, h)
	}
	slog.Debug("parsed pieces", "count", len(info.pieces), "pieceLength", info.pieceLength)

	if privateRaw, ok := raw["private"].(int64); ok {
		info.private = privateRaw == 1
	} else {
		slog.Debug("private field missing or not an int, defaulting to false")
	}

	if filesRaw, ok := raw["files"].([]any); ok {
		info.multipleFiles = true
		slog.Info("multi-file torrent detected", "fileCount", len(filesRaw))
		multi, err := parseMultiFileInfo(raw, filesRaw)
		if err != nil {
			slog.Error("failed to parse multi-file info", "error", err)
			return info, err
		}
		info.multFileInfo = multi
		return info, nil
	}

	slog.Info("single-file torrent detected")
	single, err := parseSingleFileInfo(raw)
	if err != nil {
		slog.Error("failed to parse single-file info", "error", err)
		return info, err
	}
	info.singleFileInfo = single

	return info, nil
}

func parseSingleFileInfo(raw map[string]any) (singleFileInfo, error) {
	slog.Debug("parsing single-file info")

	var sf singleFileInfo

	name, ok := raw["name"].(string)
	if !ok {
		slog.Error("missing or invalid name")
		return sf, errors.New("missing or invalid name")
	}
	sf.name = name

	length, ok := raw["length"].(int64)
	if !ok {
		slog.Error("missing or invalid length", "name", name)
		return sf, errors.New("missing or invalid length")
	}
	sf.length = length

	// Optional; the zero value means "not provided".
	sf.md5sum, _ = parseMD5(raw, "name", name)

	slog.Info("parsed single-file info", "name", sf.name, "length", sf.length)
	return sf, nil
}

func parseMultiFileInfo(raw map[string]any, filesRaw []any) (multFileInfo, error) {
	slog.Debug("parsing multi-file info", "entries", len(filesRaw))

	var mf multFileInfo

	name, ok := raw["name"].(string)
	if !ok {
		slog.Error("missing or invalid name")
		return mf, errors.New("missing or invalid name")
	}
	mf.name = name

	mf.files = make([]multFileFiles, 0, len(filesRaw))
	for i, fileEntryRaw := range filesRaw {
		// Skipping a bad entry would shift the offset of every later file
		// and silently corrupt the download, so any malformed entry is fatal.
		entry, ok := fileEntryRaw.(map[string]any)
		if !ok {
			slog.Error("file entry is not a dictionary", "index", i)
			return mf, fmt.Errorf("file entry %d is not a dictionary", i)
		}

		var f multFileFiles

		length, ok := entry["length"].(int64)
		if !ok {
			slog.Error("missing or invalid file length", "index", i)
			return mf, fmt.Errorf("missing or invalid length for file %d", i)
		}
		f.length = length

		// Optional; the zero value means "not provided".
		f.md5sum, _ = parseMD5(entry, "index", i)

		pathRaw, ok := entry["path"].([]any)
		if !ok {
			slog.Error("file entry missing path", "index", i)
			return mf, fmt.Errorf("missing or invalid path for file %d", i)
		}
		for _, p := range pathRaw {
			s, ok := p.(string)
			if !ok {
				// Dropping a segment would change where the file lands.
				slog.Error("path segment is not a string", "index", i)
				return mf, fmt.Errorf("non-string path segment in file %d", i)
			}
			f.path = append(f.path, s)
		}

		mf.files = append(mf.files, f)
	}

	slog.Info("parsed multi-file info", "name", mf.name, "fileCount", len(mf.files))
	return mf, nil
}
