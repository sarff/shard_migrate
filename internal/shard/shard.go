package shard

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"

	"github.com/sarff/shard_migrate/internal/config"
)

func GetShardIndex(conf *config.Config, value interface{}) int {
	if value == nil {
		return conf.ReservedShard
	}

	strValue, ok := value.(string)
	if !ok {
		return conf.ReservedShard
	}

	if strings.TrimSpace(strValue) == "" {
		return conf.ReservedShard
	}

	h := sha256.Sum256([]byte(strValue))

	hexHash := fmt.Sprintf("%x", h)

	val := new(big.Int)
	val.SetString(hexHash, 16)

	mod := new(big.Int).SetInt64(int64(conf.NumShards))
	result := new(big.Int).Mod(val, mod)

	return int(result.Int64())
}

func GetShardPath(index int, ShardDir string) string {
	return filepath.Join(ShardDir, "clients_shard_"+pad(index)+".db")
}

func pad(n int) string {
	if n < 10 {
		return "0" + fmt.Sprint(n)
	}
	return fmt.Sprint(n)
}
