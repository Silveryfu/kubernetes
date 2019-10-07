package cm

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/opencontainers/runc/libcontainer/cgroups"
)

const minCpuShare = 2

type Tracker struct {
	mu            sync.Mutex
	cpuCgroupRoot string
}

func NewTracker() (*Tracker, error) {
	root, err := cgroups.FindCgroupMountpointDir()
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(root); err != nil {
		return nil, err
	}

	cpuRoot := filepath.Join(root, "cpu")
	if _, err := os.Stat(cpuRoot); err != nil {
		return nil, fmt.Errorf("unable to stat cgroup cpu subsystem: %v", err)
	}

	return &Tracker{
		mu:            sync.Mutex{},
		cpuCgroupRoot: cpuRoot,
	}, nil
}

// Make the sum of each subtree's share equal to the parent;
// This unification applies to / -> system.slice/docker.service and / -> kubepods/pods only;
func (t *Tracker) Unify() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO:
	return nil
}

// Given a source and destination node, lend cpu shares from the source to the destination, making sure
// the upstream nodes (from the source) and the downstream nodes (to the destination) are updated
// accordingly; Steal() assumes the cpu hierarchy is unified, that is, for every node in the hierarchy,
// the sum of each subtree's share equal to the node's share;
// Src and dest is represented as the full path relative to the cgroup cpu root.
// TODO: make the cgroup fs update transactional -- either all succeed or none
func (t *Tracker) Steal(src []string, dest []string, share int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// index of the youngest common parents between the src and dest
	cpi := func() int {
		i := 0
		for ; i < len(src) && i < len(dest); i++ {
			if src[i] != dest[i] {
				break
			}
		}
		return i
	}()

	// parallel update
	numUpdate := len(src) + len(dest) - 2*cpi
	suc := make(chan error, numUpdate)

	var wg sync.WaitGroup
	wg.Add(numUpdate)

	// update upstream nodes until the shared parent
	for i := len(src); i > cpi; i-- {
		node := filepath.Join(t.cpuCgroupRoot, filepath.Join(src[0:i]...))
		go func() {
			defer wg.Done()
			suc <- incCpuShares(node, -share)
		}()
	}

	// update downstream nodes
	for i := len(dest); i > cpi; i-- {
		node := filepath.Join(t.cpuCgroupRoot, filepath.Join(dest[0:i]...))
		go func() {
			defer wg.Done()
			suc <- incCpuShares(node, share)
		}()
	}

	wg.Wait()
	close(suc)

	for e := range suc {
		if e != nil {
			return e
		}
	}
	return nil
}

func incCpuShares(path string, s int64) (err error) {
	var cpuShares int64

	fd, err := os.OpenFile(filepath.Join(path, "cpu.shares"), os.O_RDWR, 0644)
	if err != nil {
		return
	}

	defer func() {
		cerr := fd.Close()
		if err == nil {
			err = cerr
		}
	}()

	_, err = fmt.Fscanf(fd, "%d", &cpuShares)

	newCpuShares := cpuShares + s

	if newCpuShares < minCpuShare {
		newCpuShares = minCpuShare
	}

	_, err = fd.WriteString(strconv.FormatInt(newCpuShares, 10))
	return
}
