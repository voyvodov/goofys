// Copyright 2019 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	cGroupPath         = "/proc/self/cgroup"
	cGroupFolderPrefix = "/sys/fs/cgroup/memory"
	memLimitFileSuffix = "/memory.limit_in_bytes"
	memUsageFileSuffix = "/memory.usage_in_bytes"
)

func getCgroupAvailableMem() (retVal uint64, err error) {
	//get the memory cgroup for self and send limit - usage for the cgroup

	data, err := os.ReadFile(cGroupPath)
	if err != nil {
		log.Debugf("Unable to read file %s error: %s", cGroupPath, err)
		return 0, err
	}

	path, err := getMemoryCgroupPath(string(data))
	if err != nil {
		log.Debugf("Unable to get memory cgroup path")
		return 0, err
	}

	// newer version of docker mounts the cgroup memory limit/usage files directly under
	// /sys/fs/cgroup/memory/ rather than /sys/fs/cgroup/memory/docker/$container_id/
	if _, err := os.Stat(filepath.Join(cGroupFolderPrefix, path)); err == nil {
		path = filepath.Join(cGroupFolderPrefix, path)
	} else {
		path = filepath.Join(cGroupFolderPrefix)
	}

	log.Debugf("the memory cgroup path for the current process is %v", path)

	memLimit, err := readFileAndGetValue(filepath.Join(path, memLimitFileSuffix))
	if err != nil {
		log.Debugf("Unable to get memory limit from cgroup error: %v", err)
		return 0, err
	}

	memUsage, err := readFileAndGetValue(filepath.Join(path, memUsageFileSuffix))
	if err != nil {
		log.Debugf("Unable to get memory usage from cgroup error: %v", err)
		return 0, err
	}

	return (memLimit - memUsage), nil
}

func getMemoryCgroupPath(data string) (string, error) {

	/*
	   Content of /proc/self/cgroup

	   11:hugetlb:/
	   10:memory:/user.slice
	   9:cpuset:/
	   8:blkio:/user.slice
	   7:perf_event:/
	   6:net_prio,net_cls:/
	   5:cpuacct,cpu:/user.slice
	   4:devices:/user.slice
	   3:freezer:/
	   2:pids:/
	   1:name=systemd:/user.slice/user-1000.slice/session-1759.scope
	*/

	dataArray := strings.Split(data, "\n")
	for index := range dataArray {
		kvArray := strings.Split(dataArray[index], ":")
		if len(kvArray) == 3 {
			if kvArray[1] == "memory" {
				return kvArray[2], nil
			}
		}
	}

	return "", errors.New("unable to get memory cgroup path")
}

func readFileAndGetValue(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Debugf("Unable to read file %v error: %v", path, err)
		return 0, err
	}

	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}
