//
// Shadow copy module for Mac OSX using APFS snapshot
//
//
// This module copyright 2018 Adam Marcus (https://github.com/amarcu5)
// and may be distributed under the same terms as Duplicacy.

package duplicacy

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

var snapshotPath string
var snapshotDate string

// Converts char array to string
func CharsToString(ca []int8) string {

	len := len(ca)
	ba := make([]byte, len) 

	for i, v := range ca {
		ba[i] = byte(v)
		if ba[i] == 0 {
			len = i
			break
		}
	}

	return string(ba[:len])
}

// Get ID of device containing path
func GetPathDeviceId(path string) (deviceId int32, err error) {

	stat := syscall.Stat_t{}

	err = syscall.Stat(path, &stat)
	if err != nil {
		return 0, err
	}

	return stat.Dev, nil
}

// Executes shell command with timeout and returns stdout
func CommandWithTimeout(timeoutInSeconds int, name string, arg ...string) (output string, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutInSeconds) * time.Second)
	defer cancel() 

	cmd := exec.CommandContext(ctx, name, arg...)
	out, err := cmd.Output()

	if ctx.Err() == context.DeadlineExceeded {
		err = errors.New("Command '" + name + "' timed out")
	}

	output = string(out)
	return output, err
}

func DeleteShadowCopy() {

	if snapshotPath == "" {
		return
	}

	err := exec.Command("/sbin/umount", "-f", snapshotPath).Run()
	if err != nil {
		LOG_ERROR("VSS_DELETE", "Error while unmounting snapshot")
		return
	}

	err = exec.Command("tmutil", "deletelocalsnapshots", snapshotDate).Run()
	if err != nil {
		LOG_ERROR("VSS_DELETE", "Error while deleting local snapshot")
		return
	}

	err = os.RemoveAll(snapshotPath)
	if err != nil {
		LOG_ERROR("VSS_DELETE", "Error while deleting temporary mount directory")
		return
	}
	
	LOG_INFO("VSS_DELETE", "Shadow copy unmounted and deleted at %s", snapshotPath)

	snapshotPath = "" 
}

func CreateShadowCopy(top string, shadowCopy bool, timeoutInSeconds int) (shadowTop string) {

	if !shadowCopy {
		return top
	}

	// Check repository filesystem is APFS
	stat := syscall.Statfs_t{}
	err := syscall.Statfs(top, &stat)
	if err != nil {
		LOG_ERROR("VSS_INIT", "Unable to determine filesystem of repository path")
		return top
	}
	if CharsToString(stat.Fstypename[:]) != "apfs" {
		LOG_WARN("VSS_INIT", "VSS requires APFS filesystem")
		return top
	}

	// Check path is local as tmutil snapshots will not support APFS formatted external drives
	deviceIdLocal, err := GetPathDeviceId("/")
	if err != nil {
		LOG_ERROR("VSS_INIT", "Unable to get device ID of path: /")
		return top
	}
	deviceIdRepository, err := GetPathDeviceId(top)
	if err != nil {
		LOG_ERROR("VSS_INIT", "Unable to get device ID of path: ", top)
		return top
	}
	if deviceIdLocal != deviceIdRepository {
		LOG_WARN("VSS_PATH", "VSS not supported for non-local repository path: ", top)
		return top 
	}

	if timeoutInSeconds <= 60 {
		timeoutInSeconds = 60
	}

	// Create mount point
	snapshotPath, err = ioutil.TempDir("/tmp/", "snp_")
	if err != nil {
		LOG_ERROR("VSS_CREATE", "Failed to create temporary mount directory")
		return top
	}

	// Use tmutil to create snapshot
	tmutilOutput, err := CommandWithTimeout(timeoutInSeconds, "tmutil", "snapshot")
	if err != nil {
		LOG_ERROR("VSS_CREATE", "Error while calling tmutil: ", err)
		return top
	}

	colonPos := strings.IndexByte(tmutilOutput, ':')
	if colonPos < 0 {
		LOG_ERROR("VSS_CREATE", "Snapshot creation failed: ", tmutilOutput)
		return top
	}
	snapshotDate = strings.TrimSpace(tmutilOutput[colonPos+1:])

	// Mount snapshot as readonly and hide from GUI i.e. Finder
	_, err = CommandWithTimeout(timeoutInSeconds, 
		"/sbin/mount", "-t", "apfs", "-o", "nobrowse,-r,-s=com.apple.TimeMachine." + snapshotDate, "/", snapshotPath)
	if err != nil {
		LOG_ERROR("VSS_CREATE", "Error while mounting snapshot: ", err)
		return top
	}

	LOG_INFO("VSS_DONE", "Shadow copy created and mounted at %s", snapshotPath)

	return snapshotPath + top
}
