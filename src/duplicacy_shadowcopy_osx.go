//
// Shadow copy module for Mac OSX using APFS snapshot
//
//
// This module copyright 2018 Adam Marcus (https://github.com/amarcu5)
// and may be distributed under the same terms as Duplicacy.

package duplicacy

import (
	"os"
	"strings"
	"io/ioutil"
	"os/exec"
	"context"
	"errors"
	"time"
	"syscall"
	"strconv"
)

var snapshotPath string
var snapshotDate string

func GetKernelVersion() (major int, minor int, component int, err error) {
	
	versionString, err := syscall.Sysctl("kern.osrelease")
	if err != nil {
		return 0, 0, 0, err;
	}
	
	versionSplit := strings.Split(versionString, ".")
	if len(versionSplit) != 3 {
		return 0, 0, 0, errors.New("Sysctl returned invalid kernel version string")
	}
	
	major, _ = strconv.Atoi(versionSplit[0])
	minor, _ = strconv.Atoi(versionSplit[1])
	component, _ = strconv.Atoi(versionSplit[2])
	
	return major, minor, component, nil;
}

func CommandWithTimeout(timeoutInSeconds int, name string, arg ...string) (output string, err error) {
  
    ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutInSeconds) * time.Second)
	defer cancel() 
	
	cmd := exec.CommandContext(ctx, name, arg...)
	out, err := cmd.Output()
	
    if ctx.Err() == context.DeadlineExceeded {
	    err = errors.New("Command '" + name + "' timed out")
	}
	
	output = string(out)
	return output, err;
}

func DeleteShadowCopy() {
  
    if snapshotPath == "" {
	    return
    }

	err := exec.Command("umount", "-f", snapshotPath).Run()
    if err != nil {
	    LOG_ERROR("VSS_DELETE", "Error while unmounting snapshot")
		return
	}
	
	err = exec.Command("tmutil", "deletelocalsnapshots", snapshotDate).Run()
    if err != nil {
        LOG_ERROR("VSS_DELETE", "Error while deleting local snapshot")
		return
	}
  
    os.RemoveAll(snapshotPath)
  
    LOG_INFO("VSS_DELETE", "Shadow copy unmounted and deleted at %s", snapshotPath)
	
    snapshotPath = "" 
}

func CreateShadowCopy(top string, shadowCopy bool, timeoutInSeconds int) (shadowTop string) {

	if !shadowCopy {
		return top
	}
	
	major, _, _, err := GetKernelVersion()
	if err != nil {
		LOG_ERROR("VSS_INIT", "Failed to get kernel version: " + err.Error())
		return top
	}
	
	if major < 17 {
		LOG_WARN("VSS_INIT", "VSS requires macOS 10.13 High Sierra or higher")
		return top
	}
	
	if top == "/Volumes" || strings.HasPrefix(top, "/Volumes/") {
		LOG_ERROR("VSS_PATH", "Invalid repository path: %s", top)
		return top 
	}

	if timeoutInSeconds <= 60 {
        timeoutInSeconds = 60
	}
	
    snapshotPath, err = ioutil.TempDir("/tmp/", "snp_")
	if err != nil {
	    LOG_ERROR("VSS_CREATE", "Failed to create temporary mount directory")
		return top
	}
	
	tmutilOutput, err := CommandWithTimeout(timeoutInSeconds, "tmutil", "snapshot")
    if err != nil {
	    LOG_ERROR("VSS_CREATE", "Error while calling tmutil: " + err.Error())
		return top
	}
	
	colonPos := strings.IndexByte(tmutilOutput, ':')
	if colonPos < 0 {
	    LOG_ERROR("VSS_CREATE", "Snapshot creation failed: " + tmutilOutput)
		return top
	}
	snapshotDate = strings.TrimSpace(tmutilOutput[colonPos+1:])
	
	_, err = CommandWithTimeout(timeoutInSeconds, 
		"mount", "-t", "apfs", "-o", "nobrowse,-r,-s=com.apple.TimeMachine." + snapshotDate, "/", snapshotPath)
	if err != nil {
	    LOG_ERROR("VSS_CREATE", "Error while mounting snapshot: " + err.Error())
        return top
	}
	
    LOG_INFO("VSS_DONE", "Shadow copy created and mounted at %s", snapshotPath)
	
	return snapshotPath + top
}
