// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"os"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	ole "github.com/gilbertchen/go-ole"
)

//507C37B4-CF5B-4e95-B0AF-14EB9767467E
var IID_IVSS_ASYNC = &ole.GUID{0x507C37B4, 0xCF5B, 0x4e95, [8]byte{0xb0, 0xaf, 0x14, 0xeb, 0x97, 0x67, 0x46, 0x7e}}

type IVSSAsync struct {
	ole.IUnknown
}

type IVSSAsyncVtbl struct {
	ole.IUnknownVtbl
	cancel      uintptr
	wait        uintptr
	queryStatus uintptr
}

func (async *IVSSAsync) VTable() *IVSSAsyncVtbl {
	return (*IVSSAsyncVtbl)(unsafe.Pointer(async.RawVTable))
}

var VSS_S_ASYNC_PENDING int32 = 0x00042309
var VSS_S_ASYNC_FINISHED int32 = 0x0004230A
var VSS_S_ASYNC_CANCELLED int32 = 0x0004230B

func (async *IVSSAsync) Wait(seconds int) bool {

	startTime := time.Now().Unix()
	for {
		ret, _, _ := syscall.Syscall(async.VTable().wait, 2, uintptr(unsafe.Pointer(async)), uintptr(1000), 0)
		if ret != 0 {
			LOG_WARN("IVSSASYNC_WAIT", "IVssAsync::Wait returned %d\n", ret)
		}

		var status int32
		ret, _, _ = syscall.Syscall(async.VTable().queryStatus, 3, uintptr(unsafe.Pointer(async)),
			uintptr(unsafe.Pointer(&status)), 0)
		if ret != 0 {
			LOG_WARN("IVSSASYNC_QUERY", "IVssAsync::QueryStatus returned %d\n", ret)
		}

		if status == VSS_S_ASYNC_FINISHED {
			return true
		}
		if time.Now().Unix()-startTime > int64(seconds) {
			LOG_WARN("IVSSASYNC_TIMEOUT", "IVssAsync is pending for more than %d seconds\n", seconds)
			return false
		}
	}
}

func getIVSSAsync(unknown *ole.IUnknown, iid *ole.GUID) (async *IVSSAsync) {
	r, _, _ := syscall.Syscall(
		unknown.VTable().QueryInterface,
		3,
		uintptr(unsafe.Pointer(unknown)),
		uintptr(unsafe.Pointer(iid)),
		uintptr(unsafe.Pointer(&async)))

	if r != 0 {
		LOG_WARN("IVSSASYNC_QUERY", "IVSSAsync::QueryInterface returned %d\n", r)
		return nil
	}
	return
}

//665c1d5f-c218-414d-a05d-7fef5f9d5c86
var IID_IVSS = &ole.GUID{0x665c1d5f, 0xc218, 0x414d, [8]byte{0xa0, 0x5d, 0x7f, 0xef, 0x5f, 0x9d, 0x5c, 0x86}}

type IVSS struct {
	ole.IUnknown
}

type IVSSVtbl struct {
	ole.IUnknownVtbl
	getWriterComponentsCount      uintptr
	getWriterComponents           uintptr
	initializeForBackup           uintptr
	setBackupState                uintptr
	initializeForRestore          uintptr
	setRestoreState               uintptr
	gatherWriterMetadata          uintptr
	getWriterMetadataCount        uintptr
	getWriterMetadata             uintptr
	freeWriterMetadata            uintptr
	addComponent                  uintptr
	prepareForBackup              uintptr
	abortBackup                   uintptr
	gatherWriterStatus            uintptr
	getWriterStatusCount          uintptr
	freeWriterStatus              uintptr
	getWriterStatus               uintptr
	setBackupSucceeded            uintptr
	setBackupOptions              uintptr
	setSelectedForRestore         uintptr
	setRestoreOptions             uintptr
	setAdditionalRestores         uintptr
	setPreviousBackupStamp        uintptr
	saveAsXML                     uintptr
	backupComplete                uintptr
	addAlternativeLocationMapping uintptr
	addRestoreSubcomponent        uintptr
	setFileRestoreStatus          uintptr
	addNewTarget                  uintptr
	setRangesFilePath             uintptr
	preRestore                    uintptr
	postRestore                   uintptr
	setContext                    uintptr
	startSnapshotSet              uintptr
	addToSnapshotSet              uintptr
	doSnapshotSet                 uintptr
	deleteSnapshots               uintptr
	importSnapshots               uintptr
	breakSnapshotSet              uintptr
	getSnapshotProperties         uintptr
	query                         uintptr
	isVolumeSupported             uintptr
	disableWriterClasses          uintptr
	enableWriterClasses           uintptr
	disableWriterInstances        uintptr
	exposeSnapshot                uintptr
	revertToSnapshot              uintptr
	queryRevertStatus             uintptr
}

func (vss *IVSS) VTable() *IVSSVtbl {
	return (*IVSSVtbl)(unsafe.Pointer(vss.RawVTable))
}

func (vss *IVSS) InitializeForBackup() int {
	ret, _, _ := syscall.Syscall(vss.VTable().initializeForBackup, 2, uintptr(unsafe.Pointer(vss)), 0, 0)
	return int(ret)
}

func (vss *IVSS) GatherWriterMetadata() (int, *IVSSAsync) {
	var unknown *ole.IUnknown
	ret, _, _ := syscall.Syscall(vss.VTable().gatherWriterMetadata, 2,
		uintptr(unsafe.Pointer(vss)),
		uintptr(unsafe.Pointer(&unknown)), 0)

	if ret != 0 {
		return int(ret), nil
	} else {
		return int(ret), getIVSSAsync(unknown, IID_IVSS_ASYNC)
	}
}

func (vss *IVSS) StartSnapshotSet(snapshotID *ole.GUID) int {
	ret, _, _ := syscall.Syscall(vss.VTable().startSnapshotSet, 2,
		uintptr(unsafe.Pointer(vss)),
		uintptr(unsafe.Pointer(snapshotID)), 0)
	return int(ret)
}

func (vss *IVSS) AddToSnapshotSet(drive string, snapshotID *ole.GUID) int {

	volumeName := syscall.StringToUTF16Ptr(drive)

	var ret uintptr
	if runtime.GOARCH == "386" {
		// On 32-bit Windows, GUID is passed by value
		ret, _, _ = syscall.Syscall9(vss.VTable().addToSnapshotSet, 7,
			uintptr(unsafe.Pointer(vss)),
			uintptr(unsafe.Pointer(volumeName)),
			0, 0, 0, 0,
			uintptr(unsafe.Pointer(snapshotID)), 0, 0)
	} else {
		ret, _, _ = syscall.Syscall6(vss.VTable().addToSnapshotSet, 4,
			uintptr(unsafe.Pointer(vss)),
			uintptr(unsafe.Pointer(volumeName)),
			uintptr(unsafe.Pointer(ole.IID_NULL)),
			uintptr(unsafe.Pointer(snapshotID)), 0, 0)
	}
	return int(ret)
}

func (vss *IVSS) SetBackupState() int {
	VSS_BT_COPY := 5
	ret, _, _ := syscall.Syscall6(vss.VTable().setBackupState, 4,
		uintptr(unsafe.Pointer(vss)),
		0, 0, uintptr(VSS_BT_COPY), 0, 0)
	return int(ret)
}

func (vss *IVSS) PrepareForBackup() (int, *IVSSAsync) {
	var unknown *ole.IUnknown
	ret, _, _ := syscall.Syscall(vss.VTable().prepareForBackup, 2,
		uintptr(unsafe.Pointer(vss)),
		uintptr(unsafe.Pointer(&unknown)), 0)

	if ret != 0 {
		return int(ret), nil
	} else {
		return int(ret), getIVSSAsync(unknown, IID_IVSS_ASYNC)
	}
}

func (vss *IVSS) DoSnapshotSet() (int, *IVSSAsync) {
	var unknown *ole.IUnknown
	ret, _, _ := syscall.Syscall(vss.VTable().doSnapshotSet, 2,
		uintptr(unsafe.Pointer(vss)),
		uintptr(unsafe.Pointer(&unknown)), 0)

	if ret != 0 {
		return int(ret), nil
	} else {
		return int(ret), getIVSSAsync(unknown, IID_IVSS_ASYNC)
	}
}

type SnapshotProperties struct {
	SnapshotID           ole.GUID
	SnapshotSetID        ole.GUID
	SnapshotsCount       uint32
	SnapshotDeviceObject *uint16
	OriginalVolumeName   *uint16
	OriginatingMachine   *uint16
	ServiceMachine       *uint16
	ExposedName          *uint16
	ExposedPath          *uint16
	ProviderId           ole.GUID
	SnapshotAttributes   uint32
	CreationTimestamp    int64
	Status               int
}

func (vss *IVSS) GetSnapshotProperties(snapshotSetID ole.GUID, properties *SnapshotProperties) int {
	var ret uintptr
	if runtime.GOARCH == "386" {
		address := uint(uintptr(unsafe.Pointer(&snapshotSetID)))
		ret, _, _ = syscall.Syscall6(vss.VTable().getSnapshotProperties, 6,
			uintptr(unsafe.Pointer(vss)),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address)))),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address + 4)))),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address + 8)))),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address + 12)))),
			uintptr(unsafe.Pointer(properties)))
	} else {
		ret, _, _ = syscall.Syscall(vss.VTable().getSnapshotProperties, 3,
			uintptr(unsafe.Pointer(vss)),
			uintptr(unsafe.Pointer(&snapshotSetID)),
			uintptr(unsafe.Pointer(properties)))
	}
	return int(ret)
}

func (vss *IVSS) DeleteSnapshots(snapshotID ole.GUID) (int, int, ole.GUID) {

	VSS_OBJECT_SNAPSHOT := 3

	deleted := int32(0)

	var deletedGUID ole.GUID

	var ret uintptr
	if runtime.GOARCH == "386" {
		address := uint(uintptr(unsafe.Pointer(&snapshotID)))
		ret, _, _ = syscall.Syscall9(vss.VTable().deleteSnapshots, 9,
			uintptr(unsafe.Pointer(vss)),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address)))),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address + 4)))),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address + 8)))),
			uintptr(*(*uint32)(unsafe.Pointer(uintptr(address + 12)))),
			uintptr(VSS_OBJECT_SNAPSHOT),
			uintptr(1),
			uintptr(unsafe.Pointer(&deleted)),
			uintptr(unsafe.Pointer(&deletedGUID)))
	} else {
		ret, _, _ = syscall.Syscall6(vss.VTable().deleteSnapshots, 6,
			uintptr(unsafe.Pointer(vss)),
			uintptr(unsafe.Pointer(&snapshotID)),
			uintptr(VSS_OBJECT_SNAPSHOT),
			uintptr(1),
			uintptr(unsafe.Pointer(&deleted)),
			uintptr(unsafe.Pointer(&deletedGUID)))
	}

	return int(ret), int(deleted), deletedGUID
}

func uint16ArrayToString(p *uint16) string {
	if p == nil {
		return ""
	}
	s := make([]uint16, 0)
	address := uintptr(unsafe.Pointer(p))
	for {
		c := *(*uint16)(unsafe.Pointer(address))
		if c == 0 {
			break
		}

		s = append(s, c)
		address = uintptr(int(address) + 2)
	}

	return syscall.UTF16ToString(s)
}

func getIVSS(unknown *ole.IUnknown, iid *ole.GUID) (ivss *IVSS) {
	r, _, _ := syscall.Syscall(
		unknown.VTable().QueryInterface,
		3,
		uintptr(unsafe.Pointer(unknown)),
		uintptr(unsafe.Pointer(iid)),
		uintptr(unsafe.Pointer(&ivss)))

	if r != 0 {
		LOG_WARN("IVSS_QUERY", "IVSS::QueryInterface returned %d\n", r)
		return nil
	}

	return ivss
}

var vssBackupComponent *IVSS
var snapshotID ole.GUID
var shadowLink string

func DeleteShadowCopy() {
	if vssBackupComponent != nil {
		defer vssBackupComponent.Release()

		LOG_TRACE("VSS_DELETE", "Deleting the shadow copy used for this backup")
		ret, _, _ := vssBackupComponent.DeleteSnapshots(snapshotID)
		if ret != 0 {
			LOG_WARN("VSS_DELETE", "Failed to delete the shadow copy: %x\n", uint(ret))
		} else {
			LOG_INFO("VSS_DELETE", "The shadow copy has been successfully deleted")
		}
	}

	if shadowLink != "" {
		err := os.Remove(shadowLink)
		if err != nil {
			LOG_WARN("VSS_SYMLINK", "Failed to remove the symbolic link for the shadow copy: %v", err)
		}
	}

	ole.CoUninitialize()
}

func CreateShadowCopy(top string, shadowCopy bool, timeoutInSeconds int) (shadowTop string) {

	if !shadowCopy {
		return top
	}

	if timeoutInSeconds <= 60 {
		timeoutInSeconds = 60
	}
	ole.CoInitialize(0)
	defer ole.CoUninitialize()

	dllVssApi := syscall.NewLazyDLL("VssApi.dll")
	procCreateVssBackupComponents :=
		dllVssApi.NewProc("?CreateVssBackupComponents@@YAJPEAPEAVIVssBackupComponents@@@Z")
	if runtime.GOARCH == "386" {
		procCreateVssBackupComponents =
			dllVssApi.NewProc("?CreateVssBackupComponents@@YGJPAPAVIVssBackupComponents@@@Z")
	}

	if len(top) < 3 || top[1] != ':' || (top[2] != '/' && top[2] != '\\') {
		LOG_ERROR("VSS_PATH", "Invalid repository path: %s", top)
		return top
	}
	volume := top[:1] + ":\\"

	LOG_INFO("VSS_CREATE", "Creating a shadow copy for %s", volume)

	var unknown *ole.IUnknown
	r, _, err := procCreateVssBackupComponents.Call(uintptr(unsafe.Pointer(&unknown)))

	if r == 0x80070005 {
		LOG_ERROR("VSS_CREATE", "Only administrators can create shadow copies")
		return top
	}

	if r != 0 {
		LOG_ERROR("VSS_CREATE", "Failed to create the VSS backup component: %d", r)
		return top
	}

	vssBackupComponent = getIVSS(unknown, IID_IVSS)
	if vssBackupComponent == nil {
		LOG_ERROR("VSS_CREATE", "Failed to create the VSS backup component")
		return top
	}

	ret := vssBackupComponent.InitializeForBackup()
	if ret != 0 {
		LOG_ERROR("VSS_INIT", "Shadow copy creation failed: InitializeForBackup returned %x", uint(ret))
		return top
	}

	var async *IVSSAsync
	ret, async = vssBackupComponent.GatherWriterMetadata()
	if ret != 0 {
		LOG_ERROR("VSS_GATHER", "Shadow copy creation failed: GatherWriterMetadata returned %x", uint(ret))
		return top
	}

	if async == nil {
		LOG_ERROR("VSS_GATHER",
			"Shadow copy creation failed: GatherWriterMetadata failed to return a valid IVssAsync object")
		return top
	}

	if !async.Wait(timeoutInSeconds) {
		LOG_ERROR("VSS_GATHER", "Shadow copy creation failed: GatherWriterMetadata didn't finish properly")
		return top
	}
	async.Release()

	var snapshotSetID ole.GUID

	ret = vssBackupComponent.StartSnapshotSet(&snapshotSetID)
	if ret != 0 {
		LOG_ERROR("VSS_START", "Shadow copy creation failed: StartSnapshotSet returned %x", uint(ret))
		return top
	}

	ret = vssBackupComponent.AddToSnapshotSet(volume, &snapshotID)
	if ret != 0 {
		LOG_ERROR("VSS_ADD", "Shadow copy creation failed: AddToSnapshotSet returned %x", uint(ret))
		return top
	}

	s, _ := ole.StringFromIID(&snapshotID)
	LOG_DEBUG("VSS_ID", "Creating shadow copy %s", s)

	ret = vssBackupComponent.SetBackupState()
	if ret != 0 {
		LOG_ERROR("VSS_SET", "Shadow copy creation failed: SetBackupState returned %x", uint(ret))
		return top
	}

	ret, async = vssBackupComponent.PrepareForBackup()
	if ret != 0 {
		LOG_ERROR("VSS_PREPARE", "Shadow copy creation failed: PrepareForBackup returned %x", uint(ret))
		return top
	}
	if async == nil {
		LOG_ERROR("VSS_PREPARE",
			"Shadow copy creation failed: PrepareForBackup failed to return a valid IVssAsync object")
		return top
	}

	if !async.Wait(timeoutInSeconds) {
		LOG_ERROR("VSS_PREPARE", "Shadow copy creation failed: PrepareForBackup didn't finish properly")
		return top
	}
	async.Release()

	ret, async = vssBackupComponent.DoSnapshotSet()
	if ret != 0 {
		LOG_ERROR("VSS_SNAPSHOT", "Shadow copy creation failed: DoSnapshotSet returned %x", uint(ret))
		return top
	}
	if async == nil {
		LOG_ERROR("VSS_SNAPSHOT",
			"Shadow copy creation failed: DoSnapshotSet failed to return a valid IVssAsync object")
		return top
	}

	if !async.Wait(timeoutInSeconds) {
		LOG_ERROR("VSS_SNAPSHOT", "Shadow copy creation failed: DoSnapshotSet didn't finish properly")
		return top
	}
	async.Release()

	properties := SnapshotProperties{}

	ret = vssBackupComponent.GetSnapshotProperties(snapshotID, &properties)
	if ret != 0 {
		LOG_ERROR("VSS_PROPERTIES", "GetSnapshotProperties returned %x", ret)
		return top
	}

	SnapshotIDString, _ := ole.StringFromIID(&properties.SnapshotID)
	SnapshotSetIDString, _ := ole.StringFromIID(&properties.SnapshotSetID)

	LOG_DEBUG("VSS_PROPERTY", "SnapshotID: %s", SnapshotIDString)
	LOG_DEBUG("VSS_PROPERTY", "SnapshotSetID: %s", SnapshotSetIDString)

	LOG_DEBUG("VSS_PROPERTY", "SnapshotDeviceObject: %s", uint16ArrayToString(properties.SnapshotDeviceObject))
	LOG_DEBUG("VSS_PROPERTY", "OriginalVolumeName: %s", uint16ArrayToString(properties.OriginalVolumeName))
	LOG_DEBUG("VSS_PROPERTY", "OriginatingMachine: %s", uint16ArrayToString(properties.OriginatingMachine))
	LOG_DEBUG("VSS_PROPERTY", "OriginatingMachine: %s", uint16ArrayToString(properties.OriginatingMachine))
	LOG_DEBUG("VSS_PROPERTY", "ServiceMachine: %s", uint16ArrayToString(properties.ServiceMachine))
	LOG_DEBUG("VSS_PROPERTY", "ExposedName: %s", uint16ArrayToString(properties.ExposedName))
	LOG_DEBUG("VSS_PROPERTY", "ExposedPath: %s", uint16ArrayToString(properties.ExposedPath))

	LOG_INFO("VSS_DONE", "Shadow copy %s created", SnapshotIDString)

	snapshotPath := uint16ArrayToString(properties.SnapshotDeviceObject)

	preferencePath := GetDuplicacyPreferencePath()
	shadowLink = preferencePath + "\\shadow"
	os.Remove(shadowLink)
	err = os.Symlink(snapshotPath+"\\", shadowLink)
	if err != nil {
		LOG_ERROR("VSS_SYMLINK", "Failed to create a symbolic link to the shadow copy just created: %v", err)
		return top
	}

	return shadowLink + "\\" + top[2:]

}
