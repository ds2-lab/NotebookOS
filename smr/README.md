# SMR

SMR module leverage etcd's [raft library](https://github.com/etcd-io/etcd/raft) to provide a network SyncLog(../distributed-notebook/sync) implementation.

## Compatibility

### Mac OS Intel

What to do: In go.etcd.io/etcd/client/pkg/v3/fileutil/sync_darwin.go "func Fsync(f *os.File) error", shortcut FcntlInt call. 
Error: operation not supported.
Reason: F_FUULFSSYNC is implemented on HFS, MS-DOS (FAT), and Universal Disk Format (UDF) only.
Reference:
https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html