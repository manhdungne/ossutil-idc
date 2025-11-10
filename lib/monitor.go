package lib

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"os"
    "golang.org/x/term"
	"unicode/utf8"
)

const (
	normalExit = iota
	errExit
)

var processTickInterval int64 = 5

var clearStrLen int = 0
var clearStr string = strings.Repeat(" ", clearStrLen)

func getClearStr(s string) string {
    return "\r\x1b[2K" + s // carriage return + clear entire line, no newline
}
type Monitorer interface {
	setScanError(err error)
	updateScanNum(num int64)
	setScanEnd()
}

// for normal object operation
type MonitorSnap struct {
	okNum   int64
	errNum  int64
	skipNum int64
	dealNum int64
}

/*
 * Put same type variables together to make them 64bits alignment to avoid
 * atomic.AddInt64() panic
 * Please guarantee the alignment if you add new filed
 */
type Monitor struct {
	opStr          string
	totalNum       int64
	okNum          int64
	errNum         int64
	skipNum        int64
	seekAheadError error
	seekAheadEnd   bool
	finish         bool
	lastSnapTime   time.Time
	_              uint32 //Add padding to make sure the next data 64bits alignment
}

func (m *Monitor) init(opStr string) {
	m.opStr = opStr
	m.totalNum = 0
	m.seekAheadEnd = false
	m.seekAheadError = nil
	m.okNum = 0
	m.errNum = 0
	m.skipNum = 0
	m.finish = false
}

func (m *Monitor) setScanError(err error) {
	m.seekAheadError = err
	m.seekAheadEnd = true
}

func (m *Monitor) updateScanNum(num int64) {
	m.totalNum = m.totalNum + num
}

func (m *Monitor) setScanEnd() {
	m.seekAheadEnd = true
}

func (m *Monitor) updateOKNum(num int64) {
	atomic.AddInt64(&m.okNum, num)
}

func (m *Monitor) updateErrNum(num int64) {
	atomic.AddInt64(&m.errNum, num)
}

func (m *Monitor) getSnapshot() *MonitorSnap {
	var snap MonitorSnap
	snap.okNum = m.okNum
	snap.errNum = m.errNum
	snap.skipNum = m.skipNum
	snap.dealNum = snap.okNum + snap.errNum
	return &snap
}


func (m *Monitor) progressBar(finish bool, exitStat int) string {
	if m.finish {
		return ""
	}
	m.finish = m.finish || finish
	if !finish {
		return m.getProgressBar()
	}
	return m.getFinishBar(exitStat)
}

func (m *Monitor) getProgressBar() string {
	snap := m.getSnapshot()
	if m.seekAheadEnd && m.seekAheadError == nil {
		if snap.errNum == 0 {
			return getClearStr(fmt.Sprintf("Total %d objects. %s %d objects, Progress: %d%s", m.totalNum, m.opStr, snap.okNum, m.getPrecent(snap), "%%"))
		}
		return getClearStr(fmt.Sprintf("Total %d objects. %s %d objects, Error %d objects, Progress: %d%s", m.totalNum, m.opStr, snap.okNum, snap.errNum, m.getPrecent(snap), "%%"))
	}
	scanNum := max(m.totalNum, snap.dealNum)
	if snap.errNum == 0 {
		return getClearStr(fmt.Sprintf("Scanned %d objects. %s %d objects.", scanNum, m.opStr, snap.okNum))
	}
	return getClearStr(fmt.Sprintf("Scanned %d objects. %s %d objects, Error %d objects.", scanNum, m.opStr, snap.okNum, snap.errNum))
}


func (m *Monitor) getPrecent(snap *MonitorSnap) int {
	if m.seekAheadEnd && m.seekAheadError == nil {
		if m.totalNum != 0 {
			return int(float64((snap.dealNum)*100.0) / float64(m.totalNum))
		}
		return 100
	}
	return 0
}

func (m *Monitor) getFinishBar(exitStat int) string {
	if exitStat == normalExit {
		return m.getWholeFinishBar()
	}
	return m.getDefeatBar()
}

func (m *Monitor) getWholeFinishBar() string {
	snap := m.getSnapshot()
	if m.seekAheadEnd && m.seekAheadError == nil {
		if snap.errNum == 0 {
			return getClearStr(fmt.Sprintf("Succeed: Total %d objects. %s %d objects(skip %d objects).\n", m.totalNum, m.opStr, snap.okNum, snap.skipNum))
		}
		return getClearStr(fmt.Sprintf("FinishWithError: Total %d objects. %s %d objects(skip %d objects), Error %d objects.\n", m.totalNum, m.opStr, snap.okNum, snap.skipNum, snap.errNum))
	}
	scanNum := max(m.totalNum, snap.dealNum)
	if snap.errNum == 0 {
		return getClearStr(fmt.Sprintf("Succeed: Total %d objects. %s %d objects(skip %d objects).\n", scanNum, m.opStr, snap.okNum, snap.skipNum))
	}
	return getClearStr(fmt.Sprintf("FinishWithError: Scanned %d objects. %s %d objects(skip %d objects), Error %d objects.\n", scanNum, m.opStr, snap.okNum, snap.skipNum, snap.errNum))
}

func (m *Monitor) getDefeatBar() string {
	snap := m.getSnapshot()
	if m.seekAheadEnd && m.seekAheadError == nil {
		return getClearStr(fmt.Sprintf("Total %d objects. %s %d objects(skip %d objects), when error happens.\n", m.totalNum, m.opStr, snap.okNum, snap.skipNum))
	}
	scanNum := max(m.totalNum, snap.dealNum)
	return getClearStr(fmt.Sprintf("Scanned %d objects. %s %d objects(skip %d objects), when error happens.\n", scanNum, m.opStr, snap.okNum, snap.skipNum))
}

// For rm
type RMMonitorSnap struct {
	objectNum      int64
	uploadIdNum    int64
	errObjectNum   int64
	errUploadIdNum int64
	dealNum        int64
	errNum         int64
	removedBucket  string
}

/*
 * Put same type variables together to make them 64bits alignment to avoid
 * atomic.AddInt64() panic
 * Please guarantee the alignment if you add new filed
 */
type RMMonitor struct {
	op               int64
	totalObjectNum   int64
	totalUploadIdNum int64
	objectNum        int64
	uploadIdNum      int64
	errObjectNum     int64
	errUploadIdNum   int64
	removedBucket    string
	seekAheadError   error
	seekAheadEnd     bool
	finish           bool
	_                uint32 //Add padding to make sure the next data 64bits alignment
}

func (m *RMMonitor) init() {
	m.op = 0
	m.totalObjectNum = 0
	m.totalUploadIdNum = 0
	m.seekAheadEnd = false
	m.seekAheadError = nil
	m.objectNum = 0
	m.uploadIdNum = 0
	m.errObjectNum = 0
	m.errUploadIdNum = 0
	m.finish = false
	m.removedBucket = ""
}

func (m *RMMonitor) updateOP(op int64) {
	m.op = m.op | op
}

func (m *RMMonitor) setOP(op int64) {
	m.op = op
}

func (m *RMMonitor) setScanError(err error) {
	m.seekAheadError = err
	m.seekAheadEnd = true
}

func (m *RMMonitor) updateScanNum(num int64) {
	m.totalObjectNum = m.totalObjectNum + num
}

func (m *RMMonitor) updateScanUploadIdNum(num int64) {
	m.totalUploadIdNum = m.totalUploadIdNum + num
}

func (m *RMMonitor) setScanEnd() {
	m.seekAheadEnd = true
}

func (m *RMMonitor) updateObjectNum(num int64) {
	atomic.AddInt64(&m.objectNum, num)
}

func (m *RMMonitor) updateUploadIdNum(num int64) {
	atomic.AddInt64(&m.uploadIdNum, num)
}

func (m *RMMonitor) updateErrObjectNum(num int64) {
	atomic.AddInt64(&m.errObjectNum, num)
}

func (m *RMMonitor) updateErrUploadIdNum(num int64) {
	atomic.AddInt64(&m.errUploadIdNum, num)
}

func (m *RMMonitor) updateRemovedBucket(bucket string) {
	m.removedBucket = bucket
}

func (m *RMMonitor) getSnapshot() *RMMonitorSnap {
	var snap RMMonitorSnap
	snap.objectNum = m.objectNum
	snap.uploadIdNum = m.uploadIdNum
	snap.errObjectNum = m.errObjectNum
	snap.errUploadIdNum = m.errUploadIdNum
	snap.dealNum = snap.objectNum + snap.uploadIdNum + snap.errObjectNum + snap.errUploadIdNum
	snap.errNum = snap.errObjectNum + snap.errUploadIdNum
	snap.removedBucket = m.removedBucket
	return &snap
}

func (m *RMMonitor) progressBar(finish bool, exitStat int) string {
	if m.finish {
		return ""
	}
	m.finish = m.finish || finish
	if !finish {
		return m.getProgressBar()
	}
	return m.getFinishBar(exitStat)
}

func (m *RMMonitor) getProgressBar() string {
	if m.op&allType != 0 {
		snap := m.getSnapshot()
		if m.seekAheadEnd && m.seekAheadError == nil {
			return getClearStr(fmt.Sprintf("Total %s. %s%s Progress: %d%s", m.getTotalInfo(), m.getOKInfo(snap), m.getErrInfo(snap), m.getPrecent(snap), "%%"))
		}
		m.totalObjectNum = max(m.totalObjectNum, snap.objectNum+snap.errObjectNum)
		m.totalUploadIdNum = max(m.totalUploadIdNum, snap.uploadIdNum+snap.errUploadIdNum)
		return getClearStr(fmt.Sprintf("Scanned %s. %s%s", m.getTotalInfo(), m.getOKInfo(snap), m.getErrInfo(snap)))
	}
	return getClearStr("")
}

func (m *RMMonitor) getTotalInfo() string {
	strList := []string{}
	if m.op&objectType != 0 {
		strList = append(strList, fmt.Sprintf("%d objects", m.totalObjectNum))
	}
	if m.op&multipartType != 0 {
		strList = append(strList, fmt.Sprintf("%d uploadIds", m.totalUploadIdNum))
	}
	return strings.Join(strList, ", ")
}

func (m *RMMonitor) getOKInfo(snap *RMMonitorSnap) string {
	strList := []string{}
	if m.op&allType == 0 {
		return ""
	}
	if m.op&objectType != 0 {
		strList = append(strList, fmt.Sprintf("%d objects", snap.objectNum))
	}
	if m.op&multipartType != 0 {
		strList = append(strList, fmt.Sprintf("%d uploadIds", snap.uploadIdNum))
	}
	return fmt.Sprintf("Removed %s.", strings.Join(strList, ", "))
}

func (m *RMMonitor) getErrInfo(snap *RMMonitorSnap) string {
	if snap.errNum != 0 {
		strList := []string{}
		if snap.errObjectNum != 0 {
			strList = append(strList, fmt.Sprintf("%d objects", snap.errObjectNum))
		}
		if snap.errUploadIdNum != 0 {
			strList = append(strList, fmt.Sprintf("%d uploadIds", snap.errUploadIdNum))
		}
		return fmt.Sprintf(" Error %s.", strings.Join(strList, ", "))
	}
	return ""
}

func (m *RMMonitor) getPrecent(snap *RMMonitorSnap) int {
	if m.seekAheadEnd && m.seekAheadError == nil {
		if m.totalObjectNum+m.totalUploadIdNum != 0 {
			return int(float64((snap.dealNum)*100.0) / float64(m.totalObjectNum+m.totalUploadIdNum))
		}
		return 100
	}
	return 0
}

func (m *RMMonitor) getFinishBar(exitStat int) string {
	snap := m.getSnapshot()
	return m.getObjectFinishBar(snap, exitStat) + m.getBucketFinishBar(snap)
}

func (m *RMMonitor) getObjectFinishBar(snap *RMMonitorSnap, exitStat int) string {
	if m.op&allType != 0 {
		if m.seekAheadEnd && m.seekAheadError == nil {
			if m.getExitStat(snap, exitStat) == errExit {
				return getClearStr(fmt.Sprintf("Total %s. %s when error happens.\n", m.getTotalInfo(), m.getOKInfo(snap)))
			}
			return getClearStr(fmt.Sprintf("Succeed: Total %s. %s\n", m.getTotalInfo(), m.getOKInfo(snap)))
		}
		m.totalObjectNum = max(m.totalObjectNum, snap.objectNum+snap.errObjectNum)
		m.totalUploadIdNum = max(m.totalUploadIdNum, snap.uploadIdNum+snap.errUploadIdNum)
		if m.getExitStat(snap, exitStat) == errExit {
			return getClearStr(fmt.Sprintf("Scanned %s. %s when error happens.\n", m.getTotalInfo(), m.getOKInfo(snap)))
		}
		return getClearStr(fmt.Sprintf("Succeed: Total %s. %s\n", m.getTotalInfo(), m.getOKInfo(snap)))
	}
	return getClearStr("")
}

func (m *RMMonitor) getExitStat(snap *RMMonitorSnap, exitStat int) int {
	if exitStat != normalExit || snap.errNum != 0 || (m.op&bucketType != 0 && snap.removedBucket == "") {
		return errExit
	}
	return normalExit
}

func (m *RMMonitor) getBucketFinishBar(snap *RMMonitorSnap) string {
	if m.op&bucketType != 0 && snap.removedBucket != "" {
		return getClearStr(fmt.Sprintf("Removed Bucket: %s\n", snap.removedBucket))
	}
	return getClearStr("")
}

// for cp
type CPMonitorSnap struct {
	transferSize  int64
	skipSize      int64
	dealSize      int64
	fileNum       int64
	dirNum        int64
	skipNum       int64
	skipNumDir    int64
	errNum        int64
	okNum         int64
	dealNum       int64
	duration      int64
	incrementSize int64
}

/*
 * Put same type variables together to make them 64bits alignment to avoid
 * atomic.AddInt64() panic
 * Please guarantee the alignment if you add new filed
 */
type CPMonitor struct {
	totalSize      int64
	totalNum       int64
	transferSize   int64
	skipSize       int64
	dealSize       int64
	fileNum        int64
	dirNum         int64
	skipNum        int64
	skipNumDir     int64
	errNum         int64
	lastSnapSize   int64
	tickDuration   int64
	seekAheadError error
	op             operationType
	seekAheadEnd   bool
	finish         bool
	_              uint32 //Add padding to make sure the next data 64bits alignment
	lastSnapTime   time.Time
	mu           sync.RWMutex
    currentByWID map[int]string
	lastPanelAt time.Time
	lastL4Printed string
}

func (m *CPMonitor) init(op operationType) {
	m.op = op
	m.totalSize = 0
	m.totalNum = 0
	m.seekAheadEnd = false
	m.seekAheadError = nil
	m.transferSize = 0
	m.skipSize = 0
	m.dealSize = 0
	m.fileNum = 0
	m.dirNum = 0
	m.skipNum = 0
	m.errNum = 0
	m.finish = false
	m.lastSnapSize = 0
	m.lastSnapTime = time.Now()
	m.tickDuration = processTickInterval * int64(time.Second)
	m.currentByWID = make(map[int]string)
	m.tickDuration = 1 * int64(time.Second) // 1s/khung cho đỡ nhấp nháy
	m.lastPanelAt = time.Now()
}

func (m *CPMonitor) setScanError(err error) {
	m.seekAheadError = err
	m.seekAheadEnd = true
}

func (m *CPMonitor) updateScanNum(num int64) {
	m.totalNum = m.totalNum + num
}

func (m *CPMonitor) updateScanSizeNum(size, num int64) {
	m.totalSize = m.totalSize + size
	m.totalNum = m.totalNum + num
}

func (m *CPMonitor) setScanEnd() {
	m.seekAheadEnd = true
}

func (m *CPMonitor) updateTransferSize(size int64) {
	atomic.AddInt64(&m.transferSize, size)
}

func (m *CPMonitor) updateDealSize(size int64) {
	atomic.AddInt64(&m.dealSize, size)
}

func (m *CPMonitor) updateFile(size, num int64) {
	atomic.AddInt64(&m.fileNum, num)
	atomic.AddInt64(&m.transferSize, size)
	atomic.AddInt64(&m.dealSize, size)
}

func (m *CPMonitor) updateDir(size, num int64) {
	atomic.AddInt64(&m.dirNum, num)
	atomic.AddInt64(&m.transferSize, size)
	atomic.AddInt64(&m.dealSize, size)
}

func (m *CPMonitor) updateSkip(size, num int64) {
	atomic.AddInt64(&m.skipNum, num)
	atomic.AddInt64(&m.skipSize, size)
}

func (m *CPMonitor) updateSkipDir(num int64) {
	atomic.AddInt64(&m.skipNumDir, num)
}

func (m *CPMonitor) updateErr(size, num int64) {
	atomic.AddInt64(&m.errNum, num)
	atomic.AddInt64(&m.transferSize, size)
}

func (m *CPMonitor) getSnapshot() *CPMonitorSnap {
	var snap CPMonitorSnap
	snap.transferSize = m.transferSize
	snap.skipSize = m.skipSize
	snap.dealSize = m.dealSize + snap.skipSize
	snap.fileNum = m.fileNum
	snap.dirNum = m.dirNum
	snap.skipNum = m.skipNum
	snap.errNum = m.errNum
	snap.okNum = snap.fileNum + snap.dirNum + snap.skipNum
	snap.dealNum = snap.okNum + snap.errNum
	snap.skipNumDir = m.skipNumDir
	now := time.Now()
	snap.duration = now.Sub(m.lastSnapTime).Nanoseconds()

	return &snap
}

func (m *CPMonitor) SetCurrent(wid int, name string) {
    m.mu.Lock()
    m.currentByWID[wid] = name
    m.mu.Unlock()
}
func (m *CPMonitor) ClearCurrent(wid int) {
    m.mu.Lock()
    delete(m.currentByWID, wid)
    m.mu.Unlock()
}
func (m *CPMonitor) snapshotCurrents(max int) []string {
    m.mu.RLock()
    defer m.mu.RUnlock()
    res := make([]string, 0, len(m.currentByWID))
    for _, v := range m.currentByWID {
        if len(v) > 120 { v = v[:117] + "..." }
        res = append(res, v)
        if max > 0 && len(res) >= max { break }
    }
    return res
}


func (m *CPMonitor) progressBar(finish bool, exitStat int) string {
	if m.finish {
		return ""
	}
	m.finish = m.finish || finish
	if !finish {
		return m.getProgressBar()
	}
	return m.getFinishBar(exitStat)
}

var cpRenderer progressRenderer

func (m *CPMonitor) getProgressBar() string {
    snap := m.getSnapshot()

    if snap.duration < m.tickDuration {
        return ""
    }
    m.lastSnapTime = time.Now()
    snap.incrementSize = m.transferSize - m.lastSnapSize
    m.lastSnapSize = snap.transferSize

    scanNum   := max(m.totalNum, snap.dealNum)
    scanSize  := max(m.totalSize, snap.dealSize)
    copyCount := snap.fileNum + snap.dirNum
    skipCount := snap.skipNum + snap.skipNumDir
    errCount  := snap.errNum

    // KHÔNG in “Current object” nữa
    // currents := m.snapshotCurrents(2) // <- bỏ
    // curStr := ""                      // <- bỏ

    pctStr := ""
    if m.seekAheadEnd && m.seekAheadError == nil {
        pctStr = fmt.Sprintf(", Progress: %.3f%%", m.getPrecent(snap))
    }

    line := fmt.Sprintf(
        "Scanned num: %d, size: %s. Dealed num: %d(copy %d objects, skip %d objects, err %d objects), OK size: %s, Speed: %.2fKB/s%s",
        scanNum, getSizeString(scanSize),
        snap.dealNum, copyCount, skipCount, errCount,
        getSizeString(snap.dealSize),
        m.getSpeed(snap), pctStr,
    )
    // Tự wrap theo width, KHÔNG thêm “...”
    lines := wrapToWidth(line, termWidth())
    return cpRenderer.render(lines)
}



func (m *CPMonitor) getFinishBar(exitStat int) string {
	if exitStat == normalExit {
		return m.getWholeFinishBar()
	}
	return m.getDefeatBar()
}

func (m *CPMonitor) getWholeFinishBar() string {
	snap := m.getSnapshot()
	if m.seekAheadEnd && m.seekAheadError == nil {
		if snap.errNum == 0 {
			return getClearStr(fmt.Sprintf("Succeed: Total num: %d, size: %s. OK num: %d%s%s.\n", m.totalNum, getSizeString(m.totalSize), snap.okNum, m.getDealNumDetail(snap), m.getSkipSize(snap)))
		}
		return getClearStr(fmt.Sprintf("FinishWithError: Total num: %d, size: %s. Error num: %d. OK num: %d%s%s.\n", m.totalNum, getSizeString(m.totalSize), snap.errNum, snap.okNum, m.getOKNumDetail(snap), m.getSizeDetail(snap)))
	}
	scanNum := max(m.totalNum, snap.dealNum)
	if snap.errNum == 0 {
		return getClearStr(fmt.Sprintf("Succeed: Total num: %d, size: %s. OK num: %d%s%s.\n", scanNum, getSizeString(snap.dealSize), snap.okNum, m.getDealNumDetail(snap), m.getSkipSize(snap)))
	}
	return getClearStr(fmt.Sprintf("FinishWithError: Scanned %d %s. Error num: %d. OK num: %d%s%s.\n", scanNum, m.getSubject(), snap.errNum, snap.okNum, m.getOKNumDetail(snap), m.getSizeDetail(snap)))
}

func (m *CPMonitor) getDefeatBar() string {
	snap := m.getSnapshot()
	if m.seekAheadEnd && m.seekAheadError == nil {
		return getClearStr(fmt.Sprintf("Total num: %d, size: %s. Dealed num: %d%s%s. When error happens.\n", m.totalNum, getSizeString(m.totalSize), snap.okNum, m.getOKNumDetail(snap), m.getSizeDetail(snap)))
	}
	scanNum := max(m.totalNum, snap.dealNum)
	return getClearStr(fmt.Sprintf("Scanned %d %s. Dealed num: %d%s%s. When error happens.\n", scanNum, m.getSubject(), snap.okNum, m.getOKNumDetail(snap), m.getSizeDetail(snap)))
}

func (m *CPMonitor) getSubject() string {
	switch m.op {
	case operationTypePut:
		return "files"
	default:
		return "objects"
	}
}

func (m *CPMonitor) getDealNumDetail(snap *CPMonitorSnap) string {
	return m.getNumDetail(snap, true)
}

func (m *CPMonitor) getOKNumDetail(snap *CPMonitorSnap) string {
	return m.getNumDetail(snap, false)
}

func (m *CPMonitor) getNumDetail(snap *CPMonitorSnap, hasErr bool) string {
	if !hasErr && snap.okNum == 0 {
		return ""
	}
	strList := []string{}
	if hasErr && snap.errNum != 0 {
		strList = append(strList, fmt.Sprintf("Error %d %s", snap.errNum, m.getSubject()))
	}
	if snap.fileNum != 0 {
		strList = append(strList, fmt.Sprintf("%s %d %s", m.getOPStr(), snap.fileNum, m.getSubject()))
	}
	if snap.dirNum != 0 {
		str := fmt.Sprintf("%d directories", snap.dirNum)
		if snap.fileNum == 0 {
			str = fmt.Sprintf("%s %d directories", m.getOPStr(), snap.dirNum)
		}
		strList = append(strList, str)
	}
	if snap.skipNum != 0 {
		strList = append(strList, fmt.Sprintf("skip %d %s", snap.skipNum, m.getSubject()))
	}
	if snap.skipNumDir != 0 {
		strList = append(strList, fmt.Sprintf("skip %d directory", snap.skipNumDir))
	}

	if len(strList) == 0 {
		return ""
	}
	return fmt.Sprintf("(%s)", strings.Join(strList, ", "))
}

func (m *CPMonitor) getSpeed(snap *CPMonitorSnap) float64 {
	return (float64(snap.incrementSize) / 1024) / (float64(snap.duration) * 1e-9)
}

func (m *CPMonitor) getOPStr() string {
	switch m.op {
	case operationTypePut:
		return "upload"
	case operationTypeGet:
		return "download"
	default:
		return "copy"
	}
}

func (m *CPMonitor) getDealSizeDetail(snap *CPMonitorSnap) string {
	return fmt.Sprintf(", OK size: %s", getSizeString(snap.dealSize))
}

func (m *CPMonitor) getSkipSize(snap *CPMonitorSnap) string {
	if snap.skipSize != 0 {
		return fmt.Sprintf(", Skip size: %s", getSizeString(snap.skipSize))
	}
	return ""
}

func (m *CPMonitor) getSizeDetail(snap *CPMonitorSnap) string {
	if snap.skipSize == 0 {
		return fmt.Sprintf(", Transfer size: %s", getSizeString(snap.transferSize))
	}
	if snap.transferSize == 0 {
		return fmt.Sprintf(", Skip size: %s", getSizeString(snap.skipSize))
	}
	return fmt.Sprintf(", OK size: %s(transfer: %s, skip: %s)", getSizeString(snap.transferSize+snap.skipSize), getSizeString(snap.transferSize), getSizeString(snap.skipSize))
}

func (m *CPMonitor) getPrecent(snap *CPMonitorSnap) float64 {
	if m.seekAheadEnd && m.seekAheadError == nil {
		if m.totalSize != 0 {
			return float64((snap.dealSize)*100.0) / float64(m.totalSize)
		}
		if m.totalNum != 0 {
			return float64((snap.dealNum)*100.0) / float64(m.totalNum)
		}
		return 100
	}
	return 0
}

// Giới hạn tối đa ký tự in ra để tránh terminal tự wrap.
// Bạn có thể chỉnh con số này nếu terminal rộng hơn.
const progressMaxCols = 200

func getTermWidth() int {
    w, _, err := term.GetSize(int(os.Stderr.Fd()))
    if err != nil || w <= 0 {
        return 120
    }
    return w - 2 // chừa 2 ký tự tránh wrap mép phải
}

func truncate(s string) string {
    max := getTermWidth()
    if len(s) <= max {
        return s
    }
    if max > 3 {
        return s[:max-3] + "..."
    }
    return s[:max]
}

// Ghép base + Current sao cho vừa chiều rộng terminal.
// Base luôn được giữ nguyên; chỉ cắt bớt danh sách Current nếu thiếu chỗ.
func (m *CPMonitor) composeProgressLine(base string, currents []string) string {
    max := getTermWidth()
    if max <= 0 {
        max = 120
    }

    // Nếu base đã dài hơn width thì cắt base và trả về luôn
    if len(base) >= max {
        return truncate(base)
    }

    // Phần trống còn lại để nhét "  Current: ..."
    room := max - len(base) - 1 // chừa 1 ký tự an toàn
    if room < 12 || len(currents) == 0 {
        return base
    }

    b := strings.Builder{}
    b.WriteString(base)
    const prefix = "  Current: "
    if len(prefix) > room {
        return base
    }
    b.WriteString(prefix)
    room -= len(prefix)

    // Nhét từng mục Current, cắt gọn nếu cần
    for i, c := range currents {
        if len(c) > room {
            if room <= 3 {
                break
            }
            c = c[:room-3] + "..."
        }
        b.WriteString(c)
        room -= len(c)

        if i < len(currents)-1 {
            sep := " | "
            if len(sep) > room {
                break
            }
            b.WriteString(sep)
            room -= len(sep)
        }
        if room <= 0 {
            break
        }
    }
    return b.String()
}

// đếm “độ rộng hiển thị” (giản lược: đếm rune; đủ tốt nếu không dùng ANSI màu)
func displayWidth(s string) int {
	return utf8.RuneCountInString(s)
}

// tìm điểm cắt “mềm” <= width tại khoảng trắng; nếu không có thì cắt đúng width runes
func softCutIndex(s string, width int) int {
	if width <= 0 {
		return 0
	}
	// đi qua width rune
	i := 0
	for idx := range s {
		if i == width {
			break
		}
		i++
		if i == width {
			// idx là byte index của rune đầu tiên sau khi đủ width? ta cần vị trí sau rune thứ width
			// range cho idx của rune hiện tại; để lấy sau rune này cần tiếp tục một bước
			// nhưng đơn giản: giữ idx, rồi sau vòng kế ta có nextIdx
		}
		_ = idx
	}
	// tính byte index sau rune thứ width
	byteIdx := byteIndexAfterRunes(s, width)

	// tìm khoảng trắng gần nhất về bên trái
	left := strings.LastIndexAny(s[:byteIdx], " \t")
	if left > width/2 { // chỉ cắt ở space nếu đủ gần cuối
		return left
	}
	return byteIdx
}

// trả byte index sau N rune
func byteIndexAfterRunes(s string, n int) int {
	if n <= 0 {
		return 0
	}
	i := 0
	for idx := range s {
		if i == n {
			return idx
		}
		i++
	}
	return len(s)
}

type progressRenderer struct {
	prevRows int
}

func termWidth() int {
	w, _, err := term.GetSize(int(os.Stderr.Fd()))
	if err != nil || w <= 0 {
		return 120
	}
	return w - 2
}

func wrapToWidth(s string, width int) []string {
	if width <= 4 {
		if len(s) <= width {
			return []string{s}
		}
		return []string{s[:width-3] + "..."}
	}
	lines := []string{}
	for len(s) > width {
		lines = append(lines, s[:width])
		s = s[width:]
	}
	lines = append(lines, s)
	return lines
}

func (r *progressRenderer) render(lines []string) string {
	if len(lines) == 0 {
		lines = []string{""}
	}
	curRows := len(lines)
	maxRows := r.prevRows
	if curRows > maxRows {
		maxRows = curRows
	}

	var b strings.Builder
	// quay về đầu khối cũ
	if r.prevRows > 0 {
		b.WriteString("\r")
		if r.prevRows > 1 {
			b.WriteString(fmt.Sprintf("\x1b[%dA", r.prevRows-1))
		}
	}

	// vẽ đủ maxRows dòng
	for i := 0; i < maxRows; i++ {
		b.WriteString("\r\x1b[2K")
		if i < curRows {
			b.WriteString(lines[i])
		}
		if i < maxRows-1 {
			b.WriteByte('\n')
		}
	}

	// đưa con trỏ về đầu khối
	if maxRows > 1 {
		b.WriteString(fmt.Sprintf("\r\x1b[%dA", maxRows-1))
	} else {
		b.WriteString("\r")
	}

	r.prevRows = curRows
	return b.String()
}

// Giữ panel lại khi kết thúc (không xoá), chỉ đẩy con trỏ xuống dưới
func (r *progressRenderer) keep() string {
	if r.prevRows == 0 {
		return "\n"
	}
	var b strings.Builder
	b.WriteString("\r")
	// nhảy xuống dưới cùng của khối
	for i := 1; i < r.prevRows; i++ {
		b.WriteByte('\n')
	}
	b.WriteString("\n")
	// reset, nhưng không xoá panel
	r.prevRows = 0
	return b.String()
}

// 1 dòng tiến độ đủ thông tin, KHÔNG có '\n', dùng getClearStr để overwrite
func (m *CPMonitor) BuildProgressLineOneLine() string {
    snap := m.getSnapshot()

    // cập nhật tốc độ
    now := time.Now()
    snap.incrementSize = m.transferSize - m.lastSnapSize
    m.lastSnapSize = snap.transferSize
    m.lastSnapTime = now

    scanNum  := max(m.totalNum, snap.dealNum)
    scanSize := max(m.totalSize, snap.dealSize)
    copyCnt  := snap.fileNum + snap.dirNum
    skipCnt  := snap.skipNum + snap.skipNumDir
    errCnt   := snap.errNum
    okSize   := getSizeString(snap.dealSize)
    speed    := fmt.Sprintf("%.2fKB/s", m.getSpeed(snap))

    pctStr := ""
    if m.seekAheadEnd && m.seekAheadError == nil {
        pctStr = fmt.Sprintf(", Progress: %.3f%%", m.getPrecent(snap))
    }

    base := fmt.Sprintf(
        "Scanned num: %d, size: %s. Dealed num: %d(copy %d objects, skip %d objects, err %d objects), OK size: %s, Speed: %s%s",
        scanNum, getSizeString(scanSize),
        snap.dealNum, copyCnt, skipCnt, errCnt,
        okSize, speed, pctStr,
    )

    // gói cho vừa terminal (tuỳ bạn đã có getTermWidth/truncate)
    w := getTermWidth()
    if len(base) > w && w > 6 {
        base = base[:w-3] + "..."
    }
    // getClearStr trả về "\r...." giúp overwrite trên 1 dòng
    return getClearStr(base)
}


func (m *CPMonitor) currentLevelN(n int) string {
    cur := ""
    m.mu.RLock()
    for _, v := range m.currentByWID { // lấy 1 cái đầu tiên là đủ
        cur = v
        break
    }
    m.mu.RUnlock()
    if cur == "" {
        return ""
    }
    // bỏ scheme, bỏ slash đầu
    cur = strings.TrimPrefix(cur, "oss://")
    cur = strings.TrimPrefix(cur, "s3://")
    cur = strings.TrimLeft(cur, "/")
    parts := strings.Split(cur, "/")
    if len(parts) > n {
        parts = parts[:n]
    }
    return strings.Join(parts, "/")
}