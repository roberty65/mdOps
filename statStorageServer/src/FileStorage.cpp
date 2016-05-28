/* FileStorage.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <limits.h>
#include <errno.h>

#include "utils.h"
#include "Log.h"
#include "StatData.h"
#include "StatErrno.h"
#include "MemoryBuffer.h"
#include "StatMerger.h"
#include "StatCombiner.h"
#include "StatSystemIids.h"
#include "FileStorage.h"

char *FileStorage::frq2str(char *buf, size_t size, uint8_t ftype, uint8_t freqs)
{
	const char *fts[] = { "s", "m", "h", "d", "m", "y" };
	const char *ffs = (ftype >= 0 && ftype <= sizeof(fts)/sizeof(fts[0])) ? fts[ftype] : "?";

	xsnprintf(buf, size, "%u%s", freqs, ffs);
	return buf;
}

char *FileStorage::hip2str(char *buf, size_t size, const stat_ip_t& hip)
{
	if (hip.ver == 4)
		inet_ntop(AF_INET, (void *)&hip.ip.ip4, buf, size);
	else buf[0] = 0;

	return buf;
}

int FileStorage::makeDirectory(char *path)
{
	char save, *ptr = path;
	while ((ptr = strchr(ptr, '/')) != NULL) {
		save = *ptr; *ptr = 0;
		int retval = mkdir(path, 0775);

		if (retval < 0 && errno != EEXIST) {
			APPLOG_ERROR("makeDirectory(%s) failed: %m", path);
		}
		else if (retval == 0) {
			APPLOG_DEBUG("makeDirectory(%s) OK", path);
		}

		*ptr++ = save;
	}

	return 0;
}

char *FileStorage::makePath(char *path, size_t size, const char *typeString, int year,
			    const stat_id_t& sid, const stat_ip_t& hip,
			    uint8_t ftype, uint8_t freqs)
{
	char buf1[128], buf2[128];

	/* DIR/YEAR/PID/MID/TYPE_YEAR_PID_MID_IID_HOST_FREQ.bin */
	snprintf(path, size, "%s%04d/%04x/%04x/%s_%04x_%04x_%04x_%s_%s.bin",
		baseDir.c_str(), year, sid.pid, sid.mid,
		typeString, sid.pid, sid.mid, sid.iid,
		hip2str(buf1, sizeof buf1, hip),
		frq2str(buf2, sizeof buf2, ftype, freqs));
	path[sizeof path - 1] = 0;

	return path;
}

char *FileStorage::makePath(char *path, size_t size, const char *typeString, int year,
			    const stat_id_t& src_sid, const stat_ip_t& src_hip,
			    const stat_id_t& dst_sid, const stat_ip_t& dst_hip,
			    uint8_t ftype, uint8_t freqs)
{
	char buf1[128], buf2[128], buf3[128];

	/* DIR/YEAR/PID/MID/TYPE_YEAR_PID_MID_IID_HOST_{DST}_FREQ.bin */
	snprintf(path, size, "%s%04d/%04x/%04x/%s_%04x_%04x_%04x_%s_%04x_%04x_%04x_%s_%s.bin",
		baseDir.c_str(), year, src_sid.pid, src_sid.mid,
		typeString, src_sid.pid, src_sid.mid, src_sid.iid,
		hip2str(buf1, sizeof buf1, src_hip),
		dst_sid.pid, dst_sid.mid, dst_sid.iid,
		hip2str(buf2, sizeof buf2, dst_hip),
		frq2str(buf3, sizeof buf3, ftype, freqs));
	path[sizeof path - 1] = 0;

	return path;
}

int FileStorage::saveStatData(char *path, unsigned char *data, size_t size)
{
	int fd = open(path, O_CREAT|O_WRONLY|O_APPEND, 0664);
	if (fd < 0 /*&& errno == ENOENT*/) {
		APPLOG_WARN("open file(%s) failed: %m, try create the dir first", path);
		// create the directory first
		makeDirectory(path);
		fd = open(path, O_CREAT|O_WRONLY|O_APPEND, 0664);	
	}
		
	if (fd < 0) {
		APPLOG_WARN("open file(%s) failed: %m, give up", path);
		return -1;
	}

	int retval = 0;
	for (int i = 0; i < 5; ++i) {
		ssize_t wlen = write(fd, data, size);
		if (wlen == (ssize_t)size) {
			// in most cases, should OK
			break;	
		}

		if (wlen < 0 && errno == EINTR)
			continue;
		if (wlen < 0) {
			APPLOG_ERROR("write (%s) failed: %m", path);
			continue;	/* give up? */
		}

		if (wlen > 0) {
			APPLOG_FATAL("write into %s just finished partially, in-size=%ld, done-size=%ld",
					path, (long)size, (long)wlen);
			retval = -1;
			break;
		}
	}

	close(fd);
	return retval;
}

int FileStorage::saveStatData(const char *typeString, int64_t timestamp, const stat_ip_t& hip, 
			      const stat_id_t& sid, uint8_t ftype, uint8_t freqs,
			      unsigned char *data, size_t size)
{
	// TODO: use GM?
	time_t tsecs = timestamp / 1000;
	struct tm tmbuf, *ptm = localtime_r(&tsecs, &tmbuf);
	char path[PATH_MAX];

	makePath(path, sizeof path, typeString, ptm->tm_year + 1900, sid, hip, ftype, freqs);
	return saveStatData(path, data, size);
}

int FileStorage::saveStatData(const char *typeString, int64_t timestamp, const stat_ip_t& src_hip,
			      const stat_id_t& src_sid, const stat_ip_t& dst_hip, 
			      const stat_id_t& dst_sid, uint8_t ftype, uint8_t freqs,
			      unsigned char *data, size_t dsize)
{
	// TODO: use GM?
	time_t tsecs = timestamp / 1000;
	struct tm tmbuf, *ptm = localtime_r(&tsecs, &tmbuf);
	char path[PATH_MAX];

	makePath(path, sizeof path, typeString, ptm->tm_year + 1900,
		 src_sid, src_hip, dst_sid, dst_hip, ftype, freqs);
	return saveStatData(path, data, dsize);
}

int FileStorage::saveMergedGauge(const StatMergedGauge& gauge)
{
	unsigned char data[512];
	MemoryBuffer msg(data, sizeof data, false);

	msg.writeUint8(STAT_MERGED_GAUGE);
	if (gauge.encodeTo(&msg) < 0) {
		APPLOG_ERROR("encode merged gauge into msg failed!");
		return -1;
	}

	int retval = saveStatData("MG", gauge.timestamp, gauge.hip, gauge.sid, 
			gauge.ftype, gauge.freqs, msg.data(), msg.getWptr());
	return retval;
}

int FileStorage::saveMergedLcall(const StatMergedLcall& lcall)
{
	unsigned char data[8192];
	MemoryBuffer msg(data, sizeof data, false);

	msg.writeUint8(STAT_MERGED_LCALL);
	if (lcall.encodeTo(&msg) < 0) {
		APPLOG_ERROR("encode merged lcall into msg failed!");
		return -1;
	}

	int retval = saveStatData("ML", lcall.timestamp, lcall.hip, lcall.sid, 
			lcall.ftype, lcall.freqs, msg.data(), msg.getWptr());
	return retval;
}


int FileStorage::saveMergedRcall(const StatMergedRcall& rcall)
{
	unsigned char data[8192];
	MemoryBuffer msg(data, sizeof data, false);

	msg.writeUint8(STAT_MERGED_RCALL);
	if (rcall.encodeTo(&msg) < 0) {
		APPLOG_ERROR("encode merged rcall into msg failed!");
		return -1;
	}

	int retval = saveStatData("MR", rcall.timestamp, rcall.src_hip, rcall.src_sid, rcall.dst_hip,
			rcall.dst_sid, rcall.ftype, rcall.freqs, msg.data(), msg.getWptr());
	return retval;
}


#define CT_BUSINESS	0
#define CT_RESOURCE	1

#define GT_DEPARTMENT	0
#define GT_PRODUCT	1
#define GT_MODULE	2
#define GT_HOST		3

#define CEIL(x,u)	(((x) + (u) - 1) / (u) * (u))
#define FLOOR(x,u)	((x) / (u) * (u))

#define MULTIVAL_SEPARATORS	", \t"

// return -1 when data is not enough
// return -2 when file content is corrupted (unknown data)
int FileStorage::parseStatsItem(MemoryBuffer *msg, int64_t start, int64_t end, StatMerger& merger)
{
	uint8_t type;
	if (msg->readUint8(type) < 0) return -1;

	switch (type) {
	case STAT_MERGED_GAUGE: {
		StatMergedGauge gauge;
		if (gauge.parseFrom(msg) < 0) return -1;
		if (gauge.timestamp >= start && gauge.timestamp < end) {
			merger.addMergedGauge(gauge);
		}

		break;
	}
	case STAT_MERGED_LCALL: {
		StatMergedLcall lcall;
		if (lcall.parseFrom(msg) < 0) return -1;
		if (lcall.timestamp >= start && lcall.timestamp < end) {
			merger.addMergedLcall(lcall);
		}

		break;
	}
	case STAT_MERGED_RCALL: {
		StatMergedRcall rcall;
		if (rcall.parseFrom(msg) < 0) return -1;
		if (rcall.timestamp >= start && rcall.timestamp < end) {
			merger.addMergedRcall(rcall);
		}

		break;
	}
	default:
		APPLOG_FATAL("unknown stat type: %d", (int)type);
		return -2;
	}

	return 0;
}

int FileStorage::parseStatsData(MemoryBuffer *msg, int64_t start, int64_t end, StatMerger& merger)
{
	while (msg->getRptr() + 4 < msg->getWptr()) {
		long savedRptr = msg->getRptr();
		int retval;

		if ((retval = parseStatsItem(msg, start, end, merger)) == -2) {
			// unknown data, discard the rest
			return -1;
		}
		else if (retval == -1) {
			// not a full item, read more and parse again
			msg->setRptr(savedRptr);
			break;
		}
	}

	return 0;
}

void FileStorage::loadStatsForYear(const stat_id_t& sid, const stat_ip_t& hip, int year, int64_t start, int64_t end, StatMerger& merger)
{
	char path[PATH_MAX];
	makePath(path, sizeof path, "MG_", year, sid, hip, FT_MINUTE, 1);

	int fd = open(path, O_RDONLY);
	if (fd < 0) {
		APPLOG_ERROR("open(%s) failed: %m", path);
		return;
	}

	unsigned char buf[8192];
	size_t left = 0;

	// TODO: narrow down 

	while (1) {
		ssize_t rlen = read(fd, buf + left, sizeof buf - left);
		if (rlen > 0) {
			left += rlen;
			MemoryBuffer msg(buf, left, false);
			msg.setWptr(left);

			if (parseStatsData(&msg, start, end, merger) < 0) {
				APPLOG_ERROR("parse stats from %s failed", path);
				break;
			}

			left = msg.getWptr() - msg.getRptr();
			memcpy(buf, buf + msg.getRptr(), left);
		}
		else if (rlen == 0) {
			if (left > 0) {
				perror("why left > 0");
				break;
			}

			break;
		}
		else if (rlen < 0 && errno == EINTR) {
			continue;
		}
		else {
			perror("read failed");
			break;
		}
	}

	return;
}

void FileStorage::loadStatsForPeriod(const stat_id_t& sid, const stat_ip_t& hip, int64_t start, int64_t end, StatMerger& merger)
{
	for (int64_t i = start; i < end; /* next year */) {
		time_t tsecs = i / 1000;
		struct tm tmbuf, *ptm = localtime_r(&tsecs, &tmbuf);
		int year = ptm->tm_year + 1900;

		loadStatsForYear(sid, hip, year, start, end, merger);

		// goto next year
		ptm->tm_year += 1;
		ptm->tm_mon = 0; ptm->tm_mday = 1;
		ptm->tm_hour = ptm->tm_min = ptm->tm_sec = 0;
		ptm->tm_isdst = -1;
		i = mktime(ptm) * 1000; // how?
	}
} 

int64_t FileStorage::spanLength(int unit, int count)
{
	switch (unit) {
	case FT_SECOND: return count * 1000;
	case FT_MINUTE: return count * 60 * 1000;
	case FT_HOUR: return count * 3600 * 1000;
	case FT_DAY: return count * 24 * 3600 * 1000;
//	case FT_MONTH: // TODO:
//	case FT_YEAR:
	default:
		return 0;
	}
}

class LocalKeyScanFilter : public ScanFilter {
public:
	LocalKeyScanFilter(const char *_prefix, int _pid, int _mid, int _iid, const host_set_t* _hosts)
		: prefix(_prefix), plen(strlen(_prefix)), pid(_pid), mid(_mid), iid(_iid), hosts(_hosts)
	{ /* nothing */ }
public:
	virtual bool acceptYear(const char *name) { return true; }
	virtual bool acceptProduct(const char *name) { return pid == 0 || strtol(name, NULL, 16) == pid; }
	virtual bool acceptModule(const char *name) { return mid == 0 || strtol(name, NULL, 16) == mid; }
	virtual bool accept(const char *name) {
		if (strncmp(name, prefix, plen) ) return false;
	
		// MG_PID_MID_IID_HOST-IP_1m.bin		
		char ip[128], *vptr, *eptr;
		int pid, mid, iid;
		stat_ip_t hip;

		pid = strtol(name + plen, &eptr, 16);
		if (*eptr != '_') return false;
		
		mid = strtol(eptr + 1, &eptr, 16);
		if (*eptr != '_') return false;

		iid = strtol(eptr + 1, &eptr, 16);
		if (*eptr != '_') return false;
		
		vptr = eptr + 1;
		eptr = strchr(vptr, '_');

		if (eptr == NULL || eptr - vptr > (int)sizeof(ip) - 1)
			return false;	// ignore it
		memcpy(ip, vptr, eptr - vptr);
		ip[eptr - vptr] = 0;

		if (inet_pton(AF_INET, ip, &hip.ip.ip4) == 1) {
			hip.ver = 4;
		}
		else if (inet_pton(AF_INET6, ip, &hip.ip.ip6[0]) == 1) {
			hip.ver = 6;
		}
		else {
			APPLOG_ERROR("invlaid ip(%s) in the name component", ip);
			return false;
		}

		if (hosts != NULL && hosts->find(hip) == hosts->end())
			return false;

		ids.insert(local_key_t(hip, stat_id_t(pid, mid, iid)));
		return true;
	}

public:
	local_key_set_t ids;
private:
	const char *prefix;
	size_t plen;
	int pid, mid, iid;
	const host_set_t *hosts;
};

// TODO:
//class RcallKeyScanFilter : public ScanFilter {
//};

int FileStorage::scanDirectoryModule(ScanFilter *filter, const char *dname)
{
	char path[PATH_MAX];
	xsnprintf(path, sizeof path, "%s%s", baseDir.c_str(), dname);

	DIR *dir = opendir(path);
	if (dir == NULL) return -1;

	unsigned long long buf[PATH_MAX/sizeof(unsigned long long)];
	struct dirent *pe;

	while (readdir_r(dir, (struct dirent *)buf, &pe) == 0) {
		if (pe == NULL) break;
		//if (pe->d_type != DT_REG) continue;
		if (!strcmp(pe->d_name, ".") || !strcmp(pe->d_name, "..")) continue;

		if (!filter->accept(pe->d_name)) continue;
	}

	closedir(dir);
	return 0;
}

int FileStorage::scanDirectoryProduct(ScanFilter *filter, const char *dname)
{
	char path[PATH_MAX];
	xsnprintf(path, sizeof path, "%s%s", baseDir.c_str(), dname);

	DIR *dir = opendir(path);
	if (dir == NULL) return -1;

	unsigned long long buf[PATH_MAX/sizeof(unsigned long long)];
	struct dirent *pe;

	while (readdir_r(dir, (struct dirent *)buf, &pe) == 0) {
		if (pe == NULL) break;
		//if (pe->d_type != DT_DIR) continue;	
		if (!strcmp(pe->d_name, ".") || !strcmp(pe->d_name, "..")) continue;
		if (!filter->acceptModule(pe->d_name)) continue;
		
		char buf[256];
		snprintf(buf, sizeof buf, "%s/%s", dname, pe->d_name); buf[sizeof(buf) - 1] = 0;
		scanDirectoryModule(filter, buf);
	}

	closedir(dir);
	return 0;
}

int FileStorage::scanDirectoryYear(ScanFilter *filter, const char *dname)
{
	char path[PATH_MAX];
	xsnprintf(path, sizeof path, "%s%s", baseDir.c_str(), dname);

	DIR *dir = opendir(path);
	if (dir == NULL) return -1;

	unsigned long long buf[PATH_MAX/sizeof(unsigned long long)];
	struct dirent *pe;

	while (readdir_r(dir, (struct dirent *)buf, &pe) == 0) {
		if (pe == NULL) break;
		//if (pe->d_type != DT_DIR) continue;	
		if (!strcmp(pe->d_name, ".") || !strcmp(pe->d_name, "..")) continue;
		if (!filter->acceptProduct(pe->d_name)) continue;
		
		char buf[256];
		snprintf(buf, sizeof buf, "%s/%s", dname, pe->d_name); buf[sizeof(buf) - 1] = 0;
		scanDirectoryProduct(filter, buf);
	}

	closedir(dir);
	return 0;
}

int FileStorage::scanDirectoryRoot(ScanFilter *filter)
{
	DIR *dir = opendir(baseDir.c_str());
	if (dir == NULL) return -1;

	unsigned long long buf[PATH_MAX/sizeof(unsigned long long)];
	struct dirent *pe;

	while (readdir_r(dir, (struct dirent *)buf, &pe) == 0) {
		if (pe == NULL) break;
		//if (pe->d_type != DT_DIR) continue;
		if (!strcmp(pe->d_name, ".") || !strcmp(pe->d_name, "..")) continue;
		if (!filter->acceptYear(pe->d_name)) continue;

		scanDirectoryYear(filter, pe->d_name);
	}

	closedir(dir);
	return 0;
}

int FileStorage::expandIds(local_key_set_t& ids, int pid, int mid, int iid, const host_set_t *hosts)
{
	LocalKeyScanFilter filter("MG_", pid, mid, iid, hosts);
	if (scanDirectoryRoot(&filter) < 0) return -1;

	ids.swap(filter.ids);
	return 0;
}

int FileStorage::loadStatsForIdsHosts(const local_key_set_t& ids, int64_t start, int64_t end,
			StatMerger& merger, const std::tr1::unordered_set<int>& validIds)
{
	for (local_key_set_t::const_iterator iter = ids.begin(); iter != ids.end(); ++iter) {
		if (validIds.find(iter->sid.iid) == validIds.end())
			continue;

		loadStatsForPeriod(iter->sid, iter->hip, start, end, merger);
	}

	return 0;
}

class DepartMapper : public GroupMapper {
public:
	void map(local_key_t& newKey, const local_key_t& key) {
		newKey.sid = stat_id_t(0, 0, key.sid.iid);
		newKey.hip = 0;
	}

	std::tr1::unordered_set<int> pids;
};

class ProductMapper : public GroupMapper {
public:
	void map(local_key_t& newKey, const local_key_t& key) {
		newKey.sid = stat_id_t(key.sid.pid, 0, key.sid.iid);
		newKey.hip = 0;
	}

	std::tr1::unordered_set<int> mids;
};

class ModuleMapper : public GroupMapper {
public:
	void map(local_key_t& newKey, const local_key_t& key) {
		newKey.sid = stat_id_t(key.sid.pid, key.sid.mid, key.sid.iid);
		newKey.hip = 0;
	}

	std::tr1::unordered_set<stat_ip_t> hosts;
};

class HostMapper : public GroupMapper {
public:
	void map(local_key_t& newKey, const local_key_t& key) {
		newKey.sid = stat_id_t(0, 0, key.sid.iid);
		newKey.hip = key.hip;
	}
};

int FileStorage::combineStats(StatCombiner& combiner, const StatMerger& src, GroupMapper& groupMapper)
{
	for (int i = 0; i < src.periodCount; ++i) {
		for (merged_gauge_map_t::const_iterator iter = src.mergedGauges[i].begin();
			iter != src.mergedGauges[i].end();
				++iter) {
			local_key_t newKey;
			groupMapper.map(newKey, iter->first);

			combiner.addMergedGauge(newKey, iter->second);
		}

		for (merged_lcall_map_t::const_iterator iter = src.mergedLcalls[i].begin();
			iter != src.mergedLcalls[i].end();
				++iter) {
			local_key_t newKey;
			groupMapper.map(newKey, iter->first);

			combiner.addMergedLcall(newKey, iter->second);
		}

		for (merged_rcall_map_t::const_iterator iter = src.mergedRcalls[i].begin();
			iter != src.mergedRcalls[i].end();
				++iter) {
		// TODO:
		//	rcall_key_t newKey;
		//	groupMapper.map(newKey, iter->first);
		//	combiner.addMergedRcall(newKey, iter->second);
		}
	}

	return 0;
}

//
// case 0: depart-level
//	did => [pid,...], [pid,...]
//	host=list|auto
// case 1: pid-level
//	pid, *, *
//	host=list|auto
// case 2: mid-level
//	pid, mid, *
//	host=list|auto
// case 3: host-level
//	pid, mid, *
//	host=ip
// case 4: expand-to-individual-hosts
//	pid,mid,iid as in [0-2] (no case #3)
//	host=[auto]
//
int FileStorage::getSystemStats(StatCombiner& combiner, int context, int totalView, int64_t startDtime, int64_t endDtime, 
				int spanUnit, int spanCount, int pid, int mid,
				const std::vector<int> iids,
				const host_set_t& hosts)
{
	int mergeCount = (endDtime - startDtime) / spanLength(spanUnit, spanCount);
	StatMerger merger(spanUnit, spanCount, startDtime, mergeCount);
	
	if (pid == 0) {
		APPLOG_ERROR("pid can not be 0");
		return -1;
	}

	int cpuTotal = 0, cpuCores = 0; std::tr1::unordered_set<int> cpuIds;
	int memory = 0;
	int loadAvg = 0;
	int netAll = 0; std::tr1::unordered_set<int> netIds;
	int diskAll = 0; std::tr1::unordered_set<int> diskIds;

	for (std::vector<int>::const_iterator iter = iids.begin();
		iter != iids.end();
			++iter) {
		if (IID_IS4CPU(*iter)) {
			int id = IID2CPUNO(*iter);
			if (id == IID_CPU_CORES) cpuCores = 1;
			else if (id == IID_CPU_TOTAL) cpuTotal = 1;
			else cpuIds.insert(id);
		}
		else if (IID_IS4MEM(*iter)) {
			memory = 1;
		}
		else if (IID_IS4LOADAVG(*iter)) {
			loadAvg = 1;
		}
		else if (IID_IS4NET(*iter)) {
			int id = IID2NETNO(*iter);
			if (id == IID_NET_ALL) netAll = 1;
			else netIds.insert(id);
		}
		else if (IID_IS4DISK(*iter)) {
			int id = IID2DISKNO(*iter);
			if (id == IID_DISK_ALL) diskAll = 1;
			else diskIds.insert(id);
		}
	}

	// get all possible iids first
	local_key_set_t ids;
	if (expandIds(ids, pid, mid, 0, NULL) < 0) {
		APPLOG_WARN("invalid pid/mid/iid parameters: %m");
		return -1;
	}

	// TODO: mapping business-{pid,mid} => hosts => resource-{pid,mid}
	if (context == CT_BUSINESS) {
		// TODO:
		// mapBusiness2ResourceIds(ids, id2Map);
	}

	// step 5: load data and merge into bigger span
	if (cpuTotal || cpuCores || !cpuIds.empty()) {
		std::tr1::unordered_set<int> iids;
		std::tr1::unordered_set<int> newIds;

		for (local_key_set_t::iterator iter = ids.begin(); iter != ids.end(); ++iter) {
			if (!IID_IS4CPU(iter->sid.iid)) continue;

			int cpuId = IID2CPUNO(iter->sid.iid);
			if ((cpuTotal && cpuId == IID_CPU_TOTAL)
				|| (cpuCores && cpuId != IID_CPU_TOTAL)
					|| cpuIds.find(cpuId) != cpuIds.end()) {
				iids.insert(iter->sid.iid);
				newIds.insert(cpuId);	
			}
		}
		
		cpuIds.swap(newIds);
		loadStatsForIdsHosts(ids, startDtime, endDtime, merger, iids);
	}

	if (memory) {
		std::tr1::unordered_set<int> iids;
		for (local_key_set_t::iterator iter = ids.begin(); iter != ids.end(); ++iter) {
			if (IID_IS4MEM(iter->sid.iid)) {
				iids.insert(iter->sid.iid);
			}
		}

		loadStatsForIdsHosts(ids, startDtime, endDtime, merger, iids);
	}

	
	if (loadAvg) {
		std::tr1::unordered_set<int> iids;

		for (local_key_set_t::iterator iter = ids.begin(); iter != ids.end(); ++iter) {
			if (IID_IS4LOADAVG(iter->sid.iid)) {
				iids.insert(iter->sid.iid);
			}
		}

		loadStatsForIdsHosts(ids, startDtime, endDtime, merger, iids);
	}

	if (netAll || !netIds.empty()) {
		std::tr1::unordered_set<int> iids;
		std::tr1::unordered_set<int> newIds;

		for (local_key_set_t::iterator iter = ids.begin(); iter != ids.end(); ++iter) {
			if (!IID_IS4NET(iter->sid.iid)) continue;

			int netId = IID2NETNO(iter->sid.iid);
			if (netAll || netIds.find(netId) != netIds.end()) {
				iids.insert(iter->sid.iid);
				newIds.insert(netId);
			}
		}
		
		netIds.swap(newIds);
		loadStatsForIdsHosts(ids, startDtime, endDtime, merger, iids);
	}

	if (diskAll || !diskIds.empty()) {
		std::tr1::unordered_set<int> iids;
		std::tr1::unordered_set<int> newIds;

		for (local_key_set_t::iterator iter = ids.begin(); iter != ids.end(); ++iter) {
			if (!IID_IS4DISK(iter->sid.iid)) continue;

			int diskId = IID2DISKNO(iter->sid.iid);
			if (diskAll || diskIds.find(diskId) != diskIds.end()) {
				iids.insert(iter->sid.iid);
				newIds.insert(diskId);
			}
		}

		diskIds.swap(newIds);
		loadStatsForIdsHosts(ids, startDtime, endDtime, merger, iids);
	}

	// further merge
	//StatCombiner combiner(spanUnit, spanCount, startDtime, mergeCount);
/*	if (hasMapping) {
			for(int i = 0; i < N; ++i) {
				for (src::iterator iter = srcIds.begin(); iter != srcIds.end(); ++iter) {
					key dst;
					key = mapp(srcKey);
					get its dst metrics;
					combine();
				}
				else for each one in [i]; do {
					key = mapper(key);
					combine();
				}
			}
	}
**/
	int gtype;
	if (pid == 0 || (mid == 0 && totalView)) {
		// department-level/product-level: merge by pid
		ProductMapper mapper;
		gtype = GT_PRODUCT;
		combineStats(combiner, merger, mapper);
	}
 	else if (mid == 0 || (mid != 0 && totalView)) {
		// module-level: put all together by pid + mid
		ModuleMapper mapper;
		gtype = GT_MODULE;
		combineStats(combiner, merger, mapper);
	}
	else {
		// show one host's metrics located by {pid,mid,host}
		HostMapper mapper;
		gtype = GT_HOST;
		combineStats(combiner, merger, mapper);
	}

	return 0;
}

