/* FileStorage.h
 * Copyright by Beyondy.c.w 2002-2020
**/
#ifndef __FILE_STORAGE__H
#define __FILE_STORAGE__H

#include <stdint.h>
#include <errno.h>
#include <string>
#include <vector>
#include <tr1/unordered_set>

#include "StatData.h"

class StatMerger;
class StatCombiner;

typedef std::tr1::unordered_set<local_key_t, LocalKeyHash> local_key_set_t;
typedef std::tr1::unordered_set<rcall_key_t, LocalKeyHash> rcall_key_set_t;
typedef std::tr1::unordered_set<stat_ip_t, HipHash> host_set_t;

class ScanFilter {
public:
	virtual bool acceptYear(const char *name) = 0;
	virtual bool acceptProduct(const char *name) = 0;
	virtual bool acceptModule(const char *name) = 0;
	virtual bool accept(const char *name) = 0;
};

class GroupMapper {
public:
	virtual void map(local_key_t& newKey, const local_key_t& key) = 0;
};

class FileStorage {
public:
	FileStorage() { /* nothing */ }
private:
	FileStorage(const FileStorage&);
	FileStorage& operator=(const FileStorage&);
private:
	char *frq2str(char *buf, size_t size, uint8_t ftype, uint8_t freqs);
	char *hip2str(char *buf, size_t size, const stat_ip_t& hip);
	int makeDirectory(char *path);
	char *makePath(char *path, size_t size, const char *typeString, int year,
		       const stat_id_t& sid, const stat_ip_t& hip,
		       uint8_t ftype, uint8_t freqs);
	char *makePath(char *path, size_t size, const char *typeString, int year,
		       const stat_id_t& src_sid, const stat_ip_t& src_hip,
		       const stat_id_t& dst_sid, const stat_ip_t& dst_hip,
		       uint8_t ftype, uint8_t freqs);
	int saveStatData(char *path, unsigned char *data, size_t size);
	int saveStatData(const char *typeString, int64_t timestamp, const stat_ip_t& hip,
			 const stat_id_t& sid, uint8_t ftype, uint8_t freqs,
			 unsigned char *data, size_t size);
	int saveStatData(const char *typeString, int64_t timestamp, const stat_ip_t& src_hip,
			 const stat_id_t& src_sid, const stat_ip_t& dst_hip,
			 const stat_id_t& dst_sid, uint8_t ftype, uint8_t freqs,
			 unsigned char *data, size_t dsize);
public:
	void setDirectory(const std::string& _baseDir) { baseDir = _baseDir; }
	std::string getDirectory() const { return baseDir; }

	int saveMergedGauge(const StatMergedGauge& guage);
	int saveMergedLcall(const StatMergedLcall& lcall);
	int saveMergedRcall(const StatMergedRcall& rcall);
private:
	std::string baseDir;

private:
	int parseStatsItem(MemoryBuffer *msg, int64_t start, int64_t end, StatMerger& merger);
	int parseStatsData(MemoryBuffer *msg, int64_t start, int64_t end, StatMerger& merger);
	void loadStatsForYear(const stat_id_t& sid, const stat_ip_t& hip, int year, 
		int64_t start, int64_t end, StatMerger& merger);
	void loadStatsForPeriod(const stat_id_t& sid, const stat_ip_t& hip, 
		int64_t start, int64_t end, StatMerger& merger);
	int64_t spanLength(int unit, int count);
	int scanDirectoryModule(ScanFilter *filter, const char *dname);
	int scanDirectoryProduct(ScanFilter *filter, const char *dname);
	int scanDirectoryYear(ScanFilter *filter, const char *dname);
	int scanDirectoryRoot(ScanFilter *filter);
	int loadStatsForIdsHosts(const local_key_set_t& ids, int64_t start, int64_t end, 
		StatMerger& merger, const std::tr1::unordered_set<int>& validIds);
	int combineStats(StatCombiner& combiner, const StatMerger& src, GroupMapper& groupMapper);
	int expandIds(local_key_set_t& ids, int pid, int mid, int iid, const host_set_t *hosts);
public:
	int getSystemStats(StatCombiner& combiner, int context, int totalView, int64_t start, int64_t end,
		int spanUnit, int spanCount, int pid, int mid, 
		const std::vector<int> iids, const host_set_t& hosts);
	int getUserStats(int totalView, int64_t start, int64_t end, int spanUnit, int spanCount,
		int type, int pid, int mid, int iid, const host_set_t& hosts);

};

#endif /* __FILE_STORAGE__H */
