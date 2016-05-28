/* StatAgentProcessor.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
#ifndef __STAT_MERGER__H
#define __STAT_MERGER__H

#include <tr1/unordered_map>
#include "StatData.h"

class StatMerger {
public:
	// merging for saving stats data
	StatMerger(void *data, int (*saveMergedGauges)(void *, const merged_gauge_map_t *), 
		   int (*saveMergedLcalls)(void *, const merged_lcall_map_t *),
		   int (*saveMergedRcalls)(void *, const merged_rcall_map_t *),
		   int ftype, int freqs, int n);
	// merging for getting merged data
	StatMerger(int ftype, int freqs, int64_t periodStartTime, int n);
	~StatMerger();
private:
	StatMerger(const StatMerger&);
	StatMerger& operator=(const StatMerger&);
public:
	int addItemGauge(const StatItemGauge& gauge);
	int addMergedGauge(const StatMergedGauge& gauge);

	int addItemLcall(const StatItemLcall& lcall);
	int addMergedLcall(const StatMergedLcall& lcall);

	int addItemRcall(const StatItemRcall& rcall);
	int addMergedRcall(const StatMergedRcall& rcall);
public:
	void moveAhead(int n);
private:
	int64_t periodStart(int64_t timestamp);
	int64_t periodAdd(int64_t timestamp, int  count);
	int periodIndex(int64_t timestamp);
	int locateIndex(int64_t timestamp);
public:
	void *data;
	int (*saveMergedGauges)(void *data, const merged_gauge_map_t *);
	int (*saveMergedLcalls)(void *data, const merged_lcall_map_t *);
	int (*saveMergedRcalls)(void *data, const merged_rcall_map_t *);

	int ftype;
	int freqs;
	
	int64_t periodStartTime;
	int64_t latestTimestamp;

	int periodCount;
	merged_gauge_map_t *mergedGauges;
	merged_lcall_map_t *mergedLcalls;
	merged_rcall_map_t *mergedRcalls;
};

#endif /* __STAT_MERGER__H */
