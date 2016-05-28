/* StatAgentProcessor.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
#ifndef __STAT_COMBINER__H
#define __STAT_COMBINER__H

#include <tr1/unordered_map>
#include "StatData.h"

class StatCombiner {
public:
	StatCombiner(int ftype, int freqs, int64_t periodStartTime, int n);
	~StatCombiner();
private:
	StatCombiner(const StatCombiner&);
	StatCombiner& operator=(const StatCombiner&);
public:
	int addItemGauge(const local_key_t& key, const StatItemGauge& gauge);
	int addMergedGauge(const local_key_t& key, const StatMergedGauge& gauge);

	int addItemLcall(const local_key_t& key, const StatItemLcall& lcall);
	int addMergedLcall(const local_key_t& key, const StatMergedLcall& lcall);

	int addItemRcall(const rcall_key_t& key, const StatItemRcall& rcall);
	int addMergedRcall(const rcall_key_t& key, const StatMergedRcall& rcall);

	int parseFrom(MemoryBuffer *msg);
	int encodeTo(MemoryBuffer *msg);
private:
	int64_t periodStart(int64_t timestamp);
	int periodIndex(int64_t timestamp);
public:
	int ftype;
	int freqs;

	int64_t periodStartTime;

	int periodCount;
	merged_gauge_map_t *mergedGauges;
	merged_lcall_map_t *mergedLcalls;
	merged_rcall_map_t *mergedRcalls;
};

#endif /* __STAT_COMBINER__H */
