/* StatAgentProcessor.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
#include <assert.h>
#include <errno.h>

#include "StatMerger.h"

#define PERIOD_MAX	2

StatMerger::StatMerger(void *_data, int (*_saveG)(void *, const merged_gauge_map_t *), 
		       int (*_saveL)(void *, const merged_lcall_map_t *),
		       int (*_saveR)(void *, const merged_rcall_map_t *),
		       int _ftype, int _freqs, int _n)
	: data(_data), saveMergedGauges(_saveG), saveMergedLcalls(_saveL), saveMergedRcalls(_saveR),
	  ftype(_ftype), freqs(_freqs), periodStartTime(0), latestTimestamp(0),
	  periodCount(_n), mergedGauges(0), mergedLcalls(0), mergedRcalls(0)
{
	if (periodCount < PERIOD_MAX) periodCount = PERIOD_MAX;
	
	mergedGauges = new merged_gauge_map_t[periodCount];
	mergedLcalls = new merged_lcall_map_t[periodCount];
	mergedRcalls = new merged_rcall_map_t[periodCount];
}

StatMerger::StatMerger(int _ftype, int _freqs, int64_t _periodStartTime, int _n)
	: data(NULL), saveMergedGauges(NULL), saveMergedLcalls(NULL), saveMergedRcalls(NULL),
	  ftype(_ftype), freqs(_freqs), periodStartTime(_periodStartTime), latestTimestamp(0),
	  periodCount(_n), mergedGauges(0), mergedLcalls(0), mergedRcalls(0)
{
	if (periodCount < PERIOD_MAX) periodCount = PERIOD_MAX;

	mergedGauges = new merged_gauge_map_t[periodCount];
	mergedLcalls = new merged_lcall_map_t[periodCount];
	mergedRcalls = new merged_rcall_map_t[periodCount];
}

StatMerger::~StatMerger()
{
	delete[] mergedGauges;
	delete[] mergedLcalls;
	delete[] mergedRcalls;
}

int64_t StatMerger::periodStart(int64_t time)
{
	if (ftype == FT_SECOND) {
		return time / 1000 / freqs * freqs * 1000;
	}

	if (ftype == FT_MINUTE) {
		// assume TZ is HOUR
		return time / 60000 / freqs * freqs * 60000;
	}

	assert("TODO" == NULL);
	return time;
}

int64_t StatMerger::periodAdd(int64_t timestamp, int  count)
{
	if (ftype == FT_SECOND) return timestamp + count * 1000;
	if (ftype == FT_MINUTE) return timestamp + count * 60000;

	assert("TODO" == NULL);
	return timestamp;
}

int StatMerger::periodIndex(int64_t timestamp)
{
	int64_t delta = timestamp - periodStartTime;
	if (ftype == FT_SECOND) return delta / (freqs * 1000);
	if (ftype == FT_MINUTE) return delta / (freqs * 60000);

	assert("TODO" == NULL);
	return 0;
}

int StatMerger::locateIndex(int64_t timestamp)
{
	int64_t periodTime = periodStart(timestamp);
	int index = -1;

	if (periodStartTime == 0) {
		// first item, place at the last position
		periodStartTime = periodAdd(periodTime, -periodCount + 1);
		index = periodCount - 1;
	}
	else {
		index = periodIndex(periodTime);
	}

	if (index < 0) {
		// too old come again, discard it
		// or add into the oldest item? NOT YET
		return -1;
	}
	else if (index >= periodCount) {
		// move ahead, make it the last one
		moveAhead(index - periodCount + 1);
		index = periodCount - 1;
	}

	return index;
}

void StatMerger::moveAhead(int n)
{
	// send them out
	for (int i = 0; i < n && i < periodCount; ++i) {
		if (mergedGauges[i].size() > 0) {
			if (saveMergedGauges != NULL) {
				(*saveMergedGauges)(data, &mergedGauges[i]);
			}

			mergedGauges[i].clear();
		}

		if (mergedLcalls[i].size() > 0) {
			if (saveMergedLcalls != NULL) {
				(*saveMergedLcalls)(data, &mergedLcalls[i]);
			}

			mergedLcalls[i].clear();
		}

		if (mergedRcalls[i].size() > 0) {
			if (saveMergedRcalls != NULL) {
				(*saveMergedRcalls)(data, &mergedRcalls[i]);
			}

			mergedRcalls[i].clear();
		}
	}

	// move ahead
	for (int i = n; i < periodCount; ++i) {
		mergedGauges[i - n].swap(mergedGauges[i]);
		mergedLcalls[i - n].swap(mergedLcalls[i]);
		mergedRcalls[i - n].swap(mergedRcalls[i]);
	}

	// clear the rest
	for (int i = n; i < periodCount; ++i) {
		mergedGauges[i].clear();
		mergedLcalls[i].clear();
		mergedRcalls[i].clear();
	}

	return;
}

int StatMerger::addItemGauge(const StatItemGauge& gauge)
{
	int64_t periodTime = periodStart(gauge.timestamp);
	int index = locateIndex(gauge.timestamp);
	if (index < 0) return -1;

	merged_gauge_map_t& maps = mergedGauges[index];
	local_key_t key(gauge.hip, gauge.sid);
	gauge_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedGauge& mgauge = maps[key];
		mgauge.timestamp = periodTime;
		mgauge.hip = gauge.hip;
		mgauge.sid = gauge.sid;
		mgauge.ftype = ftype;
		mgauge.freqs = freqs;
		mgauge.gtype = gauge.gtype;
		mgauge.gval = gauge.gval;
	}
	else {
		StatMergedGauge& mgauge = iter->second;
		if (gauge.gtype == SGT_SNAPSHOT) {
			mgauge.gtype = gauge.gtype;
			mgauge.gval = gauge.gval;
		}
		else if (gauge.gtype == SGT_DELTA) {
			// TODO:
		}
	}
	
	return 0;
}

int StatMerger::addMergedGauge(const StatMergedGauge& gauge)
{
	int64_t periodTime = periodStart(gauge.timestamp);
	int index = locateIndex(gauge.timestamp);
	if (index < 0) return -1;

	merged_gauge_map_t& maps = mergedGauges[index];
	local_key_t key(gauge.hip, gauge.sid);
	gauge_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedGauge& mgauge = maps[key];
		mgauge.timestamp = periodTime;
		mgauge.hip = gauge.hip;
		mgauge.sid = gauge.sid;
		mgauge.ftype = ftype;
		mgauge.freqs = freqs;
		mgauge.gtype = gauge.gtype;
		mgauge.gval = gauge.gval;
	}
	else {
		StatMergedGauge& mgauge = iter->second;
		if (gauge.gtype == SGT_SNAPSHOT) {
			mgauge.gtype = gauge.gtype;
			mgauge.gval = gauge.gval;
		}
		else if (gauge.gtype == SGT_DELTA) {
			// TODO:
		}
	}
	
	return 0;
}


int StatMerger::addItemLcall(const StatItemLcall& lcall)
{
	int64_t periodTime = periodStart(lcall.timestamp);
	int index = locateIndex(lcall.timestamp);
	if (index < 0) return -1;

	merged_lcall_map_t& maps = mergedLcalls[index];
	local_key_t key(lcall.hip, lcall.sid);
	lcall_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedLcall& mcalls = maps[key];
		mcalls.timestamp = periodTime;
		mcalls.hip = lcall.hip;
		mcalls.sid = lcall.sid;
		mcalls.ftype = ftype;
		mcalls.freqs = freqs;

		stat_mresult_t& mresult = mcalls.rets[lcall.retcode];
		MRESULT_FIRST(mresult, lcall.result);
	}
	else {
		StatMergedLcall& mcalls = iter->second;
		StatMergedLcall::iterator iter2 = mcalls.rets.find(lcall.retcode);
		if (iter2 == mcalls.rets.end()) {
			MRESULT_FIRST(iter2->second, lcall.result);
		}
		else {
			stat_mresult_t& mresult = iter2->second;
			MRESULT_AVG(mresult, lcall.result);
		}
	}

	return 0;
}

int StatMerger::addMergedLcall(const StatMergedLcall& lcall)
{
	int64_t periodTime = periodStart(lcall.timestamp);
	int index = locateIndex(lcall.timestamp);
	if (index < 0) return -1;

	merged_lcall_map_t& maps = mergedLcalls[index];
	local_key_t key(lcall.hip, lcall.sid);
	lcall_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedLcall& mcalls = maps[key];	// create a new one first
		mcalls.timestamp = periodTime;
		mcalls.hip = lcall.hip;
		mcalls.sid = lcall.sid;
		mcalls.ftype = ftype;	/* use member's frequency */
		mcalls.freqs = freqs;

		mcalls.rets.insert(lcall.rets.begin(), lcall.rets.end());
	}
	else {
		StatMergedLcall& mcalls = iter->second;
		for (StatMergedLcall::const_iterator iter2 = lcall.rets.begin(); iter2 != lcall.rets.end(); ++iter2) {
			StatMergedLcall::iterator iter3 = mcalls.rets.find(iter2->first);
			if (iter3 == mcalls.rets.end()) {
				MRESULT_FIRST(iter3->second, iter2->second);
			}
			else {
				stat_mresult_t& mresult = iter3->second;
				MRESULT_AVG(mresult, iter2->second);
			}
		}
	}

	return 0;
}

int StatMerger::addItemRcall(const StatItemRcall& rcall)
{
	int64_t periodTime = periodStart(rcall.timestamp);
	int index = locateIndex(rcall.timestamp);
	if (index < 0) return -1;

	merged_rcall_map_t& maps = mergedRcalls[index];
	rcall_key_t key(rcall.src_hip, rcall.src_sid, rcall.dst_hip, rcall.dst_sid);
	rcall_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedRcall& mcalls = maps[key];
		mcalls.timestamp = periodTime;
		mcalls.src_hip = rcall.src_hip;
		mcalls.src_sid = rcall.src_sid;
		mcalls.dst_hip = rcall.dst_hip;
		mcalls.dst_sid = rcall.dst_sid;
		mcalls.ftype = ftype;
		mcalls.freqs = freqs;

		stat_mresult_t& mresult = mcalls.rets[rcall.retcode];
		MRESULT_FIRST(mresult, rcall.result);
	}
	else {
		StatMergedRcall& mcalls = iter->second;
		StatMergedRcall::iterator iter2 = mcalls.rets.find(rcall.retcode);
		if (iter2 == mcalls.rets.end()) {
			MRESULT_FIRST(iter2->second, rcall.result);
		}
		else {
			stat_mresult_t& mresult = iter2->second;
			MRESULT_AVG(mresult, rcall.result);
		}
	}

	return 0;
}

int StatMerger::addMergedRcall(const StatMergedRcall& rcall)
{
	int64_t periodTime = periodStart(rcall.timestamp);
	int index = locateIndex(rcall.timestamp);
	if (index < 0) return -1;

	merged_rcall_map_t& maps = mergedRcalls[index];
	rcall_key_t key(rcall.src_hip, rcall.src_sid,  rcall.dst_hip, rcall.dst_sid);
	rcall_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedRcall& mcalls = maps[key];	// insert one
		mcalls.timestamp = periodTime;
		mcalls.src_hip = rcall.src_hip;
		mcalls.src_sid = rcall.src_sid;
		mcalls.dst_hip = rcall.dst_hip;
		mcalls.dst_sid = rcall.dst_sid;
		mcalls.ftype = ftype;	/* use member's frequency */
		mcalls.freqs = freqs;

		mcalls.rets.insert(rcall.rets.begin(), rcall.rets.end());
	}
	else {
		StatMergedRcall& mcalls = iter->second;
		for (StatMergedRcall::const_iterator iter2 = rcall.rets.begin(); iter2 != rcall.rets.end(); ++iter2) {
			StatMergedRcall::iterator iter3 = mcalls.rets.find(iter2->first);
			if (iter3 == mcalls.rets.end()) {
				MRESULT_FIRST(iter3->second, iter2->second);
			}
			else {
				stat_mresult_t& mresult = iter3->second;
				MRESULT_AVG(mresult, iter2->second);
			}
		}
	}

	return 0;
}

