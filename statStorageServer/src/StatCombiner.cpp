/* StatCombiner.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
#include <assert.h>
#include <errno.h>

#include "StatCombiner.h"

#define PERIOD_MAX	2

StatCombiner::StatCombiner(int _ftype, int _freqs, int64_t _periodStartTime, int _n)
	: ftype(_ftype), freqs(_freqs), periodStartTime(_periodStartTime),
	  periodCount(_n), mergedGauges(0), mergedLcalls(0), mergedRcalls(0)
{
	if (periodCount < PERIOD_MAX) periodCount = PERIOD_MAX;
	
	mergedGauges = new merged_gauge_map_t[periodCount];
	mergedLcalls = new merged_lcall_map_t[periodCount];
	mergedRcalls = new merged_rcall_map_t[periodCount];
}

StatCombiner::~StatCombiner()
{
	delete[] mergedGauges;
	delete[] mergedLcalls;
	delete[] mergedRcalls;
}

int64_t StatCombiner::periodStart(int64_t time)
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

int StatCombiner::periodIndex(int64_t timestamp)
{
	int64_t delta = timestamp - periodStartTime;
	if (ftype == FT_SECOND) return delta / (freqs * 1000);
	if (ftype == FT_MINUTE) return delta / (freqs * 60000);

	assert("TODO" == NULL);
	return 0;
}

int StatCombiner::addItemGauge(const local_key_t& key, const StatItemGauge& gauge)
{
	return -1;
}

int StatCombiner::addMergedGauge(const local_key_t& key, const StatMergedGauge& gauge)
{
	int64_t periodTime = periodStart(gauge.timestamp);
	int index = periodIndex(gauge.timestamp);
	if (index < 0 || index >= periodCount)
		return -1;

	merged_gauge_map_t& maps = mergedGauges[index];
	gauge_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedGauge& mgauge = maps[key];
		mgauge.timestamp = periodTime;
		mgauge.hip = key.hip;	// use key's IP and ID
		mgauge.sid = key.sid;	// ignore gauge's
		mgauge.ftype = ftype;
		mgauge.freqs = freqs;
		mgauge.gtype = gauge.gtype;
		mgauge.gval = gauge.gval;
	}
	else {
		StatMergedGauge& mgauge = iter->second;
		if (gauge.gtype == SGT_SNAPSHOT) {
			mgauge.gtype = gauge.gtype;
			mgauge.gval += gauge.gval;
		}
		else if (gauge.gtype == SGT_DELTA) {
			// TODO:
			mgauge.gtype = gauge.gtype;
			mgauge.gval += gauge.gval;
		}
	}
	
	return 0;
}

int StatCombiner::addItemLcall(const local_key_t& key, const StatItemLcall& lcall)
{
	return -1;
}

int StatCombiner::addMergedLcall(const local_key_t& key, const StatMergedLcall& lcall)
{
	int64_t periodTime = periodStart(lcall.timestamp);
	int index = periodIndex(lcall.timestamp);
	if (index < 0 || index >= periodCount)
		return -1;

	merged_lcall_map_t& maps = mergedLcalls[index];
	lcall_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedLcall& mcalls = maps[key];	// create a new one first
		mcalls.timestamp = periodTime;
		mcalls.hip = key.hip;
		mcalls.sid = key.sid;
		mcalls.ftype = ftype;
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

int StatCombiner::addItemRcall(const rcall_key_t& key, const StatItemRcall& rcall)
{
	return -1;
}

// param is not const for potential swap
int StatCombiner::addMergedRcall(const rcall_key_t& key, const StatMergedRcall& rcall)
{
	int64_t periodTime = periodStart(rcall.timestamp);
	int index = periodIndex(rcall.timestamp);
	if (index < 0 || index >= periodCount)
		return -1;

	merged_rcall_map_t& maps = mergedRcalls[index];
	rcall_iterator iter = maps.find(key);
	if (iter == maps.end()) {
		StatMergedRcall& mcalls = maps[key];	// insert one
		mcalls.timestamp = periodTime;
		mcalls.src_hip = key.src_hip;
		mcalls.src_sid = key.src_sid;
		mcalls.dst_hip = key.dst_hip;
		mcalls.dst_sid = key.dst_sid;
		mcalls.ftype = ftype;	// use member's frequency
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

