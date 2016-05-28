/* SystemMetrics.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
// ** INPUT **
//
// ctx=business|resource
// group=total|list
//
// time-specify-by
//      start=&end= or last=1m/5m/...
//	span=auto|5m|30m
//      align=1m&span=auto|5m|30m
//
// width=1024px to termine how span should be if it is not specified.
//
//
// did=[Depart ID], optional
// pid=
// mid=
// iid=[all] or {cpu-total,cpu-cores,cpu-0,cpu-1,...,load-avg,mem,net-all,disk-all,net-eth0,disk-sda...}
// host=auto|ip[,...]
//
// ** OUTPUT **
// var  var systemMonitoringStats = { 
//	start: "2010-01-01 00:00:00",
//	end: "2010-12-31 23:59:59",
//	span: 5m,
//	host: { cpu-count: 4, ram: 8G, swap: 16G, disks: 1000GB, ... }
//	names:{ pid-x: { val: 1, nam: xx, mid-x: { val: 2, nam: xx, iid-x: name } } } 
//	stats: [ 
//		{ 
//			dtime: "2010-01-01 00:00:00", 
//		 	data: [ 
//				{ group: D|P|M|H, pid, mid, hip, host, name: "cpu-xx", type: "CPU", values: { usr:10,sys:20,idl:50,wt:20 } }, 
//				{ name: "memory", type: "memory", values:{free,used,cached,buffers} }, 
//				{ name: swap, type: swap, values:{free,used} }, 
//				{ name: eth0 type: network values:{in-pkt:34/2,in-bytes:1M/s,out-pkt:50/s,out-bytes:1024K/s} },
//				{ name: lo type: network values:{...} },
//				{ name: sda type: disk, values:{r/s,w/s,r-merged/s,w-merged/s,q-size,q-svrtime,...} }, 
//				{ name: sdb, type: disk, values:{} }
//			     ] 
//		}, 
//		{ ...} 
//	] 
// }
//

// Note: host's pid & mid.
//
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/times.h>
#include <fcntl.h>
#include <stdio.h>
#include <dirent.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <errno.h>
#include <arpa/inet.h>
#include <vector>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include "Log.h"
#include "utils.h"
#include "MemoryBuffer.h"
#include "StatData.h"
#include "StatCombiner.h"
#include "StatSystemIids.h"
#include "ClientConnection.h"
#include "QueryParameters.h"

#define CT_BUSINESS	0
#define CT_RESOURCE	1

#define GT_DEPARTMENT	0
#define GT_PRODUCT	1
#define GT_MODULE	2
#define GT_HOST		3

#define CEIL(x,u)	(((x) + (u) - 1) / (u) * (u))
#define FLOOR(x,u)	((x) / (u) * (u))

#define MULTIVAL_SEPARATORS	", \t"

typedef std::tr1::unordered_set<local_key_t, LocalKeyHash> local_key_set_t;
typedef std::tr1::unordered_set<rcall_key_t, LocalKeyHash> rcall_key_set_t;
typedef std::tr1::unordered_set<stat_ip_t, HipHash> host_set_t;

static const char *storageAddress = "tcp://127.0.0.1:6020";

static char *formatDtime(char *buf, size_t size, int64_t ts)
{
	time_t tsecs = ts / 1000;
	struct tm tmbuf, *ptm = localtime_r(&tsecs, &tmbuf);
	xsnprintf(buf, size, "%04d-%02d-%02d %02d:%02d:%02d",
		ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday,
		ptm->tm_hour, ptm->tm_min, ptm->tm_sec);
	return buf;
}

static int64_t parseDtime(const char *str)
{
	struct tm tmbuf;
	memset(&tmbuf, 0, sizeof tmbuf);

	int year = 1970, mon = 1, mday = 1;
	int hour = 0, min = 0, sec = 0;
	if (sscanf(str, "%d-%d-%d %d:%d:%d", &year, &mon, &mday, &hour, &min, &sec) == 6
		|| sscanf(str, "%d-%d-%d", &year, &mon, &mday) == 3
			|| sscanf(str, "%d/%d/%d", &mon, &mday, &year) == 3) {
		// OK
	}

	tmbuf.tm_year = year - 1900;
	tmbuf.tm_mon = mon - 1;
	tmbuf.tm_mday = mday;
	tmbuf.tm_hour = hour;
	tmbuf.tm_min = min;
	tmbuf.tm_sec = sec;

	return mktime(&tmbuf) * 1000;
}

static char *hip2str(char *buf, size_t size, const stat_ip_t& hip)
{
	if (hip.ver == 4)
		inet_ntop(AF_INET, (void *)&hip.ip.ip4, buf, size);
	else if (hip.ver == 6)
		inet_ntop(AF_INET6, (void *)&hip.ip.ip6[0], buf, size);
	else buf[0] = 0;

	return buf;
}

static char *frq2str(char *buf, size_t size, uint8_t ftype, uint8_t freqs)
{
	const char *fts[] = { "s", "m", "h", "d", "m", "y" };
	const char *ffs = (ftype >= 0 && ftype <= sizeof(fts)/sizeof(fts[0])) ? fts[ftype] : "?";

	xsnprintf(buf, size, "%u%s", freqs, ffs);
	return buf;
}

static char *formatSpan(char *buf, size_t size, int unit, int count)
{
	const char *postfix = "";
	switch (unit) {
	case FT_SECOND: postfix = "s"; break;
	case FT_MINUTE: postfix = "m"; break;
	case FT_HOUR: postfix = "h"; break;
	default:
		break;
	}

	return xsnprintf(buf,size, "%d%s", count, postfix);
}

static int timeUnit(const char *str)
{
	switch (*str) {
	case 's': return FT_SECOND;
	case 'm': return FT_MINUTE;
	case 'h': return FT_HOUR;
	case 'd': return FT_DAY;
//	case 'w': return FT_WEEK;
//	case 'M': return FT_MONTH;
//	case 'Y': return FT_YEAR;
	default:
		break;
	}

	return FT_UNKNOWN;
}

static int64_t spanLength(int unit, int count)
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

static int64_t alignTimeUp(int64_t ts, int unit, int count)
{
	switch (unit) {
	case FT_SECOND:
		ts = CEIL(ts, count * 1000);
		break;
	case FT_MINUTE:
		ts = CEIL(ts, count * 60 * 1000);
		break;
	case FT_HOUR:
		ts = CEIL(ts, count * 3600 * 1000);
		break;
	default:
		// TODO: more FT_XXX
		break;
	}

	return ts;
}

static int64_t alignTimeDown(int64_t ts, int unit, int count)
{
	switch (unit) {
	case FT_SECOND:
		ts = FLOOR(ts, count * 1000);
		break;
	case FT_MINUTE:
		ts = FLOOR(ts, count * 60 * 1000);
		break;
	case FT_HOUR:
		ts = FLOOR(ts, count * 3600 * 1000);
	default:
		// TODO: more FT_XXX
		break;
	}

	return ts;
}

static void outputError(int code, const char *fmt)
{
	printf("Status: %d %s\r\n", code, fmt);
	printf("Content-Type: application/json\r\n");
	printf("\r\n");
	printf("{\"code\":%d,\"msg\":\"%s\"}", code, fmt);
}

static int parseDtimeSpan(QueryParameters& parameters, int64_t& startDtime, int64_t& endDtime, int& spanUnit, int& spanCount)
{
	int lastUnit, lastCount = 0;
	int alignUnit, alignCount, width;
	char *eptr;

	const char *strLast = parameters.getString("last", NULL);
	if (strLast != NULL) {
		endDtime = time(NULL)*1000;
		lastCount = strtol(strLast, &eptr, 0);
		if ((lastUnit = timeUnit(eptr)) == FT_UNKNOWN) {
			outputError(501, "invalid time unit for last parameter");
			return -1;
		}
	}
	else {
		const char *strStart = parameters.getString("start", NULL);
		const char *strEnd = parameters.getString("end", NULL);
		if (strStart == NULL || strEnd == NULL) {
			outputError(501, "No last is provided, neither start/end. one of them must be provifded.");
			return -1;
		}

		startDtime = parseDtime(strStart);
		endDtime = parseDtime(strEnd);
	}

	const char *strSpan = parameters.getString("span", "auto");
	if (!strcmp(strSpan, "auto")) {
		spanCount = 0;
	}
	else {
		 spanCount = strtol(strSpan, &eptr, 0);
		if ((spanUnit = timeUnit(eptr)) == FT_UNKNOWN) {
			outputError(501, "Invalid time unit for span");
			return -1;
		}
	}

	const char *strAlign = parameters.getString("align", "auto");
	if (!strcmp(strAlign, "auto")) {
		alignCount = 0;
	}
	else {
		alignCount = strtol(strAlign, &eptr, 0);
		if ((alignUnit = timeUnit(eptr)) == FT_UNKNOWN) {
			outputError(502, "Invalid time unit for align");
			return -1;
		}
	}

	if ((width = parameters.getInt("width", -1)) == -1) {
		outputError(503, "No width parameter is set");
		return -1;
	}

	if (lastCount > 0) {
		endDtime = time(NULL) * 1000;
		if (alignCount != 0) {
			endDtime = alignTimeUp(endDtime, alignUnit, alignCount);
		}

		switch (lastUnit) {
		case FT_SECOND:
			startDtime = endDtime - lastCount * 1000;
			break;
		case FT_MINUTE:
			startDtime = endDtime - lastCount * 1000 * 60;
			break; 
		case FT_HOUR:
			startDtime = endDtime - lastCount * 1000 * 60 * 60;
			break;
		default:
			outputError(501, "TODO: support more lastUnit");
			return -1;
		}
	}
	else {
		if (alignCount > 0) {
			endDtime = alignTimeUp(endDtime, alignUnit, alignCount);
			startDtime = alignTimeDown(startDtime, alignUnit, alignCount);
		}
	}

	if (spanCount < 1) {
		// automatically determine span
		// at most, one pixel, one point
		int64_t msPerPx = (endDtime - startDtime) / width;
// larger than, unit, count
// { 60 * 60 * 1000, FT_HOUR, -1 },
// { M(30), FT_HOUR, 1 },
// { M(15), FT_MINUTE, 30},
		if (msPerPx > 3600 * 1000) {
			spanUnit = FT_HOUR;
			spanCount = msPerPx / (3600*1000);
		}
		else if (msPerPx > 30 * 60 * 1000) {
			spanUnit = FT_HOUR;
			spanCount = 1;
		}
		else if (msPerPx > 15 * 60 * 1000) {
			spanUnit = FT_MINUTE;
			spanCount = 30;
		}
		else if (msPerPx > 5 * 60 * 1000) {
			spanUnit = FT_MINUTE;
			spanCount = 15;
		}
		else if (msPerPx > 1 * 60 * 1000) {
			spanUnit = FT_MINUTE;
			spanCount = 5;
		}
		else {
			spanUnit = FT_MINUTE;
			spanCount = 1;
		}
				
/*
		if (msPerPx > 3M) {
			spanUnit = FT_YEAR;
			spanCount = UPPER(periodInterval/1Y);
		}
		else if (msPerPx > 1M) {
			spanUnit = FT_QUARTER;
		}
		else if (msPerPx > 7d) {
			spanUnit = FT_MONTH;
		}
		else if (msPerPx > 1d) {
			spanUnit = FT_WEEK;
		}
		> 12h => 1d
		> 8h => 12h
		> 6h => 8h
		> 4h => 6h
		> 3h => 4h
		> 2h => 3h
		> 1h => 2h
		> 30m => 1h
		> 20m => 30m
		> 15m => 20m
		> 10m => 15m
		> 5m => 10m
		> 1m => 5m	
		> 30s => 1m
		> 20s => 30s
		> 15s
		> 10s
		> 5s
		> 1s
		else if (msPerPx > 1H) {
			spanUnit = FT_DAY;
		}
		else if (msPerPx > 1M) {
			spanUnit = FT_HOUR;
		}
		else if (msPerPx > 1s) {
			spanUnit = FT_MINUTE;
		}
		else {
			spanUnit = FT_SECOND;
		}
*/
	}

	// align start/end time with span unit too
	endDtime = alignTimeUp(endDtime, spanUnit, spanCount);
	startDtime = alignTimeDown(startDtime, spanUnit, spanCount);

	return 0;
}

static void outputCpuCombinedGauges(int gtype, const merged_gauge_map_t& prev, const merged_gauge_map_t& gauges,
				    bool& first, std::tr1::unordered_set<int> cpuIds)
{
	for (std::tr1::unordered_set<int>::const_iterator idIter = cpuIds.begin(); idIter != cpuIds.end(); ++idIter) {
		local_key_set_t keys;

		for (const_gauge_iterator iter = gauges.begin(); iter != gauges.end(); ++iter) {
			if (IID_IS4CPU(iter->first.sid.iid) && IID2CPUNO(iter->first.sid.iid) == *idIter) {
				local_key_t key = iter->first;
				key.sid.iid = 0;

				keys.insert(key);
			}
		}

		for (local_key_set_t::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
			if (first) first = false; else printf(",");
			if (gtype == GT_PRODUCT) {
				// {pid,mid=0,usr|sys|idl|wt,host=0}
				printf("{\"gtype\":\"P\",\"pid\":%d,\"type\":\"cpu\"", iter->sid.pid);
			}
			else if (gtype == GT_MODULE) {
				// {pid,mid,usr|sys|idl|wt,host=0}
				printf("{\"gtype\":\"M\",\"pid\":%d,\"mid\":%d", iter->sid.pid, iter->sid.mid);
			}
			else if (gtype == GT_HOST) {
				// {pid=0,mid=0,usr|sys|idl|wt,host}
				// TODO: host-name, and support ip6
				char buf[128];
				if (iter->hip.ver == 4) inet_ntop(AF_INET, &iter->hip.ip.ip4, buf, sizeof buf);
				else if (iter->hip.ver == 6) inet_ntop(AF_INET6, &iter->hip.ip.ip6[0], buf, sizeof buf);
				printf("{\"gtype\":\"H\",\"ip\":\"%s\",\"host\":\"%s\"", buf, buf);
			}

			if (*idIter == IID_CPU_TOTAL) printf(",\"name\":\"cpu\"");
			else printf(",\"name\":\"cpu-%d\"", *idIter);
		
			local_key_t key = *iter; 
			key.sid.iid = IID_CPU(*idIter, CPU_USR);
			int64_t usr = -1, sys = -1, idl = -1, wt = -1;
			const_gauge_iterator iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				usr = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_CPU(*idIter, CPU_SYS);
			iter1 = prev.find(key); iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				sys = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_CPU(*idIter, CPU_IDL);
			iter1 = prev.find(key); iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				idl = iter2->second.gval - iter1->second.gval;
			
			key.sid.iid = IID_CPU(*idIter, CPU_WT);
			iter1 = prev.find(key); iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				wt = iter2->second.gval - iter1->second.gval;

			if (usr == -1 || sys == -1 || idl == -1 || wt == -1) {
				printf(",\"values\":{}}");
			}
			else {
				// TODO: change to percent
				//int total = usr + sys + idl + wt;
				//int all = spanLength(spanUnit, spanCount) / 1000 * 100;
				//if (all % 100
				
				printf(",\"values\":{\"usr\":%ld,\"sys\":%ld,\"idl\":%ld,\"wt\":%ld}}", usr,sys,idl,wt);
			}
		}
	}
}

void outputMemCombinedGauges(int gtype, const merged_gauge_map_t& prev, const merged_gauge_map_t& gauges, bool& first)
{
	local_key_set_t keys;
	for (const_gauge_iterator iter = gauges.begin(); iter != gauges.end(); ++iter) {
		if (IID_IS4MEM(iter->first.sid.iid)) {
			local_key_t key = iter->first;
			key.sid.iid = 0;

			keys.insert(key);
		}
	}

	for (local_key_set_t::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
		if (first) first = false; else printf(",");
		if (gtype == GT_PRODUCT) {
			// {pid,mid=0,usr|sys|idl|wt,host=0}
			printf("{\"gtype\":\"P\",\"pid\":%d,\"type\":\"mem\"", iter->sid.pid);
		}
		else if (gtype == GT_MODULE) {
			// {pid,mid,usr|sys|idl|wt,host=0}
			printf("{\"gtype\":\"M\",\"pid\":%d,\"mid\":%d,\"type\":\"mem\"", iter->sid.pid, iter->sid.mid);
		}
		else if (gtype == GT_HOST) {
			// {pid=0,mid=0,usr|sys|idl|wt,host}
			// TODO: host-name, ip6
			char buf[128];
			if (iter->hip.ver == 4) inet_ntop(AF_INET, &iter->hip.ip.ip4, buf, sizeof buf);
			else if (iter->hip.ver == 6) inet_ntop(AF_INET6, &iter->hip.ip.ip6[0], buf, sizeof buf);
			printf("{\"gtype\":\"H\",\"ip\":\"%s\",\"host\":\"%s\",\"type\":\"mem\"", buf, buf);
		}

		local_key_t key = *iter;
		const_gauge_iterator iter2;
		int64_t free = 0, cached = 0, buffers = 0, used = 0;
		key.sid.iid = IID_MEM_USED; if ((iter2 = gauges.find(key)) != gauges.end()) used = iter2->second.gval;
		key.sid.iid = IID_MEM_FREE; if ((iter2 = gauges.find(key)) != gauges.end()) free = iter2->second.gval;
		key.sid.iid = IID_MEM_CACHED; if ((iter2 = gauges.find(key)) != gauges.end()) cached = iter2->second.gval;
		key.sid.iid = IID_MEM_BUFFERS; if ((iter2 = gauges.find(key)) != gauges.end()) buffers = iter2->second.gval;
		printf(",\"values\":{\"used\":%ld,\"free\":%ld,\"cached\":%ld,\"buffers\":%ld}}", used, free, cached, buffers);
	}

	return;
}

void outputLoadavgCombinedGauges(int gtype, const merged_gauge_map_t& prev, const merged_gauge_map_t& gauges, bool& first)
{
	local_key_set_t keys;
	for (const_gauge_iterator iter = gauges.begin(); iter != gauges.end(); ++iter) {
		if (IID_IS4LOADAVG(iter->first.sid.iid)) {
			local_key_t key = iter->first;
			key.sid.iid = 0;

			keys.insert(key);
		}
	}

	for (local_key_set_t::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
		if (first) first = false; else printf(",");
		if (gtype == GT_PRODUCT) {
			// {pid,mid=0,usr|sys|idl|wt,host=0}
			printf("{\"gtype\":\"P\",\"pid\":%d,\"type\":\"load-avg\"", iter->sid.pid);
		}
		else if (gtype == GT_MODULE) {
			// {pid,mid,usr|sys|idl|wt,host=0}
			printf("{\"gtype\":\"M\",\"pid\":%d,\"mid\":%d,\"type\":\"load-avg\"", iter->sid.pid, iter->sid.mid);
		}
		else if (gtype == GT_HOST) {
			// {pid=0,mid=0,usr|sys|idl|wt,host}
			// TODO: host-name, ip6
			char buf[128];
			if (iter->hip.ver == 4) inet_ntop(AF_INET, &iter->hip.ip.ip4, buf, sizeof buf);
			else if (iter->hip.ver == 6) inet_ntop(AF_INET6, &iter->hip.ip.ip6[0], buf, sizeof buf);
			printf("{\"gtype\":\"H\",\"ip\":\"%s\",\"host\":\"%s\",\"type\":\"load-avg\"", buf, buf);
		}

		local_key_t key = *iter; 
		const_gauge_iterator iter2;
		int64_t m1 = 0, m5 = 0, m15 = 0;
		key.sid.iid = IID_LOADAVG_1; if ((iter2 = gauges.find(key)) != gauges.end()) m1 = iter2->second.gval;
		key.sid.iid = IID_LOADAVG_5; if ((iter2 = gauges.find(key)) != gauges.end()) m5 = iter2->second.gval;
		key.sid.iid = IID_LOADAVG_15; if ((iter2 = gauges.find(key)) != gauges.end()) m15 = iter2->second.gval;
		printf(",\"values\":{\"1m\":%ld,\"5m\":%ld,\"15m\":%ld}}", m1, m5, m15);
	}

	return;
}

void outputNetCombinedGauges(int gtype, const merged_gauge_map_t& prev, const merged_gauge_map_t& gauges,
			     bool& first, const std::tr1::unordered_set<int>& netIds)
{
	for (std::tr1::unordered_set<int>::const_iterator idIter = netIds.begin(); idIter != netIds.end(); ++idIter) {
		local_key_set_t keys;

		for (const_gauge_iterator iter = gauges.begin(); iter != gauges.end(); ++iter) {
			if (IID_IS4NET(iter->first.sid.iid) && IID2NETNO(iter->first.sid.iid) == *idIter) {
				local_key_t key = iter->first;
				key.sid.iid = 0;
				
				keys.insert(key);
			}
		}

		
		for (local_key_set_t::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
			if (first) first = false; else printf(",");
			if (gtype == GT_PRODUCT) {
				// {pid,mid=0,usr|sys|idl|wt,host=0}
				printf("{\"gtype\":\"P\",\"pid\":%d,\"type\":\"net\"", iter->sid.pid);
			}
			else if (gtype == GT_MODULE) {
				// {pid,mid,usr|sys|idl|wt,host=0}
				printf("{\"gtype\":\"M\",\"pid\":%d,\"mid\":%d", iter->sid.pid, iter->sid.mid);
			}
			else if (gtype == GT_HOST) {
				// {pid=0,mid=0,usr|sys|idl|wt,host}
				// TODO: host-name, ip6
				char buf[128];
				if (iter->hip.ver == 4) inet_ntop(AF_INET, &iter->hip.ip.ip4, buf, sizeof buf);
				else if (iter->hip.ver == 6) inet_ntop(AF_INET6, &iter->hip.ip.ip6[0], buf, sizeof buf);
				printf("{\"gtype\":\"H\",\"ip\":\"%s\",\"host\":\"%s\"", buf, buf);
			}

			// TODO: get its name
			printf(",\"name\":\"net-%d\"", *idIter);
		
			int64_t inBytes = 0, inPkts = 0, outBytes = 0, outPkts = 0;
			local_key_t key = *iter; 

			key.sid.iid = IID_NET(*idIter, NET_T_IN_BYTES);
			const_gauge_iterator iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				inBytes = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_NET(*idIter, NET_T_IN_PKTS);
			iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				inPkts = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_NET(*idIter, NET_T_OUT_BYTES);
			iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				outBytes = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_NET(*idIter, NET_T_OUT_PKTS);
			iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				outPkts = iter2->second.gval - iter1->second.gval;

			printf(",\"values\":{\"ib\":%ld,\"ip\":%ld,\"ob\":%ld,\"op\":%ld}}", inBytes,inPkts,outBytes,outPkts);
		}
	}

	return;
}

void outputDiskCombinedGauges(int gtype, const merged_gauge_map_t& prev, const merged_gauge_map_t& gauges,
			      bool& first, const std::tr1::unordered_set<int>& diskIds)
{
	for (std::tr1::unordered_set<int>::const_iterator idIter = diskIds.begin(); idIter != diskIds.end(); ++idIter) {
		local_key_set_t keys;

		for (const_gauge_iterator iter = gauges.begin(); iter != gauges.end(); ++iter) {
			if (IID_IS4DISK(iter->first.sid.iid) && IID2DISKNO(iter->first.sid.iid)== *idIter) {
				local_key_t key = iter->first;
				key.sid.iid = 0;

				keys.insert(key);
			}
		}

		
		for (local_key_set_t::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
			if (first) first = false; else printf(",");
			if (gtype == GT_PRODUCT) {
				// {pid,mid=0,usr|sys|idl|wt,host=0}
				printf("{\"gtype\":\"P\",\"pid\":%d,\"type\":\"disk\"", iter->sid.pid);
			}
			else if (gtype == GT_MODULE) {
				// {pid,mid,usr|sys|idl|wt,host=0}
				printf("{\"gtype\":\"M\",\"pid\":%d,\"mid\":%d,\"type\":\"disk\"", iter->sid.pid, iter->sid.mid);
			}
			else if (gtype == GT_HOST) {
				// {pid=0,mid=0,usr|sys|idl|wt,host}
				// TODO: host-name, ip6
				char buf[128];
				if (iter->hip.ver == 4) inet_ntop(AF_INET, &iter->hip.ip.ip4, buf, sizeof buf);
				else if (iter->hip.ver == 6) inet_ntop(AF_INET6, &iter->hip.ip.ip6[0], buf, sizeof buf);
				printf("{\"gtype\":\"H\",\"ip\":\"%s\",\"host\":\"%s\",\"type\":\"disk\"", buf, buf);
			}

			printf(",\"name\":\"disk-%d\"", *idIter);
		
			int64_t rCalls = 0, rBytes = 0, wCalls = 0, wBytes = 0;
			local_key_t key = *iter; 

			key.sid.iid = IID_DISK(*idIter, DISK_T_R_CALLS);
			const_gauge_iterator iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				rCalls = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_DISK(*idIter, DISK_T_R_BYTES);
			iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				rBytes = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_DISK(*idIter, DISK_T_W_CALLS);
			iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				wCalls = iter2->second.gval - iter1->second.gval;

			key.sid.iid = IID_NET(*idIter, DISK_T_W_BYTES);
			iter1 = prev.find(key), iter2 = gauges.find(key);
			if (iter1 != prev.end() && iter2 != gauges.end())
				wBytes = iter2->second.gval - iter1->second.gval;

			printf(",\"values\":{\"r-calls\":%ld,\"r-bytes\":%ld,\"w-calls\":%ld,\"w-bytes\":%ld}}", rCalls, rBytes, wCalls, wBytes);
		}
	}

	return;
}

//
// case 0: depart-level
//	dep-id => [pid,...] => [{pid,*,*}, ...]
//	host=[auto]
// case 1: pid-level
//	pid, *, *
//	host=[auto]
// case 2: mid-level
//	pid, mid, *
//	host=[auto]
// case 3: host-level
//	pid, mid, *
//	host=ip
// case 4: expand-to-individual-hosts
//	pid,mid,iid as in [0-2] (no case #3)
//	host=[auto]
//
static void handleRequest(QueryParameters& parameters)
{
	// step 1: context
	const char *strContext = parameters.getString("context", "resource");
	int context;
	if (!strcmp(strContext, "business")) {
		context = CT_BUSINESS;
	}
	else if (!strcmp(strContext, "resource")) {
		context = CT_RESOURCE;
	}
	else {
		outputError(501, "Invalid context parameter");
		return;
	}

	// step 2: group - how to combine stats together
	int totalView = 0;
	const char *strGroup = parameters.getString("group", "total");
	if (!strcmp(strGroup, "total")) {
		totalView = 1;
	}
	else if (!strcmp(strGroup, "list")) {
		totalView = 0;
	}
	else {
		outputError(501, "invalid group parameter, which should be total|list.");
		return;
	}
	
	// step 3: time period, span, align
	int64_t startDtime, endDtime;
	int spanUnit, spanCount;
	if (parseDtimeSpan(parameters, startDtime, endDtime, spanUnit, spanCount) < 0)
		return;

	// move ahead one span for some calculation need its previous stats
	startDtime -= spanLength(spanUnit, spanCount);
	int mergeCount = (endDtime - startDtime) / spanLength(spanUnit, spanCount);

//	char buf1[128], buf2[128];
//	APPLOG_DEBUG("parsed start=%s, end=%s, mergeCount=%d", 
//		formatDtime(buf1, sizeof buf1, startDtime),
//		formatDtime(buf2, sizeof buf2, endDtime), mergeCount);

	StatMerger merger(NULL, NULL, NULL, NULL, spanUnit, spanCount, mergeCount);
	merger.periodStartTime = startDtime;
	
	// step 4: ids
// TODO: group by department...
//	uint16_t did = parameters.getInt("did", 0);
	uint16_t pid = parameters.getInt("pid", 0);
	uint16_t mid = parameters.getInt("mid", 0);

	if (pid == 0) {
		outputError(501, "pid can not be 0(ANY) now");
		return;
	}

	int cpuTotal = 0, cpuCores = 0; std::tr1::unordered_set<int> cpuIds;
	int memory = 0;
	int loadAvg = 0;
	int netAll = 0; std::tr1::unordered_set<int> netIds;
	int diskAll = 0; std::tr1::unordered_set<int> diskIds;

	// step 4.1: parse iids
	const char *strIid = parameters.getString("iid", "all");
	if (strcmp(strIid, "all") == 0) {
		cpuTotal = 1;	// no cpu-cores
		memory = 1;
		loadAvg = 1;
		netAll = 1;
		diskAll = 1;
	}
	else {
		char ss[1024]; 
		strncpy(ss, strIid, sizeof ss); ss[sizeof(ss) - 1] = 0;

		char *endptr, *nptr = strtok_r(ss, MULTIVAL_SEPARATORS, &endptr);
		while (nptr != NULL) {
			if (!strcmp(nptr, "cpu-total")) cpuTotal = 1;
			else if (!strcmp(nptr, "cpu-cores")) cpuCores = 1;
			else if (!strncmp(nptr, "cpu-", 4)) cpuIds.insert(strtol(nptr + 4, NULL, 0));
			else if (!strcmp(nptr, "mem")) memory = 1;
			else if (!strcmp(nptr, "load-avg")) loadAvg = 1;
			else if (!strcmp(nptr, "net-all")) netAll = 1;
			// TODO: mapping net-name to its id
			else if (!strncmp(nptr, "net-", 4)) netIds.insert(strtol(nptr + 4, NULL, 0));
			else if (!strcmp(nptr, "disk-all")) diskAll = 1;
			// TODO: mapping disk-name to its id
			else if (!strncmp(nptr, "disk-", 5)) diskIds.insert(strtol(nptr + 5, NULL, 0));
			else {
				outputError(501, "invalid iid parameter");
				return;
			}

			nptr = strtok_r(NULL, MULTIVAL_SEPARATORS, &endptr);
		}
	}

	// step 4.2: get all possible iids first
	local_key_set_t ids;
	host_set_t hosts;

	// step 4.3: get hosts and mapping iids with hosts
	const char *strHost = parameters.getString("host", "auto");
	if (strcmp(strHost, "auto")) {
		// individual host(s)
		char ss[1024];
		strncpy(ss, strHost, sizeof ss); ss[sizeof(ss) - 1] = 0;

		char *endptr, *nptr = strtok_r(ss, MULTIVAL_SEPARATORS, &endptr);
		while (nptr != NULL) {
			stat_ip_t hip;
			if (inet_pton(AF_INET, nptr, &hip.ip.ip4) == 1) {
				hip.ver = 4; 
			}
			else if (inet_pton(AF_INET6, nptr, &hip.ip.ip6[0]) == 1) {
				hip.ver = 6;
			}
			else {
				outputError(501, "invalid host parameter");
				return;
			}

			hosts.insert(hip);
			nptr = strtok_r(NULL, MULTIVAL_SEPARATORS, &endptr);
		}
	}

	unsigned char buf[8192], rspBuf[8192];
	Memorybuffer msg(buf, sizeof buf, false);
	MemoryBuffer rsp(rspBuf, sizeof rspBuf, false);

	struct proto_h16_head *h = (struct proto_h16_head *)msg.data();
	memset(h, sizeof(*h), 0);
	msg.setWptr(sizeof(*h));

	h->cmd = CMD_STAT_GET_SYSTEM_STATS_REQ;
	h->syn = nextSyn++;
	h->ack = 0;
	h->ver = 1;
	
	msg.writeUint8(context);
	msg.writeUint8(totalView);
	msg.writeInt64(startDtime);
	msg.writeInt64(endDtime);
	msg.writeUint8(spanUnit);
	msg.writeUint8(spanCount);
	msg.writeUint16(pid);
	msg.writeUint16(mid);

	msg.writeUint16(hosts.size());
	for (hosts::iterator iter = hosts.begin(); iter != hosts.end(); ++iter) {
		if (encodeTo(msg, *iter) < 0)
			break;
	}
	
	beyondy::TimedoutCountdown timer(10*1000);
	ClientConnection client(storageAddress, 10*1000, 3);
	if (client.request(&msg, &rsp) < 0) {
		APPLOG_ERROR("request to %s failed: %m", storageAddress);
		return -1;
	}

	struct proto_h16_res *h2 = (struct proto_h16_res *)rsp.data();
	rsp.setRptr(sizeof(*h2));

	if (combiner.parseFrom(&rsp) < 0) {
		APPLOG_ERROR("parse combiner from rsp-msg failed");
		return -1;
	}

	// further merge
	StatCombiner combiner(spanUnit, spanCount, startDtime, mergeCount);
	int gtype = combiner.groupType();

	// output
	printf("Status: 200 OK\r\n");
	printf("Content-Type: application/json\r\n");
	printf("\r\n");

	int64_t spanInterval = spanLength(spanUnit, spanCount);
	int64_t ts = startDtime + spanInterval;
	char buf[128];
	printf("{\"start\":\"%s\"", formatDtime(buf, sizeof buf, ts));
	printf(",\"end\":\"%s\"", formatDtime(buf, sizeof buf, endDtime));
	printf(",\"span\":\"%s\"", formatSpan(buf, sizeof buf, spanUnit, spanCount));
	printf(",\"stats\":[");
	for (int i = 1; i < mergeCount; ++i) {
		
		printf("%s{\"dtime\":\"%s\"", i == 1 ? "" : ",", formatDtime(buf, sizeof buf, ts));
		printf(",\"data\":[");

		bool first = true;
		if (!cpuIds.empty()) {
			outputCpuCombinedGauges(gtype, combiner.mergedGauges[i-1], combiner.mergedGauges[i], first, cpuIds);
		}

		if (memory) {
			outputMemCombinedGauges(gtype, combiner.mergedGauges[i-1], combiner.mergedGauges[i], first);
		}

		if (loadAvg) {
			outputLoadavgCombinedGauges(gtype, combiner.mergedGauges[i-1], combiner.mergedGauges[i], first);
		}

		if (!netIds.empty()) {
			outputNetCombinedGauges(gtype, combiner.mergedGauges[i-1], combiner.mergedGauges[i], first, netIds);
		}

		if (!diskIds.empty()) {
			outputDiskCombinedGauges(gtype, combiner.mergedGauges[i-1], combiner.mergedGauges[i], first, diskIds);
		}

		printf("]}");

		ts += spanInterval;
	}

	printf("]}");
	return;
}

int main(int argc, char **argv)
{
	QueryParameters parameters(argc == 2 ? argv[1] : getenv("QUERY_STRING"));
	handleRequest(parameters);

	return 0;
}

