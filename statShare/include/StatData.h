/* StatData.h
 * Copyright@ Beyondy.c.w 2002-2020
**/
#ifndef __STAT_DATA__H
#define __STAT_DATA__H
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <netinet/in.h>
#include <tr1/unordered_map>
#include <assert.h>

/*
 * just for access log
**/
#define STAT_KEY_MAX		128
#define STAT_EXTRA_MAX		256

/*
 * Gauge types which may affect how to display it and
 * merge different gauges together
**/
#define SGT_SNAPSHOT		0
#define SGT_DELTA		1

/*
 * frequency types
 * like 5m, ftype = FT_MINUTE and freqs=5
**/
#define FT_SECOND		0
#define FT_MINUTE		1
#define FT_HOUR			2
#define FT_DAY			3
#define FT_MONTH		4
#define FT_YEAR			5
#define FT_UNKNOWN		99

/*
 * STAT_ types
**/
#define STAT_ITEM_GAUGE		0
#define STAT_ITEM_LCALL		1
#define STAT_ITEM_RCALL		2

#define STAT_MERGED_GAUGE	3
#define STAT_MERGED_LCALL	4
#define STAT_MERGED_RCALL	5

#include "MemoryBuffer.h"

typedef struct stat_ip_tag {
	uint8_t ver;

	// use strut in_addr, in6_addr?
	union { uint32_t ip4; uint32_t ip6[4]; } ip;

	stat_ip_tag() {}
	stat_ip_tag(uint32_t _ip4)
		: ver(4) { this->ip.ip4 = _ip4; }
} stat_ip_t;

static inline bool operator==(const stat_ip_t& x, const stat_ip_t& y)
{
	if (x.ver != y.ver) return false;
	if (x.ver == 4) return x.ip.ip4 == y.ip.ip4;
	// TODO: support ip6
	return false;
}

struct HipHash {
	size_t operator()(const stat_ip_t& hip) const {
		size_t result = 2166136261UL;
		if (hip.ver == 4) {
			result ^= hip.ip.ip4;
			result *= 16777619UL;
		}
		else if (hip.ver == 6) {
			// TODO:
		}
		else {
			assert("IP-ver invalid" == NULL);
		}

		return result;
	}
};

typedef struct stat_id_tag {
	uint16_t pid;
	uint16_t mid;
	uint16_t iid;

	stat_id_tag() {}
	stat_id_tag(uint16_t _pid, uint16_t _mid, uint16_t _iid) 
		: pid(_pid), mid(_mid), iid(_iid) {}
} stat_id_t;

static inline bool operator==(const stat_id_t& x, const stat_id_t& y)
{
	return x.pid == y.pid && x.mid == y.mid && x.iid == y.iid;
}

typedef struct local_key_tag {
	stat_ip_t hip;
	stat_id_t sid;

	local_key_tag() : hip(), sid() {}
	local_key_tag(const stat_ip_t& _hip, const stat_id_t& _sid)
		: hip(_hip), sid(_sid) {}
} local_key_t;

static inline bool operator==(const local_key_t& x, const local_key_t& y)
{
	return x.hip == y.hip && x.sid == y.sid;
}

struct LocalKeyHash {
	size_t operator()(const local_key_t& key) const {
		size_t result = 2166136261UL;
		if (key.hip.ver == 4) {
			result ^= key.hip.ip.ip4;
			result *= 16777619UL;
		}
		else if (key.hip.ver == 6) {
			// TODO:
		}
		else {
			assert("IP-ver invalid" == NULL);
		}

		result ^= ((size_t)key.sid.pid << 16) | key.sid.mid;
		result *= 16777619UL;

		result ^= key.sid.iid;
		result *= 16777619UL;
		
		return result;
	}
};

typedef struct rcall_key_tag {
	stat_ip_t src_hip;
	stat_id_t src_sid;
	stat_ip_t dst_hip;
	stat_id_t dst_sid;

	rcall_key_tag() {}
	rcall_key_tag(const stat_ip_t& _sip, const stat_id_t& _sid, const stat_ip_t& _dip, const stat_id_t& _did)
		: src_hip(_sip), src_sid(_sid), dst_hip(_dip), dst_sid(_did) {};
} rcall_key_t;

static inline bool operator==(const rcall_key_t& x, const rcall_key_t& y)
{
	return x.src_hip == y.src_hip && x.src_sid == y.src_sid
			&& x.dst_hip == y.dst_hip && x.dst_sid == y.dst_sid;
}

struct RcallKeyHash {
	size_t operator()(const rcall_key_t& key) const {
		size_t result = 2166136261UL;
		//
		// src part
		//
		if (key.src_hip.ver == 4) {
			result ^= key.src_hip.ip.ip4;
			result *= 16777619UL;
		}
		else if (key.src_hip.ver == 6) {
			// TODO:
		}
		else {
			assert("IP-ver invalid" == NULL);
		}

		result ^= ((size_t)key.src_sid.pid << 16) | key.src_sid.mid;
		result *= 16777619UL;

		result ^= key.src_sid.iid;
		result *= 16777619UL;

		//
		// + dst
		//
		if (key.dst_hip.ver == 4) {
			result ^= key.dst_hip.ip.ip4;
			result *= 16777619UL;
		}
		else if (key.dst_hip.ver == 6) {
			// TODO:
		}
		else {
			assert("IP-ver invalid" == NULL);
		}

		result ^= ((size_t)key.dst_sid.pid << 16) | key.dst_sid.mid;
		result *= 16777619UL;

		result ^= key.dst_sid.iid;
		result *= 16777619UL;

		return result;
	}
};

// single result, excluding retcode
// include retcode?
typedef struct stat_result_tag {
	uint32_t rsptime;
	uint32_t isize;
	uint32_t osize;

	stat_result_tag() {}
	stat_result_tag(uint32_t _rsptime, uint32_t _isize, uint32_t _osize)
		: rsptime(_rsptime), isize(_isize), osize(_osize) {}
} stat_result_t;

// merged result, excluding retcode
// usually retcode is the key of mapping
typedef struct stat_mresult_tag {
	uint32_t count;
	uint32_t rsptime;	/* avg: usec */
	uint32_t isize;		/* avg bytes */
	uint32_t osize;		/* avg bytes */

	stat_mresult_tag() {}
	stat_mresult_tag(uint32_t _count, uint32_t _rsptime, uint32_t _isize, uint32_t _osize)
		: count(_count), rsptime(_rsptime), isize(_isize), osize(_osize) {}
} stat_mresult_t;

// result => mresult
#define MRESULT_FIRST(mr,r) do { (mr).count=1;(mr).rsptime=(r).rsptime;(mr).isize=(r).isize;(mr).osize=(r).osize; } while(0)

// mresult += result, and re-avg it
#define __AVG1(mr,r, fld) do { (mr).fld = (uint32_t) (((uint64_t)(mr).fld * (mr).count + (r).fld) / ((mr).count + 1)); } while(0)
#define MRESULT_AVG(mr,r) do { __AVG1(mr,r,rsptime); __AVG1(mr,r,isize); __AVG1(mr,r,osize); ++(mr).count; } while(0)

// mresult += mresult, and re-avg it
#define __AVG2(mr,r,fld) do { (mr).fld = (uint32_t)(((uint64_t)(mr).fld * (mr).count + (uint64_t)(r).fld * (r).count) / ((mr).count + (r).count)); } while(0)
#define MRESULT_MERGE(mr,r) do { __AVG2(mr,r,rsptime); __AVG2(mr,r,isize); __AVG2(mr,r,osize); (mr).count += (r).count; } while(0)

class StatItemGauge {
public:
	StatItemGauge() { /* nothing */ }
	StatItemGauge(int64_t _timestamp, stat_ip_t _hip, const stat_id_t& _sid, uint8_t _gtype, int64_t _gval)
		: timestamp(_timestamp), hip(_hip), sid(_sid), gtype(_gtype), gval(_gval) {}
public:
	int parseFrom(MemoryBuffer *msg);
	int encodeTo(MemoryBuffer *msg) const;
public:
	int64_t timestamp;
	stat_ip_t hip;	/* host IP(v4 or v6) */
	stat_id_t sid;

	uint8_t gtype;	/* SGT_ */
	int64_t gval;
};

class StatMergedGauge {
public:
	StatMergedGauge() { /* nothing */ }
	StatMergedGauge(int64_t _timestamp, const stat_ip_t& _hip, const stat_id_t& _sid, uint8_t _ftype, uint8_t _freqs, uint8_t _gtype, int64_t _gval)
		: timestamp(_timestamp), hip(_hip), sid(_sid), ftype(_ftype), freqs(_freqs), gtype(_gtype), gval(_gval) {}
public:
	int parseFrom(MemoryBuffer *msg);
	int encodeTo(MemoryBuffer *msg) const;
public:
	int64_t timestamp;
	stat_ip_t hip;	/* host IP(v4 or v6) */
	stat_id_t sid;
	
	uint8_t ftype;
	uint8_t freqs;

	uint8_t gtype;	/* SGT_ */
	int64_t gval;
};

class StatItemLcall {
public:
	StatItemLcall() { key[0] = extra[0] = 0; }
	StatItemLcall(int64_t _timestamp, const stat_ip_t& _hip, const stat_id_t& _sid,
		      int32_t _retcode, const stat_result_t& _result, 
		      const char *_key, const char *_extra)
		: timestamp(_timestamp), hip(_hip), sid(_sid),
		  retcode(_retcode), result(_result) {
		if (_key != NULL) { strncpy(key, _key, sizeof key); key[sizeof key - 1] = 0; } else key[0] = 0;
		if (_extra != NULL) { strncpy(extra, _extra, sizeof extra); extra[sizeof extra - 1] = 0; } else extra[0] = 0;
	}
public:
	int parseFrom(MemoryBuffer *msg);
	int encodeTo(MemoryBuffer *msg) const;
public:
	int64_t timestamp;
	stat_ip_t hip;
	stat_id_t sid;
	
	int32_t retcode;
	stat_result_t result;

	char key[STAT_KEY_MAX];
	char extra[STAT_EXTRA_MAX];
};

class StatMergedLcall {
public:
	StatMergedLcall() {}
public:
	int parseFrom(MemoryBuffer *msg);
	int encodeTo(MemoryBuffer *msg) const;
public:
	int64_t timestamp;
	stat_ip_t hip;
	stat_id_t sid;
	
	uint8_t ftype;
	uint8_t freqs;

	typedef std::tr1::unordered_map<int32_t, stat_mresult_t> mresult_map_t;
	typedef mresult_map_t::iterator iterator;
	typedef mresult_map_t::const_iterator const_iterator;

	mresult_map_t rets;
};

class StatItemRcall {
public:
	StatItemRcall() { key[0] = extra[0] = 0; }
	StatItemRcall(int64_t _timestamp, const stat_ip_t& _src_hip, const stat_id_t& _src_sid,
		      const stat_ip_t& _dst_hip, const stat_id_t& _dst_sid, int32_t _retcode,
		      const stat_result_t& _result, const char *_key, const char *_extra)
		: timestamp(_timestamp), src_hip(_src_hip), src_sid(_src_sid),
		  dst_hip(_dst_hip), dst_sid(_dst_sid), retcode(_retcode), result(_result) {
		if (_key != NULL) { strncpy(key, _key, sizeof key); key[sizeof key - 1] = 0; } else key[0] = 0;
		if (_extra != NULL) { strncpy(extra, _extra, sizeof extra); extra[sizeof extra - 1] = 0; } else extra[0] = 0;
	}
public:
	int parseFrom(MemoryBuffer *msg);
	int encodeTo(MemoryBuffer *msg) const;
public:
	int64_t timestamp;
	stat_ip_t src_hip;
	stat_id_t src_sid;
	stat_ip_t dst_hip;
	stat_id_t dst_sid;
	
	int32_t retcode;
	stat_result_t result;

	char key[STAT_KEY_MAX];
	char extra[STAT_EXTRA_MAX];
};

class StatMergedRcall {
public:
	StatMergedRcall() {}
public:
	int parseFrom(MemoryBuffer *msg);
	int encodeTo(MemoryBuffer *msg) const;
public:
	int64_t timestamp;
	stat_ip_t src_hip;
	stat_id_t src_sid;
	stat_ip_t dst_hip;
	stat_id_t dst_sid;
	
	uint8_t freqs;
	uint8_t ftype;

	typedef std::tr1::unordered_map<int32_t, stat_mresult_t> mresult_map_t;
	typedef mresult_map_t::iterator iterator;
	typedef mresult_map_t::const_iterator const_iterator;

	mresult_map_t rets;
};

typedef std::tr1::unordered_map<local_key_t, StatMergedGauge, LocalKeyHash> merged_gauge_map_t;
typedef merged_gauge_map_t::iterator gauge_iterator;
typedef merged_gauge_map_t::const_iterator const_gauge_iterator;
	
typedef std::tr1::unordered_map<local_key_t, StatMergedLcall, LocalKeyHash> merged_lcall_map_t;
typedef merged_lcall_map_t::iterator lcall_iterator;
typedef merged_lcall_map_t::const_iterator const_lcall_iterator;
	
typedef std::tr1::unordered_map<rcall_key_t, StatMergedRcall, RcallKeyHash> merged_rcall_map_t;
typedef merged_rcall_map_t::iterator rcall_iterator;
typedef merged_rcall_map_t::const_iterator const_rcall_iterator;

int parseFrom(stat_ip_t& hip, MemoryBuffer *msg);
int encodeTo(MemoryBuffer *msg, const stat_ip_t& hip);
int parseFrom(stat_id_t& sid, MemoryBuffer *msg);
int encodeTo(MemoryBuffer *msg, const stat_id_t& sid);

#endif /* __STAT_DATA__ */
