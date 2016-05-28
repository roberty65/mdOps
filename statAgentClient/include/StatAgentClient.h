/* StatAgentClient.h
 * Copyright by yu.c.w 2002-2020
**/
#ifndef __STAT_AGENT_CLIENT__H
#define __STAT_AGENT_CLIENT__H

#include <stdint.h>
#include "StatData.h"

extern const char *logCursorPostfix;

class StatAgentClient {
public:
	static StatAgentClient _inst;
	static StatAgentClient *getInstance() { return &_inst; }
public:
	int onInit(const char *file);
	void onExit();
public:
	int logGauge(uint32_t ip4, const stat_id_t& sid, uint8_t gtype, int64_t gval);
	int logGauge(int16_t iid, uint8_t gtype, int64_t gval) {
		 stat_id_t sid(pid, mid, iid);
		 return logGauge(0, sid, gtype, gval);
	}

	int logLcall(uint32_t ip4, const stat_id_t& sid, int32_t retcode, const stat_result_t& result, const char *key, const char *extra);
	int logLcall(int16_t iid, int32_t retcode, const stat_result_t& result, const char *key, const char *extra) {
		stat_id_t sid(pid, mid, iid);
		 return logLcall(0, sid, retcode, result, key, extra);
	}

	int logRcall(uint32_t src_ip4, const stat_id_t& src_sid, uint32_t dst_ip4, const stat_id_t& dst_sid,
		     int32_t retcode, const stat_result_t& result, const char *key, const char *extra);
	int logRcall(int16_t iid, uint32_t dst_ip4, const stat_id_t& dst_sid, int32_t retcode, 
		     const stat_result_t& result, const char *key, const char *extra) {
		stat_id_t src_sid(pid, mid, iid);
		return logRcall(0, src_sid, dst_ip4, dst_sid, retcode, result, key, extra);
	}
private:
	int doLog(time_t time, unsigned char *data, size_t size);
private:
	int16_t pid;
	int16_t mid;
	stat_ip_t hip;

	char logFilePrefix[256];
};

class StatAgentLcallGuard {
public:
	StatAgentLcallGuard(int16_t _iid, int32_t& _retcode) : iid(_iid), retode(_retcode), isize(0), osize(0) {
		key[0] = 0;
		extra[0] = 0;
		gettimeofday(&startTime, NULL);
	}
	~StatAgentLcallGuard() {

	}
public:
	void setIsize(uint32_t isize) { this->isize = isize; }
	void setOsize(uint32_t osize) { this->osize = osize; }
	void setKey(const char *key) { strncpy(this->key, key, sizeof this->key); this->key[sizeof this->key - 1] = 0; }
	void setExtra(const char *extra) { strncpy(this->extra, extra, sizeof this->extra); this->extra[sizeof this->extra -1] = 0; }
private:
	int16_t iid;
	int32_t& retode;
	uint32_t isize, osize;
	char key[128];
	char extra[256];
	struct timeval startTime;
};

#endif /* __STAT_AGENT_CLIENT__H */
