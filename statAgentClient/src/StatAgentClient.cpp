/* StatAgentClient.cpp
 *  copyright by beyondy 2002-2020
 *
**/
#include <sys/types.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include <limits.h>

#include "utils.h"
#include "StatErrno.h"
#include "ConfigProperty.h"
#include "StatAgentClient.h"

const char *logCursorPostfix = "_cursor.pt";
StatAgentClient StatAgentClient::_inst;

int StatAgentClient::onInit(const char *file)
{
	ConfigProperty cfp(file);

	this->pid = cfp.getInt("pid", 0);
	this->mid = cfp.getInt("mid", 0);

	const char *ips = cfp.getString("localAddress", NULL);
	if (ips == NULL) {
		// TODO: get local address
		ips = "127.0.0.1";
	}

	int retval = inet_pton(AF_INET, ips, (void *)&hip.ip.ip4);
	if (retval == 1) { hip.ver = 4; }
	else if ((retval = inet_pton(AF_INET, ips, (void *)&hip.ip.ip6)) == 1) {
		hip.ver = 6;
	}
	else {
		return -1;
	} 

	strncpy(logFilePrefix, cfp.getString("statFilePrefix", "../stat/xx"), sizeof(logFilePrefix));
	logFilePrefix[sizeof logFilePrefix - 1] = 0;

	char cursorPath[PATH_MAX];
	xsnprintf(cursorPath, sizeof cursorPath, "%s%s", logFilePrefix, logCursorPostfix);
	
	int fd = open(cursorPath, O_CREAT|O_EXCL, 0664);
	close(fd);	// ignore it?
	
	return 0;
}

void StatAgentClient::onExit()
{
	// nothing now?
}

int StatAgentClient::doLog(time_t ts, unsigned char *data, size_t dsize)
{
	struct tm tmbuf, *ptm = localtime_r(&ts, &tmbuf);
	char path[PATH_MAX];

	snprintf(path, sizeof path, "%s_%04d_%02d_%02d.bin", 
		logFilePrefix, ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday);
	path[sizeof path - 1] = 0;

	int fd = open(path, O_CREAT|O_APPEND|O_WRONLY, 0664);
	if (fd < 0) {
		errno = E_STAT_AGENT_CLIENT_OPEN;
		return -1;
	}

	int retval = 0;
	for (int i = 0; i < 5; ++i) {
		ssize_t wlen = write(fd, data, dsize);
		if (wlen == (ssize_t)dsize)
			break;
		if (wlen < 0 && errno == EINTR)
			continue;

		if (wlen < 0) {
			// this log will be lost
			errno = E_STAT_AGENT_CLIENT_WRITE_ERROR;
		}
		else {
			// !!! file will be corrupted
			// TODO: how to recovery automatically????
			errno = E_STAT_AGENT_CLIENT_WRITE_PARTIAL;
		}
		
		retval = -1;
	}

	close(fd);
	return retval;
}

int StatAgentClient::logGauge(uint32_t ip4, const stat_id_t& sid, uint8_t gtype, int64_t gval)
{
	struct timeval ts;
	gettimeofday(&ts, NULL);

	StatItemGauge gauge(TV2MS(&ts), ip4 == 0 ? this->hip : stat_ip_t(ip4), sid, gtype, gval);

	unsigned char data[4 + sizeof(StatItemGauge)];
	MemoryBuffer msg(data, sizeof data, false);

	msg.writeUint8(STAT_ITEM_GAUGE);
	int retval = gauge.encodeTo(&msg);
	assert(retval == 0);

	return doLog(ts.tv_sec, data, msg.getWptr());
}

int StatAgentClient::logLcall(uint32_t ip4, const stat_id_t& sid, int32_t retcode, 
			      const stat_result_t& result, const char *key, const char *extra)
{
	struct timeval ts;
	gettimeofday(&ts, NULL);

	StatItemLcall lcall(TV2MS(&ts), ip4 == 0 ? this->hip : stat_ip_t(ip4), sid, retcode, result, key, extra);
	
	unsigned char data[4 + sizeof(StatItemLcall)];
	MemoryBuffer msg(data, sizeof data, false);

	msg.writeUint8(STAT_ITEM_LCALL);
	int retval = lcall.encodeTo(&msg);
	assert(retval == 0);
	
	return doLog(ts.tv_sec, data, msg.getWptr());
}

int StatAgentClient::logRcall(uint32_t src_ip4, const stat_id_t& src_sid, uint32_t dst_ip4, const stat_id_t& dst_sid,
			      int32_t retcode, const stat_result_t& result, const char *key, const char *extra)
{
	struct timeval ts;
	gettimeofday(&ts, NULL);

	StatItemRcall rcall(TV2MS(&ts), src_ip4 == 0 ? this->hip : stat_ip_t(src_ip4), src_sid,
			    dst_ip4 == 0 ? this->hip : stat_ip_t(dst_ip4), dst_sid, retcode, result, key, extra);
	
	unsigned char data[4 + sizeof(StatItemRcall)];
	MemoryBuffer msg(data, sizeof data, false);

	msg.writeUint8(STAT_ITEM_RCALL);
	int retval = rcall.encodeTo(&msg);
	assert(retval == 0);
	
	return doLog(ts.tv_sec, data, msg.getWptr());
}
