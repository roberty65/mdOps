/* StatAgentProcessor.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
#include <pthread.h>
#include <sys/types.h>
#include <sys/time.h>
#include <dirent.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>

#include "utils.h"
#include "proto_h16.h"
#include "Log.h"
#include "Message.h"
#include "StatData.h"
#include "StatErrno.h"
#include "StatCommand.h"
#include "ConfigProperty.h"
#include "StatAgentProcessor.h"

// implemented by bServer frame
// TODO: how to get frame's symbol by a better way
extern "C" int getConnector(const char *name);

beyondy::Async::Message *StatAgentProcessor::newStatMessage(size_t size)
{  
	beyondy::Async::Message *msg = beyondy::Async::Message::create(size, -1, statStorageFlow);
	if (msg != NULL) msg->setWptr(sizeof(struct proto_h16_head));
	return msg;
}

int StatAgentProcessor::sendStatMessage(beyondy::Async::Message *msg)
{
	struct proto_h16_head *h = (struct proto_h16_head *)msg->data();
	memset(h, sizeof *h, 0);

	h->len = msg->getWptr();
	h->cmd = CMD_STAT_AGENT_SAVE_STATS_REQ;
	h->ver = 1;
	h->syn = nextSyn++;

	if (sendMessage(msg) < 0) {
		APPLOG_ERROR("send stat-msg (size=%d) failed", h->len);
		return -1;
	}

	APPLOG_DEBUG("send stat-msg(size=%d, sync=%d) OK", h->len, h->syn);
	return 0;
}
	
int StatAgentProcessor::saveMergedGauges(const merged_gauge_map_t *pm)
{
	int i = 0, pkgCount = 100;
	beyondy::Async::Message *msg = NULL;

	for (const_gauge_iterator iter = pm->begin(); iter != pm->end(); ++iter) {
		if (msg == NULL) {
			// TODO: how to better predicate the size
			msg = newStatMessage(pkgCount * (sizeof(StatMergedGauge)));
			if (msg == NULL) {
				APPLOG_ERROR("create a msg for merged gauge failed, discard rest!");
				return -1;
			}
		}

		const StatMergedGauge& gauge = iter->second;
		msg->writeUint8(STAT_MERGED_GAUGE);
		gauge.encodeTo(msg);

		if (++i == pkgCount) {
			sendStatMessage(msg);
			msg = NULL;
			i = 0;
		}
	}

	// send out last one
	if (msg != NULL) {
		sendStatMessage(msg);
		msg = NULL;	
	}

	return 0;
}

int StatAgentProcessor::saveMergedLcalls(const merged_lcall_map_t *pm)
{
	int i = 0, pkgCount = 100;
	beyondy::Async::Message *msg = NULL;

	for (const_lcall_iterator iter = pm->begin(); iter != pm->end(); ++iter) {
		if (msg == NULL) {
			msg = newStatMessage(pkgCount * (sizeof(StatMergedLcall) + 32 * (8 + sizeof(stat_mresult_t))));
			if (msg == NULL) {
				APPLOG_ERROR("create a msg for merged gauge failed, discard rest!");
				return -1;
			}
		}

		const StatMergedLcall& lcall = iter->second;
		msg->writeUint8(STAT_MERGED_LCALL);
		lcall.encodeTo(msg);

		if (++i == pkgCount) {
			sendStatMessage(msg);
			msg = NULL;
			i = 0;
		}
	}

	// send out last one
	if (msg != NULL) {
		sendStatMessage(msg);
		msg = NULL;
	}

	return 0;
}

int StatAgentProcessor::saveMergedRcalls(const merged_rcall_map_t *pm)
{
	int i = 0, pkgCount = 100;
	beyondy::Async::Message *msg = NULL;

	for (const_rcall_iterator iter = pm->begin(); iter != pm->end(); ++iter) {
		if (msg == NULL) {
			msg = newStatMessage(pkgCount * (sizeof(StatMergedRcall) + 32 * (8 + sizeof(stat_mresult_t))));
			if (msg == NULL) {
				APPLOG_ERROR("create a msg for merged gauge failed, discard rest!");
				return -1;
			}
		}

		const StatMergedRcall& rcall = iter->second;
		msg->writeUint8(STAT_MERGED_RCALL);
		rcall.encodeTo(msg);

		if (++i == pkgCount) {
			sendStatMessage(msg);
			msg = NULL;
			i = 0;
		}
	}

	// send out last one
	if (msg != NULL) {
		sendStatMessage(msg);
		msg = NULL;
	}

	return 0;
}

void *StatAgentProcessor::__statLogWatcher(void *p)
{
	StatLogWatcher *wdi = (StatLogWatcher *)p;
	wdi->watchLoop();
	return NULL;
}

int StatAgentProcessor::addDirectory(const char *logFilePrefix)
{
	for (StatLogWatcherIterator iter = watchedDirectories.begin();
		iter != watchedDirectories.end();
			++iter) {
		if (strcmp((*iter)->logFilePrefix, logFilePrefix) == 0) {
			return 0;
		}
	}

	StatLogWatcher *wdi = new (std::nothrow)StatLogWatcher(this, logFilePrefix, 
					statMergeFtype, statMergeFreqs, maxCachedPeriod);
	if (wdi == NULL) {
		APPLOG_ERROR("can not create wdi for log-entry: %s", logFilePrefix);
		return -1;
	}
	
	errno = pthread_create(&wdi->tid, NULL, __statLogWatcher, (void *)wdi);
	if (errno != 0) {
		APPLOG_ERROR("create thread for log-entry: %s failed: %m", logFilePrefix);
		delete wdi;
		return -1;
	}

	watchedDirectories.push_back(wdi);
	return 0;
}

int StatAgentProcessor::checkDirectory()
{
	APPLOG_DEBUG("check Directory(%s) for potential new stat files", statDirectory.c_str());
	DIR *dir = opendir(statDirectory.c_str());
	if (dir == NULL) {
		APPLOG_ERROR("open dir(%s) failed: %m\n", statDirectory.c_str());
		return -1;
	}

	int plen = strlen(logCursorPostfix);
#if 0
	int name_max = pathconf(statDirectory.c_str(), _PC_NAME_MAX);
	if (name_max < 0) name_max = 1024;
	unsigned long ebuf[sizeof(struct dirent) + name_max];
#else
	unsigned long ebuf[PATH_MAX/sizeof(unsigned long)];
#endif
	struct dirent *pe;
	
	while (readdir_r(dir, (struct dirent *)ebuf, &pe) == 0) {
		if (pe == NULL) break;
		// disable it, not all system support it
		//if (pe->d_type != DT_REG) continue;
		
		APPLOG_DEBUG("reg-dir: %s", pe->d_name);
		int nlen = strlen(pe->d_name);
		if (nlen <= plen || strcmp(pe->d_name + nlen - plen, logCursorPostfix) != 0)
			continue;
		char logFilePrefix[PATH_MAX];
		if (nlen >= (int)sizeof(logFilePrefix) - 16)	/* 2002-0320.bin */
			continue;	// file name is too long

		// just copy the prefix
		memcpy(logFilePrefix, pe->d_name, nlen - plen);
		logFilePrefix[nlen - plen] = 0;
		addDirectory(logFilePrefix);
	}

	closedir(dir);
	time(&lastCheckDirectoryTimestamp);

	return 0;
}

void StatAgentProcessor::reportHostInfo()
{
	beyondy::Async::Message *msg = beyondy::Async::Message::create(1024, -1, statServerFlow);
	struct proto_h16_head *h = (struct proto_h16_head *)msg->data();
	memset(h, sizeof *h, 0);

	h->cmd = CMD_STAT_AGENT_REPORT_HOSTINFO_REQ;
	h->len = sizeof *h;
	
//	msg->writeString(hostName);
//	msg->writeUint32(hostIp4);

	if (sendMessage(msg) < 0) {
		APPLOG_ERROR("send reportHostInfo message failed");
		beyondy::Async::Message::destroy(msg);
		return;
	}
	
	APPLOG_ERROR("send out reportHostInfo to meta-server");
	time(&lastReportingTimestamp);

	return;
}

void* StatAgentProcessor::__watchEntry(void *p)
{
	StatAgentProcessor *proc = (StatAgentProcessor *)p;
	proc->watchEntry();
	return NULL;
}

void StatAgentProcessor::watchEntry()
{
	APPLOG_ERROR("watch-thread started, report this host...");
	firstReportingDone = false;
	reportHostInfo();

	struct timeval t1, t2;
	gettimeofday(&t1, NULL);

	while (isRunning) {
		gettimeofday(&t2, NULL);
		long ms = TV_DIFF_MS(&t1, &t2);
		ms = watchInterval * 1000 - ms;
		if (ms > 0) {
			totalSleep(ms);
		}

		t1 = t2;
		if (!isRunning) break;
		
		if (!firstReportingDone) {
			int gap = reportHostInfoInterval / 3;
			if (gap < 60) gap = 60; else if (gap > 600) gap = 600;
			if (gap > reportHostInfoInterval) gap = reportHostInfoInterval;
			
			if (t2.tv_sec - lastReportingTimestamp >= gap)
				reportHostInfo();
		}
		else if (t2.tv_sec - lastReportingTimestamp >= reportHostInfoInterval) {
			reportHostInfo();
		}

		if (t2.tv_sec - lastCheckDirectoryTimestamp > checkDirectoryInterval) {
			checkDirectory();
		}
	}

	APPLOG_ERROR("watch-thread exit");
	return;
}

int StatAgentProcessor::onInit()
{
	isRunning = 1;

	statStorageFlow = getConnector("storage");
	statServerFlow = getConnector("meta");
//	if (statStorageFlow < 0 || statServerFlow < 0) {
//		APPLOG_FATAL("storage-flow=%d, meta-flow=%d", statStorageFlow, statServerFlow);
//		return -1;
//	}

	/*
	 * load custom configurations
	**/
	ConfigProperty cfp;
	if (cfp.parse("../conf/stat.conf") < 0) return -1;

//	const char *strLevel = cfp.getString("LogLevel", "ERROR");
//	const char *logFileName = cfp.getString("logFileName", "../logs/statAgent.log");
//	int logFileMaxSize = cfp.getInt("logFileMaxSize", 10*1024*1024);
//	int logMaxBackup = cfp.getInt("logMaxBackup", 10);
	APPLOG_INIT();

	// TODO:
	// hostname, host-ip?

	watchInterval = cfp.getInt("watchInterval", 2);
	reportHostInfoInterval = cfp.getInt("reportHostInfoInterval", 3600);
	firstReportingDone = false;
	lastReportingTimestamp = 0;
	
	checkDirectoryInterval = cfp.getInt("checkDirectoryInterval", 60);
	lastCheckDirectoryTimestamp = 0;
	
	statDirectory.assign(cfp.getString("statDirectory", "../stats/"));
	statCheckInterval = cfp.getInt("statCheckInterval", 2);

	const char *str = cfp.getString("statMergeFrequency", "5m");
	char *eptr;
	statMergeFreqs = strtol(str, &eptr, 0);
	statMergeFtype = FT_MINUTE;
	if (*eptr == 's' || *eptr == 'S') statMergeFtype = FT_SECOND;
	else if (*eptr == 'm' || *eptr == 'M') statMergeFtype = FT_MINUTE;
	else if (*eptr == 'h' || *eptr == 'h') statMergeFtype = FT_HOUR;
	else if (*eptr == 'd' || *eptr == 'D') statMergeFtype = FT_DAY;
	else {
		APPLOG_FATAL("invalid merge-frequency-type: %s", eptr);
		return -1;
	}

	maxCachedPeriod = cfp.getInt("statCachedPeriods", 2);
	flushDelay = cfp.getInt("statFlushDelay", 5);

	errno = pthread_create(&watchTid, NULL, __watchEntry, (void *)this);
	if (errno) {
		APPLOG_FATAL("create watch-thrad failed: %m");
		return -1;
	}

	nextSyn = 0;
	maxInputSize = 1024000;
	maxOutputSize = 1024000;

	return 0;
}

void StatAgentProcessor::onExit()
{
	isRunning = false;
}

void StatAgentProcessor::onReportHostInfoDone(beyondy::Async::Message *msg)
{
	firstReportingDone = true;
	struct proto_h16_res *rsp = (struct proto_h16_res *)msg->data();
	
	APPLOG_INFO("report host info done: retval=%d", rsp->ret);
	beyondy::Async::Message::destroy(msg);
}

void StatAgentProcessor::onGetStorageDistributionDone(beyondy::Async::Message *msg)
{
	// TODO:
	beyondy::Async::Message::destroy(msg);
}

void StatAgentProcessor::onSaveStatsDone(beyondy::Async::Message *msg)
{
	struct proto_h16_res *h;
	if (msg->getWptr() >= (long)sizeof(struct proto_h16_res)) {
		h = (struct proto_h16_res *)msg->data();
		APPLOG_DEBUG("stat-msg(ack=%u) saved result: %d", h->ack, h->ret);
	}
	else {
		APPLOG_ERROR("stat-msg rsp is too little: size=%ld, should be %ld", msg->getWptr(), (long)sizeof(*h));
	}

	beyondy::Async::Message::destroy(msg);
	return;
}

int StatAgentProcessor::onMessage(beyondy::Async::Message *req)
{
	struct proto_h16_head *h = (struct proto_h16_head *)req->data();
	if (req->getWptr() < (long)sizeof(struct proto_h16_head)) return -1;	// should not have such case!!!

	req->incRptr(sizeof(*h));
	switch (h->cmd) {
	case CMD_STAT_AGENT_REPORT_HOSTINFO_RSP:
		onReportHostInfoDone(req);
		break;
	case CMD_STAT_AGENT_GET_STORAGE_DISTRIBUTION_RSP:
		onGetStorageDistributionDone(req);
		break;
	case CMD_STAT_AGENT_SAVE_STATS_RSP:
		onSaveStatsDone(req);
		break;
	default:
		APPLOG_WARN("unknown command=%d", h->cmd);
		beyondy::Async::Message::destroy(req);
		break;
	}

	return 0;
}

int StatAgentProcessor::onSent(beyondy::Async::Message *msg, int status)
{
	if (status != SS_OK) {
		APPLOG_ERROR("sending msg out failed: %d", status);
	}

	beyondy::Async::Message::destroy(msg);
	return 0;
}

extern "C" beyondy::Async::Processor *createProcessor()
{
	return new StatAgentProcessor();
}

extern "C" void destroyProcess(beyondy::Async::Processor *_proc)
{
	StatAgentProcessor *proc = dynamic_cast<StatAgentProcessor *>(_proc);
	if (proc != NULL) delete proc;
}

