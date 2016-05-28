/* StatAgentServer.h
 * Copyright by Beyondy.c.w 2002-2010
**/
#ifndef __STAT_AGENT_PROCESSOR__H
#define __STAT_AGENT_PROCESSOR__H

#include <tr1/unordered_map>
#include <vector>
#include <string>

#include "proto_h16.h"
#include "Message.h"
#include "Processor.h"
#include "StatLogWatcher.h"

class StatAgentProcessor : public beyondy::Async::Processor {
public:
	virtual int onInit();
	virtual void onExit();
public:
	virtual size_t headerSize() const {
                return sizeof (proto_h16_head);
        }

        virtual ssize_t calcMessageSize(beyondy::Async::Message *msg) const {
                uint32_t len;
                memcpy(&len, msg->data() + msg->getRptr(), sizeof(len));

                if (len < sizeof(proto_h16_head) || len > maxInputSize) {
                        errno = EINVAL;
                        ssize_t retval = (ssize_t)len;
                        if (retval > 0) return -retval;
                        return retval;
                }

                return len;
        }

	virtual int onMessage(beyondy::Async::Message *msg);
	virtual int onSent(beyondy::Async::Message *msg, int status);
private:
	beyondy::Async::Message *newStatMessage(size_t size);
	int sendStatMessage(beyondy::Async::Message *msg);
public:
	int saveMergedGauges(const merged_gauge_map_t *pm);
	int saveMergedLcalls(const merged_lcall_map_t *pm);
	int saveMergedRcalls(const merged_rcall_map_t *pm);
private:
	static void *__statLogWatcher(void *p);
	int addDirectory(const char *logFilePrefix);
	int checkDirectory();
	void reportHostInfo();
	static void* __watchEntry(void *p);
	void watchEntry();

	void onReportHostInfoDone(beyondy::Async::Message *msg);
	void onGetStorageDistributionDone(beyondy::Async::Message *msg);
	void onSaveStatsDone(beyondy::Async::Message *msg);
private:
	friend StatLogWatcher;

	typedef std::vector<StatLogWatcher *> StatLogWatcherList;
	typedef StatLogWatcherList::iterator StatLogWatcherIterator;

	StatLogWatcherList watchedDirectories;
private:
	volatile bool isRunning;
	int statServerFlow;
	int statStorageFlow;

	// how often watch-thread will be wakeup
	int watchInterval;

	// report host information to meta-db
	int reportHostInfoInterval;
	bool firstReportingDone;
	time_t lastReportingTimestamp;
	
	// check stat-dir for new files
	// restart dead threads?
	int checkDirectoryInterval;
	time_t lastCheckDirectoryTimestamp;
	
	// base stats directory
	std::string statDirectory;

	// if reach EOF when reading stat log file,
	// waiting below INTERVAL, and try again.
	int statCheckInterval;
	
	int statMergeFtype;
	int statMergeFreqs;

	int maxCachedPeriod;
	int flushDelay;

	pthread_t watchTid;

	uint32_t nextSyn;
	size_t maxInputSize;
	size_t maxOutputSize;
};

#endif /* __STAT_AGENT_PROCESSOR__H */
