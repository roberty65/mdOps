/* StatLogWatcher.h
 * Copyright by Beyondy.c.w 2002-2020
**/
#ifndef __STAT_LOG_WATCHER__H
#define __STAT_LOG_WATCHER__H

#include <stdint.h>
#include <limits.h>	/* PATH_MAX */

#include "StatMerger.h"

class StatAgentProcessor;
class LogFileFilter;
class Message;

class StatLogWatcher {
public:
	StatLogWatcher(StatAgentProcessor *proc, const char *logFilePrefix, int ftype, int freqs, int mcnt);

private:
	int scanLogDirectory(LogFileFilter *filter);
	std::string findEarliestLogFile();
	std::string findNextLogFile(const std::string& curFile);
	void makeCursorPath(char *path, size_t size);
	std::string getLogFilePosition(const char *logFilePrefix, long& logOffset);
	int saveLogFilePosition(const std::string& logFile, long logOffset);
	int parseLogItem(beyondy::Async::Message *msg);
	void parseLogData();
	int parseLogFile(const std::string& logFile, long& logOffset);
	bool nextLogFileAvailable(const std::string& logFile);
	int watchFile(const std::string &logFile, long logOffset);
	std::string getLogFile(long& logOffset);
public:
	void watchLoop();
public:
	pthread_t tid;
	char logFilePrefix[PATH_MAX];
private:

	unsigned char logBuf[8192];
	long unhandledSize;

	time_t lastActiveTimestamp;

	std::string lastLogFile;
	long lastLogOffset;

	StatMerger merger;
	StatAgentProcessor *proc;
};

extern const char *logCursorPostfix;

#endif /* __STAT_LOG_WATCHER__H */

