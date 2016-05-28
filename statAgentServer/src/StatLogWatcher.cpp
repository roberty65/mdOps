/* StatLogWatcher.cpp
 * Copyright by Beyondy.c.w 2002-2020
**/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <assert.h>
#include "utils.h"
#include "Log.h"
#include "Message.h"
#include "StatAgentProcessor.h"
#include "StatMerger.h"
#include "StatLogWatcher.h"

const char *logCursorPostfix = "_cursor.pt";
const int ioRetries = 5;

class LogFileFilter {
public:
	virtual bool filter(const char *name) = 0;
	virtual std::string getResult() const = 0;
};

class EarliestLogFileFilter : public LogFileFilter {
public:
	EarliestLogFileFilter() {}
public:
	virtual bool filter(const char *name) {
		if (earliestFile.empty() || strcmp(earliestFile.c_str(), name) > 0)
			earliestFile.assign(name);
		return false;
	}
	
	virtual std::string getResult() const {
		return earliestFile;
	}
private:
	std::string earliestFile;
};

class NextLogFileFilter : public LogFileFilter {
public:
	NextLogFileFilter(std::string _curFile) : curFile(_curFile) {}
public:
	virtual bool filter(const char *name) {
		if (strcmp(name, curFile.c_str()) <= 0) return false;
		if (nxtFile.empty() || strcmp(nxtFile.c_str(), name) > 0)
			nxtFile.assign(name);
		return false;
	}

	virtual std::string getResult() const { return nxtFile; }
private:
	std::string curFile;
	std::string nxtFile;
};

namespace helper {
int saveMergedGauges(void *data, const merged_gauge_map_t *pm)
{
	StatAgentProcessor *proc = static_cast<StatAgentProcessor *>(data);
	return proc->saveMergedGauges(pm);
}

int saveMergedLcalls(void *data, const merged_lcall_map_t *pm)
{
	StatAgentProcessor *proc = static_cast<StatAgentProcessor *>(data);
	return proc->saveMergedLcalls(pm);
}

int saveMergedRcalls(void *data, const merged_rcall_map_t *pm)
{
	StatAgentProcessor *proc = static_cast<StatAgentProcessor *>(data);
	return proc->saveMergedRcalls(pm);
}
} /* end of helper */

#define NEXT_STEP_CONT	0
#define NEXT_STEP_EOF	1
#define NEXT_STEP_EXIT	2
#define NEXT_STEP_ERROR	3

StatLogWatcher::StatLogWatcher(StatAgentProcessor *_proc, const char *_logFilePrefix, int _ftype, int _freqs, int _mcnt)
	: merger(_proc, helper::saveMergedGauges, helper::saveMergedLcalls, helper::saveMergedRcalls, _ftype, _freqs, _mcnt),
	  proc(_proc)
{
	xsnprintf(logFilePrefix, sizeof logFilePrefix, "%s", _logFilePrefix);
}

int StatLogWatcher::scanLogDirectory(LogFileFilter *filter)
{
	char cursorFile[PATH_MAX];
	snprintf(cursorFile, sizeof cursorFile - 1, "%s%s", logFilePrefix, logCursorPostfix);
	cursorFile[sizeof cursorFile - 1] = 0;

	int plen = strlen(logFilePrefix);
	DIR *dir = opendir(proc->statDirectory.c_str());
	if (dir == NULL) {
		APPLOG_ERROR("open dir %s failed: %m", proc->statDirectory.c_str());
		return -1;
	}

	long debuf[(sizeof(struct dirent) + 1024) / sizeof(long)];
	struct dirent *pe;

	while (readdir_r(dir, (struct dirent *)debuf, &pe) == 0) {
		if (pe == NULL)
			break;
		//if (pe->d_type != DT_REG)
		//	continue;

		if (strncmp(pe->d_name, logFilePrefix, plen) != 0)
			continue;
		if (strcmp(pe->d_name, cursorFile) == 0)
			continue;

		if (filter->filter(pe->d_name))
			break;
	}

	closedir(dir);
	return 0;
}

std::string StatLogWatcher::findEarliestLogFile()
{
	EarliestLogFileFilter logFilter;
	int retval = scanLogDirectory(&logFilter);

	if (retval < 0) return std::string();
	std::string result = logFilter.getResult();
	if (result.empty()) return result;

	return proc->statDirectory + result;
}

std::string StatLogWatcher::findNextLogFile(const std::string& curFile)
{
	NextLogFileFilter logFilter(curFile);
	int retval = scanLogDirectory(&logFilter);

	if (retval < 0) return std::string();
	std::string result = logFilter.getResult();
	if (result.empty()) return result;

	return proc->statDirectory + result;
}

void StatLogWatcher::makeCursorPath(char *path, size_t size)
{
	snprintf(path, size, "%s%s%s", proc->statDirectory.c_str(), logFilePrefix, logCursorPostfix);
	path[size - 1] = 0;
}

std::string StatLogWatcher::getLogFilePosition(const char *logFilePrefix, long& logOffset)
{
	char cursorPath[PATH_MAX];
	makeCursorPath(cursorPath, sizeof cursorPath);

	std::string strCursor = getFileContent(cursorPath);
	std::size_t pos = strCursor.find_first_of(' ');

	if (pos == std::string::npos)
		strCursor.find_first_of('\t');

	if (pos == std::string::npos) {
		return std::string();
	}

	std::string logFile = strCursor.substr(0, pos);
	logOffset = strtoul(strCursor.c_str() + pos + 1, NULL, 0);

	APPLOG_DEBUG("load cursor from %s is %s %ld", cursorPath, logFile.c_str(), logOffset);
	if (logFile.empty()) return logFile;

	return proc->statDirectory + logFile;
}

int StatLogWatcher::saveLogFilePosition(const std::string& logFile, long logOffset)
{
	char cursorPath[PATH_MAX];
	makeCursorPath(cursorPath, sizeof cursorPath);

	char buf[1024];	// just save its name part
	snprintf(buf, sizeof buf, "%s %ld", logFile.c_str() + proc->statDirectory.length(), logOffset);
	buf[sizeof buf - 1] = 0;

	int retval = saveFileContent(cursorPath, buf, 0);
	if (retval < 0) {
		APPLOG_ERROR("save cursor at %s %ld failed, cache it", logFile.c_str(), logOffset);
		lastLogFile.assign(logFile);
		lastLogOffset = logOffset;
	}
	else {
		APPLOG_DEBUG("save cursor at %s %ld OK", logFile.c_str(), logOffset);
		// will load from cursor again
		lastLogFile.clear();
		lastLogOffset = 0;
	}

	return retval;
}

int StatLogWatcher::parseLogItem(beyondy::Async::Message *msg)
{
	uint8_t type = -1;
	if (msg->readUint8(type) < 0) return -1;

	switch (type) {
	case STAT_ITEM_GAUGE: {
		StatItemGauge gauge;
		if (gauge.parseFrom(msg) < 0) return -1; // not read the whole record in
		merger.addItemGauge(gauge);
		break;
	}
	case STAT_MERGED_GAUGE: {
		StatMergedGauge gauge;
		if (gauge.parseFrom(msg) < 0) return -1;
		merger.addMergedGauge(gauge);
		break;
	}
	case STAT_ITEM_LCALL: {
		StatItemLcall lcall;
		if (lcall.parseFrom(msg) < 0) return -1;
		merger.addItemLcall(lcall);
		break;
	}
	case STAT_MERGED_LCALL: {
		StatMergedLcall lcall;
		if (lcall.parseFrom(msg) < 0) return -1;
		merger.addMergedLcall(lcall);
		break;
	}
	case STAT_ITEM_RCALL: {
		StatItemRcall rcall;
		if (rcall.parseFrom(msg) < 0) return -1;
		merger.addItemRcall(rcall);
		break;
	}
	case STAT_MERGED_RCALL: {
		StatMergedRcall rcall;
		if (rcall.parseFrom(msg) < 0) return -1;
		merger.addMergedRcall(rcall);
		break;
	}
	default:
		APPLOG_ERROR("unknown stat-type=%d", (int)type);
		break;
	}

	return 0;
}

void StatLogWatcher::parseLogData()
{
	beyondy::Async::Message msg(logBuf, unhandledSize);
	msg.setWptr(unhandledSize);	// set end ptr
	long count = 0;
	
	while (msg.getRptr() + 4 <= msg.getWptr()) {
		long savedRptr = msg.getRptr();
		if (parseLogItem(&msg) < 0) {
			msg.setRptr(savedRptr);
			break;
		}

		++count;
	}

	// move the rest partial record ahead
	unhandledSize -=  msg.getRptr();
	memcpy(logBuf, logBuf + msg.getRptr(), unhandledSize);
	
	APPLOG_DEBUG("log(total-size=%ld) is parsed done, got item=%ld, remaining=%ld", msg.getWptr(), count, unhandledSize);
	return;
}

// but update logOffset
int StatLogWatcher::parseLogFile(const std::string& logFile, long& logOffset)
{
	int fd = -1;
	long consumedSize = 0;

	for (int i = 0; i < ioRetries; ++i) {
		if ((fd = open(logFile.c_str(), O_RDONLY)) >= 0)
			break;
		
		if (errno == EINTR)
			continue;

		APPLOG_ERROR("open logfile(%s) failed: %m", logFile.c_str());
		return NEXT_STEP_ERROR;
	}
	
	if (lseek(fd, logOffset, SEEK_SET) != logOffset) {
		close(fd);
		APPLOG_ERROR("seek logfile(%s) to offset=%ld failed: %m", logFile.c_str(), logOffset);
		return NEXT_STEP_ERROR; // try later again
	}

	// must set as nonblock?
	int flags = fcntl(fd, F_GETFL);
	if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
		close(fd);
		APPLOG_ERROR("fctl(%s) nonblock failed: %m", logFile.c_str());
		return NEXT_STEP_ERROR;
	}
	
	int retval = NEXT_STEP_ERROR;
	while (true) {
		// check exiting? if so, exit immediately
		if (!proc->isRunning) {
			saveLogFilePosition(logFile, logOffset);
			APPLOG_DEBUG("logWatcher(%s) will exit after app exiting", logFilePrefix);
			retval = NEXT_STEP_EXIT;
			break;
		}

		ssize_t rlen = -1;
		for (int i = 0; i < ioRetries; ++i) {
			rlen = read(fd, logBuf + unhandledSize, sizeof logBuf - unhandledSize);
			if (rlen < 0 && errno == EINTR)
				continue;
			break;
		}

		if (rlen > 0) {
			// parse & aggregate
			consumedSize += rlen;
			unhandledSize += rlen;

			logOffset += unhandledSize;
			parseLogData();

			// update cursor first
			logOffset -= unhandledSize;

			// update cursor very time???
			saveLogFilePosition(logFile, logOffset);
			continue;
		}
		else if (rlen == 0 || (rlen < 0 && errno == EAGAIN)) {
			// EOF or no more data available
			// try later
			APPLOG_ERROR("read(%s) got rlen=%ld, consumed=%ld error: %m", 
				logFile.c_str(), (long)rlen, consumedSize);
			retval = consumedSize > 0 ? NEXT_STEP_CONT : NEXT_STEP_EOF;
			break;
		}

		// IO error, try later
		APPLOG_ERROR("consumed=%ld, and read(%s) got error: %m", consumedSize, logFile.c_str());
		retval = NEXT_STEP_ERROR;
		break;
	}

	close(fd);
	return retval;
}

bool StatLogWatcher::nextLogFileAvailable(const std::string& logFile)
{
	// step 1: check whether has newer file?
	std::string nextLogFile = findNextLogFile(logFile.substr(proc->statDirectory.length()));
	if (nextLogFile.empty()) {
		// no new file, watch current file again
		APPLOG_DEBUG("did not get newer file, watch the current file again");
		return false;	
	}

	APPLOG_DEBUG("got newer file: %s", nextLogFile.c_str());
	// has newer file, the current one should not be update again
	// go next
	if (unhandledSize > 0) {
		APPLOG_ERROR("unhandled-size=%ld in log-file: %s", (long)unhandledSize, logFile.c_str());
		unhandledSize = 0;
	}

	if (saveLogFilePosition(nextLogFile, 0) < 0) {
		APPLOG_FATAL("save cursor to [%s, 0] failed", nextLogFile.c_str());
		// ignore error
	}

	return true;
}

int StatLogWatcher::watchFile(const std::string &logFile, long logOffset)
{
	int ioeCount = 0;	// I/O continuous error
	int eofCount = 0;

	unhandledSize = 0;	// reset it
	while (true) {
		int nextStep = parseLogFile(logFile, logOffset);
		if (nextStep == NEXT_STEP_CONT) {
			ioeCount = eofCount = 0; // reset both
		}	
		else if (nextStep == NEXT_STEP_EOF) {
			++eofCount;
			APPLOG_DEBUG("got the %dth EOF for %s", eofCount, logFile.c_str());

			ioeCount = 0; // clear it
			if (eofCount >= 2 && nextLogFileAvailable(logFile)) {
				return 0;
			}
		}
		else if (nextStep == NEXT_STEP_EXIT) {
			return -1;
		}
		else if (nextStep == NEXT_STEP_ERROR) {
			++ioeCount;
			APPLOG_DEBUG("got the %dth IO error for %s", ioeCount, logFile.c_str());

			eofCount = 0;	// reset it?
			if (ioeCount >= ioRetries && nextLogFileAvailable(logFile)) {
				// give us the current one, try next file
				return 0;
			}
		}
		else {
			assert("No Such Case" == NULL);
		}

		// if not exiting, wait and try again
		if (!proc->isRunning)
			return -1;

		totalSleep(proc->statCheckInterval * 1000);
		if (!proc->isRunning)
			return -1;
	}

	return 0;
}

std::string StatLogWatcher::getLogFile(long& logOffset)
{
	if (lastLogFile.empty()) {
		// no saved pointer
		std::string logFile = getLogFilePosition(logFilePrefix, logOffset);
		if (!logFile.empty()) return logFile;

		logOffset = 0;
		logFile = findEarliestLogFile();
		if (!logFile.empty()) return logFile;

		/* NO LOG NOW */
		return std::string();
	}
	
	// use saved pointer
	logOffset = lastLogOffset;
	return lastLogFile;
}

void StatLogWatcher::watchLoop()
{
	APPLOG_INFO("logWatcher on %s started...", logFilePrefix);
	struct timeval t1, t2;
	gettimeofday(&t1, NULL);

	while (proc->isRunning) {
		gettimeofday(&t2, NULL);
		long ms = TV_DIFF_MS(&t1, &t2);		
		ms = proc->statCheckInterval * 1000 - ms;
		if (ms > 0) {
			totalSleep(ms);
		}
		
		t1 = t2;
		if (!proc->isRunning)
			break;

		long logOffset = 0;
		std::string logFile = getLogFile(logOffset);
		if (logFile.empty()) continue;
		
		if (watchFile(logFile, logOffset) < 0)
			break;
	}

	APPLOG_INFO("logWatcher on %s exit", logFilePrefix);
	return;
}

