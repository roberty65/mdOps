#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <regex.h>
#include <tr1/unordered_map>

#include "utils.h"
#include "Log.h"
#include "ConfigProperty.h"
#include "StatAgentClient.h"
#include "StatSystemIids.h"
#include "NameFilter.h"


static NameFilter netNameFilter;
static NameFilter diskNameFilter;

static long gapNextPeriodStart(struct timeval *tv, int ftype, int freqs)
{
	struct timeval t2 = { tv->tv_sec, tv->tv_usec };
	if (tv->tv_usec > 0) { t2.tv_sec += 1; t2.tv_usec = 0; }

	if (ftype == FT_SECOND) {
		t2.tv_sec += freqs;
		t2.tv_sec = t2.tv_sec / freqs * freqs;
	}
	else if (ftype == FT_MINUTE) {
		t2.tv_sec += freqs * 60;
		t2.tv_sec = t2.tv_sec / (freqs * 60) * (freqs * 60); 
	}
	else if (ftype == FT_HOUR) {
		// assume TZ just +/-hours
		t2.tv_sec += freqs * 3600;
		t2.tv_sec = t2.tv_sec / (freqs * 3600) * (freqs * 3600);
	}
	else {
		assert("does not support other period unit, except s/m/h" == NULL);
	}

	long ms = (t2.tv_sec - tv->tv_sec) * 1000 + (t2.tv_usec - tv->tv_usec + 999) / 1000;
	tv->tv_sec = t2.tv_sec; tv->tv_usec = 0;
	return ms;
}


// parse out at most the first n fields
static int splitString(char *line, char **arrs, int n)
{
	const char *sep = " \t";
	char *eptr;
	int i = 0;

	char *nptr = strtok_r(line, sep, &eptr);
	while (nptr != NULL && i < n) {
		arrs[i++] = nptr;
		nptr = strtok_r(NULL, sep, &eptr);
	}

	return i;
}

static void collectCpuUsage(struct timeval& tnow)
{
	struct timeval t2; gettimeofday(&t2, NULL);
	fprintf(stderr, "tnow=%ld, %ld, while t2(real)=%ld, %ld\n", tnow.tv_sec, tnow.tv_usec, t2.tv_sec, t2.tv_usec);
	static long lastUsr = 0;

	FILE *fp = fopen("/proc/stat", "r");
	if (fp == NULL) {
		fprintf(stderr, "open(/proc/stat) for cpu usage failed: %m\n");
		return;
	}

	StatAgentClient *cltAgent = StatAgentClient::getInstance();
	assert(cltAgent != NULL);

	char buf[1024]; // hope buffer is enough for one read
	while (fgets(buf, sizeof buf, fp) != NULL) {
		buf[sizeof buf - 1] = 0;
		char *eptr = buf + strlen(buf);
		while (eptr >= buf && (eptr[-1] == '\r' || eptr[-1] == '\n'))
			--eptr;
		*eptr = 0;

		//     user  nice sys  idl      iowat irq softirq steal guest guest-nice
		//cpu  26853 20 154637 12695753 21345 0 34601 0 0 0
		//cpu0 11742 10 104853 6309223 11142 0 18475 0 0 0
		//cpu1 15110 9 49784 6386529 10203 0 16125 0 0 0
		if (strncmp(buf, "cpu", 3) != 0)
			continue;
		int cno = 99;
		if (buf[3] >= '0' && buf[3] <= '9')
			cno = strtoul(buf + 3, &eptr, 0);
		else
			eptr = buf + 3;
		long usr = strtoul(eptr + 1, &eptr, 0);
if (cno == 99) {
	long delta = usr - lastUsr;
	fprintf(stderr, "lastUsr=%ld, usr=%ld, delta=%ld\n", lastUsr, usr, delta);
	lastUsr = usr;
}
		long nis = strtoul(eptr + 1, &eptr, 0);
		long sys = strtoul(eptr + 1, &eptr, 0);
		long idl = strtoul(eptr + 1, &eptr, 0);
		long wat = strtoul(eptr + 1, &eptr, 0);
//		long irq = strtoul(eptr + 1, &eptr, 0);
//		long softIRQ = strtoul(eptr + 1, &eptr, 0);
//		long steal = strtoul(eptr + 1, &eptr, 0);
//		long guest = strtoul(eptr + 1, &eptr, 0);
//		long guestNice = strtoul(eptr + 1, &eptr, 0);

		cltAgent->logGauge(IID_CPU(cno, CPU_USR), SGT_SNAPSHOT, usr+nis);
		cltAgent->logGauge(IID_CPU(cno, CPU_SYS), SGT_SNAPSHOT, sys);
		cltAgent->logGauge(IID_CPU(cno, CPU_IDL), SGT_SNAPSHOT, idl);
		cltAgent->logGauge(IID_CPU(cno, CPU_WT), SGT_SNAPSHOT, wat);
	}

	fclose(fp);
	return;
}

static void collectLoadAverage(const struct timeval& tv)
{
	FILE *fp = fopen("/proc/loadavg", "r");
	if (fp == NULL) {
		fprintf(stderr, "open(/proc/loadavg) for load-avg failed: %m");
		return;
	}

	char buf[1024];
	if (fgets(buf, sizeof buf, fp) != NULL) {
		buf[sizeof buf - 1] = 0;
		char *eptr = buf + strlen(buf);
		while (eptr >= buf && (eptr[-1] == '\r' || eptr[-1] == '\n'))
			--eptr;
		*eptr = 0;

		float m1 = strtof(buf, &eptr) * 100;
		float m5 = strtof(eptr + 1, &eptr) * 100;
		float m15 = strtof(eptr + 1, &eptr) * 100;

		StatAgentClient *cltAgent = StatAgentClient::getInstance();
		assert(cltAgent != NULL);
fprintf(stderr, "load-avg at %ld-%ld: m1=%.2f, m4=%.2f, m15=%.2f\n", tv.tv_sec, tv.tv_usec, m1, m5, m15);
		cltAgent->logGauge(IID_LOADAVG_1, SGT_SNAPSHOT, m1);
		cltAgent->logGauge(IID_LOADAVG_5, SGT_SNAPSHOT, m5);
		cltAgent->logGauge(IID_LOADAVG_15, SGT_SNAPSHOT, m15);
	}

	fclose(fp);
	return;
}

static void collectMemoryUsage(const struct timeval& tv)
{
	FILE *fp = fopen("/proc/meminfo", "r");
	if (fp == NULL) {
		fprintf(stderr, "open(/proc/meminfo) for memory usage failed: %m");
		return;
	}

	unsigned long total = 0, free = 0, buffers = 0, cached = 0, used;
	unsigned long swapTotal = 0, swapFree = 0, swapUsed;
	char buf[1024]; // hope buffer is enough for one read
	while (fgets(buf, sizeof buf, fp) != NULL) {
		buf[sizeof buf - 1] = 0;
		char *eptr = buf + strlen(buf);
		while (eptr >= buf && (eptr[-1] == '\r' || eptr[-1] == '\n'))
			--eptr;
		*eptr = 0;

#define MULTI(v) do { while (*eptr == ' ' || *eptr == '\t') ++eptr; \
		if (*eptr == 'k' || *eptr == 'K') (v) *= 1024; \
		else if (*eptr == 'm' || *eptr == 'M') (v) *= 1024 * 1024; } while(0)
	
		if (strncmp(buf, "MemTotal:", 9) == 0) {
			total = strtoul(buf + 9, &eptr, 0);
			MULTI(total);	
		}
		else if (strncmp(buf, "MemFree:", 8) == 0) {
			free = strtoul(buf + 8, &eptr, 0);
			MULTI(free);
		}
		else if (strncmp(buf, "Buffers:", 8) == 0) {
			buffers = strtoul(buf + 8, &eptr, 0);
			MULTI(buffers);
		}
		else if (strncmp(buf, "Cached:", 7) == 0) {
			cached = strtoul(buf + 7, &eptr, 0);
			MULTI(cached);
		}
		else if (strncmp(buf, "SwapTotal:", 10) == 0) {
			swapTotal = strtoul(buf + 10, &eptr, 0);
			MULTI(swapTotal);
		}
		else if (strncmp(buf, "SwapFree:", 9) == 0) {
			swapFree = strtoul(buf + 9, &eptr, 0);
			MULTI(swapFree);
		}
#undef MULTI
	}

	used = total - free - buffers - cached;
	swapUsed = swapTotal - swapFree;

	StatAgentClient *cltAgent = StatAgentClient::getInstance();
	assert(cltAgent != NULL);

	cltAgent->logGauge(IID_MEM_USED, SGT_SNAPSHOT, used);
	cltAgent->logGauge(IID_MEM_FREE, SGT_SNAPSHOT, free);
	cltAgent->logGauge(IID_MEM_CACHED, SGT_SNAPSHOT, cached);
	cltAgent->logGauge(IID_MEM_BUFFERS, SGT_SNAPSHOT, buffers);
	cltAgent->logGauge(IID_SWAP_USED, SGT_SNAPSHOT, swapUsed);
	cltAgent->logGauge(IID_SWAP_FREE, SGT_SNAPSHOT, swapFree);

	fclose(fp);
	return;
}

static void collectNetworkUsage(const struct timeval& tv) 
{
	FILE *fp = fopen("/proc/net/dev", "r");
	if (fp == NULL) {
		fprintf(stderr, "open(/proc/net/dev) for network usage failed: %m");
		return;
	}

	char buf[1024]; // hope buffer is enough for one read
	while (fgets(buf, sizeof buf, fp) != NULL) {
		buf[sizeof buf - 1] = 0;
		char *eptr = buf + strlen(buf);
		while (eptr >= buf && (eptr[-1] == '\r' || eptr[-1] == '\n'))
			--eptr;
		*eptr = 0;

		// the first two lines are header, discard them
		// assume there is no ':'
		char *cptr = strchr(buf, ':');
		if (cptr == NULL) continue;

		// skip whitespaces
		char *nptr = buf;
		while (nptr < cptr && (*nptr == ' ' || *nptr == '\t')) ++nptr;
		if (nptr >= cptr) {
			fprintf(stderr, "no net-name found in line: %s\n", buf);
			continue;
		}

		*cptr = 0;
		int nno = netNameFilter.getId(nptr);
		if (nno < 0) continue;
		
 		// face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
		// eno16777736: 412184835  301829    0    0    0     0          0         0  8250721  131693    0    0    0     0       0   0
		char *arrs[16];
		int n = splitString(cptr + 1, arrs, sizeof(arrs)/sizeof(arrs[0]));
		if (n < 10) continue;

		// TODO: more data
		int64_t inBytes, inPkts, outBytes, outPkts;
		inBytes = strtoul(arrs[0], &eptr, 0);
		inPkts = strtoul(arrs[1], &eptr, 0);
		outBytes = strtoul(arrs[8], &eptr, 0);
		outPkts = strtoul(arrs[9], &eptr, 0);	
		fprintf(stderr, "%s in-{bytes: %ld, pkts: %ld}, out-{bytes: %ld, pkts: %ld}\n", nptr, inBytes, inPkts, outBytes, outPkts);

		StatAgentClient *cltAgent = StatAgentClient::getInstance();
		assert(cltAgent != NULL);

		cltAgent->logGauge(IID_NET(nno, NET_T_IN_BYTES), SGT_SNAPSHOT, inBytes);
		cltAgent->logGauge(IID_NET(nno, NET_T_IN_PKTS), SGT_SNAPSHOT, inPkts);
		cltAgent->logGauge(IID_NET(nno, NET_T_OUT_BYTES), SGT_SNAPSHOT, outBytes);
		cltAgent->logGauge(IID_NET(nno, NET_T_OUT_BYTES), SGT_SNAPSHOT, outPkts);
	}

	fclose(fp);
	return;
}

static void collectDiskUsage(const struct timeval& tv)
{
	FILE *fp = fopen("/proc/diskstats", "r");
	if (fp == NULL) {
		fprintf(stderr, "open(/proc/diskstats) for disk usage failed: %m");
		return;
	}

	char buf[1024]; // hope buffer is enough for one read
	while (fgets(buf, sizeof buf, fp) != NULL) {
		buf[sizeof buf - 1] = 0;
		char *eptr = buf + strlen(buf);
		while (eptr >= buf && (eptr[-1] == '\r' || eptr[-1] == '\n'))
			--eptr;
		*eptr = 0;

		// 8 0 sda 218816 138882 19416939 8584688 44089 264373 4073727 1166561 0 1212670 9743408
		char *arrs[14];
		int n = splitString(buf, arrs, sizeof(arrs)/sizeof(arrs[0]));
		if (n < 11) continue;

		int dno = diskNameFilter.getId(arrs[2]);
		if (dno < 0) continue;

		// TODO: more data
		int64_t rCalls, rMerged, rBytes, rTime, wCalls, wMerged, wBytes, wTime;
		rCalls = strtoul(arrs[3], &eptr, 0);
		rMerged = strtoul(arrs[4], &eptr, 0);
		rBytes = strtoul(arrs[5], &eptr, 0);
		rTime = strtoul(arrs[6], &eptr, 0);
		wCalls = strtoul(arrs[7], &eptr, 0);
		wMerged = strtoul(arrs[8], &eptr, 0);
		wBytes = strtoul(arrs[9], &eptr, 0);
		wTime = strtoul(arrs[10], &eptr, 0);
		fprintf(stderr, "%s R-{calls: %ld, merged: %ld}, W-{rcalls: %ld, merged: %ld}\n", arrs[2], rCalls, rMerged, wCalls, wMerged);
	
		StatAgentClient *cltAgent = StatAgentClient::getInstance();
		assert(cltAgent != NULL);

		cltAgent->logGauge(IID_DISK(dno, DISK_T_R_CALLS), SGT_SNAPSHOT, rCalls);
		cltAgent->logGauge(IID_DISK(dno, DISK_T_R_MERGED), SGT_SNAPSHOT, rMerged);
		cltAgent->logGauge(IID_DISK(dno, DISK_T_R_BYTES), SGT_SNAPSHOT, rBytes);
		cltAgent->logGauge(IID_DISK(dno, DISK_T_R_TIME), SGT_SNAPSHOT, rTime);
		cltAgent->logGauge(IID_DISK(dno, DISK_T_W_CALLS), SGT_SNAPSHOT, wCalls);
		cltAgent->logGauge(IID_DISK(dno, DISK_T_W_MERGED), SGT_SNAPSHOT, wMerged);
		cltAgent->logGauge(IID_DISK(dno, DISK_T_W_BYTES), SGT_SNAPSHOT, wBytes);
		cltAgent->logGauge(IID_DISK(dno, DISK_T_W_TIME), SGT_SNAPSHOT, wTime);
	}

	fclose(fp);
	return;
	
}

int main(int argc, char **argv)
{
	ConfigProperty cfp("../conf/watch.conf");

	const char *istr = cfp.getString("interval", "1m");
	char *eptr;

	int ftype = FT_MINUTE;
	int freqs = strtol(istr, &eptr, 0);
	if (*eptr == 's' || *eptr == 'S') ftype = FT_SECOND;
	else if (*eptr == 'm' || *eptr == 'M') ftype = FT_MINUTE;
	else if (*eptr == 'h' || *eptr == 'H') ftype = FT_HOUR;
	else {
		fprintf(stderr, "Just support period unit: s/m/h\n");
		exit(1);
	}

	char buf[8192], *nptr;
	const char *strNetwork = cfp.getString("network", "");
	strncpy(buf, strNetwork, sizeof buf); buf[sizeof(buf) - 1] = 0;
	for (nptr = strtok_r(buf, ", \t", &eptr); 
		nptr != NULL;
			nptr = strtok_r(NULL, ", \t", &eptr)) {
		char *ptr2 = strchr(nptr, ':');
		int id = -1;
		if (ptr2 == NULL) {
			// use specified id
			id = netNameFilter.add(nptr, -1);
		}
		else {
			// use specified id
			*ptr2 = 0;
			id = strtol(ptr2 + 1, NULL, 0);
			id = netNameFilter.add(nptr, id);
		}
		
		fprintf(stderr, "watch net: %s => %d\n", nptr, id);
		if (id < 0) exit(3);		
	}

	const char *strDisk = cfp.getString("disk", "sda");
	strncpy(buf, strDisk, sizeof buf); buf[sizeof(buf) - 1] = 0;
	for (nptr = strtok_r(buf, ",", &eptr); 
		nptr != NULL;
			nptr = strtok_r(NULL, ",", &eptr)) {
		char *ptr2 = strchr(nptr, ':');
		int id = -1;
		if (ptr2 == NULL) {
			// use specified id
			id = diskNameFilter.add(nptr, -1);
		}
		else {
			// use specified id
			*ptr2 = 0;
			id = strtol(ptr2 + 1, NULL, 0);
			id = diskNameFilter.add(nptr, id);
		}
		
		fprintf(stderr, "watch disk: %s => %d\n", nptr, id);
		if (id < 0) exit(3);		
	}

	StatAgentClient *cltAgent = StatAgentClient::getInstance();
	if (cltAgent->onInit("../conf/stat.conf") < 0) {
		fprintf(stderr, "statAgentClinet->init(../conf/stat.conf) failed: %m\n");
		exit(1);
	}

	while (true) {
		// sleep to next interval's start
		struct timeval tv; gettimeofday(&tv, NULL);
		long ms = gapNextPeriodStart(&tv, ftype, freqs);
		totalSleep(ms);
	
		collectCpuUsage(tv);
		collectLoadAverage(tv);
		collectMemoryUsage(tv);
		collectNetworkUsage(tv);
		collectDiskUsage(tv);
	}

	return 0;
}
