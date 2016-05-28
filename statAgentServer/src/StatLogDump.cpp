#include <sys/types.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "utils.h"
#include "Message.h"
#include "StatData.h"

namespace helper {
//
//some helper functions
//
static char *tst2str(char *buf, size_t size, int64_t timestamp)
{
	time_t secs = timestamp / 1000;
	long ms = timestamp % 1000;

	struct tm tmbuf, *ptm = localtime_r(&secs, &tmbuf);
	xsnprintf(buf, size, "%04d-%02d-%02d %02d:%02d:%02d.%03ld", ptm->tm_year + 1900,
		ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,
		ms);
	return buf;
}

static char *hip2str(char *buf, size_t size, const stat_ip_t& hip)
{
	if (hip.ver == 4)
		inet_ntop(AF_INET, (void *)&hip.ip.ip4, buf, size);
	else buf[0] = 0;

	return buf;
}

static char *sid2str(char *buf, size_t size, const stat_id_t& sid)
{
	xsnprintf(buf, size, "%04x-%04x-%04x", sid.pid, sid.mid, sid.iid);
	return buf;
}

static char *frq2str(char *buf, int size, uint8_t ftype, uint8_t freqs)
{
	const char *fts[] = { "s", "m", "h", "d", "m", "y" };
	const char *ffs = (ftype >= 0 && ftype <= sizeof(fts)/sizeof(fts[0])) ? fts[ftype] : "?";

	xsnprintf(buf, size, "%u%s", freqs, ffs);		
	return buf;
}

static void dump(const StatItemGauge& gauge)
{
	char buf1[64], buf2[64], buf3[64];
	
	printf("IG\t%s\t%s\t%s\t%u\t%ld\n", 
		tst2str(buf1, sizeof buf1, gauge.timestamp),
		hip2str(buf2, sizeof buf2, gauge.hip),
		sid2str(buf3, sizeof buf3, gauge.sid),
		(unsigned int)gauge.gtype, gauge.gval);
}

static void dump(const StatMergedGauge& gauge)
{
	char buf1[64], buf2[64], buf3[64], buf4[64];
	
	printf("MG\t%s\t%s\t%s\t%s\t%u\t%ld\n", 
		tst2str(buf1, sizeof buf1, gauge.timestamp),
		hip2str(buf2, sizeof buf2, gauge.hip),
		sid2str(buf3, sizeof buf3, gauge.sid),
		frq2str(buf4, sizeof buf4, gauge.ftype, gauge.freqs),
		(unsigned int)gauge.gtype, gauge.gval);
}

static void dump(const StatItemLcall& lcall)
{
	char buf1[64], buf2[64], buf3[64];

	printf("IL\t%s\t%s\t%s\t%d\t%u\t\%u\t\%u\t%s\t%s\n",
		tst2str(buf1, sizeof buf1, lcall.timestamp),
		hip2str(buf2, sizeof buf2, lcall.hip),
		sid2str(buf3, sizeof buf3, lcall.sid),
		lcall.retcode, lcall.result.rsptime, 
		lcall.result.isize, lcall.result.osize,
		lcall.key, lcall.extra);
}

static void dump(const StatMergedLcall& lcall)
{
	char buf1[64], buf2[64], buf3[64], buf4[64];

	printf("ML\t%s\t%s\t%s\t%s\n",
		tst2str(buf1, sizeof buf1, lcall.timestamp),
		hip2str(buf2, sizeof buf2, lcall.hip),
		sid2str(buf3, sizeof buf3, lcall.sid),
		frq2str(buf4, sizeof buf4, lcall.ftype, lcall.freqs));

	for (StatMergedLcall::const_iterator iter = lcall.rets.begin();
		iter != lcall.rets.end();
			++iter) {
		printf("\t%d\t%u\t%u\t%u\t%u\n",
			iter->first, iter->second.count, iter->second.rsptime,
			iter->second.isize, iter->second.osize);
	}
}

static void dump(const StatItemRcall& rcall)
{
	char buf1[64], buf2[64], buf3[64], buf4[64], buf5[64];
	
	printf("IR\t%s\t%s\t%s\t%s\t%s\t%d\t%u\t%u\t%u\t%s\t%s\n",
		tst2str(buf1, sizeof buf1, rcall.timestamp),
		hip2str(buf2, sizeof buf2, rcall.src_hip), 
		sid2str(buf3, sizeof buf3, rcall.src_sid),
		hip2str(buf4, sizeof buf4, rcall.dst_hip),
		sid2str(buf5, sizeof buf5, rcall.dst_sid),
		rcall.retcode, rcall.result.rsptime, 
		rcall.result.isize, rcall.result.osize,
		rcall.key, rcall.extra);
}

static void dump(const StatMergedRcall& rcall)
{
	char buf1[64], buf2[64], buf3[64], buf4[64], buf5[64], buf6[64];
	
	printf("MR\t%s\t%s\t%s\t%s\t%s\t%s\n",
		tst2str(buf1, sizeof buf1, rcall.timestamp),
		hip2str(buf2, sizeof buf2, rcall.src_hip), 
		sid2str(buf3, sizeof buf3, rcall.src_sid),
		hip2str(buf4, sizeof buf4, rcall.dst_hip),
		sid2str(buf5, sizeof buf5, rcall.dst_sid),
		frq2str(buf6, sizeof buf6, rcall.ftype, rcall.freqs));

	for (StatMergedRcall::const_iterator iter = rcall.rets.begin();
		iter != rcall.rets.end();
			++iter) {
		printf("\t%d\t%u\t%u\t%u\t%u\n",
			iter->first, iter->second.count, iter->second.rsptime,
			iter->second.isize, iter->second.osize);
	}
}

}; /* helper */

// copy from StatLogWatcher::parseLogItem
// TODO: better sharing
int parseLogItem(beyondy::Async::Message *msg)
{
	uint8_t type;
	if (msg->readUint8(type) < 0) return -1;
	switch (type) {
	case STAT_ITEM_GAUGE: {
		StatItemGauge gauge;
		if (gauge.parseFrom(msg) < 0) return -1; // not read the whole record in
		helper::dump(gauge);
		break;
	}
	case STAT_MERGED_GAUGE: {
		StatMergedGauge gauge;
		if (gauge.parseFrom(msg) < 0) return -1;
		helper::dump(gauge);
		break;
	}
	case STAT_ITEM_LCALL: {
		StatItemLcall lcall;
		if (lcall.parseFrom(msg) < 0) return -1;
		helper::dump(lcall);
		break;
	}
	case STAT_MERGED_LCALL: {
		StatMergedLcall lcall;
		if (lcall.parseFrom(msg) < 0) return -1;
		helper::dump(lcall);
		break;
	}
	case STAT_ITEM_RCALL: {
		StatItemRcall rcall;
		if (rcall.parseFrom(msg) < 0) return -1;
		helper::dump(rcall);
		break;
	}
	case STAT_MERGED_RCALL: {
		StatMergedRcall rcall;
		if (rcall.parseFrom(msg) < 0) return -1;
		helper::dump(rcall);
		break;
	}
	default:
		fprintf(stderr, "unknown stat-type=%d\n", (int)type);
		break;
	}

	return 0;
}

static int parseLogData(beyondy::Async::Message *msg)
{
	while (msg->getRptr() + 4 < msg->getWptr()) {
		long savedRptr = msg->getRptr();
		int retval;

		if ((retval = parseLogItem(msg)) == -2) {
			fprintf(stderr, "data corrupted?\n");
			return -1;
		}
		else if (retval == -1) {
			msg->setRptr(savedRptr);
			break;
		}
	}

	return 0;
}

int statLogDump(const char *file)
{
	int fd = open(file, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "open %s failed: %m\n", file);
		return -1;
	}

	unsigned char buf[16384];
	long boff = 0, foff = 0;
	int retval = 0;
	
	while (true) {
		ssize_t rlen = read(fd, buf + boff, sizeof buf - boff);
		if (rlen == 0) // no more
			break;
		if (rlen < 0 && errno == EINTR)
			continue;
		if (rlen < 0) {
			fprintf(stderr, "read at %ld failed: %m\n", foff);
			retval = -1;
			break;
		}

		// parse and dump it
		beyondy::Async::Message msg(buf, boff + rlen);
		msg.setWptr(boff + rlen); // set it manually

		if (parseLogData(&msg) < 0) {
			fprintf(stderr, "data corrupted at about %ld?!\n", foff);
			break;
		}

		if (msg.getRptr() < msg.getWptr()) {
			boff = msg.getWptr() - msg.getRptr();
			memcpy(buf, buf + msg.getRptr(), boff);
		}

		foff += rlen;
	}

	close(fd);
	return retval;
}

int main(int argc, const char **argv)
{
	if (argc < 2 || (argc >= 2 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0))) {
		fprintf(stderr, "Usage: %s stat-log-file [...]\n", argv[0]);
		fprintf(stderr, "       %s -h | --help ---- show this help message\n", argv[0]);
		exit(0);
	}

	for (int i = 1; i < argc; ++i) {
		statLogDump(argv[i]);
	}

	return 0;
}
