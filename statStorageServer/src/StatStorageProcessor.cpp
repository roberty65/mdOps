#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <limits.h>
#include <errno.h>

#include "utils.h"
#include "Log.h"
#include "Message.h"
#include "StatData.h"
#include "StatErrno.h"
#include "StatCommand.h"
#include "StatCombiner.h"
#include "StatStorageProcessor.h"
#include "ConfigProperty.h"

int StatStorageProcessor::onInit()
{
	const char *file = "../conf/stat.conf";
	ConfigProperty cfp;
	if (cfp.parse(file) < 0) {
		fprintf(stderr, "parse %s failed: %m\n", file);
		return -1;
	}

	baseDir = cfp.getString("statsDir", "../stats/");
	storage.setDirectory(baseDir);

	nextSyn = 0;
	maxInputSize = 10*1024*1024;
	maxOutputSize = 10*1024*1024;

	APPLOG_INFO("StatStorageProcessor init");
	return 0;
}

void StatStorageProcessor::onExit()
{
	APPLOG_INFO("StatStorageProcessor exit");
}

int StatStorageProcessor::doResponse(beyondy::Async::Message *rsp, int cmd, int retcode, const struct proto_h16_head *h, const beyondy::Async::Message *msg)
{
	struct proto_h16_res *h2;
	if (rsp == NULL && (rsp = beyondy::Async::Message::create(sizeof *h2, msg->fd, msg->flow)) == NULL) {
		APPLOG_ERROR("create message for rsp(cmd=%d, ack=%d, retcode=%d) failed", cmd, h->syn, retcode);
		return -1;	
	}

	h2 = (struct proto_h16_res *)rsp->data();
	memset(h2, sizeof *h2, 0);

	h2->len = sizeof *h2;
	h2->cmd = cmd;
	h2->syn = nextSyn;
	h2->ack = h->syn;
	h2->ret = retcode;

	rsp->setWptr(sizeof *h2);
	int retval = sendMessage(rsp);

	if (retval < 0) {
		APPLOG_ERROR("send save-stats for rsp(cmd=%d, ack=%d, retcode=%d) failed", cmd, h->syn, retcode);
		beyondy::Async::Message::destroy(rsp);
		return -1;
	}

	return 0;
}

int StatStorageProcessor::onSaveStatsRequest(struct proto_h16_head *h, beyondy::Async::Message *msg)
{
	int retval = 0, cnt = 0;
	while (msg->getRptr() + 5 < msg->getWptr()) {
		uint8_t type = -1;
		msg->readUint8(type);
		switch (type) {
		case STAT_MERGED_GAUGE: {
			StatMergedGauge gauge;
			if ((retval = gauge.parseFrom(msg)) < 0) {
				APPLOG_WARN("parse MergedGauge failed at @%ld", (long)msg->getRptr());
				break;	
			}

			storage.saveMergedGauge(gauge);
			break;
		}
		case STAT_MERGED_LCALL: {
			StatMergedLcall lcall;
			if ((retval = lcall.parseFrom(msg)) < 0) {
				APPLOG_WARN("parse MergedLcall failed at @%ld", (long)msg->getRptr());
				break;	
			}

			storage.saveMergedLcall(lcall);
			break;
		}
		case STAT_MERGED_RCALL: {
			StatMergedRcall rcall;
			if ((retval = rcall.parseFrom(msg)) < 0) {
				APPLOG_WARN("parse MergedRcall failed at @%ld", (long)msg->getRptr());
				break;	
			}

			storage.saveMergedRcall(rcall);
			break;
		}
		default:
			APPLOG_WARN("unknown type=%d, ignore the rest!", (int)type);
			retval = -1;
			break;
		}

		++cnt;
	}

	APPLOG_DEBUG("saved %d stats-data: size=%u, syn=%u", cnt, h->len, h->syn); 

	if (doResponse(NULL, CMD_STAT_AGENT_SAVE_STATS_RSP, retval, h, msg) < 0) {
		APPLOG_ERROR("response SaveStats failed");
		retval = -1;	
	}

	beyondy::Async::Message::destroy(msg);
	return retval;
}

int StatStorageProcessor::onGetSystemStatsRequest(const struct proto_h16_head *h, beyondy::Async::Message *msg)
{
	beyondy::Async::Message *rsp = NULL;
	int retval = 0, retcode = E_STAT_PARAMETER_MISSING;

	uint8_t context, totalView, ftype, freqs;
	int64_t start, end;
	uint16_t pid, mid;
	std::vector<int> iids;
	host_set_t hosts;

	if (msg->readUint8(context) < 0 || msg->readUint8(totalView) < 0
		|| msg->readInt64(start) < 0 || msg->readInt64(end) < 0
		|| msg->readUint8(ftype) < 0 || msg->readUint8(freqs) < 0
			|| msg->readUint16(pid) < 0 || msg->readUint16(mid) < 0) {
		APPLOG_ERROR("invalid parameters in onGetSystemStatsRequest");
param_missing:
		retcode = E_STAT_PARAMETER_MISSING;
		doResponse(rsp, CMD_STAT_GET_SYSTEM_STATS_RSP, retcode, h, msg);
		return -1;	
	}

	uint16_t cnt = 0;
	if (msg->readUint16(cnt) < 0) goto param_missing;
	for (int i = 0; i < cnt; ++i) {
		uint16_t iid = 0;
		if (msg->readUint16(iid) < 0) goto param_missing;
		iids.push_back(iid);
	}

	if (msg->readUint16(cnt) < 0) goto param_missing;
	for (int i = 0; i < cnt; ++i) {
		stat_ip_t hip;
		if (parseFrom(hip, msg) < 0) goto param_missing;
		hosts.insert(hip);
	}

	int mergeCount = (end - start) / ftype; // TODO:
	StatCombiner combiner(ftype, freqs, start, mergeCount);
	if (storage.getSystemStats(combiner, context, totalView, start, end, ftype, freqs, pid, mid, iids, hosts) < 0) {
		APPLOG_ERROR("getSystemStats failed");
		retcode = E_STAT_GET_SYSTEM_STATS_FAILED;
	}
	else if ((rsp = beyondy::Async::Message::create(4096, msg->fd, msg->flow)) == NULL) {
		APPLOG_ERROR("allocate messge for getSystemStats failed");
		retcode = E_STAT_OOM;
	}
	else if (combiner.encodeTo(rsp) < 0) {
		APPLOG_ERROR("encodeMessage failed");
		retcode = E_STAT_ENCODE_FAILED;
	}
	else {
		retcode = 0;
	}

	if (doResponse(rsp, CMD_STAT_GET_SYSTEM_STATS_RSP, retcode, h, msg) < 0) {
		APPLOG_ERROR("response for GetSystemStatsRequst failed");
		retval = -1;
	}
	
	beyondy::Async::Message::destroy(msg);
	return retval;
}

int StatStorageProcessor::onGetUserStatsRequest(const struct proto_h16_head *h, beyondy::Async::Message *msg)
{
	int retval = 0;

	// TODO:

	if (doResponse(NULL, CMD_STAT_GET_USER_STATS_RSP, 0, h, msg) < 0) {
		APPLOG_ERROR("response for GetUserStatsRequst failed");
		retval = -1;
	}
	
	beyondy::Async::Message::destroy(msg);
	return retval;
}

int StatStorageProcessor::onMessage(beyondy::Async::Message *req)
{
	struct proto_h16_head *h = (struct proto_h16_head *)req->data();
	if (req->getWptr() < (long)sizeof(struct proto_h16_head)) return -1;	// should not have such case!!!

	req->incRptr(sizeof(*h));
	switch (h->cmd) {
	case CMD_STAT_AGENT_SAVE_STATS_REQ:
		onSaveStatsRequest(h, req);
		break;
	case CMD_STAT_GET_SYSTEM_STATS_REQ:
		onGetSystemStatsRequest(h, req);
		break;
	case CMD_STAT_GET_USER_STATS_REQ:
		onGetUserStatsRequest(h, req);
		break;
	default:
		APPLOG_WARN("unknown command=%d", h->cmd);
		beyondy::Async::Message::destroy(req);
		break;
	}

	return 0;
}

int StatStorageProcessor::onSent(beyondy::Async::Message *msg, int status)
{
	struct proto_h16_head *h = (struct proto_h16_head *)msg->data();
	APPLOG_DEBUG("msg(cmd=%d, size=%d) sent status: %d", (int)h->cmd, (int)h->len, status);

	beyondy::Async::Message::destroy(msg);
	return 0;
}

extern "C" beyondy::Async::Processor *createProcessor()
{
	return new StatStorageProcessor;
}

extern "C" void destroyProcessor(beyondy::Async::Processor *_proc)
{
	StatStorageProcessor *proc = dynamic_cast<StatStorageProcessor *>(_proc);
	if (proc) delete proc;
}

