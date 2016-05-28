/* StatStorageProcessor.h
 * Copyright by Beyondy.c.w 2002-2020
**/
#ifndef __STAT_STORAGE_PROCESSOR__H
#define __STAT_STORAGE_PROCESSOR__H

#include <stdint.h>
#include <errno.h>
#include <string>

#include "proto_h16.h"
#include "FileStorage.h"
#include "Processor.h"

class Message;

class StatStorageProcessor : public beyondy::Async::Processor {
public:
	virtual size_t headerSize() const {
		return sizeof(struct proto_h16_head);
	}

	virtual ssize_t calcMessageSize(beyondy::Async::Message *msg) const {
                uint32_t len;
                memcpy(&len, msg->rb(), sizeof(len));

                if (len < sizeof(proto_h16_head) || len > maxInputSize) {
                        errno = EINVAL;
                        ssize_t retval = (ssize_t)len;
                        if (retval > 0) return -retval;
                        return retval;
                }

                return len;
        }

	virtual int onInit();
	virtual void onExit();

	virtual int onMessage(beyondy::Async::Message *msg);
	virtual int onSent(beyondy::Async::Message *msg, int status);
private:
	int doResponse(beyondy::Async::Message *rsp, int cmd, int retcode, const struct proto_h16_head *h, const beyondy::Async::Message *msg);
	int onSaveStatsRequest(struct proto_h16_head *h, beyondy::Async::Message *msg);
	int onGetSystemStatsRequest(const struct proto_h16_head *h, beyondy::Async::Message *msg);
	int onGetUserStatsRequest(const struct proto_h16_head *h, beyondy::Async::Message *msg);
private:
	std::string baseDir;
	FileStorage storage;
	
	uint32_t nextSyn;
	long maxInputSize;
	long maxOutputSize;
};

#endif /* __STAT_STORAGE_PROCESSOR__H */
