/* ClientConnection.h
 * Copyright by Beyondy.c.w 2008-2020
**/
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <proto_h16.h>
#include <beyondy/xbs_naddr.h>
#include <beyondy/xbs_socket.h>
#include <beyondy/xbs_io.h>

#include "MemoryBuffer.h"
#include "ClientConnection.h"

ClientConnection::ClientConnection(const char *_addr, int _timeout, int _retries)
	: fd(-1), timeout(_timeout), retires(_retries), addr(_addr)
{
	/* nothing */
}

ClientConnection::~ClientConnection()
{
	if (fd >= 0) ::close(fd);
}

int ClientConnection::connect(beyondy::TimedoutCountdown& timerDown)
{
	if ((fd = beyondy::XbsClient(addr.c_str(), 0, timerDown.Update())) < 0)
		return -1;

	if (errno != 0) {
		close();
		return -1;
	}	
		
	return 0;
}

void ClientConnection::close()
{
	::close(fd);
	fd = -1;
}

int ClientConnection::send(const MemoryBuffer *req, beyondy::TimedoutCountdown& timerDown)
{
	const struct proto_h16_head *h = (const struct proto_h16_head *)req->data();
	if (beyondy::XbsWriteN(fd, req->data(), h->len, timerDown.Update()) < 0)
		return -1;
	return 0;
}

int ClientConnection::recv(MemoryBuffer *rsp, beyondy::TimedoutCountdown& timerDown)
{
	struct proto_h16_res *h;
	rsp->ensureCapacity(sizeof(*h));

	if (beyondy::XbsReadN(fd, rsp->data(), sizeof(*h), timerDown.Update()) < 0)
		return -1;

	h = (struct proto_h16_res *)rsp->data();
	if (rsp->ensureCapacity(h->len) < 0)
		return -1;

	if (beyondy::XbsReadN(fd, rsp->data() + sizeof(*h), h->len - sizeof(*h), timerDown.Update()) < 0)
		return -1;

	return 0;
}

int ClientConnection::request(const MemoryBuffer *req, MemoryBuffer *rsp)
{
	beyondy::TimedoutCountdown timerDown(timeout);
	int retval;

	if (fd < 0) {
		if ((retval = connect(timerDown)) < 0) {
			return retval;
		}

		assert(fd >= 0);
	}

	if ((retval = send(req, timerDown)) < 0) {
		close();
		return retval;
	}

	if ((retval = recv(rsp, timerDown)) < 0) {
		close();
		return retval;
	}

	close();
	return 0;
}
