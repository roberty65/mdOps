/* ClientConnection.h
 * Copyright by Beyondy.c.w 2008-2020
**/
#ifndef CLIENT_CONNECTION__H
#define CLIENT_CONNECTION__H

#include <string>
#include <beyondy/timedout_countdown.hpp>

class MemoryBuffer;

class ClientConnection {
public:
	ClientConnection(const char *addr, int timeout, int retires);
	~ClientConnection();
private:
	int connect(beyondy::TimedoutCountdown& timerDown);
	void close();
	int send(const MemoryBuffer *req, beyondy::TimedoutCountdown& timerDown);
	int recv(MemoryBuffer *rsp, beyondy::TimedoutCountdown& timerDown);
public:
	int request(const MemoryBuffer *req, MemoryBuffer *rsp);
private:
	int fd;
	int timeout;
	int retires;
	std::string addr;
};


#endif /* CLIENT_CONNECTION__H */

