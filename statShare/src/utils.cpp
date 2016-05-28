/* Utils.cpp
 * Copyright yu.c.w 2002-2020
**/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>

#include "utils.h"

static int ioRetries = 5;	// TODO: some where in configuration

void totalSleep(long ms)
{
	if (ms < 0) return;
	struct timespec ts;

	ts.tv_sec = ms / 1000;
	ts.tv_nsec = (ms % 1000) * 1000000;
	while (nanosleep(&ts, &ts) < 0) { /* nothing */ }
}

char* xsnprintf(char *buf, size_t size, const char *fmt...)
{
	va_list ap; 
	va_start(ap, fmt);

	int slen = vsnprintf(buf, size, fmt, ap);
	if (slen >= (int)size - 1) buf[size - 1] = 0;

	va_end(ap);
	return buf;
}

// for small file
std::string getFileContent(const char *file)
{
	int fd = open(file, O_RDONLY);
	if (fd < 0) return std::string();	// with errno set

	char buf[8192];
	ssize_t rlen = -1;
	for (int i = 0; i < ioRetries; ++i) {
		rlen = ::read(fd, buf, sizeof buf);
		if (rlen < 0 && errno == EINTR)
			continue;
		break;
	}

	close(fd);
	if (rlen < 0) return std::string();	// with errno set

	errno = 0;	// clear errno
	return std::string(buf, rlen);
}

// for small file only
int saveFileContent(const char *file, const char *str, size_t len)
{
	int fd = open(file, O_CREAT|O_TRUNC|O_WRONLY, 0664);
	if (fd < 0) return -1;

	if (len == 0) len = strlen(str);

	ssize_t wlen = -1;
	for (int i = 0; i < ioRetries; ++i) {
		wlen = write(fd, str, len);
		if (wlen < 0 && errno == EINTR)
			continue;
		break;
	}

	close(fd);

	if (wlen == (ssize_t)len) return 0;
	if (wlen < 0) return -1;

	errno = EINPROGRESS;	/* HOW TO RECOVERY IT!!! */
	return -1;
}

