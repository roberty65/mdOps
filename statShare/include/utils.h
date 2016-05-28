/* utils.h
 * Copyright@ yu.c.w 2002 - 2020
**/
#ifndef __UTILS__H
#define __UTILS__H
#include <sys/time.h>
#include <string>

// already defined in limits.h
//#define PATH_MAX		1024

#define TV2MS(t) ((int64_t)(t)->tv_sec * 1000 + (t)->tv_usec / 1000)
#define TV_DIFF_MS(t1, t2) (((t2)->tv_sec - (t1)->tv_sec) * 1000 + ((t2)->tv_usec - (t1)->tv_usec) / 1000)

void totalSleep(long ms);
char *xsnprintf(char * buf, size_t size, const char * fmt...);

std::string getFileContent(const char * file);
int saveFileContent(const char * file, const char * str, size_t len);

#endif /* __UTILS__H */
