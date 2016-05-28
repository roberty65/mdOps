/* NameFilter.h
 * Copyright by Beyondy.c.w 2002-2020
**/
#ifndef __NAME_FILTER__H
#define __NAME_FILTER__H

#include <sys/types.h>
#include <regex.h>
#include <vector>

class NameFilter {
public:
	int add(const char *regx, int id);
	int getId(const char *name) const;
private:
	typedef std::pair<regex_t*, int> regex_id_t;
	std::vector<regex_id_t> reghs;
};

#endif /* __NAME_FILTER__H */
