/* NameFilter.h
 * Copyright by Beyondy.c.w 2002-2020
**/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <map>
#include "NameFilter.h"

int NameFilter::add(const char *regx, int id)
{
	char buf[1024];
	int slen;

	if (regx == NULL || (slen = strlen(regx)) < 1 || slen >= (int)sizeof(buf) - 3) {
		fprintf(stderr, "regstr is NULL, empty, or too long\n");
		return -1;
	}
	
	if (*regx != '^') {
		buf[0] = '^'; strcpy(buf + 1, regx); buf[1 + slen] = '$';
		buf[1 + slen + 1] = 0;
	}
	else {
		strcpy(buf, regx);
	}
		
	regex_t	 *preg = (regex_t *)malloc(sizeof(*preg));
	if (preg == NULL) {
		fprintf(stderr, "malloc regex_t failed");
		return -1;
	}

	int retval = regcomp(preg, buf, REG_EXTENDED | REG_NEWLINE | REG_NOSUB);
	if (retval) {
		fprintf(stderr, "regcomp(%s) failed", buf);
		return -1;
	}

	if (id == -1) {
		for (id = 0; id < (int)reghs.size() + 1; ++id) {
			bool find = false;
			for (std::vector<regex_id_t>::iterator iter = reghs.begin();
				iter != reghs.end();
					++iter) {
				if (id == iter->second) {
					find = true;
					break;
				}
			}

			if (!find) {
				break;
			}
		}
	}
	else {
		for (std::vector<regex_id_t>::iterator iter = reghs.begin();
			iter != reghs.end();
				++iter) {
			if (id == iter->second) {
				throw std::runtime_error("duplicated id");
			}
		}
	}

	fprintf(stderr, "added into filter: %s => %d\n", buf, id);
	reghs.push_back(std::make_pair(preg, id));
	return id;
}

int NameFilter::getId(const char *name) const
{
	for (std::vector<regex_id_t>::const_iterator iter = reghs.begin();
		iter != reghs.end();
			++iter) {
		int retval = regexec(iter->first, name, 0, NULL, 0);
		if (retval == 0) return iter->second;
	}

	return -1;
}

