/* QueryParameters.h
 * Copyright by Beyondy.c.w 2002-2020
**/
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "QueryParameters.h"

void QueryParameters::parse(const char *str)
{
	const char *end = str + strlen(str);
	const char *sptr, *nptr = str;

	while (nptr < end) {
		if ((sptr = strchr(nptr, '&')) == NULL)
			sptr = end;

		const char *eptr = nptr;
		while (eptr < sptr && *eptr != '=') ++eptr;

		if (eptr == sptr) {
			// no value
			std::string name(nptr, size_t(sptr - nptr));
			parameters.insert(std::make_pair(name, std::string()));
		}
		else {
			std::string name(nptr, size_t(eptr - nptr));
			std::string value(eptr + 1, size_t(sptr - eptr - 1));

			value = decodeUrlEncodedString(value);
			parameters.insert(std::make_pair(name, value));
		}

		nptr = sptr + 1;
	}


	return;
}

std::string QueryParameters::decodeUrlEncodedString(const std::string& val) const
{
	// TODO:	
	return val;
}

bool QueryParameters::exist(const char *name) const
{
	const_iterator iter = parameters.find(name);
	if (iter != parameters.end()) return true;
	return false;
}

const char *QueryParameters::getString(const char *name, const char *def) const
{
	const_iterator iter = parameters.find(name);
	if (iter != parameters.end())
		return iter->second.c_str();
	return def;
}

int QueryParameters::getInt(const char *name, int def) const
{
	const char *str = getString(name, NULL);
	if (str != NULL) {
		char *eptr;
		return strtol(str, &eptr, 0);
	}

	return def;
}

long QueryParameters::getLong(const char *name, long def) const
{
	const char *str = getString(name, NULL);
	if (str != NULL) {
		return strtol(str, NULL, 0);
	}
	
	return def;
}

float QueryParameters::getFloat(const char *name, float def) const
{
	const char *str = getString(name, NULL);
	if (str != NULL) {
		return strtof(name, NULL);
	}

	return def;
}

double QueryParameters::getDouble(const char *name, double def) const
{
	const char *str = getString(name, NULL);
	if (str != NULL) {
		return strtod(str, NULL);
	}

	return def;
}

