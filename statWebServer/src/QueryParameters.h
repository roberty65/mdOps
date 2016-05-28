/* QueryParameters.h
 * Copyright by Beyondy.c.w 2002-2020
**/
#ifndef __QUERY_PARAMETERS__H
#define __QUERY_PARAMETERS__H

#include <cstdlib>
#include <string>
#include <tr1/unordered_map>

class QueryParameters {
public:
	QueryParameters() { parse(getenv("QUERY_STRING")); }
	QueryParameters(const char *str) { parse(str); }
public:
	bool exist(const char *name) const;
	const char *getString(const char *name, const char *def) const;
	int getInt(const char *name, int def) const;
	long getLong(const char *name, long def) const;
	float getFloat(const char *name, float def) const;
	double getDouble(const char *name, double def) const;
private:
	void parse(const char *str);
	std::string decodeUrlEncodedString(const std::string& val) const;
private:
	// need multi-map?
	typedef std::tr1::unordered_map<std::string, std::string> parameter_map_t;
	typedef parameter_map_t::iterator iterator;
	typedef parameter_map_t::const_iterator const_iterator;

	parameter_map_t parameters;
};

#endif /* __QUERY_PARAMETERS__H */
