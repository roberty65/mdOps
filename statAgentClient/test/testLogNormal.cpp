/* testLogNormal.cpp
 * Copyright@ yu.c.w 2002-2020
**/
#include "StatData.h"
#include "ConfigProperty.h"
#include "StatAgentClient.h"

int main(int argc, char **argv)
{
	StatAgentClient *stc = StatAgentClient::getInstance();
	stc->onInit("../conf/stat.conf");

	stc->logGauge(100, SGT_DELTA, 100);	
	stc->logLcall(200, 0, stat_result_t(100,23,1024), "key", "extra");

	uint32_t ip2 = 0x0a0a0a0a;
	stc->logRcall(300, ip2, stat_id_t(2000,400,1), 2, stat_result_t(120,1024,32), "key2", "extra-2");

	stc->onExit();
	return 0;
}
