#ifndef __LOG__H
#define __LOG__H

#define APPLOG_INIT() do {} while(0)
#define APPLOG_EXIT() do {} while(0)

#define APPLOG_FATAL(fmt...) do { fprintf(stderr, fmt); fprintf(stderr, "\n"); } while(0)
#define APPLOG_ERROR(fmt...) do { fprintf(stderr, fmt); fprintf(stderr, "\n"); } while(0)
#define APPLOG_WARN(fmt...) do { fprintf(stderr, fmt); fprintf(stderr, "\n"); } while(0)
#define APPLOG_DEBUG(fmt...) do { fprintf(stderr, fmt); fprintf(stderr, "\n"); } while(0)
#define APPLOG_INFO(fmt...) do { fprintf(stderr, fmt); fprintf(stderr, "\n"); } while(0)

#endif /* __LOG__H */

