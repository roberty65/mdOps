/* StatSystemIids.h
 * Copyright by Beyondy.c.w 2008-2020
**/
#ifndef __STAT_SYSTEM_IIDS__H
#define  __STAT_SYSTEM_IIDS__H

// [1000,2000)
#define CPU_SYS			0
#define CPU_USR			1
#define CPU_IDL			2
#define CPU_WT			3
#define CPU_ST			4

#define IID_CPU_CORES		98
#define IID_CPU_TOTAL		99

#define IID_CPU(no,type)	(1000 + (no)*10 + (type))	// type=CPU_SYS...
#define IID_IS4CPU(id)		((id) >= 1000 && (id) < 2000)
#define IID2CPUNO(id)		(((id) - 1000) / 10)

// [2000,2020)
#define IID_MEM_USED		2000
#define IID_MEM_FREE		2001
#define IID_MEM_CACHED		2002
#define IID_MEM_BUFFERS		2003
#define IID_SWAP_USED		2010
#define IID_SWAP_FREE		2011

#define IID_IS4MEM(id)		((id) >= 2000 && (id) < 2020)

// [2020,2030)
#define IID_LOADAVG_1		2020
#define IID_LOADAVG_5		2021
#define IID_LOADAVG_15		2022

#define IID_IS4LOADAVG(id)	((id) >= 2020 && (id) < 2030)

// [2100, 3000)
#define NET_T_IN_BYTES		0
#define NET_T_IN_PKTS		1
#define NET_T_OUT_BYTES		2
#define NET_T_OUT_PKTS		3
#define NET_T_CONN_ESTABLISHED	4
#define NET_T_CONN_WAIT		5

#define IID_NET_ALL		99

#define IID_NET(no,type)	(2100+(no)*10+(type))
#define IID_IS4NET(id)		((id) >= 2100 && (id) < 3000)
#define IID2NETNO(id)		(((id) - 2100) / 10)

// [3000,4000)
#define DISK_T_R_CALLS		0	/* number of issued reads */
#define DISK_T_R_MERGED		1	/* number of reads merged */
#define DISK_T_R_BYTES		2	/* number of data read */
#define DISK_T_R_TIME		3	/* number of milliseconds spent reading */
#define DISK_T_W_CALLS		4
#define DISK_T_W_MERGED		5
#define DISK_T_W_BYTES		6
#define DISK_T_W_TIME		7
#define DISK_T_Q_SIZE		8
#define DISK_T_UTILS		9

#define IID_DISK_ALL		99

#define IID_DISK(no,type)	(3000+(no)*10+(type))
#define IID_IS4DISK(id)		((id) >= 3000 && (id) < 4000)
#define IID2DISKNO(id)		(((id) - 3000) / 10)

#endif /* __STAT_SYSTEM_IIDS__H */
