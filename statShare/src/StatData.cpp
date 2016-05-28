/* StatData.cpp
 * Copyright@ Beyondy.c.w 2002-2020
**/
#include <errno.h>

#include "MemoryBuffer.h"
#include "StatErrno.h"
#include "StatData.h"

namespace helper {
//
// some helper functions
// 
int parseFrom(stat_ip_t& hip, MemoryBuffer *msg)
{
	if (msg->readUint8(hip.ver) < 0) return -1;
	if (hip.ver == 4) return msg->readUint32(hip.ip.ip4);
	return -1;
}

int encodeTo(MemoryBuffer *msg, const stat_ip_t& hip)
{
	if (msg->writeUint8(hip.ver) < 0) return -1;
	if (hip.ver == 4) return msg->writeUint32(hip.ip.ip4);
	return -1;
}

int parseFrom(stat_id_t& sid, MemoryBuffer *msg)
{
	if (msg->readUint16(sid.pid) < 0
		|| msg->readUint16(sid.mid) < 0
			|| msg->readUint16(sid.iid) < 0)
		return -1;
	return 0;
}

int encodeTo(MemoryBuffer *msg, const stat_id_t& sid)
{
	if (msg->writeUint16(sid.pid) < 0
		|| msg->writeUint16(sid.mid) < 0
			|| msg->writeUint16(sid.iid) < 0)
		return -1;
	return 0;
}

static inline int parseFrom(stat_result_t& result, MemoryBuffer *msg)
{
	if (msg->readUint32(result.rsptime) < 0
		|| msg->readUint32(result.isize) < 0
			|| msg->readUint32(result.osize) < 0)
		return -1;
	return 0;
}

static inline int encodeTo(MemoryBuffer *msg, const stat_result_t& result)
{
	if (msg->writeUint32(result.rsptime) < 0
		|| msg->writeUint32(result.isize) < 0
			|| msg->writeUint32(result.osize) <  0)
		return -1;
	return 0;
}

static inline int parseFrom(stat_mresult_t& mresult, MemoryBuffer *msg)
{
	if (msg->readUint32(mresult.count) < 0 || msg->readUint32(mresult.rsptime) < 0
		|| msg->readUint32(mresult.isize) < 0
			|| msg->readUint32(mresult.osize) <  0)
		return -1;
	return 0;
}

static inline int encodeTo(MemoryBuffer *msg, const stat_mresult_t& mresult)
{
	if (msg->writeUint32(mresult.count) < 0 || msg->writeUint32(mresult.rsptime) < 0
		|| msg->writeUint32(mresult.isize) < 0
			|| msg->writeUint32(mresult.osize) < 0)
		return -1;
	return 0;
}

//
// end of helper
//
}; /* helper */

int StatItemGauge::parseFrom(MemoryBuffer *msg)
{
	long savedRptr = msg->getRptr();

	if (msg->readInt64(timestamp) < 0 || helper::parseFrom(hip, msg) < 0
		|| helper::parseFrom(sid, msg) < 0 || msg->readUint8(gtype) < 0
			|| msg->readInt64(gval) < 0) {
		msg->setRptr(savedRptr);
		errno = E_STAT_DATA_PARSE_GAUGE_NOT_ENOUGH;
		return -1;
	}

	return 0;
}

int StatItemGauge::encodeTo(MemoryBuffer *msg) const
{
	long savedWptr = msg->getWptr();	

	if (msg->writeInt64(timestamp) < 0 || helper::encodeTo(msg, hip) < 0
		|| helper::encodeTo(msg, sid) < 0 || msg->writeUint8(gtype) < 0
			|| msg->writeInt64(gval) < 0) {
		msg->setWptr(savedWptr);
		errno = E_STAT_DATA_ENCODE_GAUGE_NOT_ENOUGH;
		return -1;
	}

	return 0;
}

int StatMergedGauge::parseFrom(MemoryBuffer *msg)
{
	long savedRptr = msg->getRptr();

	if (msg->readInt64(timestamp) < 0 || helper::parseFrom(hip, msg) < 0
		|| helper::parseFrom(sid, msg) < 0 || msg->readUint8(ftype) < 0 
		|| msg->readUint8(freqs) < 0 || msg->readUint8(gtype) < 0
			|| msg->readInt64(gval) < 0) {
		msg->setRptr(savedRptr);
		errno = E_STAT_DATA_PARSE_GAUGE_NOT_ENOUGH;
		return -1;
	}

	return 0;
}

int StatMergedGauge::encodeTo(MemoryBuffer *msg) const
{
	long savedWptr = msg->getWptr();	

	if (msg->writeInt64(timestamp) < 0 || helper::encodeTo(msg, hip) < 0
		|| helper::encodeTo(msg, sid) < 0 || msg->writeUint8(ftype) < 0
		|| msg->writeUint8(freqs) < 0 || msg->writeUint8(gtype) < 0
			|| msg->writeInt64(gval) < 0) {
		msg->setWptr(savedWptr);
		errno = E_STAT_DATA_ENCODE_GAUGE_NOT_ENOUGH;
		return -1;
	}

	return 0;
}

int StatItemLcall::parseFrom(MemoryBuffer *msg)
{
	long savedRptr = msg->getRptr();

	if (msg->readInt64(timestamp) < 0 || helper::parseFrom(hip, msg) < 0
		|| helper::parseFrom(sid, msg) < 0 || msg->readInt32(retcode) < 0
			|| helper::parseFrom(result, msg) < 0 || msg->readString(key, sizeof key)
				|| msg->readString(extra, sizeof extra) < 0) {
		msg->setRptr(savedRptr);
		errno = E_STAT_DATA_ITEM_LCALL_PARSE_FAILED;
		return -1;
	}

	return 0;
}

int StatItemLcall::encodeTo(MemoryBuffer *msg) const
{
	long savedWptr = msg->getWptr();	

	if (msg->writeInt64(timestamp) < 0 || helper::encodeTo(msg, hip) < 0
		|| helper::encodeTo(msg, sid) || msg->writeInt32(retcode) < 0
			|| helper::encodeTo(msg, result) < 0 || msg->writeString(key)
				|| msg->writeString(extra) < 0) {
		msg->setWptr(savedWptr);
		errno = E_STAT_DATA_ITEM_LCALL_ENCODE_FAILED;
		return -1;
	}

	return 0;
}

int StatMergedLcall::parseFrom(MemoryBuffer *msg)
{
	long savedRptr = msg->getRptr();
	uint16_t rcnt = 0;

	if (msg->readInt64(timestamp) < 0 || helper::parseFrom(hip, msg) < 0
		|| helper::parseFrom(sid, msg) < 0 || msg->readUint8(ftype) < 0
			|| msg->readUint8(freqs) || msg->readUint16(rcnt) < 0)
		goto restore_rptr;

	for (int i = 0; i < (int)rcnt; ++i) {
		int32_t retcode;
		if (msg->readInt32(retcode) < 0 || helper::parseFrom(rets[retcode], msg) < 0)
			goto restore_rptr;
	}

	return 0;

restore_rptr:
	msg->setRptr(savedRptr);
	errno = E_STAT_DATA_MERGED_LCALL_PARSE_FAILED;
	return -1;
}

int StatMergedLcall::encodeTo(MemoryBuffer *msg) const
{
	long savedWptr = msg->getWptr();
	int rcnt = rets.size(), i = 0;
	if (rcnt < 0) rcnt = 0; else if (rcnt >= 0xffff) rcnt = 0xffff;

	if (msg->writeInt64(this->timestamp) < 0 || helper::encodeTo(msg, hip) < 0
		|| helper::encodeTo(msg, sid) < 0 || msg->writeUint8(ftype) < 0
			|| msg->writeUint8(freqs) || msg->writeUint16((uint16_t)rcnt) < 0)
		goto restore_wptr;

	for (const_iterator iter = rets.begin(); iter != rets.end(); ++iter) {
		if (msg->writeInt32(iter->first) < 0 || helper::encodeTo(msg, iter->second) < 0)
			goto restore_wptr;
		if (++i >= rcnt) break;
	}

	return 0;
restore_wptr:
	msg->setWptr(savedWptr);
	errno = E_STAT_DATA_MERGED_LCALL_ENCODE_FAILED;
	return -1;
}

int StatItemRcall::parseFrom(MemoryBuffer *msg)
{
	long savedRptr = msg->getRptr();

	if (msg->readInt64(timestamp) < 0 || helper::parseFrom(src_hip, msg) < 0
		|| helper::parseFrom(src_sid, msg) < 0 || helper::parseFrom(dst_hip, msg) < 0
		|| helper::parseFrom(dst_sid, msg) < 0 || msg->readInt32(retcode) < 0
			|| helper::parseFrom(result, msg) < 0 || msg->readString(key, sizeof key)
			|| msg->readString(extra, sizeof extra) < 0) {
		msg->setRptr(savedRptr);
		errno = E_STAT_DATA_ITEM_RCALL_PARSE_FAILED;
		return -1;
	}

	return 0;
}

int StatItemRcall::encodeTo(MemoryBuffer *msg) const
{
	long savedWptr = msg->getWptr();

	if (msg->writeInt64(timestamp) || helper::encodeTo(msg, src_hip) < 0
		|| helper::encodeTo(msg, src_sid) < 0 || helper::encodeTo(msg, dst_hip) < 0
		|| helper::encodeTo(msg, dst_sid) < 0 || msg->writeInt32(retcode) < 0
			|| helper::encodeTo(msg, result) < 0 || msg->writeString(key) < 0
			|| msg->writeString(extra) < 0) {
		msg->setWptr(savedWptr);
		errno = E_STAT_DATA_ITEM_RCALL_ENCODE_FAILED;
		return -1;
	}

	return 0;
}

int StatMergedRcall::parseFrom(MemoryBuffer *msg)
{
	long savedRptr = msg->getRptr();
	uint16_t rcnt = 0;

	if (msg->readInt64(timestamp) < 0 || helper::parseFrom(src_hip, msg) < 0
		|| helper::parseFrom(src_sid, msg) < 0 || helper::parseFrom(dst_hip, msg) < 0
		|| helper::parseFrom(dst_sid, msg) < 0 || msg->readUint8(ftype) < 0
			|| msg->readUint8(freqs) || msg->readUint16(rcnt) < 0)
		goto restore_rptr;

	for (int i = 0; i < (int)rcnt; ++i) {
		int32_t retcode;
		if (msg->readInt32(retcode) < 0 || helper::parseFrom(rets[retcode], msg) < 0)
			goto restore_rptr;
	}

	return 0;

restore_rptr:
	errno = E_STAT_DATA_MERGED_RCALL_PARSE_FAILED;
	msg->setRptr(savedRptr);
	return -1;
}

int StatMergedRcall::encodeTo(MemoryBuffer *msg) const
{
	long savedWptr = msg->getWptr();

	int rcnt = rets.size(), i = 0;
	if (rcnt < 0) rcnt = 0; else if (rcnt >= 0xffff) rcnt = 0xffff;

	if (msg->writeInt64(timestamp) || helper::encodeTo(msg, src_hip) < 0
		|| helper::encodeTo(msg, src_sid) < 0 || helper::encodeTo(msg, dst_hip) < 0
		|| helper::encodeTo(msg, dst_sid) < 0 || msg->writeUint8(ftype) < 0
			|| msg->writeUint8(freqs) || msg->writeUint16((uint16_t)rcnt) < 0)
		goto restore_wptr;	

	for (const_iterator iter = rets.begin(); iter != rets.end(); ++iter) {
		if (msg->writeInt32(iter->first) < 0 || helper::encodeTo(msg, iter->second) < 0)
			goto restore_wptr;	
		if (++i >= rcnt) break;
	}

	return 0;
restore_wptr:
	msg->setWptr(savedWptr);
	errno = E_STAT_DATA_MERGED_RCALL_ENCODE_FAILED;
	return -1;
}

