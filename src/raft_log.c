/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief ADT for managing Raft log entries (aka entries)
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"
#include "raft_log.h"

#define INITIAL_CAPACITY 10
#define REL_POS(_i, _s) ((_i) % (_s))

typedef struct
{
    /* size of array */
    int size;

    /* the amount of elements in the array */
    int count;

    /* Log's absolute starting index */
    int front;
    
    /* Log's absolute ending index */
    int back;

    raft_entry_t* entries;

    /* callbacks */
    raft_cbs_t *cb;
    void* raft;
} log_private_t;

static raft_entry_t* __get_ety(log_private_t* me, const int idx)
{
    return &me->entries[REL_POS(idx, me->size)];
}

static void __ensurecapacity(log_private_t * me, int count)
{
    int i;
    raft_entry_t *temp;

    if (me->count + count <= me->size)
        return;

    while (me->size < me->count + count)
    {
        temp = (raft_entry_t*)calloc(1, sizeof(raft_entry_t) * me->size * 2);

        for (i = me->front; i < me->back; i++)
        {
            memcpy(
                &temp[REL_POS(i, 2*me->size)], 
                &me->entries[REL_POS(i, me->size)], 
                sizeof(raft_entry_t));
        }

        /* clean up old entries */
        free(me->entries);

        me->size *= 2;
        me->entries = temp;
    }
}

#if 1
void log_load_from_snapshot(log_t *me_, int idx, raft_entry_t *entry)
{
    log_private_t* me = (log_private_t*)me_;

    /* idx starts at 1 */
    if (idx <= 0)
    {
        log_clear(me_);
        return;
    }

    me->front = idx - 1;
    me->back  = idx;
    memcpy(__get_ety(me, me->front), entry, sizeof(raft_entry_t));
    me->count = 1;
}
#endif

log_t* log_new()
{
    log_private_t* me = (log_private_t*)calloc(1, sizeof(log_private_t));
    if (!me)
        return NULL;
    me->size = INITIAL_CAPACITY;
    log_clear((log_t*)me);
    me->entries = (raft_entry_t*)calloc(1, sizeof(raft_entry_t) * me->size);
    return (log_t*)me;
}

void log_set_callbacks(log_t* me_, raft_cbs_t* funcs, void* raft)
{
    log_private_t* me = (log_private_t*)me_;

    me->raft = raft;
    me->cb = funcs;
}

void log_clear(log_t* me_)
{
    log_private_t* me = (log_private_t*)me_;
    me->count = me->back = me->front = 0;
}

/** TODO: rename log_append */
int log_append_entry(log_t* me_, raft_entry_t* ety)
{
    log_private_t* me = (log_private_t*)me_;
    int e = 0;

    __ensurecapacity(me, 1);

    if (me->cb && me->cb->log_offer)
    {
        void* ud = raft_get_udata(me->raft);
        e = me->cb->log_offer(me->raft, ud, ety, me->back);
        raft_offer_log(me->raft, ety, me->back);
        if (e == RAFT_ERR_SHUTDOWN)
            return e;
    }

    memcpy(__get_ety(me, me->back), ety, sizeof(raft_entry_t));

    me->count += 1;
    me->back += 1;
    return e;
}

raft_entry_t* log_get_from_idx(log_t* me_, int idx, int *n_etys)
{
    log_private_t* me = (log_private_t*)me_;
    int i, back;

    /* printf("idx %d\n", idx); */

    assert(0 <= idx - 1);

    /* idx starts at 1 */
    idx -= 1;

    if (idx < me->front || me->back <= idx)
    {
        /* printf("%d %d %d\n", idx, me->front, me->back); */
        *n_etys = 0;
        return NULL;
    }

    i = REL_POS(idx, me->size);
    back = REL_POS(me->back, me->size);

    int logs_till_end_of_log;

    if (i < back)
        logs_till_end_of_log = back - i;
    else
        logs_till_end_of_log = me->size - i;

    *n_etys = logs_till_end_of_log;
    return &me->entries[i];
}

raft_entry_t* log_get_at_idx(log_t* me_, int idx)
{
    log_private_t* me = (log_private_t*)me_;

    assert(0 <= idx - 1);

    /* idx starts at 1 */
    idx -= 1;

    if (idx < me->front || me->back <= idx)
        return NULL;

    return __get_ety(me, idx);
}

int log_count(log_t* me_)
{
    return ((log_private_t*)me_)->count;
}

/** TODO: rename to pop */
void log_delete(log_t* me_, int idx)
{
    log_private_t* me = (log_private_t*)me_;

    /* printf("X\n"); */
    
    while (idx <= me->back)
    {
        raft_entry_t* ety = __get_ety(me, me->back - 1);
        if (me->cb && me->cb->log_pop)
            me->cb->log_pop(me->raft, raft_get_udata(me->raft), ety, me->back);
        raft_pop_log(me->raft, ety, me->back);
        me->back--;
        me->count--;
    }
}

void *log_poll(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == log_count(me_))
        return NULL;

    raft_entry_t* ety = __get_ety(me, me->front);

    if (me->cb && me->cb->log_poll)
        me->cb->log_poll(
            me->raft, 
            raft_get_udata(me->raft),
            ety,
            me->front);

    me->front++;
    me->count--;
    return ety;
}

int log_offer_first(log_t * me_, raft_entry_t* ety)
{
    log_private_t* me = (log_private_t*)me_;
    int e = 0;

    /* __ensurecapacity(me, 1); */

    if (me->front == 0)
        return -1;

    /* printf("me_>front %d\n", me->front); */

    me->front -= 1;

    if (me->cb && me->cb->log_offer)
    {
        void* ud = raft_get_udata(me->raft);
        e = me->cb->log_offer(me->raft, ud, ety, me->front);
        if (e == RAFT_ERR_SHUTDOWN)
            return e;
    }

    memcpy(__get_ety(me, me->front), ety, sizeof(raft_entry_t));

    me->count += 1;
    return e;
}

raft_entry_t *log_peektail(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == log_count(me_))
        return NULL;

    return __get_ety(me, me->back - 1);
}

void log_free(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    free(me->entries);
    free(me);
}

int log_get_current_idx(log_t* me_)
{
    return ((log_private_t*)me_)->back;
}
