
#include <sched.h>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "test.h"

void thread_t::init(uint64_t thd_id, workload * workload) {
	_thd_id = thd_id;
	_wl = workload;
	srand48_r((_thd_id + 1) * get_sys_clock(), &buffer);
	_abort_buffer_size = ABORT_BUFFER_SIZE;
	_abort_buffer = (AbortBufferEntry *) _mm_malloc(sizeof(AbortBufferEntry) * _abort_buffer_size, 64);
	for (int i = 0; i < _abort_buffer_size; i++)
		_abort_buffer[i].query = NULL;
	_abort_buffer_empty_slots = _abort_buffer_size;
	_abort_buffer_enable = (g_params["abort_buffer_enable"] == "true");
  sample_read = sample_part = sample_trans = in_prog = sample_conf = false;
  mark_cntr = sample_cntr = /*home_mark_cntr = home_sample_cntr =*/ 0;
  part_num = 0;
  next_lock = -1;
  /*home_mark_state = */mark_state /*= part_state*/ = true;
  report_info = {0,0,0,0,0,0,0,0,0,0};
}

uint64_t thread_t::get_thd_id() { return _thd_id; }
uint64_t thread_t::get_host_cid() {	return _host_cid; }
void thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t thread_t::get_cur_cid() { return _cur_cid; }
void thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

RC thread_t::run() {
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );

	set_affinity(get_thd_id());

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	txn_man * m_txn;
	rc = _wl->get_txn_man(m_txn, this);
	assert (rc == RCOK);
	glob_manager->set_txn_man(m_txn);

	base_query * m_query = NULL;
	uint64_t thd_txn_id = 0;
	UInt64 txn_cnt = 0;
  uint64_t count = 1;

	while (true) {
    sample_conf = false;
		ts_t starttime = get_sys_clock();
		if (WORKLOAD != TEST) {
			int trial = 0;
			if (_abort_buffer_enable) {
				m_query = NULL;
				while (trial < 2) {
					ts_t curr_time = get_sys_clock();
					ts_t min_ready_time = UINT64_MAX;
					if (_abort_buffer_empty_slots < _abort_buffer_size) {
						for (int i = 0; i < _abort_buffer_size; i++) {
							if (_abort_buffer[i].query != NULL && curr_time > _abort_buffer[i].ready_time) {
								m_query = _abort_buffer[i].query;
								_abort_buffer[i].query = NULL;
								_abort_buffer_empty_slots ++;
								break;
							} else if (_abort_buffer_empty_slots == 0
									  && _abort_buffer[i].ready_time < min_ready_time)
								min_ready_time = _abort_buffer[i].ready_time;
						}
					}
					if (m_query == NULL && _abort_buffer_empty_slots == 0) {
						assert(trial == 0);
						M_ASSERT(min_ready_time >= curr_time, "min_ready_time=%ld, curr_time=%ld\n", min_ready_time, curr_time);
						usleep(min_ready_time - curr_time);
					}
					else if (m_query == NULL) {
						m_query = query_queue->get_next_query( _thd_id );
            if (count % READRATE == 0)
              sample_read = true;
            if (count % TRANSRATE == 0)
              sample_trans = true;
            if (count % PARTRATE == 0) {
              sample_part = true;
            }
            sample_conf = true;
            count++;
					#if CC_ALG == WAIT_DIE
						m_txn->set_ts(get_next_ts());
					#endif
					}
					if (m_query != NULL)
						break;
				}
			} else {
				if (rc == RCOK) {
					m_query = query_queue->get_next_query( _thd_id );
          if (count % READRATE == 0)
            sample_read = true;
          if (count % TRANSRATE == 0)
            sample_trans = true;
          if (count % PARTRATE == 0) {
            sample_part = true;
          }
          sample_conf = true;
          count++;
        }
			}
		}
		INC_STATS(_thd_id, time_query, get_sys_clock() - starttime);
		m_txn->abort_cnt = 0;
    #if CC_ALG == VLL
		  _wl->get_txn_man(m_txn, this);
    #endif
		m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
		thd_txn_id ++;
    /*
    if (thd_txn_id % READRATE == 0)
      sample_read = true;
    if (thd_txn_id % TRANSRATE == 0)
      sample_trans = true;
    if (thd_txn_id % PARTRATE == 0) {
      sample_part = true;
      }*/

    if (sample_part) {
      if (next_lock >= static_cast<int64_t>(part_num)) {
        for (int i = part_num - 1; i >= 0; i--) {
          part_con[part_to_access[i]] = false;
        }
        next_lock = -1;
      }
      else {
        if (next_lock == -1) {
          next_lock = 0;
          part_num = m_query->part_num;
          for (unsigned int i = 0; i < part_num; i++) {
            part_to_access[i] = m_query->part_to_access[i];
          }
          for (int i = part_num - 1; i > 0; i--)
            for (int j = 0; j < i; j ++)
              if (part_to_access[j] > part_to_access[j + 1]) {
                uint64_t tmp = part_to_access[j];
                part_to_access[j] = part_to_access[j + 1];
                part_to_access[j + 1] = tmp;
              }
          report_info.part_success += part_num;
        }
        while (next_lock < static_cast<int64_t>(part_num)) {
          if (!ATOM_CAS(part_con[part_to_access[next_lock]], false, true)) {
              report_info.part_attempt++;
              break;
          }
          next_lock++;
        }
      }
    }
    /*

      memcpy(part_to_access, m_query->part_to_access, m_query->part_num * sizeof(uint64_t));
      part_num = m_query->part_num;
      for (int i = part_num - 1; i > 0; i--)
        for (int j = 0; j < i; j ++)
          if (part_to_access[j] > part_to_access[j + 1]) {
            uint64_t tmp = part_to_access[j];
            part_to_access[j] = part_to_access[j + 1];
pfy            part_to_access[j + 1] = tmp;
          }
      next_lock = -1;
    }
    if (next_lock = -1) {
      next_lock = 0;
        in_prog = true;
        memcpy(part_to_access, m_query->part_to_access, m_query->part_num * sizeof(uint64_t));
        part_num = m_query->part_num;
        for (int i = part_num - 1; i > 0; i--)
          for (int j = 0; j < i; j ++)
            if (part_to_access[j] > part_to_access[j + 1]) {
              uint64_t tmp = part_to_access[j];
              part_to_access[j] = part_to_access[j + 1];
              part_to_access[j + 1] = tmp;
            }
        next_lock = 0;
      }
      while (next_lock < part_num) {
        __sync_synchronize();
        if (ATOM_CAS(part_con[part_to_access[next_lock]], false, true)) {
          report_info.part_success++;
          next_lock++;
        }
        else {
          report_info.part_attempt++;
          break;
        }
      }
      if (next_lock >= part_num)
        in_prog = false;
    }
    done:*/

		if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
				|| CC_ALG == MVCC
				|| CC_ALG == HEKATON
				|| CC_ALG == TIMESTAMP)
			m_txn->set_ts(get_next_ts());

		rc = RCOK;
#if CC_ALG == HSTORE
		if (WORKLOAD == TEST) {
			uint64_t part_to_access[1] = {0};
			rc = part_lock_man.lock(m_txn, &part_to_access[0], 1);
		} else
			rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
		vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
		glob_manager->add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
		// In the original OCC paper, start_ts only reads the current ts without advancing it.
		// But we advance the global ts here to simplify the implementation. However, the final
		// results should be the same.
		m_txn->start_ts = get_next_ts();
#endif
		if (rc == RCOK)
		{
#if CC_ALG != VLL
			if (WORKLOAD == TEST)
				rc = runTest(m_txn);
			else
				rc = m_txn->run_txn(this, m_query);
#endif
      sample_read = sample_trans = sample_part = false;
#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				part_lock_man.unlock(m_txn, &part_to_access[0], 1);
			} else
				part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#endif
		}
		if (rc == Abort) {
			uint64_t penalty = 0;
			if (ABORT_PENALTY != 0)  {
				double r;
				drand48_r(&buffer, &r);
				penalty = r * ABORT_PENALTY;
			}
			if (!_abort_buffer_enable)
				usleep(penalty / 1000);
			else {
				assert(_abort_buffer_empty_slots > 0);
				for (int i = 0; i < _abort_buffer_size; i ++) {
					if (_abort_buffer[i].query == NULL) {
						_abort_buffer[i].query = m_query;
						_abort_buffer[i].ready_time = get_sys_clock() + penalty;
						_abort_buffer_empty_slots --;
						break;
					}
				}
			}
		}

		ts_t endtime = get_sys_clock();
		uint64_t timespan = endtime - starttime;
		INC_STATS(get_thd_id(), run_time, timespan);
		INC_STATS(get_thd_id(), latency, timespan);
		//stats.add_lat(get_thd_id(), timespan);
		if (rc == RCOK) {
			INC_STATS(get_thd_id(), txn_cnt, 1);
			stats.commit(get_thd_id());
			txn_cnt ++;
		} else if (rc == Abort) {
			INC_STATS(get_thd_id(), time_abort, timespan);
			INC_STATS(get_thd_id(), abort_cnt, 1);
			stats.abort(get_thd_id());
			m_txn->abort_cnt ++;
		}

		if (rc == FINISH)
			return rc;
		if (!warmup_finish && txn_cnt >= WARMUP / g_thread_cnt)
		{
			stats.clear( get_thd_id() );
			return FINISH;
		}

		if (warmup_finish && txn_cnt >= MAX_TXN_PER_PART) {
			assert(txn_cnt == MAX_TXN_PER_PART);
	        if( !ATOM_CAS(_wl->sim_done, false, true) )
				assert( _wl->sim_done);
	    }
	    if (_wl->sim_done) {
   		    return FINISH;
   		}
	}
	assert(false);
}


ts_t
thread_t::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager->get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager->get_ts(get_thd_id());
		return _curr_ts;
	}
}

RC thread_t::runTest(txn_man * txn)
{
	RC rc = RCOK;
	if (g_test_case == READ_WRITE) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
#if CC_ALG == OCC
		txn->start_ts = get_next_ts();
#endif
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 1);
		printf("READ_WRITE TEST PASSED\n");
		return FINISH;
	}
	else if (g_test_case == CONFLICT) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
		if (rc == RCOK)
			return FINISH;
		else
			return rc;
	}
	assert(false);
	return RCOK;
}

void thread_t::mark_row(row_t * row, uint64_t part_id) {
  if (sample_conf && mark_state && !row->mark[_thd_id]) {
    row->mark[_thd_id] = true;
    rec_set[mark_cntr] = row;
    mark_cntr++;
    mark_cntr %= MAXMARK;
    //printf("%lu ", mark_cntr);
    if (mark_cntr == 0) {
      mark_state = false;
      sample_cntr = 0;
    }
  } else if (!mark_state) {
    if (sample_cntr++ % RECORDRATE == 0) {
      uint64_t *cont_cntr;
      uint64_t *access_cntr;
      if (part_id != _thd_id) {
        cont_cntr = &report_info.cont_cntr;
        access_cntr = &report_info.access_cntr;
      }
      else {
        cont_cntr = &report_info.home_cont;
        access_cntr = &report_info.home_access;
      }

      for (unsigned int i = 0; i < _thd_id; i++) {
        if (row->mark[i])
          (*cont_cntr)++;
      }
      for (auto i = _thd_id + 1; i < g_thread_cnt; i++) {
        if (row->mark[i])
          (*cont_cntr)++;
      }
      (*access_cntr)++;
      //printf("%ld %ld\n", report_info.cont_cntr, report_info.access_cntr);
      mark_cntr++;
      mark_cntr %= MAXDETECT;
      if (mark_cntr == 0) {
        mark_state = true;
        for (auto i = 0; i < MAXMARK; i++) {
          rec_set[i]->mark[_thd_id] = false;
        }
      }
    }
  }
}
/*
void thread_t::mark_part(uint64_t part_id) {
  if (part_state && !part_con[part_id][_thd_id]) {
    part_con[part_id][_thd_id] = true;
    part_cntr++;
    part_cntr %= MAXPART;
    //printf("%lu ", mark_cntr);
    if (part_cntr == 0) {
      part_state = false;
      sample_cntr = 0;
    }
  } else if (!mark_state) {
    if (sample_cntr++ % RECORDRATE == 0) {
      for (unsigned int i = 0; i < _thd_id; i++) {
        if (row->mark[i])
          report_info.cont_cntr++;
      }
      for (auto i = _thd_id + 1; i < g_thread_cnt; i++) {
        if (row->mark[i])
          report_info.cont_cntr ++;
      }
      report_info.access_cntr++;
      //printf("%ld %ld\n", report_info.cont_cntr, report_info.access_cntr);
      mark_cntr++;
      mark_cntr %= MAXDETECT;
      if (mark_cntr == 0) {
        mark_state = true;
        for (auto i = 0; i < MAXMARK; i++) {
          rec_set[i]->mark[_thd_id] = false;
        }
      }
    }
  }
}


void thread_t::home_mark_row(row_t * row) {
  if (sample_conf && home_mark_state && !row->home_mark[_thd_id]) {
    row->home_mark[_thd_id] = true;
    home_rec_set[home_mark_cntr] = row;
    home_mark_cntr++;
    home_mark_cntr %= MAXMARK;
    if (home_mark_cntr == 0) {
      home_mark_state = false;
      home_sample_cntr = 0;
    }
  } else if (!home_mark_state) {
    if (home_sample_cntr++ % RECORDRATE == 0) {
      for (unsigned int i = 0; i < _thd_id; i++) {
        if (row->home_mark[i])
          report_info.home_cont++;
      }
      for (auto i = _thd_id + 1; i < g_thread_cnt; i++) {
        if (row->home_mark[i])
          report_info.home_cont++;
      }
      report_info.home_access ++;
      home_mark_cntr++;
      home_mark_cntr %= MAXDETECT;
      if (home_mark_cntr == 0) {
        home_mark_state = true;
        for (auto i = 0; i < MAXMARK; i++) {
          home_rec_set[i]->home_mark[_thd_id] = false;
        }
      }
    }
  }
}
*/

void thread_t::sample_row(access_t type, size_t table_size) {
  if (sample_read) {
    if (type == RD || type == SCAN) {
      report_info.read_cnt++;
    } else {
      report_info.write_cnt++;
    }
  }
}
