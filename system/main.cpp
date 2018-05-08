#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

void * f(void *);
#define BILLION 1000000000UL
thread_t ** m_thds;

void check();

// defined in parser.cpp
void parser(int argc, char * argv[]);

unsigned char part_con[THREAD_CNT] = {};

int main(int argc, char* argv[])
{
	parser(argc, argv);

	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt);
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT)
		dl_detector.init();
	printf("mem_allocator initialized!\n");
	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
			m_wl = new TestWorkload;
			((TestWorkload *)m_wl)->tick();
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("workload initialized!\n");


	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt/* - 1*/];
	m_thds = new thread_t * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
	if (WORKLOAD != TEST)
		query_queue->init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i]->init(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt /* - 1*/; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	// f((void *)(thd_cnt - 1));
  check();
	for (uint32_t i = 0; i < thd_cnt - 1; i++)
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();

	if (WORKLOAD != TEST) {
		printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}

void check() {
  uint64_t count = 0;;
  double pc = 0;
  double tl = 0;;
  double rr = 0;
  double cr = 0;
  double thp = 0;
  double home = 0;
  double tot_count = 0;
  while (count == 0) {
    ts_t starttime = get_sys_clock();
    sleep(5);
    long double part_attempt = 0;
    long double part_success = 0;
    long double read_cnt = 0;
    long double write_cnt = 0;
    long double access_cnt = 0;
    long double trans_cnt = 0;
    long double cont_cntr = 0;
    long double access_cntr = 0;
    long double home_access = 0;
    long double home_cont = 0;
    long double txn_cnt = 0;
    long double run_time = 0;
    //long double last_time = 0;
    count = 0;
    for (uint64_t i = 0; i < g_thread_cnt; i++) {
        part_attempt += m_thds[i]->report_info.part_attempt;
        part_success += m_thds[i]->report_info.part_success;
        read_cnt += m_thds[i]->report_info.read_cnt;
        write_cnt += m_thds[i]->report_info.write_cnt;
        access_cnt += m_thds[i]->report_info.access_cnt;
        trans_cnt += m_thds[i]->report_info.trans_cnt;
        access_cntr += m_thds[i]->report_info.access_cntr;
        cont_cntr += m_thds[i]->report_info.cont_cntr;
        home_access += m_thds[i]->report_info.home_access;
        home_cont += m_thds[i]->report_info.home_cont;
        txn_cnt += stats._stats[i]->txn_cnt;
        //run_time += stats._stats[i]->run_time;
        if (!m_thds[i]->_wl->sim_done) {
      } else {
        count++;
      }
    }
    ts_t endtime = get_sys_clock();
		uint64_t timespan = endtime - starttime;
    //run_time /= (g_thread_cnt - count);
    //if (count == 0) {
      rr += (read_cnt) / (read_cnt + write_cnt);
      tl += access_cnt / trans_cnt;
      pc += part_attempt / part_success;
      cr += cont_cntr / access_cntr;
      home += home_cont / home_access;
      thp += txn_cnt / ((timespan) / BILLION);
      tot_count++;
      last_time = run_time;
      //}
  }
  pc /= tot_count;
  tl /= tot_count;
  rr /= tot_count;
  cr /= tot_count;
  thp /= tot_count;
  home /= tot_count;
  FILE * outf = fopen("pcc-train.out", "a");
  fprintf(outf, "\t%.4lf\t0\t%.4lf\t0\t%.4lf\t%.4lf\t%.4lf", pc, tl, rr, home, cr);
  FILE * temp = fopen("temp.out", "w");
  fprintf(temp, "%f", thp);
}
