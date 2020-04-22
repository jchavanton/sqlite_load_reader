#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>

static int debug=0;
static int max_read=100000;

/**
 * Returns the current resident set size (physical memory use) measured
 * in Kbytes, or zero if the value cannot be determined on this OS.
 */
uint32_t get_mem_usage(void) {
	long rss = 0L;
	FILE* fp = NULL;
	if ((fp = fopen( "/proc/self/statm", "r" )) == NULL)
		return (size_t)0L;
	if (fscanf(fp, "%*s%ld", &rss ) != 1) {
	    fclose(fp);
	    return (size_t)0L;
	}
	fclose(fp);
	return (((size_t)rss * (size_t)sysconf( _SC_PAGESIZE))/1024);
}

long time_diff(struct timeval x , struct timeval y) {
	long x_ms , y_ms , diff;
	x_ms = (long)x.tv_sec*1000000 + (long)x.tv_usec;
	y_ms = (long)y.tv_sec*1000000 + (long)y.tv_usec;
	diff = (long)y_ms - (long)x_ms;
	return diff;
}

void db_query (sqlite3 *conn, const char *query, int print) {
	int i;
	sqlite3_stmt *stmt;

	int rc = sqlite3_prepare_v2(conn, query, strlen(query), &stmt, NULL);
	if (rc != SQLITE_OK) {
		printf("error prepare query [%s]\n", sqlite3_errmsg(conn));
		// error prepare query [database is locked]
		return;
	}	
	while (1) {
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE) {
			goto finalize;
		} else if (rc != SQLITE_ROW) {
			printf("sqlite3_step failed: %s\n", sqlite3_errmsg(conn));
			goto finalize;
		} else {
			/* get column types */
			int cols = sqlite3_column_count(stmt);
			int c=0;
			if (print) {
				// printf("sqlite3_column_count: %d\n", cols);
				for (int i=0; i<cols; i++)
					printf("%s: %s\n", sqlite3_column_name(stmt, i), sqlite3_column_text(stmt, i));
			}
			// sqlite3_column_name(stmt, c);
			// sqlite3_column_decltype(stmt, c);
			// sqlite3_column_type(stmt, c)
		}
	}
	// sqlite3_bind_text
finalize:
	if (stmt != NULL) {
		rc = sqlite3_finalize(stmt);
		if (rc != SQLITE_OK)
		printf("finalize failed: %s\n",sqlite3_errmsg(conn));
		stmt = NULL;
	}
}

sqlite3 * db_connect (const char *db_name) {
	sqlite3 *conn;;
	int flags = SQLITE_OPEN_READONLY;
	int rc = sqlite3_open_v2(db_name, &conn, flags, NULL);

	if (rc != SQLITE_OK) {
		printf("error opening [%s]\n", db_name);
		return NULL;
	} else {
		printf("[%s] connected\n", db_name);
	}
	// PRAGMA journal_mode=WAL;
	db_query(conn, "PRAGMA journal_mode=WAL", 1);
	db_query(conn, "PRAGMA CACHE_SIZE", 1);
	//db_query(conn, "PRAGMA synchronous=NORMAL", 1);
	db_query(conn, "PRAGMA synchronous", 1);
	return conn;
}

typedef struct stats {
        int max_us;     // max explased time (slowest query)
	int max_idx;    // index of the slowest query
	int slow;       // slow queries
	char num[10];
	float average;  // average query time
	int32_t count;
} stats_t;

void update_stats(stats_t* stats, int elapsed_us, char *num) {
	stats->count++;
	if (stats->max_us < elapsed_us) {
		stats->max_us = elapsed_us;
		stats->max_idx = stats->count;
		strcpy(stats->num, num);
	}
	if (elapsed_us >= 100000)
		stats->slow++;
	float delta = elapsed_us - stats->average;
	stats->average += delta/stats->count;
}

int work(int pid) {
	const char *db = "/opt/flowroute/neustardb/neustar.sqlite";
	char query[1024];
	sqlite3 *conn = db_connect(db);
	if (!conn) {
		printf("error connecting to [%s]\n", db);
		return -1;
	}

	time_t timer;
	char ts[26];
	struct tm* tm_info;
	struct timeval stop, start;
	printf("worker started[%d]\n", pid);
	stats_t stats;
	memset(&stats, 0, sizeof(stats_t));

	uint32_t kbytes = get_mem_usage();
	printf("mem[%"PRIu32"KB/%"PRIu32"MB]\n", kbytes, kbytes/1024);
	char num[10];
	while (stats.count < max_read) {
		// int n = rand() % 8;
		// 2012000014 key
		time(&timer);
		tm_info = localtime(&timer);
		strftime(ts, 26, "%Y-%m-%d %H:%M:%S", tm_info);

		snprintf(num,11,"%d%d%d%d%d%d%d%d%d%d",rand()%8+1,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9);
		snprintf(query,1024,"SELECT cast(lrn as text) as lrn, spid, 1 as ord FROM sv WHERE key = %.10s"
		" UNION SELECT cast(lrn as text) as lrn, spid, 2 as ord FROM block   WHERE key = %.10s"
		" UNION SELECT cast(lrn as text) as lrn, spid, 3 as ord FROM npanxxx WHERE key = %.7s"
		" UNION SELECT cast(lrn as text) as lrn, spid, 4 as ord FROM npanxx  WHERE key = %.6s"
		" ORDER BY ord ASC LIMIT 1", num, num, num, num);

		// printf("\n%s\n", query);
		// snprintf(query,1024,"select * from sv where key = %s limit 1", num);
		// snprintf(query,1024,"select * from sv limit 1");
		gettimeofday(&start, NULL);
		db_query(conn, query, debug);
		gettimeofday(&stop, NULL);
		uint32_t sleep_us = 100 * (rand()%1000);
		// printf("pid[%d]count[%d][%d][%s][%d][%s]\n",pid,stats.count,sleep_us,ts,(int)(stop.tv_usec-start.tv_usec),query);
		// uint32_t elapsed_us = stop.tv_usec-start.tv_usec;
		long elapsed_us  = time_diff(start, stop);
		update_stats(&stats, elapsed_us, num);

		if (debug)
			printf("pid[%d]count[%d]number[%s][%ldus]\n",pid,stats.count,num,elapsed_us);

		// usleep(1000000);
		// usleep(sleep_us);
	}
	kbytes = get_mem_usage();
	// printf("%s", query);
	printf("pid[%d]done[%d]slow[%d]max_ms[%dms][%d|%s]avg_ms[%dus]mem[%"PRIu32"KB/%"PRIu32"MB]\n",
		pid,stats.count,stats.slow,(uint32_t)(stats.max_us/1000),stats.max_idx,stats.num,(uint32_t)(stats.average), kbytes, kbytes/1024);
	fflush(stdout);
	exit(stats.count);
}

int main() {
	int max_process = 10;
	pid_t pid[max_process];
	int p;
	struct timeval stop, start;
	printf(">>>> starting workers: %d\n", max_process);

	gettimeofday(&start, NULL);
	
	for (p=0;p<max_process;p++) {
		pid[p] = fork();
		srand(time(NULL) ^ (getpid()<<16));
		if (pid[p] == 0) {
			printf("child pid[%d][%d]\n", pid[p], p);
			work(getpid());
		}
	}
	for (p=0;p<max_process;p++) {
		waitpid(pid[p], NULL, 0);
	}
	gettimeofday(&stop, NULL);
	long elapsed_us  = time_diff(start, stop);
	printf(">>> done, elapsed_ms[%d]\n", (uint32_t)(elapsed_us/1024));
}

