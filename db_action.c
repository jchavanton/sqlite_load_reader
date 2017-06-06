#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

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
	return conn;
}

void db_query (sqlite3 *conn, const char *query ) {
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
			return;
		} else if (rc != SQLITE_ROW) {
			printf("sqlite3_step failed: %s\n", sqlite3_errmsg(conn));
			return;
		} else {
			/* get column types */
			rc = sqlite3_column_count(stmt);
			int c=0;
			printf("sqlite3_column_count: %d\n", rc);
			// sqlite3_column_name(stmt, c);
			// sqlite3_column_decltype(stmt, c);	
			// sqlite3_column_type(stmt, c)
		}
	}
	// sqlite3_bind_text
	if (stmt != NULL) {
		rc = sqlite3_finalize(stmt);
		if (rc != SQLITE_OK)
		printf("finalize failed: %s\n",sqlite3_errmsg(conn));
		stmt = NULL;
	}
}

int main () {
	const char *db = "/var/lib/docker/volumes/neustardb/_data/neustar.sqlite";
	sqlite3 *conn = db_connect(db);
	char query[1024];
	srand(time(NULL));
	time_t timer;
	char ts[26];
	struct tm* tm_info;
	struct timeval stop, start;

//do stuff

	while (1) {
		// int n = rand() % 8;
		// 2012000014 key
		time(&timer);
		tm_info = localtime(&timer);
		strftime(ts, 26, "%Y-%m-%d %H:%M:%S", tm_info);

		char num[10];
		snprintf(num,11,"%d%d%d%d%d%d%d%d%d%d",rand()%8+1,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9);
		snprintf(query,1024,"SELECT cast(lrn as text) as lrn, spid, 1 as ord FROM sv WHERE key = %.10s"          
	" UNION SELECT cast(lrn as text) as lrn, spid, 2 as ord FROM block   WHERE key = %.10s"
	" UNION SELECT cast(lrn as text) as lrn, spid, 3 as ord FROM npanxxx WHERE key = %.7s"
	" UNION SELECT cast(lrn as text) as lrn, spid, 4 as ord FROM npanxx  WHERE key = %.6s"
	" ORDER BY ord ASC LIMIT 1", num, num, num, num	); 

		// snprintf(query,1024,"select * from sv where key = %s limit 1", num);
		// snprintf(query,1024,"select * from sv limit 1");
		gettimeofday(&start, NULL);
		db_query(conn, query);
		gettimeofday(&stop, NULL);
		//printf("done\n");
		printf("[%s][%d][%s]\n",ts, (int)(stop.tv_usec - start.tv_usec) ,query);
		//usleep(100000);
		// usleep(1000);
	}
}
