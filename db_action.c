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
		printf("erro query [%s]\n", sqlite3_errmsg(conn));	
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
		int n = rand() % 8;
		// 2012000014 key
		time(&timer);
	        tm_info = localtime(&timer);
		strftime(ts, 26, "%Y-%m-%d %H:%M:%S", tm_info);
		snprintf(query,1024,"select * from sv where key = %d%d%d%d%d%d%d%d%d%d limit 1",
				rand()%8+1,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9);
		// snprintf(query,1024,"select * from sv limit 1");
		gettimeofday(&start, NULL);
		db_query(conn, query);
		gettimeofday(&stop, NULL);
		//printf("done\n");
		printf("[%s][%d][%s]\n",ts, (int)(stop.tv_usec - start.tv_usec) ,query);
		usleep(100000);
		//usleep(1000);
	}
}


