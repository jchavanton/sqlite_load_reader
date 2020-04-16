#include <stdio.h>
#include <stdlib.h>
#include <postgresql/libpq-fe.h>
#include <sys/time.h>
#include <time.h>

void do_exit(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

void do_exit2(PGconn *conn, PGresult *res) {
    fprintf(stderr, "%s\n", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    exit(1);
}
// https://docs.huihoo.com/doxygen/postgresql/pg__dump_8c_source.html#l00919

int main() {
	PGconn *conn;
	srand(time(NULL));
	char query[2048];

	// [CI]
	// conn = PQconnectdb("user=flowroute host=192.0.2.8 dbname=test_flowroute");

	// [staging]
	putenv("PGPASSWORD=hndlsnvki387cvnk&kdmdtuvcnsiocns");
	// conn = PQconnectdb("user=flowroute host=localhost port=7773 dbname=flowroute");
	conn = PQconnectdb("user=flowroute host=sd1d7h3fc7zawmd.cm9fyzdwhad9.us-west-2.rds.amazonaws.com dbname=flowroute");

	if (PQstatus(conn) == CONNECTION_BAD) {
	    fprintf(stderr, "Connection to database failed: %s\n",
	        PQerrorMessage(conn));
	    do_exit(conn);
	}

	// SET AUTOCOMMIT { = | TO } { ON | OFF }
	// \set AUTOCOMMIT off

	const char *pstatus = PQparameterStatus(conn, "server_version");
	printf("server_version: %s\n", pstatus);

	PGresult *res = PQexec(conn, "SELECT VERSION()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
	    printf("No data retrieved\n");
	    PQclear(res);
	    do_exit(conn);
	}
	printf(">>> %s\n", PQgetvalue(res, 0, 0));

//	res = PQexec(conn, "SET AUTOCOMMIT TO 'off'");
//	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
//		printf("SET autocommit command failed\n");
//		PQclear(res);
//		do_exit(conn);
//	}
	int count = 0;
	for (int y=0; y<1000; y++) {
		// BEGIN
		res = PQexec(conn, "BEGIN");
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			printf("BEGIN command failed\n");
			PQclear(res);
			do_exit(conn);
		}
	
		for (int i=0; i<2500 ; i++) {
			snprintf(query, 2048, "insert into trunking_did "
			"(type_id,value,customer_id,is_reserved,channel_limit,route_id,failover_id,port_order_id,"
			  "use_cnam_delivery,inbound_tier_id, cost_ratesheet_id, ratecenter_id, alias, status_id,"
			  "last_modified_at, last_modified_by_id)"
			  " values (13,1%d%d%d%d%d%d%d%d%d%d,44427,'f',0,84877, 84877,29460,'f',12,52,'QC~MONTREAL','','PURCHASED', '2019-04-16 18:29:31.738915+00', 39349)"
			  " ON CONFLICT DO NOTHING;",
			rand()%8+1,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9);
	
			res = PQexec(conn, query);
			if (PQresultStatus(res) != PGRES_COMMAND_OK) {
				do_exit2(conn, res);
			}
			// printf(">>> inserted [%s]\n", query);
			PQclear(res);
			count++;
		}
		// COMMIT
		res = PQexec(conn, "COMMIT");
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			printf("COMMIT command failed\n");
			PQclear(res);
//			do_exit(conn);
		}
		printf(" inserted: %d\n", count);
	}

	// PQclear(res);
	// select value, id, ctid::text from trunking_did where ctid > '(0,0)'::tid  limit 1;



	// PQfinish(conn);

	return 0;
}



//		snprintf(query, 2048, "insert into trunking_did (customer_id, type_id, value, is_reserved, channel_limit) values(130, 5, 1%d%d%d%d%d%d%d%d%d%d, false, 2);",
//				rand()%8+1,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9,rand()%9);

	/*
id   | type_id |    value    | customer_id | is_reserved | channel_limit | route_id | failover_id | port_order_id | exclusive_platform_id | use_cnam_delivery | e911_address_id | inbound_tier_id | cost_ratesheet_id | cnam_preset_id | ratecenter_id | alias | messaging_network_id | status_id |       last_modified_at        | last_modified_by_id
--------+---------+-------------+-------------+-------------+---------------+----------+-------------+---------------+-----------------------+-------------------+-----------------+-----------------+-------------------+----------------+---------------+-------+----------------------+-----------+-------------------------------+---------------------
 604045 |      13 | 15144009530 |       44427 | f           |             0 |    84877 |             |         29460 |                       | f                 |                 |              12 |                52 |                | QC~MONTREAL   |       |                      | PURCHASED | 2019-04-16 18:29:31.738915+00 |               39349
*/

//snprintf(query, 2048, "insert into trunking_did (type_id,value,customer_id,is_reserved,channel_limit,route_id,failover_id,port_order_id,use_cnam_delivery,inbound_tier_id, cost_ratesheet_id, ratecenter_id, alias, status_id, last_modified_at, last_modified_by_id) values (13,15144009531,44427,'f',0,84877, 84877,29460,'f',12,52,'QC~MONTREAL','','PURCHASED', '2019-04-16 18:29:31.738915+00', 39349);");
