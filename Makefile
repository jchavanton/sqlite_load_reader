INCLUDES = -Ilib/sqlite

all:
	gcc src/db_action.c -o bin/db_action -lsqlite3  $(INCLUDES)
