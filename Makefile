INCLUDES = -Ilib/sqlite

all:
	gcc db_action.c -o db_action -lsqlite3  $(INCLUDES)
