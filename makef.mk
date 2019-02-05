test_expr: test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o btree_mgr.o
	gcc -o test_expr test_expr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o btree_mgr.o -lm buffer_mgr_stat.o -lm

test_assign4_1.o: test_assign4_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h expr.h tables.h record_mgr.h
	gcc -c test_assign4_1.c -lm

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h tables.h test_helper.h
	gcc -c test_expr.c -lm

record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h 
	gcc -c record_mgr.c -lm

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	gcc -c expr.c -lm

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	gcc -c rm_serializer.c -lm

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	gcc -c buffer_mgr_stat.c -lm

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h 
	gcc -c buffer_mgr.c -lm

storage_mgr.o: storage_mgr.c storage_mgr.h 
	gcc -c storage_mgr.c -lm

dberror.o: dberror.c dberror.h 
	gcc -c dberror.c -lm

btree_mgr.o: btree_mgr.c btree_mgr.h tables.h buffer_mgr.h storage_mgr.h record_mgr.h 
	gcc -c btree_mgr.c -lm
