KDIR = /lib/modules/`uname -r`/build
obj-m := nanonap.o
M := make -C ${KDIR} M=`pwd`

all:
	${M} modules

clean-kernel:
	${M} clean

user: test_nanonap
test_nanonap: test_nanonap.c
	gcc -o test_nanonap test_nanonap.c -lpthread
clean-user:
	rm test_nanonap

clean: clean-user clean-kernel
