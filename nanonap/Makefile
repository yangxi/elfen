KDIR = /lib/modules/`uname -r`/build
obj-m := nanonap.o
M := make -C ${KDIR} M=`pwd`

all:
	${M} modules
