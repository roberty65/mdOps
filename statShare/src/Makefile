INC = -I ../../../bServer/common/include -I ../include
LIB = -L ../../../bServer/common/lib -lcommon

CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

DEST = ../lib/libstatShare.a
OBJS = StatData.o StatMerger.o utils.o

.PHONY: mkdirs all clean distclean

all: mkdirs $(DEST)

$(DEST): $(OBJS)
	ar cru $@ $(OBJS)
.cpp.o:
	g++ -c -o $@ $(INC) $(CXXFLAGS) $<
.c.o:
	gcc -c -o $@ $(INC) $(CXXFLAGS) $<
mkdirs:
	mkdir -p ../lib
clean:
	rm -f $(OBJS) *~ *.s *.ii *.i
distclean: clean
	rm -f $(DEST)

