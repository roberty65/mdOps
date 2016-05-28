INC = -I ../../../bServer/common/include \
      -I ../../../bServer/frame/share/include \
      -I ../../statShare/include
LIB = -L ../../statShare/lib -lstatShare \
      -L ../../../bServer/frame/share/lib -lProcessor \
      -L ../../../bServer/common/lib -lcommon \
      -lpthread
CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

DEST = ../lib/libstatAgentProcessor.so
OBJS = StatLogWatcher.o StatAgentProcessor.o

DUMP = ../bin/statLogDump
DOBJ = StatLogDump.o

.PHONY: mkdirs all clean distclean

all: mkdirs $(DEST) $(DUMP)

$(DEST): $(OBJS)
	g++ -o $@ -shared $(LDFLAGS) $(OBJS) $(LIB)
$(DUMP): $(DOBJ)
	g++ -o $@ $(LDFLAGS) $(DOBJ) $(LIB)
.cpp.o:
	g++ -c -o $@ $(INC) $(CXXFLAGS) $<
.c.o:
	gcc -c -o $@ $(INC) $(CXXFLAGS) $<
mkdirs:
	mkdir -p ../lib ../bin ../logs ../stats
clean:
	rm -f $(OBJS) $(DOBJ) *~ *.s *.ii *.i
distclean: clean
	rm -f $(DEST) $(DUMP)

