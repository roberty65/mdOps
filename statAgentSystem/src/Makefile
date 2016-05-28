INC = -I ../../../bServer/common/include \
      -I ../../statShare/include \
      -I ../../statAgentClient/include
LIB = -L ../../statAgentClient/lib -lstatAgentClient \
      -L ../../statShare/lib -lstatShare \
      -L ../../../bServer/common/lib -lcommon -lpthread

CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

DEST = ../bin/statAgentSystemd
OBJS = NameFilter.o main.o

.PHONY: mkdirs all clean distclean

all: mkdirs $(DEST) $(DUMP)

$(DEST): $(OBJS)
	g++ -o $@ $(LDFLAGS) $(OBJS) $(LIB)
.cpp.o:
	g++ -c -o $@ $(INC) $(CXXFLAGS) $<
.c.o:
	gcc -c -o $@ $(INC) $(CXXFLAGS) $<
mkdirs:
	mkdir -p ../bin ../logs
clean:
	rm -f $(OBJS) $(DOBJ) *~ *.s *.ii *.i
distclean: clean
	rm -f $(DEST) $(DUMP)

