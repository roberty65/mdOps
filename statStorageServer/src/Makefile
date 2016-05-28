INC = -I ../include -I ../../../bServer/common/include -I ../../../bServer/frame/share/include -I ../../statShare/include
LIB = -L ../../../bServer/frame/share/lib -lProcessor -L ../../statShare/lib -lstatShare -L ../../../bServer/common/lib -lcommon -lpthread

CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

DEST = ../lib/libstatStorageProcessor.so
OBJS = FileStorage.o StatCombiner.o StatStorageProcessor.o

.PHONY: mkdirs all clean distclean

all: mkdirs $(DEST)

$(DEST): $(OBJS)
	g++ -o $@ -shared $(LDFLAGS) $(OBJS) $(LIB)
.cpp.o:
	g++ -c -o $@ $(INC) $(CXXFLAGS) $<
.c.o:
	gcc -c -o $@ $(INC) $(CXXFLAGS) $<
mkdirs:
	mkdir -p ../lib ../bin ../logs ../stats
clean:
	rm -f $(OBJS) *~ *.s *.ii *.i
distclean: clean
	rm -f $(DEST)

