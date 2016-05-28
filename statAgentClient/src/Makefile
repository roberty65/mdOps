INC = -I ../../../bServer/common/include \
      -I ../../../bServer/frame/share/include \
      -I ../../statShare/include \
      -I ../include
LIB = -L ../../statShare/lib -lstatShare \
      -L ../../../bServer/frame/share/lib -lprocessor \
      -L ../../../bServer/common/lib -lcommon
CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

DEST = ../lib/libstatAgentClient.a
OBJS = StatAgentClient.o

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

