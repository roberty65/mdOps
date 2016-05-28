INC = -I ../../../bServer/common/include \
      -I ../../../bServer/frame/share/include \
      -I ../../statShare/include \
      -I ../include
LIB = -L ../lib -lstatAgentClient \
      -L ../../statShare/lib -lstatShare \
      -L ../../../bServer/frame/share/lib -lProcessor \
      -L ../../../bServer/common/lib -lcommon
CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

.PHONY: mkdirs all clean distclean
CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

DEST = ./testLogNormal
OBJS = testLogNormal.o

.PHONY: mkdirs all clean distclean

all: mkdirs $(DEST)

$(DEST): $(OBJS)
	g++ -o $@ $(LDFLAGS) $(OBJS) $(LIB)
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

