############
MPICXX= mpic++
MPICXX_FLAGS= -std=c++11 -DASIO_STANDALONE -D_VARIADIC_MAX=10 -w
LIBCXX= ar
LIBCXX_FLAGS= rcs

NETSOCKET_DIR= $(HOME)/local
OPENSSL_DIR=/usr/local/opt/openssl
DDR_DIR= $(HOME)/Dev/ddr

# HPC STREAM LIBRARY
INC= -I${NETSOCKET_DIR}/include -I$(OPENSSL_DIR)/include -I./include
LIB= -L${NETSOCKET_DIR}/lib -lnetsocket -lssl -lcrypto
SRCDIR= src
OBJDIR= obj
LIBDIR= lib
BINDIR= bin
OBJS= $(addprefix $(OBJDIR)/, server.o client.o)
EXEC= $(addprefix $(LIBDIR)/, libhpcstream.a)

# CREATE DIRECTORIES (IF DON'T ALREADY EXIST)
mkdirs:= $(shell mkdir -p $(OBJDIR) $(LIBDIR) $(BINDIR))

# BUILD EVERYTHING
all: $(EXEC)

$(EXEC): $(OBJS)
	$(LIBCXX) $(LIBCXX_FLAGS) $@ $^

$(OBJDIR)/%.o: $(SRCDIR)/%.cpp
	$(MPICXX) $(MPICXX_FLAGS) -c -o $@ $< $(INC)

# REMOVE OLD FILES
clean:
	rm -f $(OBJS) $(EXEC)
