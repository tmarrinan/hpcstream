#ifndef __HPCSTREAM_H_
#define __HPCSTREAM_H_

#include <iostream>

#define HPCSTREAM_FLOATTEST 1.9961090087890625e2 // IEEE 754 ==> 0x4068F38C80000000
#define HPCSTREAM_FLOATBINARY 0x4068F38C80000000LL

namespace HpcStream {
    enum DataType : uint8_t {Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float, Double, ArraySize};
    enum Endian : uint8_t {Little, Big};

    class Server;
    class Client;
}

#endif // __HPCSTREAM_H_