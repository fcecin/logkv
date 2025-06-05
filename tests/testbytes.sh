rm -f testbytes
g++ -std=c++20 -Wall -g -fsanitize=address -fsanitize=undefined -I ../include -o testbytes testbytes.cpp
testbytes
