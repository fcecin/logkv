rm -f benchmark_debug
g++ -std=c++20 -Wall -g -fsanitize=address -fsanitize=undefined -I ../include -o benchmark_debug benchmark.cpp 
benchmark_debug
