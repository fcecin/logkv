rm -f benchmark
g++ -std=c++20 -O3 -I ../include -o benchmark benchmark.cpp 
benchmark
