rm -f teststore_nosanitizer
g++ -std=c++20 -Wall -g -I ../include -o teststore_nosanitizer teststore.cpp
teststore_nosanitizer
