rm -f teststore
g++ -std=c++20 -Wall -g -fsanitize=address -fsanitize=undefined -I ../include -o teststore teststore.cpp
teststore
