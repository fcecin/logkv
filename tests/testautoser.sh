rm -f testautoser
g++ -std=c++20 -Wall -g -fsanitize=address -fsanitize=undefined -I ../include -o testautoser testautoser.cpp
testautoser
