#include <iostream>
#include "nlohmann/json.hpp"


using json = nlohmann::json;
using namespace std;

int main (int argc, const char *argv[]) {
    json j = {"a", "123"};
    cout << j << endl;
    return 0;
}