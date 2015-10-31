#include "carvpath.hpp"
#include <map>

int main(int argc, char **argv) {
  std::map<std::string,std::string> m;
  carvpath::Context<std::map<std::string,std::string>,160> t(m);
  bool ok=true;
  ok &= t.testrange(std::cerr,200000000000,"0+123456789000",true);
  ok &= t.testrange(std::cerr,200000000000,"0+100000000000/0+50000000",true);
  ok &= t.testrange(std::cerr,20000,"0+100000000000/0+50000000",false);
  if (ok == false) {
    return -1;
  }
  std::cerr << "OK" << std::endl;
  return 0;
}
