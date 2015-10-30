#include "carvpath.ipp"
#include <map>

int main(int argc, char **argv) {
  std::map<std::string,std::string> m;
  carvpath::Context<std::map<std::string,std::string>,160> t(m);
  bool ok=true;
  t.testrange(200000000000,"0+123456789000",true);
  t.testrange(200000000000,"0+100000000000/0+50000000",true);
  t.testrange(20000,"0+100000000000/0+50000000",false);
}
