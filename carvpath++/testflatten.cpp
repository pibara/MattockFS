#include "carvpath.hpp"
#include <map>

int main(int argc, char **argv) {
  std::map<std::string,std::string> m;
  carvpath::Context<std::map<std::string,std::string>,160> t(m);
  bool ok=true;
  ok &= t.testflatten(std::cerr,"0+0","S0");
  ok &= t.testflatten(std::cerr,"S0","S0");
  ok &= t.testflatten(std::cerr,"0+0/0+0","S0");
  ok &= t.testflatten(std::cerr,"20000+0","S0");
  ok &= t.testflatten(std::cerr,"20000+0_89765+0","S0");
  ok &= t.testflatten(std::cerr,"1000+0_2000+0/0+0","S0");
  ok &= t.testflatten(std::cerr,"0+5","0+5");
  ok &= t.testflatten(std::cerr,"S1_S1","S2");
  ok &= t.testflatten(std::cerr,"S100_S200","S300");
  ok &= t.testflatten(std::cerr,"0+20000_20000+20000","0+40000");
  ok &= t.testflatten(std::cerr,"0+20000_20000+20000/0+40000","0+40000");
  ok &= t.testflatten(std::cerr,"0+20000_20000+20000/0+30000","0+30000");
  ok &= t.testflatten(std::cerr,"0+20000_20000+20000/10000+30000","10000+30000");
  ok &= t.testflatten(std::cerr,"0+20000_40000+20000/10000+20000","10000+10000_40000+10000");
  ok &= t.testflatten(std::cerr,"0+20000_40000+20000/10000+20000/5000+10000","15000+5000_40000+5000");
  ok &= t.testflatten(std::cerr,"0+20000_40000+20000/10000+20000/5000+10000/2500+5000","17500+2500_40000+2500");
  ok &= t.testflatten(std::cerr,"0+20000_40000+20000/10000+20000/5000+10000/2500+5000/1250+2500","18750+1250_40000+1250");
  ok &= t.testflatten(std::cerr,"0+20000_40000+20000/10000+20000/5000+10000/2500+5000/1250+2500/625+1250","19375+625_40000+625");
  ok &=t.testflatten(std::cerr,"0+100_101+100_202+100_303+100_404+100_505+100_606+100_707+100_808+100_909+100_1010+100_1111+100_1212+100_1313+100_1414+100_1515+100_1616+100_1717+100_1818+100_1919+100_2020+100_2121+100_2222+100_2323+100_2424+100","D98F3C36391A089C5A033DC38CE846EE2E20CF0F79B42F0B5194C81021AA9A757");
  ok &= t.testflatten(std::cerr,"0+100_101+100_202+100_303+100_404+100_505+100_606+100_707+100_808+100_909+100_1010+100_1111+100_1212+100_1313+100_1414+100_1515+100_1616+100_1717+100_1818+100_1919+100_2020+100_2121+100_2222+100_2323+100_2424+100/1+2488","DF39B0F1EC1197BDBAD4C7CEB8A10B1102B8889B1C889A9A3AD6DD2A6522871C0");  
  ok &= t.testflatten(std::cerr,"D98F3C36391A089C5A033DC38CE846EE2E20CF0F79B42F0B5194C81021AA9A757/1+2488","DF39B0F1EC1197BDBAD4C7CEB8A10B1102B8889B1C889A9A3AD6DD2A6522871C0");
  ok &= t.testflatten(std::cerr,"D98F3C36391A089C5A033DC38CE846EE2E20CF0F79B42F0B5194C81021AA9A757/350+100","353+50_404+50");
  if (ok == false) {
    return -1;
  }
  std::cerr << "OK" << std::endl;
  return 0;
}
