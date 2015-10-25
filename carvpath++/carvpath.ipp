//Copyright (c) 2015, Rob J Meijer.
//Copyright (c) 2015, University College Dublin
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without
//modification, are permitted provided that the following conditions are met:
//1. Redistributions of source code must retain the above copyright
//   notice, this list of conditions and the following disclaimer.
//2. Redistributions in binary form must reproduce the above copyright
//   notice, this list of conditions and the following disclaimer in the
//   documentation and/or other materials provided with the distribution.
//3. All advertising materials mentioning features or use of this software
//   must display the following acknowledgement:
//   This product includes software developed by the <organization>.
//4. Neither the name of the <organization> nor the
//   names of its contributors may be used to endorse or promote products
//   derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY
//EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
//DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
//
//This code constitutes a C++ port of the CarvPath library. It is meant to be used
//by forensic tools and frameworks in the service of implementing zero-storage carving
//facilities or other processes where designation of of potentially fragmented and sparse 
//sub-entities is esential.
//
#ifndef _CARVPATH_IPP_
#define _CARVPATH_IPP_
#include <string>
#include <vector>
#include <memory>
#include <stdint.h>
#include <boost/lexical_cast.hpp>
#include <iostream>

namespace carvpath {
  //The gereric interface for both fragments and chunks of sparse
  struct FragmentInterface {
    virtual uint64_t getoffset()=0;
    virtual uint64_t getsize()=0;
    virtual bool issparse()=0;
    virtual void grow(uint64_t)=0;
    virtual operator std::string()=0;
  };
  //A regular fragment
  struct FragmentImpl: public FragmentInterface {
      FragmentImpl(uint64_t offset,uint64_t size):mOffset(offset),mSize(size){}
      operator std::string(){return std::to_string(mOffset) + "+" + std::to_string(mSize);}
      uint64_t getoffset(){return mOffset;}
      uint64_t getsize(){return mSize;}
      bool issparse(){return false;}
      void grow(uint64_t chunk){ mSize += chunk;}
    private:
      const uint64_t mOffset;
      uint64_t mSize;
  };
  //A sparse fragment
  struct SparseImpl: public FragmentInterface {
      SparseImpl(uint64_t size):mSize(size){}
      operator std::string(){return "S" + std::to_string(mSize);}
      uint64_t getoffset(){throw std::logic_error("getoffset should not be called on a Sparse!");}
      uint64_t getsize(){return mSize;}
      bool issparse(){return true;}
      void grow(uint64_t chunk){ mSize += chunk;}
    private:
      uint64_t mSize;        
  };  
  //A forwarder for either of the two fragment types.
  struct Fragment: public FragmentInterface {
      Fragment(FragmentInterface *frag):mImpl(frag){}
      operator std::string(){return static_cast<std::string>(*mImpl);}
      uint64_t getoffset(){return mImpl->getoffset();}
      uint64_t getsize(){return mImpl->getsize();}
      bool issparse(){return mImpl->issparse();}
      void grow(uint64_t chunk){ mImpl->grow(chunk);}
    private:
      std::shared_ptr<FragmentInterface> mImpl;   
  };

  //Convert a fragment carvpath substring to a fragment.
  Fragment asfrag(std::string fragstring){
    if ((fragstring.size() > 1) and (fragstring[0] == 'S')) {
      return Fragment(new SparseImpl(boost::lexical_cast<uint64_t>(fragstring.substr(1))));
    } else {
      off_t found=fragstring.find('+');
      if ((found!=std::string::npos) and (found != (fragstring.size()-1))) {
        std::string num1=fragstring.substr(0,found);
        std::string num2=fragstring.substr(found+1);
        return Fragment(new FragmentImpl(boost::lexical_cast<uint64_t>(num1),boost::lexical_cast<uint64_t>(num2)));
      }
    }
    throw std::runtime_error("Invalid CarvPath fragment string!");
  };

  template <typename M,int Maxtokenlen> //Note: M is the type of our pseudo map for storing long paths by hash.
  struct Entity {
      Entity(M &map):mTotalsize(0),mLongPathMap(map){}
      Entity(M &map,std::string cp):mTotalsize(0),mLongPathMap(map){
        std::string carvpath=cp;
        if (cp.at(0) == 'D') {
          carvpath=mLongPathMap[cp];
        }
        std::vector<std::string> tokens;
        boost::split(tokens,carvpath,boost::is_any_of("_"));
        for(std::vector<std::string>::iterator it = tokens.begin(); it != tokens.end(); ++it) {
          mFragments.append(asfrag(*it));
        }   
      }
      Entity(M &map,uint64_t topsize):mTotalsize(topsize),mLongPathMap(map){
        Fragment f(new FragmentImpl(0,topsize));
        mFragments.append(f);
      }
      Entity(M &map,std::vector<Fragment> &fragments):mTotalsize(0),mLongPathMap(map),mFragments(fragments){}
      Entity& operator+=(FragmentInterface& rhs){
        Fragment &lastfrag=mFragments[mFragments.size()-1];
        if (lastfrag.issparse() == rhs.issparse() and (lastfrag.issparse() or (rhs.getoffset() == lastfrag.getoffset() + lastfrag.getsize()))) {
          mFragments[mFragments.size()-1].grow(rhs.getsize()); 
        } else {
          mFragments.push_back(rhs);
        }
        mTotalsize += rhs.getsize();       
        return *this;
      }
      operator std::string() {
        if (mFragments.size() == 0) {
          return "S0";
        }
        std::string rval = "";
        for (FragWrapper& f : mFragments ) {
          if (rval != "") {
            rval += "_";
          }
          rval += static_cast<std::string>(f);
        }
        if (rval.size() > Maxtokenlen) {
          std::string hash=hashFunction(rval);
          mLongPathMap[hash]=rval;
          return hash;
        } else {
          return rval;
        }
      }
      uint64_t getsize() { return mTotalsize;}
      //FIXME start of todo for this class.
      Entity<M,Maxtokenlen> subentity(Entity &subent) { 
         Entity result(mLongPathMap);
         for(std::vector<Fragment>::iterator it = subent.begin(); it != subent.end(); ++it) {
           Fragment &frag=*it;
           if (frag.issparse()) {
              result += frag;
           } else {
              
           }
         }
         return Entity<M,Maxtokenlen>(mLongPathMap);
      }
      size_t size(){
        return mFragments.size();
      }      
      Fragment &operator[](size_t index){
        return mFragments[index];
      }
      void grow(uint64_t chunk){
         //FIXME, grow last fragment or create a fragment if non exists.
      }
      bool test(Entity<M,Maxtokenlen> &child) {/*FIXME*/ return false;}
    private:
      std::string hashFunction(std::string longpath) {
        uint8_t out[32];
        char hexout[65];
        hexout[0]='D';
        blake2bp(out,longpath.c_str(),NULL,32,longpath.size(),0);
        for (int i=0;i<32;i++) {
           std::sprintf(hexout+2*i+1, "%02X", out[i]);
        }
        return std::string(hexout,65);   
      }
      std::vector<Fragment> mFragments;
      uint64_t mTotalsize;
      M &mLongPathMap;
    protected:
      std::vector<Fragment> mFragments;
  };

  template <typename M,int Maxtokenlen>
  class Top {
      Top(M &map,uint64_t sz): mMap(map),mTopEntity(map,sz){}
      void grow(uint64_t chunk) {
        mTopEntity.grow(chunk);
      }
      bool test(Entity<M,Maxtokenlen> &child) {
        try:
          Entity b=mTopentity.subentity(child);
        catch (...) {
          return false;
        }
        return true;
      }
  };

  template <typename M,int Maxtokenlen>
  struct Context {
    Context(M &map):mMap(map){}
    Entity<M,Maxtokenlen> create_top(uint64_t size=0){return Top<M,Maxtokenlen>(mMap,size);}
    Entity<M,Maxtokenlen> parse(std::string path) {
      Entity<M,Maxtokenlen> none(mMap);
      Entity<M,Maxtokenlen> levelmin(mMap);
      Entity<M,Maxtokenlen> ln(mMap);
      std::vector<std::string> tokens;
      boost::split(tokens,path,boost::is_any_of("\/"));
      for(std::vector<std::string>::iterator it = tokens.begin(); it != tokens.end(); ++it) {
        ln = Entity(mMap,*it);
        if (not (levelmin == none)) {
          ln = levelmin.subentity(ln);
        }
        levelmin = ln
      }
      return ln;
    }
    bool testflatten(std::string pin,std::string pout){
          Entity a=parse<M,Maxtokenlen>(pin);
          if (static_cast<std::string>(a) != pout) {
            std::cerr << "FAIL: in='" <<  pin << "' expected='" << pout << "' result='" << static_cast<std::string>(a) << "'" << std::endl;
            return false
          return true
    }
    void testrange(uint64_t topsize,std::string carvpath,bool expected){
          Top<M,Maxtokenlen> top(topsize);
          Entity<M,Maxtokenlen> entity=parse<M,Maxtokenlen>(carvpath);
          if (top.test(entity) != expected) {
            std::cerr << "FAIL: topsize=" << topsize << "path=" << carvpath << "result=" << (not expected) << std::endl;
            return false;
          }
          return true;
    }
   private:
    M &mMap;
  };

}

bool operator==(const carvpath::FragmentInterface& lhs, const carvpath::FragmentInterface & rhs){
  /*FIXME*/ 
  return false;
}

template <typename M,int Maxtokenlen>
bool operator==(const carvpath::Entity<M,Maxtokenlen> & lhs, const carvpath::Entity<M,Maxtokenlen> & rhs){
  /*FIXME*/ 
  return false;
}
#endif
