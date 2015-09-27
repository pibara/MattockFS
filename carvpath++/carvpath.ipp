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

namespace carvpath {
  struct FragmentInterface {
    virtual uint64_t getoffset()=0;
    virtual uint64_t getsize()=0;
    virtual bool issparse()=0;
    virtual void grow(uint64_t)=0;
    virtual operator std::string()=0;
  };
  struct Fragment: public FragmentInterface {
      Fragment(uint64_t offset,uint64_t size):mOffset(offset),mSize(size){}
      operator std::string(){return std::to_string(mOffset) + "+" + std::to_string(mSize);}
      uint64_t getoffset(){return mOffset;}
      uint64_t getsize(){return mSize;}
      bool issparse(){return false;}
      void grow(uint64_t chunk){ mSize += chunk;}
    private:
      const uint64_t mOffset;
      uint64_t mSize;
  };
  struct Sparse: public FragmentInterface {
      Sparse(uint64_t size):mSize(size){}
      operator std::string(){return "S" + std::to_string(mSize);}
      uint64_t getoffset(){throw std::logic_error("getoffset should not be called on a Sparse!");}
      uint64_t getsize(){return mSize;}
      bool issparse(){return true;}
      void grow(uint64_t chunk){ mSize += chunk;}
    private:
      uint64_t mSize;        
  };  
  struct FragWrapper: public FragmentInterface {
      FragWrapper(FragmentInterface *frag):mImpl(frag){}
      operator std::string(){return static_cast<std::string>(*mImpl);}
      uint64_t getoffset(){return mImpl->getoffset();}
      uint64_t getsize(){return mImpl->getsize();}
      bool issparse(){return mImpl->issparse();}
      void grow(uint64_t chunk){ mImpl->grow(chunk);}
    private:
      std::shared_ptr<FragmentInterface> mImpl;   
  };

  FragWrapper asfrag(std::string fragstring){
    if ((fragstring.size() > 1) and (fragstring[0] == 'S')) {
      return FragWrapper(new Sparse(boost::lexical_cast<uint64_t>(fragstring.substr(1))));
    } else {
      off_t found=fragstring.find('+');
      if ((found!=std::string::npos) and (found != (fragstring.size()-1))) {
        std::string num1=fragstring.substr(0,found);
        std::string num2=fragstring.substr(found+1);
        return FragWrapper(new Fragment(boost::lexical_cast<uint64_t>(num1),boost::lexical_cast<uint64_t>(num2)));
      }
    }
    throw std::runtime_error("Invalid CarvPath fragment string!");
  };

  template <typename M,int Maxtokenlen>
  struct Entity {
      Entity(std::function<std::string(std::string)> &hashfunc,M &map):mTotalsize(0),mHashFunction(hashfunc),mLongPathMap(map){}
      Entity& operator+=(FragmentInterface& rhs){
        Fragment &lastfrag=mFragments[mFragments.size()-1];
        if (lastfrag.issparse() == rhs.issparse() and (lastfrag.issparse() or (rhs.getoffset() == lastfrag.getoffset() + lastfrag.getsize()))) {
          mFragments[mFragments.size()-1].grow(rhs.getsize()); 
        } else {
          mFragments.push_back(rhs);
        }
        return *this;
      }
      operator std::string() {
        std::string rval = "";
        for (FragWrapper& f : mFragments ) {
          if (rval != "") {
            rval += "_";
          }
          rval += static_cast<std::string>(f);
        }
        return rval;
      }
      uint64_t getsize() { return mTotalsize;}
      Entity<M,Maxtokenlen> subentity(Entity &subent) { 
         /*FIXME*/ 
         return Entity<M,Maxtokenlen>(mHashFunction,mLongPathMap);
      }
    private:
      std::vector<FragWrapper> mFragments;
      uint64_t mTotalsize;
      std::function<std::string(std::string)> &mHashFunction;
      M &mLongPathMap;
  };

  template <typename M,int Maxtokenlen>
  struct Top {
      Top(M &map,std::function<std::string(std::string)> hash,uint64_t size):mSize(size),mTopentity(Entity<M,Maxtokenlen>(hash,map)){}
      void grow(uint64_t chunksize){mSize += chunksize;}
      bool test(Entity<M,Maxtokenlen> &child) {/*FIXME*/ return false;}
    private:
      uint64_t mSize;
      Entity<M,Maxtokenlen> mTopentity;
  };

  template <typename M,int Maxtokenlen>
  struct Context {
      Context(M &map,std::function<std::string(std::string)> hash):mMap(map),mHash(hash){}
      Top<M,Maxtokenlen> top(uint64_t size=0){return Top<M,Maxtokenlen>(mMap,mHash,size);}
      Entity<M,Maxtokenlen> parse(std::string entitystring){
        /*FIXME*/ 
        return Entity<M,Maxtokenlen>(mHash,mMap);
      }
      void testflatten(std::string pin,std::string pout){
        /*FIXME*/ 
        return;
      }
      void testrange(uint64_t topsize,std::string carvpath,bool expected){
        /*FIXME*/ 
        return;
      }
    private:
      M &mMap;
      std::function<std::string(std::string)> mHash;
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
