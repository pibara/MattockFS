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
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <iostream>
#include <blake2.h>

namespace carvpath {
  //A forwarder for either of the two fragment types.
  struct Fragment {
      struct SPARSE{};
      Fragment(uint64_t offset,uint64_t size):mIsSparse(false),mOffset(offset),mSize(size){}
      Fragment(SPARSE,uint64_t size):mIsSparse(true),mOffset(0),mSize(size){}
      Fragment(std::string carvpath):mIsSparse(false),mOffset(0),mSize(0) {
        try {
          if (carvpath.at(0) == 'S') {
            mIsSparse=true;
            mSize=boost::lexical_cast<uint64_t>(carvpath.substr(1));
          } else {
            std::vector<std::string> tokens;
            boost::split(tokens,carvpath,boost::is_any_of("+"));
            mOffset=boost::lexical_cast<uint64_t>(tokens[0]);
            mSize=boost::lexical_cast<uint64_t>(tokens[1]);
          }
        } 
        catch (const boost::bad_lexical_cast &e) { 
          throw std::runtime_error("Invalid CarvPath fragment string!");
        }
        catch (const std::out_of_range& oor) {
          throw std::runtime_error("Invalid CarvPath fragment string!");
        }
      }
      operator std::string() const {
        if (mIsSparse) {
          return std::string("S") + std::to_string(mSize);
        } else {
          return std::to_string(mOffset) + "+" + std::to_string(mSize);
        }
      }
      uint64_t getoffset() const {return mOffset;}
      uint64_t getsize() const {return mSize;}
      bool issparse() const {return mIsSparse;}
      void grow(uint64_t chunk){mSize += chunk;}
    private:
      bool mIsSparse;
      uint64_t mOffset;
      uint64_t mSize;
  };

  template <typename M,int Maxtokenlen> //Note: M is the type of our pseudo map for storing long paths by hash.
  struct Entity {
      Entity(M & map):mTotalsize(0),mLongPathMap(map){} //New zero size entity.
      Entity(M & map,std::string cp):mTotalsize(0),mLongPathMap(map){ //Entity from carvpath
        std::string carvpath=cp;
        //If the carvpath was so long that it needed representation as a hash, lookup the original carvpath using the hash.
        if (cp.at(0) == 'D') {
          carvpath=mLongPathMap[cp];
        }
        //Now split the carvpath into its individual fragments.
        std::vector<std::string> tokens;
        boost::split(tokens,carvpath,boost::is_any_of("_"));
        //Convert each fragment string to a fragment and append it to our mFragments vector.
        for(std::vector<std::string>::iterator it = tokens.begin(); it != tokens.end(); ++it) {
          Fragment frag(*it);
          (*this) += frag;
          //mFragments.push_back(frag);
          //mTotalsize += frag.getsize();
        }
      }
      Entity(M & map,uint64_t topsize):mTotalsize(topsize),mLongPathMap(map){
        mFragments.push_back(Fragment(0,topsize));
      }
      Entity(M &map,std::vector<Fragment> &fragments):mTotalsize(0),mLongPathMap(map),mFragments(fragments){}

      Entity& operator+=(const Fragment& rhs){
        if (mFragments.size() != 0 and //Non empty fragmentlist to begin with.
            mFragments.back().issparse() == rhs.issparse() and //Fragements are either both sparse or both normal fragments.
            ( mFragments.back().issparse() or  //And either both are sparse; or:
              ( rhs.getoffset() == mFragments.back().getoffset() +mFragments.back().getsize()) //The new fragment begins at old one's end.
            )
           ) {
          //perfect fit, we can just grow the last fragment.
          mFragments.back().grow(rhs.getsize()); 
        } else {
          //not a perfect fit, need to add as new fragment.
          mFragments.push_back(rhs);
        }
        mTotalsize += rhs.getsize(); //Update the total size.
        return *this;
      }
      Entity& operator+(const Entity &rhs) {
         size_t sz=rhs.size();
         for(size_t index=0;index<sz;index++) {
            (*this) += rhs[index];
         }
      }
      Entity(Entity const &ent):mTotalsize(ent.mTotalsize),mFragments(ent.mFragments),mLongPathMap(ent.mLongPathMap){}
      Entity & operator=(Entity &ent) {
        mTotalsize = ent.mTotalsize;
        mFragments=ent.mFragments;
        return *this;
      }
      Entity(Entity const &&ent):mTotalsize(ent.mTotalsize),mLongPathMap(ent.mLongPathMap),mFragments(std::move(ent.mFragments)){}
      Entity & operator=(Entity &&ent) {
        mTotalsize = ent.mTotalsize;
        mFragments=std::move(ent.mFragments);
        return *this;
      }
      operator std::string() const {
        //Check for zero size.
        if (mTotalsize == 0) {
          return "S0";
        }
        //Non zero: start of with empty string.
        std::string rval = "";
        //Process each fragment.
        for (Fragment f : mFragments ) {
          if (rval != "") {
            rval += "_"; //Fragment seperator character.
          }
          rval += static_cast<std::string>(f); //Append string representation of fragment.
        }
        //Check if result exceeds maximum string size for token. 
        if (rval.size() > Maxtokenlen) {
          std::string hash=hashFunction(rval); //Calculate the hash
          mLongPathMap[hash]=rval; //Store the original carvpath in some distributed key/value store.
          return hash; //Return the hash, not the real carvpath.
        } else {
          return rval; //Return the regular carvpath.
        }
      }
      uint64_t getsize() const { return mTotalsize;}
      Entity subchunk(uint64_t offset,uint64_t size) const {
        if (offset + size > mTotalsize) {
          throw std::out_of_range("Sub entity outside of entity bounds for carvpath Entity");
        }
        uint64_t parentfragstart=0; //Start within our parent entity.
        uint64_t startoffset=offset; //Start-offset within our result
        uint64_t startsize=size; 
        Entity rval(mLongPathMap);
        size_t sz=this->size(); //Number of fragments in parent entity.
        for (size_t index=0;index < sz;index++) {
           Fragment const &parentfrag=(*this)[index]; 
           if ((parentfragstart + parentfrag.getsize()) > startoffset) {
             uint64_t maxchunk = parentfrag.getsize() + parentfragstart - startoffset;
             uint64_t chunksize=maxchunk;
             if (maxchunk > startsize) {
               chunksize=startsize;
             } else {
             }
             if (parentfrag.issparse()) {
               rval+=Fragment(Fragment::SPARSE(),chunksize);
             } else {
               rval+=Fragment(parentfrag.getoffset()+startoffset-parentfragstart,chunksize);
             }
             startsize -= chunksize;
             if (startsize > 0) {
               startoffset += chunksize;
             } else {
               startoffset=this->mTotalsize + 1;
             }
           }
           parentfragstart += parentfrag.getsize();
        }
        return rval;
      }
      Entity<M,Maxtokenlen> subentity(Entity &childent) const { 
         Entity<M,Maxtokenlen> rval(mLongPathMap);
         for(std::vector<Fragment>::iterator it = childent.mFragments.begin(); it != childent.mFragments.end(); ++it) {
           Fragment &frag=*it;
           std::string fs=frag;
           if (frag.issparse()) {
              rval += frag;
           } else {
              Entity chunk=subchunk(frag.getoffset(),frag.getsize());
              size_t sz=chunk.size();
              for (size_t index=0;index < sz;index++) {
                Fragment const &frag2=chunk[index];
                std::string fs2=frag2;
                rval+=frag2;
              }
           }
         }
         return rval;
      }
      size_t size() const{
        return mFragments.size();
      }      
      Fragment const &operator[](size_t index) const {
        return mFragments.at(index);
      }
      void grow(uint64_t chunk){
         if (this->size() == 0) {
           (*this) += Fragment(0,chunk);     
         } else {
           mFragments.back().grow(chunk);
         }
      }
      bool test(Entity<M,Maxtokenlen> &child) const {
         try {
           Entity<M,Maxtokenlen> testval=this->subentity(child);
         } catch (...) {
           return false;
         }
         return true;
      }
    private:
      std::string hashFunction(std::string longpath) const {
        uint8_t out[32];
        char hexout[65];
        hexout[0]='D';
        blake2bp(out,longpath.c_str(),NULL,32,longpath.size(),0);
        for (int i=0;i<32;i++) {
           std::sprintf(hexout+2*i+1, "%02X", out[i]);
        }
        return std::string(hexout,65);   
      }
      uint64_t mTotalsize;
      M &mLongPathMap;
      std::vector<Fragment> mFragments;
  };

  template <typename M,int Maxtokenlen>
  struct Top {
      Top(M &map,uint64_t sz): size(sz),mMap(map),topentity(map,sz){}
      void grow(uint64_t chunk) {
        topentity.grow(chunk);
        size += chunk;
      }
      bool test(Entity<M,Maxtokenlen> &child) const {
        return topentity.test(child);
      }
      const Entity<M,Maxtokenlen> &entity() const {return topentity;}
     private:
      uint64_t size;
      M &mMap;
      Entity<M,Maxtokenlen> topentity;
  };

  template <typename M,int Maxtokenlen>
  struct Context {
    Context(M &map):mMap(map){}
    Top<M,Maxtokenlen> create_top(uint64_t size=0){return Top<M,Maxtokenlen>(mMap,size);}
    Entity<M,Maxtokenlen> parse(std::string path) const {
      Entity<M,Maxtokenlen> none(mMap);
      Entity<M,Maxtokenlen> levelmin(mMap);
      Entity<M,Maxtokenlen> ln(mMap);
      std::vector<std::string> tokens;
      boost::split(tokens,path,boost::is_any_of("/"));
      for(std::vector<std::string>::iterator it = tokens.begin(); it != tokens.end(); ++it) {
        ln = Entity<M,Maxtokenlen>(mMap,*it);
        if (not (levelmin == none)) {
          ln = levelmin.subentity(ln);
        }
        levelmin = ln;
      }
      return ln;
    }
    bool testflatten(std::string pin,std::string pout) const {
          Entity<M,Maxtokenlen> a=std::move(parse(pin));
          if (static_cast<std::string>(a) != pout) {
            std::cerr << "FAIL: in='" <<  pin << "' expected='" << pout << "' result='" << static_cast<std::string>(a) << "'" << std::endl;
            return false;
          } else {
            std::cerr << "OK: in='" <<  pin << "' expected='" << pout << "' result='" << static_cast<std::string>(a) << "'" << std::endl;
            return true;
          }
    }
    bool testrange(uint64_t topsize,std::string carvpath,bool expected) const{
          Top<M,Maxtokenlen> top(mMap,topsize);
          std::string t=top.entity();
          Entity<M,Maxtokenlen> entity=parse(carvpath);
          if (top.test(entity) != expected) {
            std::cerr << "FAIL: topsize=" << topsize << " path=" << carvpath << " result=" << (not expected) << std::endl;
            return false;
          } else {
            std::cerr << "OK: topsize=" << topsize << " path=" << carvpath << " result=" << expected << std::endl;
          }
          return true;
    }
   private:
    M & mMap;
  };

bool operator==(const carvpath::Fragment& lhs, const carvpath::Fragment & rhs) {
  return ( lhs.issparse() == rhs.issparse()) and 
         ( lhs.getsize() == rhs.getsize()) and 
         ( lhs.issparse() or 
           ( lhs.getoffset() == rhs.getoffset())
         );
}

bool operator!=(const carvpath::Fragment& lhs, const carvpath::Fragment & rhs) {
  return not(lhs==rhs);
}

template <typename M,int Maxtokenlen>
bool operator==(const carvpath::Entity<M,Maxtokenlen> & lhs, const carvpath::Entity<M,Maxtokenlen> & rhs) {
  if (lhs.getsize() == rhs.getsize() and
      lhs.size() == rhs.size()) {
    size_t sz=lhs.size();
    for (size_t index=0;index<sz;index++) {
      //If a single fragment is inequal, than everything is inequal.
      if (lhs[index] != rhs[index]) {
        return false;
      }
    }
    //All fragments are the same for both entities.
    return true;
  }
  //Not the same size or number of fragments, so entities can't be equal.
  return false;
}

template <typename M,int Maxtokenlen>
bool operator!=(const carvpath::Entity<M,Maxtokenlen> & lhs, const carvpath::Entity<M,Maxtokenlen> & rhs) {
  return not(lhs==rhs);
}


};
#endif
