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
  //Class for holding individual fragments.
  struct Fragment {
      struct SPARSE{}; //Helper type for friendly API for creating sparse fragments.
      //Constructor for regular fragments.
      Fragment(uint64_t offset,uint64_t size):mIsSparse(false),mOffset(offset),mSize(size){}
      //Constructor for sparse fragments.
      Fragment(SPARSE,uint64_t size):mIsSparse(true),mOffset(0),mSize(size){}
      //Constructor using fragment chunk of carvpath.
      Fragment(std::string carvpath):mIsSparse(false),mOffset(0),mSize(0) {
        try {
          if (carvpath.at(0) == 'S') { //Sparse fragment
            mIsSparse=true;
            mSize=boost::lexical_cast<uint64_t>(carvpath.substr(1));
          } else { //Regular fragment
            std::vector<std::string> tokens;
            boost::split(tokens,carvpath,boost::is_any_of("+"));
            mOffset=boost::lexical_cast<uint64_t>(tokens[0]);
            mSize=boost::lexical_cast<uint64_t>(tokens[1]);
          }
        } 
        //Convert different parse error related exceptions to runtime_error exceptions.
        catch (const boost::bad_lexical_cast &e) { 
          throw std::runtime_error("Invalid CarvPath fragment string!");
        }
        catch (const std::out_of_range& oor) {
          throw std::runtime_error("Invalid CarvPath fragment string!");
        }
      }
      //Allow static casting to string
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

  //Template class for holding a carvpath entity. M is the type of a key/value map-like onject used for storing
  //long carvpaths by their secure hash value. Maxtokenlen defines the maximum carvpath token that won't need 
  //to be stored in such a map. Long paths need this solution in order to not violate OS constraints on the size of 
  //direcory token sizes.
  template <typename M,int Maxtokenlen> //Note: M is the type of our pseudo map for storing long paths by hash.
  struct Entity {
      Entity(M & map):mTotalsize(0),mLongPathMap(map),mFragments(){} //New zero size entity.
      Entity(M & map,std::string cp):mTotalsize(0),mLongPathMap(map),mFragments(){ //Entity from carvpath token.
        std::string carvpath=cp;
        //If the carvpath was so long that it needed representation as a hash, lookup the original carvpath using the hash.
        if (cp.at(0) == 'D') {
          carvpath=mLongPathMap[cp];
        }
        //Now split the carvpath into its individual fragments.
        std::vector<std::string> tokens;
        boost::split(tokens,carvpath,boost::is_any_of("_"));
        //Convert each fragment string to a fragment and append it to our mFragments vector.
        for (std::string token : tokens ) {
          (*this) += Fragment(token);
        }
      }
      Entity(M & map,uint64_t topsize):mTotalsize(topsize),mLongPathMap(map),mFragments(){ //Construct with a single zero offset fragment.
        mFragments.push_back(Fragment(0,topsize));
      }
      //Construct from vector of fragments (Note, no tests for merging needs or range matches)
      Entity(M &map,std::vector<Fragment> &fragments):mTotalsize(0),mLongPathMap(map),mFragments(fragments){} 
      //Add a single fragment, merge if possible
      Entity& operator+=(const Fragment& rhs){
        if (mFragments.size() != 0 and //Non empty fragmentlist to begin with.
            mFragments.back().issparse() == rhs.issparse() and //Fragements are either both sparse or both normal fragments.
            ( mFragments.back().issparse() or  //And either both are sparse; or:
              ( rhs.getoffset() == mFragments.back().getoffset() +mFragments.back().getsize()) //The new fragment begins at old one's end.
            )
           ) {
          //perfect fit, we can just merge by growing the last fragment.
          mFragments.back().grow(rhs.getsize()); 
        } else {
          //not a perfect fit, need to add as new fragment.
          mFragments.push_back(rhs);
        }
        mTotalsize += rhs.getsize(); //Update the total size.
        return *this;
      }
      //Add a whole entity, merge first fragment if possible
      Entity& operator+=(const Entity<M,Maxtokenlen> &rhs) {
         for (Fragment frag: rhs) {
           (*this) += frag;
         }
         return *this;
      }
      //Copy constructor
      Entity(Entity<M,Maxtokenlen> const &ent):mTotalsize(ent.mTotalsize),mFragments(ent.mFragments),mLongPathMap(ent.mLongPathMap){}
      //Copy assignment
      Entity & operator=(Entity<M,Maxtokenlen> &ent) {
        mTotalsize = ent.mTotalsize;
        mFragments=ent.mFragments;
        return *this;
      }
      //Move constructor
      Entity(Entity<M,Maxtokenlen> const &&ent):mTotalsize(ent.mTotalsize),mLongPathMap(ent.mLongPathMap),mFragments(std::move(ent.mFragments)){}
      //Move assignment
      Entity & operator=(Entity<M,Maxtokenlen> &&ent) {
        mTotalsize = ent.mTotalsize;
        mFragments=std::move(ent.mFragments);
        return *this;
      }
      //Allow static casting to string
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
      //Allow C++11 style for loops by providing begin/end forwarders.
      std::vector<Fragment>::const_iterator begin() const { return mFragments.begin();}
      std::vector<Fragment>::const_iterator end() const { return mFragments.end();}
      uint64_t getsize() const { return mTotalsize;}
      //Helper for getting fragments from parrent defined for one single child chunk.
      Entity<M,Maxtokenlen> subchunk(uint64_t offset,uint64_t fsize) const {
        //First check if child chunk isn't outside of the spacce defined for the parent.
        if (offset + fsize > mTotalsize) {
          throw std::out_of_range("Sub entity outside of entity bounds for carvpath Entity");
        }
        uint64_t parentfragstart=0; //Start within our parent entity.
        uint64_t startoffset=offset; //Start-offset within our result
        uint64_t startsize=fsize; 
        Entity<M,Maxtokenlen> rval(mLongPathMap); //Start off with an empty Entity for return value
        for (Fragment parentfrag: mFragments) {
           //Parent fragment ends AFTER startoffset ? Than we need to look at it.
           if ((parentfragstart + parentfrag.getsize()) > startoffset) {
             //Determine the maximum chunk size that could reside in this parent chunk
             uint64_t maxchunk = parentfrag.getsize() + parentfragstart - startoffset;
             //Chunksize is either the maxchunk size or the startsize; whatever is smaller.
             uint64_t chunksize=maxchunk;
             if (maxchunk > startsize) {
               chunksize=startsize;
             }
             //Add the right size/type chunk to the return value.
             if (parentfrag.issparse()) {
               rval+=Fragment(Fragment::SPARSE(),chunksize);
             } else {
               rval+=Fragment(parentfrag.getoffset()+startoffset-parentfragstart,chunksize);
             }
             //Update startsize for the next loop
             startsize -= chunksize;
             if (startsize > 0) {
               //Update the startoffset
               startoffset += chunksize;
             } else {
               //Or make sure the above "AFTER startoffset" test fails the next time around
               startoffset=this->mTotalsize + 1;
             }
           }
           //Update parentfragstart value.
           parentfragstart += parentfrag.getsize();
        }
        return rval;
      }
      //Get a subentity as top level entity.
      Entity<M,Maxtokenlen> subentity(Entity<M,Maxtokenlen> &childent) const { 
         Entity<M,Maxtokenlen> rval(mLongPathMap);
         for(Fragment childfrag: childent.mFragments) {
         //for(std::vector<Fragment>::iterator it = childent.mFragments.begin(); it != childent.mFragments.end(); ++it) {
         //  Fragment &frag=*it;
           if (childfrag.issparse()) {
              //sparse fragments from the child are added irregardless.
              rval += childfrag;
           } else {
              //Convert one one child fragment at a time.
              Entity<M,Maxtokenlen> chunk=subchunk(childfrag.getoffset(),childfrag.getsize());
              rval += chunk;
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
           //If there is nothing there yet, create a first regular fragment.
           (*this) += Fragment(0,chunk);     
         } else {
           //Otherwise; grow the last chunk.
           mFragments.back().grow(chunk);
         }
      }
      //Test if a child is a valid subentity of this entity.
      bool test(Entity<M,Maxtokenlen> &child) const {
         try {
           Entity<M,Maxtokenlen> testval=this->subentity(child);
         } catch (...) {
           return false;
         }
         return true;
      }
    private:
      //A convenient string in string out hash function.
      std::string hashFunction(std::string longpath) const {
        uint8_t out[32]; //Binary result
        char hexout[65]; //Hex result
        hexout[0]='D'; //Prefix hex result with a 'D'
        //Call the core hash function
        blake2b(out,longpath.c_str(),NULL,32,longpath.size(),0);
        //Convert to hex
        for (int i=0;i<32;i++) {
           std::sprintf(hexout+2*i+1, "%02x", out[i]);
        }
        //return as string
        return std::string(hexout,65);   
      }
      uint64_t mTotalsize;
      M &mLongPathMap;
      std::vector<Fragment> mFragments;
  };
  //A simple top of the tree.
  template <typename M,int Maxtokenlen>
  struct Top {
      //Create a top entity of given size.
      Top(M &map,uint64_t sz): size(sz),mMap(map),topentity(map,sz){}
      //Grow the top entity
      void grow(uint64_t chunk) {
        topentity.grow(chunk);
        size += chunk;
      }
      //Test if child entity fits within top entity
      bool test(Entity<M,Maxtokenlen> &child) const {
        return topentity.test(child);
      }
      //Get the actual entity
      const Entity<M,Maxtokenlen> &entity() const {return topentity;}
     private:
      uint64_t size;
      M &mMap;
      Entity<M,Maxtokenlen> topentity;
  };

  //Helper context.
  template <typename M,int Maxtokenlen>
  struct Context {
    //Constructor takes a key/value map-like object for longpath storage by hash.
    Context(M &map):mMap(map){}
    //Create a top of the tree.
    Top<M,Maxtokenlen> create_top(uint64_t size=0){return Top<M,Maxtokenlen>(mMap,size);}
    //Parse multi level carvpath.
    Entity<M,Maxtokenlen> parse(std::string path) const {
      Entity<M,Maxtokenlen> none(mMap); //Empty entity for thesting against empty entities.
      Entity<M,Maxtokenlen> levelmin(mMap); //Start of with empty levelmin
      Entity<M,Maxtokenlen> ln(mMap); //And empty ln
      //Tokenize the multi level carvpath into per level carvath strings.
      std::vector<std::string> tokens; 
      boost::split(tokens,path,boost::is_any_of("/"));
      for (std::string pathtoken: tokens) {
        //Create a level N entity from path token.
        ln = Entity<M,Maxtokenlen>(mMap,pathtoken);
        if (levelmin != none) {
          //Convert level N entity to a level 0 entity.
          ln = levelmin.subentity(ln);
        }
        //update the level N-1 value for our next loop.
        levelmin = ln;
      }
      return ln;
    }
    //Test for flattening
    bool testflatten(std::ostream& os,std::string pin,std::string pout) const {
          Entity<M,Maxtokenlen> a=std::move(parse(pin));
          if (static_cast<std::string>(a) != pout) {
            os << "FAIL: in='" <<  pin << "' expected='" << pout << "' result='" << a << "'" << std::endl;
            return false;
          } else {
            os << "OK: in='" <<  pin << "' expected='" << pout << "' result='" << a << "'" << std::endl;
            return true;
          }
    }
    //Test for subrange
    bool testrange(std::ostream& os,uint64_t topsize,std::string carvpath,bool expected) const{
          Top<M,Maxtokenlen> top(mMap,topsize);
          std::string t=top.entity();
          Entity<M,Maxtokenlen> entity=parse(carvpath);
          if (top.test(entity) != expected) {
            os << "FAIL: topsize=" << topsize << " path=" << carvpath << " result=" << (not expected) << std::endl;
            return false;
          } else {
            os << "OK: topsize=" << topsize << " path=" << carvpath << " result=" << expected << std::endl;
          }
          return true;
    }
   private:
    M & mMap;
  };

//Compare two fragments for equality
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

//Compare two entities for equality
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

//Stream out a fragment
std::ostream& operator<<(std::ostream& os, const carvpath::Fragment& obj)
{
  os << static_cast<std::string>(obj);
  return os;
}

//Stream out an entity
template <typename M,int Maxtokenlen>
std::ostream& operator<<(std::ostream& os, const carvpath::Entity<M,Maxtokenlen>& obj)
{
  os << static_cast<std::string>(obj);
  return os;
}


};
#endif
