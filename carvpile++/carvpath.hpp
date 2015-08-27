namespace carvpath {
  struct Sparse {
      Sparse(uint64_t);
      operator uint64_t();
     private:
      const uint64_t mValue;
  };
  struct 
  struct TopEntity: public AbstractTopEntity {
      Entity(AbstractEntity *);
      ~Entity();
      void grow(uint64_t newsize);
      Entity parse(std::string,OnOOR);
      operator std::string() ;
      uint64_t size();
      size_t fragcount();
      Fragment operator[](size_t);
      Entity derive(uint64_t offset, uint64_t size, OnOOR);
      Entity newsparse(uint64_t size);
      void append(uint64_t offset, uint64_t size,OnOOR);
      void appendsparse(uint64_t size,OnOOR);
      Entity flatten();
  };
  struct AbstractLibInstance {
      ~AbstractLibInstance() {};
      Entity operator () (uint64_t fullsize, std::string topcarvpath);
  };
  struct LibInstance: public AbstractLibInstance {
      LibInstance(AbstractLibInstance *);
      ~LibInstance();
      Entity operator ()(uint64_t fullsize,std::string topcarvpath);
    private:
      std::uniqptr<AbstractLibInstance> mInstance;  
  };
  LibInstance libinstance(bool uselongtokendb=true,bool compatibilitymode=false);
}
