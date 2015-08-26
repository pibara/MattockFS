#include "carvpath.hpp"
namespace carvpile {
    struct AbstractPile {
        ~AbstractPile() {};
        void registerpath(carvpath::Composit) =0;
        carvpath::Composit unregisterpath(carvpath::Composit) =0;
        uint64-t pilesize()=0;
    };
    struct Pile: public AbstractPile {
        Pile(AbstractPile *);
        ~Pile();
        void registerpath(carvpath::Composit);
        carvpath::Composit unregisterpath(carvpath::Composit);
        uint64_t pilesize();
      private:
        std::uniqptr<AbstractPile> mPile;
    };
    Pile createpile();
}
