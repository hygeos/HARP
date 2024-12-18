
from harp.providers.NASA.MERRA2._merra2 import BaseMerra2

model = "MERRA2_MONTHLY"

class raster:

    class M2IMNXASM(BaseMerra2): pass
    M2IMNXASM.init_class(model)

    class M2IMNXGAS(BaseMerra2): pass
    M2IMNXGAS.init_class(model)

    class M2IMNXINT(BaseMerra2): pass
    M2IMNXINT.init_class(model)

    class M2IMNXLFO(BaseMerra2): pass
    M2IMNXLFO.init_class(model)

    class M2SMNXSLV(BaseMerra2): pass
    M2SMNXSLV.init_class(model)

    class M2TMNXADG(BaseMerra2): pass
    M2TMNXADG.init_class(model)

    class M2TMNXAER(BaseMerra2): pass
    M2TMNXAER.init_class(model)

    class M2TMNXCHM(BaseMerra2): pass
    M2TMNXCHM.init_class(model)

    class M2TMNXCSP(BaseMerra2): pass
    M2TMNXCSP.init_class(model)

    class M2TMNXFLX(BaseMerra2): pass
    M2TMNXFLX.init_class(model)

    class M2TMNXGLC(BaseMerra2): pass
    M2TMNXGLC.init_class(model)

    class M2TMNXINT(BaseMerra2): pass
    M2TMNXINT.init_class(model)

    class M2TMNXLFO(BaseMerra2): pass
    M2TMNXLFO.init_class(model)

    class M2TMNXLND(BaseMerra2): pass
    M2TMNXLND.init_class(model)

    class M2TMNXOCN(BaseMerra2): pass
    M2TMNXOCN.init_class(model)

    class M2TMNXRAD(BaseMerra2): pass
    M2TMNXRAD.init_class(model)

    class M2TMNXSLV(BaseMerra2): pass
    M2TMNXSLV.init_class(model)