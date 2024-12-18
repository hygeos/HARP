from harp.providers.NASA.MERRA2._merra2 import BaseMerra2

model = "MERRA2_DIURNAL"

class raster:

    class M2IUNXASM(BaseMerra2): pass
    M2IUNXASM.init_class(model)

    class M2IUNXGAS(BaseMerra2): pass
    M2IUNXGAS.init_class(model)

    class M2IUNXINT(BaseMerra2): pass
    M2IUNXINT.init_class(model)

    class M2IUNXLFO(BaseMerra2): pass
    M2IUNXLFO.init_class(model)

    class M2TUNXADG(BaseMerra2): pass
    M2TUNXADG.init_class(model)

    class M2TUNXAER(BaseMerra2): pass
    M2TUNXAER.init_class(model)

    class M2TUNXCHM(BaseMerra2): pass
    M2TUNXCHM.init_class(model)

    class M2TUNXCSP(BaseMerra2): pass
    M2TUNXCSP.init_class(model)

    class M2TUNXFLX(BaseMerra2): pass
    M2TUNXFLX.init_class(model)

    class M2TUNXGLC(BaseMerra2): pass
    M2TUNXGLC.init_class(model)

    class M2TUNXINT(BaseMerra2): pass
    M2TUNXINT.init_class(model)

    class M2TUNXLFO(BaseMerra2): pass
    M2TUNXLFO.init_class(model)

    class M2TUNXLND(BaseMerra2): pass
    M2TUNXLND.init_class(model)

    class M2TUNXOCN(BaseMerra2): pass
    M2TUNXOCN.init_class(model)

    class M2TUNXRAD(BaseMerra2): pass
    M2TUNXRAD.init_class(model)

    class M2TUNXSLV(BaseMerra2): pass
    M2TUNXSLV.init_class(model)


