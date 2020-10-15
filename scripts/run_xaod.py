# Show we can run up-root

from func_adl_servicex import ServiceXSourceXAOD


def simple_call():
    dataset_xaod = "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00"
    ds = ServiceXSourceXAOD(dataset_xaod)
    data = ds \
        .SelectMany('lambda e: (e.Jets("AntiKt4EMTopoJets"))') \
        .Where('lambda j: (j.pt()/1000)>30') \
        .Select('lambda j: j.pt()') \
        .AsAwkwardArray(["JetPt"]) \
        .value()

    print(data['JetPt'])


if __name__ == "__main__":
    # import logging
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG)
    # logging.getLogger('servicex').setLevel(logging.DEBUG)
    # logging.getLogger('servicex').addHandler(ch)
    simple_call()
