# Show we can run up-root

from func_adl_servicex import ServiceXSourceXAOD


def simple_call():
    dataset_xaod = "user.kchoi:user.kchoi.ttHML_80fb_ttbar"
    ds = ServiceXSourceXAOD(dataset_xaod)
    data = ds \
        .SelectMany('lambda e: (e.Jets("AntiKt4EMTopoJets"))') \
        .Where('lambda j: (j.pt()/1000)>30') \
        .Select('lambda j: (j.pt())') \
        .AsROOTTTree('file.root', 'tree-me', "JetPt") \
        .value()

    print(data)


if __name__ == "__main__":
    simple_call()
