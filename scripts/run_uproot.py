# Show we can run up-root

from func_adl_servicex import ServiceXSourceUpROOT


def simple_call():
    dataset_uproot = "user.kchoi:user.kchoi.ttHML_80fb_ttbar"
    ds = ServiceXSourceUpROOT(dataset_uproot, "nominal")
    data = ds.Select("lambda e: {'lep_pt_1': e.lep_Pt_1, 'lep_pt_2': e.lep_Pt_2}") \
        .AsParquetFiles('junk.parquet') \
        .value()

    print(data)


if __name__ == "__main__":
    # import logging
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG)
    # logging.getLogger('servicex').setLevel(logging.DEBUG)
    # logging.getLogger('servicex').addHandler(ch)
    simple_call()
