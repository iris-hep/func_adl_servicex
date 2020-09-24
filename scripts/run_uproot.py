# Show we can run up-root

from servicex import ServiceXDataset
from func_adl_servicex import ServiceXSourceUpROOT


def simple_call():
    dataset_uproot = "user.kchoi:user.kchoi.ttHML_80fb_ttbar"
    uproot_transformer_image = "sslhep/servicex_func_adl_uproot_transformer:issue6"

    sx_dataset = ServiceXDataset(dataset_uproot, image=uproot_transformer_image)
    ds = ServiceXSourceUpROOT(sx_dataset, "nominal")
    data = ds.Select("lambda e: {'lep_pt_1': e.lep_Pt_1, 'lep_pt_2': e.lep_Pt_2}") \
        .AsAwkwardArray() \
        .value()

    print(data)


if __name__ == "__main__":
    simple_call()
