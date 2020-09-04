# Show we can run up-root

from servicex import ServiceXDataset
from func_adl_servicex import ServiceXDatasetSource


def simple_call():
    dataset_uproot = "user.kchoi:user.kchoi.ttHML_80fb_ttbar"
    uproot_transformer_image = "sslhep/servicex_func_adl_uproot_transformer:issue6"
    # query_uproot = "(Select (Where (call EventDataset 'bogus' 'nominal') (lambda (list event) (and (and (and (> (attr event 'lep_Pt_1') 20000.0) (> (attr event 'lep_Pt_2') 20000.0)) (> (attr event 'lep_isolationFixedCutLoose_0') 0)) (> (call abs (- (attr event 'Mlll012') 91200.0)) 15000.0)))) (lambda (list event) (dict (list 'lep_Pt_1' 'lep_Pt_2') (list (attr event 'lep_Pt_1') (attr event 'lep_Pt_2')))))"

    sx_dataset = ServiceXDataset(dataset_uproot, image=uproot_transformer_image)
    ds = ServiceXDatasetSource(sx_dataset, "nominal")
    data = ds.Select("lambda e: {'lep_pt_1': e.lep_Pt_1, 'lep_pt_2': e.lep_Pt_2}") \
        .AsParquetFiles('junk.parquet') \
        .value()

    print(data)


if __name__ == "__main__":
    simple_call()
