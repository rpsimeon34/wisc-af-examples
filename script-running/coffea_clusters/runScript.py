from dask.distributed import Client
from dask.diagnostics import ProgressBar
import dask
import os
import argparse
import datetime
import coffea.util
from coffea.dataset_tools import preprocess, apply_to_fileset
from cowtools import GetDefaultCondorClient, move_x509

from coffea_clusters_function import ExampleAnalyzer
from filesets.example_files import fileset

def main(kwargs):
    if kwargs.executor == "local":
        client = Client() #Create local Dask cluster
    else:
        x509_path = move_x509
        client = GetDefaultCondorClient(x509_path,max_workers=args.workers)
    #Run using our local cluster, and include a progress bar for convenience (not required)
    with client, ProgressBar():
        print("About to preprocess")
        fileset_runnable, _ = preprocess(fileset)
        print("Finished preprocessing. Here is the pre-processed fileset:")
        print(fileset_runnable)
        #Run across the fileset
        outputs, reports = apply_to_fileset(ExampleAnalyzer,fileset_runnable,uproot_options={"allow_read_errors_with_report": True, "skipbadfiles": True, "timeout": 300})
        #Actually compute the outputs
        print("About to compute the outputs")
        coutputs, creports = dask.compute(outputs,reports)
        print("Finished computing outputs")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = os.path.join(args.outdir, f"output_coffea_clusters_run{timestamp}.coffea")
    coffea.util.save(coutputs,outfile)
    print(f"Saved output to {outfile} (this is what you pass to plotting.py)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch processing script"
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="Outputs",
        help="Where to put the output files",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of workers",
    )
    parser.add_argument(
        "-e",
        "--executor",
        choices=["local","wiscjq"],
        default="local",
        help="How to run the processing",
    )
    args = parser.parse_args()

    main(args)