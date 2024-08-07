import matplotlib.pyplot as plt
import os
import argparse
from pathlib import Path
import coffea.util

def main(args):
    coutputs = coffea.util.load(args.inputs[0])
    for dset,results in coutputs.items():
        print(f"Dataset: {dset}")
        print(f"Event count: {results['EventCount']}")
    fig, ax = plt.subplots(1,1,figsize=(7,4))
    coutputs["ZH_HToBB_ZToLL"]["MyHist"].plot()
    outfile = os.path.join(args.outdir, "coffea_clusters_MyHist.png")
    Path(args.outdir).mkdir(parents=True, exist_ok=True)
    plt.savefig(outfile)
    print(f"Plots saved to {outfile}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plotting script"
    )
    parser.add_argument(
        "inputs",
        type=str,
        nargs=1,
        help="The input file with physics results",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="Plots",
        help="Directory to save the output files",
    )
    args = parser.parse_args()

    main(args)
