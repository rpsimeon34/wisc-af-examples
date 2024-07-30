# Coffea Clusters

This directory is an adaptation of the example in CalVer/coffea_clusters.ipynb. Files in this directory include:

- **runScript.py**: This runs the analysis code from coffea_clusters_function.py. It can be run with `python3 runScript.py`. Options arguments include
  `executor` which can be either `local` or `wiscjq` (for execution on a distributed Condor cluster), `workers` for the number of workers, and `outdir`
  for the directory to save the results into. Example running from the command line: `python3 runScript.py --executor local --outdir outputs_dir`.
- **coffea_clusters_function.py**: This file has the function with the physics logic for our analysis
- **filesets/example_files.py**: This file contains the fileset definition that we pass to coffea's apply_to_fileset function