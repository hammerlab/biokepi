Biokepi-friendly Ubuntu Container
---------------------------------

This `Dockerfile` provides a fresh Ubuntu machine with the few dependencies that
Biokepi workflows require (Bioinformatics tools are, by default, installed by
the Biokepi workflows).


- `cmake` is required by tools like Kallisto 
  (cf. issue [`solvuu/biopam#27`](https://github.com/solvuu/biopam/issues/27)).
- `r-base` is for `Seq2HLA`
- `tcsh` and `gawk` are for `NetMHC`
  (issue [`hammerlab/biokepi#287`](https://github.com/hammerlab/biokepi/issues/287))
- `libx11-dev`, `libfreetype6-dev`, and `pkg-config` are dependencies of
  Matplotlib which is used by Python tools (`topiary`, `vaxrank`, …).
