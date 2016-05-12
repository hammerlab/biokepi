open Biokepi_run_environment
open Common

let download_whess ~(run_with: Machine.t) destination  =
  let open KEDSL in
  let url =
    "ftp://genetics.bwh.harvard.edu/pph2/whess/polyphen-2.2.2-whess-2011_12.sqlite.bz2"
  in
  let folder = Filename.basename destination in
  let bz2_name = destination ^ ".bz2" in
  let host = (Machine.(as_host run_with)) in
  let name = "Download Polyphen/WHESS data" in
  workflow_node (single_file ~host destination)
    ~name
    ~make:(
      Machine.run_download_program run_with ~name
        Program.(
          exec ["mkdir"; "-p"; folder]
          && Workflow_utilities.Download.(wget_program ~output_filename:bz2_name url)
          && shf "bunzip2 -c %s > %s" bz2_name destination
          && exec ["rm"; "-f"; destination]
        )
    )

let annotate ~(run_with: Machine.t) ~whessdb ~vcf ~output_vcf =
  let open KEDSL in
  let vap_tool =
    Machine.get_tool run_with Machine.Tool.Definition.(python_package "vcf-annotate-polyphen")
  in
  let name = sprintf "vcf-annotate-polyphen_%s" (Filename.basename vcf) in
  workflow_node
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure vap_tool);
      depends_on (download_whess ~run_with whessdb);
    ]
    ~make:(
      Machine.quick_run_program run_with ~name
        Program.(
          Machine.Tool.(init vap_tool)
          && shf "vcf-annotate-polyphen %s %s %s" whessdb vcf output_vcf
        )
    )
