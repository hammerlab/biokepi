open Biokepi_run_environment
open Common



open Workflow_utilities.Download (* All the wget* functions *)
module Vcftools = Workflow_utilities.Vcftools

let of_specification
    ~toolkit ~host ~run_program ~destination_path specification =
  let open Reference_genome in
  let {
    Specification.
    name;
    metadata;
    fasta;
    dbsnp;
    cosmic;
    exome_gtf; (* maybe desrves a better name? *)
    cdna;
    whess;
    major_contigs;
  } = specification in
  let dest_file f = destination_path // name // f in
  let rec compile_location filename =
    function
    | `Url url (* Right now, `wget_gunzip` is clever enough to not gunzip *)
    | `Gunzip `Url url ->
      Workflow_utilities.Download.wget_gunzip
        ~host ~run_program ~destination:(dest_file filename) url
    | `Bunzip2 `Url url ->
      Workflow_utilities.Download.wget_bunzip2
        ~host ~run_program ~destination:(dest_file filename) url
    | `Vcf_concat l ->
      let vcfs =
        List.map ~f:(fun (n, loc) -> compile_location n loc) l
      in
      let vcftools =
        Machine.Tool.Kit.get_exn toolkit Machine.Tool.Default.vcftools in
      let concated =
        let tmp_vcf =
          dest_file (Filename.chop_extension filename ^ "-cat.vcf") in
        Vcftools.vcf_concat_no_machine
          ~host ~vcftools ~run_program ~final_vcf:tmp_vcf vcfs in
      let sorted =
        let final_vcf_path = dest_file filename in
        Vcftools.vcf_sort_no_machine
          ~host ~vcftools ~run_program
          ~src:concated ~dest:final_vcf_path () in
      sorted
    | other ->
      failwithf "Reference_genome.compile_location this kind of location \
                 is not yet implemented"
  in
  let compile_location_opt filename =
    Option.map ~f:(compile_location filename) in
  create specification
    (compile_location (name ^ ".fasta") fasta)
    ?cosmic:(compile_location_opt "cosmic.vcf" cosmic)
    ?dbsnp:(compile_location_opt "dbsnp.vcf" dbsnp)
    ?gtf:(compile_location_opt "transcripts.gtf" exome_gtf)
    ?cdna:(compile_location_opt "cdns-all.fa" cdna)
    ?whess:(compile_location_opt "whess.sqlite" whess)

type pull_function =
  toolkit:Machine.Tool.Kit.t ->
  host:Common.KEDSL.Host.t ->
  run_program:Machine.Make_fun.t ->
  destination_path:string -> Reference_genome.t


let pull_b37 ~toolkit ~host ~(run_program : Machine.Make_fun.t) ~destination_path =
  of_specification ~toolkit ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.b37

let pull_b37decoy ~toolkit ~host ~(run_program : Machine.Make_fun.t) ~destination_path =
  of_specification ~toolkit ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.b37decoy

let pull_b38 ~toolkit ~host ~(run_program : Machine.Make_fun.t) ~destination_path =
  of_specification ~toolkit ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.b38

let pull_hg19 ~toolkit ~host ~(run_program : Machine.Make_fun.t) ~destination_path =
  of_specification ~toolkit ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.hg19

let pull_hg18 ~toolkit ~host ~(run_program : Machine.Make_fun.t) ~destination_path =
  of_specification ~toolkit ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.hg18

let pull_mm10 ~toolkit ~host ~(run_program : Machine.Make_fun.t) ~destination_path =
  of_specification ~toolkit ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.mm10

let default_genome_providers = [
  Reference_genome.Specification.Default.Name.b37, pull_b37;
  Reference_genome.Specification.Default.Name.b37decoy, pull_b37decoy;
  Reference_genome.Specification.Default.Name.b38, pull_b38;
  Reference_genome.Specification.Default.Name.hg18, pull_hg18;
  Reference_genome.Specification.Default.Name.hg19, pull_hg19;
  Reference_genome.Specification.Default.Name.mm10, pull_mm10;
]

let get_reference_genome name =
  match List.find default_genome_providers ~f:(fun (a, _) -> a = name) with
  | Some (_, pull) -> pull
  | None -> failwithf "Cannot find the reference genorme called %S" name
              
