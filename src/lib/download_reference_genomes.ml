open Common

open Run_environment


open Workflow_utilities.Download (* All the wget* functions *)

let of_specification
    ~host ~run_program ~destination_path specification =
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
    major_contigs;
  } = specification in
  let rec compile_location filename =
    function
    | `Url url (* Right now, `wget_gunzip` is clever enough to not gunzip *)
    | `Gunzip `Url url ->
      Workflow_utilities.Download.wget_gunzip
        ~host ~run_program ~destination:(destination_path // filename) url
    | `Vcf_concat l ->
      let vcfs =
        List.map ~f:(fun (n, loc) -> compile_location n loc) l
      in
      let final_vcf = destination_path // filename in
      let vcftools = Tool.create Tool.Default.vcftools in
      Vcftools.vcf_concat_no_machine ~host ~vcftools ~run_program ~final_vcf vcfs
    | other ->
      failwithf "Reference_genome.compile_location this kind of location \
                 is not yet implemented"
  in
  let compile_location_opt filename =
    Option.map ~f:(compile_location filename) in
  create name
    (compile_location (name ^ ".fasta") fasta)
    ?cosmic:(compile_location_opt "cosmic.vcf" cosmic)
    ?dbsnp:(compile_location_opt "dbsnp.vcf" dbsnp)
    ?gtf:(compile_location_opt "transcripts.gtf" exome_gtf)
    ?cdna:(compile_location_opt "cdns-all.fa" cdna)


let pull_b37 ~host ~(run_program : Machine.run_function) ~destination_path =
  of_specification ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.b37

let pull_b37decoy ~host ~(run_program : Machine.run_function) ~destination_path =
  of_specification ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.b37decoy

let pull_b38 ~host ~(run_program : Machine.run_function) ~destination_path =
  of_specification ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.b38

let pull_hg19 ~host ~(run_program : Machine.run_function) ~destination_path =
  of_specification ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.hg19

let pull_hg18 ~host ~(run_program : Machine.run_function) ~destination_path =
  of_specification ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.hg18

let pull_mm10 ~host ~(run_program : Machine.run_function) ~destination_path =
  of_specification ~host ~run_program ~destination_path
    Reference_genome.Specification.Default.mm10
