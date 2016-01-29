open Common

type specification =
  [`B37 | `B38 | `hg19 | `hg18 | `B37decoy | `mm10 ]

(** A reference genome has a name (for display/matching) and a
     cluster-dependent path.
     Corresponding Cosmic and dbSNP databases (VCFs) can be added to the mix.
*)
type t = {
  name: string;
  location: KEDSL.file_workflow;
  cosmic:  KEDSL.file_workflow option;
  dbsnp:  KEDSL.file_workflow option;
  gtf:  KEDSL.file_workflow option;
  cdna: KEDSL.file_workflow option;
}

let create ?cosmic ?dbsnp ?gtf ?cdna name location =
  {name; location; cosmic; dbsnp; gtf; cdna}


let name t = t.name
let path t = t.location#product#path
let cosmic_path_exn t =
  let msg = sprintf "cosmic_path_exn of %s" t.name in
  let cosmic = Option.value_exn ~msg t.cosmic in
  cosmic#product#path

let dbsnp_path_exn t =
  let msg = sprintf "dbsnp_path_exn of %s" t.name in
  let trgt = Option.value_exn ~msg t.dbsnp in
  trgt#product#path

let gtf_path_exn t =
  let msg = sprintf "gtf_path_exn of %s" t.name in
  let trgt = Option.value_exn ~msg t.gtf in
  trgt#product#path

let cdna_path_exn t =
    let msg = sprintf "cdna_path_exn of %s" t.name in
    let target = Option.value_exn ~msg t.cdna in
    target#product#path

let fasta: t -> KEDSL.file_workflow = fun t -> t.location
let cosmic_exn t =
  Option.value_exn ~msg:(sprintf "%s: no COSMIC" t.name) t.cosmic
let dbsnp_exn t =
  Option.value_exn ~msg:(sprintf "%s: no DBSNP" t.name) t.dbsnp
let gtf_exn t =
  Option.value_exn ~msg:(sprintf "%s: no GTF" t.name) t.gtf
let cdna_exn t =
  Option.value_exn ~msg:(sprintf "%s: no cDNA fasta file" t.name) t.cdna
