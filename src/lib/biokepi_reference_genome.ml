open Biokepi_common

(** A reference genome has a name (for display/matching) and a
     cluster-dependent path.
     Corresponding Cosmic and dbSNP databases (VCFs) can be added to the mix.
*)
type t = {
  name: string;
  location: KEDSL.file_workflow;
  cosmic:  KEDSL.file_workflow option;
  dbsnp:  KEDSL.file_workflow option;
}
let create ?cosmic ?dbsnp name location =
  {name; location; cosmic; dbsnp}

let on_host ~host ?cosmic ?dbsnp name path =
  let open KEDSL in
  let location =
    workflow_node (single_file ~host path) in
  let optional = Option.map ~f:(fun p -> workflow_node (single_file ~host p)) in
  let cosmic = optional cosmic in
  let dbsnp = optional dbsnp in
  create ?cosmic ?dbsnp name location


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

let fasta: t -> KEDSL.file_workflow = fun t -> t.location
let cosmic_exn t =
  Option.value_exn ~msg:(sprintf "%s: no COSMIC" t.name) t.cosmic
let dbsnp_exn t =
  Option.value_exn ~msg:(sprintf "%s: no DBSNP" t.name) t.dbsnp
