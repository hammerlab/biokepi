open Biokepi_common

(** A reference genome has a name (for display/matching) and a
     cluster-dependent path.
     Corresponding Cosmic and dbSNP databases (VCFs) can be added to the mix.
*)
type t = {
  name: string;
  location: Ketrew.EDSL.user_target;
  cosmic:  Ketrew.EDSL.user_target option;
  dbsnp:  Ketrew.EDSL.user_target option;
}
let create ?cosmic ?dbsnp name location =
  {name; location; cosmic; dbsnp}

let on_host ~host ?cosmic ?dbsnp name path =
  let location = Ketrew.EDSL.file_target ~host path in
  let cosmic = Option.map ~f:(Ketrew.EDSL.file_target ~host) cosmic in
  let dbsnp = Option.map ~f:(Ketrew.EDSL.file_target ~host) dbsnp in
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

let fasta: t -> Ketrew.EDSL.user_target = fun t -> t.location
let cosmic_exn t =
  Option.value_exn ~msg:(sprintf "%s: no COSMIC" t.name) t.cosmic
let dbsnp_exn t =
  Option.value_exn ~msg:(sprintf "%s: no DBSNP" t.name) t.dbsnp
