(* Hassle-free HLA/MHC handling *)
open Common

type predictor_type = [
  | `NetMHC
  | `NetMHCpan
  | `NetMHCIIpan
  | `NetMHCcons
  | `Random
  | `SMM
  | `SMM_PMBEC
  | `NetMHCpan_IEDB
  | `NetMHCcons_IEDB
  | `SMM_IEDB
  | `SMM_PMBEC_IEDB
]

let predictor_to_string = function
  | `NetMHC -> "netmhc"
  | `NetMHCpan -> "netmhcpan"
  | `NetMHCIIpan -> "netmhciipan"
  | `NetMHCcons -> "netmhccons"
  | `Random -> "random"
  | `SMM -> "smm"
  | `SMM_PMBEC -> "smm-pmbec"
  | `NetMHCpan_IEDB -> "netmhcpan-iedb"
  | `NetMHCcons_IEDB -> "netmhccons-iedb"
  | `SMM_IEDB -> "smm-iedb"
  | `SMM_PMBEC_IEDB -> "smm-pmbec-iedb"

let predictor_to_tool ~run_with predictor =
  let get_tool t =
    let tool =
      Machine.get_tool
        run_with
        Machine.Tool.Definition.(create t)
    in
    let ensure = Machine.Tool.(ensure tool) in
    let init = Machine.Tool.(init tool) in
    (ensure, init)
  in
  match predictor with
  | `NetMHC -> Some (get_tool "netMHC")
  | `NetMHCpan -> Some (get_tool "netMHCpan")
  | `NetMHCIIpan -> Some (get_tool "netMHCIIpan")
  | `NetMHCcons -> Some (get_tool "netMHCcons")
  | _ -> None


(* 
  Example input (all in):
    1,A*03:01,,0.026478,samplename
    1,B*37:01',,0.000000,samplename
    1, C*06:02,,0.000086,samplename
    2,DQA1*01:02 ,,0.000000,samplename
  Example output (type-I predictions out as a plain list):
    A*03:01
    B*37:01
    C*06:02
    C*07:02

  So the following "one-liner"
    - extracts the second column
    - trims the allele names from white-spaces around them
    - gets rid of 's at the end of the allele names
    - removes type-II predictions (since we don't support
      type-II predictions)
  to be able to feed the file into `mhctools` based utilities,
  including topiary, vaxrank, and netmhctools itself.
*)

let sanitize_hlarp_out_for_mhctools ~run_with ~hlarp_result ~output_path = 
  let open KEDSL in
  let input_path = hlarp_result#product#path in
  let name = 
    sprintf
      "Extract and sanitize alleles: %s"
      (hlarp_result#render#name)
  in
  let edges = [ depends_on hlarp_result; ] in
  let product = single_file ~host:(Machine.as_host run_with) output_path in
  let tmp_path = output_path ^ ".tmp" in
  let make = Machine.(
    run_program run_with
      ~requirements:[ `Quick_run; ]
      Program.(
        shf "cat %s \
             | grep -v '^2' \
             | awk -F , '{ gsub(/^[ \t]+|[ \t]+$/,\"\", $2); print $2}' \
             | tail -n +2 \
             | sed \"s/'//\" > %s \
             && mv %s %s"
           input_path tmp_path tmp_path output_path
      )
    )
  in
  workflow_node product ~name ~make ~edges 
