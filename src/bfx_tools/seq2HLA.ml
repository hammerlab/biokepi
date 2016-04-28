
open Biokepi_run_environment
open Common

let hla_type ~work_dir ~run_with ~r1 ~r2 ~run_name =
  let tool = Machine.get_tool run_with (`Biopamed "seq2HLA") in
  (* Why quote this here? Seems like it easy to create a bug,
     why not enforce this at node construction ?*)
  let r1pt = Filename.quote r1#product#path in
  let r2pt = Filename.quote r2#product#path in
  let name = sprintf "seq2HLA-%s" run_name in
  let host = Machine.as_host run_with in
  let processors = Machine.max_processors run_with in
  let make =
    Machine.run_big_program run_with ~name ~processors
      KEDSL.Program.(Machine.Tool.init tool
                     && exec ["mkdir"; "-p"; work_dir]
                     && exec ["cd"; work_dir]
                     && shf "seq2HLA -1 %s -2 %s -r %s -p %d"
                       r1pt r2pt run_name processors)
  in
  let class1 = work_dir // (sprintf "%s-ClassI.HLAgenotype4digits" run_name) in
  let class2 = work_dir // (sprintf "%s-ClassII.HLAgenotype4digits" run_name) in
  let cond = KEDSL.list_of_files ~host [class1; class2] in
  KEDSL.workflow_node ~name cond ~make
    ~edges:[
      KEDSL.depends_on (Machine.Tool.ensure tool);
      KEDSL.depends_on r1;
      KEDSL.depends_on r2;
    ]

