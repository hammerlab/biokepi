
open Common
open Run_environment
module K = KEDSL

let hla_type ~work_dir ~run_with ~r1 ~r2 ~run_name =
  let tool = Machine.get_tool run_with (`Biopamed "seq2HLA") in
  (* Why quote this here? Seems like it easy to create a bug,
     why not enforce this at node construction ?*)
  let r1pt = Filename.quote r1#product#path in
  let r2pt = Filename.quote r2#product#path in
  let name = sprintf "seq2HLA-%s" run_name in
  let make =
    Machine.run_program run_with ~name
      K.Program.(Tool.init tool
                && exec ["mkdir"; "-p"; work_dir]
                && exec ["cd"; work_dir]
                && shf "seq2HLA -1 %s -2 %s -r %s" r1pt r2pt run_name)
  in
  K.workflow_node K.nothing
    ~make
    ~edges:[ K.depends_on (Tool.ensure tool)
           ; K.depends_on r1
           ; K.depends_on r2
           ]

