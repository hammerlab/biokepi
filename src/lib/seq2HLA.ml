
open Common
open Run_environment
module K = KEDSL

let hla_type ?host ~work_dir ~run_with ~r1 ~r2 ~run_name =
  let tool = Machine.get_tool run_with (`Biopamed "seq2HLA") in
  let install_path = Machine.get_toolkit_path run_with `Biopam in
  let r1pt = Filename.quote r1#product#path in
  let r2pt = Filename.quote r2#product#path in
  let name = sprintf "seq2HLA-%s" run_name in
  let make =
    Machine.run_program run_with ~name
      K.Program.(Tool.init tool
                && exec ["mkdir"; "-p"; work_dir]
                && exec ["cd"; work_dir]
                && Conda.run_in_biokepi_env ~install_path
                    (shf "seq2HLA -1 %s -2 %s -r %s" r1pt r2pt run_name))
  in
  let class1 = work_dir // (sprintf "%s-ClassI.HLAgenotype4digits" run_name) in
  let class2 = work_dir // (sprintf "%s-ClassII.HLAgenotype4digits" run_name) in
  let cond = K.list_of_files ?host [class1; class2] in
  K.workflow_node cond ~make
    ~edges:[ K.depends_on (Tool.ensure tool)
           ; K.depends_on r1
           ; K.depends_on r2
           ]

