
open Biokepi_run_environment
open Common


type product = <
    is_done : Ketrew_pure.Target.Condition.t option ;
    class1_path : string;
    class2_path: string;
    work_dir_path: string >


let hla_type
    ~work_dir ~run_with ~r1 ~r2 ~run_name : product KEDSL.workflow_node =
  let tool = Machine.get_tool run_with Machine.Tool.Default.seq2hla in
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
  let class1_path = work_dir // (sprintf "%s-ClassI.HLAgenotype4digits" run_name) in
  let class2_path = work_dir // (sprintf "%s-ClassII.HLAgenotype4digits" run_name) in
  let product =
    let class1 = KEDSL.single_file ~host class1_path in
    let class2 = KEDSL.single_file ~host class2_path in
    object
      method is_done =
        Some (`And
                (List.filter_map ~f:(fun f -> f#is_done) [class1; class2]))
      method class1_path = class1_path
      method class2_path = class2_path
      method work_dir_path = work_dir
    end
  in
  KEDSL.workflow_node ~name product ~make
    ~edges:[
      KEDSL.depends_on (Machine.Tool.ensure tool);
      KEDSL.depends_on r1;
      KEDSL.depends_on r2;
      KEDSL.on_failure_activate
        (Workflow_utilities.Remove.directory work_dir ~run_with);
    ]

