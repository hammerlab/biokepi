
open Biokepi_run_environment
open Common


type product = <
  host: Ketrew_pure.Host.t;
  is_done : Ketrew_pure.Target.Condition.t option ;
  path: string >

let transform_optitype_product ?host ~path o =
  let host = match host with
  | None -> o#host
  | Some h -> h
  in
  let vol =
    let open Ketrew_pure.Target.Volume in
    create (dir (Filename.basename path) []) ~host
      ~root:(Ketrew_pure.Path.absolute_directory_exn (Filename.dirname path))
  in
  object
    method host = host
    method is_done = Some (`Volume_exists vol)
    method path = path
  end

(**
   Run OptiType in [`RNA] or [`DNA] mode.

   Please provide a fresh [work_dir] directory, it will be deleted in case of
   failure.
*)
let hla_type ~work_dir ~run_with ~fastq ~run_name nt
  : product KEDSL.workflow_node
  =
  let tool = Machine.get_tool run_with Machine.Tool.Default.optitype in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "optitype-%s" run_name in
  let make =
    Machine.run_big_program run_with ~name
      ~self_ids:["optitype"]
      KEDSL.Program.(
        Machine.Tool.init tool
        && exec ["mkdir"; "-p"; work_dir]
        && exec ["cd"; work_dir]
        && sh "cp -r ${OPTITYPE_DATA}/data ." (* HLA reference data *)
        && (* config example *)
        sh "cp -r ${OPTITYPE_DATA}/config.ini.example config.ini"
        && (* adjust config razers3 path *)
        sh "sed -i.bak \"s|\\/path\\/to\\/razers3|$(which razers3)|g\" config.ini"
        &&
        shf "OptiTypePipeline --verbose --input %s %s %s -o %s "
          (Filename.quote r1_path)
          (Option.value_map ~default:"" r2_path_opt ~f:Filename.quote)
          (match nt with | `DNA -> "--dna" | `RNA -> "--rna")
          run_name)
  in
  let product =
    let host = Machine.as_host run_with in
    let vol =
      let open Ketrew_pure.Target.Volume in
      create (dir run_name []) ~host
        ~root:(Ketrew_pure.Path.absolute_directory_exn work_dir)
    in
    object
      method is_done = Some (`Volume_exists vol)
      method path = work_dir
      method host = host
    end
  in
  KEDSL.workflow_node product ~name ~make
    ~edges:(
      [
        KEDSL.depends_on (Machine.Tool.ensure tool);
        KEDSL.depends_on fastq;
        KEDSL.on_failure_activate
          (Workflow_utilities.Remove.directory ~run_with work_dir);
      ]
    )
