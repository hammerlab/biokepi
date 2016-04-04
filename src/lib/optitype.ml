
open Common
open Run_environment

let hla_type ~work_dir ~run_with ~r1 ~r2 ~run_name nt =
  let tool = Machine.get_tool run_with (`Biopamed "optitype") in
  let r1pt = Filename.quote r1#product#path in
  let r2pt = Filename.quote r2#product#path in
  let name = sprintf "optitype-%s" run_name in
  let make =
    Machine.run_program run_with ~name
      KEDSL.Program.(Tool.init tool
        && exec ["mkdir"; "-p"; work_dir]
        && exec ["cd"; work_dir]
        && sh "cp -r $(opam config var lib)/optitype/data ."                         (* HLA reference data *)
        && sh "cp -r $(opam config var lib)/optitype/config.ini.example config.ini"  (* config example *)
        && sh "sed -i.bak \"s|\\/path\\/to\\/razers3|$(which razers3)|g\" config.ini"   (* adjust config razers3 path *)
        && shf "OptiTypePipeline -i %s %s %s -o %s "
            r1pt r2pt (match nt with | `DNA -> "--dna" | `RNA -> "--rna") run_name)
  in
  (* The real condition should be that a directory with run_name exists.
     I don't understand how to create that.
    let host = Machine.as_host run_with in *)
  let cond = KEDSL.nothing in
  KEDSL.workflow_node cond ~name ~make
    ~edges:[
      KEDSL.depends_on (Tool.ensure tool);
      KEDSL.depends_on r1;
      KEDSL.depends_on r2;
    ]

