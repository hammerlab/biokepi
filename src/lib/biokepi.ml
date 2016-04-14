
module Pipeline = Biokepi_pipeline_edsl.Pipeline

module EDSL = struct

  module type Semantics = Biokepi_pipeline_edsl.Semantics.Bioinformatics_base

  module Compile = struct
    module To_display = Biokepi_pipeline_edsl.To_display
    module To_workflow = Biokepi_pipeline_edsl.To_workflow

    module To_json : Semantics.Bioinformatics_base
      with type 'a repr = var_count: int -> Yojson.Basic.json
       and
       type 'a observation = Yojson.Basic.json =
      Biokepi_pipeline_edsl.To_json

    module To_dot : Semantics.Bioinformatics_base
      with 
       type 'a observation = string =
      Biokepi_pipeline_edsl.To_dot
  end

  module Transform = struct

    module Apply_functions = Biokepi_pipeline_edsl.Transform_applications.Apply

  end

end

module KEDSL = Biokepi_run_environment.Common.KEDSL

module Metadata = Biokepi_run_environment.Metadata

module Machine = Biokepi_run_environment.Machine

module Tools = Biokepi_bfx_tools

module Setup = struct

  include Biokepi_environment_setup

end

